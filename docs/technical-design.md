# Technical Design

Architecture and implementation details of the MongoDB Replication Tool.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [State Management](#state-management)
- [Parallel Processing Model](#parallel-processing-model)
- [PII Detection Pipeline](#pii-detection-pipeline)
- [Data Flow](#data-flow)
- [Extension Points](#extension-points)

## Architecture Overview

The tool follows a modular, layered architecture:

```
┌─────────────────────────────────────────────────────────┐
│                     CLI Layer                           │
│  (Typer commands, interactive prompts, output formatting)│
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Orchestration Layer                      │
│    (ReplicationOrchestrator, Collection Discovery)     │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                 Replication Layer                       │
│  (CollectionReplicator, StateManager, IndexManager)    │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Processing Layer                         │
│   (PII Handler, Field Transformer, Field Excluder)     │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                  Data Layer                             │
│          (PyMongo, ConnectionManager)                   │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. ReplicationOrchestrator

**Responsibility**: Coordinates the entire replication process

**Key Methods**:
- `replicate()`: Main entry point for replication
- `_build_collection_config()`: Merges explicit and default configurations
- `_replicate_single_collection()`: Replicates one collection with error handling
- `_validate_cursor_fields()`: Ensures cursor fields exist and are indexed

**State Management**:
- Creates run record at start (`create_run()`)
- Creates collection state before replicating each collection
- Updates run statistics on completion
- Handles failures and error aggregation

**Parallelism**:
- Uses `ThreadPoolExecutor` for parallel collection processing
- Configurable worker pool size via `max_parallel_collections`
- Thread-safe state updates

### 2. CollectionReplicator

**Responsibility**: Replicates a single collection from source to destination

**Key Methods**:
- `replicate()`: Main replication logic with write disposition handling
- `_replicate_replace()`: Drop and recreate collection
- `_replicate_append()`: Add documents without deleting existing
- `_replicate_merge()`: Update existing documents, insert new ones

**Incremental Loading**:
- Queries last cursor value from state
- Applies cursor filter to source query
- Updates cursor position after each batch
- Supports any monotonically increasing field

**Batch Processing**:
- Processes documents in configurable batch sizes
- Applies transformations and PII redaction per batch
- Updates state after each batch for fault tolerance

### 3. StateManager

**Responsibility**: Manages job run and collection state

**Schema**:

#### `_rep_runs` Collection
```python
{
    "_id": ObjectId,                    # Run ID
    "status": str,                       # "running", "completed", "failed"
    "startedAt": datetime,
    "endedAt": datetime,
    "durationSeconds": float,
    "collections": {
        "processed": int,
        "succeeded": int,
        "failed": int
    },
    "documents": {
        "processed": int,
        "succeeded": int,
        "failed": int
    },
    "errors": {
        "summary": {},
        "collections": []
    }
}
```

#### `_rep_state` Collection
```python
{
    "_id": ObjectId,                    # State ID
    "runId": ObjectId,                  # Reference to run
    "collection": str,
    "status": str,                       # "running", "completed", "failed", "skipped"
    "startedAt": datetime,
    "endedAt": datetime,
    "durationSeconds": float,
    "documents": {
        "processed": int,
        "succeeded": int,
        "failed": int
    },
    "lastCursorValue": Any,             # Native BSON type
    "lastCursorField": str,
    "error": {}
}
```

**Indexes**:
- `(collection, endedAt)`: Compound index for efficient incremental loading lookups
- Automatically drops legacy `collection_name_unique` index

**Key Methods**:
- `create_run()`: Creates new run record
- `start_collection()`: Creates collection state and links to run
- `complete_collection()`: Updates collection state with final statistics
- `update_collection_state()`: Updates cursor position during batch processing
- `get_last_cursor_value()`: Retrieves cursor for incremental loading

### 4. CollectionDiscovery

**Responsibility**: Auto-discovers collections from source database

**Features**:
- Lists all collections in source database
- Applies include/exclude regex patterns
- Automatically excludes state management collections
- Distinguishes configured vs auto-discovered collections

**Filtering Logic**:
1. Exclude state collections (always)
2. If `replicate_all=true`: Include all except those matching `exclude_patterns`
3. If `replicate_all=false`: Only include those matching `include_patterns`
4. Apply exclusions (takes precedence)

### 5. PII Handler

**Responsibility**: Detects and anonymizes PII in documents

**Detection Modes**:

#### Field Name Detection
- Fast, rule-based matching
- Checks field names against known PII patterns
- Example: `email`, `phone`, `ssn`

#### Content Detection
- Uses Microsoft Presidio NLP engine
- Analyzes field values for PII entities
- Supports: PERSON, EMAIL_ADDRESS, PHONE_NUMBER, CREDIT_CARD, etc.

**Anonymization Methods**:

```python
class PIIHandler:
    def anonymize_email(value: str) -> str:
        # "user@example.com" → "u***@example.com"
        
    def anonymize_hash(value: str) -> str:
        # SHA-256 hash
        
    def anonymize_redact(value: Any) -> str:
        # Replace with "[REDACTED]"
        
    def anonymize_replace(value: str, entity_type: str) -> str:
        # Generate fake data matching type
```

**Pipeline Integration**:
- Applied during batch processing
- Processes nested documents recursively
- Preserves document structure
- Handles arrays of documents

## State Management

### Design Principles

1. **Parent-Child Relationship**: Runs contain multiple collection states
2. **ObjectId-based**: Uses MongoDB ObjectIds for all IDs (not UUIDs)
3. **Configurable Names**: Collection names are configurable via YAML
4. **Fault Tolerance**: State updated after each batch, not just at end
5. **Backward Compatibility**: Deprecated methods for legacy code

### Run Lifecycle

```python
# 1. Orchestrator creates run
run_id = state_mgr.create_run()

# 2. For each collection
state_id = state_mgr.start_collection(
    run_id=run_id,
    collection_name="users",
    cursor_field="updated_at"
)

# 3. During replication
for batch in batches:
    process_batch(batch)
    state_mgr.update_collection_state(
        state_id=state_id,
        last_cursor_value=batch[-1]["updated_at"],
        documents_processed=len(batch)
    )

# 4. On completion
state_mgr.complete_collection(
    state_id=state_id,
    documents_processed=total_docs,
    duration_seconds=elapsed
)

# 5. After all collections
state_mgr.complete_run(
    run_id=run_id,
    collections_stats={...},
    documents_stats={...}
)
```

### Incremental Loading

```python
# Get last cursor value from previous run
last_cursor = state_mgr.get_last_cursor_value("users")

# Build query with cursor filter
query = {}
if last_cursor is not None:
    query[cursor_field] = {"$gt": last_cursor}

# Execute query
cursor = source_collection.find(query).sort(cursor_field, 1)
```

## Parallel Processing Model

### Collection-Level Parallelism

```python
with ThreadPoolExecutor(max_workers=max_parallel) as executor:
    futures = {}
    
    for collection_name in collections:
        future = executor.submit(
            self._replicate_single_collection,
            collection_name,
            config
        )
        futures[future] = collection_name
    
    for future in as_completed(futures):
        collection_name = futures[future]
        result = future.result()
        # Handle result
```

**Advantages**:
- Collections processed independently
- Failure of one collection doesn't affect others
- Better resource utilization

**Thread Safety**:
- Each collection has its own replicator instance
- State updates are atomic at document level
- PyMongo connections are thread-safe

### Batch Processing

```python
batch = []
for doc in cursor:
    batch.append(doc)
    
    if len(batch) >= batch_size:
        process_batch(batch)
        update_state(last_cursor=doc[cursor_field])
        batch = []

# Process remaining
if batch:
    process_batch(batch)
```

**Trade-offs**:
- Smaller batches: More frequent state updates, slower overall
- Larger batches: Faster overall, but more work lost on failure

## PII Detection Pipeline

### Pipeline Stages

```
Document → Field Identification → Detection → Anonymization → Output
```

### 1. Field Identification

```python
def identify_pii_fields(doc, config):
    """Recursively identify fields containing PII."""
    pii_fields = []
    
    for field_path in config.pii.fields:
        if field_path in doc:
            pii_fields.append((field_path, doc[field_path]))
    
    return pii_fields
```

### 2. Detection

#### Field Name Mode:
```python
FIELD_NAME_PATTERNS = {
    'email': ['email', 'e_mail', 'email_address'],
    'phone': ['phone', 'telephone', 'mobile'],
    'ssn': ['ssn', 'social_security'],
}

def detect_by_field_name(field_name):
    for entity_type, patterns in FIELD_NAME_PATTERNS.items():
        if any(p in field_name.lower() for p in patterns):
            return entity_type
    return None
```

#### Content Mode:
```python
from presidio_analyzer import AnalyzerEngine

analyzer = AnalyzerEngine()

def detect_by_content(text, entities):
    results = analyzer.analyze(
        text=str(text),
        entities=entities,
        language='en'
    )
    return results
```

### 3. Anonymization

```python
def anonymize_value(value, method):
    if method == 'mask':
        return mask_string(value)
    elif method == 'hash':
        return hashlib.sha256(str(value).encode()).hexdigest()
    elif method == 'redact':
        return '[REDACTED]'
    elif method == 'replace':
        return generate_fake_value(value)
```

## Data Flow

### Complete Replication Flow

```
1. CLI Command
   ↓
2. Load Configuration (YAML + Env Vars)
   ↓
3. Create Connections (Source & Dest)
   ↓
4. Discovery (Auto-discover collections)
   ↓
5. Build Configs (Merge explicit + defaults)
   ↓
6. Validate (Cursor fields, relationships)
   ↓
7. Create Run (State management)
   ↓
8. Parallel Collection Processing:
   ├─ For each collection:
   │  ├─ Create collection state
   │  ├─ Get last cursor value
   │  ├─ Query source with cursor filter
   │  ├─ For each batch:
   │  │  ├─ Apply transformations
   │  │  ├─ Apply PII redaction
   │  │  ├─ Exclude fields
   │  │  ├─ Write to destination
   │  │  └─ Update state
   │  └─ Complete collection state
   ↓
9. Complete Run (Aggregate statistics)
   ↓
10. Report Results
```

### Write Disposition Flows

#### Replace Mode:
```
1. Drop destination collection
2. Recreate with data
3. Replicate indexes
```

#### Append Mode:
```
1. Query source (with cursor filter)
2. Insert documents to destination
3. Update state
```

#### Merge Mode:
```
1. Query source (with cursor filter)
2. For each document:
   - Upsert by primary_key
3. Update state
```

## Extension Points

### Custom Field Transformations

```python
from mongo_replication.engine.transformations import FieldTransformer

class CustomTransformer:
    def transform(self, value, config):
        # Your transformation logic
        return transformed_value

# Register in configuration
transformer = FieldTransformer(
    transforms=[
        {
            "field": "price",
            "operation": "custom",
            "function": custom_price_transform
        }
    ]
)
```

### Custom PII Detection

```python
from mongo_replication.engine.pii import PIIHandler

class CustomPIIHandler(PIIHandler):
    def detect_custom_entity(self, text):
        # Custom detection logic
        return entity_type
    
    def anonymize_custom(self, value, entity_type):
        # Custom anonymization
        return anonymized_value
```

### Custom State Backends

```python
from mongo_replication.engine.state import StateManager

class CustomStateManager(StateManager):
    def __init__(self, backend_config):
        # Initialize custom backend (Redis, PostgreSQL, etc.)
        pass
    
    def create_run(self):
        # Store in custom backend
        pass
```

### Progress Callbacks

```python
from mongo_replication import ReplicationOrchestrator

def progress_callback(collection_name, status, result):
    print(f"{collection_name}: {status}")
    if result:
        print(f"  Docs: {result.documents_processed}")

orchestrator.replicate(progress_callback=progress_callback)
```

## Performance Considerations

### Indexing

**Required Indexes**:
1. Cursor fields on source collections
2. Primary keys on destination (for merge mode)
3. State collection indexes (automatic)

### Memory Management

- Batch processing prevents loading entire collections into memory
- Generator-based cursor iteration
- Explicit cursor cleanup after each collection

### Network Optimization

- Batch inserts reduce round trips
- Connection pooling via PyMongo
- Compression support in connection URI

### Monitoring

**Metrics to Track**:
- Documents per second
- Collections per minute
- State update frequency
- Error rates
- Memory usage

**Example**:
```python
result = orchestrator.replicate()

print(f"Throughput: {
    result.total_documents_processed / 
    result.total_duration_seconds
} docs/sec")
```

## Error Handling

### Levels of Error Handling

1. **Document-level**: Skip invalid documents, continue batch
2. **Batch-level**: Retry failed batches with exponential backoff
3. **Collection-level**: Mark collection as failed, continue with others
4. **Run-level**: Fail entire run if critical error

### Error Propagation

```python
try:
    result = replicator.replicate()
except DocumentValidationError as e:
    # Log and skip document
    logger.warning(f"Skipping invalid document: {e}")
except BatchProcessingError as e:
    # Retry batch
    retry_with_backoff(batch)
except CollectionError as e:
    # Mark collection as failed
    state_mgr.fail_collection(state_id, str(e))
except CriticalError as e:
    # Fail entire run
    state_mgr.fail_run(run_id, str(e))
    raise
```

## Testing Strategy

### Unit Tests
- Individual component behavior
- Mock external dependencies
- Test edge cases and error conditions

### Integration Tests
- End-to-end replication flows
- Real MongoDB instances
- State management verification