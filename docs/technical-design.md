# Technical Design

Comprehensive architecture and implementation details of the MongoDB Replication Tool.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
  - [ReplicationOrchestrator](#1-replicationorchestrator)
  - [CollectionReplicator](#2-collectionreplicator)
  - [StateManager](#3-statemanager)
  - [CollectionDiscovery](#4-collectiondiscovery)
  - [PresidioAnalyzer](#5-presidioanalyzer)
  - [PresidioAnonymizer](#6-presidioanonymizer)
  - [Additional Components](#7-additional-components)
- [State Management](#state-management)
- [Parallel Processing Model](#parallel-processing-model)
- [PII Detection & Anonymization](#pii-detection--anonymization)
- [Data Flow](#data-flow)
- [Extension Points](#extension-points)
- [Performance Considerations](#performance-considerations)
- [Error Handling](#error-handling)

## Architecture Overview

The tool follows a modular, layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────┐
│                     CLI Layer                           │
│  (Typer commands, signal handlers, interactive UI,     │
│   progress reporting, output formatting)               │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Orchestration Layer                      │
│  (ReplicationOrchestrator, CollectionDiscovery,        │
│   JobManager, Progress Callbacks)                      │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                 Replication Layer                       │
│  (CollectionReplicator, StateManager, IndexManager,    │
│   CursorValidation, CascadeFilter)                     │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Processing Layer                         │
│   (PresidioAnonymizer, FieldTransformer,              │
│    FieldExcluder, CustomOperators)                    │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                  Analysis Layer                         │
│  (PresidioAnalyzer, PIIAnalysisEngine,                │
│   CollectionSampler, RelationshipInferrer)            │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                  Data Layer                             │
│          (PyMongo, ConnectionManager,                   │
│           Config Management)                            │
└─────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Single Responsibility**: Each component has one clear purpose
2. **Dependency Injection**: Components receive dependencies, not create them
3. **Configuration over Code**: Behavior driven by YAML configuration
4. **Fail Fast**: Validate early, fail with clear error messages
5. **Thread Safety**: Thread-safe where needed for parallel processing
6. **Native BSON**: Preserves MongoDB types throughout the pipeline

## Core Components

### 1. ReplicationOrchestrator

**Location**: `src/mongo_replication/engine/orchestrator.py`

**Responsibility**: Coordinates the entire replication process across multiple collections

**Key Methods**:
```python
def replicate(
    collection_names: Optional[List[str]] = None,
    collection_filters: Optional[Dict[str, Any]] = None,
    cascade_from_ids: Optional[Dict[str, List]] = None,
    cascade_from_query: Optional[Dict[str, str]] = None,
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None
) -> OrchestrationResult
```

**Workflow**:
1. **Create Run**: Initialize run tracking in state management
2. **Discovery**: Auto-discover collections + merge with configured collections
3. **Build Configs**: For each collection:
   - Use explicit config if available
   - Merge with defaults for auto-discovered collections
   - Validate cursor fields (with fallback to `_id`)
4. **Parallel Execution**: ThreadPoolExecutor with configurable workers
5. **Result Aggregation**: Collect results, update run statistics
6. **Error Handling**: Isolate collection failures, aggregate errors

**Configuration Merging**:
```python
def _build_collection_config(name, explicit_config):
    """Merge explicit + defaults with validation."""
    config = copy.deepcopy(defaults)  # Start with defaults

    if explicit_config:
        # Override with explicit config
        config.write_disposition = explicit_config.write_disposition or config.write_disposition
        config.cursor_fields = explicit_config.cursor_fields or config.cursor_fields
        # ... merge all fields

    # Validate cursor fields exist in collection
    config.cursor_field = validate_cursor_field(name, config.cursor_fields)

    return config
```

**Parallelism**:
- Uses `ThreadPoolExecutor` for collection-level parallelism
- Configurable via `max_parallel_collections` (default: 5)
- Thread-safe state updates via PyMongo's connection pooling
- Independent failure isolation - one collection failure doesn't stop others

**State Management**:
- Creates run record at start
- Creates collection state before each collection
- Updates run statistics on completion
- Handles failures and error aggregation

### 2. CollectionReplicator

**Location**: `src/mongo_replication/engine/replicator.py`

**Responsibility**: Replicates a single collection from source to destination

**Key Methods**:
```python
def replicate() -> CollectionResult:
    """Main replication logic."""
    if write_disposition == "replace":
        return _replicate_replace()
    elif write_disposition == "append":
        return _replicate_append()
    elif write_disposition == "merge":
        return _replicate_merge()
```

**Write Strategies**:

#### Replace Mode
```python
def _replicate_replace():
    """Drop and recreate collection."""
    # 1. Drop destination collection (first batch only)
    if is_first_batch:
        dest_collection.drop()

    # 2. Query source (NO cursor filter - full reload)
    #    Uses skip/limit pagination instead
    query = match_filter or {}
    cursor = source.find(query).skip(skip).limit(batch_size)

    # 3. Process batches
    while batch := fetch_batch():
        processed = apply_transformations_and_exclusions(batch)
        dest_collection.insert_many(processed)
        skip += batch_size

    # 4. Recreate indexes
    index_manager.replicate_indexes(source, dest)
```

#### Append Mode
```python
def _replicate_append():
    """Insert new documents only."""
    # 1. Get last cursor value
    last_cursor = state_mgr.get_last_cursor_value(collection)

    # 2. Build query with cursor filter
    query = build_query(cursor_field, last_cursor, match_filter)
    cursor = source.find(query).sort(cursor_field, 1)

    # 3. Process batches
    last_cursor_value = last_cursor
    while batch := fetch_batch():
        processed = apply_transformations_and_exclusions(batch)
        dest_collection.insert_many(processed)
        last_cursor_value = batch[-1][cursor_field]

        # Track cursor locally (don't re-read state)
        query = {cursor_field: {"$gt": last_cursor_value}}

    # 4. Update state ONCE at end
    state_mgr.update_collection_state(state_id, last_cursor_value, ...)
```

#### Merge Mode
```python
def _replicate_merge():
    """Upsert documents by primary key."""
    # Similar to append but uses bulk_write with ReplaceOne
    operations = [
        ReplaceOne(
            filter={primary_key: doc[primary_key]},
            replacement=doc,
            upsert=True
        )
        for doc in processed_batch
    ]
    dest_collection.bulk_write(operations)
```

**Processing Pipeline** (CRITICAL ORDER):
```python
def _apply_transformations_and_exclusions(documents):
    """Apply transformations in strict order."""
    # 1. Field transformations (regex replace)
    if field_transformer:
        documents = field_transformer.transform_documents(documents)

    # 2. PII redaction (operates on transformed data!)
    documents = pii_handler.process_documents(documents)

    # 3. Field exclusions (remove unwanted fields)
    if field_excluder:
        documents = field_excluder.exclude_fields_from_documents(documents)

    return documents
```

> **⚠️ Order Matters**: PII detection runs on *transformed* data, ensuring anonymization sees final field values. Exclusions run last to remove unwanted fields after PII processing.

**Query Building**:
```python
def _build_query(cursor_field, last_cursor, match_filter):
    """Combine cursor + user filters."""
    filters = []

    # Incremental cursor filter
    if cursor_field and last_cursor is not None:
        filters.append({cursor_field: {"$gt": last_cursor}})

    # User match filter (from YAML config)
    if match_filter:
        filters.append(match_filter)

    # Combine with $and
    return {"$and": filters} if len(filters) > 1 else filters[0] if filters else {}
```

**Error Handling**:
- **Transform errors**: Configurable - "skip" (log and continue) or "fail" (stop)
- **Bulk write errors**: Summarized (removes document data for readability)
- **Collection failure**: Isolated - doesn't affect other collections

### 3. StateManager

**Location**: `src/mongo_replication/engine/state.py`

**Responsibility**: Manages replication run tracking and collection state

**Design**:
- Two-collection design: `_rep_runs` (runs) + `_rep_state` (collection states)
- Parent-child relationship: Runs contain multiple collection states
- ObjectId-based (not UUIDs) for native MongoDB integration
- Configurable collection names via YAML

#### `_rep_runs` Schema

```python
{
    "_id": ObjectId,                     # Run ID
    "status": "running|completed|failed",
    "startedAt": datetime,               # ISO 8601
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
        "summary": {},                    # {collection: error_msg}
        "collections": []                 # List of failed collection names
    }
}
```

#### `_rep_state` Schema

```python
{
    "_id": ObjectId,                     # State ID
    "runId": ObjectId,                   # Reference to parent run
    "collection": str,                   # Collection name
    "status": "running|completed|failed|skipped",
    "startedAt": datetime,
    "endedAt": datetime,
    "durationSeconds": float,
    "documents": {
        "processed": int,
        "succeeded": int,
        "failed": int
    },
    "lastCursorValue": Any,              # 🔑 Native BSON type!
    "lastCursorField": str,
    "error": {}                          # Error details if failed
}
```

> **🔑 Key Feature**: `lastCursorValue` preserves **native BSON types** (datetime, ObjectId, Decimal128, etc.) - no string conversion needed for incremental loading.

**Indexes**:
```python
# Compound index for efficient cursor lookups
("collection", "endedAt"): 1
```

**Key Methods**:
```python
# Run lifecycle
run_id = create_run()
complete_run(run_id, collections_stats, documents_stats)
fail_run(run_id, error_message)

# Collection lifecycle
state_id = start_collection(run_id, collection_name, cursor_field)
update_collection_state(state_id, last_cursor_value, docs_processed)
complete_collection(state_id, docs_processed, duration)
fail_collection(state_id, error_message)

# Incremental loading
last_cursor = get_last_cursor_value(collection_name)
```

**State Update Strategy**:
- **Append/Merge modes**: State updated **once at end** of replication
  - Cursor tracked locally during batch processing
  - Single state update when all batches complete
  - More efficient, trades granular resume for performance
- **Replace mode**: State not needed (full reload)

### 4. CollectionDiscovery

**Location**: `src/mongo_replication/engine/discovery.py`

**Responsibility**: Auto-discovers collections from source database with pattern matching

**Filtering Logic**:
```python
def discover_collections(configured_names):
    """Discover collections with pattern matching."""
    # 1. List all collections in source DB
    all_collections = source_db.list_collection_names()

    # 2. Always exclude state management collections
    all_collections = [c for c in all_collections
                      if c not in [runs_collection, state_collection]]

    # 3. Apply pattern matching
    if replicate_all:
        # Include all except those matching exclude patterns
        included = [c for c in all_collections
                   if not matches_any_pattern(c, exclude_patterns)]
    else:
        # Only include those matching include patterns
        included = [c for c in all_collections
                   if matches_any_pattern(c, include_patterns)]

        # Then apply exclusions (precedence)
        included = [c for c in included
                   if not matches_any_pattern(c, exclude_patterns)]

    # 4. Distinguish configured vs auto-discovered
    configured = [c for c in included if c in configured_names]
    auto_discovered = [c for c in included if c not in configured_names]

    return DiscoveryResult(
        included=included,
        excluded=excluded,
        configured=configured,
        auto_discovered=auto_discovered
    )
```

**Features**:
- Regex pattern matching (include/exclude)
- Automatic exclusion of state collections
- Distinguishes explicit vs auto-discovered collections
- Used by both `scan` and `run` commands

### 5. PresidioAnalyzer

**Location**: `src/mongo_replication/engine/pii/presidio_analyzer.py`

**Responsibility**: Auto-detects PII in MongoDB documents using Microsoft Presidio NLP

**Key Features**:
- **Singleton pattern** with lazy initialization (~500MB model loading)
- Supports **English and French** NLP models:
  - `en_core_web_lg` (English, default)
  - `fr_core_news_lg` (French)
- **YAML-based custom recognizers** for domain-specific PII
- **Sophisticated path resolution**:
  1. Absolute path
  2. Current working directory
  3. `config/` subdirectory
  4. Bundled default location

**Detection Process**:
```python
def analyze_document(doc, entity_types, allowlist, confidence_threshold):
    """Detect PII in a MongoDB document."""
    # 1. Flatten document to dot-notation
    flattened = flatten_document(doc)
    # {"user.email": "test@example.com", "user.profile.phone": "555-1234"}

    # 2. Filter out allowlisted fields
    fields = {k: v for k, v in flattened.items()
              if not matches_allowlist(k, allowlist)}

    # 3. Analyze each string field with Presidio
    pii_map = {}
    for field_path, value in fields.items():
        if isinstance(value, str):
            results = analyzer_engine.analyze(
                text=value,
                entities=entity_types,
                language='en'
            )

            # Filter by confidence
            for result in results:
                if result.score >= confidence_threshold:
                    pii_map[field_path] = (result.entity_type, result.score)

    return pii_map
    # {"user.email": ("EMAIL_ADDRESS", 0.95), "user.profile.phone": ("PHONE_NUMBER", 0.85)}
```

**Entity Types Detected**:
- Built-in: `EMAIL_ADDRESS`, `PHONE_NUMBER`, `PERSON`, `US_SSN`, `CREDIT_CARD`, `IBAN_CODE`, `IP_ADDRESS`, `URL`
- Custom: Defined via YAML recognizers (e.g., `EMPLOYEE_ID`, `PATIENT_ID`)

**Configuration** (`presidio.yaml`):
```yaml
custom_recognizers:
  - name: EmployeeIdRecognizer
    supported_entity: EMPLOYEE_ID
    patterns:
      - name: emp_pattern
        regex: "\\bEMP-\\d{5,8}\\b"
        score: 0.7
    context:
      - employee
      - staff
```

### 6. PresidioAnonymizer

**Location**: `src/mongo_replication/engine/pii/presidio_anonymizer.py`

**Responsibility**: Anonymizes PII in documents using Presidio AnonymizerEngine + custom operators

**Architecture**:
```python
class PresidioAnonymizer:
    def __init__(self, presidio_config_path=None):
        # 1. Initialize Presidio AnonymizerEngine
        self.anonymizer_engine = AnonymizerEngine()

        # 2. Register 10 custom operators
        for operator_class in CUSTOM_OPERATORS:
            self.anonymizer_engine.add_anonymizer(operator_class)

        # 3. Load YAML configuration
        self.presidio_config = PresidioConfig(presidio_config_path)
        self.operator_configs = presidio_config.get_operator_configs()
        self.strategy_aliases = presidio_config.get_strategy_aliases()
```

**Anonymization Process**:
```python
def apply_anonymization(document, pii_map, manual_overrides):
    """Anonymize document with detected + manual PII fields."""
    # 1. Build field operators (merge auto + manual)
    field_operators = {}

    # Auto-detected PII
    for field_path, (entity_type, score) in pii_map.items():
        operator_config = operator_configs.get(entity_type)
        field_operators[field_path] = operator_config

    # Manual overrides (precedence)
    for field_path, strategy in manual_overrides.items():
        resolved_operator = resolve_strategy(strategy)
        field_operators[field_path] = resolved_operator

    # 2. Anonymize each field
    result = copy.deepcopy(document)
    for field_path, operator_config in field_operators.items():
        value = get_nested_value(result, field_path)

        # Create synthetic RecognizerResult spanning entire value
        recognizer_result = RecognizerResult(
            entity_type=field_path,  # Use field path as type
            start=0,
            end=len(str(value)),
            score=1.0
        )

        # Anonymize with Presidio
        anonymized = anonymizer_engine.anonymize(
            text=str(value),
            analyzer_results=[recognizer_result],
            operators={field_path: operator_config}
        )

        # Update nested field
        set_nested_value(result, field_path, anonymized.text)

    return result
```

**Strategy Resolution**:
```python
def resolve_strategy(strategy_name):
    """Resolve strategy to operator config."""
    # 1. Check strategy aliases (e.g., "fake_email" → FakeEmailOperator)
    if strategy_name in strategy_aliases:
        operator_name = strategy_aliases[strategy_name]
        return OperatorConfig(operator_name, {})

    # 2. Check entity type mappings (e.g., "EMAIL_ADDRESS" → "mask")
    if strategy_name in operator_configs:
        return operator_configs[strategy_name]

    # 3. Use as direct operator name (e.g., "hash", "redact")
    return OperatorConfig(strategy_name, {})
```

**Custom Operators** (`custom_operators.py`):

10 custom operators implemented:

**Fake Data Generators** (Mimesis-based):
- `FakeEmailOperator` - Realistic email addresses
- `FakeNameOperator` - Full names (locale-aware)
- `FakePhoneOperator` - Phone numbers
- `FakeAddressOperator` - Street addresses
- `FakeSSNOperator` - US Social Security Numbers
- `FakeCreditCardOperator` - Credit card numbers
- `FakeIBANOperator` - International bank account numbers
- `FakeUSBankAccountOperator` - Routing + account numbers

**Special Operators**:
- `StripeTestingCCOperator` - Stripe test card numbers (4242...)
- `SmartRedactOperator` - Format-preserving redaction:
  ```python
  # Examples
  "user@example.com"    → "u***@example.com"
  "123-45-6789"         → "***-**-6789"
  "(555) 123-4567"      → "(***)***-1234"
  "192.168.1.1"         → "192.***.***.1"
  "generic string"      → "gen***ing"
  ```

**Configuration** (`presidio.yaml`):
```yaml
anonymization_operators:
  EMAIL_ADDRESS:
    operator: smart_redact
    params: {}

  PERSON:
    operator: replace
    params:
      new_value: "ANONYMOUS"

  PHONE_NUMBER:
    operator: mask
    params:
      masking_char: "*"
      chars_to_mask: 10
      from_end: false

  CREDIT_CARD:
    operator: hash
    params:
      hash_type: sha256

custom_strategy_aliases:
  fake_email:
    description: "Generate realistic fake email"
    operator: fake_email

  fake_name:
    description: "Generate realistic fake name"
    operator: fake_name
```

### 7. Additional Components

#### FieldTransformer
**Location**: `src/mongo_replication/engine/transformations.py`

**Purpose**: Apply regex-based transformations to field values

```yaml
field_transforms:
  - field: billing_plan
    type: regex_replace
    pattern: ".*"
    replacement: "free"
```

#### FieldExcluder
**Location**: `src/mongo_replication/engine/field_exclusion.py`

**Purpose**: Remove specified fields from documents

**Features**:
- "Keep parent with remaining fields" logic (doesn't remove empty parents)
- Supports dot notation for nested fields

#### IndexManager
**Location**: `src/mongo_replication/engine/indexes.py`

**Purpose**: Replicate indexes from source to destination

**Supported Index Types**:
- Single field
- Compound
- Text
- Geospatial (2d, 2dsphere)
- Hashed
- Wildcard

#### CursorValidation
**Location**: `src/mongo_replication/engine/validation.py`

**Purpose**: Validate cursor fields exist with automatic fallback

```python
def validate_cursor_field(collection, cursor_fields):
    """Check cursor fields exist, fallback to _id."""
    schema = get_collection_schema(collection)

    for field in cursor_fields:
        if field in schema:
            return field

    # Fallback to _id (always exists)
    return "_id"
```

#### CascadeFilter
**Location**: `src/mongo_replication/engine/cascade_filter.py`

**Purpose**: Build filters for cascade replication based on schema relationships

**Example**:
```python
# User specifies: --ids customers=507f1f77bcf86cd799439011
# Relationships: customers → orders → order_items

cascade_filter = CascadeFilterBuilder(relationships)
filters = cascade_filter.build_filters(
    root_collection="customers",
    root_ids=["507f1f77bcf86cd799439011"]
)

# Returns:
{
    "customers": {"_id": {"$in": [ObjectId("507f1f77bcf86cd799439011")]}},
    "orders": {"customer_id": {"$in": [ObjectId("507f1f77bcf86cd799439011")]}},
    "order_items": {"order_id": {"$in": [ObjectId(...), ObjectId(...)]}}
}
```

## State Management

### Design Principles

1. **Parent-Child Relationship**: Runs contain multiple collection states
2. **ObjectId-based**: Uses MongoDB ObjectIds (not UUIDs)
3. **Native BSON Types**: Cursor values preserved as-is (datetime, ObjectId, Decimal128)
4. **Configurable Names**: Collection names configurable via YAML
5. **Fault Tolerance**: State updated strategically for performance vs resume granularity
6. **Index Optimization**: Compound index `(collection, endedAt)` for fast lookups

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

# 3. During replication (cursor tracked locally)
last_cursor_value = None
for batch in batches:
    process_batch(batch)
    last_cursor_value = batch[-1]["updated_at"]

# 4. On completion (single state update)
state_mgr.update_collection_state(
    state_id=state_id,
    last_cursor_value=last_cursor_value,
    documents_processed=total_docs
)
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
# Returns native BSON type: datetime(2024, 1, 15, 10, 30, 0)

# Build query with cursor filter
query = {}
if last_cursor is not None:
    query[cursor_field] = {"$gt": last_cursor}  # Direct BSON comparison

# Execute query
cursor = source_collection.find(query).sort(cursor_field, 1)
```

## Parallel Processing Model

### Collection-Level Parallelism

```python
with ThreadPoolExecutor(max_workers=max_parallel) as executor:
    futures = {}

    for collection_name, config in collection_configs.items():
        future = executor.submit(
            _replicate_single_collection,
            run_id,
            collection_name,
            config
        )
        futures[future] = collection_name

    for future in as_completed(futures):
        collection_name = futures[future]
        try:
            result = future.result()
            collection_results[collection_name] = result
        except Exception as e:
            # Failure isolation - other collections continue
            logger.error(f"Collection {collection_name} failed: {e}")
```

**Advantages**:
- Collections processed independently
- Failure of one collection doesn't affect others
- Better resource utilization
- Configurable parallelism (default: 5 workers)

**Thread Safety**:
- Each collection has its own replicator instance
- State updates are atomic at document level
- PyMongo connections are thread-safe (connection pooling)
- No shared mutable state between threads

### Batch Processing

```python
# Cursor tracking (local variable)
last_cursor_value = last_cursor  # From state
query = {cursor_field: {"$gt": last_cursor_value}} if last_cursor_value else {}

cursor = source.find(query).sort(cursor_field, 1).batch_size(batch_size)

for batch in fetch_batches(cursor, batch_size):
    # 1. Apply transformations
    processed = apply_transformations_and_exclusions(batch)

    # 2. Write to destination
    write_batch(processed)

    # 3. Track cursor locally (no state read)
    last_cursor_value = batch[-1][cursor_field]
    query = {cursor_field: {"$gt": last_cursor_value}}

# Single state update at end
state_mgr.update_collection_state(state_id, last_cursor_value, total_docs)
```

**Trade-offs**:
- **Smaller batches**: More frequent state updates → slower overall, better resume granularity
- **Larger batches**: Faster overall → less frequent state updates, more work lost on failure
- **Current strategy**: Update state once at end → optimal performance, resume from last successful run

## PII Detection & Anonymization

> **📖 Detailed Documentation**: For comprehensive implementation details, operator documentation, and configuration examples, see [Presidio Documentation](presidio.md).

### High-Level Architecture

```
Document → Flattening → Allowlist Filter → NLP Analysis → Strategy Resolution → Anonymization → Output
```

### Pipeline Stages

#### 1. Document Flattening

```python
{
    "user": {
        "email": "test@example.com",
        "profile": {
            "phone": "555-1234"
        }
    },
    "orders": [
        {"id": 1, "total": 100}
    ]
}

# Flattened to:
{
    "user.email": "test@example.com",
    "user.profile.phone": "555-1234",
    "orders.0.id": 1,
    "orders.0.total": 100
}
```

#### 2. Allowlist Filtering

```yaml
pii_analysis:
  allowlist:
    - "_id"
    - "metadata.*"
    - "*.created_at"
```

#### 3. NLP Analysis (Presidio)

```python
# For each string field
results = analyzer.analyze(
    text="test@example.com",
    entities=["EMAIL_ADDRESS", "PHONE_NUMBER", "PERSON"],
    language="en"
)
# [RecognizerResult(entity_type="EMAIL_ADDRESS", score=0.95, ...)]
```

#### 4. Strategy Resolution

```python
# Auto-detected: field → (entity_type, score)
# Manual: field → strategy_name

# Resolve to operator config
entity_type = "EMAIL_ADDRESS"
operator_config = presidio_config.get_operator_configs()[entity_type]
# OperatorConfig(operator_name="smart_redact", params={})
```

#### 5. Anonymization

```python
# Presidio AnonymizerEngine with custom operators
result = anonymizer_engine.anonymize(
    text="test@example.com",
    analyzer_results=[recognizer_result],
    operators={"EMAIL_ADDRESS": OperatorConfig("smart_redact", {})}
)
# result.text = "t***@example.com"
```

### Processing Pipeline Integration

```python
def _apply_transformations_and_exclusions(documents):
    """CRITICAL ORDER for data integrity."""

    # 1. Field transformations (regex replace)
    #    Example: billing_plan → "free"
    if field_transformer:
        documents = field_transformer.transform_documents(documents)

    # 2. PII redaction (operates on TRANSFORMED data!)
    #    Ensures PII detection sees final field values
    documents = pii_handler.process_documents(documents)

    # 3. Field exclusions (remove unwanted fields)
    #    Runs last to avoid PII detection on excluded fields
    if field_excluder:
        documents = field_excluder.exclude_fields_from_documents(documents)

    return documents
```

> **⚠️ Why Order Matters**:
> - PII detection must see transformed values (e.g., after regex replacement)
> - Field exclusions run last to avoid wasted PII processing
> - Changing this order can cause PII leaks or incorrect anonymization

## Data Flow

### Complete Replication Flow

```
1. CLI Command (init/scan/run)
   ↓
2. Load Configuration
   ├─ YAML config file
   ├─ Environment variables
   └─ CLI arguments (highest priority)
   ↓
3. Signal Handlers
   └─ Register SIGINT/SIGTERM handlers
   ↓
4. Create Connections
   ├─ Source MongoDB
   └─ Destination MongoDB
   ↓
5. Discovery
   ├─ Auto-discover collections
   ├─ Apply include/exclude patterns
   └─ Merge with configured collections
   ↓
6. Build Collection Configs
   ├─ Explicit configs
   ├─ Defaults for auto-discovered
   └─ Validate cursor fields
   ↓
7. Create Run (State Management)
   └─ Initialize run tracking
   ↓
8. Parallel Collection Processing
   ├─ ThreadPoolExecutor (max_workers=5)
   ├─ For each collection:
   │  ├─ Create collection state
   │  ├─ Get last cursor value
   │  ├─ Build query (cursor + match filters)
   │  ├─ Replicate indexes (before data)
   │  ├─ For each batch:
   │  │  ├─ Fetch from source
   │  │  ├─ Apply field transformations
   │  │  ├─ Apply PII redaction
   │  │  ├─ Exclude fields
   │  │  ├─ Write to destination
   │  │  └─ Track cursor locally
   │  ├─ Update state (once at end)
   │  └─ Complete collection state
   ↓
9. Complete Run
   └─ Aggregate statistics
   ↓
10. Report Results
    ├─ Console output
    └─ Progress callbacks
```

### Write Disposition Flows

#### Replace Mode
```
1. Drop destination collection (first batch)
2. Query source (no cursor filter)
   └─ Use skip/limit pagination
3. Fetch batches
4. Apply transformations & PII & exclusions
5. Insert to destination
6. Recreate indexes (after all data)
```

#### Append Mode
```
1. Get last cursor value from state
2. Build query with cursor filter
   └─ {cursor_field: {$gt: last_cursor}}
3. Fetch batches (sorted by cursor field)
4. Apply transformations & PII & exclusions
5. Insert to destination (fails on duplicates)
6. Update state (once at end)
```

#### Merge Mode
```
1. Get last cursor value from state
2. Build query with cursor filter
3. Fetch batches (sorted by cursor field)
4. Apply transformations & PII & exclusions
5. Upsert to destination (by primary_key)
   └─ ReplaceOne with upsert=True
6. Update state (once at end)
```

## Extension Points

### Custom Field Transformations

```python
# In YAML configuration
replication:
  collections:
    orders:
      field_transforms:
        - field: price
          type: regex_replace
          pattern: '\d+'
          replacement: '0'
```

### Custom PII Operators

```python
# Define custom operator
from presidio_anonymizer.operators import Operator, OperatorType

class CustomOperator(Operator):
    OPERATOR_NAME = "custom_mask"
    OPERATOR_TYPE = OperatorType.Anonymize

    def operate(self, text, params):
        # Custom anonymization logic
        return f"CUSTOM-{len(text)}"

# Register in anonymizer
anonymizer.anonymizer_engine.add_anonymizer(CustomOperator)
```

### Custom Presidio Recognizers

```yaml
# In presidio.yaml
custom_recognizers:
  - name: CustomPatternRecognizer
    supported_entity: CUSTOM_ENTITY
    patterns:
      - name: custom_pattern
        regex: "\\bCUST-\\d{8}\\b"
        score: 0.8
    context:
      - customer
      - client
```

### Progress Callbacks

```python
def progress_callback(collection_name, status, result):
    """Custom progress reporting."""
    print(f"{collection_name}: {status}")
    if result:
        print(f"  Docs: {result.documents_processed}")
        print(f"  Duration: {result.duration_seconds}s")

orchestrator.replicate(progress_callback=progress_callback)
```

## Performance Considerations

### Indexing Strategy

**Required Indexes**:
1. **Cursor fields** on source collections (for efficient incremental queries)
2. **Primary keys** on destination (for merge mode upserts)
3. **State collection indexes** (automatic):
   - `(collection, endedAt)` compound index

**Index Replication Timing**:
- **Replace mode**: Recreate indexes *after* data load (faster initial load)
- **Append/Merge modes**: Create indexes *before* data (better insert performance)

### Memory Management

**Design Features**:
- **Generator-based cursor iteration**: Never loads entire collection into memory
- **Batch processing**: Configurable batch sizes (default: 1000)
- **Explicit cursor cleanup**: Cursors closed after each collection
- **PyMongo cursor caching**: Minimal memory footprint

**Tuning**:
```yaml
replication:
  performance:
    batch_size: 1000  # Adjust based on document size
```

### Network Optimization

**Features**:
- **Batch inserts**: Reduces round trips to destination
- **Connection pooling**: PyMongo's built-in pooling
- **Compression support**: Configure in connection URI:
  ```
  mongodb://host:27017/?compressors=snappy,zlib
  ```

### Monitoring Metrics

**Tracked Metrics**:
- Documents per second
- Collections per minute
- State update frequency
- Error rates
- Memory usage

**Example**:
```python
result = orchestrator.replicate()

throughput = result.total_documents_processed / result.total_duration_seconds
print(f"Throughput: {throughput:.2f} docs/sec")
print(f"Collections: {result.total_collections_processed}")
print(f"Success rate: {result.total_documents_succeeded / result.total_documents_processed * 100:.1f}%")
```

## Error Handling

### Error Levels

1. **Document-level**: Skip invalid documents, continue batch (if `transform_error_mode: skip`)
2. **Batch-level**: Bulk write errors summarized (removes document data)
3. **Collection-level**: Mark collection as failed, continue with others
4. **Run-level**: Fail entire run only for critical errors

### Error Isolation

```python
# Collection-level isolation
for collection in collections:
    try:
        result = replicate_collection(collection)
        collection_results[collection] = result
    except Exception as e:
        # Collection fails independently
        logger.error(f"Collection {collection} failed: {e}")
        failed_collections.append(collection)
        # Other collections continue!

# Run completes with partial success
state_mgr.complete_run(run_id, errors=failed_collections)
```

### Error Aggregation

```python
# Run-level error summary
{
    "errors": {
        "summary": {
            "users": "Duplicate key error on _id",
            "orders": "Network timeout during batch insert"
        },
        "collections": ["users", "orders"]
    }
}
```

### Transform Error Modes

```yaml
replication:
  defaults:
    transform_error_mode: skip  # or "fail"
```

- **skip**: Log error, exclude document, continue
- **fail**: Stop replication on first error

## Testing Strategy

### Unit Tests

**Coverage Areas**:
- Individual component behavior
- Mock external dependencies (MongoDB, Presidio)
- Test edge cases and error conditions
- 29 tests for PII anonymization operators

**Example**:
```python
def test_smart_redact_email():
    """Test smart redaction of email addresses."""
    operator = SmartRedactOperator()
    result = operator.operate("user@example.com", {})
    assert result == "u***@example.com"
```

### Integration Tests

**Coverage Areas**:
- End-to-end replication flows
- Real MongoDB instances (Docker)
- State management verification
- Index replication
- Error handling

**Example Flow**:
```python
def test_incremental_replication():
    """Test cursor-based incremental loading."""
    # 1. Initial replication
    result1 = orchestrator.replicate()
    assert result1.total_documents_processed == 100

    # 2. Add new documents to source
    source.insert_many([{"_id": i, "updated_at": now()} for i in range(100, 110)])

    # 3. Incremental replication
    result2 = orchestrator.replicate()
    assert result2.total_documents_processed == 10  # Only new docs
```

---

## Summary

The MongoDB Replication Tool is a **production-grade system** with:

  ✅ **Robust Architecture**: Layered design with clear separation of concerns
  ✅ **Parallel Processing**: ThreadPoolExecutor for collection-level parallelism
  ✅ **Smart State Management**: Native BSON types, optimized updates
  ✅ **Advanced PII Detection**: Presidio integration with 10 custom operators
  ✅ **Fault Tolerance**: Collection-level failure isolation
  ✅ **Performance Optimizations**: Batch processing, index management, cursor tracking
  ✅ **Extensibility**: Custom operators, transformations, recognizers
  ✅ **Comprehensive Testing**: Unit + integration tests

The architecture follows **SOLID principles** and provides a **flexible, maintainable foundation** for MongoDB replication workflows with built-in PII anonymization capabilities.
