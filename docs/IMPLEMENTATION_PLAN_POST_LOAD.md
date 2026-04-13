# Post-Load Transformations Implementation Plan

## Overview

Implement ELT (Extract-Load-Transform) pattern for MongoDB Replication Tool with post-load transformations that execute after data is loaded into the destination database.

## Design Decisions

### 1. Idempotency via Cursor Scope ✅
- Transformations automatically scoped to replicated documents using cursor field range
- Prevents re-processing same documents on incremental runs
- Default behavior: `use_cursor_scope: true`

### 2. Transaction Support ✅
- Optional transaction support for atomic multi-step transformations
- Requires MongoDB 4.0+ with replica set or sharded cluster
- All-or-nothing execution with automatic rollback on failure

### 3. Conditional PII Anonymization ✅
- Support for conditional PII anonymization based on field patterns
- Example: Only anonymize emails NOT matching `@acme.com`
- Implemented via enhanced PII config

### 4. Custom Scripts ⏳
- Planned for future releases pending security review
- Security considerations: sandboxing, resource limits, code injection prevention

### 5. No Rollback Support ❌
- Transformations are not reversible (out of scope)
- Users must test with dry-run and staging environments
- Future consideration: snapshot/backup integration

### 6. No Batch Size Configuration ❌
- Use MongoDB's internal batching
- Performance tuning via indexes and query optimization

## Architecture

### Component Structure

```
src/mongo_replication/engine/post_load/
├── __init__.py
├── transformer.py              # PostLoadTransformer orchestrator
├── models.py                   # Pydantic config models
├── validators.py               # Config validation
├── operations.py               # Base executor classes
└── executors/
    ├── __init__.py
    ├── update_executor.py      # update_fields
    ├── delete_executor.py      # delete_documents
    ├── field_executor.py       # remove_fields
    ├── sanitize_executor.py    # sanitize_fields
    └── aggregation_executor.py # aggregation_pipeline
```

### Integration Points

1. **CollectionReplicator** (`src/mongo_replication/engine/replicator.py`)
   - Add `post_load_transformer` parameter
   - Call transformations after successful replication
   - Update state only if transformations succeed

2. **StateManager** (`src/mongo_replication/engine/state.py`)
   - Add transformation tracking to collection state
   - Track cursor range for each run
   - Store transformation results

3. **Configuration** (`src/mongo_replication/config/models.py`)
   - Add `PostLoadTransformationsConfig` models
   - Add deprecation warnings for `field_transforms` and `fields_exclude`
   - Enhance `PIIConfig` for conditional anonymization

4. **Orchestrator** (`src/mongo_replication/engine/orchestrator.py`)
   - Create PostLoadTransformer instance
   - Pass cursor range to transformer

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)

**Goal:** Basic framework with update_fields and delete_documents

#### Tasks

1. **Create Module Structure**
   - [ ] Create `src/mongo_replication/engine/post_load/` directory
   - [ ] Create `__init__.py`, `transformer.py`, `models.py`, `operations.py`
   - [ ] Create `executors/` subdirectory

2. **Implement Config Models** (`models.py`)
   - [ ] `TransformationType` enum
   - [ ] `SanitizationType` enum
   - [ ] `BaseTransformationStep` base class
   - [ ] `UpdateFieldsStep` model
   - [ ] `DeleteDocumentsStep` model
   - [ ] `RemoveFieldsStep` model
   - [ ] `SanitizeFieldsStep` model
   - [ ] `AggregationPipelineStep` model
   - [ ] `PostLoadTransformationsConfig` model
   - [ ] `TransformationResult` model
   - [ ] `TransactionOptions` model

3. **Implement Base Classes** (`operations.py`)
   - [ ] `BaseExecutor` abstract class
   - [ ] Helper methods for cursor scoping
   - [ ] Error handling utilities

4. **Implement Basic Executors**
   - [ ] `UpdateExecutor` (`executors/update_executor.py`)
   - [ ] `DeleteExecutor` (`executors/delete_executor.py`)
   - [ ] Tests for both executors

5. **Implement Transformer Orchestrator** (`transformer.py`)
   - [ ] `PostLoadTransformer` class
   - [ ] Executor initialization
   - [ ] Sequential execution
   - [ ] Cursor scope logic
   - [ ] Error handling and logging
   - [ ] Result aggregation

6. **Update Configuration Models**
   - [ ] Add `post_load_transformations` to `CollectionConfig`
   - [ ] Add deprecation warnings for `field_transforms`
   - [ ] Add deprecation warnings for `fields_exclude`

**Deliverables:**
- Working update_fields and delete_documents transformations
- Configuration models and validation
- Unit tests for core components

### Phase 2: Advanced Executors (Week 2)

**Goal:** Implement remaining transformation types

#### Tasks

1. **Field Removal Executor** (`executors/field_executor.py`)
   - [ ] Implement `FieldRemovalExecutor`
   - [ ] Support dot notation for nested fields
   - [ ] Cursor scope integration
   - [ ] Tests

2. **Sanitize Executor** (`executors/sanitize_executor.py`)
   - [ ] Implement `SanitizeExecutor`
   - [ ] Sanitization type implementations:
     - [ ] `lowercase_trim`
     - [ ] `uppercase_trim`
     - [ ] `trim`
     - [ ] `normalize_email`
     - [ ] `remove_special_chars`
   - [ ] Aggregation pipeline builder
   - [ ] Tests

3. **Aggregation Pipeline Executor** (`executors/aggregation_executor.py`)
   - [ ] Implement `AggregationExecutor`
   - [ ] Cursor scope injection
   - [ ] Support for `$merge` operator
   - [ ] Error handling for pipeline errors
   - [ ] Tests

4. **Transaction Support** (`transformer.py`)
   - [ ] Add transaction session management
   - [ ] Wrap transformations in transaction
   - [ ] Rollback on failure
   - [ ] MongoDB version check
   - [ ] Tests with mock transactions

5. **Validation** (`validators.py`)
   - [ ] Validate MongoDB query syntax
   - [ ] Validate update operators
   - [ ] Validate aggregation pipelines
   - [ ] Check for unsupported operations
   - [ ] Tests

**Deliverables:**
- All transformation types working
- Transaction support
- Comprehensive validation

### Phase 3: Integration (Week 3)

**Goal:** Integrate with existing replication engine

#### Tasks

1. **State Management Updates** (`state.py`)
   - [ ] Add transformation tracking to collection state schema
   - [ ] Store cursor range for each run
   - [ ] Store transformation results
   - [ ] Add `get_cursor_range()` method
   - [ ] Tests

2. **CollectionReplicator Integration** (`replicator.py`)
   - [ ] Add `post_load_transformer` parameter
   - [ ] Add `_run_post_load_transformations()` method
   - [ ] Call transformations after successful replication
   - [ ] Update state only if transformations succeed
   - [ ] Error handling
   - [ ] Add transformation results to `ReplicationResult`
   - [ ] Tests

3. **Orchestrator Integration** (`orchestrator.py`)
   - [ ] Create `PostLoadTransformer` instance
   - [ ] Pass cursor range to transformer
   - [ ] Log deprecation warnings for old configs
   - [ ] Tests

4. **Dry-Run Support** (`run.py`)
   - [ ] Preview transformation steps
   - [ ] Estimate affected documents
   - [ ] Display transformation plan
   - [ ] Tests

5. **Conditional PII Anonymization** (`pii/presidio_anonymizer.py`)
   - [ ] Add `conditions` support to PII config
   - [ ] Implement condition evaluation
   - [ ] Update PII processing logic
   - [ ] Tests
   - [ ] Update `docs/presidio.md`

**Deliverables:**
- Full integration with replication engine
- Working end-to-end flow
- Dry-run support
- Conditional PII anonymization

### Phase 4: Testing & Documentation (Week 4)

**Goal:** Comprehensive testing and documentation

#### Tasks

1. **Unit Tests**
   - [ ] Test all executors independently
   - [ ] Test transformer orchestration
   - [ ] Test config models and validation
   - [ ] Test cursor scoping logic
   - [ ] Test transaction handling
   - [ ] Test error scenarios
   - [ ] Achieve 90%+ code coverage

2. **Integration Tests**
   - [ ] Test full replication + transformation flow
   - [ ] Test state management with transformations
   - [ ] Test incremental replication idempotency
   - [ ] Test transaction rollback scenarios
   - [ ] Test all transformation types together
   - [ ] Test dry-run mode

3. **End-to-End Tests**
   - [ ] Real MongoDB instances
   - [ ] Complex transformation scenarios
   - [ ] Performance benchmarks
   - [ ] Large dataset tests

4. **Documentation**
   - [x] `docs/post-load-transformations.md` - Complete guide
   - [ ] `docs/migration-to-elt.md` - Migration guide
   - [ ] Update `docs/technical-design.md` - Add ELT architecture
   - [ ] Update `docs/configuration.md` - Add post_load_transformations
   - [ ] Update `README.md` - Highlight ELT pattern
   - [ ] Create example configs in `examples/`

5. **Examples**
   - [ ] `examples/staging_environment.yaml` - Prod to staging
   - [ ] `examples/data_cleanup.yaml` - Sanitization
   - [ ] `examples/pii_conditional.yaml` - Conditional PII
   - [ ] `examples/complex_aggregations.yaml` - Aggregations
   - [ ] `examples/transactional.yaml` - Transaction example

**Deliverables:**
- Comprehensive test suite
- Complete documentation
- Example configurations
- Migration guide

## Configuration Schema

### Enhanced CollectionConfig

```python
class CollectionConfig(BaseModel):
    name: str
    write_disposition: str
    cursor_fields: List[str]

    # PII - runs DURING replication
    pii: Optional[PIIConfig] = None

    # DEPRECATED - use post_load_transformations
    field_transforms: List[FieldTransformConfig] = Field(
        default_factory=list,
        deprecated="Use post_load_transformations instead"
    )
    fields_exclude: List[str] = Field(
        default_factory=list,
        deprecated="Use post_load_transformations with remove_fields"
    )

    # NEW - post-load transformations
    post_load_transformations: Optional[PostLoadTransformationsConfig] = None
```

### PostLoadTransformationsConfig

```python
class PostLoadTransformationsConfig(BaseModel):
    use_cursor_scope: bool = True
    execution_order: str = "sequential"  # or 'parallel'
    transactional: bool = False
    transaction_options: Optional[TransactionOptions] = None
    steps: List[TransformationStep] = Field(default_factory=list)
```

### Conditional PII Config

```python
class PIICondition(BaseModel):
    field: str
    operator: str  # 'regex', 'not_regex', 'eq', 'ne', 'in', 'nin'
    value: Any

class PIIFieldConfig(BaseModel):
    strategy: str
    conditions: Optional[List[PIICondition]] = None

class PIIConfig(BaseModel):
    enabled: bool = True
    fields: Dict[str, Union[str, PIIFieldConfig]]
```

## State Schema Enhancement

```python
{
    "_id": ObjectId,
    "runId": ObjectId,
    "collection": str,
    "status": "completed",

    # Existing fields
    "lastCursorValue": Any,
    "lastCursorField": str,

    # NEW: Cursor range for current run
    "currentRun": {
        "startCursor": datetime(2026, 04, 01),
        "endCursor": datetime(2026, 04, 08),
        "documentsReplicated": 1500
    },

    # NEW: Transformation tracking
    "transformations": {
        "enabled": true,
        "totalSteps": 5,
        "completedSteps": 5,
        "failedSteps": 0,
        "lastExecution": datetime,
        "results": [
            {
                "stepIndex": 0,
                "stepType": "update_fields",
                "status": "success",
                "documentsAffected": 1500,
                "durationSeconds": 2.5
            }
        ]
    }
}
```

## Testing Strategy

### Unit Tests

```python
# tests/unit/post_load/test_update_executor.py
def test_update_fields_with_cursor_scope():
    """Test update_fields applies cursor scope correctly."""

def test_update_fields_without_cursor_scope():
    """Test update_fields can override cursor scope."""

def test_update_fields_error_handling():
    """Test error handling for invalid updates."""
```

### Integration Tests

```python
# tests/integration/test_post_load_transformations.py
def test_full_replication_with_transformations():
    """Test complete flow: replicate + transform."""

def test_incremental_replication_idempotency():
    """Test transformations only apply to new data."""

def test_transaction_rollback():
    """Test transaction rollback on step failure."""
```

### Performance Tests

```python
# tests/performance/test_transformation_performance.py
def test_large_collection_transformations():
    """Benchmark transformations on 1M+ documents."""

def test_parallel_vs_sequential_execution():
    """Compare parallel vs sequential performance."""
```

## Migration Strategy

### Phase 1: Deprecation Warnings (v0.2.0)
- Add deprecation warnings to logs
- Update documentation with migration guide
- Keep existing functionality working

### Phase 2: Dual Support (v0.3.0 - v1.0)
- Support both old and new configs
- Encourage migration via warnings
- Provide auto-migration tool

### Phase 3: Removal (v2.0.0)
- Remove `field_transforms`
- Remove `fields_exclude`
- Keep PII anonymization as pre-load only

## Security Considerations

### Current Scope
- All transformation configs validated
- MongoDB query injection prevented via PyMongo
- No code execution (custom scripts deferred)

### Future: Custom Scripts
1. **Sandboxing**
   - RestrictedPython or similar
   - Resource limits (CPU, memory, time)
   - No network access
   - No file system access

2. **Code Review**
   - Store scripts in version control
   - Review before execution
   - Audit trail

3. **Execution Environment**
   - Isolated process
   - Limited permissions
   - Monitored execution

## Performance Optimization

### Indexing Strategy
- Auto-detect needed indexes
- Suggest index creation in dry-run
- Log slow transformations

### Query Optimization
- Use covered queries where possible
- Batch operations efficiently
- Monitor query performance

### Parallel Execution
- Safe parallel execution for independent steps
- Automatic dependency detection
- Resource pooling

## Error Handling

### Transformation Failure Modes

1. **Sequential Mode** (default)
   - Stop on first failure
   - Log error details
   - Mark collection as failed
   - Don't update state

2. **Parallel Mode**
   - All steps execute
   - Log all failures
   - Mark collection as failed if any fail
   - Don't update state

3. **Transactional Mode**
   - Rollback all changes on any failure
   - Log transaction abort
   - Mark collection as failed
   - Don't update state

### Recovery Strategy
- Failed transformations don't update cursor
- Next run will retry transformations
- Manual intervention may be needed

## Open Questions

1. **Dry-Run Accuracy**: How to accurately estimate affected documents for complex aggregations?
   - **Answer**: Use `$count` stage in pipeline preview

2. **Transaction Timeout**: What's a reasonable default for `max_commit_time_ms`?
   - **Answer**: 30000ms (30 seconds), configurable

3. **Parallel Execution Safety**: How to detect step dependencies automatically?
   - **Answer**: User must configure manually (parallel vs sequential)

4. **Index Recommendations**: Should we auto-create missing indexes?
   - **Answer**: No, only suggest in dry-run mode

5. **State Cleanup**: Should we cleanup transformation state from old runs?
   - **Answer**: Keep last 10 runs, configurable

## Success Criteria

- [ ] All 6 transformation types working
- [ ] Cursor-scoped idempotency working
- [ ] Transaction support working
- [ ] Conditional PII anonymization working
- [ ] 90%+ test coverage
- [ ] Complete documentation
- [ ] Migration guide
- [ ] Example configurations
- [ ] Performance benchmarks
- [ ] No breaking changes to existing configs

## Timeline

- **Week 1**: Core infrastructure + basic executors
- **Week 2**: Advanced executors + transactions
- **Week 3**: Integration + conditional PII
- **Week 4**: Testing + documentation

**Total: 4 weeks**

## Next Steps

1. Review and approve this plan
2. Set up project tracking (GitHub issues/project board)
3. Begin Phase 1 implementation
4. Regular check-ins and iterations
