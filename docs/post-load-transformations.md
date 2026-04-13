# Post-Load Transformations (ELT Pattern)

## Overview

The MongoDB Replication Tool supports **post-load transformations** that execute after data is loaded into the destination database. This ELT (Extract-Load-Transform) pattern provides more powerful and flexible data transformation capabilities compared to the traditional ETL approach.

## Why ELT over ETL?

### Benefits of Post-Load Transformations

1. **More Powerful**: Access to full MongoDB query and aggregation capabilities
2. **Better Performance**: Transformations run directly in the database
3. **Easier Debugging**: Inspect loaded data before transformations
4. **Flexible**: Add/modify transformations without re-replicating data
5. **Safer**: PII anonymization still happens during load (no data leakage)

### When to Use What

| Feature | Timing | Use Case |
|---------|--------|----------|
| **PII Anonymization** | During replication (pre-load) | Prevent PII data leakage |
| **Field Updates** | After load (post-load) | Change values for staging/testing |
| **Document Deletion** | After load (post-load) | Remove test/unwanted data |
| **Field Removal** | After load (post-load) | Clean up unnecessary fields |
| **Data Sanitization** | After load (post-load) | Normalize/clean data |
| **Complex Transformations** | After load (post-load) | Business logic, aggregations |

## Configuration

### Basic Structure

```yaml
replication:
  collections:
    users:
      # Replication settings
      write_disposition: merge
      cursor_fields: [updated_at]

      # PII anonymization (runs DURING replication)
      pii:
        enabled: true
        fields:
          email: mask
          ssn: redact

      # Post-load transformations (runs AFTER replication)
      post_load_transformations:
        # Apply transformations only to newly replicated documents
        use_cursor_scope: true  # default: true

        # Transaction support (requires MongoDB 4.0+ replica set)
        transactional: false  # default: false

        # Execution order
        execution_order: sequential  # or 'parallel'

        # Transformation steps
        steps:
          - type: update_fields
            description: "Update field values"
            # ... step config ...
```

### Cursor-Scoped Transformations (Idempotency)

By default, transformations only apply to documents replicated in the current run:

```yaml
post_load_transformations:
  use_cursor_scope: true  # Automatically adds cursor range to filters

  steps:
    - type: update_fields
      filter:
        status: "active"
      # Actual filter executed:
      # {
      #   status: "active",
      #   updated_at: { $gt: last_cursor, $lte: current_cursor }
      # }
      updates:
        $set:
          environment: "staging"
```

**How it works:**
- Uses the collection's `cursor_field` (e.g., `updated_at`)
- Automatically adds range filter: `{ cursor_field: { $gt: last_cursor, $lte: current_cursor } }`
- Ensures transformations are idempotent (re-running same range is safe)

**Override cursor scope for specific steps:**

```yaml
steps:
  - type: update_fields
    use_cursor_scope: false  # Apply to ALL documents
    filter: {}
    updates:
      $set:
        last_sync: "$$NOW"
```

### Transactional Transformations

Enable transactions to ensure all-or-nothing execution:

```yaml
post_load_transformations:
  transactional: true
  transaction_options:
    read_concern: "snapshot"
    write_concern: "majority"
    max_commit_time_ms: 30000

  steps:
    - type: update_fields
      filter: { status: "pending" }
      updates:
        $set: { status: "test" }

    - type: delete_documents
      filter: { type: "temporary" }
```

**Requirements:**
- MongoDB 4.0 or higher
- Replica set or sharded cluster
- All operations must be on same database

**Behavior:**
- If any step fails, all changes are rolled back
- Provides ACID guarantees across all transformation steps

## Transformation Types

### 1. Update Fields

Update field values using MongoDB update operators.

```yaml
- type: update_fields
  description: "Set all users to free plan"
  filter:
    billing_plan: { $ne: "enterprise" }
  updates:
    $set:
      billing_plan: "free"
      updated_by: "replication_tool"
    $unset:
      payment_method: ""
    $inc:
      migration_count: 1
  options:
    multi: true  # updateMany (default) vs updateOne
```

**Supported Update Operators:**
- `$set`, `$unset`, `$inc`, `$mul`, `$rename`, `$min`, `$max`
- `$currentDate`, `$setOnInsert`
- Array operators: `$push`, `$pull`, `$addToSet`, `$pop`

### 2. Delete Documents

Remove documents matching criteria.

```yaml
- type: delete_documents
  description: "Remove test users"
  filter:
    $or:
      - email: { $regex: "@test\\.example\\.com$" }
      - username: { $regex: "^test_" }
      - created_at: { $lt: "2020-01-01T00:00:00Z" }
  options:
    multi: true  # deleteMany (default) vs deleteOne
```

### 3. Remove Fields

Remove specific fields from documents (replaces deprecated `fields_exclude`).

```yaml
- type: remove_fields
  description: "Remove sensitive fields"
  fields:
    - password_hash
    - api_keys
    - internal_notes
    - metadata.audit_log  # Supports dot notation
```

**Implementation:**
```javascript
// Executed as:
db.collection.updateMany(
  { cursor_field: { $gt: last_cursor, $lte: current_cursor } },
  { $unset: { password_hash: "", api_keys: "", ... } }
)
```

### 4. Sanitize Fields

Clean and normalize field values.

```yaml
- type: sanitize_fields
  description: "Normalize user data"
  operations:
    - field: email
      sanitization: lowercase_trim

    - field: username
      sanitization: trim

    - field: phone
      sanitization: remove_special_chars
```

**Supported Sanitization Types:**

| Type | Description | Example |
|------|-------------|---------|
| `lowercase_trim` | Convert to lowercase and trim whitespace | `" User@Example.COM "` → `"user@example.com"` |
| `uppercase_trim` | Convert to uppercase and trim whitespace | `" hello "` → `"HELLO"` |
| `trim` | Remove leading/trailing whitespace | `"  hello  "` → `"hello"` |
| `normalize_email` | Lowercase, trim, validate format | `" User@Example.COM "` → `"user@example.com"` |
| `remove_special_chars` | Keep only alphanumeric | `"(555) 123-4567"` → `"5551234567"` |

### 5. Aggregation Pipeline

Run MongoDB aggregation pipelines for complex transformations.

```yaml
- type: aggregation_pipeline
  description: "Anonymize IP addresses"
  pipeline:
    - $match:
        ip_address: { $exists: true }

    - $set:
        anonymized_ip:
          $concat:
            - $arrayElemAt:
                - $split: ["$ip_address", "."]
                - 0
            - ".xxx.xxx.xxx"
        ip_country: "$$REMOVE"  # Remove field

    - $unset: "ip_address"

    - $merge:
        into: "users"  # Collection name
        whenMatched: "merge"  # merge, replace, keepExisting, fail
        whenNotMatched: "discard"
```

**Use Cases:**
- Complex field calculations
- Data denormalization
- Computed fields
- Conditional updates

### 6. Custom Scripts (Future)

**Note:** Custom script execution is planned for future releases pending security review.

```yaml
- type: custom_script
  description: "Complex business logic"
  language: python
  script_path: "scripts/transform_users.py"
  args:
    min_age: 18
    environment: "staging"
```

## Advanced Patterns

### Conditional Transformations

Use MongoDB query operators for conditional logic:

```yaml
steps:
  # Only update documents created this year
  - type: update_fields
    filter:
      created_at:
        $gte: "2026-01-01T00:00:00Z"
    updates:
      $set:
        status: "current"

  # Different updates for different plans
  - type: update_fields
    filter:
      billing_plan: "enterprise"
    updates:
      $set:
        data_retention_days: 365

  - type: update_fields
    filter:
      billing_plan: { $in: ["free", "basic"] }
    updates:
      $set:
        data_retention_days: 30
```

### Multi-Step Workflow

Chain transformations for complex workflows:

```yaml
steps:
  # Step 1: Normalize emails
  - type: sanitize_fields
    operations:
      - field: email
        sanitization: lowercase_trim

  # Step 2: Mark duplicates
  - type: aggregation_pipeline
    pipeline:
      - $group:
          _id: "$email"
          count: { $sum: 1 }
          ids: { $push: "$_id" }
      - $match:
          count: { $gt: 1 }
      - $unwind: "$ids"
      - $lookup:
          from: "users"
          localField: "ids"
          foreignField: "_id"
          as: "user"
      - $replaceRoot:
          newRoot: { $arrayElemAt: ["$user", 0] }
      - $set:
          is_duplicate: true
      - $merge:
          into: "users"
          whenMatched: "merge"

  # Step 3: Delete duplicates (keep first)
  - type: delete_documents
    filter:
      is_duplicate: true
```

### Environment-Specific Transformations

Different transformations for different environments:

```yaml
# staging_config.yaml
replication:
  collections:
    orders:
      post_load_transformations:
        steps:
          - type: update_fields
            description: "Zero out prices for staging"
            filter: {}
            updates:
              $set:
                price: 0
                total: 0

# production_replica_config.yaml
replication:
  collections:
    orders:
      post_load_transformations:
        steps:
          - type: remove_fields
            description: "Remove internal metadata only"
            fields:
              - internal_notes
```

## Migration from Deprecated Features

### Migrating `fields_exclude`

**Old (Deprecated):**
```yaml
collections:
  users:
    fields_exclude:
      - password_hash
      - api_keys
```

**New (Recommended):**
```yaml
collections:
  users:
    post_load_transformations:
      steps:
        - type: remove_fields
          fields:
            - password_hash
            - api_keys
```

### Migrating `field_transforms`

**Old (Deprecated):**
```yaml
collections:
  orders:
    field_transforms:
      - field: billing_plan
        type: regex_replace
        pattern: '.*'
        replacement: 'free'
```

**New (Recommended):**
```yaml
collections:
  orders:
    post_load_transformations:
      steps:
        - type: update_fields
          filter: {}
          updates:
            $set:
              billing_plan: "free"
```

## Performance Considerations

### Indexing

Ensure proper indexes for transformation filters:

```javascript
// For cursor-scoped transformations
db.users.createIndex({ updated_at: 1 })

// For filter-based transformations
db.users.createIndex({ status: 1 })
db.users.createIndex({ email: 1 })
```

### Batch Size

For large collections, transformations use MongoDB's internal batching. Monitor performance:

```bash
# Check transformation duration in logs
[users] Post-load transformations complete: 5/5 successful in 12.5s
[users]   Step 0 (update_fields): 15000 documents in 8.2s
[users]   Step 1 (delete_documents): 250 documents in 0.3s
```

### Parallel Execution

For independent transformations, use parallel mode:

```yaml
post_load_transformations:
  execution_order: parallel  # Run all steps concurrently

  steps:
    - type: update_fields  # Independent
      filter: { type: "A" }
      updates: { $set: { status: "test" } }

    - type: update_fields  # Independent
      filter: { type: "B" }
      updates: { $set: { status: "test" } }
```

**Caution:** Only use parallel mode when steps don't depend on each other.

## Error Handling

### Sequential Mode (Default)

Stops on first failure:

```yaml
execution_order: sequential

steps:
  - type: update_fields  # Executes
    # ...

  - type: delete_documents  # If this fails, stops here
    # ...

  - type: remove_fields  # NOT executed if previous step failed
    # ...
```

### Parallel Mode

All steps execute independently; failures logged but don't stop others:

```yaml
execution_order: parallel  # All steps run regardless of failures
```

### Transactional Mode

All-or-nothing execution:

```yaml
transactional: true  # If ANY step fails, ALL changes rolled back
```

## Testing

### Dry Run

Preview transformations without executing:

```bash
mongorep run my_job --dry-run
```

**Output:**
```
[users] Would execute 3 post-load transformation(s):
  Step 0: update_fields - Set all users to free plan
    Filter: {}
    Updates: { $set: { billing_plan: "free" } }
    Estimated documents: 15000

  Step 1: delete_documents - Remove test users
    Filter: { email: { $regex: "@test\\.example\\.com$" } }
    Estimated documents: 25

  Step 2: remove_fields - Remove sensitive fields
    Fields: [password_hash, api_keys]
    Estimated documents: 15000
```

### Validation

Validate configuration before running:

```bash
mongorep validate my_job
```

Checks:
- Transformation config syntax
- MongoDB query validity
- Index availability
- Transaction requirements

## Examples

See [examples/](../examples/) directory for complete examples:
- `staging_environment.yaml` - Production to staging transformations
- `data_cleanup.yaml` - Data sanitization and cleanup
- `pii_conditional.yaml` - Conditional PII anonymization
- `complex_aggregations.yaml` - Advanced aggregation pipelines

## Troubleshooting

### Transformations Not Applied

1. Check `use_cursor_scope` setting
2. Verify cursor field has proper index
3. Check filter matches documents
4. Review logs for errors

### Performance Issues

1. Add indexes for filter fields
2. Use `execution_order: parallel` for independent steps
3. Consider breaking large transformations into smaller steps
4. Monitor MongoDB slow query log

### Transaction Failures

1. Ensure MongoDB 4.0+ with replica set
2. Check transaction timeout settings
3. Reduce number of documents per transaction
4. Verify write concern compatibility

## Future Enhancements

Planned features for future releases:
- Custom script execution (Python/JavaScript)
- Transformation templates
- Pre-defined sanitization libraries
- Transformation rollback support
- Enhanced dry-run with sampling
