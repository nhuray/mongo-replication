# Configuration Reference

Complete reference for configuring the MongoDB Replication Tool. This document describes all configuration options available in the YAML configuration files.

## Table of Contents

- [Configuration File Structure](#configuration-file-structure)
- [Scan Configuration](#scan-configuration)
  - [Discovery Settings](#discovery-settings)
  - [PII Detection Settings](#pii-detection-settings)
- [Replication Configuration](#replication-configuration)
  - [Default Settings](#default-settings)
  - [Collection-Specific Configuration](#collection-specific-configuration)
  - [Schema (Relationships)](#schema-relationships)
- [Complete Examples](#complete-examples)

## Configuration File Structure

Configuration files use YAML format with two main root sections:

```yaml
scan:
  # Settings for scanning and analyzing collections
  discovery: {...}
  pii: {...}

replication:
  # Settings for replicating collections
  defaults: {...}
  collections: {...}
  schema: [...]
```

## Scan Configuration

The `scan` section controls how the tool discovers collections and detects PII during the scan phase.

### Discovery Settings

Controls which collections are included in the scan operation.

```yaml
scan:
  discovery:
    include_patterns: []
    exclude_patterns: []
```

#### Options

**`include_patterns`** (list of strings, default: `[]`)
- List of regex patterns for collections to include in scan
- Empty list means scan all collections
- Example: `["^user_", "^customer_"]`
- Only collections matching at least one pattern will be scanned

**`exclude_patterns`** (list of strings, default: `[]`)
- List of regex patterns for collections to exclude from scan
- Takes precedence over `include_patterns`
- Example: `["^tmp_", "^cache_", "_audit$"]`
- Applied after include patterns

### PII Detection Settings

Configuration for automatic PII detection using Microsoft Presidio.

```yaml
scan:
  pii:
    enabled: true
    confidence_threshold: 0.85
    entity_types: [...]
    sample_size: 1000
    sample_strategy: "stratified"
    default_strategies: {...}
    allowlist: [...]
```

#### Options

**`enabled`** (boolean, default: `true`)
- Whether to run PII detection during scan
- When disabled, no PII analysis will be performed
- Useful for performance when PII detection is not needed

**`confidence_threshold`** (float, default: `0.85`)
- Minimum confidence score for PII detection (range: 0.0-1.0)
- Higher values = fewer false positives but might miss some PII
- Lower values = more detections but more false positives
- Recommended range: 0.75-0.95

**`entity_types`** (list of strings, default: see below)
- List of PII entity types to detect
- Uses Microsoft Presidio's entity recognition
- Default types:
  - `EMAIL_ADDRESS`: Email addresses
  - `PHONE_NUMBER`: Phone numbers (various formats)
  - `PERSON`: Person names
  - `US_SSN`: US Social Security Numbers
  - `CREDIT_CARD`: Credit card numbers
  - `IBAN_CODE`: International Bank Account Numbers
  - `IP_ADDRESS`: IPv4 and IPv6 addresses
  - `URL`: Web URLs

**`sample_size`** (integer, default: `1000`)
- Number of documents to sample per collection for PII detection
- Larger samples = more accurate detection but slower
- Smaller samples = faster but might miss PII in rare fields
- Recommended range: 500-5000

**`sample_strategy`** (string, default: `"stratified"`)
- Sampling strategy for PII detection
- Options:
  - `"random"`: Random sampling (faster, simpler)
  - `"stratified"`: Ensures diverse sample across collection (recommended)

**`default_strategies`** (object, default: see below)
- Default anonymization strategy per PII entity type
- Applied when PII is detected but no manual strategy is specified
- Available strategies:
  - `"fake"`: Generate realistic fake data using Mimesis library
    - Best for: email, phone, name, address
    - Pros: Realistic, maintains data utility
    - Cons: Not deterministic (same input → different output)
  - `"redact"`: Smart format-preserving redaction
    - Best for: SSN, credit cards, structured PII
    - Pros: Shows partial data, maintains format
    - Example: `john.doe@corp.com` → `jo30f6oe@corp.com` (preserves domain)
  - `"hash"`: SHA-256 hashing
    - Best for: IDs, usernames, values needing referential integrity
    - Pros: Deterministic (same input → same output), irreversible
    - Cons: Doesn't maintain data utility
  - `"mask"`: Replace with asterisks
    - Best for: Completely obscuring values
    - Example: `"sensitive"` → `"********"`
  - `"null"`: Replace with null/None
    - Best for: Removing PII completely
    - Use when field is not needed in destination

Default strategy mapping:
```yaml
EMAIL_ADDRESS: "fake"    # Replace with realistic fake email
PHONE_NUMBER: "fake"     # Replace with realistic fake phone
PERSON: "hash"           # Hash the name (preserves uniqueness)
US_SSN: "redact"         # Partial redaction (e.g., ***-**-6789)
CREDIT_CARD: "redact"    # Partial redaction
IBAN_CODE: "redact"      # Partial redaction
IP_ADDRESS: "hash"       # Hash the IP address
URL: "hash"              # Hash the URL
```

**`allowlist`** (list of strings, default: `["_id", "meta.*", "*.id"]`)
- Field patterns to exclude from PII detection
- Supports wildcard patterns using `*`
- Default patterns:
  - `"_id"`: MongoDB document identifier (never PII)
  - `"meta.*"`: All fields starting with "meta." (metadata fields)
  - `"*.id"`: Any field ending in ".id" (typically foreign keys)
- Examples:
  - `"metadata.*"`: Exclude all fields starting with "metadata."
  - `"*.created_at"`: Exclude all "created_at" fields
  - `"*.updated_at"`: Exclude all "updated_at" fields
  - `"internal_*"`: Exclude all fields starting with "internal_"

## Replication Configuration

The `replication` section controls how collections are replicated from source to destination.

### Default Settings

Global settings that apply to all collections unless overridden in collection-specific configuration.

```yaml
replication:
  defaults:
    replicate_all: true
    include_patterns: []
    exclude_patterns: []
    write_disposition: "merge"
    cursor_fields: [...]
    fallback_cursor: "_id"
    initial_value: "2020-01-01T00:00:00Z"
    max_parallel_collections: 5
    batch_size: 1000
    transform_error_mode: "skip"
    state: {...}
```

#### Collection Discovery & Filtering

**`replicate_all`** (boolean, default: `true`)
- If `true`: Auto-discover and replicate all collections not explicitly excluded
- If `false`: Only replicate collections matching `include_patterns` or explicitly configured in `collections` section
- Use `false` for more control over which collections are replicated

**`include_patterns`** (list of strings, default: `[]`)
- List of regex patterns for collections to include in replication
- Only used when `replicate_all: false`
- Empty list with `replicate_all: false` means no auto-discovery
- Example: `["^user_", "^order_"]`

**`exclude_patterns`** (list of strings, default: `[]`)
- List of regex patterns for collections to exclude from replication
- Takes precedence over `include_patterns`
- Applied after include patterns
- Example: `["^tmp_", "^test_", "_backup$"]`
- Useful for excluding temporary or system collections

#### Write Strategies

**`write_disposition`** (string, default: `"merge"`)
- Default write strategy for collections
- Options:
  - `"merge"`: Upsert based on primary_key (incremental updates)
    - Updates existing documents, inserts new ones
    - Requires `primary_key` to be set (typically `_id`)
    - Best for: Incremental synchronization
  - `"append"`: Insert new documents only
    - Fails on duplicate primary keys
    - Best for: Append-only logs, events
  - `"replace"`: Drop and recreate collection (full refresh)
    - Deletes all existing data before inserting
    - Best for: Dimension tables, small reference data

#### Cursor & Incremental Loading

**`cursor_fields`** (list of strings, default: see below)
- List of field names to try as cursor fields for incremental loading
- The scan command auto-detects if any of these fields exist in collections
- Checked in priority order (first match wins)
- Default priority order:
  ```yaml
  - "updated_at"
  - "updatedAt"
  - "meta.updated_at"
  - "meta.updatedAt"
  ```
- Requirements for cursor fields:
  - Must be indexed for performance
  - Should be monotonically increasing
  - Should represent the last modification time

**`fallback_cursor`** (string, default: `"_id"`)
- Field to use for incremental loading when no `cursor_fields` match
- Used when collection doesn't have timestamp fields
- Default `_id` works for most cases (uses ObjectId timestamp)
- Alternative: Any indexed field with ascending values

**`initial_value`** (string, default: `"2020-01-01T00:00:00Z"`)
- Initial cursor value for first-time replication
- Used when no previous replication state exists
- Format: ISO 8601 datetime string
- Determines starting point for incremental replication

#### Performance Settings

**`max_parallel_collections`** (integer, default: `5`)
- Maximum number of collections to replicate concurrently
- Higher values = faster overall but more resource intensive
- Considerations:
  - Network bandwidth
  - Source database load
  - Destination database load
  - Available memory
- Recommended range: 3-10

**`batch_size`** (integer, default: `1000`)
- Number of documents to process in each batch
- Larger batches = fewer round trips but more memory usage
- Smaller batches = more frequent state updates
- Considerations:
  - Document size
  - Network latency
  - Memory availability
- Recommended range: 500-5000

#### Transformation Error Handling

**`transform_error_mode`** (string, default: `"skip"`)
- How to handle errors during field transformations
- Options:
  - `"skip"`: Log error and continue with original value
    - Best for: Production environments
    - Ensures replication continues despite transformation errors
  - `"fail"`: Raise exception and stop replication
    - Best for: Development/testing
    - Ensures transformations work correctly

#### State Management

**`state`** (object)
- Configuration for replication state tracking

```yaml
state:
  runs_collection: "_rep_runs"
  state_collection: "_rep_state"
```

**`runs_collection`** (string, default: `"_rep_runs"`)
- Collection name for storing job run history
- Each document represents one complete replication job run
- Contains:
  - Job ID
  - Start/end timestamps
  - Status (success/failure)
  - Collections replicated
  - Error messages (if any)

**`state_collection`** (string, default: `"_rep_state"`)
- Collection name for storing per-collection replication state
- Each document represents the replication state for one collection
- Contains:
  - Collection name
  - Last cursor value (for incremental loading)
  - Last successful replication timestamp
  - Document counts
- Used for resuming interrupted replications

### Collection-Specific Configuration

Override defaults and configure PII anonymization per collection.

```yaml
replication:
  collections:
    users:
      enabled: true
      cursor_field: "updated_at"
      write_disposition: "merge"
      primary_key: "_id"
      match: {...}
      fields_exclude: [...]
      pii: {...}
      field_transforms: [...]
```

#### Basic Collection Options

**`enabled`** (boolean, default: `true`)
- Whether to replicate this collection
- Set to `false` to skip replication
- Useful for temporarily disabling collections

**`cursor_field`** (string, optional)
- Field to track position for incremental loading
- Overrides the auto-detected cursor field from scan
- Must be indexed and monotonically increasing
- Falls back to `defaults.fallback_cursor` if not specified

**`write_disposition`** (string, optional)
- Override global write strategy for this collection
- Values: `"merge"`, `"append"`, `"replace"`
- See Write Strategies section above for details

**`primary_key`** (string, required for `"merge"`)
- Field to use for matching documents during merge operations
- Typically `"_id"` or a business key
- Must be unique
- Required when `write_disposition: "merge"`

#### Match Filters

**`match`** (object, optional)
- MongoDB query filter to select which documents to replicate
- Applied to source collection during replication
- Supports all MongoDB query operators
- Example:
  ```yaml
  match:
    status: "active"
    created_at: { $gte: "2024-01-01" }
    plan: { $in: ["premium", "enterprise"] }
  ```

#### Field Exclusion

**`fields_exclude`** (list of strings, optional)
- List of field names to exclude from replication
- Supports nested fields with dot notation
- Example:
  ```yaml
  fields_exclude:
    - password_hash
    - internal_notes
    - metadata.internal_id
    - legacy_data
  ```

#### PII Configuration

**`pii`** (object, optional)
- Configure PII detection and anonymization for this collection

```yaml
pii:
  enabled: true
  fields:
    email: "fake"
    phone: "hash"
    ssn: "redact"
```

**`pii.enabled`** (boolean, default: `false`)
- Enable PII anonymization for this collection
- When `true`, fields specified in `fields` will be anonymized

**`pii.fields`** (object, optional)
- Map of field names to anonymization strategies
- Keys: Field names (supports dot notation for nested fields)
- Values: Strategy name (`"fake"`, `"redact"`, `"hash"`, `"mask"`, `"null"`)
- Example:
  ```yaml
  fields:
    email: "fake"              # Generate fake email
    phone: "hash"              # Hash phone number
    ssn: "redact"              # Redact SSN
    "contact.email": "fake"    # Nested field
    password_hash: "null"      # Remove completely
  ```

#### Field Transformations

**`field_transforms`** (list of objects, optional)
- Apply transformations to fields during replication
- Uses regex patterns for flexible string manipulation

```yaml
field_transforms:
  - field: "phone"
    type: "regex_replace"
    pattern: "\\D"
    replacement: ""
```

**Transform Options:**
- `field` (string, required): Field name (supports dot notation)
- `type` (string, required): Transformation type (currently only `"regex_replace"`)
- `pattern` (string, required): Regular expression pattern to match
- `replacement` (string, required): Replacement string
- `error_mode` (string, optional): `"skip"` or `"fail"` (overrides global setting)

Example transformations:
```yaml
# Remove all non-digits from phone numbers
- field: "phone"
  type: "regex_replace"
  pattern: "\\D"
  replacement: ""

# Convert URLs to domains
- field: "website"
  type: "regex_replace"
  pattern: "^https?://([^/]+).*"
  replacement: "\\1"

# Mask email domains
- field: "email"
  type: "regex_replace"
  pattern: "@.*$"
  replacement: "@example.com"
```

### Schema (Relationships)

Define parent-child relationships between collections for cascade replication.

```yaml
replication:
  schema:
    - parent: customers
      child: orders
      parent_field: _id
      child_field: customerId

    - parent: orders
      child: order_items
      parent_field: _id
      child_field: orderId
```

When using the `--ids` or `--query` option with the `run` command, the tool will:
1. Replicate records from the root collection matching your filter
2. Find related records in child collections based on relationships
3. Cascade through the entire relationship chain

#### Relationship Options

**`parent`** (string, required)
- Name of the parent collection
- Must match an existing collection name

**`child`** (string, required)
- Name of the child collection
- Must match an existing collection name
- Each child can only have ONE parent collection

**`parent_field`** (string, required)
- Field in parent collection (typically `_id`)
- Should be the primary key or unique identifier

**`child_field`** (string, required)
- Field in child collection that references parent
- Foreign key field pointing to parent collection
- Should be indexed for performance

#### Using Cascade Replication

Once relationships are defined, use the CLI options:

**With --ids (ID-based filtering):**
```bash
# Replicate specific customers and all related data
mongo-rep run job_name --ids customers=507f1f77bcf86cd799439011

# Multiple IDs
mongo-rep run job_name --ids customers=507f1f77bcf86cd799439011,507f191e810c19729de860ea
```

**With --query (Query-based filtering):**
```bash
# Replicate customers by plan and all related data
mongo-rep run job_name --query customers='{"plan": "Basic"}'

# Complex queries
mongo-rep run job_name --query customers='{"createdAt": {"$gte": "2024-01-01"}, "status": "active"}'
```

#### Important Notes

- Relationships are directional (parent → child)
- Each child can only have ONE parent collection
- Multiple levels of cascading are supported
- All IDs must be valid ObjectIds (24-character hex strings)
- With `--ids` or `--query`, all collections use `"merge"` (upsert) write strategy
- Incremental cursor-based replication is disabled with cascade filtering
- Collections with zero matching records are skipped

## Complete Examples

### Example 1: Basic Configuration

```yaml
scan:
  discovery:
    exclude_patterns:
      - "^tmp_"
      - "^test_"

  pii:
    enabled: true
    confidence_threshold: 0.85
    sample_size: 1000

replication:
  defaults:
    replicate_all: true
    write_disposition: "merge"
    batch_size: 1000
    max_parallel_collections: 5

    exclude_patterns:
      - "^system\\."
      - "^tmp_"

  collections:
    users:
      cursor_field: "updated_at"
      primary_key: "_id"
      fields_exclude:
        - password_hash
      pii:
        enabled: true
        fields:
          email: "fake"
          phone: "hash"

    orders:
      cursor_field: "created_at"
      primary_key: "_id"
      match:
        status: { $ne: "draft" }
```

### Example 2: With Cascade Relationships

```yaml
replication:
  defaults:
    replicate_all: true
    write_disposition: "merge"

  collections:
    customers:
      primary_key: "_id"
      pii:
        enabled: true
        fields:
          email: "fake"
          phone: "hash"
          ssn: "redact"

    orders:
      primary_key: "_id"

    order_items:
      primary_key: "_id"

  schema:
    - parent: customers
      child: orders
      parent_field: _id
      child_field: customerId

    - parent: orders
      child: order_items
      parent_field: _id
      child_field: orderId
```

### Example 3: Field Transformations

```yaml
replication:
  collections:
    users:
      primary_key: "_id"
      field_transforms:
        # Normalize phone numbers
        - field: "phone"
          type: "regex_replace"
          pattern: "\\D"
          replacement: ""

        # Extract domain from email
        - field: "email_domain"
          type: "regex_replace"
          pattern: ".*@(.*)$"
          replacement: "\\1"

      pii:
        enabled: true
        fields:
          email: "fake"
          name: "hash"
```

### Example 4: Advanced PII Detection

```yaml
scan:
  pii:
    enabled: true
    confidence_threshold: 0.90
    entity_types:
      - EMAIL_ADDRESS
      - PHONE_NUMBER
      - PERSON
      - US_SSN
      - CREDIT_CARD
    sample_size: 2000
    sample_strategy: "stratified"
    default_strategies:
      EMAIL_ADDRESS: "fake"
      PHONE_NUMBER: "redact"
      PERSON: "hash"
      US_SSN: "redact"
      CREDIT_CARD: "redact"
    allowlist:
      - "_id"
      - "meta.*"
      - "*.id"
      - "*.created_at"
      - "*.updated_at"
      - "internal_*"

replication:
  collections:
    sensitive_data:
      pii:
        enabled: true
        fields:
          email: "fake"
          phone: "redact"
          ssn: "redact"
          credit_card: "null"
```

## Environment Variables

Configure jobs via environment variables (useful for CI/CD):

```bash
# Enable the job
export MONGOREP_MY_JOB_ENABLED=true

# Connection URIs
export MONGOREP_MY_JOB_SOURCE_URI="mongodb://user:pass@source:27017/source_db?authSource=admin"
export MONGOREP_MY_JOB_DESTINATION_URI="mongodb://user:pass@dest:27017/dest_db?authSource=admin"

# Configuration file path
export MONGOREP_MY_JOB_CONFIG_PATH="config/my_job_config.yaml"
```

### Connection URI Options

MongoDB connection URIs support various options:

```
mongodb://[username:password@]host[:port][/database][?options]
```

Common options:
- `authSource=admin`: Database for authentication
- `retryWrites=true`: Enable retryable writes
- `w=majority`: Write concern
- `readPreference=primary`: Read preference
- `serverSelectionTimeoutMS=30000`: Server selection timeout
- `maxPoolSize=100`: Maximum connection pool size

## Validation

The tool validates configuration on load:

- Required fields are present
- Field types match expectations
- Relationships reference existing collections
- PII configuration is valid
- Regex patterns are valid
- Anonymization strategies are valid

Validation errors are reported with specific details and suggestions for fixes.
