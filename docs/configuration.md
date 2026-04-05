# Configuration Reference

Complete reference for configuring MongoDB replication jobs.

## Table of Contents

- [Configuration File Structure](#configuration-file-structure)
- [Defaults Section](#defaults-section)
- [Collections Section](#collections-section)
- [Relationships Section](#relationships-section)
- [PII Configuration](#pii-configuration)
- [Field Transformations](#field-transformations)
- [Environment Variables](#environment-variables)

## Configuration File Structure

Configuration files use YAML format with three main sections:

```yaml
defaults:
  # Global settings applied to all collections
  
collections:
  # Per-collection configuration
  
relationships:
  # Foreign key relationships for cascade filtering
```

## Defaults Section

Global settings that apply to all collections unless overridden.

### Replication Behavior

```yaml
defaults:
  # Include all collections by default
  replicate_all: true
  
  # Number of documents per batch
  batch_size: 1000
  
  # Maximum collections to process in parallel
  max_parallel_collections: 5
  
  # Default cursor field for incremental loading
  fallback_cursor: _id
  
  # Default write strategy
  write_disposition: replace  # or 'append', 'merge'
```

#### Options:

- **`replicate_all`** (boolean, default: `true`)
  - `true`: Replicate all collections except those matching `exclude_patterns`
  - `false`: Only replicate collections matching `include_patterns`

- **`batch_size`** (integer, default: `1000`)
  - Number of documents to process in each batch
  - Adjust based on document size and network latency
  - Smaller batches: More frequent state updates, slower overall
  - Larger batches: Faster overall, but more work lost on failure

- **`max_parallel_collections`** (integer, default: `5`)
  - Maximum number of collections to replicate simultaneously
  - Higher values increase throughput but consume more resources
  - Recommended: 3-5 for network replication, 5-10 for local

- **`fallback_cursor`** (string, default: `"_id"`)
  - Cursor field used when collection doesn't specify one
  - Should be indexed and monotonically increasing
  - Common options: `_id`, `created_at`, `updated_at`

- **`write_disposition`** (string, default: `"replace"`)
  - `replace`: Drop and recreate collection on each run
  - `append`: Add new documents without deleting existing ones
  - `merge`: Update existing documents, insert new ones (requires `primary_key`)

### Collection Filtering

```yaml
defaults:
  # Regex patterns for collections to include (when replicate_all=false)
  include_patterns:
    - "^user_"
    - "^order_"
  
  # Regex patterns for collections to exclude
  exclude_patterns:
    - "^system\\."
    - "^tmp_"
    - "_backup$"
```

#### Options:

- **`include_patterns`** (list of strings)
  - Only used when `replicate_all: false`
  - Regular expressions to match collection names
  - Collections must match at least one pattern to be included

- **`exclude_patterns`** (list of strings)
  - Regular expressions to exclude collection names
  - Takes precedence over `include_patterns`
  - Always applied regardless of `replicate_all` setting

### State Management

```yaml
defaults:
  state:
    # Collection for tracking job runs
    runs_collection: _rep_runs
    
    # Collection for tracking collection-level state
    state_collection: _rep_state
```

#### Options:

- **`runs_collection`** (string, default: `"_rep_runs"`)
  - Collection name for storing job run metadata
  - One document per replication job run
  
- **`state_collection`** (string, default: `"_rep_state"`)
  - Collection name for storing per-collection state
  - One document per collection per run

## Collections Section

Per-collection configuration that overrides defaults.

### Basic Collection Config

```yaml
collections:
  users:
    # Cursor field for incremental loading
    cursor_field: updated_at
    
    # Write strategy
    write_disposition: merge
    
    # Primary key for merge operations
    primary_key: _id
    
    # Whether to replicate this collection
    enabled: true
```

#### Options:

- **`cursor_field`** (string, optional)
  - Field to track position for incremental loading
  - Must be indexed and monotonically increasing
  - Falls back to `defaults.fallback_cursor` if not specified

- **`write_disposition`** (string, optional)
  - Override global write strategy
  - Values: `replace`, `append`, `merge`

- **`primary_key`** (string, required for `merge`)
  - Field to use for matching documents during merge
  - Typically `_id` or a business key
  - Must be unique

- **`enabled`** (boolean, default: `true`)
  - Set to `false` to skip this collection

### Match Filters

Filter documents during replication:

```yaml
collections:
  orders:
    match:
      status: { $in: ["completed", "shipped"] }
      created_at: { $gte: "2024-01-01" }
```

#### Options:

- **`match`** (object)
  - MongoDB query filter
  - Applied to source collection during replication
  - Supports all MongoDB query operators

### Field Exclusion

Exclude specific fields from replication:

```yaml
collections:
  users:
    fields_exclude:
      - password_hash
      - internal_notes
      - legacy_field
```

#### Options:

- **`fields_exclude`** (list of strings)
  - Field names to exclude from replicated documents
  - Supports nested fields with dot notation: `address.internal_code`

## PII Configuration

Configure PII detection and anonymization per collection.

### Basic PII Config

```yaml
collections:
  users:
    pii:
      enabled: true
      fields:
        - email
        - phone
        - ssn
      detection_mode: field_name
```

#### Options:

- **`enabled`** (boolean, default: `false`)
  - Enable PII detection for this collection

- **`fields`** (list of strings)
  - Fields to check for PII
  - Supports nested fields: `contact.email`

- **`detection_mode`** (string, default: `"field_name"`)
  - `field_name`: Detect PII based on field names
  - `content`: Analyze field content with NLP (slower, more accurate)

### Anonymization Strategies

```yaml
collections:
  users:
    pii:
      enabled: true
      fields:
        - email
        - phone
        - ssn
        - address
      anonymization:
        email: mask       # foo@bar.com → f**@bar.com
        phone: hash       # Hash with SHA-256
        ssn: redact       # Replace with [REDACTED]
        address: replace  # Replace with fake data
```

#### Anonymization Methods:

- **`mask`**: Partially hide the value
  - Emails: `user@example.com` → `u***@example.com`
  - Strings: Show first and last character only

- **`hash`**: One-way hash with SHA-256
  - Irreversible transformation
  - Same input always produces same output

- **`redact`**: Replace with `[REDACTED]`
  - Completely removes information
  - Useful for highly sensitive data

- **`replace`**: Replace with realistic fake data
  - Uses Faker library to generate plausible values
  - Maintains data format and type

### Entity Recognition

For content-based detection:

```yaml
collections:
  documents:
    pii:
      enabled: true
      detection_mode: content
      entities:
        - PERSON
        - EMAIL_ADDRESS
        - PHONE_NUMBER
        - CREDIT_CARD
        - IP_ADDRESS
        - LOCATION
```

#### Supported Entities:

- `PERSON`: Person names
- `EMAIL_ADDRESS`: Email addresses
- `PHONE_NUMBER`: Phone numbers
- `CREDIT_CARD`: Credit card numbers
- `IP_ADDRESS`: IP addresses
- `LOCATION`: Physical addresses
- `DATE_TIME`: Dates and times
- `NRP`: Nationalities, religions, political groups
- `US_SSN`: US Social Security Numbers
- `US_DRIVER_LICENSE`: US driver's license numbers

## Field Transformations

Apply transformations to fields during replication.

### Numeric Transformations

```yaml
collections:
  products:
    field_transforms:
      - field: price
        operation: multiply
        value: 1.15  # Add 15% markup
        
      - field: quantity
        operation: add
        value: 100  # Add safety stock
```

### String Transformations

```yaml
collections:
  users:
    field_transforms:
      - field: status
        operation: map
        mapping:
          0: inactive
          1: active
          2: suspended
          
      - field: email
        operation: lowercase
```

### Date Transformations

```yaml
collections:
  events:
    field_transforms:
      - field: event_date
        operation: add_days
        value: 7  # Shift dates forward 7 days
```

### Available Operations:

- **Numeric**: `add`, `subtract`, `multiply`, `divide`
- **String**: `uppercase`, `lowercase`, `map`
- **Date**: `add_days`, `subtract_days`, `add_hours`
- **Custom**: `custom` (with Python function)

## Relationships Section

Define foreign key relationships for cascade filtering.

```yaml
relationships:
  - source_collection: customers
    target_collection: orders
    source_field: _id
    target_field: customer_id
    
  - source_collection: orders
    target_collection: order_items
    source_field: _id
    target_field: order_id
    
  - source_collection: orders
    target_collection: invoices
    source_field: _id
    target_field: order_id
```

#### Options:

- **`source_collection`** (string, required)
  - Name of the parent collection

- **`target_collection`** (string, required)
  - Name of the child collection

- **`source_field`** (string, required)
  - Field in parent collection (typically `_id`)

- **`target_field`** (string, required)
  - Field in child collection that references parent

### Using Cascade Filters

Once relationships are defined:

```bash
# Replicate customer and all related orders, invoices, etc.
mongo-replication run my_job --select customers=507f1f77bcf86cd799439011

# Multiple root IDs
mongo-replication run my_job --select customers=id1,id2,id3
```

## Environment Variables

Configure jobs via environment variables:

```env
# Enable the job
REP_MY_JOB_ENABLED=true

# Connection URIs
REP_MY_JOB_SOURCE_URI=mongodb://source:27017/source_db?retryWrites=true
REP_MY_JOB_DESTINATION_URI=mongodb://dest:27017/dest_db?authSource=admin

# Configuration file path
REP_MY_JOB_CONFIG_PATH=config/my_job_config.yaml
```

### Connection URI Options

MongoDB connection URIs support various options:

```
mongodb://user:pass@host:port/database?option1=value1&option2=value2
```

Common options:
- `authSource=admin`: Database for authentication
- `retryWrites=true`: Enable retryable writes
- `w=majority`: Write concern
- `readPreference=primary`: Read preference
- `serverSelectionTimeoutMS=30000`: Server selection timeout

## Complete Example

```yaml
defaults:
  replicate_all: true
  batch_size: 1000
  max_parallel_collections: 5
  fallback_cursor: _id
  write_disposition: merge
  
  state:
    runs_collection: _rep_runs
    state_collection: _rep_state
  
  exclude_patterns:
    - "^system\\."
    - "^tmp_"

collections:
  users:
    cursor_field: updated_at
    write_disposition: merge
    primary_key: _id
    pii:
      enabled: true
      fields:
        - email
        - phone
      anonymization:
        email: mask
        phone: hash
    fields_exclude:
      - password_hash
      
  orders:
    cursor_field: created_at
    write_disposition: append
    match:
      status: { $ne: "draft" }
      
  products:
    write_disposition: replace
    field_transforms:
      - field: price
        operation: multiply
        value: 1.1

relationships:
  - source_collection: users
    target_collection: orders
    source_field: _id
    target_field: user_id
    
  - source_collection: orders
    target_collection: order_items
    source_field: _id
    target_field: order_id
```

## Validation

The tool validates configuration on load:

- Required fields are present
- Field types match expectations
- Relationships reference existing collections
- PII configuration is valid
- Regex patterns are valid

Validation errors are reported with specific line numbers and suggestions for fixes.
