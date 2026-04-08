# Configuration Reference

Complete reference for configuring the MongoDB Replication Tool. This document describes all configuration options available in the YAML configuration files.

## Table of Contents

- [Configuration File Structure](#configuration-file-structure)
- [Scan Configuration](#scan-configuration)
  - [Discovery Settings](#discovery-settings)
  - [Sampling Configuration](#sampling-configuration)
  - [PII Analysis Settings](#pii-analysis-settings)
  - [Cursor Detection Settings](#cursor-detection-settings)
  - [Schema Relationship Analysis](#schema-relationship-analysis)
- [Replication Configuration](#replication-configuration)
  - [Collection Discovery](#collection-discovery)
  - [State Management](#state-management)
  - [Performance Settings](#performance-settings)
  - [Collection Defaults](#collection-defaults)
  - [Collection-Specific Configuration](#collection-specific-configuration)
  - [Schema Relationships](#schema-relationships)
- [Complete Examples](#complete-examples)

## Configuration File Structure

Configuration files use YAML format with two main root sections:

```yaml
scan:
  # Settings for scanning and analyzing collections
  discovery: {...}
  sampling: {...}
  pii_analysis: {...}
  cursor_detection: {...}
  schema_relationships: {...}

replication:
  # Settings for replicating collections
  discovery: {...}
  state_management: {...}
  performance: {...}
  defaults: {...}
  collections: {...}

schema_relationships: [...]
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

### Sampling Configuration

Controls how many documents are sampled for PII detection and cursor field detection.

```yaml
scan:
  sampling:
    sample_size: 1000
    sample_strategy: "stratified"
```

#### Options

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

### PII Analysis Settings

Configuration for automatic PII detection using Microsoft Presidio.

```yaml
scan:
  pii_analysis:
    enabled: true
    confidence_threshold: 0.85
    entity_types: [...]
    default_strategies: {...}
    allowlist: [...]
    presidio_config: null
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

**`presidio_config`** (string or null, default: `null`)
- Path to custom Presidio YAML configuration file
- Allows defining domain-specific PII recognizers without writing Python code
- When `null`, uses default Presidio configuration with built-in recognizers

**Use cases:**
- Detect custom patterns (employee IDs, patient numbers, internal codes)
- Add context words to improve detection accuracy
- Override or customize default recognizers
- Configure different NLP models for better entity recognition

**Path resolution (checked in order):**
1. Absolute path (e.g., `/path/to/presidio.yaml`)
2. Relative to current working directory
3. Relative to `config/` directory
4. Default location: `src/mongo_replication/config/presidio.yaml`

**Example configuration:**
```yaml
scan:
  pii_analysis:
    presidio_config: "config/custom_presidio.yaml"
```

See [Custom Presidio Configuration](#custom-presidio-configuration) section below for detailed examples.

### Cursor Detection Settings

Configuration for automatic cursor field detection during scan.

```yaml
scan:
  cursor_detection:
    cursor_fields:
      - updated_at
      - updatedAt
      - meta.updated_at
      - meta.updatedAt
```

#### Options

**`cursor_fields`** (list of strings, default: see above)
- List of field names to try as cursor fields for incremental loading
- The scan command auto-detects if any of these fields exist in collections
- Checked in priority order (first match wins)
- Requirements for cursor fields:
  - Must be indexed for performance
  - Should be monotonically increasing
  - Should represent the last modification time
- Detected cursor fields are automatically set in collection configurations
- If no cursor field is found, replication will use full collection scan

### Schema Relationship Analysis

Configuration for automatic schema relationship detection during scan.

```yaml
scan:
  schema_relationships:
    enabled: false
```

#### Options

**`enabled`** (boolean, default: `false`)
- Whether to automatically infer parent-child relationships between collections during scan
- When enabled, the scan command analyzes field names to detect relationships
- Inferred relationships are saved to the root-level `schema_relationships` section
- Can be used later with `--ids` or `--query` for cascade replication

**How it works:**
The analyzer examines sampled documents and matches field names to collection names using patterns:
- **Snake case**: `customer_id` in `orders` → relationship to `customers`
- **Camel case**: `customerId` in `orders` → relationship to `customers`
- **Nested fields**: `meta.customer_id` → relationship to `customers`
- **Plural/singular**: `category_id` → relationship to `categories`

**Ignored fields:**
Common non-relationship fields are automatically excluded:
- `_id`, `id` (document identifiers)
- `created_at`, `updated_at`, `deleted_at` (timestamps)
- `created_by`, `updated_by`, `deleted_by` (audit fields)

**Example detected relationships:**
```yaml
schema_relationships:
  - parent: customers
    child: orders
    parent_field: _id
    child_field: customer_id

  - parent: orders
    child: order_items
    parent_field: _id
    child_field: order_id
```

**When to enable:**
- ✅ Enable if you plan to use cascade replication with `--ids` or `--query`
- ✅ Enable for databases with clear naming conventions (e.g., `*_id` fields)
- ❌ Disable if your field names don't follow conventions
- ❌ Disable if you want to manually define relationships

See [Schema Relationships](#schema-relationships) section below for using relationships with cascade replication.

## Replication Configuration

The `replication` section controls how collections are replicated from source to destination.

### Collection Discovery

Controls which collections are automatically discovered and replicated.

```yaml
replication:
  discovery:
    replicate_all: true
    include_patterns: []
    exclude_patterns: []
```

#### Options

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

### State Management

Configuration for replication state tracking.

```yaml
replication:
  state_management:
    runs_collection: "_rep_runs"
    state_collection: "_rep_state"
```

#### Options

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

### Performance Settings

Configuration for parallel processing and batch sizes.

```yaml
replication:
  performance:
    max_parallel_collections: 5
    batch_size: 1000
```

#### Options

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

### Collection Defaults

Global settings that apply to all collections unless overridden in collection-specific configuration.

```yaml
replication:
  defaults:
    write_disposition: "merge"
    cursor_fields: [...]
    cursor_fallback_field: "_id"
    cursor_initial_value: "2020-01-01T00:00:00Z"
    transform_error_mode: "skip"
```

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

**`cursor_fallback_field`** (string, default: `"_id"`)
- Field to use for incremental loading when no `cursor_fields` match
- Used when collection doesn't have timestamp fields
- Default `_id` works for most cases (uses ObjectId timestamp)
- Alternative: Any indexed field with ascending values

**`cursor_initial_value`** (string, default: `"2020-01-01T00:00:00Z"`)
- Initial cursor value for first-time replication
- Used when no previous replication state exists
- Format: ISO 8601 datetime string
- Determines starting point for incremental replication

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
      pii_anonymized_fields: {...}
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

**`pii_anonymized_fields`** (object, optional)
- Configure PII fields anonymization for this collection

```yaml
pii_anonymized_fields:
  email: "fake"
  phone: "hash"
  ssn: "redact"
```

**`pii_anonymized_fields`** (object, optional)
- Keys: Field names (supports dot notation for nested fields)
- Values: Strategy name (`"fake"`, `"redact"`, `"hash"`, `"mask"`, `"null"`)
- Example:
```yaml
pii_anonymized_fields:
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
field_transforms:
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

### Schema Relationships

Define parent-child relationships between collections for cascade replication.

```yaml
schema_relationships:
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

  sampling:
    sample_size: 1000
    sample_strategy: stratified

  pii_analysis:
    enabled: true
    confidence_threshold: 0.85

replication:
  discovery:
    replicate_all: true
    exclude_patterns:
      - "^system\\."
      - "^tmp_"

  performance:
    max_parallel_collections: 5
    batch_size: 1000

  defaults:
    write_disposition: "merge"

  collections:
    users:
      cursor_field: "updated_at"
      primary_key: "_id"
      fields_exclude:
        - password_hash
      pii_anonymized_fields:
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
  discovery:
    replicate_all: true

  defaults:
    write_disposition: "merge"

  collections:
    customers:
      primary_key: "_id"
      pii_anonymized_fields:
        email: "fake"
        phone: "hash"
        ssn: "redact"

    orders:
      primary_key: "_id"

    order_items:
      primary_key: "_id"

schema_relationships:
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

      pii_anonymized_fields:
        email: "fake"
        name: "hash"
```

### Example 4: Advanced PII Detection

```yaml
scan:
  sampling:
    sample_size: 2000
    sample_strategy: "stratified"

  pii_analysis:
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
      pii_anonymized_fields:
        email: "fake"
        phone: "redact"
        ssn: "redact"
        credit_card: "null"
```

## Custom Presidio Configuration

The `presidio_config` field allows you to define custom PII recognizers using YAML configuration without writing Python code. This is useful for detecting domain-specific patterns like employee IDs, patient numbers, internal codes, or any custom PII types specific to your organization.

### Getting Started

1. **Copy the default template:**
   ```bash
   cp src/mongo_replication/config/presidio.yaml config/my_job_presidio.yaml
   ```

2. **Reference it in your config:**
   ```yaml
   scan:
     pii_analysis:
       presidio_config: "config/my_job_presidio.yaml"
   ```

3. **Customize recognizers** in the YAML file (see examples below)

### Configuration Structure

A Presidio YAML configuration has three main sections:

```yaml
# NLP Engine Configuration
nlp_engine_name: spacy
nlp_configuration:
  nlp_engine_name: spacy
  models:
    - lang_code: en
      model_name: en_core_web_lg

# Global Settings
supported_languages: [en]
default_score_threshold: 0.35

# Recognizers (predefined + custom)
recognizers:
  - name: EmailRecognizer
    supported_entity: EMAIL_ADDRESS
    # ... configuration ...
```

### Example 1: Employee ID Recognizer

Detect employee IDs in format `EMP-12345` or `E12345`:

```yaml
recognizers:
  - name: EmployeeIdRecognizer
    supported_entity: EMPLOYEE_ID
    supported_languages: [en]
    patterns:
      # EMP-12345 format
      - name: emp_prefix_pattern
        regex: "\\bEMP-\\d{5,8}\\b"
        score: 0.7
      # E12345 format
      - name: emp_short_pattern
        regex: "\\bE\\d{5,8}\\b"
        score: 0.6
    context:
      - "employee"
      - "employee id"
      - "emp"
      - "staff"
      - "worker"
      - "badge"
```

**Key points:**
- `patterns`: List of regex patterns with confidence scores
- `context`: Words that boost confidence when found nearby
- Higher `score` = more confident match
- Use `\\b` for word boundaries to avoid partial matches

### Example 2: Patient ID Recognizer (Healthcare)

Detect patient IDs in format `PT-YYYYMMDD-XXXX`:

```yaml
recognizers:
  - name: PatientIdRecognizer
    supported_entity: PATIENT_ID
    supported_languages: [en]
    patterns:
      - name: patient_id_pattern
        regex: "\\bPT-\\d{8}-\\d{4}\\b"
        score: 0.8
    context:
      - "patient"
      - "patient id"
      - "medical record"
      - "mrn"
      - "chart"
      - "admission"
```

### Example 3: Custom API Key Detector

Detect API keys with common prefixes:

```yaml
recognizers:
  - name: ApiKeyRecognizer
    supported_entity: API_KEY
    supported_languages: [en]
    patterns:
      - name: api_key_pattern
        regex: "\\b(api_key|apikey|api-key)\\s*[=:]\\s*['\"]?([A-Za-z0-9_\\-]{20,})['\"]?"
        score: 0.9
    context:
      - "api"
      - "key"
      - "token"
      - "secret"
      - "credential"
      - "authentication"
```

### Example 4: Deny-List Based Recognizer

Detect professional titles using exact matches:

```yaml
recognizers:
  - name: TitleRecognizer
    supported_entity: TITLE
    supported_languages: [en]
    deny_list:
      - "Dr."
      - "Mr."
      - "Mrs."
      - "Ms."
      - "Miss"
      - "PhD"
      - "MD"
      - "Esq"
    context:
      - "title"
      - "name"
      - "salutation"
```

**Note:** Deny-list recognizers match exact strings (case-sensitive).

### Example 5: US Bank Account Numbers

Detect routing numbers and account numbers:

```yaml
recognizers:
  - name: UsBankAccountRecognizer
    supported_entity: US_BANK_ACCOUNT
    supported_languages: [en]
    patterns:
      # US Routing Number: 9 digits
      - name: routing_number_pattern
        regex: "\\b[0-9]{9}\\b"
        score: 0.5
      # US Account Number: 8-17 digits
      - name: account_number_pattern
        regex: "\\b[0-9]{8,17}\\b"
        score: 0.5
    context:
      - "routing"
      - "routing number"
      - "account"
      - "account number"
      - "bank account"
      - "checking"
      - "savings"
      - "ach"
      - "wire"
```

**Note:** Lower scores with strong context words reduce false positives.

### Complete Configuration Example

Full configuration with multiple custom recognizers:

```yaml
scan:
  pii_analysis:
    enabled: true
    confidence_threshold: 0.85
    presidio_config: "config/healthcare_presidio.yaml"

    # Map custom entity types to anonymization strategies
    default_strategies:
      EMAIL_ADDRESS: "fake"
      PHONE_NUMBER: "fake"
      PERSON: "hash"
      PATIENT_ID: "hash"        # Custom entity
      MEDICAL_RECORD: "hash"    # Custom entity
      EMPLOYEE_ID: "redact"     # Custom entity

    # Allowlist to prevent false positives
    allowlist:
      - "_id"
      - "meta.*"
      - "*.id"
      - "*.created_at"
      - "*.updated_at"
```

Then in `config/healthcare_presidio.yaml`:

```yaml
nlp_engine_name: spacy
nlp_configuration:
  nlp_engine_name: spacy
  models:
    - lang_code: en
      model_name: en_core_web_lg

supported_languages: [en]
default_score_threshold: 0.35

recognizers:
  # Include default recognizers
  - name: EmailRecognizer
    supported_entity: EMAIL_ADDRESS
    supported_languages: [en]

  - name: PhoneRecognizer
    supported_entity: PHONE_NUMBER
    supported_languages: [en]

  # Custom healthcare recognizers
  - name: PatientIdRecognizer
    supported_entity: PATIENT_ID
    supported_languages: [en]
    patterns:
      - name: patient_id_pattern
        regex: "\\bPT-\\d{8}-\\d{4}\\b"
        score: 0.8
    context:
      - "patient"
      - "patient id"
      - "mrn"

  - name: MedicalRecordRecognizer
    supported_entity: MEDICAL_RECORD
    supported_languages: [en]
    patterns:
      - name: mrn_pattern
        regex: "\\bMRN-\\d{6,10}\\b"
        score: 0.8
    context:
      - "medical record"
      - "record"
      - "chart"
```

### Regex Pattern Tips

1. **Use word boundaries (`\\b`)** to avoid partial matches:
   ```yaml
   regex: "\\bEMP-\\d{5}\\b"  # Good: matches "EMP-12345"
   regex: "EMP-\\d{5}"        # Bad: matches "TEMP-12345"
   ```

2. **Escape special characters** with double backslash:
   ```yaml
   regex: "\\$\\d+\\.\\d{2}"  # Matches "$99.99"
   ```

3. **Test your patterns** at [regex101.com](https://regex101.com/) before deployment

4. **Start with lower scores** and adjust based on false positives:
   ```yaml
   score: 0.6  # Start conservative
   # Test with real data
   # Increase if too many false positives
   # Decrease if missing true positives
   ```

### Context Words Best Practices

Context words boost confidence when found near detected patterns:

```yaml
context:
  - "employee"      # Exact match
  - "emp id"        # Multi-word phrase
  - "staff number"  # Alternative phrasing
```

**Guidelines:**
- Include common field names from your MongoDB collections
- Add domain-specific terminology
- Include abbreviations and variations
- Be specific but comprehensive

### NLP Model Selection

The NLP model affects detection accuracy:

```yaml
models:
  - lang_code: en
    model_name: en_core_web_lg  # Large: Best accuracy, slower
  # model_name: en_core_web_md  # Medium: Good balance
  # model_name: en_core_web_sm  # Small: Faster, less accurate
```

**Installation:**
```bash
python -m spacy download en_core_web_lg
```

### Testing Your Configuration

1. **Run a small scan first:**
   ```bash
   mongorep scan my_job --sample-size 100
   ```

2. **Check for false positives** in the scan results

3. **Adjust scores and patterns** as needed

4. **Validate with production-like data** before full deployment

### Troubleshooting

**Configuration file not found:**
```
Error: Presidio configuration file not found: config/custom.yaml
```
- Check file path is correct (relative or absolute)
- Ensure file exists in one of the search locations
- Use absolute path if relative path issues persist

**Invalid YAML syntax:**
```
Error: Failed to load Presidio configuration: YAML syntax error
```
- Validate YAML syntax at [yamllint.com](http://www.yamllint.com/)
- Check indentation (use spaces, not tabs)
- Ensure proper quoting of regex patterns

**Too many false positives:**
- Increase `score` values in patterns
- Add more specific context words
- Use more restrictive regex patterns
- Add false positive fields to `allowlist`

**Missing detections:**
- Decrease `score` values
- Add more context words
- Broaden regex patterns
- Lower `confidence_threshold` in scan config

### Resources

- **Default template:** `src/mongo_replication/config/presidio.yaml`
- **Presidio documentation:** https://microsoft.github.io/presidio/
- **YAML configuration guide:** https://microsoft.github.io/presidio/samples/python/no_code_config/
- **Adding recognizers:** https://microsoft.github.io/presidio/analyzer/adding_recognizers/
- **Supported entities:** https://microsoft.github.io/presidio/supported_entities/
- **Regex testing:** https://regex101.com/

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
