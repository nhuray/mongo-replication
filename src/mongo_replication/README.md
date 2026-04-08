# MongoDB Replication CLI

Command-line interface for MongoDB replication with PII detection and anonymization.

## Quick Start

```bash
# Install
pip install -e .

# Initialize a new job (interactive wizard)
mongorep init prod

# Scan collections and detect PII
mongorep scan prod --sample-size 1000

# Run replication
mongorep run prod
```

---

## Commands

### `mongorep init` - Initialize New Job Configuration

Initialize a new replication job with an interactive wizard that guides you through the setup process.

**Usage:**
```bash
mongorep init <job> [OPTIONS]
```

**Arguments:**
- `job` - Job ID to initialize (e.g., 'prod', 'staging', 'backup')

**Options:**
- `--output, -o PATH` - Output config file path (default: `config/<job>_config.yaml`)

**Examples:**
```bash
# Basic initialization
mongorep init prod

# Custom output location
mongorep init prod --output /path/to/custom_config.yaml
```

**What the wizard does:**
1. **Prompt for source MongoDB URI** - Enter source database connection string
2. **Validate source connection** - Test connectivity to source database
3. **Prompt for destination MongoDB URI** - Enter destination database connection string
4. **Validate destination connection** - Test connectivity to destination database
5. **Configure collection discovery** - Set include/exclude patterns for collections
6. **Set up PII detection** - Configure confidence threshold, entity types, and sample size
7. **Select anonymization strategies** - Choose how to handle each PII entity type (fake, hash, redact, etc.)
8. **Choose collections to replicate** - Select all, use patterns, or manually pick collections
9. **Generate configuration file** - Create config file at specified path
10. **Display environment variables** - Show environment variables to add to `.env` file

**Output:**
- Configuration file: `config/<job>_config.yaml`
- Environment variable template (displayed in console)

**Next steps after `init`:**
- Run `mongorep scan <job>` to analyze collections and detect PII
- Run `mongorep run <job>` to start replication
- Edit the generated config file to fine-tune settings

---

### `mongorep scan` - Discover Collections & Analyze PII

Scan a MongoDB database to discover collections and analyze for PII (Personally Identifiable Information).

**Usage:**
```bash
mongorep scan <job> [OPTIONS]
```

**Arguments:**
- `job` - Job ID to scan (e.g., 'prod', 'staging')

**Options:**
- `--output, -o PATH` - Output path for config file (default: `config/<job>_config.yaml`)
- `--collections NAMES` - Comma-separated list of collections to scan (default: all)
- `--interactive, -i` - Interactively select collections to scan
- `--sample-size, -s N` - Number of documents to sample per collection (default: 1000)
- `--confidence, -c FLOAT` - Minimum confidence for PII detection 0.0-1.0 (default: 0.85)
- `--language, -l LANG` - Language for NLP analysis: en, fr (default: en)
- `--no-pii` - Skip PII analysis (only discover collections)

**Examples:**
```bash
# Basic scan
mongorep scan prod

# Scan specific collections only
mongorep scan prod --collections users,orders,customers

# High-accuracy scan with more samples
mongorep scan prod --sample-size 2000 --confidence 0.9

# Interactive collection selection
mongorep scan prod --interactive

# Skip PII analysis (fast collection discovery only)
mongorep scan prod --no-pii

# Custom output location
mongorep scan prod --output configs/prod_full_config.yaml

# French language PII detection
mongorep scan prod_fr --language fr
```

**Output:**
- Configuration file: `config/<job>_config.yaml`
- PII report: `config/<job>_pii_report.md`

---

### `mongorep run` - Execute Replication

Execute replication job from source to destination MongoDB.

**Usage:**
```bash
mongorep run <job> [OPTIONS]
```

**Arguments:**
- `job` - Job ID to run (e.g., 'prod', 'staging')

**Options:**
- `--collections NAMES` - Comma-separated list of collections to replicate (default: all configured)
- `--interactive, -i` - Interactively select collections to replicate
- `--dry-run` - Preview what would be replicated without executing
- `--parallel, -p N` - Maximum number of parallel collections (default: 5)
- `--batch-size, -b N` - Batch size for document processing
- `--ids TEXT` - Cascade replication from specific document IDs (format: `collection=id1,id2,id3`)
- `--query TEXT` - Cascade replication from MongoDB query (format: `collection='{"field": "value"}'`)

**Examples:**
```bash
# Basic replication
mongorep run prod

# Replicate specific collections only
mongorep run prod --collections users,orders

# Dry run to preview
mongorep run prod --dry-run

# Interactive collection selection
mongorep run prod --interactive

# High-performance replication
mongorep run prod --parallel 10 --batch-size 2000

# Cascade replication by specific IDs
mongorep run prod --ids customers=507f1f77bcf86cd799439011

# Cascade replication by query
mongorep run prod --query customers='{"plan": "Basic", "status": "active"}'

# Multiple document IDs
mongorep run prod --ids customers=507f1f77bcf86cd799439011,507f191e810c19729de860ea
```

**Cascade Replication:**
When using `--ids` or `--query`, the tool will:
1. Find documents in the root collection matching your filter
2. Find related documents in child collections based on defined relationships (see schema configuration)
3. Cascade through the entire relationship chain
4. Replicate all matching documents

Note: `--ids` and `--query` cannot be used together. Both are mutually exclusive with `--collections` and `--interactive`.

---

## Configuration

### Environment Variables

Each job requires these environment variables:

```bash
# Enable the job
MONGOREP_<JOB>_ENABLED=true

# Source MongoDB URI
MONGOREP_<JOB>_SOURCE_URI=mongodb://localhost:27017/source_db

# Destination MongoDB URI
MONGOREP_<JOB>_DESTINATION_URI=mongodb://localhost:27017/dest_db

# Configuration file path
MONGOREP_<JOB>_CONFIG_PATH=config/job_config.yaml
```

**Example:**
```bash
# Production to Analytics job
MONGOREP_PROD_ENABLED=true
MONGOREP_PROD_SOURCE_URI=mongodb://prod-host:27017/prod_db
MONGOREP_PROD_DESTINATION_URI=mongodb://analytics-host:27017/analytics_db
MONGOREP_PROD_CONFIG_PATH=config/prod_config.yaml
```

**Job Naming Rules:**
- Use uppercase letters, numbers, and underscores
- Be descriptive: `PROD`, `STAGING`, `PROD_TO_ANALYTICS`, `DAILY_BACKUP`
- Internally normalized to lowercase

---

### Configuration File

The configuration file has two main sections:

#### 1. Scan Configuration (optional)

```yaml
scan:
  # Collection discovery settings
  discovery:
    include_patterns:
      - "^users.*"
      - "^transactions.*"
    exclude_patterns:
      - "^system\\."
      - "^_dlt_.*"

  # PII detection settings
  pii:
    enabled: true
    confidence_threshold: 0.85
    sample_size: 1000
    entity_types:
      - EMAIL_ADDRESS
      - PHONE_NUMBER
      - PERSON
      - US_SSN
      - CREDIT_CARD
    default_strategies:
      EMAIL_ADDRESS: fake
      PHONE_NUMBER: fake
      PERSON: hash
      US_SSN: redact
```

#### 2. Replication Configuration (required)

```yaml
replication:
  # Default settings for all collections
  defaults:
    replicate_all: true
    batch_size: 1000
    max_parallel_collections: 5
    write_disposition: merge

  # Per-collection configuration
  collections:
    users:
      pii_fields:
        email: fake
        phone: fake
        ssn: redact
      cursor_field: "meta.updatedAt"
      write_disposition: merge
      primary_key: _id
```

**PII Anonymization Operators:**

See [docs/presidio.md](../../docs/presidio.md) for comprehensive documentation.

**Built-in Presidio operators:**
- `replace` - Replace with fixed value (e.g., "ANONYMOUS")
- `redact` - Complete redaction (empty string)
- `mask` - Replace with asterisks (configurable)
- `hash` - SHA-256 hashing (random salt)
- `encrypt` / `decrypt` - AES encryption
- `keep` - Keep original value

**Custom operators:**
- `fake_email`, `fake_name`, `fake_phone`, `fake_address` - Realistic fake data via Mimesis
- `fake_ssn`, `fake_credit_card`, `fake_iban`, `fake_us_bank_account` - Financial data
- `stripe_testing_cc` - Stripe test credit card numbers
- `smart_redact` - Format-preserving redaction (emails, SSN, phone, IP, URLs)

**Schema Configuration (for cascade replication):**

```yaml
replication:
  schema:
    - parent: customers
      child: orders
      parent_field: _id
      child_field: customer_id

    - parent: orders
      child: order_items
      parent_field: _id
      child_field: order_id
```

Define parent-child relationships for cascade replication. Use with `--ids` or `--query` options to replicate related documents across collections.

---

## Typical Workflow

### 1. Initialize a new job (Recommended)

Run the interactive wizard to set up your job:

```bash
mongorep init prod
```

The wizard will:
- Prompt for source and destination MongoDB URIs
- Validate connections
- Configure collection discovery and PII detection settings
- Generate configuration file
- Display environment variables to add to `.env`

**OR manually set up environment variables:**

Create a `.env` file or export variables:

```bash
export MONGOREP_PROD_ENABLED=true
export MONGOREP_PROD_SOURCE_URI=mongodb://localhost:27017/prod_db
export MONGOREP_PROD_DESTINATION_URI=mongodb://localhost:27017/analytics_db
export MONGOREP_PROD_CONFIG_PATH=config/prod_config.yaml
```

### 2. Scan for collections and PII (Optional)

If you want to update the configuration with automatic PII detection:

```bash
mongorep scan prod --sample-size 1000 --confidence 0.85
```

This generates:
- `config/prod_config.yaml` - Configuration file (updated)
- `config/prod_pii_report.md` - Detailed PII analysis report

**Note:** If you used `mongorep init`, a basic configuration is already created. The `scan` command will enhance it with PII detection results.

### 3. Review and adjust configuration

```bash
# Review PII findings
cat config/prod_pii_report.md

# Edit configuration if needed
vim config/prod_config.yaml
```

### 4. Test with dry-run

```bash
mongorep run prod --dry-run
```

### 5. Execute replication

```bash
mongorep run prod
```

---

## Multiple Jobs

You can configure multiple jobs for different replication scenarios:

```bash
# Job 1: Production to Analytics
MONGOREP_PROD_TO_ANALYTICS_ENABLED=true
MONGOREP_PROD_TO_ANALYTICS_SOURCE_URI=mongodb://prod:27017/prod_db
MONGOREP_PROD_TO_ANALYTICS_DESTINATION_URI=mongodb://analytics:27017/analytics_db
MONGOREP_PROD_TO_ANALYTICS_CONFIG_PATH=config/prod_to_analytics.yaml

# Job 2: Production to Backup
MONGOREP_PROD_TO_BACKUP_ENABLED=true
MONGOREP_PROD_TO_BACKUP_SOURCE_URI=mongodb://prod:27017/prod_db
MONGOREP_PROD_TO_BACKUP_DESTINATION_URI=mongodb://backup:27017/backup_db
MONGOREP_PROD_TO_BACKUP_CONFIG_PATH=config/prod_to_backup.yaml

# Run them
mongorep scan prod_to_analytics
mongorep run prod_to_analytics

mongorep scan prod_to_backup
mongorep run prod_to_backup
```

---

## Features

### Interactive Mode

Use `--interactive` to select collections with a checkbox UI:

```bash
mongorep scan prod --interactive
mongorep run prod --interactive
```

Navigate with arrow keys, select with Space, confirm with Enter.

### PII Detection

Automatically detects:
- Email addresses
- Phone numbers
- Person names
- Social Security Numbers (US)
- Credit card numbers
- IBAN codes
- IP addresses
- URLs

Uses Presidio NLP for high-accuracy detection with configurable confidence threshold.

### Progress Tracking

Real-time progress bars show:
- Collection sampling progress
- PII analysis progress
- Replication progress

### Rich Console Output

Color-coded output with:
- Success messages (green)
- Warnings (yellow)
- Errors (red)
- Info messages (blue)
- Summary panels

---

## Advanced Usage

### Cascade Replication

Replicate related documents across collections based on defined relationships.

**Define relationships in configuration:**

```yaml
replication:
  schema:
    - parent: customers
      child: orders
      parent_field: _id
      child_field: customer_id

    - parent: orders
      child: order_items
      parent_field: _id
      child_field: order_id
```

**Replicate by specific IDs:**

```bash
# Replicate specific customers and all related orders, order_items
mongorep run prod --ids customers=507f1f77bcf86cd799439011

# Multiple IDs
mongorep run prod --ids customers=507f1f77bcf86cd799439011,507f191e810c19729de860ea
```

**Replicate by query:**

```bash
# Replicate customers matching query and all related data
mongorep run prod --query customers='{"plan": "Basic"}'

# Complex queries
mongorep run prod --query customers='{"status": "active", "createdAt": {"$gte": "2024-01-01"}}'
```

The tool will:
1. Find documents in root collection matching filter
2. Find related documents in child collections
3. Cascade through entire relationship chain
4. Replicate all matching documents

### Collection Filtering

Filter collections using patterns in configuration:

```yaml
scan:
  discovery:
    include_patterns:
      - "^users"      # All collections starting with "users"
      - "^orders"     # All collections starting with "orders"
    exclude_patterns:
      - "^system\\."  # Exclude system collections
      - "^tmp_"       # Exclude temporary collections
      - "^_dlt_"      # Exclude DLT metadata
```

### Incremental Replication

Use cursor fields for incremental updates:

```yaml
collections:
  users:
    cursor_field: "meta.updatedAt"  # Track by update timestamp
    write_disposition: merge         # Merge with existing data
```

### Parallel Processing

Control parallelism for performance tuning:

```bash
# More parallel workers (faster, more memory)
mongorep run prod --parallel 10

# Fewer parallel workers (slower, less memory)
mongorep run prod --parallel 2
```

### Batch Size Tuning

Adjust batch size based on document size:

```bash
# Large batches for small documents
mongorep run prod --batch-size 5000

# Small batches for large documents
mongorep run prod --batch-size 100
```

---

## Troubleshooting

### Job not found

```bash
✗ Job 'prod' not found. Available jobs:
```

**Solution:** Set environment variables:
```bash
export MONGOREP_PROD_ENABLED=true
export MONGOREP_PROD_SOURCE_URI=mongodb://...
```

### Config file not found

```bash
✗ Config file not found: config/prod_config.yaml
ℹ Run 'mongorep init prod' to generate the config file.
```

**Solution:** Run `mongorep init prod` first or create the config manually.

### No collections selected (interactive mode)

```bash
⚠ No collections selected. Exiting.
```

**Solution:** Use Space to select at least one collection, then press Enter.

### Connection errors

```bash
✗ Failed to connect to MongoDB
```

**Solution:**
- Check MongoDB URI is correct
- Ensure MongoDB is running
- Verify network connectivity
- Check authentication credentials

---

## Help & Documentation

```bash
# General help
mongorep--help

# Command-specific help
mongorep scan --help
mongorep run --help
```

For more information:
- **Environment Examples**: `.env.example`
- **Configuration Schema**: See above
