# MongoDB Replication CLI

Command-line interface for MongoDB replication with PII detection and anonymization.

## Quick Start

```bash
# Install
pip install -e .

# Scan collections and detect PII
mongorep scan prod --sample-size 1000

# Run replication
mongorep run prod
```

---

## Commands

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
```

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

**PII Anonymization Strategies:**
- `fake` - Replace with realistic fake data (via Faker library)
- `hash` - One-way hash (consistent, can't reverse)
- `redact` - Replace with `***REDACTED***`
- `mask` - Partial masking (e.g., `***-**-1234`)

---

## Typical Workflow

### 1. Set up environment variables

Create a `.env` file or export variables:

```bash
export MONGOREP_PROD_ENABLED=true
export MONGOREP_PROD_SOURCE_URI=mongodb://localhost:27017/prod_db
export MONGOREP_PROD_DESTINATION_URI=mongodb://localhost:27017/analytics_db
export MONGOREP_PROD_CONFIG_PATH=config/prod_config.yaml
```

### 2. Scan for collections and PII

```bash
mongorep scan prod --sample-size 1000 --confidence 0.85
```

This generates:
- `config/prod_config.yaml` - Configuration file
- `config/prod_pii_report.md` - Detailed PII analysis report

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
ℹ Run 'mongorep scan prod' to generate the config file.
```

**Solution:** Run `mongorep scan prod` first or create the config manually.

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
