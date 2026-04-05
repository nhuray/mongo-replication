# MongoDB Replication Tool

A production-grade MongoDB replication tool with built-in PII redaction, parallel processing, cascade filtering, and intelligent state management.

## Features

### Core Capabilities
- **Parallel Replication**: Process multiple collections simultaneously with configurable worker pools
- **Incremental Loading**: Cursor-based state management for efficient incremental updates
- **PII Redaction**: Built-in support for detecting and anonymizing sensitive data using Microsoft Presidio
- **Cascade Filtering**: Replicate related documents across collections based on defined relationships
- **Native BSON Support**: Preserves MongoDB data types (ObjectId, Date, Decimal128, etc.)
- **Multiple Write Modes**: Support for replace, append, and merge strategies
- **Field Transformations**: Apply custom transformations to fields during replication
- **Index Management**: Automatically replicate indexes from source to destination

### State Management
- **Run Tracking**: Track job runs with comprehensive statistics and error reporting
- **Collection State**: Detailed per-collection state with cursor position tracking
- **Configurable State Collections**: Customize state collection names via configuration
- **Automatic Index Cleanup**: Handles migration from legacy state schemas

## Installation

```bash
pip install mongo-replication
```

For development installation:

```bash
git clone https://github.com/nhuray/mongo-replication.git
cd mongo-replication
uv sync
```

## Quick Start

### 1. Initialize a New Job

```bash
rep init my_job
```

This creates:
- A configuration file at `config/my_job_config.yaml`
- Environment variable template to set in `.env`

### 2. Configure Environment Variables

Add to your `.env` file:

```env
REP_MY_JOB_SOURCE_URI=mongodb://source-host:27017/source_db
REP_MY_JOB_DESTINATION_URI=mongodb://dest-host:27017/dest_db
REP_MY_JOB_CONFIG_PATH=config/my_job_config.yaml
REP_MY_JOB_ENABLED=true
```

### 3. Scan Collections (Optional)

Auto-discover collections and generate configuration:

```bash
rep scan my_job
```

### 4. Run Replication

```bash
# Replicate all configured collections
rep run my_job

# Replicate specific collections
rep run my_job --collections users,orders

# Cascade replication from a root document
rep run my_job --select customers=507f1f77bcf86cd799439011

# Interactive mode
rep run my_job --interactive

# Dry run (preview without executing)
rep run my_job --dry-run
```

## Configuration

### Basic Configuration Structure

```yaml
defaults:
  # Replication behavior
  replicate_all: true
  batch_size: 1000
  max_parallel_collections: 5
  fallback_cursor: _id
  
  # State management
  state:
    runs_collection: _rep_runs
    state_collection: _rep_state
  
  # Collection filtering
  include_patterns: []
  exclude_patterns:
    - "^system\\."
    - "^tmp_"

collections:
  users:
    cursor_field: updated_at
    write_disposition: merge
    primary_key: _id
    
  orders:
    cursor_field: created_at
    write_disposition: append
    match:
      status: { $in: ["completed", "shipped"] }
    
    # PII redaction
    pii:
      enabled: true
      fields:
        - customer_email
        - shipping_address
      detection_mode: field_name
      anonymization:
        email: mask
        address: redact
```

See [Configuration Documentation](docs/configuration.md) for complete reference.

## CLI Commands

### `init` - Initialize a New Job

```bash
rep init <job_name> [OPTIONS]

Options:
 --output  -o      PATH  Output config file path (default: config/<job>_config.yaml)                                                                                                                             │
 --help                  Show this message and exit. 
```

### `scan` - Auto-Discover Collections

```bash
rep scan <job_name> [OPTIONS]

Options:
 --output       -o      TEXT     Output path for config file (default: config/<job>_config.yaml)                                                                                                                 │
 --collections          TEXT     Comma-separated list of collections to scan (default: all)                                                                                                                      │
 --interactive  -i               Interactively select collections to scan                                                                                                                                        │
 --sample-size  -s      INTEGER  Number of documents to sample per collection (default: from config or 1000)                                                                                                     │
 --confidence   -c      FLOAT    Minimum confidence for PII detection (default: from config or 0.85)                                                                                                             │
 --language     -l      TEXT     Language for NLP analysis (default: en)                                                                                                                                         │
 --no-pii                        Skip PII analysis (only discover collections)                                                                                                                                   │
 --help                          Show this message and exit.  
```

### `run` - Execute Replication

```bash
rep run <job_name> [OPTIONS]

Options:
 --collections          TEXT     Comma-separated list of collections to replicate (default: all configured)                                                                                                      │
 --interactive  -i               Interactively select collections to replicate                                                                                                                                   │
 --dry-run                       Preview what would be replicated without executing                                                                                                                              │
 --parallel     -p      INTEGER  Maximum number of parallel collections (default: from config or 5)                                                                                                              │
 --batch-size   -b      INTEGER  Batch size for document processing                                                                                                                                              │
 --select               TEXT     Cascade replication from specific records. Format: collection=id1,id2,id3 (e.g., --select customers=507f1f77bcf86cd799439011,507f191e810c19729de860ea)                          │
 --help                          Show this message and exit.  
```

## Advanced Usage

### Cascade Replication

Replicate related documents across collections:

```bash
# Replicate customer and all related orders, invoices, etc.
rep run my_job --select customers=507f1f77bcf86cd799439011
```

Define schema relationships in configuration:

```yaml
replication:
   schema:
     - source_collection: customers
       target_collection: orders
       source_field: _id
       target_field: customer_id
       
     - source_collection: orders
       target_collection: order_items
       source_field: _id
       target_field: order_id
```

### PII Redaction

Built-in PII detection and anonymization:

```yaml
replication:
   collections:
     users:
       pii:
         enabled: true
         fields:
           - email
           - phone
           - ssn
         detection_mode: field_name  # or 'content'
         anonymization:
           email: mask          # user@example.com → u***@example.com
           phone: hash          # Hash the value
           ssn: redact          # Replace with [REDACTED]
           address: replace     # Replace with fake data
```

### Field Transformations

Apply custom transformations:

```yaml
replication:
   collections:
     orders:
       field_transforms:
         - field: total_amount
           operation: multiply
           value: 1.1  # Add 10% tax
           
         - field: status
           operation: map
           mapping:
             0: pending
             1: completed
             2: cancelled
```

### Field Exclusion

Exclude sensitive fields:

```yaml
replication:
   collections:
     users:
       fields_exclude:
         - password_hash
         - internal_notes
         - legacy_data
```

## State Management

The tool maintains two state collections:

### `_rep_runs` - Job Run Tracking
Tracks each replication job run with:
- Status (running, completed, failed)
- Timestamps and duration
- Document/collection statistics
- Error summaries

### `_rep_state` - Collection State
Per-collection state including:
- Last cursor position for incremental loading
- Processing status
- Error details
- Link to parent run


## Programmatic Usage

Use as a Python library:

```python
from mongo_replication import (
    ConnectionManager,
    ReplicationOrchestrator,
    load_replication_config
)

# Load configuration
config = load_replication_config("config/my_job_config.yaml")

# Setup connections
conn_mgr = ConnectionManager(
    source_uri="mongodb://source:27017/source_db",
    dest_uri="mongodb://dest:27017/dest_db"
)

# Create orchestrator
orchestrator = ReplicationOrchestrator(
    connection_manager=conn_mgr,
    config=config
)

# Execute replication
result = orchestrator.replicate()

print(f"Collections processed: {result.total_collections_processed}")
print(f"Documents replicated: {result.total_documents_processed}")
print(f"Duration: {result.total_duration_seconds}s")
```

## Architecture

See [Technical Design Documentation](docs/technical-design.md) for:
- System architecture overview
- State management design
- Parallel processing model
- PII detection pipeline
- Extension points

## Performance Tips

1. **Batch Size**: Adjust based on document size and network latency
   - Large documents: 100-500
   - Small documents: 1000-5000

2. **Parallel Collections**: Balance based on available resources
   - Local replication: 5-10
   - Network replication: 3-5

3. **Indexes**: Ensure cursor fields are indexed on source collections

4. **Incremental Loading**: Use timestamp-based cursor fields for optimal performance

## Troubleshooting

### Common Issues

**State collection conflicts**
```bash
# Reset state for a collection
rep run my_job --reset users
```

**Performance issues**
```bash
# Reduce parallel processing
rep run my_job --max-parallel 2 --batch-size 500
```

**Connection timeouts**
- Increase `serverSelectionTimeoutMS` in connection URI
- Check network connectivity and firewall rules

### Debug Logging

Enable verbose logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/nhuray/mongo-replication/issues)
- **Documentation**: [Full Documentation](https://github.com/nhuray/mongo-replication#readme)

## Acknowledgments

Built with:
- [PyMongo](https://pymongo.readthedocs.io/) - MongoDB Python driver
- [Typer](https://typer.tiangolo.com/) - CLI framework
- [Rich](https://rich.readthedocs.io/) - Terminal formatting
- [Presidio](https://microsoft.github.io/presidio/) - PII detection and anonymization
