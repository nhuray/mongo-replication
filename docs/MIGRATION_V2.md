# Migration Guide: v1.x to v2.0.0

This guide helps you upgrade from the v1.x configuration format to the unified transformation pipeline introduced in v2.0.0.

## Overview

**Version 2.0.0 introduces breaking changes** by replacing three separate configuration sections with a unified `transforms` pipeline:

| **v1.x (Deprecated)** | **v2.0.0 (New)** | **Purpose** |
|----------------------|------------------|-------------|
| `fields_exclude` | `remove_field` transform | Remove unwanted fields |
| `pii_anonymized_fields` | `anonymize` transform | PII anonymization |
| `field_transforms` | `regex_replace` transform | Regex-based transformations |

### Why the Change?

The unified transformation system provides:

✅ **Predictable Execution Order** - Transforms execute sequentially in configuration order
✅ **More Transform Types** - 7 transform types (was 3 separate systems)
✅ **Conditional Execution** - Apply transforms based on field conditions
✅ **Template Syntax** - Reference other fields, use `$now`, `$null`
✅ **Consistent Configuration** - Single section for all transformations
✅ **Better Maintainability** - Single engine, not 3 fragmented systems

## Quick Migration

### Step 1: Update Configuration Structure

**Before (v1.x):**
```yaml
collections:
  users:
    fields_exclude:
      - internal_notes
      - debug_info

    pii_anonymized_fields:
      email: fake
      ssn: redact

    field_transforms:
      - field: phone
        type: regex_replace
        pattern: "\\D"
        replacement: ""
```

**After (v2.0.0):**
```yaml
collections:
  users:
    transforms:
      # Field removal (was fields_exclude)
      - type: remove_field
        field: internal_notes

      - type: remove_field
        field: debug_info

      # Regex transformations (was field_transforms)
      - type: regex_replace
        field: phone
        pattern: "\\D"
        replacement: ""

      # PII anonymization (was pii_anonymized_fields)
      - type: anonymize
        field: email
        operator: fake

      - type: anonymize
        field: ssn
        operator: redact
```

### Step 2: Test Your Configuration

After migrating, test with a small dataset:

```bash
# Dry run to validate configuration
mongo-replication run my_job --dry-run

# Run on a single collection
mongo-replication run my_job --collections users --limit 100
```

## Detailed Migration Examples

### Example 1: Field Exclusion

**Before (v1.x):**
```yaml
collections:
  orders:
    fields_exclude:
      - internal_notes
      - debug_trace
      - temp_data
```

**After (v2.0.0):**
```yaml
collections:
  orders:
    transforms:
      - type: remove_field
        field: internal_notes

      - type: remove_field
        field: debug_trace

      - type: remove_field
        field: temp_data
```

**With Conditional Removal (New Feature!):**
```yaml
collections:
  orders:
    transforms:
      # Only remove internal_notes for non-admin orders
      - type: remove_field
        field: internal_notes
        condition:
          field: user_role
          operator: not_equals
          value: "admin"
```

### Example 2: PII Anonymization

**Before (v1.x):**
```yaml
collections:
  customers:
    pii_anonymized_fields:
      email: fake
      phone: redact
      ssn: hash
      credit_card: null
      "address.street": fake_address
```

**After (v2.0.0):**
```yaml
collections:
  customers:
    transforms:
      - type: anonymize
        field: email
        operator: fake

      - type: anonymize
        field: phone
        operator: redact

      - type: anonymize
        field: ssn
        operator: hash

      - type: anonymize
        field: credit_card
        operator: "null"

      - type: anonymize
        field: "address.street"
        operator: fake_address
```

**With Conditional Anonymization (New Feature!):**
```yaml
collections:
  customers:
    transforms:
      # Only anonymize email for non-test accounts
      - type: anonymize
        field: email
        operator: fake
        condition:
          field: is_test_account
          operator: not_equals
          value: true
```

### Example 3: Field Transformations

**Before (v1.x):**
```yaml
collections:
  users:
    field_transforms:
      - field: phone
        type: regex_replace
        pattern: "\\D"
        replacement: ""

      - field: email
        type: regex_replace
        pattern: ".*@(.*)$"
        replacement: "\\1"
```

**After (v2.0.0):**
```yaml
collections:
  users:
    transforms:
      - type: regex_replace
        field: phone
        pattern: "\\D"
        replacement: ""

      # Extract domain to a NEW field
      - type: regex_replace
        field: email
        pattern: ".*@(.*)$"
        replacement: "\\1"
```

### Example 4: Combined Transformations

**Before (v1.x):**
```yaml
collections:
  orders:
    # Order was: field_transforms → PII → exclusions (fixed order)

    field_transforms:
      - field: total_amount
        type: regex_replace
        pattern: "\\d+"
        replacement: "0"

    pii_anonymized_fields:
      customer_email: fake
      "billing.card_number": redact

    fields_exclude:
      - internal_notes
      - _temp
```

**After (v2.0.0):**
```yaml
collections:
  orders:
    transforms:
      # You control the order! Transforms execute sequentially.

      # 1. First: Modify fields
      - type: regex_replace
        field: total_amount
        pattern: "\\d+"
        replacement: "0"

      # 2. Then: Anonymize PII
      - type: anonymize
        field: customer_email
        operator: fake

      - type: anonymize
        field: "billing.card_number"
        operator: redact

      # 3. Finally: Remove fields
      - type: remove_field
        field: internal_notes

      - type: remove_field
        field: _temp
```

## New Features in v2.0.0

The unified transformation system introduces powerful new capabilities:

### 1. Additional Transform Types

Beyond the v1.x equivalents, you now have:

```yaml
transforms:
  # Add a new field
  - type: add_field
    field: processed_at
    value: "$now"  # Current timestamp

  # Set/overwrite a field
  - type: set_field
    field: status
    value: "migrated"

  # Rename a field
  - type: rename_field
    from_field: old_name
    to_field: new_name

  # Copy a field
  - type: copy_field
    from_field: email
    to_field: email_backup
```

### 2. Template Syntax

Reference other fields and use special values:

```yaml
transforms:
  # Copy from another field
  - type: set_field
    field: backup_email
    value: "$email"

  # Set to current timestamp
  - type: add_field
    field: migrated_at
    value: "$now"

  # Set to null
  - type: set_field
    field: deprecated_field
    value: "$null"

  # Combine fields (with regex)
  - type: set_field
    field: full_name
    value: "$first_name $last_name"
```

### 3. Conditional Execution

Apply transforms only when conditions are met:

```yaml
transforms:
  # Set discount only for high-value orders
  - type: set_field
    field: discount
    value: "10%"
    condition:
      field: total
      operator: greater_than
      value: 1000

  # Anonymize email only in production
  - type: anonymize
    field: email
    operator: fake
    condition:
      field: environment
      operator: equals
      value: "production"

  # Complex conditions
  - type: remove_field
    field: debug_info
    condition:
      field: user_role
      operator: in
      value: ["customer", "guest"]
```

**Available Operators:**
- Comparison: `equals`, `not_equals`, `greater_than`, `less_than`, `greater_or_equal`, `less_or_equal`
- Collection: `in`, `not_in`
- Existence: `exists`, `not_exists`
- String: `regex_match`, `starts_with`, `ends_with`, `contains`

### 4. Error Handling

Configure how transform errors are handled:

```yaml
replication:
  defaults:
    transform_error_mode: skip  # or "fail"
```

- **`skip`** (default): Log the error, skip the document, continue replication
- **`fail`**: Stop replication on first transform error

## Migration Checklist

Use this checklist to ensure a complete migration:

- [ ] **Backup your configuration** - Save a copy of your v1.x config
- [ ] **Update `fields_exclude`** - Convert to `remove_field` transforms
- [ ] **Update `pii_anonymized_fields`** - Convert to `anonymize` transforms
- [ ] **Update `field_transforms`** - Convert to `regex_replace` transforms
- [ ] **Review transform order** - Ensure logical execution sequence
- [ ] **Test with dry run** - `mongo-replication run --dry-run`
- [ ] **Test with small dataset** - `--limit 100 --collections <single_collection>`
- [ ] **Check logs** - Verify no transform errors
- [ ] **Validate output** - Inspect destination data
- [ ] **Update documentation** - Update internal docs/runbooks
- [ ] **Update CI/CD** - Update deployment scripts if needed

## Backward Compatibility

**⚠️ NO BACKWARD COMPATIBILITY**: v2.0.0 completely removes the old configuration fields.

If you try to use v1.x configuration with v2.0.0, you will get validation errors:

```
Configuration Error: Unknown field 'fields_exclude' in collection config
Configuration Error: Unknown field 'pii_anonymized_fields' in collection config
Configuration Error: Unknown field 'field_transforms' in collection config
```

You **must** migrate your configuration to the new `transforms` format.

## Automation Script

For large configurations with many collections, use this script to automate the migration:

```python
#!/usr/bin/env python3
"""
Migrate v1.x configuration to v2.0.0 format.
Usage: python migrate_config.py old_config.yaml > new_config.yaml
"""

import sys
import yaml

def migrate_collection(collection_config):
    """Migrate a single collection's config to v2.0.0 format."""
    transforms = []

    # 1. Migrate field_transforms (keep original order)
    if 'field_transforms' in collection_config:
        for transform in collection_config['field_transforms']:
            transforms.append({
                'type': 'regex_replace',
                'field': transform['field'],
                'pattern': transform['pattern'],
                'replacement': transform['replacement']
            })
        del collection_config['field_transforms']

    # 2. Migrate pii_anonymized_fields
    if 'pii_anonymized_fields' in collection_config:
        for field, operator in collection_config['pii_anonymized_fields'].items():
            transforms.append({
                'type': 'anonymize',
                'field': field,
                'operator': operator
            })
        del collection_config['pii_anonymized_fields']

    # 3. Migrate fields_exclude
    if 'fields_exclude' in collection_config:
        for field in collection_config['fields_exclude']:
            transforms.append({
                'type': 'remove_field',
                'field': field
            })
        del collection_config['fields_exclude']

    # Add transforms if any exist
    if transforms:
        collection_config['transforms'] = transforms

    return collection_config

def migrate_config(config):
    """Migrate entire configuration file."""
    if 'replication' in config and 'collections' in config['replication']:
        for name, collection_config in config['replication']['collections'].items():
            config['replication']['collections'][name] = migrate_collection(collection_config)

    return config

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python migrate_config.py old_config.yaml", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        config = yaml.safe_load(f)

    migrated = migrate_config(config)
    print(yaml.dump(migrated, default_flow_style=False, sort_keys=False))
```

**Usage:**
```bash
# Backup original
cp config/my_job.yaml config/my_job.yaml.v1.bak

# Migrate
python migrate_config.py config/my_job.yaml > config/my_job_v2.yaml

# Review changes
diff config/my_job.yaml config/my_job_v2.yaml

# Replace when ready
mv config/my_job_v2.yaml config/my_job.yaml
```

## Troubleshooting

### Issue: "Unknown field 'pii_anonymized_fields'"

**Cause:** Using v1.x configuration with v2.0.0

**Solution:** Migrate to `transforms` format (see examples above)

### Issue: Transforms not executing in expected order

**Cause:** Transforms execute sequentially in configuration order

**Solution:** Reorder transforms in your YAML to match desired execution order

### Issue: PII not being anonymized

**Cause:** The `anonymize` transform requires the `operator` field (not just a strategy name)

**Solution:**
```yaml
# ❌ Wrong
- type: anonymize
  field: email
  value: fake  # This sets field to literal "fake"

# ✅ Correct
- type: anonymize
  field: email
  operator: fake  # This uses the fake operator
```

### Issue: Field not being removed

**Cause:** Using wrong transform type or field doesn't exist

**Solution:**
```yaml
# ✅ Remove a field
- type: remove_field
  field: unwanted_field

# Not:
- type: set_field
  field: unwanted_field
  value: "$null"  # This sets to null, doesn't remove
```

## Support

For questions or issues during migration:

- **Documentation:** [Configuration Guide](configuration.md)
- **Examples:** See `docs/configuration.md` for complete examples
- **Issues:** https://github.com/your-org/mongo-replication/issues

## Version History

- **v2.0.0** (Current) - Unified transformation pipeline
- **v1.x** (Deprecated) - Fragmented transformation system
