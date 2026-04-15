# Presidio PII Detection and Anonymization

This document explains how to configure and use Microsoft Presidio for PII (Personally Identifiable Information) detection and anonymization in the MongoDB replication tool.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Anonymization Operators](#anonymization-operators)
  - [Built-in Presidio Operators](#built-in-presidio-operators)
  - [Custom Operators](#custom-operators)
- [Default Entity Strategies](#default-entity-strategies)
- [Configuration](#configuration)
  - [Basic Configuration](#basic-configuration)
  - [Custom Presidio YAML Configuration](#custom-presidio-yaml-configuration)
- [Usage Examples](#usage-examples)
- [Advanced Topics](#advanced-topics)

## Overview

The tool uses [Microsoft Presidio](https://microsoft.github.io/presidio/) for PII detection and anonymization:

- **Detection Phase** (during `scan` command): Analyzes sample documents to detect PII fields
- **Anonymization Phase** (during `run` command): Applies anonymization operators to PII fields during replication

### Key Features

- **Automatic PII Detection**: Uses Presidio's analyzer to detect 20+ entity types (emails, SSN, credit cards, etc.)
- **YAML-Configured Operators**: Anonymization strategies configured via YAML for easy customization
- **Custom Operators**: 10 custom operators for realistic fake data generation and smart redaction
- **Field-Level Anonymization**: Applies operators to individual MongoDB fields (not full-text documents)
- **Extensible**: Add custom entity recognizers and anonymization operators

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SCAN PHASE                              │
├─────────────────────────────────────────────────────────────────┤
│  Sample Documents → Presidio Analyzer → Detected PII Fields    │
│                                                                 │
│  Output: Field mappings (email → EMAIL_ADDRESS, ssn → US_SSN)  │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                      REPLICATION PHASE                          │
├─────────────────────────────────────────────────────────────────┤
│  Source Document → PresidioAnonymizer → Anonymized Document    │
│                                                                 │
│  - Loads operator configs from presidio.yaml                   │
│  - Applies field-level anonymization using AnonymizerEngine    │
│  - Supports manual field overrides                             │
└─────────────────────────────────────────────────────────────────┘
```

### Components

1. **PresidioAnalyzer** (`presidio_analyzer.py`)
   - Wraps Presidio's `AnalyzerEngine`
   - Detects PII in MongoDB documents
   - Returns field-to-entity-type mappings

2. **PresidioAnonymizer** (`presidio_anonymizer.py`)
   - Wraps Presidio's `AnonymizerEngine`
   - Loads operator configs from YAML
   - Applies anonymization to detected PII fields

3. **Custom Operators** (`custom_operators.py`)
   - Extend Presidio's `Operator` base class
   - Implement custom anonymization logic

4. **PresidioConfig** (`presidio_config.py`)
   - Parses `presidio.yaml` configuration
   - Converts YAML to `OperatorConfig` objects

## Anonymization Operators

### Built-in Presidio Operators

Presidio provides these standard operators:

#### `replace`
Replaces PII with a fixed value.

```yaml
operator: replace
params:
  new_value: "REDACTED"
```

**Use cases:**
- Replace names with "ANONYMOUS"
- Replace locations with "UNKNOWN"

#### `redact`
Removes the PII entirely (replaces with empty string).

```yaml
operator: redact
params: {}
```

**Use cases:**
- Complete removal of sensitive data
- When PII field is not needed in destination

#### `mask`
Masks characters with a specified character (default: `*`).

```yaml
operator: mask
params:
  masking_char: "*"
  chars_to_mask: 10      # Number of characters to mask
  from_end: false        # Mask from beginning (true = mask from end)
```

**Use cases:**
- Show last 4 digits of SSN: `***-**-6789`
- Show last 4 digits of credit card: `************1234`

#### `hash`
Hashes the PII using SHA-256 or SHA-512.

```yaml
operator: hash
params:
  hash_type: sha256  # or sha512
```

**Note:** Presidio's hash operator uses a **random salt** by default, so the same input produces different hashes on each run. For deterministic hashing, consider using a custom operator.

**Use cases:**
- When referential integrity is needed (same ID → same hash)
- Irreversible anonymization

#### `encrypt` / `decrypt`
Encrypts/decrypts PII using AES encryption.

```yaml
operator: encrypt
params:
  key: "your-encryption-key"  # 128, 192, or 256 bits
```

**Use cases:**
- When PII needs to be reversible
- Secure storage with decryption capability

#### `keep`
Keeps the PII unchanged.

```yaml
operator: keep
params: {}
```

**Use cases:**
- Explicitly mark fields as non-sensitive
- Override auto-detected PII

### Custom Operators

The tool provides 10 custom operators for enhanced anonymization:

#### `fake_email`
Generates realistic fake email addresses using Mimesis.

```yaml
operator: fake_email
params: {}
```

**Output examples:**
- `john.doe@example.com` → `alice.johnson@gmail.com`
- `admin@company.org` → `bob.smith@yahoo.com`

**Use cases:**
- Maintain email format for testing
- Generate realistic test data

#### `fake_name`
Generates realistic fake names using Mimesis.

```yaml
operator: fake_name
params: {}
```

**Output examples:**
- `John Doe` → `Emily Martinez`
- `Jane Smith` → `Michael Brown`

#### `fake_phone`
Generates realistic fake phone numbers using Mimesis.

```yaml
operator: fake_phone
params: {}
```

**Output examples:**
- `555-123-4567` → `202-555-0142`
- `+1-555-123-4567` → `+1-310-555-0199`

#### `fake_address`
Generates realistic fake addresses using Mimesis.

```yaml
operator: fake_address
params: {}
```

**Output examples:**
- `123 Main St, New York, NY 10001` → `456 Oak Ave, Los Angeles, CA 90001`

#### `fake_ssn`
Generates realistic fake US Social Security Numbers using Mimesis.

```yaml
operator: fake_ssn
params: {}
```

**Output examples:**
- `123-45-6789` → `987-65-4321`

#### `fake_credit_card`
Generates realistic fake credit card numbers using Mimesis.

```yaml
operator: fake_credit_card
params: {}
```

**Output examples:**
- `4532-1234-5678-9010` → `5105-1051-0510-5100`

#### `fake_iban`
Generates realistic fake IBAN (International Bank Account Numbers) using Mimesis.

```yaml
operator: fake_iban
params: {}
```

**Output examples:**
- `GB82 WEST 1234 5698 7654 32` → `DE89 3704 0044 0532 0130 00`

#### `fake_us_bank_account`
Generates realistic fake US bank account numbers using Mimesis.

```yaml
operator: fake_us_bank_account
params: {}
```

**Output examples:**
- `123456789` → `987654321`

#### `smart_mask`
#### `smart_mask`
Format-preserving masking that maintains structure while hiding sensitive data.

```yaml
operator: smart_mask
params: {}
```

**Behavior by format:**

- **Email**: Preserves domain, shows partial local part
  - `john.doe@example.com` → `jo****@example.com`

- **SSN**: Shows last 4 digits
  - `123-45-6789` → `***-**-6789`

- **Phone**: Shows last 4 digits
  - `555-123-4567` → `***-***-4567`

- **IP Address**: Shows first and last octet
  - `192.168.1.100` → `192.***.***. 100`

- **URL**: Shows protocol and partial path
  - `https://example.com/user/secret` → `https://***.com/***`

- **Generic**: Shows first 3 and last 3 characters
  - `sensitive-data-here` → `sen***ere`

**Use cases:**
- Preserve data format for debugging
- Maintain partial visibility for support teams
- Balance between privacy and utility

## Default Entity Strategies

The bundled `presidio.yaml` configures these default anonymization strategies:

```yaml
EMAIL_ADDRESS: smart_mask      # Preserves domain: jo****@example.com
PERSON: replace                # Replaces with "ANONYMOUS"
PHONE_NUMBER: mask             # Shows last 4 digits: ***-***-4567
LOCATION: mask                 # Partial masking: New Y***
US_SSN: mask                   # Shows last 4 digits: ***-**-6789
SSN: mask                      # Shows last 4 digits: ***-**-6789
CREDIT_CARD: hash              # SHA-256 hash (random salt)
IBAN_CODE: hash                # SHA-256 hash (random salt)
CRYPTO: hash                   # SHA-256 hash (random salt)
US_PASSPORT: mask              # Partial masking
US_BANK_ACCOUNT: mask          # Partial masking
US_DRIVER_LICENSE: mask        # Partial masking
UK_NHS: mask                   # Partial masking
DATE_TIME: mask                # Partial masking
IP_ADDRESS: mask               # Partial masking
URL: mask                      # Partial masking
DEFAULT: redact                # Fallback: complete redaction
```

### Strategy Aliases

The configuration also provides convenient aliases for common strategies:

```yaml
custom_strategy_aliases:
  # Fake data generation
  fake: fake_email
  fake_data: fake_email
  synthetic: fake_email

  # Redaction strategies
  redact: smart_redact
  partial_redact: smart_redact

  # Hashing
  hash: hash
  sha256: hash

  # Masking
  mask: mask
  obscure: mask

  # Removal
  null: redact
  remove: redact
  delete: redact
```

**Usage example:**
```yaml
pii_anonymized_fields:
  email: fake           # Alias for fake_email
  ssn: partial_redact   # Alias for smart_redact
  id: hash              # Uses hash operator
```

## Configuration

### Basic Configuration

Enable PII detection in your job configuration:

```yaml
scan:
  pii_analysis:
    enabled: true
    confidence_threshold: 0.8
    entities:
      - EMAIL_ADDRESS
      - PHONE_NUMBER
      - PERSON
      - US_SSN
      - CREDIT_CARD
```

Then configure anonymization per collection:

```yaml
collections:
  users:
    pii_anonymized_fields:
      email: smart_redact      # Presidio operator
      phone: fake_phone        # Custom operator
      ssn: mask                # Presidio operator
      "address.street": fake_address  # Nested field
```

### Custom Presidio YAML Configuration

Create a custom Presidio configuration for domain-specific PII detection:

#### Step 1: Copy the default configuration

```bash
cp src/mongo_replication/config/presidio.yaml config/my_custom_presidio.yaml
```

#### Step 2: Reference it in your job config

```yaml
scan:
  pii_analysis:
    enabled: true
    presidio_config: "config/my_custom_presidio.yaml"
```

#### Step 3: Customize the YAML

A Presidio YAML configuration has three main sections:

##### 1. Recognizers (PII Detection)

Define patterns to detect custom PII types:

```yaml
recognizers:
  - name: EmployeeIdRecognizer
    supported_entity: EMPLOYEE_ID
    supported_languages:
      - en
    patterns:
      - name: employee_id_pattern
        regex: "EMP-\\d{6}"
        score: 0.9
    context:
      - "employee"
      - "emp"
      - "staff"
```

##### 2. Anonymization Operators

Configure how each entity type should be anonymized:

```yaml
anonymization_operators:
  EMPLOYEE_ID:
    operator: hash
    params:
      hash_type: sha256

  EMAIL_ADDRESS:
    operator: fake_email
    params: {}

  CUSTOM_ID:
    operator: replace
    params:
      new_value: "REDACTED_ID"
```

##### 3. Strategy Aliases

Define custom aliases for operators:

```yaml
custom_strategy_aliases:
  anonymize: fake_email
  hide: smart_redact
  secure: hash
```

## Usage Examples

### Example 1: Healthcare Data

Detect patient information and medical record numbers:

**`config/healthcare_presidio.yaml`:**
```yaml
recognizers:
  # Medical Record Number
  - name: MedicalRecordRecognizer
    supported_entity: MEDICAL_RECORD_NUMBER
    supported_languages:
      - en
    patterns:
      - name: mrn_pattern
        regex: "MRN-\\d{8}"
        score: 0.95
    context:
      - "medical"
      - "patient"
      - "mrn"
      - "record"

  # Patient ID
  - name: PatientIdRecognizer
    supported_entity: PATIENT_ID
    supported_languages:
      - en
    patterns:
      - name: patient_id_pattern
        regex: "PT\\d{10}"
        score: 0.95
    context:
      - "patient"
      - "admission"

anonymization_operators:
  MEDICAL_RECORD_NUMBER:
    operator: hash
    params:
      hash_type: sha256

  PATIENT_ID:
    operator: hash
    params:
      hash_type: sha256

  PERSON:
    operator: fake_name
    params: {}

  PHONE_NUMBER:
    operator: fake_phone
    params: {}

  US_SSN:
    operator: mask
    params:
      masking_char: "*"
      chars_to_mask: 7
      from_end: false
```

**Job configuration:**
```yaml
scan:
  pii_analysis:
    enabled: true
    presidio_config: "config/healthcare_presidio.yaml"
    entities:
      - MEDICAL_RECORD_NUMBER
      - PATIENT_ID
      - PERSON
      - PHONE_NUMBER
      - US_SSN

collections:
  patients:
    pii_anonymized_fields:
      mrn: hash
      patient_id: hash
      name: fake_name
      phone: fake_phone
      ssn: mask
```

### Example 2: Financial Data

Anonymize account numbers and customer data:

**`config/finance_presidio.yaml`:**
```yaml
recognizers:
  # Account Number
  - name: AccountNumberRecognizer
    supported_entity: ACCOUNT_NUMBER
    supported_languages:
      - en
    patterns:
      - name: account_pattern
        regex: "\\d{10,12}"
        score: 0.85
    context:
      - "account"
      - "acct"
      - "number"

anonymization_operators:
  ACCOUNT_NUMBER:
    operator: fake_us_bank_account
    params: {}

  CREDIT_CARD:
    operator: stripe_testing_cc
    params: {}

  EMAIL_ADDRESS:
    operator: fake_email
    params: {}

  IBAN_CODE:
    operator: fake_iban
    params: {}

custom_strategy_aliases:
  fake_account: fake_us_bank_account
```

**Job configuration:**
```yaml
scan:
  pii_analysis:
    enabled: true
    presidio_config: "config/finance_presidio.yaml"

collections:
  transactions:
    pii_anonymized_fields:
      account_number: fake_account    # Uses alias
      iban: fake_iban
      customer_email: fake_email
```

### Example 3: E-commerce Platform

Mix auto-detection with manual overrides:

```yaml
scan:
  pii_analysis:
    enabled: true
    confidence_threshold: 0.85
    entities:
      - EMAIL_ADDRESS
      - PHONE_NUMBER
      - PERSON
      - CREDIT_CARD

collections:
  customers:
    pii_anonymized_fields:
      # Auto-detected fields use default strategies
      # Manual overrides for specific needs
      email: fake_email              # Override default smart_mask
      phone: fake_phone              # Override default mask
      "billing.card_number": fake_credit_card  # Nested field
      "shipping.address": fake_address

  orders:
    pii_anonymized_fields:
      customer_email: fake_email
      "payment.last4": keep          # Keep last 4 digits (not sensitive)
```

## Advanced Topics

### Custom Strategy Implementation

To add a completely custom anonymization strategy, create a new operator class:

```python
from presidio_anonymizer.entities import Operator, OperatorType

class CustomOperator(Operator):
    def operate(self, text: str = None, params: Dict = None) -> str:
        """Your custom anonymization logic."""
        if text is None:
            return ""

        # Implement your logic
        return anonymized_text

    def validate(self, params: Dict = None) -> None:
        """Validate parameters."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "custom_operator"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize
```

Then add it to `CUSTOM_OPERATORS` in `custom_operators.py`.

### Field-Level vs Text-Level Anonymization

**The tool uses field-level anonymization:**
- Each MongoDB field value is treated as a separate text
- Presidio's `AnonymizerEngine.anonymize()` is called per field
- Better for structured database data

**Alternative (not used):**
- Text-level: Treat entire document as text
- Better for unstructured documents (articles, comments)

### Nested and Array Field Support

The anonymizer supports complex MongoDB structures:

**Nested fields:**
```yaml
pii_anonymized_fields:
  "user.profile.email": fake_email
  "billing.address.street": fake_address
```

**Array fields:**
```yaml
pii_anonymized_fields:
  "contacts.email": fake_email        # Applies to all array elements
  "orders.items.price": keep
```

### Performance Considerations

- **Sampling**: PII detection only analyzes sampled documents (configurable via `scan.sampling.sample_size`)
- **Batch processing**: Anonymization processes documents in batches (configurable via `replication.batch_size`)
- **Custom operators**: Some operators (like `fake_*`) use Mimesis which is fast but creates new data each time

### Troubleshooting

**High false positive rate:**
- Increase `confidence_threshold` (e.g., from 0.8 to 0.9)
- Add context words to recognizers
- Use `allowlist` to exclude known non-PII fields

**Missing PII detection:**
- Decrease `confidence_threshold`
- Add custom recognizers for domain-specific patterns
- Check entity types in `entities` list

**Anonymization errors:**
- Check operator parameters in YAML
- Verify operator names match available operators
- Review error logs for specific failures

## Resources

- **Presidio Documentation**: https://microsoft.github.io/presidio/
- **YAML Configuration Guide**: https://microsoft.github.io/presidio/samples/python/no_code_config/
- **Supported Entities**: https://microsoft.github.io/presidio/supported_entities/
- **Adding Recognizers**: https://microsoft.github.io/presidio/analyzer/adding_recognizers/
- **Default Template**: `src/mongo_replication/config/presidio.yaml`
