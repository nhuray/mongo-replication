# CHANGELOG


## v1.0.2 (2026-04-16)

### Bug Fixes

* fix: Include documents with missing cursor field in merge queries (#21)

fix: include documents with missing cursor field in merge queries

When replicating with write_disposition=merge and a cursor_field that
doesn't exist in some documents, those documents were being excluded
from replication, causing data loss.

This fix updates the MongoDB query to use $or with $exists:false to
include documents where the cursor field is missing OR greater than
the last cursor value.

Changes:
- Updated _build_query() to use $or with $exists:false
- Updated in-loop queries in _replicate_merge() and _replicate_append()
- Added 3 new tests for cursor_initial_value and nested fields
- Updated 3 existing tests to match new query structure

Example query:
{$or: [{"meta.updatedAt": {$exists: false}}, {"meta.updatedAt": {$gt: ISODate(...)}}]}

All 435 tests passing. ([`d352c1a`](https://github.com/nhuray/mongo-replication/commit/d352c1aa0714e858a1dfc0ef334402945533283d))


## v1.0.1 (2026-04-15)

### Bug Fixes

* fix: update FakeEmailOperator to generate unique email addresses ([`767ba25`](https://github.com/nhuray/mongo-replication/commit/767ba25bca5f383f0e8392bd629775715fbc987e))

### Chores

* chore(release): 1.0.1 [skip ci] ([`9828958`](https://github.com/nhuray/mongo-replication/commit/9828958c7235ad9fa0d176128400a9a8d57345ee))


## v1.0.0 (2026-04-15)

### Breaking

* feat!: Change configuration and enhance Presidio anonymizers (#20)

* chore: minor change in presidio.yaml

* feat: create custom operators

* feat: update config models.py to support new pii_anonymization

* feat: update scan.py, pii_analyzer.py, orchestrator.py

* feat: update scan_report.py and template

* feat: fix some bugs running manual tests

* feat: add entity_type to all operator examples and remove deprecated operators

- Add entity_type field to all anonymizer examples in presidio.yaml (~86 additions)
  - Required by init command wizard (step 6) to show anonymization strategies
  - Ensures consistent example format across all mask and fake operators

- Remove smart_redact operator (deprecated, replaced by smart_mask)
  - Delete SmartRedactOperator class from custom_operators.py (~160 lines)
  - Remove from CUSTOM_OPERATORS registry
  - Update all references in docstrings and tests to use smart_mask

- Remove stripe_testing_cc operator (test-only utility)
  - Delete StripeTestingCCOperator class from custom_operators.py (~55 lines)
  - Remove from CUSTOM_OPERATORS registry

- Update documentation to reflect current operators
  - Replace smart_redact → smart_mask in all docs and examples
  - Remove stripe_testing_cc references from README, docs/presidio.md, docs/configuration.md
  - Keep historical references in CHANGELOG.md and old scan reports

- Add comprehensive test coverage for presidio_config.py
  - Add 8 new tests for get_supported_entity_types, get_operator_examples, get_operators_for_entity_type
  - Ensure config registry functions work correctly with entity_type field

All 73 PII-related tests pass

* feat: add entity_type filter to get_operator_examples function

- Add optional entity_type parameter to get_operator_examples() in presidio_config.py
- Allows filtering examples by specific entity type (e.g., EMAIL_ADDRESS, PHONE_NUMBER)
- Useful for smart operators that support multiple entity types (smart_mask, smart_fake)
- Add comprehensive test coverage for entity_type filtering
- All 74 PII-related tests pass

* feat: add complete examples for smart_mask and smart_fake operators

- Add missing 7 examples for smart_mask operator (now has all 10 entity types)
  - Added: CREDIT_CARD, IP_ADDRESS, IBAN_CODE, PERSON, LOCATION, US_BANK_ACCOUNT, CA_BANK_ACCOUNT
  - Previously had: EMAIL_ADDRESS, PHONE_NUMBER, US_SSN

- Add missing 7 examples for smart_fake operator (now has all 10 entity types)
  - Added: CREDIT_CARD, IP_ADDRESS, IBAN_CODE, PERSON, LOCATION, US_BANK_ACCOUNT, CA_BANK_ACCOUNT
  - Previously had: EMAIL_ADDRESS, PHONE_NUMBER, US_SSN

- Add comprehensive test to verify smart operators have complete examples
  - test_smart_operators_have_complete_examples() verifies all 10 entity types
  - Tests both operators have examples for each supported entity type
  - Tests entity_type filtering works for all entity types

- Examples match the outputs from corresponding entity-specific operators
- All 75 PII-related tests pass (1 new test added)

* fix: minor fix

* feat: add multi-entity PII support for fields with multiple entity types

- Added support for applying multiple anonymization operators to a single field
- Fields can now have multiple entity types (e.g., PERSON and EMAIL_ADDRESS in same field)
- Operators are applied sequentially in order of detection confidence (highest first)
- Updated PIIHandler to accept List[PIIFieldAnonymization] instead of Dict
- Added apply_multi_entity_anonymization() method to PresidioAnonymizer
- Enhanced CollectionPIIAnalysis to sort operators by confidence for multi-entity fields
- Maintains backward compatibility with legacy dict format (pii_anonymized_fields)
- Added 23 comprehensive tests for multi-entity anonymization scenarios
- Updated documentation with multi-entity examples

Changes:
- Modified PIIHandler.__init__ to accept both list and dict formats
- Added field_operators dict to group operators by field
- Updated process_documents to use new multi-entity anonymization method
- Enhanced PresidioAnonymizer with apply_multi_entity_anonymization()
- Updated orchestrator to pass full pii_anonymization list
- Modified CollectionPIIAnalysis.get_pii_anonymization_list() to sort by confidence
- Added test_pii_handler.py with 15 tests
- Added 8 multi-entity tests to test_presidio_anonymizer.py

* chore: remove deprecated custom_strategy_aliases

* chore: pass params to build OperatorConfig

* chore: remove entity_type from PIIFieldAnonymization to only use params

* feat: add configurable keep_first and keep_last params to SmartMaskOperator

- Add keep_first parameter to control chars kept at the beginning (default: 3)
- Add keep_last parameter to control chars kept at the end (default: 3)
- Parameters only affect fallback masking for GENERIC entity types
- Entity-specific operators (email, phone, etc.) use their own masking logic
- Add comprehensive tests for new parameters including edge cases
- All 402 tests passing

* fix: preserve array type during PII anonymization

Previously, array fields were being converted to string representations
(e.g., ['item1', 'item2'] -> "['item1', 'item2']") instead of
preserving the array structure.

This fix adds special handling in _anonymize_value() to recursively
anonymize each array element while maintaining the array type.

- Handle arrays by anonymizing each element individually
- Preserve empty arrays
- Handle None values within arrays
- Add 4 comprehensive tests for array anonymization

* feat: add configurable params to mask_email operator

Add three new parameters to MaskEmailOperator for flexible email masking:
- keep_domain (bool, default: True): Whether to preserve the domain
- min_local_part (int, default: 4): Minimum length for local part
- min_domain_part (int, default: 4): Minimum length for domain

If the local part or domain length is below the minimum thresholds,
they are fully masked. Otherwise, partial masking applies (showing
first 2 and last 2 chars for local parts > 4 chars).

Examples:
- john.smith@example.com -> jo******th@example.com (default)
- joe@example.com -> ***@example.com (local < 4, fully masked)
- john@ex.c -> j***@ex.c (local = 4, domain = 4, partial masking)

Added 13 comprehensive tests covering all parameter combinations.

* refactor: update mask_email thresholds and add edge case tests

- Change default min_local_part and min_domain_part from 4 to 5
- Change threshold logic from < to <= (values at threshold are now fully masked)
- Remove special case for 4-char local parts (simplified logic)
- Update documentation to reflect new defaults and behavior
- Add 4 missing edge case tests:
  - test_mask_email_at_min_local_part (boundary: local == 5)
  - test_mask_email_above_min_local_part (boundary: local > 5)
  - test_mask_email_domain_at_threshold (boundary: domain == 5)
  - test_mask_email_domain_above_threshold (boundary: domain > 5)

All 419 tests passing.

* feat: add mask_sin and mask_tin operators for Canadian IDs ([`f13b666`](https://github.com/nhuray/mongo-replication/commit/f13b66620718fd51e43ab18641c728799e607586))

### Chores

* chore(release): 1.0.0 [skip ci] ([`9d91cb7`](https://github.com/nhuray/mongo-replication/commit/9d91cb7c6154bb59675bc9bd61671fb58bfbe2ed))

### Refactoring

* refactor: remove unused PII code and improve API clarity (#19)

This commit removes dead code from the PII handling system and improves API clarity:

**Removed:**
- PIIRedactor class (200 lines) - completely unused, replaced by Presidio
- pii_map parameter from anonymization API - never used in production (always None)

**Renamed:**
- manual_overrides → pii_field_strategy (clearer parameter name)

**Why this is safe:**
1. PIIRedactor had zero usage - not imported anywhere in production code
2. pii_map was designed for auto-detected PII but workflow is:
   - Scan phase: detect PII → convert to manual config → save to YAML
   - Run phase: load manual config → anonymize (pii_map always None)
3. All 264 unit tests pass after changes

**Benefits:**
- Removes ~300 lines of dead code
- Simpler, cleaner API
- Less confusion about workflow
- Better parameter naming (pii_field_strategy vs manual_overrides) ([`9c58d2f`](https://github.com/nhuray/mongo-replication/commit/9c58d2f69b85356d48a1e2144596f4fd0a71e5ff))


## v0.2.0 (2026-04-13)

### Chores

* chore(release): 0.2.0 [skip ci] ([`f46d2e7`](https://github.com/nhuray/mongo-replication/commit/f46d2e78b883418b6df2581bf0c9b0b584b3b396))

### Features

* feat: add per-collection batch_size override (#18)

- Added batch_size field to CollectionConfig to override performance.batch_size
- Removed duplicate cursor_initial_value validator (now inherits from parent)
- Updated orchestrator to use per-collection batch_size when replicating
- Enhanced CLI display to show batch_size information:
  * Changed 'Batch Size' to 'Default Batch Size' in banner and info
  * Show batch_size for each collection during config building
  * Display batch_size in live progress when overridden
- Kept cursor_field and cursor_initial_value overrides (needed for per-collection customization)

This allows fine-tuning batch sizes per collection:
- Large collections can use smaller batches (avoid memory issues)
- Small collections can use larger batches (better performance)
- Auto-discovered collections use global default ([`ab91376`](https://github.com/nhuray/mongo-replication/commit/ab91376e9e53043ed39f604f48cd3b992208c049))


## v0.1.4 (2026-04-13)

### Bug Fixes

* fix: respect cursor_initial_value on first replication run (#17)

* fix: respect cursor_initial_value on first replication run

- Changed cursor_initial_value from string to datetime type with validation
- Added field validators to parse ISO 8601 strings and fail fast on invalid formats
- Updated CollectionReplicator to accept and use cursor_initial_value parameter
- Modified _build_query() to use cursor_initial_value when no previous state exists
- Updated orchestrator to pass cursor_initial_value to replicator

This fixes the issue where first-time replication with merge strategy would
replicate all documents instead of only documents after cursor_initial_value.

* fix: deep merge collection config with defaults using exclude_unset

The previous fix didn't fully work because _build_collection_config was
returning explicit_config as-is without merging it with defaults.

CollectionConfig fields like cursor_initial_value are defined as Optional
with None default, which overrides the parent ReplicationDefaultsConfig
default value. This means collections without explicit cursor_initial_value
would get None instead of inheriting the default.

This commit fixes _build_collection_config to use the same robust pattern
as load_config in manager.py:
- Import deep_merge utility from config.manager
- Create base config dict from defaults using model_dump()
- For explicit configs, get only explicitly set fields using model_dump(exclude_unset=True)
- Use deep_merge to combine base and override (handles nested dicts properly)
- Validate merged data with CollectionConfig.model_validate()

This approach is more maintainable and less fragile than manually listing
every field, and automatically works if new fields are added. ([`e98299f`](https://github.com/nhuray/mongo-replication/commit/e98299f2c22caeaab0adab5aeab1312e01d66316))

### Chores

* chore(release): 0.1.4 [skip ci] ([`417eb21`](https://github.com/nhuray/mongo-replication/commit/417eb21551a5691330d9039b424fd9433977681f))


## v0.1.3 (2026-04-13)

### Bug Fixes

* fix(scan-report): improve scan report organization and readability (#16)

fix(scan-report): sort collections alphabetically and reorder relationship table columns

- Sort collections alphabetically in PII Analysis section (instead of by PII field count)
- Reorder Relationship Details table columns to match Mermaid graph order:
  - Old: Parent Collection | Child Collection | Child Field | Parent Field
  - New: Child Collection | Child Field | Parent Collection | Parent Field
- Sort relationships alphabetically by child collection name ([`b39beee`](https://github.com/nhuray/mongo-replication/commit/b39beee60cd4a19add2354f4b359063073104d80))

### Chores

* chore(release): 0.1.3 [skip ci] ([`3da279e`](https://github.com/nhuray/mongo-replication/commit/3da279e03d4236b60a5888b4c9496350111b5903))


## v0.1.2 (2026-04-13)

### Bug Fixes

* fix: respect default_strategies from config during scan (#15)

* chore: update uv.lock

* fix: fix default strategies not applied during scan ([`a11da3a`](https://github.com/nhuray/mongo-replication/commit/a11da3a1c4644b9440d187fb10fe48dfe931e7fb))

### Chores

* chore(release): 0.1.2 [skip ci] ([`88c5dc9`](https://github.com/nhuray/mongo-replication/commit/88c5dc900fd4689662097c635878a137ddabd09d))


## v0.1.1 (2026-04-09)

### Bug Fixes

* fix: update README.md ([`9b9a9da`](https://github.com/nhuray/mongo-replication/commit/9b9a9da8276bbc9cea0c2b6f53f3022b5757f64d))

### Chores

* chore(release): 0.1.1 [skip ci] ([`b67e403`](https://github.com/nhuray/mongo-replication/commit/b67e403a8c3d9abcd7ca481f22ebaace3cf74eb9))


## v0.1.0 (2026-04-09)

### Bug Fixes

* fix(ci): use pip instead of uv in semantic-release build command (#14)

fix(ci): use pip install build in semantic-release build command

The python-semantic-release action runs in its own Docker container
which doesn't have uv installed. Changed the build_command to use
standard pip and python instead of uv.

Fixes GitHub Actions Release workflow failure where 'uv: command not found'
was causing the build to fail with exit code 127. ([`4bc8b2c`](https://github.com/nhuray/mongo-replication/commit/4bc8b2cdad1583b256e7a2f093263f73ec49619e))

* fix: include database name in URI normalization for validation

Updated _normalize_uri to include database name in comparison. This ensures
that connections to the same MongoDB cluster but different databases are
allowed, while still preventing replication to the same database. ([`ca10e65`](https://github.com/nhuray/mongo-replication/commit/ca10e652cd561785898c266af282803e1cc58967))

* fix: rename parent_collection and child_collection to parent and child in scan command ([`2697b5a`](https://github.com/nhuray/mongo-replication/commit/2697b5a012e66ee3f230fae2fe26296ac05d647b))

* fix: remove incorrect parent_collection/child_collection fallback

SchemaRelationshipConfig uses 'parent' and 'child' attributes, not 'parent_collection' and 'child_collection'. The hasattr check was unnecessary and the fallback was causing AttributeError. ([`436d8b6`](https://github.com/nhuray/mongo-replication/commit/436d8b6f64d8bf8ed276b3bb08919f3d3fb9c887))

* fix: add python-dotenv dependency and fix exception handling

Issues fixed:
1. .env file not being loaded because python-dotenv was not installed
   - Add python-dotenv>=1.0.0 to dependencies in pyproject.toml
   - Improve error handling in main.py to warn if dotenv import fails
   - Explicitly check for .env file existence and load from cwd

2. Wrong exception type caught in scan and run commands
   - JobManager.get_job() raises ValueError, not KeyError
   - Fix scan.py to catch ValueError instead of KeyError
   - Fix run.py to catch ValueError instead of KeyError
   - Display actual error message from ValueError for better UX

All 286 tests passing. ([`c88d76f`](https://github.com/nhuray/mongo-replication/commit/c88d76f0ce2f82f3a598c30c4a5ab85ca984aeea))

* fix: make CollectionsConfig subscriptable

Add __getitem__, __contains__, get(), items(), keys(), and values() methods
to CollectionsConfig RootModel to enable dictionary-style access.

This fixes the test failure where config.replication.collections['users']
was not working. ([`425afb8`](https://github.com/nhuray/mongo-replication/commit/425afb8cc5bc80bc351b8c60e08a83564a9fc98b))

* fix: fix scan command to sample docs ([`b64b363`](https://github.com/nhuray/mongo-replication/commit/b64b3636b9633762d7e7b87875606d576f40ac95))

* fix: update Presidio tests to match new YAML structure and caching

- Update test YAML configs to use recognizer_registry.recognizers hierarchy
- Fix test_get_analyzer_lazy_initialization to check 'default' cache key
- Update test_load_invalid_yaml_raises_error to use truly invalid structure
- All 270 tests now passing ([`4363f61`](https://github.com/nhuray/mongo-replication/commit/4363f61c0b9a1c9ed86c7e975aacea98c3fc2fea))

### Chores

* chore(release): 0.1.0 [skip ci] ([`e1a3fd6`](https://github.com/nhuray/mongo-replication/commit/e1a3fd697176231a71e08f9a526ca8c68fc328e9))

* chore: update to use PAT for release workflow ([`d96dc03`](https://github.com/nhuray/mongo-replication/commit/d96dc0321ed58a50a6fc1f0993a9c38b598cca9b))

* chore: minor change ([`ffa869f`](https://github.com/nhuray/mongo-replication/commit/ffa869f98d68d43914af8864e7519a3608821b69))

* chore: minor change ([`640e698`](https://github.com/nhuray/mongo-replication/commit/640e698b53c52d1df2883a0dec324422bca6dbcc))

* chore: fix some run issues ([`8c312e3`](https://github.com/nhuray/mongo-replication/commit/8c312e3b4d6d1775028a918377f463e99970d018))

* chore: update jinja template ([`5ac3357`](https://github.com/nhuray/mongo-replication/commit/5ac33579ab6b7ff43403cee9d85f891ef37d962c))

* chore: update sacn command ([`a20de90`](https://github.com/nhuray/mongo-replication/commit/a20de90dfc9efc9df1c7a0f8d43fc9d74da60a16))

* chore: update jinja2 template and defaults for cursor detection ([`23b2b3c`](https://github.com/nhuray/mongo-replication/commit/23b2b3c0338fbc8ca83a0efe99eadc4fca685241))

* chore: change cursor_initial_value ([`7ccfac9`](https://github.com/nhuray/mongo-replication/commit/7ccfac9d8348634e48e7cbfa673899d742b4fb4a))

* chore: minor change to init.py ([`0621fa7`](https://github.com/nhuray/mongo-replication/commit/0621fa7a0cb55b7ba651e4be04889bba50ad6fb7))

* chore: minor change to jinja2 template ([`fc5db25`](https://github.com/nhuray/mongo-replication/commit/fc5db2545b6678ca7da07d252b43e401dea16314))

* chore: refactor config with pydantic models ([`4498c75`](https://github.com/nhuray/mongo-replication/commit/4498c759ce3d4a40fc1cd275716e0bc6cc5f1eea))

* chore: remove unrelated test file from PR ([`6ac6886`](https://github.com/nhuray/mongo-replication/commit/6ac688639c83fa640350570b80cc6ee439e4539d))

* chore: update config variables from REP_ to MONGOREP_ ([`f423191`](https://github.com/nhuray/mongo-replication/commit/f4231917446f4e1f6c1145834fc8e1f3831a23d7))

* chore: set _id and meta.* fields in pii allowlist (skip) ([`7777e21`](https://github.com/nhuray/mongo-replication/commit/7777e21cb7eeced66673c6699e90d7dfe3ded3ae))

* chore: update reference to cli ([`c2703e5`](https://github.com/nhuray/mongo-replication/commit/c2703e5b7c46448eca3f75dbed29389d823c0b59))

* chore: update cascade_filter.py to support query based selection

Rename --select option to --ids
Introduce --query option ([`d400f61`](https://github.com/nhuray/mongo-replication/commit/d400f61a5166ef4a5b37767635d045953f324f33))

* chore: Update logic to detect cursor field automatically on scan ([`d8a15b0`](https://github.com/nhuray/mongo-replication/commit/d8a15b0ce70412f253d5df68c62cbdbd0ead1926))

* chore: Rename loader.py to manager.py ([`9192820`](https://github.com/nhuray/mongo-replication/commit/9192820fc16aa9e053f2b451cb16a00cfff8fb89))

* chore: Move config models in models.py ([`7d2f3fc`](https://github.com/nhuray/mongo-replication/commit/7d2f3fc0565e04e42d859f84ce9e5b0d0ac27942))

* chore: Homogenize replication defaults config with generated config ([`f0ec9cf`](https://github.com/nhuray/mongo-replication/commit/f0ec9cf15deaa9feac672816293b46bc76cfb606))

### Documentation

* docs: add banner image to README ([`4a64901`](https://github.com/nhuray/mongo-replication/commit/4a649013ff70023f5b020ac38d72d9435f0b9d7f))

* docs: add comprehensive Presidio documentation and update references

- Add new docs/presidio.md with complete Presidio operator documentation
  - All 16 operators documented (built-in + custom)
  - Default entity-to-operator mappings
  - Strategy aliases
  - Custom YAML configuration examples (healthcare, finance, e-commerce)
  - Architecture overview and usage examples

- Update docs/configuration.md
  - Replace outdated default strategies with correct mappings
  - Reference presidio.md for detailed operator documentation
  - Simplify Custom Presidio Configuration section
  - Update PII analysis settings with correct operator names

- Update README.md files
  - Replace outdated strategy mappings with current operators
  - Reference presidio.md for comprehensive documentation
  - Update examples to use correct operator names

- Update docs/technical-design.md
  - Add note referencing presidio.md for implementation details
  - Keep high-level architecture overview ([`0ebe4c4`](https://github.com/nhuray/mongo-replication/commit/0ebe4c4df39ff84b11fe26fc031b0017bd54fef3))

* docs: add schema relationship analysis documentation

- Add Schema Relationship Analysis section to configuration.md with detailed explanation
- Add Cursor Detection Settings section to configuration.md
- Update README.md to mention schema relationship inference in features
- Update scan description to include relationship detection
- Document configuration options, patterns, and usage examples ([`a326cde`](https://github.com/nhuray/mongo-replication/commit/a326cdecdd74fb6230260e57277c2045362aa6c9))

* docs: fix docs ([`c54025e`](https://github.com/nhuray/mongo-replication/commit/c54025e754d0207bcf30fbda4f36e9dee6461a6d))

* docs: update documentation to reflect new config structure

- Update README.md with new scan and replication structure
- Update docs/configuration.md with detailed new config sections
- Rename schema -> schema_relationships in examples
- Split scan.pii into scan.sampling and scan.pii_analysis
- Reorganize replication into discovery, state_management, performance, and defaults
- Update field names: fallback_cursor -> cursor_fallback_field, initial_value -> cursor_initial_value
- Fix config_template.yaml.j2 to use new structure
- Update defaults.yaml to remove duplicate entries ([`3bb7edb`](https://github.com/nhuray/mongo-replication/commit/3bb7edb1409880fbfa5c7b3e0c0fc0a2cd4c5778))

* docs: update README.md ([`9b2d3a9`](https://github.com/nhuray/mongo-replication/commit/9b2d3a933ec9a20e179f7e9426a34c25010451e4))

* docs: update README.md ([`12d5cd3`](https://github.com/nhuray/mongo-replication/commit/12d5cd3c36d64123c1f731671e34aea4bf6cc681))

* docs: update documentation ([`d709608`](https://github.com/nhuray/mongo-replication/commit/d709608003f4dadd95e5d9337515b42b2d751e6a))

* docs: update documentation ([`e70b5a5`](https://github.com/nhuray/mongo-replication/commit/e70b5a51914055a5f8596bda258a350ec6a38731))

* docs: update configuration documentation ([`b4a9ab7`](https://github.com/nhuray/mongo-replication/commit/b4a9ab7a77ba83f9f2a8ad132e0418b953269449))

* docs: update README.md ([`c937959`](https://github.com/nhuray/mongo-replication/commit/c937959d28b854492707a1c6af21ee51313ce341))

* docs: update documentation ([`b59614c`](https://github.com/nhuray/mongo-replication/commit/b59614cda8299fdfdb2d976509f93af776611183))

### Features

* feat(cli): improve signal handling for graceful interruption

Add comprehensive signal handling to gracefully handle SIGINT (Ctrl+C),
SIGTERM, and EOFError (Ctrl+D) across all CLI commands.

Changes:
- Add signal_handler.py utility with SignalHandler context manager
- Register global signal handlers in main.py for SIGINT and SIGTERM
- Wrap init command with exception handling for KeyboardInterrupt and EOFError
- Update scan and run commands to handle EOFError in addition to KeyboardInterrupt
- Extract init wizard logic into _run_init_wizard() for cleaner error handling

Benefits:
- Graceful shutdown on Ctrl+C with proper cleanup
- Handles Ctrl+D (EOFError) in interactive prompts
- Consistent exit code 130 for user interruptions (standard for SIGINT)
- Clean error messages without stack traces
- Signal handlers registered at application level for global coverage

Exit codes:
- 0: Success
- 1: Error/failure
- 130: User interruption (SIGINT/Ctrl+C)

Addresses issue with signals not being trapped properly when running commands. ([`a62302c`](https://github.com/nhuray/mongo-replication/commit/a62302cb0e08a684e312315cf71794c42cc338cf))

* feat(cli): show default anonymization strategies in init wizard

Enhance the 'mongorep init' command UX to display default anonymization
strategies for each PII entity type, making it clearer for users what
will happen to detected PII data.

Changes:
- Step 5: Entity type selection now shows default strategy next to each
  type (e.g., 'EMAIL_ADDRESS → smart_redact')
- Step 6: Replaced hardcoded outdated strategies with actual defaults
  from DEFAULT_ENTITY_STRATEGIES imported from presidio_anonymizer
- Updated available operators description to reflect both built-in and
  custom operators from presidio.yaml
- Expanded strategy selection choices from 3 to 10 common operators
  including custom ones (fake_email, fake_name, smart_redact, etc.)

This aligns the CLI UX with the recent Presidio operator refactoring
and makes the configuration process more transparent and user-friendly. ([`f8a6579`](https://github.com/nhuray/mongo-replication/commit/f8a6579954977f4be086139acf4547d7138a3dea))

* feat: refactor PII anonymization to use Presidio AnonymizerEngine with YAML-configured operators

- Add YAML configuration for anonymization operators (presidio.yaml)
- Create custom Presidio operators: fake_email, fake_name, fake_phone, fake_address, fake_ssn, fake_credit_card, fake_iban, fake_us_bank_account, stripe_testing_cc, smart_redact
- Implement PresidioConfig parser to convert YAML to OperatorConfig objects
- Refactor presidio_anonymizer.py to properly use Presidio's AnonymizerEngine.anonymize()
- Update PIIHandler to use new PresidioAnonymizer instead of pii_redaction module
- Remove deprecated PIIRedactor and redact_document exports
- Rewrite test_presidio_anonymizer.py to test new implementation (29 tests, all passing)
- Remove deprecated test_pii_redaction.py
- All 271 tests passing

This change moves from a fragile custom implementation to proper Presidio integration with operator-based configuration, improving maintainability and extensibility. ([`8e99af2`](https://github.com/nhuray/mongo-replication/commit/8e99af229fb331aa432beaa9b5ba593c84a2d3ec))

* feat: validate source and destination databases are different

Adds validation to prevent source and destination MongoDB connections from
pointing to the same database, which could cause data corruption.

Changes:
- Add validation in ConnectionManager.__init__() that compares normalized
  URIs and database names
- Implement URI normalization to extract host:port for comparison
- Update init, run, and scan commands to catch and handle ValueError
- Raise clear error message if source and destination match

All 286 existing tests continue to pass. ([`f363a25`](https://github.com/nhuray/mongo-replication/commit/f363a25162ac8a3200d5fc95ae411240ee9b6231))

* feat: enhance scan report with Jinja2 template and additional sections

- Convert PII report to comprehensive scan report using Jinja2
- Add cursor field detection section with sample values
- Add schema relationships section with Mermaid diagram
- Rename report from pii_report.md to scan_report.md
- Track cursor field info during detection for reporting
- All 286 tests passing ([`9ddc211`](https://github.com/nhuray/mongo-replication/commit/9ddc21199fcdae51ee335e7ae6cb797732f826e0))

* feat: sync discovery config between scan and replication

- Update init command to set both scan.discovery and replication.discovery with same include/exclude patterns
- Update run command interactive mode to filter collections by discovery patterns
- Scan command already respects discovery patterns via CollectionDiscovery
- All 286 tests passing ([`ca0e6e1`](https://github.com/nhuray/mongo-replication/commit/ca0e6e18610ce79b91ebf05c66982c74005f3784))

* feat: add automatic schema relationship inference during scan

- Add SchemaRelationshipAnalyzer class to detect parent-child relationships between collections based on field name patterns (e.g., customer_id -> customers)
- Integrate analyzer into scan command as Step 6, running after PII analysis when scan.schema_relationships.enabled is true
- Add configuration option in init command (Step 9) to enable/disable relationship analysis
- Update config models with ScanSchemaRelationshipsConfig and add scan.schema_relationships section to template
- Inferred relationships are saved to root-level schema_relationships in generated config for cascade replication
- Support snake_case (customer_id), camelCase (customerId), nested fields (meta.order_id), and plural/singular conversions
- Add 16 comprehensive unit tests for analyzer (all passing, 286 total tests) ([`163f5af`](https://github.com/nhuray/mongo-replication/commit/163f5af0308c4b1aaf4be30ad0cc424b4b955073))

* feat: add cursor_detection config and improve init command flow

- Add scan.cursor_detection.cursor_fields to config model and defaults
- Update Jinja template to include cursor_detection section
- Add cursor_fields question in init command (Step 4 of 9)
- Reorganize init command steps to group all PII-related questions (Steps 5-8)
- Update scan.py to read cursor_fields from scan.cursor_detection config
- All 270 tests passing

This allows users to customize which cursor field candidates are checked
during scan for incremental replication, with sensible defaults:
- updated_at
- updatedAt
- meta.updated_at
- meta.updatedAt ([`e1f1afe`](https://github.com/nhuray/mongo-replication/commit/e1f1afe40994d816c991965339bdd7814c86e607))

* feat: make PII analysis respect scan.pii.enabled config flag

- Read scan.pii.enabled from existing config if present
- Precedence: --no-pii CLI flag > scan.pii.enabled config > default (enabled)
- Update banner to show PII status source (--no-pii, config, or enabled)
- Add informative messages when PII is skipped
- Always save pii config section (not conditionally based on --no-pii)
- Save enabled=false when --no-pii is passed, enabled=true otherwise

This allows users to disable PII analysis by setting enabled: false in
their config file, without having to pass --no-pii on every scan. ([`5805d82`](https://github.com/nhuray/mongo-replication/commit/5805d82a1e5d46389644afc4627d20b1c5be8d2b))

* feat: add Presidio YAML configuration support for custom PII recognizers

Add comprehensive YAML-based configuration system for Presidio PII detection, allowing users to define custom PII recognizers without writing Python code.

Key Features:
- Default presidio.yaml with common recognizers (EMAIL, PHONE, SSN, CREDIT_CARD, etc.)
- Custom recognizer examples (Bank Account, Employee ID, Patient ID)
- Smart path resolution (absolute → cwd → config/ → default)
- Fail-fast validation with clear error messages
- Analyzer caching by config path for performance
- Support for regex patterns and deny-lists
- Context-aware detection with context words

Changes:
- Add presidio_config field to ScanPIIConfig model
- Update PresidioAnalyzer to load from YAML using AnalyzerEngineProvider
- Update PIIAnalyzer to pass config path through chain
- Update scan command to read/save presidio_config
- Update init command to ask about custom Presidio config (now 8 steps)
- Update config manager to load/save presidio_config field
- Update defaults.yaml with presidio_config documentation
- Add 300+ lines of documentation with examples in docs/configuration.md
- Add 15 unit tests covering path resolution, YAML loading, and caching

Files Modified:
- src/mongo_replication/config/models.py
- src/mongo_replication/engine/pii/presidio_analyzer.py
- src/mongo_replication/engine/pii/pii_analyzer.py
- src/mongo_replication/cli/commands/scan.py
- src/mongo_replication/cli/commands/init.py
- src/mongo_replication/config/manager.py
- src/mongo_replication/config/defaults.yaml
- docs/configuration.md

Files Added:
- src/mongo_replication/config/presidio.yaml (392 lines)
- tests/unit/test_presidio_config.py (414 lines)

Total: 1589 insertions, 61 deletions across 10 files
All tests passing ✓ ([`a20b7df`](https://github.com/nhuray/mongo-replication/commit/a20b7dfec43a6e43d5ae33010992f1c86b17fc5b))

### Refactoring

* refactor(cli): load anonymization strategies from presidio.yaml config

Remove hardcoded DEFAULT_ENTITY_STRATEGIES and load strategies dynamically
from the presidio.yaml configuration file (default or user-provided).

Changes:
- Move Presidio config question earlier in init wizard (right after 'Enable PII Analysis?')
- Add load_entity_strategies_from_config() helper to read strategies from YAML
- Entity type selection now shows strategies from the loaded config
- Strategy configuration uses loaded config instead of hardcoded dictionary
- Remove DEFAULT_ENTITY_STRATEGIES from presidio_anonymizer.py
- Add TEST_ENTITY_STRATEGIES in test_presidio_anonymizer.py for unit tests
- Remove DEFAULT_ENTITY_STRATEGIES from __init__.py exports

Benefits:
- Single source of truth: presidio.yaml is the only place to define strategies
- Respects user's custom config when provided
- No need to maintain duplicate hardcoded mappings
- Tests use TEST_ENTITY_STRATEGIES to avoid config file dependency

This addresses feedback to read the actual configuration file rather than
maintaining a separate hardcoded dictionary. ([`585ebd2`](https://github.com/nhuray/mongo-replication/commit/585ebd2db6451c3c3eb0b840bb9c307b4efce693))

* refactor: rename pii_report.py to scan_report.py and update function name

- Rename pii_report.py to scan_report.py to better reflect comprehensive nature
- Rename generate_pii_report() to generate_scan_report()
- Update import in scan.py
- All 286 tests passing ([`3392f9a`](https://github.com/nhuray/mongo-replication/commit/3392f9a0ca1a51543bbcf9b7ffdc39a1b4cc1410))

* refactor: rename 'Schema Relationship Analysis' to 'Schema Relationship Inference'

Update terminology throughout codebase and documentation for clarity:
- Update init.py: Step 9 now says 'Schema Relationship Inference'
- Update scan.py: Step 6 now says 'Infer Schema Relationships'
- Update configuration.md: Section renamed to 'Schema Relationship Inference'
- Update config templates and defaults.yaml
- Update models.py docstrings
- All 286 tests still passing ([`2987af6`](https://github.com/nhuray/mongo-replication/commit/2987af6ac253a97a4e9dc86d68b4f6f9dcf6140e))

* refactor: rename RelationshipConfig to SchemaRelationshipConfig and move the config at root-level ([`7dc1875`](https://github.com/nhuray/mongo-replication/commit/7dc1875b5600da6504e71d4d41049caa81b1ea5b))

* refactor: make CollectionConfig inherit from ReplicationDefaultsConfig

This eliminates field duplication between CollectionConfig and
ReplicationDefaultsConfig by using inheritance. Now CollectionConfig
automatically inherits all default fields (cursor_field, cursor_fallback_field,
cursor_initial_value, primary_key, write_disposition, transform_error_mode)
from ReplicationDefaultsConfig, making the code more DRY.

Benefits:
- Reduces code duplication
- Ensures consistency between defaults and collection overrides
- Makes it easier to add new default fields in the future
- All 270 tests still passing ([`2450b03`](https://github.com/nhuray/mongo-replication/commit/2450b03d08bfdef345127bafe9570bb067fe13aa))

* refactor: reorganize config structure for better separation of concerns

Restructure the configuration models to better organize settings:

SCAN configuration:
- Split 'scan.pii' into 'scan.sampling' and 'scan.pii_analysis'
- Move sample_size and sample_strategy to scan.sampling
- Move PII analysis settings to scan.pii_analysis
- Add scan.cursor_detection for cursor field detection

REPLICATION configuration:
- Separate concerns into logical sub-sections:
  - replication.discovery: replicate_all, include/exclude patterns
  - replication.state_management: runs_collection, state_collection
  - replication.performance: max_parallel_collections, batch_size
  - replication.defaults: cursor defaults, write disposition, etc.
- Remove cursor_fields from defaults (determined by scan)
- Rename fallback_cursor to cursor_fallback_field
- Rename initial_value to cursor_initial_value

COLLECTIONS configuration:
- Rename 'pii_fields' to 'pii_anonymized_fields' for clarity

Updated all code references:
- scan.py: Use new scan.sampling.* and scan.pii_analysis.* paths
- run.py: Use new replication.discovery.*, performance.* paths
- orchestrator.py: Use new nested config structure
- init.py: Create separate sampling and pii_analysis configs
- config_template.yaml.j2: Update Jinja2 template to new structure

All 270 unit tests pass. ([`01ebb51`](https://github.com/nhuray/mongo-replication/commit/01ebb51bbaa77e1e7dfa8deb99219fe45868ff82))

* refactor: convert remaining dataclasses to Pydantic models

- Convert IndexInfo in indexes.py to Pydantic BaseModel
- Convert ReplicationResult in replicator.py to Pydantic BaseModel
- Convert OrchestrationResult in orchestrator.py to Pydantic BaseModel
- Convert SamplingResult in pii/sampler.py to Pydantic BaseModel
- Convert FieldPIIStats and CollectionPIIAnalysis in pii/pii_analyzer.py to Pydantic BaseModel
- Update test_cascade_replication.py to use keyword arguments for Relationship instances

All 268 unit tests passing ([`41868ce`](https://github.com/nhuray/mongo-replication/commit/41868ceab42904ef4d7379623df57902de96ac07))

* refactor: remove legacy code to load config ([`5fefb2d`](https://github.com/nhuray/mongo-replication/commit/5fefb2db3aa620096645611b78d77938881af9cf))

* refactor: migrate from dataclasses to Pydantic models

- Replace dataclass with Pydantic BaseModel for all config models
- Add Pydantic validation using field_validator and model_validator
- Simplify load_config with deep merge and Pydantic validation
- Add _save_defaults function to generate defaults.yaml from models
- Add main() function to allow saving defaults via CLI
- Update tests to handle Pydantic ValidationError
- Add pydantic>=2.0.0 dependency

This refactoring provides:
- Better validation with clearer error messages
- Automatic type coercion and validation
- Simplified config loading logic
- Ability to regenerate defaults.yaml from code ([`625c594`](https://github.com/nhuray/mongo-replication/commit/625c5949361c1b687c031fba5cf368cdb9a3f868))

* refactor: replace fragile _write_yaml_with_comments with Jinja2 template

Replace the 290-line fragile _write_yaml_with_comments function that manually
concatenates YAML strings with a clean Jinja2 template-based approach.

Changes:
- Add jinja2>=3.0.0 to project dependencies
- Create config_template.yaml.j2 template for config serialization
- Add _get_jinja_env() to initialize Jinja2 environment
- Add custom filters: toyaml and tojson for YAML formatting
- Replace _write_yaml_with_comments() with _render_config_template()
- Remove _format_yaml_value() helper function (no longer needed)
- Update save_config() to use template rendering

Benefits:
- Cleaner separation of logic and presentation
- Easier to maintain and modify YAML structure
- No manual string concatenation
- Better whitespace handling
- All 270 tests passing

The template properly handles:
- Scan configuration (discovery patterns, PII settings)
- Replication configuration (defaults, collections)
- Schema/relationships
- Comments and documentation
- Empty lists vs populated lists
- Proper YAML indentation ([`daa94ff`](https://github.com/nhuray/mongo-replication/commit/daa94ffccc1b43615c62803e49b75aba016fc564))

* refactor: remove hardcoded defaults from scan command, use centralized defaults.yaml

- Load scan defaults from defaults.yaml at the start of scan_command()
- Replace all hardcoded default values with lookups from loaded defaults
- Maintain proper precedence: CLI options > Config file > defaults.yaml
- Remove duplicate load_defaults() call (was loading twice)
- All 270 tests passing

Key changes:
- Lines 201-205: Load system_defaults, scan_defaults, discovery_defaults, pii_defaults early
- Line 234: Get pii.enabled from defaults instead of hardcoding True
- Lines 249-251: Get sample_size, confidence_threshold from pii_defaults
- Line 294: Get exclude_patterns from discovery_defaults
- Lines 377-380: Get entity_types, default_strategies, allowlist, sample_strategy from pii_defaults
- Line 488: Remove duplicate load_defaults() call

This ensures both init and scan commands use the same centralized defaults.yaml as the source of truth. ([`1e8197f`](https://github.com/nhuray/mongo-replication/commit/1e8197fbd485e778935c92d41836ead06d873b17))

### Testing

* test: revert test assertion for test_load_replication_config test ([`6979bb9`](https://github.com/nhuray/mongo-replication/commit/6979bb9890a133a6c5c9f9197c8aeee6a0697a00))

### Unknown

* Merge pull request #13 from nhuray/docs/improve-documentation

docs: comprehensive documentation improvements ([`949cd3d`](https://github.com/nhuray/mongo-replication/commit/949cd3da9f608f8a91ad6e592b89f9f22f0856eb))

* Update technical-design.md ([`61dc832`](https://github.com/nhuray/mongo-replication/commit/61dc83214a4b2f8518a6097fb0b153d0059a9294))

* Update technical-design.md ([`c64df53`](https://github.com/nhuray/mongo-replication/commit/c64df53d5518dda6713fb0f748fa1cd7737e87bf))

* Merge pull request #12 from nhuray/feat/improve-signal-handling

feat(cli): Improve signal handling for graceful interruption ([`08ddc4c`](https://github.com/nhuray/mongo-replication/commit/08ddc4c4228d77135e50929b40d11930d6ae10e2))

* Merge pull request #12 from nhuray/feat/improve-signal-handling

feat(cli): Improve signal handling for graceful interruption ([`cbf73d1`](https://github.com/nhuray/mongo-replication/commit/cbf73d16f98651c96450b2cd2ac0031b3c783fad))

* Merge pull request #11 from nhuray/feat/show-anonymization-strategies-in-init

feat(cli): Show default anonymization strategies in init wizard ([`dd436c8`](https://github.com/nhuray/mongo-replication/commit/dd436c8d0e534ce44a8b0bda3ceb78567318859c))

* Merge pull request #10 from nhuray/feat/presidio-operators-yaml-config

feat: Refactor PII anonymization to use Presidio AnonymizerEngine with YAML-configured operators ([`f81bb11`](https://github.com/nhuray/mongo-replication/commit/f81bb11f4045f70dedb15bc5241d96fd80e63daa))

* Merge pull request #9 from nhuray/feat/validate-source-dest-urls

feat: validate source and destination databases are different ([`00a3f49`](https://github.com/nhuray/mongo-replication/commit/00a3f49b36e3cf10611094d50d54644c0310bf0a))

* Merge pull request #8 from nhuray/feat/enhanced-scan-report

feat: Enhanced scan report with Jinja2 template and comprehensive sections ([`adb8c8a`](https://github.com/nhuray/mongo-replication/commit/adb8c8aef50abe9e90de553139b3e217cc174b41))

* Merge pull request #7 from nhuray/feat/sync-discovery-config

Sync discovery config between scan and replication ([`e69e3c0`](https://github.com/nhuray/mongo-replication/commit/e69e3c0e9505d5a5efcea6b50227e1bc9fabfd0e))

* Merge pull request #6 from nhuray/fix/dotenv-loading-and-exception-handling

fix: Add python-dotenv dependency and fix exception handling ([`636022c`](https://github.com/nhuray/mongo-replication/commit/636022c17f4ba884ec4c96183202093fd94c9ff3))

* Merge pull request #5 from nhuray/chore/infer-schema-relationships

feat: Add automatic schema relationship inference during scan ([`73defcb`](https://github.com/nhuray/mongo-replication/commit/73defcbfce4933cc8ceb9a442a6e4da04c59a52a))

* Merge pull request #4 from nhuray/refactor/config

refactor: reorganize config structure for better separation of concerns ([`c26d741`](https://github.com/nhuray/mongo-replication/commit/c26d741b319e4ccdcada3aa36381a456a1903cc4))

* Merge pull request #3 from nhuray/chore/pydantic-config

refactor: migrate from dataclasses to Pydantic models ([`90d2fa2`](https://github.com/nhuray/mongo-replication/commit/90d2fa2673d88f55cb37c7705e4a02e156876d85))

* Merge pull request #2 from nhuray/feat/jinja-config-template

refactor: replace fragile YAML writer with Jinja2 template ([`a945d4a`](https://github.com/nhuray/mongo-replication/commit/a945d4a1590fd22d8cd643c0cf8b1629d64001b3))

* Merge pull request #1 from nhuray/feat/presidio-config

feat: Add Presidio YAML configuration support for custom PII recognizers ([`d2782de`](https://github.com/nhuray/mongo-replication/commit/d2782de32b197e94ba5f4db4f472c30ac07bc4b0))

* minor change ([`6da5661`](https://github.com/nhuray/mongo-replication/commit/6da56617096d6aacae269f30a0512639f868c6f9))

* Add semantic-release and github actions workflows ([`42bb44f`](https://github.com/nhuray/mongo-replication/commit/42bb44ffb62837c1e44c54ac333e781cfda9ba63))

* Format lint and fix tests ([`ef6bc0e`](https://github.com/nhuray/mongo-replication/commit/ef6bc0e602904563e5641b4a2e8620e0effa16f6))

* Remove black in favor of ruff

Update pre-commit hooks and install it ([`ade762a`](https://github.com/nhuray/mongo-replication/commit/ade762a79f246489cfb9bc0f0b452b1e78b5bbbc))

* Fix lint errors ([`9574db1`](https://github.com/nhuray/mongo-replication/commit/9574db1bb886bb0cef2918cd25f6d807b9d4fe58))

* Add pre-commit hooks for code formatting and linting ([`ee2190a`](https://github.com/nhuray/mongo-replication/commit/ee2190a0add5bfb28253f3ea10c55518aeb29492))

* Add .env.example file ([`6a50a52`](https://github.com/nhuray/mongo-replication/commit/6a50a528208e6238d31eec35dbd51c4a85926932))

* Review README.md for replication CLI ([`f9a3354`](https://github.com/nhuray/mongo-replication/commit/f9a33541b503a5923a2c013a97495d955cb0eced))

* Configure uv to install dev dependencies ([`aaaf6ee`](https://github.com/nhuray/mongo-replication/commit/aaaf6eed9501fcdbfaa6d27eaea1bcd204917b4b))

* Fix tests and deprecate legacy code ([`c333a0d`](https://github.com/nhuray/mongo-replication/commit/c333a0d70a1d36155e0b0b1cfdd1451a18991033))

* Add test coverage documentation ([`fa92767`](https://github.com/nhuray/mongo-replication/commit/fa9276704cbf0747adde7910828d3e0085d61b1f))

* Add comprehensive unit test suite

- Copied 13 test modules from analytics-pipelines
- Updated all imports to use mongo_replication package
- Added pytest configuration with coverage settings
- Added mongomock as dev dependency
- 223 passing tests covering:
  * PII detection and anonymization (Presidio)
  * Field transformations and exclusions
  * Cascade replication and relationships
  * State management (loads and collection state)
  * Configuration models and loading
  * Index replication
  * Job management
  * Error summarization

Note: 13 tests fail due to deprecated API usage in legacy tests
These tests validate deprecated methods that still work with warnings ([`f08e30d`](https://github.com/nhuray/mongo-replication/commit/f08e30dd737bb68a510798545cea5496e558ae9a))

* Initial commit: MongoDB Replication Tool

- Production-grade MongoDB replication with PII redaction
- Parallel processing with configurable worker pools
- Incremental loading with cursor-based state management
- Cascade filtering for related document replication
- Support for replace, append, and merge write strategies
- Field transformations and exclusions
- Comprehensive CLI with interactive mode
- Full documentation (README, configuration, technical design)
- MIT licensed and ready for PyPI publishing ([`6dab292`](https://github.com/nhuray/mongo-replication/commit/6dab2929affc35cea901e15e6365f0daa39addf7))
