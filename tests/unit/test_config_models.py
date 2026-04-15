"""Unit tests for new configuration models (ScanConfig, RepConfig)."""

import pytest
from pathlib import Path
from tempfile import NamedTemporaryFile

from pydantic import ValidationError

from mongo_replication.config.models import (
    ScanConfig,
    ScanDiscoveryConfig,
    ScanSamplingConfig,
    ScanPIIAnalysisConfig,
    Config,
    PIIFieldAnonymization,
    CollectionConfig,
)
from mongo_replication.config.manager import (
    load_config,
    load_scan_config,
    load_replication_config,
    save_config,
)


class TestScanDiscoveryConfig:
    """Tests for ScanDiscoveryConfig."""

    def test_default_values(self):
        """Test default values."""
        config = ScanDiscoveryConfig()

        assert config.include_patterns == []
        assert config.exclude_patterns == []

    def test_with_patterns(self):
        """Test with include/exclude patterns."""
        config = ScanDiscoveryConfig(
            include_patterns=["^users.*", "^transactions.*"],
            exclude_patterns=["^system\.", "^_.*"],
        )

        assert len(config.include_patterns) == 2
        assert len(config.exclude_patterns) == 2


class TestScanSamplingConfig:
    """Tests for ScanSamplingConfig."""

    def test_default_values(self):
        """Test default values."""
        config = ScanSamplingConfig()

        assert config.sample_size == 1000
        assert config.sample_strategy == "stratified"

    def test_custom_values(self):
        """Test custom values."""
        config = ScanSamplingConfig(
            sample_size=500,
            sample_strategy="random",
        )

        assert config.sample_size == 500
        assert config.sample_strategy == "random"

    def test_invalid_sample_size_raises_error(self):
        """Test that invalid sample size raises error."""
        with pytest.raises(ValueError, match="sample_size must be"):
            ScanSamplingConfig(sample_size=0)

        with pytest.raises(ValueError, match="sample_size must be"):
            ScanSamplingConfig(sample_size=-10)

    def test_invalid_sample_strategy_raises_error(self):
        """Test that invalid sample strategy raises error."""
        with pytest.raises(ValidationError, match="Input should be 'random' or 'stratified'"):
            ScanSamplingConfig(sample_strategy="invalid")


class TestScanPIIAnalysisConfig:
    """Tests for ScanPIIAnalysisConfig."""

    def test_default_values(self):
        """Test default values."""
        config = ScanPIIAnalysisConfig()

        assert config.enabled is True
        assert config.confidence_threshold == 0.85
        assert "EMAIL_ADDRESS" in config.entity_types
        assert "PHONE_NUMBER" in config.entity_types
        assert config.default_strategies["EMAIL_ADDRESS"] == "smart_mask"
        assert config.allowlist == []

    def test_custom_values(self):
        """Test custom values."""
        config = ScanPIIAnalysisConfig(
            enabled=False,
            confidence_threshold=0.9,
            entity_types=["EMAIL_ADDRESS"],
            default_strategies={"EMAIL_ADDRESS": "hash"},
            allowlist=["metadata.*"],
        )

        assert config.enabled is False
        assert config.confidence_threshold == 0.9
        assert config.entity_types == ["EMAIL_ADDRESS"]
        assert config.default_strategies == {"EMAIL_ADDRESS": "hash"}
        assert config.allowlist == ["metadata.*"]

    def test_invalid_confidence_threshold_raises_error(self):
        """Test that invalid confidence threshold raises error."""
        with pytest.raises(ValueError, match="confidence_threshold must be between"):
            ScanPIIAnalysisConfig(confidence_threshold=1.5)

        with pytest.raises(ValueError, match="confidence_threshold must be between"):
            ScanPIIAnalysisConfig(confidence_threshold=-0.1)


class TestScanConfig:
    """Tests for ScanConfig."""

    def test_default_values(self):
        """Test default values."""
        config = ScanConfig()

        assert isinstance(config.discovery, ScanDiscoveryConfig)
        assert isinstance(config.sampling, ScanSamplingConfig)
        assert isinstance(config.pii_analysis, ScanPIIAnalysisConfig)

    def test_with_custom_configs(self):
        """Test with custom sub-configs."""
        discovery = ScanDiscoveryConfig(include_patterns=["^users.*"])
        sampling = ScanSamplingConfig(sample_size=500)
        pii_analysis = ScanPIIAnalysisConfig(confidence_threshold=0.9)

        config = ScanConfig(discovery=discovery, sampling=sampling, pii_analysis=pii_analysis)

        assert config.discovery.include_patterns == ["^users.*"]
        assert config.sampling.sample_size == 500
        assert config.pii_analysis.confidence_threshold == 0.9


class TestRepConfig:
    """Tests for RepConfig (root config)."""

    def test_with_scan_only(self):
        """Test RepConfig with only scan section."""
        scan = ScanConfig()
        config = Config(scan=scan)

        assert config.scan is not None
        assert config.replication is None

    def test_with_replication_only(self):
        """Test RepConfig with only replication section."""
        # We'll use a mock here since ReplicationConfig is complex
        config = Config(replication={"defaults": {}, "collections": {}})

        assert config.scan is None
        assert config.replication is not None

    def test_with_both_sections(self):
        """Test RepConfig with both sections."""
        scan = ScanConfig()
        replication = {"defaults": {}, "collections": {}}

        config = Config(scan=scan, replication=replication)

        assert config.scan is not None
        assert config.replication is not None

    def test_empty_config_raises_error(self):
        """Test that empty config raises error."""
        with pytest.raises(ValueError, match="at least one"):
            Config()


class TestConfigLoading:
    """Tests for loading and saving configuration files."""

    def test_load_scan_only(self):
        """Test loading scan section only."""
        yaml_content = """
scan:
  discovery:
    include_patterns:
      - '^users.*'
    exclude_patterns:
      - '^system\\\\.'
  sampling:
    sample_size: 500
    sample_strategy: stratified
  pii_analysis:
    enabled: true
    confidence_threshold: 0.9
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            config = load_config(config_path)

            assert config.scan is not None
            assert config.replication is not None  # defaults
            assert config.scan.discovery.include_patterns == ["^users.*"]
            assert config.scan.pii_analysis.confidence_threshold == 0.9
            assert config.scan.sampling.sample_size == 500
        finally:
            config_path.unlink()

    def test_load_replication_only(self):
        """Test loading replication section only."""
        yaml_content = """
replication:
  discovery:
    replicate_all: true
  performance:
    batch_size: 1000
  collections:
    users:
      cursor_field: updated_at
      write_disposition: merge
      primary_key: _id
      pii_anonymized_fields:
        email: fake
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            config = load_config(config_path)

            assert config.scan is not None  # defaults
            assert config.replication is not None
            assert config.replication.discovery.replicate_all is True
            assert "users" in config.replication.collections
        finally:
            config_path.unlink()

    def test_load_scan_config_only(self):
        """Test loading only scan config."""
        yaml_content = """
scan:
  pii_analysis:
    enabled: true
replication:
  defaults:
    replicate_all: true
  collections: {}
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            scan_config = load_scan_config(config_path)

            assert isinstance(scan_config, ScanConfig)
            assert scan_config.pii_analysis.enabled is True
        finally:
            config_path.unlink()

    def test_load_replication_config_only(self):
        """Test loading only replication config."""
        yaml_content = """
scan:
  pii_analysis:
    enabled: true
replication:
  discovery:
    replicate_all: true
  collections: {}
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            rep_config = load_replication_config(config_path)

            assert rep_config.discovery.replicate_all is True
        finally:
            config_path.unlink()

    def test_save_and_load_roundtrip(self):
        """Test saving and loading config roundtrip."""
        # Create config
        scan = ScanConfig(
            discovery=ScanDiscoveryConfig(include_patterns=["^users.*"]),
            sampling=ScanSamplingConfig(sample_size=500),
            pii_analysis=ScanPIIAnalysisConfig(confidence_threshold=0.9),
        )
        config = Config(scan=scan)

        # Save to temp file
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            save_config(config, temp_path)

            # Load back
            loaded = load_config(temp_path)

            assert loaded.scan is not None
            assert loaded.scan.discovery.include_patterns == ["^users.*"]
            assert loaded.scan.sampling.sample_size == 500
            assert loaded.scan.pii_analysis.confidence_threshold == 0.9
        finally:
            temp_path.unlink()

    def test_load_nonexistent_file_raises_error(self):
        """Test that loading nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/file.yaml"))


class TestPIIFieldAnonymization:
    """Tests for PIIFieldAnonymization model."""

    def test_create_with_all_fields(self):
        """Test creating PIIFieldAnonymization with all required fields."""
        pii_config = PIIFieldAnonymization(
            field="email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
        )

        assert pii_config.field == "email"
        assert pii_config.operator == "mask_email"
        assert pii_config.params == {"entity_type": "EMAIL_ADDRESS"}

    def test_params_is_optional(self):
        """Test that params field is optional."""
        pii_config = PIIFieldAnonymization(
            field="email",
            operator="mask_email",
            # params is optional
        )
        assert pii_config.params is None

    def test_nested_field_path(self):
        """Test PIIFieldAnonymization with nested field path."""
        pii_config = PIIFieldAnonymization(
            field="user.profile.email",
            operator="mask_email",
            params={"entity_type": "EMAIL_ADDRESS"},
        )

        assert pii_config.field == "user.profile.email"
        assert pii_config.operator == "mask_email"
        assert pii_config.params == {"entity_type": "EMAIL_ADDRESS"}

    def test_various_operators(self):
        """Test PIIFieldAnonymization with various operator types."""
        # Mask operator
        pii1 = PIIFieldAnonymization(
            field="phone", operator="mask_phone", params={"entity_type": "PHONE_NUMBER"}
        )
        assert pii1.operator == "mask_phone"

        # Fake operator
        pii2 = PIIFieldAnonymization(
            field="ssn", operator="fake_ssn", params={"entity_type": "US_SSN"}
        )
        assert pii2.operator == "fake_ssn"

        # Smart operator (should not be used in configs, but valid)
        pii3 = PIIFieldAnonymization(
            field="email", operator="smart_mask", params={"entity_type": "EMAIL_ADDRESS"}
        )
        assert pii3.operator == "smart_mask"

        # Hash operator
        pii4 = PIIFieldAnonymization(
            field="credit_card", operator="hash", params={"entity_type": "CREDIT_CARD"}
        )
        assert pii4.operator == "hash"

    def test_unknown_entity_type(self):
        """Test PIIFieldAnonymization with UNKNOWN entity type in params."""
        pii_config = PIIFieldAnonymization(
            field="some_field", operator="mask", params={"entity_type": "UNKNOWN"}
        )

        assert pii_config.params == {"entity_type": "UNKNOWN"}


class TestCollectionConfigPIIAnonymization:
    """Tests for pii_anonymization field in CollectionConfig."""

    def test_pii_anonymization_list_format(self):
        """Test using new pii_anonymization list format."""
        config = CollectionConfig(
            name="users",
            pii_anonymization=[
                PIIFieldAnonymization(
                    field="email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
                ),
                PIIFieldAnonymization(
                    field="phone", operator="mask_phone", params={"entity_type": "PHONE_NUMBER"}
                ),
            ],
        )

        assert len(config.pii_anonymization) == 2
        assert config.pii_anonymization[0].field == "email"
        assert config.pii_anonymization[0].operator == "mask_email"
        assert config.pii_anonymization[0].params == {"entity_type": "EMAIL_ADDRESS"}
        assert config.pii_anonymization[1].field == "phone"
        assert config.pii_anonymization[1].operator == "mask_phone"
        assert config.pii_anonymization[1].params == {"entity_type": "PHONE_NUMBER"}

    def test_backward_compatibility_migration(self):
        """Test backward compatibility with old pii_anonymized_fields format."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            config = CollectionConfig(
                name="users", pii_anonymized_fields={"email": "mask_email", "phone": "mask_phone"}
            )

            # Should have triggered deprecation warning
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()

        # Should have auto-migrated to new format
        assert len(config.pii_anonymization) == 2

        # Find email and phone fields (order not guaranteed)
        email_config = next((p for p in config.pii_anonymization if p.field == "email"), None)
        phone_config = next((p for p in config.pii_anonymization if p.field == "phone"), None)

        assert email_config is not None
        assert email_config.operator == "mask_email"
        assert email_config.params is None  # Legacy format has no params

        assert phone_config is not None
        assert phone_config.operator == "mask_phone"
        assert phone_config.params is None  # Legacy format has no params

    def test_empty_pii_anonymization_list(self):
        """Test collection with empty pii_anonymization list."""
        config = CollectionConfig(name="users", pii_anonymization=[])

        assert config.pii_anonymization == []

    def test_default_empty_list(self):
        """Test that pii_anonymization defaults to empty list."""
        config = CollectionConfig(name="users")

        assert config.pii_anonymization == []


class TestConfigTemplateSerialization:
    """Tests for config template serialization with pii_anonymization."""

    def test_save_and_load_with_pii_anonymization(self):
        """Test saving and loading config with new pii_anonymization format."""
        from mongo_replication.config.models import ReplicationConfig, Config
        from mongo_replication.config.manager import save_config, load_config

        # Create config with new pii_anonymization format
        collections_dict = {
            "users": {
                "cursor_field": "updated_at",
                "write_disposition": "merge",
                "primary_key": "_id",
                "pii_anonymization": [
                    {
                        "field": "email",
                        "operator": "mask_email",
                        "params": {"entity_type": "EMAIL_ADDRESS"},
                    },
                    {
                        "field": "phone",
                        "operator": "mask_phone",
                        "params": {"entity_type": "PHONE_NUMBER"},
                    },
                ],
            }
        }

        replication_config = ReplicationConfig(collections=collections_dict)
        config = Config(replication=replication_config)

        # Save to temp file
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            save_config(config, temp_path)

            # Verify YAML content includes params with entity_type
            with open(temp_path, "r") as f:
                yaml_content = f.read()
                assert "pii_anonymization:" in yaml_content
                assert "params:" in yaml_content
                assert "entity_type: EMAIL_ADDRESS" in yaml_content
                assert "entity_type: PHONE_NUMBER" in yaml_content

            # Load back and verify
            loaded_config = load_config(temp_path)
            users_config = loaded_config.replication.collections.root["users"]

            assert len(users_config.pii_anonymization) == 2

            # Check first PII field
            email_pii = users_config.pii_anonymization[0]
            assert email_pii.field == "email"
            assert email_pii.operator == "mask_email"
            assert email_pii.params == {"entity_type": "EMAIL_ADDRESS"}

            # Check second PII field
            phone_pii = users_config.pii_anonymization[1]
            assert phone_pii.field == "phone"
            assert phone_pii.operator == "mask_phone"
            assert phone_pii.params == {"entity_type": "PHONE_NUMBER"}
        finally:
            temp_path.unlink()

    def test_template_handles_both_old_and_new_format(self):
        """Test that template can handle both old and new PII formats."""
        from mongo_replication.config.models import ReplicationConfig, Config
        from mongo_replication.config.manager import save_config

        # Test with new format (already tested above)
        # Now test that old format still works if someone has it
        collections_dict = {
            "users": {
                "write_disposition": "merge",
                "primary_key": "_id",
                "pii_anonymized_fields": {"email": "fake", "phone": "redact"},
            }
        }

        import warnings

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            replication_config = ReplicationConfig(collections=collections_dict)
            config = Config(replication=replication_config)

        # Save to temp file
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            save_config(config, temp_path)

            # Should still save successfully (even though it auto-migrates)
            with open(temp_path, "r") as f:
                yaml_content = f.read()
                # After migration, should have new format
                assert "pii_anonymization:" in yaml_content
                # Legacy format doesn't have params, so no params section should appear
                assert "params:" not in yaml_content
        finally:
            temp_path.unlink()
