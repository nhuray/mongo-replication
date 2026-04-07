"""Unit tests for new configuration models (ScanConfig, RepConfig)."""

import pytest
from pathlib import Path
from tempfile import NamedTemporaryFile

from pydantic import ValidationError

from mongo_replication.config.models import (
    ScanConfig,
    ScanDiscoveryConfig,
    ScanPIIConfig,
    Config,
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


class TestScanPIIConfig:
    """Tests for ScanPIIConfig."""

    def test_default_values(self):
        """Test default values."""
        config = ScanPIIConfig()

        assert config.enabled is True
        assert config.confidence_threshold == 0.85
        assert "EMAIL_ADDRESS" in config.entity_types
        assert "PHONE_NUMBER" in config.entity_types
        assert config.sample_size == 1000
        assert config.sample_strategy == "stratified"
        assert config.default_strategies["EMAIL_ADDRESS"] == "fake"
        assert config.allowlist == []

    def test_custom_values(self):
        """Test custom values."""
        config = ScanPIIConfig(
            enabled=False,
            confidence_threshold=0.9,
            entity_types=["EMAIL_ADDRESS"],
            sample_size=500,
            sample_strategy="random",
            default_strategies={"EMAIL_ADDRESS": "hash"},
            allowlist=["metadata.*"],
        )

        assert config.enabled is False
        assert config.confidence_threshold == 0.9
        assert config.entity_types == ["EMAIL_ADDRESS"]
        assert config.sample_size == 500
        assert config.sample_strategy == "random"
        assert config.default_strategies == {"EMAIL_ADDRESS": "hash"}
        assert config.allowlist == ["metadata.*"]

    def test_invalid_confidence_threshold_raises_error(self):
        """Test that invalid confidence threshold raises error."""
        with pytest.raises(ValueError, match="confidence_threshold must be between"):
            ScanPIIConfig(confidence_threshold=1.5)

        with pytest.raises(ValueError, match="confidence_threshold must be between"):
            ScanPIIConfig(confidence_threshold=-0.1)

    def test_invalid_sample_size_raises_error(self):
        """Test that invalid sample size raises error."""
        with pytest.raises(ValueError, match="sample_size must be"):
            ScanPIIConfig(sample_size=0)

        with pytest.raises(ValueError, match="sample_size must be"):
            ScanPIIConfig(sample_size=-10)

    def test_invalid_sample_strategy_raises_error(self):
        """Test that invalid sample strategy raises error."""
        with pytest.raises(ValidationError, match="Input should be 'random' or 'stratified'"):
            ScanPIIConfig(sample_strategy="invalid")


class TestScanConfig:
    """Tests for ScanConfig."""

    def test_default_values(self):
        """Test default values."""
        config = ScanConfig()

        assert isinstance(config.discovery, ScanDiscoveryConfig)
        assert isinstance(config.pii, ScanPIIConfig)

    def test_with_custom_configs(self):
        """Test with custom sub-configs."""
        discovery = ScanDiscoveryConfig(include_patterns=["^users.*"])
        pii = ScanPIIConfig(confidence_threshold=0.9)

        config = ScanConfig(discovery=discovery, pii=pii)

        assert config.discovery.include_patterns == ["^users.*"]
        assert config.pii.confidence_threshold == 0.9


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

    def test_load_new_format_scan_only(self):
        """Test loading new format with scan section only."""
        yaml_content = """
scan:
  discovery:
    include_patterns:
      - '^users.*'
    exclude_patterns:
      - '^system\\\\.'
  pii:
    enabled: true
    confidence_threshold: 0.9
    sample_size: 500
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            config = load_config(config_path)

            assert config.scan is not None
            assert config.replication is None
            assert config.scan.discovery.include_patterns == ["^users.*"]
            assert config.scan.pii.confidence_threshold == 0.9
            assert config.scan.pii.sample_size == 500
        finally:
            config_path.unlink()

    def test_load_new_format_replication_only(self):
        """Test loading new format with replication section only."""
        yaml_content = """
replication:
  defaults:
    replicate_all: true
    batch_size: 1000
  collections:
    users:
      cursor_field: updated_at
      write_disposition: merge
      primary_key: _id
      pii_fields:
        email: fake
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            config = load_config(config_path)

            assert config.scan is None
            assert config.replication is not None
            assert config.replication.defaults["replicate_all"] is True
            assert "users" in config.replication.collections
        finally:
            config_path.unlink()

    def test_load_old_format_auto_migrates(self):
        """Test that old format is auto-migrated with warning."""
        yaml_content = """
defaults:
  replicate_all: true
  batch_size: 1000

collections:
  users:
    cursor_field: updated_at
    write_disposition: merge
    primary_key: _id
    pii_fields:
      email: hash
"""
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            config_path = Path(f.name)

        try:
            with pytest.warns(DeprecationWarning, match="deprecated format"):
                config = load_config(config_path)

            # Should be migrated to replication section
            assert config.replication is not None
            assert config.replication.defaults["replicate_all"] is True
            assert "users" in config.replication.collections
        finally:
            config_path.unlink()

    def test_load_scan_config_only(self):
        """Test loading only scan config."""
        yaml_content = """
scan:
  pii:
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
            assert scan_config.pii.enabled is True
        finally:
            config_path.unlink()

    def test_load_scan_config_missing_raises_error(self):
        """Test that loading scan config from file without scan section raises error."""
        yaml_content = """
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
            with pytest.raises(ValueError, match="no 'scan' section"):
                load_scan_config(config_path)
        finally:
            config_path.unlink()

    def test_load_replication_config_only(self):
        """Test loading only replication config."""
        yaml_content = """
scan:
  pii:
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
            rep_config = load_replication_config(config_path)

            assert rep_config.defaults["replicate_all"] is True
        finally:
            config_path.unlink()

    def test_save_and_load_roundtrip(self):
        """Test saving and loading config roundtrip."""
        # Create config
        scan = ScanConfig(
            discovery=ScanDiscoveryConfig(include_patterns=["^users.*"]),
            pii=ScanPIIConfig(confidence_threshold=0.9),
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
            assert loaded.scan.pii.confidence_threshold == 0.9
        finally:
            temp_path.unlink()

    def test_load_nonexistent_file_raises_error(self):
        """Test that loading nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/file.yaml"))
