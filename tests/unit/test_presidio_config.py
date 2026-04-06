"""Unit tests for Presidio YAML configuration support."""

from textwrap import dedent

import pytest

from mongo_replication.engine.pii.presidio_analyzer import PresidioAnalyzer


class TestPresidioConfigPathResolution:
    """Test path resolution for Presidio configuration files."""

    def test_absolute_path_resolution(self, tmp_path):
        """Test that absolute paths are used as-is."""
        # Create a test config file
        config_file = tmp_path / "test_presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            default_score_threshold: 0.5
            recognizers:
              - name: EmailRecognizer
                supported_entity: EMAIL_ADDRESS
                supported_languages: [en]
        """)
        )

        analyzer = PresidioAnalyzer()
        resolved_path = analyzer._resolve_config_path(str(config_file))

        assert resolved_path == config_file
        assert resolved_path.exists()

    def test_absolute_path_not_found(self):
        """Test that FileNotFoundError is raised for non-existent absolute path."""
        analyzer = PresidioAnalyzer()

        with pytest.raises(FileNotFoundError) as exc_info:
            analyzer._resolve_config_path("/non/existent/presidio.yaml")

        assert "not found at absolute path" in str(exc_info.value)

    def test_relative_path_resolution_cwd(self, tmp_path, monkeypatch):
        """Test that relative paths are resolved relative to cwd first."""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        # Create config in cwd
        config_file = tmp_path / "presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizers: []
        """)
        )

        analyzer = PresidioAnalyzer()
        resolved_path = analyzer._resolve_config_path("presidio.yaml")

        assert resolved_path == config_file
        assert resolved_path.exists()

    def test_relative_path_resolution_config_dir(self, tmp_path, monkeypatch):
        """Test that relative paths fall back to config/ directory."""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        # Create config in config/ subdirectory
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizers: []
        """)
        )

        analyzer = PresidioAnalyzer()
        resolved_path = analyzer._resolve_config_path("presidio.yaml")

        assert resolved_path == config_file
        assert resolved_path.exists()

    def test_relative_path_not_found(self, tmp_path, monkeypatch):
        """Test that FileNotFoundError is raised when file not found anywhere."""
        monkeypatch.chdir(tmp_path)

        analyzer = PresidioAnalyzer()

        with pytest.raises(FileNotFoundError) as exc_info:
            analyzer._resolve_config_path("nonexistent.yaml")

        assert "not found" in str(exc_info.value)
        assert "Searched locations:" in str(exc_info.value)


class TestPresidioConfigLoading:
    """Test loading Presidio configuration from YAML."""

    def test_load_valid_yaml_config(self, tmp_path):
        """Test loading a valid YAML configuration."""
        config_file = tmp_path / "valid_presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg

            supported_languages: [en]
            default_score_threshold: 0.35

            recognizers:
              - name: EmailRecognizer
                supported_entity: EMAIL_ADDRESS
                supported_languages: [en]

              - name: PhoneRecognizer
                supported_entity: PHONE_NUMBER
                supported_languages: [en]
        """)
        )

        analyzer = PresidioAnalyzer()
        engine = analyzer._create_from_yaml(config_file)

        assert engine is not None
        assert hasattr(engine, "analyze")
        assert len(engine.registry.recognizers) >= 2  # At least EMAIL and PHONE

    def test_load_custom_recognizer(self, tmp_path):
        """Test loading configuration with custom recognizer."""
        config_file = tmp_path / "custom_presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg

            supported_languages: [en]
            default_score_threshold: 0.35

            recognizers:
              - name: EmployeeIdRecognizer
                supported_entity: EMPLOYEE_ID
                supported_languages: [en]
                patterns:
                  - name: emp_pattern
                    regex: "\\\\bEMP-\\\\d{5}\\\\b"
                    score: 0.7
        """)
        )

        analyzer = PresidioAnalyzer()
        engine = analyzer._create_from_yaml(config_file)

        assert engine is not None
        # Check that EMPLOYEE_ID is in supported entities
        supported_entities = engine.get_supported_entities(language="en")
        assert "EMPLOYEE_ID" in supported_entities

    def test_load_invalid_yaml_raises_error(self, tmp_path):
        """Test that invalid YAML raises ValueError with helpful message."""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: syntax:")

        analyzer = PresidioAnalyzer()

        with pytest.raises(ValueError) as exc_info:
            analyzer._create_from_yaml(config_file)

        assert "Failed to load Presidio configuration" in str(exc_info.value)


class TestPresidioAnalyzerCaching:
    """Test analyzer caching by config path."""

    def test_analyzer_cached_by_config_path(self, tmp_path):
        """Test that analyzer is cached per config path."""
        config_file = tmp_path / "cached_presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizers:
              - name: EmailRecognizer
                supported_entity: EMAIL_ADDRESS
                supported_languages: [en]
        """)
        )

        analyzer = PresidioAnalyzer()

        # First call - creates analyzer
        engine1 = analyzer.get_analyzer(presidio_config_path=str(config_file))

        # Second call - should return cached analyzer
        engine2 = analyzer.get_analyzer(presidio_config_path=str(config_file))

        # Should be the same object (cached)
        assert engine1 is engine2

    def test_different_configs_not_cached(self, tmp_path):
        """Test that different config paths create different analyzers."""
        config1 = tmp_path / "config1.yaml"
        config1.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizers:
              - name: EmailRecognizer
                supported_entity: EMAIL_ADDRESS
                supported_languages: [en]
        """)
        )

        config2 = tmp_path / "config2.yaml"
        config2.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizers:
              - name: PhoneRecognizer
                supported_entity: PHONE_NUMBER
                supported_languages: [en]
        """)
        )

        analyzer = PresidioAnalyzer()

        engine1 = analyzer.get_analyzer(presidio_config_path=str(config1))
        engine2 = analyzer.get_analyzer(presidio_config_path=str(config2))

        # Should be different objects
        assert engine1 is not engine2

    def test_default_config_cached_separately(self, tmp_path):
        """Test that default config (None) is cached separately."""
        config_file = tmp_path / "custom.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizers:
              - name: EmailRecognizer
                supported_entity: EMAIL_ADDRESS
                supported_languages: [en]
        """)
        )

        analyzer = PresidioAnalyzer()

        # Get default analyzer (no config)
        default_engine1 = analyzer.get_analyzer(presidio_config_path=None)
        default_engine2 = analyzer.get_analyzer(presidio_config_path=None)

        # Get custom config analyzer
        custom_engine = analyzer.get_analyzer(presidio_config_path=str(config_file))

        # Default should be cached
        assert default_engine1 is default_engine2

        # Custom should be different from default
        assert custom_engine is not default_engine1


class TestPresidioDocumentAnalysis:
    """Test document analysis with custom Presidio config."""

    def test_analyze_with_custom_recognizer(self, tmp_path):
        """Test that custom recognizers detect PII correctly."""
        config_file = tmp_path / "employee_presidio.yaml"
        config_file.write_text(
            dedent("""
            nlp_engine_name: spacy
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg

            supported_languages: [en]
            default_score_threshold: 0.35

            recognizers:
              - name: EmployeeIdRecognizer
                supported_entity: EMPLOYEE_ID
                supported_languages: [en]
                patterns:
                  - name: emp_pattern
                    regex: "\\\\bEMP-\\\\d{5}\\\\b"
                    score: 0.7
                context:
                  - "employee"
                  - "staff"
        """)
        )

        analyzer = PresidioAnalyzer()

        # Document with employee ID
        document = {"name": "John Doe", "employee_id": "EMP-12345", "department": "Engineering"}

        pii_map = analyzer.analyze_document(
            document=document,
            confidence_threshold=0.5,
            presidio_config_path=str(config_file),
        )

        # Should detect EMPLOYEE_ID
        assert "employee_id" in pii_map
        entity_type, score = pii_map["employee_id"]
        assert entity_type == "EMPLOYEE_ID"
        assert score >= 0.5

    def test_analyze_without_custom_config(self):
        """Test analysis with default config (no custom recognizers)."""
        analyzer = PresidioAnalyzer()

        document = {"email": "test@example.com", "phone": "+1-555-123-4567", "name": "John Doe"}

        pii_map = analyzer.analyze_document(
            document=document,
            confidence_threshold=0.5,
            presidio_config_path=None,  # Use default
        )

        # Should detect EMAIL_ADDRESS with default config
        assert "email" in pii_map
        entity_type, _ = pii_map["email"]
        assert entity_type == "EMAIL_ADDRESS"


class TestConfigModelIntegration:
    """Test integration with ScanPIIConfig model."""

    def test_presidio_config_field_in_model(self):
        """Test that presidio_config field exists in ScanPIIConfig."""
        from mongo_replication.config.models import ScanPIIConfig

        # Create config with presidio_config
        config = ScanPIIConfig(
            enabled=True,
            confidence_threshold=0.85,
            presidio_config="config/custom_presidio.yaml",
        )

        assert config.presidio_config == "config/custom_presidio.yaml"

    def test_presidio_config_defaults_to_none(self):
        """Test that presidio_config defaults to None."""
        from mongo_replication.config.models import ScanPIIConfig

        config = ScanPIIConfig(enabled=True)

        assert config.presidio_config is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
