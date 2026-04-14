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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            default_score_threshold: 0.5
            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg

            supported_languages: [en]
            default_score_threshold: 0.35

            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg

            supported_languages: [en]
            default_score_threshold: 0.35

            recognizer_registry:
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
        """Test that invalid YAML structure raises ValueError with helpful message."""
        config_file = tmp_path / "invalid.yaml"
        # Invalid structure - missing required keys
        config_file.write_text(
            dedent("""
            # Invalid - missing nlp_configuration and recognizer_registry
            invalid_key: some_value
            another_bad_key: another_value
        """)
        )

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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizer_registry:
              recognizers:
                - name: EmailRecognizer
                  supported_entity: EMAIL_ADDRESS
                  supported_languages: [en]
        """)
        )

        config2 = tmp_path / "config2.yaml"
        config2.write_text(
            dedent("""
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg
            supported_languages: [en]
            recognizer_registry:
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
            nlp_configuration:
              nlp_engine_name: spacy
              models:
                - lang_code: en
                  model_name: en_core_web_lg

            supported_languages: [en]
            default_score_threshold: 0.35

            recognizer_registry:
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
    """Test integration with ScanPIIAnalysisConfig model."""

    def test_presidio_config_field_in_model(self):
        """Test that presidio_config field exists in ScanPIIAnalysisConfig."""
        from mongo_replication.config.models import ScanPIIAnalysisConfig

        # Create config with presidio_config
        config = ScanPIIAnalysisConfig(
            enabled=True,
            confidence_threshold=0.85,
            presidio_config="config/custom_presidio.yaml",
        )

        assert config.presidio_config == "config/custom_presidio.yaml"

    def test_presidio_config_defaults_to_none(self):
        """Test that presidio_config defaults to None."""
        from mongo_replication.config.models import ScanPIIAnalysisConfig

        config = ScanPIIAnalysisConfig(enabled=True)

        assert config.presidio_config is None


class TestPresidioConfigRegistry:
    """Test PresidioConfig methods for querying the anonymizer registry."""

    def test_get_supported_entity_types(self):
        """Test getting all supported entity types from registry."""
        from mongo_replication.config.presidio_config import PresidioConfig

        # Load default config
        config = PresidioConfig()
        entity_types = config.get_supported_entity_types()

        # Should include common entity types
        assert "EMAIL_ADDRESS" in entity_types
        assert "PHONE_NUMBER" in entity_types
        assert "PERSON" in entity_types
        assert "CREDIT_CARD" in entity_types
        assert "US_SSN" in entity_types

        # Should be sorted
        assert entity_types == sorted(entity_types)

    def test_get_operator_examples(self):
        """Test getting examples for a specific operator."""
        from mongo_replication.config.presidio_config import PresidioConfig

        config = PresidioConfig()

        # Test mask_email examples
        examples = config.get_operator_examples("mask_email")
        assert len(examples) > 0
        assert "input" in examples[0]
        assert "output" in examples[0]
        assert "entity_type" in examples[0]

        # Test fake_phone examples
        examples = config.get_operator_examples("fake_phone")
        assert len(examples) > 0

        # Test non-existent operator
        examples = config.get_operator_examples("nonexistent_operator")
        assert examples == []

    def test_get_operator_examples_with_entity_filter(self):
        """Test filtering examples by entity type."""
        from mongo_replication.config.presidio_config import PresidioConfig

        config = PresidioConfig()

        # Test smart_mask which supports multiple entity types
        all_examples = config.get_operator_examples("smart_mask")
        assert len(all_examples) > 1  # Should have examples for multiple entity types

        # Filter by EMAIL_ADDRESS
        email_examples = config.get_operator_examples("smart_mask", entity_type="EMAIL_ADDRESS")
        assert len(email_examples) > 0
        assert all(ex.get("entity_type") == "EMAIL_ADDRESS" for ex in email_examples)

        # Filter by PHONE_NUMBER
        phone_examples = config.get_operator_examples("smart_mask", entity_type="PHONE_NUMBER")
        assert len(phone_examples) > 0
        assert all(ex.get("entity_type") == "PHONE_NUMBER" for ex in phone_examples)

        # Filter by non-existent entity type
        no_examples = config.get_operator_examples("smart_mask", entity_type="NONEXISTENT_TYPE")
        assert no_examples == []

        # Test single-entity operator with filter
        mask_email_examples = config.get_operator_examples(
            "mask_email", entity_type="EMAIL_ADDRESS"
        )
        assert len(mask_email_examples) > 0
        assert all(ex.get("entity_type") == "EMAIL_ADDRESS" for ex in mask_email_examples)

    def test_smart_operators_have_complete_examples(self):
        """Test that smart_mask and smart_fake have examples for all supported entity types."""
        from mongo_replication.config.presidio_config import PresidioConfig

        config = PresidioConfig()

        # Get the list of supported entity types for smart operators
        smart_operators = ["smart_mask", "smart_fake"]
        expected_entities = [
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "CREDIT_CARD",
            "US_SSN",
            "IP_ADDRESS",
            "IBAN_CODE",
            "PERSON",
            "LOCATION",
            "US_BANK_ACCOUNT",
            "CA_BANK_ACCOUNT",
        ]

        for operator in smart_operators:
            all_examples = config.get_operator_examples(operator)

            # Extract entity types from examples
            example_entity_types = {ex.get("entity_type") for ex in all_examples}

            # Verify all expected entity types have examples
            for entity_type in expected_entities:
                assert entity_type in example_entity_types, (
                    f"{operator} missing example for {entity_type}"
                )

                # Verify we can filter by this entity type
                filtered = config.get_operator_examples(operator, entity_type=entity_type)
                assert len(filtered) > 0, f"{operator} has no examples for {entity_type}"
                assert all(ex.get("entity_type") == entity_type for ex in filtered)

    def test_get_operators_for_entity_type(self):
        """Test getting operators that support a specific entity type."""
        from mongo_replication.config.presidio_config import PresidioConfig

        config = PresidioConfig()

        # Test EMAIL_ADDRESS
        operators = config.get_operators_for_entity_type("EMAIL_ADDRESS")
        assert "mask_email" in operators
        assert "fake_email" in operators

        # Test PHONE_NUMBER
        operators = config.get_operators_for_entity_type("PHONE_NUMBER")
        assert "mask_phone" in operators
        assert "fake_phone" in operators

        # Test non-existent entity type
        operators = config.get_operators_for_entity_type("NONEXISTENT_TYPE")
        assert operators == []

    def test_entity_types_from_custom_config(self, tmp_path):
        """Test getting entity types from custom config."""
        from mongo_replication.config.presidio_config import PresidioConfig

        # Create custom config with limited operators
        config_file = tmp_path / "custom_presidio.yaml"
        config_file.write_text(
            dedent("""
            anonymizer_registry:
              custom_email_mask:
                description: "Custom email masking"
                class: MaskEmailOperator
                supported_entities: [EMAIL_ADDRESS, CUSTOM_EMAIL]
                examples:
                  - input: "test@example.com"
                    output: "t***@example.com"
              custom_phone_mask:
                description: "Custom phone masking"
                class: MaskPhoneOperator
                supported_entities: [PHONE_NUMBER]
                examples:
                  - input: "555-1234"
                    output: "***-1234"
        """)
        )

        config = PresidioConfig(str(config_file))
        entity_types = config.get_supported_entity_types()

        assert "EMAIL_ADDRESS" in entity_types
        assert "CUSTOM_EMAIL" in entity_types
        assert "PHONE_NUMBER" in entity_types
        assert len(entity_types) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
