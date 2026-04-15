"""Unit tests for Presidio PII anonymizer with operator-based configuration."""

import pytest

from mongo_replication.engine.pii import (
    PresidioAnonymizer,
    apply_anonymization,
    get_anonymizer,
)

# Test entity strategies for unit tests (avoids loading default config)
TEST_ENTITY_STRATEGIES = {
    "EMAIL_ADDRESS": "smart_mask",
    "PERSON": "replace",
    "PHONE_NUMBER": "mask",
    "LOCATION": "mask",
    "US_SSN": "mask",
    "SSN": "mask",
    "CREDIT_CARD": "hash",
    "IBAN_CODE": "hash",
    "CRYPTO": "hash",
    "US_PASSPORT": "hash",
    "US_BANK_ACCOUNT": "hash",
    "US_DRIVER_LICENSE": "mask",
    "UK_NHS": "mask",
    "DATE_TIME": "mask",
    "IP_ADDRESS": "mask",
    "URL": "mask",
    "DEFAULT": "redact",
}


@pytest.fixture
def anonymizer():
    """Create a PresidioAnonymizer instance for testing."""
    return PresidioAnonymizer()


@pytest.fixture
def sample_doc():
    """Sample MongoDB document with PII."""
    return {
        "_id": "doc123",
        "name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "555-123-4567",
        "ssn": "123-45-6789",
        "address": {"street": "123 Main St", "city": "New York"},
        "contacts": [
            {"name": "Jane Doe", "email": "jane@example.com"},
            {"name": "Bob Smith", "email": "bob@example.com"},
        ],
    }


class TestPresidioAnonymizerInitialization:
    """Test anonymizer initialization and configuration."""

    def test_initialization(self):
        """Test anonymizer initializes successfully."""
        anon = PresidioAnonymizer()
        assert anon.anonymizer_engine is not None
        assert anon.operator_configs is not None
        assert anon.presidio_config is not None

    def test_test_entity_strategies(self):
        """Test TEST_ENTITY_STRATEGIES has reasonable defaults for testing."""
        # Key entity types should be present
        assert "EMAIL_ADDRESS" in TEST_ENTITY_STRATEGIES
        assert "PERSON" in TEST_ENTITY_STRATEGIES
        assert "PHONE_NUMBER" in TEST_ENTITY_STRATEGIES
        assert "US_SSN" in TEST_ENTITY_STRATEGIES
        assert "CREDIT_CARD" in TEST_ENTITY_STRATEGIES

        # Should have a default fallback
        assert "DEFAULT" in TEST_ENTITY_STRATEGIES

        # Check some specific strategy assignments
        assert TEST_ENTITY_STRATEGIES["EMAIL_ADDRESS"] == "smart_mask"
        assert TEST_ENTITY_STRATEGIES["PERSON"] == "replace"
        assert TEST_ENTITY_STRATEGIES["CREDIT_CARD"] == "hash"

    def test_singleton_get_anonymizer(self):
        """Test get_anonymizer returns singleton instance."""
        anon1 = get_anonymizer()
        anon2 = get_anonymizer()
        assert anon1 is anon2  # Same instance

    def test_custom_config_path(self):
        """Test that invalid config path raises appropriate error."""
        # PresidioConfig raises FileNotFoundError for invalid paths
        with pytest.raises(FileNotFoundError):
            PresidioAnonymizer(presidio_config_path="/tmp/nonexistent.yaml")


class TestApplyAnonymization:
    """Test document anonymization with various scenarios."""

    def test_anonymize_simple_document(self, anonymizer):
        """Test anonymizing a simple document with manual overrides."""
        doc = {"email": "test@example.com", "name": "John Doe"}
        pii_field_strategy = {
            "email": "smart_mask",
            "name": "fake_name",
        }

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Original document should not be modified
        assert doc["email"] == "test@example.com"
        assert doc["name"] == "John Doe"

        # Result should be anonymized
        assert result["email"] != "test@example.com"
        assert result["name"] != "John Doe"  # fake_name generates different name

    def test_anonymize_nested_document(self, anonymizer):
        """Test anonymizing nested fields."""
        doc = {
            "user": {
                "email": "test@example.com",
                "profile": {"name": "John Doe"},
            }
        }
        pii_field_strategy = {
            "user.email": "smart_mask",
            "user.profile.name": "fake_name",
        }

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Nested fields should be anonymized
        assert result["user"]["email"] != "test@example.com"
        assert result["user"]["profile"]["name"] != "John Doe"

    def test_anonymize_array_fields(self, anonymizer):
        """Test anonymizing fields within arrays."""
        doc = {
            "contacts": [
                {"email": "alice@example.com", "name": "Alice"},
                {"email": "bob@example.com", "name": "Bob"},
            ]
        }
        pii_field_strategy = {
            "contacts.email": "smart_mask",
            "contacts.name": "fake_name",
        }

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # All array elements should be anonymized
        assert result["contacts"][0]["email"] != "alice@example.com"
        assert result["contacts"][1]["email"] != "bob@example.com"
        assert result["contacts"][0]["name"] != "Alice"
        assert result["contacts"][1]["name"] != "Bob"

    def test_pii_field_strategy_only(self, anonymizer):
        """Test anonymizing with PII field strategies."""
        doc = {"field1": "value1", "field2": "value2"}
        pii_field_strategy = {
            "field1": "hash",
            "field2": "fake_name",  # Use custom operator that doesn't need params
        }

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Fields should be anonymized per PII field strategies
        assert result["field1"] != "value1"
        assert len(result["field1"]) == 64  # SHA-256 hash
        assert result["field2"] != "value2"
        assert isinstance(result["field2"], str)  # Fake name generated

    def test_no_pii_no_changes(self, anonymizer):
        """Test that documents without PII are unchanged."""
        doc = {"id": 123, "status": "active", "count": 42}

        result = anonymizer.apply_anonymization(doc, None)

        # Document should be identical (deep copy)
        assert result == doc
        assert result is not doc  # But not the same object


class TestAnonymizationStrategies:
    """Test different anonymization strategies via end-to-end document anonymization."""

    def test_hash_strategy(self, anonymizer):
        """Test hash strategy produces hashes."""
        doc = {"ssn": "123-45-6789"}
        pii_field_strategy = {"ssn": "hash"}

        result1 = anonymizer.apply_anonymization(doc, pii_field_strategy)
        result2 = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Presidio hash uses random salt, so hashes will differ
        # But both should be valid SHA-256 hashes
        assert len(result1["ssn"]) == 64
        assert len(result2["ssn"]) == 64
        # Values should be anonymized (not original)
        assert result1["ssn"] != "123-45-6789"
        assert result2["ssn"] != "123-45-6789"

    def test_redact_strategy(self, anonymizer):
        """Test redact strategy."""
        doc = {"ssn": "123-45-6789"}
        pii_field_strategy = {"ssn": "redact"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Should be redacted (empty string)
        assert result["ssn"] == ""

    def test_fake_name_strategy(self, anonymizer):
        """Test fake_name custom operator."""
        doc = {"name": "John Doe"}
        pii_field_strategy = {"name": "fake_name"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Should generate a realistic fake name
        assert result["name"] != "John Doe"
        assert isinstance(result["name"], str)
        assert len(result["name"]) > 0

    def test_fake_phone_strategy(self, anonymizer):
        """Test fake_phone custom operator."""
        doc = {"phone": "555-123-4567"}
        pii_field_strategy = {"phone": "fake_phone"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Should generate a realistic fake phone
        assert result["phone"] != "555-123-4567"
        assert isinstance(result["phone"], str)
        assert len(result["phone"]) > 0


class TestConvenienceFunction:
    """Test the module-level convenience function."""

    def test_apply_anonymization_function(self):
        """Test apply_anonymization convenience function."""
        doc = {"email": "test@example.com"}
        pii_field_strategy = {"email": "smart_mask"}

        result = apply_anonymization(doc, pii_field_strategy)

        # Should anonymize using default anonymizer
        assert result["email"] != "test@example.com"

    def test_apply_anonymization_with_custom_config(self, tmp_path):
        """Test apply_anonymization with custom config path."""
        doc = {"email": "test@example.com"}
        pii_field_strategy = {"email": "hash"}

        # Using None config path should work (uses default)
        result = apply_anonymization(doc, pii_field_strategy, None)

        # Should anonymize
        assert result["email"] != "test@example.com"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_none_values(self, anonymizer):
        """Test handling of None values in document."""
        doc = {"field1": None, "field2": "value"}
        pii_field_strategy = {"field1": "hash", "field2": "hash"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # None should be handled gracefully
        assert "field1" in result
        # field2 should be hashed
        assert len(result["field2"]) == 64

    def test_empty_document(self, anonymizer):
        """Test handling of empty document."""
        doc = {}
        pii_field_strategy = {"email": "smart_mask"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Should return empty document
        assert result == {}

    def test_empty_string_values(self, anonymizer):
        """Test handling of empty strings."""
        doc = {"email": ""}
        pii_field_strategy = {"email": "smart_mask"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Should handle empty string gracefully
        assert "email" in result

    def test_numeric_values(self, anonymizer):
        """Test anonymizing numeric values."""
        doc = {"ssn": 123456789}  # Numeric SSN
        pii_field_strategy = {"ssn": "hash"}

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Should convert to string and hash
        assert isinstance(result["ssn"], str)
        assert len(result["ssn"]) == 64

    def test_array_of_strings(self, anonymizer):
        """Test anonymizing an array of string values."""
        doc = {"phoneNumbers": ["1-407-314-9685", "1-407-914-1726", "1-813-996-3381"]}

        field_operators = {"phoneNumbers": [{"operator": "mask_phone", "params": None}]}

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Should preserve array type (not convert to string)
        assert isinstance(result["phoneNumbers"], list)
        assert len(result["phoneNumbers"]) == 3

        # Each element should be anonymized
        assert result["phoneNumbers"][0] != "1-407-314-9685"
        assert result["phoneNumbers"][1] != "1-407-914-1726"
        assert result["phoneNumbers"][2] != "1-813-996-3381"

        # Should preserve phone format (ends with last 4 digits)
        assert result["phoneNumbers"][0].endswith("9685")
        assert result["phoneNumbers"][1].endswith("1726")
        assert result["phoneNumbers"][2].endswith("3381")

    def test_array_of_emails(self, anonymizer):
        """Test anonymizing an array of email addresses."""
        doc = {"emails": ["john@example.com", "jane@test.org", "bob@demo.net"]}

        field_operators = {"emails": [{"operator": "mask_email", "params": None}]}

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Should preserve array type
        assert isinstance(result["emails"], list)
        assert len(result["emails"]) == 3

        # Each email should be anonymized but preserve domain
        assert "@example.com" in result["emails"][0]
        assert "@test.org" in result["emails"][1]
        assert "@demo.net" in result["emails"][2]

        # Local parts should be masked
        assert "john" not in result["emails"][0]
        assert "jane" not in result["emails"][1]
        assert "bob" not in result["emails"][2]

    def test_empty_array(self, anonymizer):
        """Test handling empty arrays."""
        doc = {"phoneNumbers": []}

        field_operators = {"phoneNumbers": [{"operator": "mask_phone", "params": None}]}

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Should preserve empty array
        assert isinstance(result["phoneNumbers"], list)
        assert len(result["phoneNumbers"]) == 0

    def test_array_with_none_values(self, anonymizer):
        """Test handling arrays containing None values."""
        doc = {"phoneNumbers": ["1-407-314-9685", None, "1-813-996-3381"]}

        field_operators = {"phoneNumbers": [{"operator": "mask_phone", "params": None}]}

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Should preserve array type
        assert isinstance(result["phoneNumbers"], list)
        assert len(result["phoneNumbers"]) == 3

        # None should remain None
        assert result["phoneNumbers"][1] is None

        # Other values should be anonymized
        assert result["phoneNumbers"][0] != "1-407-314-9685"
        assert result["phoneNumbers"][2] != "1-813-996-3381"


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_full_document_anonymization(self, sample_doc, anonymizer):
        """Test anonymizing a complete realistic document."""
        pii_field_strategy = {
            "email": "smart_mask",
            "phone": "fake_phone",
            "ssn": "hash",
            "contacts.email": "smart_mask",
        }

        result = anonymizer.apply_anonymization(sample_doc, pii_field_strategy)

        # Original document should be unchanged
        assert sample_doc["email"] == "john.doe@example.com"

        # Result should be anonymized
        assert result["email"] != "john.doe@example.com"
        assert result["phone"] != "555-123-4567"
        assert result["ssn"] != "123-45-6789"
        assert result["contacts"][0]["email"] != "jane@example.com"
        assert result["contacts"][1]["email"] != "bob@example.com"

        # Non-PII fields should be unchanged
        assert result["_id"] == "doc123"
        assert result["address"]["city"] == "New York"

    def test_multiple_manual_strategies(self, anonymizer):
        """Test using different manual strategies on different fields."""
        doc = {
            "email": "test@example.com",
            "ssn": "123-45-6789",
            "custom_sensitive": "secret-data",
        }
        pii_field_strategy = {
            "email": "fake_email",
            "ssn": "hash",
            "custom_sensitive": "hash",
        }

        result = anonymizer.apply_anonymization(doc, pii_field_strategy)

        # Email should use fake_email
        assert "@" in result["email"]
        assert result["email"] != "test@example.com"

        # SSN should be hashed
        assert len(result["ssn"]) == 64

        # Custom field should be hashed
        assert len(result["custom_sensitive"]) == 64


class TestMultiEntityAnonymization:
    """Test multi-entity anonymization support."""

    def test_multi_entity_single_field(self, anonymizer):
        """Test applying multiple operators to a single field."""
        doc = {"contact_info": "John Smith john@example.com"}

        # Simulate field with both PERSON and EMAIL_ADDRESS entities
        field_operators = {
            "contact_info": [
                {"operator": "mask_person", "params": {"entity_type": "PERSON"}},
                {"operator": "mask_email", "params": {"entity_type": "EMAIL_ADDRESS"}},
            ]
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Original should not be modified
        assert doc["contact_info"] == "John Smith john@example.com"

        # Result should be anonymized (both operators applied)
        assert result["contact_info"] != "John Smith john@example.com"
        # The exact output depends on operator implementation
        # but it should be different from original

    def test_multi_entity_confidence_order(self, anonymizer):
        """Test that operators are applied in order (highest confidence first)."""
        doc = {"data": "sensitive information"}

        # Operators should be applied in list order
        field_operators = {
            "data": [
                {"operator": "hash", "params": {"entity_type": "SENSITIVE_1"}},  # Applied first
                {"operator": "mask", "params": {"entity_type": "SENSITIVE_2"}},  # Applied second
            ]
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # First operator (hash) will hash the text
        # Second operator (mask) won't have much effect on hashed data
        # Main test: no errors and something changed
        assert result["data"] != "sensitive information"

    def test_multi_entity_nested_field(self, anonymizer):
        """Test multi-entity anonymization on nested fields."""
        doc = {"user": {"full_contact": "Jane Doe jane.doe@company.com"}}

        field_operators = {
            "user.full_contact": [
                {"operator": "mask_person", "params": {"entity_type": "PERSON"}},
                {"operator": "mask_email", "params": {"entity_type": "EMAIL_ADDRESS"}},
            ]
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Nested field should be anonymized
        assert result["user"]["full_contact"] != "Jane Doe jane.doe@company.com"

    def test_multi_entity_array_field(self, anonymizer):
        """Test multi-entity anonymization on array fields."""
        doc = {
            "contacts": [
                {"info": "Alice Smith alice@example.com"},
                {"info": "Bob Jones bob@example.com"},
            ]
        }

        field_operators = {
            "contacts.info": [
                {"operator": "fake_name", "params": {"entity_type": "PERSON"}},
                {"operator": "fake_email", "params": {"entity_type": "EMAIL_ADDRESS"}},
            ]
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # All array elements should be anonymized
        assert result["contacts"][0]["info"] != "Alice Smith alice@example.com"
        assert result["contacts"][1]["info"] != "Bob Jones bob@example.com"

    def test_multi_entity_with_smart_operators(self, anonymizer):
        """Test multi-entity with smart operators that use entity_type."""
        doc = {"field": "some text"}

        field_operators = {
            "field": [
                {"operator": "smart_mask", "params": {"entity_type": "PERSON"}},
                {"operator": "smart_mask", "params": {"entity_type": "EMAIL_ADDRESS"}},
            ]
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Smart operators should receive entity_type and delegate appropriately
        assert result["field"] != "some text"

    def test_multi_entity_mixed_with_single_entity(self, anonymizer):
        """Test document with both multi-entity and single-entity fields."""
        doc = {"multi": "John Doe john@example.com", "single": "jane@example.com"}

        field_operators = {
            "multi": [
                {"operator": "mask_person", "params": {"entity_type": "PERSON"}},
                {"operator": "mask_email", "params": {"entity_type": "EMAIL_ADDRESS"}},
            ],
            "single": [{"operator": "mask_email", "params": {"entity_type": "EMAIL_ADDRESS"}}],
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Both fields should be anonymized
        assert result["multi"] != "John Doe john@example.com"
        assert result["single"] != "jane@example.com"

    def test_empty_field_operators(self, anonymizer):
        """Test with empty field operators dict."""
        doc = {"email": "test@example.com"}
        field_operators = {}

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Document should be unchanged
        assert result == doc

    def test_field_operators_with_none_params(self, anonymizer):
        """Test field operators where params might be None (legacy)."""
        doc = {"data": "sensitive"}

        field_operators = {"data": [{"operator": "hash", "params": None}]}

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Should still work with None params
        assert result["data"] != "sensitive"

    def test_field_operators_with_custom_params(self, anonymizer):
        """Test that custom params are correctly passed to operators."""
        doc = {"email": "test@example.com"}

        # Pass custom params along with entity_type
        field_operators = {
            "email": [
                {
                    "operator": "mask_email",
                    "params": {
                        "entity_type": "EMAIL_ADDRESS",
                        "custom_param": "custom_value",
                    },
                }
            ]
        }

        result = anonymizer.apply_multi_entity_anonymization(doc, field_operators)

        # Should anonymize (we can't easily test if custom params were used,
        # but at least verify it doesn't crash and anonymizes)
        assert result["email"] != "test@example.com"

    def test_build_operator_config_with_params(self, anonymizer):
        """Test _build_operator_config correctly uses params dict."""
        # Test with params including entity_type and custom params
        operator_config = anonymizer._build_operator_config(
            operator_name="mask_email",
            params={
                "entity_type": "EMAIL_ADDRESS",
                "masking_char": "#",
                "chars_to_mask": 5,
            },
        )

        assert operator_config.operator_name == "mask_email"
        assert operator_config.params["entity_type"] == "EMAIL_ADDRESS"
        assert operator_config.params["masking_char"] == "#"
        assert operator_config.params["chars_to_mask"] == 5

    def test_build_operator_config_params_only(self, anonymizer):
        """Test _build_operator_config with params but no entity_type."""
        operator_config = anonymizer._build_operator_config(
            operator_name="hash", params={"custom_param": "value"}
        )

        assert operator_config.operator_name == "hash"
        assert operator_config.params["custom_param"] == "value"
        assert "entity_type" not in operator_config.params

    def test_build_operator_config_with_entity_type(self, anonymizer):
        """Test _build_operator_config with entity_type in params."""
        operator_config = anonymizer._build_operator_config(
            operator_name="smart_mask", params={"entity_type": "EMAIL_ADDRESS"}
        )

        assert operator_config.operator_name == "smart_mask"
        assert operator_config.params["entity_type"] == "EMAIL_ADDRESS"

    def test_build_operator_config_no_params(self, anonymizer):
        """Test _build_operator_config with no params."""
        operator_config = anonymizer._build_operator_config(operator_name="hash")

        assert operator_config.operator_name == "hash"
        assert operator_config.params == {}
