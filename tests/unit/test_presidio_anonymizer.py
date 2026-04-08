"""Unit tests for Presidio PII anonymizer with operator-based configuration."""

import re

import pytest

from mongo_replication.engine.pii import (
    PresidioAnonymizer,
    apply_anonymization,
    get_anonymizer,
)

# Test entity strategies for unit tests (avoids loading default config)
TEST_ENTITY_STRATEGIES = {
    "EMAIL_ADDRESS": "smart_redact",
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
        assert anon.strategy_aliases is not None
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
        assert TEST_ENTITY_STRATEGIES["EMAIL_ADDRESS"] == "smart_redact"
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
        """Test anonymizing a simple document with detected PII."""
        doc = {"email": "test@example.com", "name": "John Doe"}
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "name": ("PERSON", 0.90),
        }

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Original document should not be modified
        assert doc["email"] == "test@example.com"
        assert doc["name"] == "John Doe"

        # Result should be anonymized
        assert result["email"] != "test@example.com"
        assert result["name"] == "ANONYMOUS"  # Default PERSON strategy is "replace"

    def test_anonymize_nested_document(self, anonymizer):
        """Test anonymizing nested fields."""
        doc = {
            "user": {
                "email": "test@example.com",
                "profile": {"name": "John Doe"},
            }
        }
        pii_map = {
            "user.email": ("EMAIL_ADDRESS", 0.95),
            "user.profile.name": ("PERSON", 0.90),
        }

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Nested fields should be anonymized
        assert result["user"]["email"] != "test@example.com"
        assert result["user"]["profile"]["name"] == "ANONYMOUS"

    def test_anonymize_array_fields(self, anonymizer):
        """Test anonymizing fields within arrays."""
        doc = {
            "contacts": [
                {"email": "alice@example.com", "name": "Alice"},
                {"email": "bob@example.com", "name": "Bob"},
            ]
        }
        pii_map = {
            "contacts.email": ("EMAIL_ADDRESS", 0.95),
            "contacts.name": ("PERSON", 0.90),
        }

        result = anonymizer.apply_anonymization(doc, pii_map)

        # All array elements should be anonymized
        assert result["contacts"][0]["email"] != "alice@example.com"
        assert result["contacts"][1]["email"] != "bob@example.com"
        assert result["contacts"][0]["name"] == "ANONYMOUS"
        assert result["contacts"][1]["name"] == "ANONYMOUS"

    def test_manual_overrides_only(self, anonymizer):
        """Test anonymizing with manual overrides (no auto-detected PII)."""
        doc = {"field1": "value1", "field2": "value2"}
        manual_overrides = {
            "field1": "hash",
            "field2": "fake_name",  # Use custom operator that doesn't need params
        }

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Fields should be anonymized per manual overrides
        assert result["field1"] != "value1"
        assert len(result["field1"]) == 64  # SHA-256 hash
        assert result["field2"] != "value2"
        assert isinstance(result["field2"], str)  # Fake name generated

    def test_manual_overrides_precedence(self, anonymizer):
        """Test that manual overrides take precedence over detected PII."""
        doc = {"email": "test@example.com"}
        pii_map = {"email": ("EMAIL_ADDRESS", 0.95)}  # Would normally use smart_redact
        manual_overrides = {"email": "hash"}  # Override to hash

        result = anonymizer.apply_anonymization(doc, pii_map, manual_overrides)

        # Should use hash (manual override) not smart_redact (auto-detected)
        assert len(result["email"]) == 64  # Hashed

    def test_no_pii_no_changes(self, anonymizer):
        """Test that documents without PII are unchanged."""
        doc = {"id": 123, "status": "active", "count": 42}

        result = anonymizer.apply_anonymization(doc, None, None)

        # Document should be identical (deep copy)
        assert result == doc
        assert result is not doc  # But not the same object

    def test_missing_field_graceful(self, anonymizer):
        """Test graceful handling when PII map references non-existent field."""
        doc = {"email": "test@example.com"}
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "phone": ("PHONE_NUMBER", 0.90),  # Field doesn't exist
        }

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Should anonymize existing field and ignore missing field
        assert result["email"] != "test@example.com"
        assert "phone" not in result


class TestAnonymizationStrategies:
    """Test different anonymization strategies via end-to-end document anonymization."""

    def test_hash_strategy(self, anonymizer):
        """Test hash strategy produces hashes."""
        doc = {"ssn": "123-45-6789"}
        manual_overrides = {"ssn": "hash"}

        result1 = anonymizer.apply_anonymization(doc, None, manual_overrides)
        result2 = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Presidio hash uses random salt, so hashes will differ
        # But both should be valid SHA-256 hashes
        assert len(result1["ssn"]) == 64
        assert len(result2["ssn"]) == 64
        # Values should be anonymized (not original)
        assert result1["ssn"] != "123-45-6789"
        assert result2["ssn"] != "123-45-6789"

    def test_redact_strategy(self, anonymizer):
        """Test basic redact strategy."""
        doc = {"data": "sensitive-information"}
        manual_overrides = {"data": "redact"}

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Should be redacted but not the original value
        assert result["data"] != "sensitive-information"
        # Presidio's redact operator returns empty string by default
        assert result["data"] == ""

    def test_mask_strategy(self, anonymizer):
        """Test mask strategy."""
        doc = {"ssn": "123-45-6789"}
        pii_map = {"ssn": ("US_SSN", 0.99)}  # US_SSN uses mask by default

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Should contain asterisks (masked)
        assert "*" in result["ssn"]
        assert result["ssn"] != "123-45-6789"

    def test_replace_strategy(self, anonymizer):
        """Test replace strategy."""
        doc = {"name": "John Doe"}
        pii_map = {"name": ("PERSON", 0.95)}  # PERSON uses replace by default

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Should be replaced with placeholder
        assert result["name"] == "ANONYMOUS"

    def test_smart_redact_strategy(self, anonymizer):
        """Test smart_redact strategy on email."""
        doc = {"email": "john.doe@example.com"}
        pii_map = {"email": ("EMAIL_ADDRESS", 0.95)}  # EMAIL uses smart_redact

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Smart redact should preserve some structure
        assert result["email"] != "john.doe@example.com"
        # Should preserve domain
        assert "@example.com" in result["email"]

    def test_fake_email_strategy(self, anonymizer):
        """Test fake_email custom operator."""
        doc = {"email": "test@example.com"}
        manual_overrides = {"email": "fake_email"}

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Should generate a realistic fake email
        assert result["email"] != "test@example.com"
        assert "@" in result["email"]
        assert re.match(r"[^@]+@[^@]+\.[^@]+", result["email"])

    def test_fake_name_strategy(self, anonymizer):
        """Test fake_name custom operator."""
        doc = {"name": "John Doe"}
        manual_overrides = {"name": "fake_name"}

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Should generate a realistic fake name
        assert result["name"] != "John Doe"
        assert isinstance(result["name"], str)
        assert len(result["name"]) > 0

    def test_fake_phone_strategy(self, anonymizer):
        """Test fake_phone custom operator."""
        doc = {"phone": "555-123-4567"}
        manual_overrides = {"phone": "fake_phone"}

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Should generate a realistic fake phone
        assert result["phone"] != "555-123-4567"
        assert isinstance(result["phone"], str)
        assert len(result["phone"]) > 0


class TestConvenienceFunction:
    """Test the module-level convenience function."""

    def test_apply_anonymization_function(self):
        """Test apply_anonymization convenience function."""
        doc = {"email": "test@example.com"}
        pii_map = {"email": ("EMAIL_ADDRESS", 0.95)}

        result = apply_anonymization(doc, pii_map)

        # Should anonymize using default anonymizer
        assert result["email"] != "test@example.com"

    def test_apply_anonymization_with_custom_config(self, tmp_path):
        """Test apply_anonymization with custom config path."""
        doc = {"email": "test@example.com"}
        manual_overrides = {"email": "hash"}

        # Using None config path should work (uses default)
        result = apply_anonymization(doc, None, manual_overrides, None)

        # Should anonymize
        assert result["email"] != "test@example.com"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_none_values(self, anonymizer):
        """Test handling of None values in document."""
        doc = {"field1": None, "field2": "value"}
        manual_overrides = {"field1": "hash", "field2": "hash"}

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # None should be handled gracefully
        assert "field1" in result
        # field2 should be hashed
        assert len(result["field2"]) == 64

    def test_empty_document(self, anonymizer):
        """Test handling of empty document."""
        doc = {}
        pii_map = {"email": ("EMAIL_ADDRESS", 0.95)}

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Should return empty document
        assert result == {}

    def test_unknown_entity_type_uses_default(self, anonymizer):
        """Test that unknown entity types fall back to DEFAULT strategy."""
        doc = {"field": "value"}
        pii_map = {"field": ("UNKNOWN_ENTITY_TYPE", 0.95)}

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Should use DEFAULT strategy (redact)
        assert result["field"] != "value"

    def test_empty_string_values(self, anonymizer):
        """Test handling of empty strings."""
        doc = {"email": ""}
        pii_map = {"email": ("EMAIL_ADDRESS", 0.95)}

        result = anonymizer.apply_anonymization(doc, pii_map)

        # Should handle empty string gracefully
        assert "email" in result

    def test_numeric_values(self, anonymizer):
        """Test anonymizing numeric values."""
        doc = {"ssn": 123456789}  # Numeric SSN
        manual_overrides = {"ssn": "hash"}

        result = anonymizer.apply_anonymization(doc, None, manual_overrides)

        # Should convert to string and hash
        assert isinstance(result["ssn"], str)
        assert len(result["ssn"]) == 64


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_full_document_anonymization(self, sample_doc, anonymizer):
        """Test anonymizing a complete realistic document."""
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "phone": ("PHONE_NUMBER", 0.90),
            "ssn": ("US_SSN", 0.99),
            "contacts.email": ("EMAIL_ADDRESS", 0.95),
        }

        result = anonymizer.apply_anonymization(sample_doc, pii_map)

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

    def test_mixed_auto_and_manual_strategies(self, anonymizer):
        """Test combining auto-detected PII with manual overrides."""
        doc = {
            "email": "test@example.com",
            "ssn": "123-45-6789",
            "custom_sensitive": "secret-data",
        }
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "ssn": ("US_SSN", 0.99),
        }
        manual_overrides = {
            "email": "fake_email",  # Override smart_redact with fake_email
            "custom_sensitive": "hash",  # Add manual field
        }

        result = anonymizer.apply_anonymization(doc, pii_map, manual_overrides)

        # Email should use fake_email (manual override)
        assert "@" in result["email"]
        assert result["email"] != "test@example.com"

        # SSN should use auto-detected strategy (mask)
        assert "*" in result["ssn"]

        # Custom field should be hashed
        assert len(result["custom_sensitive"]) == 64

    def test_strategy_aliases(self, anonymizer):
        """Test that strategy aliases work correctly."""
        doc = {"email": "test@example.com"}

        # Test various strategy names that should work
        working_strategies = ["hash", "mask", "fake_email", "smart_redact"]

        for strategy in working_strategies:
            manual_overrides = {"email": strategy}
            result = anonymizer.apply_anonymization(doc, None, manual_overrides)
            # Should not crash and should anonymize (or at least try)
            # Some strategies like redact might return empty string
            assert "email" in result
