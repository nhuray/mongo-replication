"""Unit tests for Presidio PII anonymizer."""

import re

import pytest

from mongo_replication.engine.pii import (
    DEFAULT_ENTITY_STRATEGIES,
    PresidioAnonymizer,
    apply_anonymization,
)


@pytest.fixture
def anonymizer():
    """Create a PresidioAnonymizer instance for testing."""
    return PresidioAnonymizer()


class TestPresidioAnonymizer:
    """Test suite for PresidioAnonymizer class."""

    def test_initialization(self):
        """Test anonymizer initialization."""
        anon = PresidioAnonymizer()
        assert anon.person is not None
        assert anon.address is not None
        assert anon.anonymizer_engine is not None
        assert anon.entity_strategy_map == DEFAULT_ENTITY_STRATEGIES

    def test_custom_entity_strategy_map(self):
        """Test initialization with custom entity strategy map."""
        custom_map = {"EMAIL_ADDRESS": "hash", "PERSON": "redact"}
        anon = PresidioAnonymizer(entity_strategy_map=custom_map)
        assert anon.entity_strategy_map == custom_map

    def test_fake_email_strategy(self, anonymizer):
        """Test fake_email anonymization strategy."""
        original = "john.doe@example.com"
        result = anonymizer._fake_email(original)

        assert result != original
        assert isinstance(result, str)
        # Should be a valid email format
        assert re.match(r"[^@]+@[^@]+\.[^@]+", result)

    def test_fake_name_strategy(self, anonymizer):
        """Test fake_name anonymization strategy."""
        original = "John Doe"
        result = anonymizer._fake_name(original)

        assert result != original
        assert isinstance(result, str)
        assert len(result) > 0

    def test_fake_phone_strategy(self, anonymizer):
        """Test fake_phone anonymization strategy."""
        original = "+1-202-555-0173"
        result = anonymizer._fake_phone(original)

        assert result != original
        assert isinstance(result, str)
        assert len(result) > 0

    def test_fake_address_strategy(self, anonymizer):
        """Test fake_address anonymization strategy."""
        original = "123 Main St, New York, NY 10001"
        result = anonymizer._fake_address(original)

        assert result != original
        assert isinstance(result, str)
        assert "\n" not in result  # Should replace newlines with commas

    def test_hash_strategy(self, anonymizer):
        """Test hash anonymization strategy."""
        original = "sensitive-value"
        result = anonymizer._hash(original)

        assert result != original
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex digest length

        # Should be deterministic (same input = same hash)
        result2 = anonymizer._hash(original)
        assert result == result2

        # Different input = different hash
        result3 = anonymizer._hash("different-value")
        assert result != result3

    def test_hash_strategy_with_none(self, anonymizer):
        """Test hash strategy with None value."""
        result = anonymizer._hash(None)
        assert isinstance(result, str)
        assert len(result) == 64

    def test_redact_strategy(self, anonymizer):
        """Test smart redact anonymization strategy."""
        # Generic strings - preserve first 3 and last 3
        result = anonymizer._redact("sensitive-data-here")
        assert result == "sen***ere"

        # Short strings
        result = anonymizer._redact("abc")
        assert result == "***"

        result = anonymizer._redact("abcd")
        assert result == "a***"

        result = anonymizer._redact("abcdef")
        assert result == "a***"

        # Email format - preserve domain and uniqueness
        result = anonymizer._redact("john.doe@example.com")
        assert "@example.com" in result, "Should preserve full domain"
        assert result.startswith("jo"), "Should show first 2 chars"
        assert result != "john.doe@example.com", "Should be redacted"

        # SSN format - preserve last 4
        result = anonymizer._redact("123-45-6789")
        assert "6789" in result
        assert "***" in result

        # Phone format - preserve last 4
        result = anonymizer._redact("555-123-4567")
        assert "4567" in result
        assert "***" in result

        # IP format
        result = anonymizer._redact("192.168.1.1")
        assert "192" in result
        assert "1" in result[-1]
        assert "***" in result

        # None value
        result = anonymizer._redact(None)
        assert result == "***"

    def test_mask_strategy(self, anonymizer):
        """Test mask anonymization strategy."""
        # Normal string
        result = anonymizer._mask("password123")
        assert result == "***********"

        # With special characters (preserved)
        result = anonymizer._mask("123-45-6789")
        assert result == "***-**-****"

        # None value
        result = anonymizer._mask(None)
        assert result == "***"

    def test_null_strategy(self, anonymizer):
        """Test null anonymization strategy."""
        result = anonymizer._null("any-value")
        assert result is None

    def test_apply_strategy_unknown(self, anonymizer):
        """Test that unknown strategy raises ValueError."""
        with pytest.raises(ValueError, match="Unknown anonymization strategy"):
            anonymizer._apply_strategy("value", "unknown_strategy")

    def test_merge_strategies_auto_only(self, anonymizer):
        """Test merging with only auto-detected PII."""
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "ssn": ("US_SSN", 0.99),
        }

        result = anonymizer._merge_strategies(pii_map, None)

        # Both now use 'redact' by default (updated strategy)
        assert result["email"] == "redact"
        assert result["ssn"] == "redact"

    def test_merge_strategies_manual_override(self, anonymizer):
        """Test that manual overrides take precedence."""
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "ssn": ("US_SSN", 0.99),
        }
        manual_overrides = {
            "email": "hash",  # Override auto-detected strategy
            "custom_field": "fake_email",  # Add new field not auto-detected
        }

        result = anonymizer._merge_strategies(pii_map, manual_overrides)

        assert result["email"] == "hash"  # Manual override wins
        assert result["ssn"] == "redact"  # Auto-detected, no override (now redact by default)
        assert result["custom_field"] == "fake_email"  # Manual addition

    def test_merge_strategies_null_override_disables(self, anonymizer):
        """Test that None override disables anonymization for a field."""
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "phone": ("PHONE_NUMBER", 0.90),
        }
        manual_overrides = {
            "email": None,  # Disable anonymization
        }

        result = anonymizer._merge_strategies(pii_map, manual_overrides)

        assert "email" not in result  # Excluded by None override
        assert "phone" in result  # Still included

    def test_apply_anonymization_simple(self, anonymizer):
        """Test anonymization of a simple document."""
        document = {
            "email": "john@example.com",
            "name": "John Doe",
            "age": 30,
        }
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "name": ("PERSON", 0.90),
        }

        result = anonymizer.apply_anonymization(document, pii_map)

        # Email and name should be anonymized
        assert result["email"] != document["email"]
        assert result["name"] != document["name"]

        # Age should be unchanged (not in pii_map)
        assert result["age"] == document["age"]

    def test_apply_anonymization_nested(self, anonymizer):
        """Test anonymization of nested documents."""
        document = {
            "user": {
                "email": "test@example.com",
                "profile": {
                    "phone": "+1-555-1234",
                },
            },
            "metadata": {
                "created": "2024-01-01",
            },
        }
        pii_map = {
            "user.email": ("EMAIL_ADDRESS", 0.95),
            "user.profile.phone": ("PHONE_NUMBER", 0.90),
        }

        result = anonymizer.apply_anonymization(document, pii_map)

        # Nested fields should be anonymized
        assert result["user"]["email"] != document["user"]["email"]
        assert result["user"]["profile"]["phone"] != document["user"]["profile"]["phone"]

        # Non-PII fields should be unchanged
        assert result["metadata"]["created"] == document["metadata"]["created"]

    def test_apply_anonymization_with_manual_overrides(self, anonymizer):
        """Test anonymization with manual strategy overrides."""
        document = {
            "email": "test@example.com",
            "ssn": "123-45-6789",
        }
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
        }
        manual_overrides = {
            "email": "hash",  # Override default redact
            "ssn": "fake_email",  # Add manual field not auto-detected
        }

        result = anonymizer.apply_anonymization(document, pii_map, manual_overrides)

        # Email should be hashed (manual override)
        assert len(result["email"]) == 64  # SHA-256 hash

        # SSN should use fake_email (manual addition)
        assert "@" in result["ssn"]  # Fake email format

    def test_apply_anonymization_array_fields(self, anonymizer):
        """Test anonymization of fields in arrays."""
        document = {
            "contacts": [
                {"email": "first@example.com"},
                {"email": "second@example.com"},
            ]
        }
        pii_map = {
            "contacts[0].email": ("EMAIL_ADDRESS", 0.95),
            "contacts[1].email": ("EMAIL_ADDRESS", 0.95),
        }

        result = anonymizer.apply_anonymization(document, pii_map)

        # Array elements should be anonymized
        assert result["contacts"][0]["email"] != document["contacts"][0]["email"]
        assert result["contacts"][1]["email"] != document["contacts"][1]["email"]

    def test_anonymize_field_missing(self, anonymizer):
        """Test that anonymizing missing fields doesn't crash."""
        document = {"existing": "value"}

        # Should not crash even if field doesn't exist
        anonymizer._anonymize_field(document, "missing", "hash")
        anonymizer._anonymize_field(document, "nested.missing", "hash")

        # Document should be unchanged
        assert document == {"existing": "value"}

    def test_anonymize_nested_field_array_notation(self, anonymizer):
        """Test nested field anonymization with array notation."""
        document = {
            "items": [
                {"name": "Item 1", "email": "item1@example.com"},
                {"name": "Item 2", "email": "item2@example.com"},
            ]
        }

        # Anonymize first item's email
        anonymizer._anonymize_nested_field(document, "items[0].email", "hash")

        # First item should be anonymized
        assert len(document["items"][0]["email"]) == 64  # Hash length

        # Second item should be unchanged
        assert document["items"][1]["email"] == "item2@example.com"

    def test_default_entity_strategies_coverage(self):
        """Test that DEFAULT_ENTITY_STRATEGIES has reasonable defaults."""
        # Should have common entity types
        assert "EMAIL_ADDRESS" in DEFAULT_ENTITY_STRATEGIES
        assert "PERSON" in DEFAULT_ENTITY_STRATEGIES
        assert "PHONE_NUMBER" in DEFAULT_ENTITY_STRATEGIES
        assert "US_SSN" in DEFAULT_ENTITY_STRATEGIES

        # Should have a DEFAULT fallback
        assert "DEFAULT" in DEFAULT_ENTITY_STRATEGIES

        # Most should use smart redaction now (preserves data utility)
        assert DEFAULT_ENTITY_STRATEGIES["EMAIL_ADDRESS"] == "redact"
        assert DEFAULT_ENTITY_STRATEGIES["PERSON"] == "redact"
        assert DEFAULT_ENTITY_STRATEGIES["PHONE_NUMBER"] == "redact"
        assert DEFAULT_ENTITY_STRATEGIES["US_SSN"] == "redact"

        # Very sensitive data should still be hashed
        assert DEFAULT_ENTITY_STRATEGIES["CREDIT_CARD"] == "hash"
        assert DEFAULT_ENTITY_STRATEGIES["CRYPTO"] == "hash"
        assert DEFAULT_ENTITY_STRATEGIES["IBAN"] == "hash"

    def test_convenience_function(self):
        """Test the convenience apply_anonymization function."""
        document = {
            "email": "test@example.com",
        }
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
        }

        result = apply_anonymization(document, pii_map)

        assert result["email"] != document["email"]
        # Should use default redact strategy (domain-preserving)
        assert "@example.com" in result["email"], "Should preserve full domain"
        assert result["email"].startswith("t"), "Should show first char of local part"

    def test_convenience_function_custom_strategy_map(self):
        """Test convenience function with custom strategy map."""
        document = {
            "email": "test@example.com",
        }
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
        }
        custom_map = {"EMAIL_ADDRESS": "hash", "DEFAULT": "hash"}

        result = apply_anonymization(document, pii_map, entity_strategy_map=custom_map)

        # Should use hash instead of fake_email
        assert len(result["email"]) == 64

    def test_hash_referential_integrity(self, anonymizer):
        """Test that hashing maintains referential integrity."""
        # Same value in multiple places should hash to same result
        document = {
            "email1": "same@example.com",
            "email2": "same@example.com",
            "email3": "different@example.com",
        }
        pii_map = {
            "email1": ("EMAIL_ADDRESS", 0.95),
            "email2": ("EMAIL_ADDRESS", 0.95),
            "email3": ("EMAIL_ADDRESS", 0.95),
        }
        manual_overrides = {
            "email1": "hash",
            "email2": "hash",
            "email3": "hash",
        }

        result = anonymizer.apply_anonymization(document, pii_map, manual_overrides)

        # Same emails should hash to same value
        assert result["email1"] == result["email2"]

        # Different email should hash to different value
        assert result["email1"] != result["email3"]

    def test_document_copy_not_modified(self, anonymizer):
        """Test that original document is not modified."""
        original = {
            "email": "test@example.com",
            "name": "John Doe",
        }
        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "name": ("PERSON", 0.90),
        }

        # Make a copy to compare
        original_copy = original.copy()

        # Anonymize
        result = anonymizer.apply_anonymization(original, pii_map)

        # Original should be unchanged
        assert original == original_copy

        # Result should be different
        assert result != original


class TestSmartRedaction:
    """Test suite for smart format-preserving redaction."""

    def test_redact_email_format(self):
        """Test email redaction preserves domain and maintains uniqueness."""
        anonymizer = PresidioAnonymizer()

        # Standard email - should preserve full domain
        result = anonymizer._redact("john.doe@example.com")
        assert "@" in result
        assert "@example.com" in result, "Domain should be preserved exactly"
        assert result.startswith("jo"), "Should show first 2 chars of local part"
        assert result != "john.doe@example.com", "Should be redacted"
        print(f"Email redaction: john.doe@example.com -> {result}")

        # Short local part
        result = anonymizer._redact("ab@example.com")
        assert "@example.com" in result, "Domain should be preserved"
        assert result.startswith("a"), "Should show first char for short local parts"

        # Complex domain - full domain should be preserved
        result = anonymizer._redact("user@subdomain.example.co.uk")
        assert "@subdomain.example.co.uk" in result, "Full domain should be preserved"

        # Test uniqueness: different emails should produce different results
        result1 = anonymizer._redact("alice@corp.com")
        result2 = anonymizer._redact("bob@corp.com")
        assert result1 != result2, "Different emails should produce different redacted values"
        assert "@corp.com" in result1 and "@corp.com" in result2, "Both should preserve domain"

        # Test consistency: same email should produce same result
        result_a = anonymizer._redact("test@example.com")
        result_b = anonymizer._redact("test@example.com")
        assert result_a == result_b, "Same email should produce consistent redacted value"

    def test_redact_ssn_format(self):
        """Test SSN redaction preserves last 4 digits."""
        anonymizer = PresidioAnonymizer()

        # Standard format
        result = anonymizer._redact("123-45-6789")
        assert "6789" in result
        assert "***" in result or "-" in result
        print(f"SSN redaction: 123-45-6789 -> {result}")

        # Without dashes
        result = anonymizer._redact("123456789")
        if anonymizer._is_ssn_format("123456789"):
            assert "***" in result

    def test_redact_phone_format(self):
        """Test phone number redaction preserves last 4 digits."""
        anonymizer = PresidioAnonymizer()

        # Standard US format
        result = anonymizer._redact("555-123-4567")
        assert "4567" in result
        assert "***" in result
        print(f"Phone redaction: 555-123-4567 -> {result}")

        # With parentheses
        result = anonymizer._redact("(555) 123-4567")
        assert "4567" in result

        # International format
        result = anonymizer._redact("+1-555-123-4567")
        assert "4567" in result

    def test_redact_ip_format(self):
        """Test IP address redaction preserves first and last octet."""
        anonymizer = PresidioAnonymizer()

        result = anonymizer._redact("192.168.1.100")
        assert "192" in result
        assert "100" in result
        assert "***" in result
        print(f"IP redaction: 192.168.1.100 -> {result}")

    def test_redact_url_format(self):
        """Test URL redaction preserves structure."""
        anonymizer = PresidioAnonymizer()

        result = anonymizer._redact("https://example.com/path/to/resource")
        assert "http" in result
        assert "***" in result
        print(f"URL redaction: https://example.com/path/to/resource -> {result}")

    def test_redact_generic_long_string(self):
        """Test generic redaction for non-special strings."""
        anonymizer = PresidioAnonymizer()

        # Long string - preserve first 3 and last 3
        result = anonymizer._redact("John Michael Smith")
        assert result == "Joh***ith"
        print(f"Name redaction: John Michael Smith -> {result}")

        # Exactly 7 chars
        result = anonymizer._redact("ABCDEFG")
        assert result == "ABC***EFG"

    def test_redact_short_strings(self):
        """Test redaction behavior for short strings."""
        anonymizer = PresidioAnonymizer()

        # 3 chars or less - full mask
        assert anonymizer._redact("ABC") == "***"
        assert anonymizer._redact("AB") == "***"
        assert anonymizer._redact("A") == "***"

        # 4-6 chars - show first char only
        assert anonymizer._redact("ABCD") == "A***"
        assert anonymizer._redact("ABCDE") == "A***"
        assert anonymizer._redact("ABCDEF") == "A***"

    def test_format_detection(self):
        """Test format detection methods."""
        anonymizer = PresidioAnonymizer()

        # SSN detection
        assert anonymizer._is_ssn_format("123-45-6789")
        assert anonymizer._is_ssn_format("123456789")
        assert not anonymizer._is_ssn_format("12-345-6789")

        # Phone detection
        assert anonymizer._is_phone_format("555-123-4567")
        assert anonymizer._is_phone_format("(555) 123-4567")
        assert anonymizer._is_phone_format("5551234567")
        assert not anonymizer._is_phone_format("123")

        # IP detection
        assert anonymizer._is_ip_format("192.168.1.1")
        assert anonymizer._is_ip_format("10.0.0.255")
        assert anonymizer._is_ip_format("999.999.999.999")  # Format-wise valid
        assert not anonymizer._is_ip_format("192.168.1")

    def test_end_to_end_redaction(self):
        """Test end-to-end redaction with real PII."""
        anonymizer = PresidioAnonymizer()

        document = {
            "email": "alice.smith@company.com",
            "ssn": "987-65-4321",
            "phone": "555-867-5309",
            "ip": "10.20.30.40",
            "name": "Alice Rebecca Smith",
        }

        pii_map = {
            "email": ("EMAIL_ADDRESS", 0.95),
            "ssn": ("US_SSN", 0.99),
            "phone": ("PHONE_NUMBER", 0.90),
            "ip": ("IP_ADDRESS", 0.85),
            "name": ("PERSON", 0.92),
        }

        result = anonymizer.apply_anonymization(document, pii_map)

        print("\nEnd-to-end redaction results:")
        for field, value in document.items():
            if field in result:
                print(f"  {field}: {value} -> {result[field]}")

        # Email should preserve full domain and be unique
        assert "@company.com" in result["email"], "Should preserve full domain"
        assert result["email"].startswith("al"), "Should show first 2 chars"
        assert result["email"] != document["email"], "Should be redacted"

        # SSN should preserve last 4
        assert "4321" in result["ssn"]

        # Phone should preserve last 4
        assert "5309" in result["phone"]

        # IP should preserve first and last octet
        assert "10" in result["ip"]
        assert "40" in result["ip"]

        # Name should preserve first 3 and last 3
        assert result["name"][:3] == "Ali"
        assert result["name"][-3:] == "ith"
