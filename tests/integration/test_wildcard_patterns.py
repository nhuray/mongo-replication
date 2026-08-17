"""Integration test demonstrating wildcard pattern matching in anonymize transforms."""

from mongo_replication.config.models import PIIFieldAnonymization
from mongo_replication.engine.pii.pii_handler import PIIHandler


class TestWildcardPatternIntegration:
    """End-to-end tests for wildcard pattern matching."""

    def test_user_sin_number_wildcard_pattern(self):
        """Test the user.*.sinNumber wildcard pattern example from requirements."""
        # Configuration: anonymize all sinNumber fields under user.*
        pii_anonymization = [
            PIIFieldAnonymization(
                field="user.*.sinNumber", operator="redact", params={"entity_type": "US_SSN"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        # Test documents with various nested sinNumber fields
        documents = [
            {
                "_id": "1",
                "name": "John Doe",
                "user": {
                    "forms": {"sinNumber": "123-456-789", "type": "tax"},
                    "profile": {"sinNumber": "987-654-321", "name": "John"},
                    "account": {"id": "abc123", "status": "active"},
                },
            },
            {"_id": "2", "user": {"forms": {"sinNumber": "555-555-555"}}},
        ]

        result = handler.process_documents(documents)

        # Verify documents were processed
        assert len(result) == 2

        # Verify the structure is maintained
        assert result[0]["_id"] == "1"
        assert result[0]["name"] == "John Doe"
        assert "user" in result[0]
        assert "forms" in result[0]["user"]
        assert "profile" in result[0]["user"]

        # Verify sinNumber fields were anonymized (changed from original values)
        assert result[0]["user"]["forms"]["sinNumber"] != "123-456-789"
        assert result[0]["user"]["profile"]["sinNumber"] != "987-654-321"
        assert result[1]["user"]["forms"]["sinNumber"] != "555-555-555"

        # Verify non-matching fields are untouched
        assert result[0]["user"]["forms"]["type"] == "tax"
        assert result[0]["user"]["profile"]["name"] == "John"
        assert result[0]["user"]["account"]["id"] == "abc123"

    def test_metadata_suffix_wildcard(self):
        """Test metadata.* pattern to anonymize all metadata fields."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="metadata.*", operator="redact", params={"entity_type": "ANY"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": "1",
                "title": "Document 1",
                "metadata": {
                    "created_by": "john@example.com",
                    "created_at": "2024-01-01",
                    "internal_notes": "Confidential information",
                    "version": "1.0",
                },
                "content": "Public content",
            }
        ]

        result = handler.process_documents(documents)

        # Verify non-metadata fields are untouched
        assert result[0]["title"] == "Document 1"
        assert result[0]["content"] == "Public content"

        # Verify metadata fields were anonymized
        assert result[0]["metadata"]["created_by"] != "john@example.com"
        assert result[0]["metadata"]["internal_notes"] != "Confidential information"

    def test_email_prefix_wildcard(self):
        """Test *.email pattern to anonymize all email fields."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="*.email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": "1",
                "user": {"email": "user@example.com", "name": "John"},
                "contact": {"email": "contact@example.com", "phone": "555-1234"},
                "profile": {"bio": "Software engineer"},
            }
        ]

        result = handler.process_documents(documents)

        # Verify email fields were anonymized
        assert result[0]["user"]["email"] != "user@example.com"
        assert result[0]["contact"]["email"] != "contact@example.com"

        # Verify non-email fields are untouched
        assert result[0]["user"]["name"] == "John"
        assert result[0]["contact"]["phone"] == "555-1234"
        assert result[0]["profile"]["bio"] == "Software engineer"

    def test_multiple_wildcards_in_single_pattern(self):
        """Test pattern with multiple wildcards (e.g., user.*.*.email)."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="data.*.*.value", operator="redact", params={"entity_type": "ANY"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": "1",
                "data": {
                    "level1": {
                        "level2": {"value": "sensitive-123", "other": "keep-this"},
                        "another": {"value": "secret-456"},
                        "value": "keep-this",
                    },
                    "other_branch": {"nested": {"value": "confidential-789"}},
                },
                "public": "not-sensitive",
            }
        ]

        result = handler.process_documents(documents)

        # Verify all deeply nested "value" fields were anonymized
        assert result[0]["data"]["level1"]["level2"]["value"] != "sensitive-123"
        assert result[0]["data"]["level1"]["another"]["value"] != "secret-456"
        assert result[0]["data"]["other_branch"]["nested"]["value"] != "confidential-789"

        # Verify non-matching fields are untouched
        assert result[0]["data"]["level1"]["value"] == "keep-this"
        assert result[0]["data"]["level1"]["level2"]["other"] == "keep-this"
        assert result[0]["public"] == "not-sensitive"

    def test_multiple_separate_wildcard_patterns(self):
        """Test using multiple separate wildcard patterns together."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="*.email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
            PIIFieldAnonymization(
                field="user.*.ssn", operator="hash", params={"entity_type": "US_SSN"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": "1",
                "user": {
                    "email": "user@example.com",
                    "profile": {"ssn": "123-45-6789"},
                    "forms": {"ssn": "987-65-4321"},
                },
                "contact": {"email": "contact@example.com"},
            }
        ]

        result = handler.process_documents(documents)

        # Verify both patterns were applied
        assert result[0]["user"]["email"] != "user@example.com"
        assert result[0]["contact"]["email"] != "contact@example.com"
        assert result[0]["user"]["profile"]["ssn"] != "123-45-6789"
        assert result[0]["user"]["forms"]["ssn"] != "987-65-4321"

    def test_wildcard_with_exact_match(self):
        """Test mixing wildcard patterns with exact field matches."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="email",  # Exact match
                operator="mask_email",
                params={"entity_type": "EMAIL_ADDRESS"},
            ),
            PIIFieldAnonymization(
                field="user.*",  # Wildcard
                operator="redact",
                params={"entity_type": "ANY"},
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": "1",
                "email": "root@example.com",
                "user": {"name": "John", "age": 30},
                "title": "Document",
            }
        ]

        result = handler.process_documents(documents)

        # Verify both exact and wildcard patterns work
        assert result[0]["email"] != "root@example.com"
        assert result[0]["user"]["name"] != "John"
        assert result[0]["user"]["age"] != 30
        assert result[0]["title"] == "Document"  # Untouched
