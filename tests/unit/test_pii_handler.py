"""Unit tests for PIIHandler with multi-entity support."""

from mongo_replication.config.models import PIIFieldAnonymization
from mongo_replication.engine.pii.pii_handler import PIIHandler, create_pii_handler_from_config


class TestPIIHandlerInitialization:
    """Test PIIHandler initialization with various input formats."""

    def test_init_with_list_format(self):
        """Test initialization with new list format (PIIFieldAnonymization objects)."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
            PIIFieldAnonymization(
                field="phone", operator="mask_phone", params={"entity_type": "PHONE_NUMBER"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert len(handler.field_operators) == 2
        assert "email" in handler.field_operators
        assert "phone" in handler.field_operators
        assert handler.field_operators["email"][0]["operator"] == "mask_email"
        assert handler.field_operators["email"][0]["params"]["entity_type"] == "EMAIL_ADDRESS"

    def test_init_with_dict_format(self):
        """Test initialization with legacy dict format (backward compatibility)."""
        pii_fields = {"email": "mask_email", "phone": "mask_phone"}

        handler = PIIHandler(pii_anonymization=pii_fields)

        assert len(handler.field_operators) == 2
        assert "email" in handler.field_operators
        assert handler.field_operators["email"][0]["operator"] == "mask_email"
        assert (
            handler.field_operators["email"][0]["params"] is None
        )  # Legacy format has no entity_type

    def test_init_with_dict_list_format(self):
        """Test initialization with list of dicts format."""
        pii_anonymization = [
            {"field": "email", "operator": "mask_email", "entity_type": "EMAIL_ADDRESS"},
            {"field": "phone", "operator": "mask_phone", "entity_type": "PHONE_NUMBER"},
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert len(handler.field_operators) == 2
        assert handler.field_operators["email"][0]["operator"] == "mask_email"

    def test_init_empty(self):
        """Test initialization with no PII config."""
        handler = PIIHandler()

        assert len(handler.field_operators) == 0

    def test_init_with_none(self):
        """Test initialization with None."""
        handler = PIIHandler(pii_anonymization=None)

        assert len(handler.field_operators) == 0
        assert handler.pii_field_count == 0


class TestPIIHandlerProperties:
    """Test PIIHandler properties."""

    def test_pii_field_count_single_entity(self):
        """Test pii_field_count with single-entity fields."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
            PIIFieldAnonymization(
                field="phone", operator="mask_phone", params={"entity_type": "PHONE_NUMBER"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert handler.pii_field_count == 2

    def test_pii_field_count_multi_entity(self):
        """Test pii_field_count with multi-entity fields."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="contact", operator="mask_person", params={"entity_type": "PERSON"}
            ),
            PIIFieldAnonymization(
                field="contact", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
            PIIFieldAnonymization(field="ssn", operator="hash", params={"entity_type": "US_SSN"}),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        # Should count unique fields, not total operators
        assert handler.pii_field_count == 2  # contact and ssn

    def test_pii_field_count_empty(self):
        """Test pii_field_count with empty config."""
        handler = PIIHandler()

        assert handler.pii_field_count == 0


class TestPIIHandlerMultiEntity:
    """Test PIIHandler with multi-entity fields."""

    def test_multi_entity_field(self):
        """Test field with multiple entity types."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="contact", operator="mask_person", params={"entity_type": "PERSON"}
            ),
            PIIFieldAnonymization(
                field="contact", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        # Should have one field with multiple operators
        assert len(handler.field_operators) == 1
        assert "contact" in handler.field_operators
        assert len(handler.field_operators["contact"]) == 2

        # Should preserve order (confidence order)
        assert handler.field_operators["contact"][0]["operator"] == "mask_person"
        assert handler.field_operators["contact"][1]["operator"] == "mask_email"

    def test_process_documents_multi_entity(self):
        """Test processing documents with multi-entity fields."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="info", operator="fake_name", params={"entity_type": "PERSON"}
            ),
            PIIFieldAnonymization(
                field="info", operator="fake_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {"_id": 1, "info": "John Doe john@example.com"},
            {"_id": 2, "info": "Jane Smith jane@example.com"},
        ]

        result = handler.process_documents(documents)

        # Should anonymize both documents
        assert len(result) == 2
        assert result[0]["info"] != "John Doe john@example.com"
        assert result[1]["info"] != "Jane Smith jane@example.com"

        # _id should remain unchanged
        assert result[0]["_id"] == 1
        assert result[1]["_id"] == 2

    def test_process_empty_documents(self):
        """Test processing empty document list."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            )
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)
        result = handler.process_documents([])

        assert result == []

    def test_process_documents_no_config(self):
        """Test processing documents with no PII config."""
        handler = PIIHandler()

        documents = [{"email": "test@example.com"}]
        result = handler.process_documents(documents)

        # Should return unchanged
        assert result == documents


class TestPIIHandlerComplexScenarios:
    """Test PIIHandler with complex real-world scenarios."""

    def test_mixed_single_and_multi_entity_fields(self):
        """Test document with both single-entity and multi-entity fields."""
        pii_anonymization = [
            # Multi-entity field
            PIIFieldAnonymization(
                field="contact", operator="mask_person", params={"entity_type": "PERSON"}
            ),
            PIIFieldAnonymization(
                field="contact", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
            # Single-entity fields
            PIIFieldAnonymization(field="ssn", operator="hash", params={"entity_type": "US_SSN"}),
            PIIFieldAnonymization(
                field="phone", operator="mask_phone", params={"entity_type": "PHONE_NUMBER"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert len(handler.field_operators) == 3  # contact, ssn, phone
        assert len(handler.field_operators["contact"]) == 2
        assert len(handler.field_operators["ssn"]) == 1
        assert len(handler.field_operators["phone"]) == 1

    def test_nested_multi_entity_fields(self):
        """Test nested fields with multiple entities."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="user.details", operator="mask_person", params={"entity_type": "PERSON"}
            ),
            PIIFieldAnonymization(
                field="user.details", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [{"_id": 1, "user": {"details": "Alice Cooper alice@rock.com"}}]

        result = handler.process_documents(documents)

        # Nested field should be anonymized
        assert result[0]["user"]["details"] != "Alice Cooper alice@rock.com"

    def test_array_multi_entity_fields(self):
        """Test array fields with multiple entities."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="contacts.info", operator="fake_name", params={"entity_type": "PERSON"}
            ),
            PIIFieldAnonymization(
                field="contacts.info",
                operator="fake_email",
                params={"entity_type": "EMAIL_ADDRESS"},
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": 1,
                "contacts": [
                    {"info": "Bob Dylan bob@music.com"},
                    {"info": "Tom Petty tom@music.com"},
                ],
            }
        ]

        result = handler.process_documents(documents)

        # All array elements should be anonymized
        assert result[0]["contacts"][0]["info"] != "Bob Dylan bob@music.com"
        assert result[0]["contacts"][1]["info"] != "Tom Petty tom@music.com"


class TestCreatePIIHandlerFromConfig:
    """Test factory function for creating PIIHandler."""

    def test_create_from_list(self):
        """Test creating handler from list format."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            )
        ]

        handler = create_pii_handler_from_config(pii_anonymization)

        assert isinstance(handler, PIIHandler)
        assert len(handler.field_operators) == 1

    def test_create_from_dict(self):
        """Test creating handler from legacy dict format."""
        pii_fields = {"email": "mask_email"}

        handler = create_pii_handler_from_config(pii_fields)

        assert isinstance(handler, PIIHandler)
        assert len(handler.field_operators) == 1

    def test_create_from_empty(self):
        """Test creating handler with empty config."""
        handler = create_pii_handler_from_config([])

        assert isinstance(handler, PIIHandler)
        assert len(handler.field_operators) == 0


class TestPIIFieldAnonymizationWithParams:
    """Test PIIFieldAnonymization with custom params."""

    def test_init_with_params(self):
        """Test initialization with custom params."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="email",
                operator="mask_email",
                params={
                    "entity_type": "EMAIL_ADDRESS",
                    "masking_char": "#",
                    "chars_to_mask": 5,
                },
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert len(handler.field_operators) == 1
        assert handler.field_operators["email"][0]["operator"] == "mask_email"
        assert handler.field_operators["email"][0]["params"]["entity_type"] == "EMAIL_ADDRESS"
        assert handler.field_operators["email"][0]["params"] == {
            "entity_type": "EMAIL_ADDRESS",
            "masking_char": "#",
            "chars_to_mask": 5,
        }

    def test_init_without_params(self):
        """Test initialization without params (should be None)."""
        pii_anonymization = [
            PIIFieldAnonymization(field="email", operator="mask_email"),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert handler.field_operators["email"][0]["params"] is None

    def test_multi_entity_with_params(self):
        """Test multi-entity field with different params."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="contact",
                operator="mask_person",
                params={
                    "entity_type": "PERSON",
                    "preserve_length": True,
                },
            ),
            PIIFieldAnonymization(
                field="contact",
                operator="mask_email",
                params={
                    "entity_type": "EMAIL_ADDRESS",
                    "masking_char": "*",
                },
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert len(handler.field_operators["contact"]) == 2
        assert handler.field_operators["contact"][0]["params"] == {
            "entity_type": "PERSON",
            "preserve_length": True,
        }
        assert handler.field_operators["contact"][1]["params"] == {
            "entity_type": "EMAIL_ADDRESS",
            "masking_char": "*",
        }


class TestPIIHandlerWildcardPatterns:
    """Test PIIHandler wildcard pattern matching and expansion."""

    def test_init_with_wildcard_patterns(self):
        """Test initialization with wildcard patterns sets the flag."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="user.*.email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
            PIIFieldAnonymization(
                field="metadata.*", operator="redact", params={"entity_type": "ANY"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert handler._has_wildcard_patterns is True
        assert len(handler.field_operators) == 2

    def test_init_without_wildcard_patterns(self):
        """Test initialization without wildcard patterns doesn't set the flag."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="user.email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        assert handler._has_wildcard_patterns is False

    def test_expand_patterns_suffix_wildcard(self):
        """Test pattern expansion with suffix wildcard (metadata.*)."""
        handler = PIIHandler(pii_anonymization={"metadata.*": "redact"})

        doc = {
            "name": "John",
            "metadata": {"created_at": "2024-01-01", "updated_at": "2024-01-02", "user_id": "123"},
        }

        expanded = handler._expand_patterns(doc)

        # Should match all fields under metadata
        assert "metadata.created_at" in expanded
        assert "metadata.updated_at" in expanded
        assert "metadata.user_id" in expanded
        # Should not match fields outside metadata
        assert "name" not in expanded

    def test_expand_patterns_prefix_wildcard(self):
        """Test pattern expansion with prefix wildcard (*.email)."""
        handler = PIIHandler(pii_anonymization={"*.email": "mask_email"})

        doc = {
            "user": {"email": "user@test.com", "name": "John"},
            "contact": {"email": "contact@test.com", "phone": "123"},
            "profile": {"bio": "Hello"},
        }

        expanded = handler._expand_patterns(doc)

        # Should match all fields ending with .email
        assert "user.email" in expanded
        assert "contact.email" in expanded
        # Should not match non-email fields
        assert "user.name" not in expanded
        assert "contact.phone" not in expanded
        assert "profile.bio" not in expanded

    def test_expand_patterns_middle_wildcard(self):
        """Test pattern expansion with middle wildcard (user.*.sinNumber)."""
        handler = PIIHandler(pii_anonymization={"user.*.sinNumber": "hash"})

        doc = {
            "user": {
                "forms": {"sinNumber": "123-456-789", "type": "tax"},
                "profile": {"sinNumber": "987-654-321", "name": "John"},
                "account": {"id": "abc123"},
            }
        }

        expanded = handler._expand_patterns(doc)

        # Should match user.*.sinNumber patterns
        assert "user.forms.sinNumber" in expanded
        assert "user.profile.sinNumber" in expanded
        # Should not match other fields
        assert "user.forms.type" not in expanded
        assert "user.profile.name" not in expanded
        assert "user.account.id" not in expanded

    def test_expand_patterns_with_arrays(self):
        """Test pattern expansion with arrays."""
        handler = PIIHandler(pii_anonymization={"contacts.email": "mask_email"})

        doc = {
            "contacts": [
                {"email": "a@test.com", "name": "Alice"},
                {"email": "b@test.com", "name": "Bob"},
            ]
        }

        expanded = handler._expand_patterns(doc)

        # Should match email fields in array
        # Pattern is exact match, not wildcard, so it should work directly
        assert "contacts.email" in expanded

    def test_expand_patterns_mixed_exact_and_wildcard(self):
        """Test pattern expansion with both exact and wildcard patterns."""
        handler = PIIHandler(
            pii_anonymization={
                "user.email": "mask_email",  # Exact match
                "user.*.sinNumber": "hash",  # Wildcard
            }
        )

        doc = {
            "user": {
                "email": "user@test.com",
                "forms": {"sinNumber": "123-456-789"},
                "profile": {"sinNumber": "987-654-321"},
            }
        }

        expanded = handler._expand_patterns(doc)

        # Both exact and wildcard patterns should be expanded
        assert "user.email" in expanded
        assert "user.forms.sinNumber" in expanded
        assert "user.profile.sinNumber" in expanded

    def test_expand_patterns_no_matches(self):
        """Test pattern expansion when no fields match."""
        handler = PIIHandler(pii_anonymization={"user.*.email": "mask_email"})

        doc = {"user": {"name": "John", "phone": "123"}}

        expanded = handler._expand_patterns(doc)

        # No email fields match the pattern
        assert "user.*.email" not in expanded
        assert len(expanded) == 0

    def test_process_documents_with_wildcards(self):
        """Test end-to-end document processing with wildcard patterns."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="user.*.email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        documents = [
            {
                "_id": "1",
                "user": {
                    "profile": {"email": "profile@test.com"},
                    "contact": {"email": "contact@test.com"},
                },
            }
        ]

        # Process documents - this will call the anonymizer
        # We're mainly testing that the pattern expansion happens correctly
        result = handler.process_documents(documents)

        # Should return the same number of documents
        assert len(result) == 1
        # The actual anonymization is tested in presidio_anonymizer tests

    def test_process_documents_without_wildcards_uses_fast_path(self):
        """Test that documents without wildcards use the fast path."""
        pii_anonymization = [
            PIIFieldAnonymization(
                field="user.email", operator="mask_email", params={"entity_type": "EMAIL_ADDRESS"}
            ),
        ]

        handler = PIIHandler(pii_anonymization=pii_anonymization)

        # _has_wildcard_patterns should be False
        assert handler._has_wildcard_patterns is False

        documents = [{"_id": "1", "user": {"email": "test@test.com"}}]

        result = handler.process_documents(documents)

        # Should process without calling _expand_patterns
        assert len(result) == 1
