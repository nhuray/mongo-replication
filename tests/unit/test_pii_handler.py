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
