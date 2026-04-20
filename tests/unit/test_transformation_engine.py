"""Unit tests for TransformationEngine."""

import pytest
from datetime import datetime
from bson import ObjectId

from mongo_replication.engine.transformations import (
    TransformationEngine,
    TransformationError,
    TransformResults,
)
from mongo_replication.config.models import (
    AddFieldTransform,
    SetFieldTransform,
    RemoveFieldTransform,
    RenameFieldTransform,
    CopyFieldTransform,
    RegexReplaceTransform,
    AnonymizeTransform,
    ConditionConfig,
)


class TestAddFieldTransform:
    """Tests for add_field transform."""

    def test_add_simple_field(self):
        """Test adding a simple field."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="status", value="active")]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "status": "active"}

    def test_add_nested_field(self):
        """Test adding a nested field."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="address.city", value="NYC")]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "address": {"city": "NYC"}}

    def test_add_field_to_existing_nested_object(self):
        """Test adding a field to an existing nested object."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="address.zipcode", value="10001")]
        )

        doc = {"name": "Alice", "address": {"city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "address": {"city": "NYC", "zipcode": "10001"}}

    def test_add_field_error_when_exists(self):
        """Test that adding a field that exists raises error."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="name", value="Bob")],
            error_mode="fail",
        )

        doc = {"name": "Alice"}
        with pytest.raises(TransformationError, match="field already exists"):
            _, _ = engine.transform_documents([doc])

    def test_add_field_error_when_nested_exists(self):
        """Test that adding a nested field that exists raises error."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="address.city", value="LA")],
            error_mode="fail",
        )

        doc = {"address": {"city": "NYC"}}
        with pytest.raises(TransformationError, match="field already exists"):
            _, _ = engine.transform_documents([doc])

    def test_add_field_with_literal_values(self):
        """Test adding fields with different literal types."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="count", value=42),
                AddFieldTransform(field="price", value=19.99),
                AddFieldTransform(field="active", value=True),
                AddFieldTransform(field="tags", value=["a", "b", "c"]),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["count"] == 42
        assert result["price"] == 19.99
        assert result["active"] is True
        assert result["tags"] == ["a", "b", "c"]

    def test_add_field_with_template_single_field(self):
        """Test adding field with single field reference template."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="name_copy", value="$name")]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "name_copy": "Alice"}

    def test_add_field_with_template_concatenation(self):
        """Test adding field with concatenation template."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="full_name", value="$first_name $last_name")]
        )

        doc = {"first_name": "Alice", "last_name": "Smith"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["full_name"] == "Alice Smith"

    def test_add_field_with_template_nested_reference(self):
        """Test adding field with nested field reference."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="city_copy", value="$address.city")]
        )

        doc = {"address": {"city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["city_copy"] == "NYC"

    def test_add_field_with_special_value_now(self):
        """Test adding field with $now special value."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="created_at", value="$now")]
        )

        doc = {}
        before = datetime.utcnow()
        result, _ = engine.transform_documents([doc])
        result = result[0]
        after = datetime.utcnow()

        assert isinstance(result["created_at"], datetime)
        assert before <= result["created_at"] <= after

    def test_add_field_with_special_value_null(self):
        """Test adding field with $null special value."""
        engine = TransformationEngine(
            transforms=[AddFieldTransform(field="deleted_at", value="$null")]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"deleted_at": None}

    def test_add_field_with_dict_value(self):
        """Test adding field with a nested dictionary value."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(
                    field="metadata",
                    value={
                        "created_by": "system",
                        "version": 1,
                        "tags": ["prod", "verified"],
                    },
                )
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {
            "name": "Alice",
            "metadata": {
                "created_by": "system",
                "version": 1,
                "tags": ["prod", "verified"],
            },
        }

    def test_add_field_with_nested_dict_to_nested_path(self):
        """Test adding a dict value to a nested field path."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(
                    field="config.settings",
                    value={"timeout": 30, "retries": 3},
                )
            ]
        )

        doc = {"name": "Alice", "config": {}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {
            "name": "Alice",
            "config": {"settings": {"timeout": 30, "retries": 3}},
        }


class TestSetFieldTransform:
    """Tests for set_field transform."""

    def test_set_new_field(self):
        """Test setting a new field."""
        engine = TransformationEngine(
            transforms=[SetFieldTransform(field="status", value="active")]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "status": "active"}

    def test_set_existing_field_overwrites(self):
        """Test that setting existing field overwrites value."""
        engine = TransformationEngine(
            transforms=[SetFieldTransform(field="status", value="inactive")]
        )

        doc = {"name": "Alice", "status": "active"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "status": "inactive"}

    def test_set_nested_field(self):
        """Test setting a nested field."""
        engine = TransformationEngine(
            transforms=[SetFieldTransform(field="address.city", value="LA")]
        )

        doc = {"name": "Alice", "address": {"city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["address"]["city"] == "LA"

    def test_set_field_with_template(self):
        """Test setting field with template (single field reference)."""
        engine = TransformationEngine(
            transforms=[SetFieldTransform(field="name_copy", value="$name")]
        )

        doc = {"name": "alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["name_copy"] == "alice"

    def test_set_field_creates_nested_structure(self):
        """Test that set_field creates intermediate dicts."""
        engine = TransformationEngine(transforms=[SetFieldTransform(field="a.b.c.d", value="deep")])

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"a": {"b": {"c": {"d": "deep"}}}}

    def test_set_field_with_dict_value(self):
        """Test setting field with a nested dictionary value."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="config",
                    value={
                        "enabled": True,
                        "settings": {"timeout": 30, "retries": 3},
                    },
                )
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {
            "name": "Alice",
            "config": {
                "enabled": True,
                "settings": {"timeout": 30, "retries": 3},
            },
        }

    def test_set_field_overwrites_with_dict_value(self):
        """Test that set_field overwrites scalar with dict value."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="status",
                    value={"active": True, "reason": "verified"},
                )
            ]
        )

        doc = {"name": "Alice", "status": "active"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {
            "name": "Alice",
            "status": {"active": True, "reason": "verified"},
        }


class TestRemoveFieldTransform:
    """Tests for remove_field transform."""

    def test_remove_single_field(self):
        """Test removing a single field."""
        engine = TransformationEngine(transforms=[RemoveFieldTransform(field="password")])

        doc = {"name": "Alice", "password": "secret"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice"}

    def test_remove_multiple_fields(self):
        """Test removing multiple fields."""
        engine = TransformationEngine(
            transforms=[RemoveFieldTransform(field=["password", "ssn", "secret"])]
        )

        doc = {"name": "Alice", "password": "secret", "ssn": "123-45-6789", "age": 30}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "age": 30}

    def test_remove_nested_field(self):
        """Test removing a nested field."""
        engine = TransformationEngine(transforms=[RemoveFieldTransform(field="address.zipcode")])

        doc = {"name": "Alice", "address": {"city": "NYC", "zipcode": "10001"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "address": {"city": "NYC"}}

    def test_remove_nested_field_cleans_empty_parents(self):
        """Test that removing nested field cleans up empty parent objects."""
        engine = TransformationEngine(transforms=[RemoveFieldTransform(field="address.city")])

        doc = {"name": "Alice", "address": {"city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Empty 'address' object should be removed
        assert result == {"name": "Alice"}

    def test_remove_nonexistent_field_no_error(self):
        """Test that removing nonexistent field doesn't error."""
        engine = TransformationEngine(transforms=[RemoveFieldTransform(field="nonexistent")])

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice"}

    def test_remove_deeply_nested_field_cleans_chain(self):
        """Test that removing deeply nested field cleans up entire empty chain."""
        engine = TransformationEngine(transforms=[RemoveFieldTransform(field="a.b.c.d")])

        doc = {"name": "Alice", "a": {"b": {"c": {"d": "value"}}}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # All empty parents should be cleaned up
        assert result == {"name": "Alice"}

    def test_remove_nested_field_preserves_siblings(self):
        """Test that removing nested field preserves sibling fields."""
        engine = TransformationEngine(transforms=[RemoveFieldTransform(field="address.zipcode")])

        doc = {
            "name": "Alice",
            "address": {"city": "NYC", "zipcode": "10001", "country": "USA"},
        }
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "address": {"city": "NYC", "country": "USA"}}


class TestRenameFieldTransform:
    """Tests for rename_field transform."""

    def test_rename_simple_field(self):
        """Test renaming a simple field."""
        engine = TransformationEngine(
            transforms=[RenameFieldTransform(from_field="old_name", to_field="new_name")]
        )

        doc = {"old_name": "value", "other": "data"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"new_name": "value", "other": "data"}

    def test_rename_nested_field(self):
        """Test renaming a nested field."""
        engine = TransformationEngine(
            transforms=[RenameFieldTransform(from_field="address.zip", to_field="address.zipcode")]
        )

        doc = {"address": {"zip": "10001", "city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"address": {"zipcode": "10001", "city": "NYC"}}

    def test_rename_field_to_different_path(self):
        """Test renaming field to a different path."""
        engine = TransformationEngine(
            transforms=[RenameFieldTransform(from_field="city", to_field="address.city")]
        )

        doc = {"name": "Alice", "city": "NYC"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "address": {"city": "NYC"}}

    def test_rename_nonexistent_field_no_error(self):
        """Test that renaming nonexistent field doesn't error."""
        engine = TransformationEngine(
            transforms=[RenameFieldTransform(from_field="nonexistent", to_field="new_name")]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice"}

    def test_rename_field_error_when_target_exists(self):
        """Test that renaming to existing field raises error."""
        engine = TransformationEngine(
            transforms=[RenameFieldTransform(from_field="old_name", to_field="existing")],
            error_mode="fail",
        )

        doc = {"old_name": "value", "existing": "data"}
        with pytest.raises(TransformationError, match="target field already exists"):
            _, _ = engine.transform_documents([doc])

    def test_rename_field_with_overwrite(self):
        """Test renaming with overwrite=True."""
        engine = TransformationEngine(
            transforms=[
                RenameFieldTransform(from_field="old_name", to_field="existing", overwrite=True)
            ]
        )

        doc = {"old_name": "new_value", "existing": "old_value"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"existing": "new_value"}

    def test_rename_cleans_empty_nested_parents(self):
        """Test that rename cleans up empty parent objects after removing source."""
        engine = TransformationEngine(
            transforms=[RenameFieldTransform(from_field="old.nested.field", to_field="new_field")]
        )

        doc = {"old": {"nested": {"field": "value"}}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Empty parent chain should be cleaned up
        assert result == {"new_field": "value"}


class TestCopyFieldTransform:
    """Tests for copy_field transform."""

    def test_copy_simple_field(self):
        """Test copying a simple field."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="name", to_field="name_backup")]
        )

        doc = {"name": "Alice", "age": 30}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "age": 30, "name_backup": "Alice"}

    def test_copy_nested_field(self):
        """Test copying a nested field."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="address.city", to_field="city")]
        )

        doc = {"name": "Alice", "address": {"city": "NYC", "zipcode": "10001"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["city"] == "NYC"
        assert result["address"]["city"] == "NYC"  # Original still exists

    def test_copy_field_to_nested_path(self):
        """Test copying field to a nested path."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="city", to_field="backup.city")]
        )

        doc = {"city": "NYC"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"city": "NYC", "backup": {"city": "NYC"}}

    def test_copy_nonexistent_field_no_error(self):
        """Test that copying nonexistent field doesn't error."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="nonexistent", to_field="new_field")]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice"}

    def test_copy_field_error_when_target_exists(self):
        """Test that copying to existing field raises error."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="name", to_field="existing")],
            error_mode="fail",
        )

        doc = {"name": "Alice", "existing": "data"}
        with pytest.raises(TransformationError, match="target field already exists"):
            _, _ = engine.transform_documents([doc])

    def test_copy_field_with_overwrite(self):
        """Test copying with overwrite=True."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="name", to_field="existing", overwrite=True)]
        )

        doc = {"name": "Alice", "existing": "old_value"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "existing": "Alice"}

    def test_copy_field_deep_copy(self):
        """Test that copy creates a deep copy, not reference."""
        engine = TransformationEngine(
            transforms=[CopyFieldTransform(from_field="data", to_field="data_copy")]
        )

        doc = {"data": {"nested": "value"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Verify it's a deep copy by checking they're different objects
        assert result["data"] == result["data_copy"]
        assert result["data"] is not result["data_copy"]


class TestRegexReplaceTransform:
    """Tests for regex_replace transform."""

    def test_regex_replace_simple(self):
        """Test simple regex replacement."""
        engine = TransformationEngine(
            transforms=[
                RegexReplaceTransform(
                    field="email", pattern="@example\\.com$", replacement="@test.com"
                )
            ]
        )

        doc = {"email": "alice@example.com"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"email": "alice@test.com"}

    def test_regex_replace_with_capture_groups(self):
        """Test regex replacement with capture groups."""
        engine = TransformationEngine(
            transforms=[
                RegexReplaceTransform(
                    field="name", pattern="^(\\w+) (\\w+)$", replacement="\\2, \\1"
                )
            ]
        )

        doc = {"name": "Alice Smith"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Smith, Alice"}

    def test_regex_replace_nested_field(self):
        """Test regex replacement on nested field."""
        engine = TransformationEngine(
            transforms=[
                RegexReplaceTransform(
                    field="user.email",
                    pattern="@example\\.com$",
                    replacement="@test.com",
                )
            ]
        )

        doc = {"user": {"email": "alice@example.com"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["user"]["email"] == "alice@test.com"

    def test_regex_replace_no_match(self):
        """Test regex replacement when pattern doesn't match."""
        engine = TransformationEngine(
            transforms=[
                RegexReplaceTransform(
                    field="email", pattern="@example\\.com$", replacement="@test.com"
                )
            ]
        )

        doc = {"email": "alice@other.com"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Value should remain unchanged
        assert result == {"email": "alice@other.com"}

    def test_regex_replace_nonexistent_field(self):
        """Test regex replacement on nonexistent field."""
        engine = TransformationEngine(
            transforms=[
                RegexReplaceTransform(field="nonexistent", pattern="test", replacement="replaced")
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Document should remain unchanged
        assert result == {"name": "Alice"}

    def test_regex_replace_non_string_field(self):
        """Test regex replacement on non-string field."""
        engine = TransformationEngine(
            transforms=[RegexReplaceTransform(field="age", pattern="\\d+", replacement="XX")]
        )

        doc = {"age": 30}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Non-string field should remain unchanged
        assert result == {"age": 30}

    def test_regex_replace_multiple_matches(self):
        """Test regex replacement with multiple matches in string."""
        engine = TransformationEngine(
            transforms=[RegexReplaceTransform(field="text", pattern="foo", replacement="bar")]
        )

        doc = {"text": "foo foo foo"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"text": "bar bar bar"}


class TestAnonymizeTransform:
    """Tests for anonymize transform."""

    def test_anonymize_mask_email(self):
        """Test anonymizing email with mask operator."""
        engine = TransformationEngine(
            transforms=[AnonymizeTransform(field="email", operator="mask_email")]
        )

        doc = {"name": "Alice", "email": "alice@example.com"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Email should be masked (exact format depends on PII handler)
        assert result["name"] == "Alice"
        assert result["email"] != "alice@example.com"
        assert "@" in result["email"]  # Should preserve @ symbol

    def test_anonymize_hash(self):
        """Test anonymizing with hash operator."""
        engine = TransformationEngine(transforms=[AnonymizeTransform(field="ssn", operator="hash")])

        doc = {"name": "Alice", "ssn": "123-45-6789"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # SSN should be hashed
        assert result["name"] == "Alice"
        assert result["ssn"] != "123-45-6789"
        assert len(result["ssn"]) > 0

    def test_anonymize_with_params(self):
        """Test anonymizing with custom parameters (smart_mask with entity_type)."""
        engine = TransformationEngine(
            transforms=[
                AnonymizeTransform(
                    field="email",
                    operator="smart_mask",
                    params={"entity_type": "EMAIL_ADDRESS"},
                )
            ]
        )

        doc = {"email": "alice@example.com"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Email should be masked
        assert result["email"] != "alice@example.com"

    def test_anonymize_nested_field(self):
        """Test anonymizing nested field."""
        engine = TransformationEngine(
            transforms=[AnonymizeTransform(field="user.email", operator="mask_email")]
        )

        doc = {"user": {"name": "Alice", "email": "alice@example.com"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["user"]["name"] == "Alice"
        assert result["user"]["email"] != "alice@example.com"

    def test_anonymize_multiple_fields(self):
        """Test anonymizing multiple fields."""
        engine = TransformationEngine(
            transforms=[
                AnonymizeTransform(field="email", operator="mask_email"),
                AnonymizeTransform(field="ssn", operator="hash"),
            ]
        )

        doc = {"email": "alice@example.com", "ssn": "123-45-6789"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["email"] != "alice@example.com"
        assert result["ssn"] != "123-45-6789"


class TestConditionalExecution:
    """Tests for conditional transform execution."""

    def test_condition_exists_true(self):
        """Test condition with $exists=true."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="has_email",
                    value=True,
                    condition=ConditionConfig(field="email", operator="$exists", value=True),
                )
            ]
        )

        doc = {"name": "Alice", "email": "alice@example.com"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["has_email"] is True

    def test_condition_exists_false(self):
        """Test condition with $exists=false."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="no_email",
                    value=True,
                    condition=ConditionConfig(field="email", operator="$exists", value=False),
                )
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["no_email"] is True

    def test_condition_exists_skips_when_false(self):
        """Test that condition $exists skips transform when field doesn't exist."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="has_email",
                    value=True,
                    condition=ConditionConfig(field="email", operator="$exists", value=True),
                )
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Transform should be skipped, field not added
        assert "has_email" not in result

    def test_condition_eq(self):
        """Test condition with $eq."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="is_active",
                    value=True,
                    condition=ConditionConfig(field="status", operator="$eq", value="active"),
                )
            ]
        )

        doc = {"status": "active"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["is_active"] is True

    def test_condition_ne(self):
        """Test condition with $ne."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="is_active",
                    value=False,
                    condition=ConditionConfig(field="status", operator="$ne", value="active"),
                )
            ]
        )

        doc = {"status": "inactive"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["is_active"] is False

    def test_condition_gt(self):
        """Test condition with $gt."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="is_adult",
                    value=True,
                    condition=ConditionConfig(field="age", operator="$gt", value=18),
                )
            ]
        )

        doc = {"age": 25}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["is_adult"] is True

    def test_condition_gte(self):
        """Test condition with $gte."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="can_vote",
                    value=True,
                    condition=ConditionConfig(field="age", operator="$gte", value=18),
                )
            ]
        )

        doc = {"age": 18}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["can_vote"] is True

    def test_condition_lt(self):
        """Test condition with $lt."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="is_minor",
                    value=True,
                    condition=ConditionConfig(field="age", operator="$lt", value=18),
                )
            ]
        )

        doc = {"age": 15}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["is_minor"] is True

    def test_condition_lte(self):
        """Test condition with $lte."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="discount_eligible",
                    value=True,
                    condition=ConditionConfig(field="age", operator="$lte", value=12),
                )
            ]
        )

        doc = {"age": 12}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["discount_eligible"] is True

    def test_condition_in(self):
        """Test condition with $in."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="premium_user",
                    value=True,
                    condition=ConditionConfig(
                        field="tier", operator="$in", value=["gold", "platinum"]
                    ),
                )
            ]
        )

        doc = {"tier": "gold"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["premium_user"] is True

    def test_condition_nin(self):
        """Test condition with $nin."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="basic_user",
                    value=True,
                    condition=ConditionConfig(
                        field="tier", operator="$nin", value=["gold", "platinum"]
                    ),
                )
            ]
        )

        doc = {"tier": "bronze"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["basic_user"] is True

    def test_condition_on_nested_field(self):
        """Test condition on nested field."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="in_usa",
                    value=True,
                    condition=ConditionConfig(field="address.country", operator="$eq", value="USA"),
                )
            ]
        )

        doc = {"address": {"country": "USA", "city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["in_usa"] is True

    def test_condition_regexp_match(self):
        """Test condition with $regex that matches."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="has_valid_email",
                    value=True,
                    condition=ConditionConfig(
                        field="email", operator="$regex", value=r"^[\w\.-]+@[\w\.-]+\.\w+$"
                    ),
                )
            ]
        )

        doc = {"email": "alice@example.com"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["has_valid_email"] is True

    def test_condition_regexp_no_match(self):
        """Test condition with $regex that doesn't match."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="has_valid_email",
                    value=True,
                    condition=ConditionConfig(
                        field="email", operator="$regex", value=r"^[\w\.-]+@[\w\.-]+\.\w+$"
                    ),
                )
            ]
        )

        doc = {"email": "not-an-email"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Transform should be skipped, field not added
        assert "has_valid_email" not in result

    def test_condition_regexp_on_non_string(self):
        """Test that $regex returns False for non-string fields."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="matched",
                    value=True,
                    condition=ConditionConfig(field="value", operator="$regex", value=r"\d+"),
                )
            ]
        )

        doc = {"value": 12345}  # Integer, not string
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Transform should be skipped because field is not a string
        assert "matched" not in result

    def test_condition_regexp_partial_match(self):
        """Test that $regex performs partial match (search, not full match)."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="contains_number",
                    value=True,
                    condition=ConditionConfig(field="text", operator="$regex", value=r"\d+"),
                )
            ]
        )

        doc = {"text": "Hello 123 World"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["contains_number"] is True

    def test_condition_regexp_case_sensitive(self):
        """Test that $regex is case-sensitive by default."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="has_uppercase",
                    value=True,
                    condition=ConditionConfig(field="text", operator="$regex", value=r"^HELLO"),
                )
            ]
        )

        # Should not match lowercase
        doc1 = {"text": "hello world"}
        result1, _ = engine.transform_documents([doc1])
        result1 = result1[0]
        assert "has_uppercase" not in result1

        # Should match uppercase
        doc2 = {"text": "HELLO world"}
        result2, _ = engine.transform_documents([doc2])
        result2 = result2[0]
        assert result2["has_uppercase"] is True

    def test_condition_regexp_case_insensitive(self):
        """Test $regex with case-insensitive flag."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(
                    field="has_hello",
                    value=True,
                    condition=ConditionConfig(field="text", operator="$regex", value=r"(?i)^hello"),
                )
            ]
        )

        # Should match with case-insensitive flag
        doc = {"text": "HELLO world"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["has_hello"] is True

    def test_condition_on_anonymize_transform(self):
        """Test that anonymize transforms respect conditions."""
        engine = TransformationEngine(
            transforms=[
                AnonymizeTransform(
                    field="email",
                    operator="mask_email",
                    condition=ConditionConfig(field="status", operator="$eq", value="public"),
                )
            ]
        )

        # Test document that meets condition - should be anonymized
        doc1 = {"email": "alice@example.com", "status": "public"}
        # Test document that doesn't meet condition - should NOT be anonymized
        doc2 = {"email": "bob@example.com", "status": "private"}

        result, stats = engine.transform_documents([doc1, doc2])

        # First document should be anonymized (status=public)
        assert result[0]["email"] != "alice@example.com"
        assert "@" in result[0]["email"]  # Still looks like email but masked

        # Second document should NOT be anonymized (status=private)
        assert result[1]["email"] == "bob@example.com"

        # Check statistics
        assert stats.transforms_applied == 1  # Only one document was anonymized
        assert stats.transforms_skipped == 1  # One was skipped

    def test_condition_on_anonymize_multiple_transforms(self):
        """Test multiple anonymize transforms with different conditions."""
        engine = TransformationEngine(
            transforms=[
                AnonymizeTransform(
                    field="email",
                    operator="mask_email",
                    condition=ConditionConfig(field="tier", operator="$eq", value="free"),
                ),
                AnonymizeTransform(
                    field="phone",
                    operator="hash",
                    condition=ConditionConfig(
                        field="tier", operator="$in", value=["free", "basic"]
                    ),
                ),
            ]
        )

        # free tier: both transforms apply
        doc1 = {"email": "free@example.com", "phone": "123-456-7890", "tier": "free"}
        # basic tier: only phone transform applies
        doc2 = {"email": "basic@example.com", "phone": "123-456-7890", "tier": "basic"}
        # premium tier: no transforms apply
        doc3 = {"email": "premium@example.com", "phone": "123-456-7890", "tier": "premium"}

        result, stats = engine.transform_documents([doc1, doc2, doc3])

        # Free tier: both should be anonymized
        assert result[0]["email"] != "free@example.com"
        assert result[0]["phone"] != "123-456-7890"

        # Basic tier: only phone should be anonymized
        assert result[1]["email"] == "basic@example.com"  # NOT anonymized
        assert result[1]["phone"] != "123-456-7890"  # Anonymized

        # Premium tier: neither should be anonymized
        assert result[2]["email"] == "premium@example.com"
        assert result[2]["phone"] == "123-456-7890"

        # Check statistics: 2 transforms for doc1 + 1 transform for doc2 = 3 applied, 3 skipped
        assert stats.transforms_applied == 3
        assert stats.transforms_skipped == 3


class TestTransformPipelineOrdering:
    """Tests for transform pipeline ordering."""

    def test_transforms_applied_in_order(self):
        """Test that transforms are applied in the order defined."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="step1", value="first"),
                AddFieldTransform(field="step2", value="second"),
                AddFieldTransform(field="step3", value="third"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"step1": "first", "step2": "second", "step3": "third"}

    def test_later_transforms_see_earlier_changes(self):
        """Test that later transforms see changes from earlier transforms."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="name", value="Alice"),
                AddFieldTransform(field="greeting", value="Hello $name"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice", "greeting": "Hello Alice"}

    def test_set_then_rename(self):
        """Test set followed by rename."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="old_name", value="data"),
                RenameFieldTransform(from_field="old_name", to_field="new_name"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"new_name": "data"}

    def test_copy_then_modify(self):
        """Test copy followed by modification."""
        engine = TransformationEngine(
            transforms=[
                CopyFieldTransform(from_field="original", to_field="backup"),
                SetFieldTransform(field="original", value="modified"),
            ]
        )

        doc = {"original": "data"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["original"] == "modified"
        assert result["backup"] == "data"

    def test_complex_pipeline(self):
        """Test complex pipeline with multiple transform types."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="first_name", value="Alice"),
                AddFieldTransform(field="last_name", value="Smith"),
                AddFieldTransform(field="full_name", value="$first_name $last_name"),
                RemoveFieldTransform(field=["first_name", "last_name"]),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Only full_name should remain
        assert result == {"full_name": "Alice Smith"}


class TestErrorHandling:
    """Tests for error handling modes."""

    def test_error_mode_skip_continues_on_error(self):
        """Test that error_mode=skip continues processing on error."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="name", value="Alice"),  # This will fail
            ],
            error_mode="skip",
        )

        doc = {"name": "Bob"}  # Field already exists
        results, stats = engine.transform_documents([doc])

        # Original document should be returned
        assert results[0] == {"name": "Bob"}
        assert stats.documents_failed == 1
        assert stats.documents_processed == 0

    def test_error_mode_fail_raises_on_error(self):
        """Test that error_mode=fail raises exception on error."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="name", value="Alice"),  # This will fail
            ],
            error_mode="fail",
        )

        doc = {"name": "Bob"}  # Field already exists
        with pytest.raises(TransformationError, match="Transform failed"):
            _, _ = engine.transform_documents([doc])

    def test_error_mode_skip_batch_processing(self):
        """Test error_mode=skip with batch processing."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ],
            error_mode="skip",
        )

        docs = [
            {"name": "Alice"},  # Will succeed
            {"name": "Bob", "status": "existing"},  # Will fail
            {"name": "Charlie"},  # Will succeed
        ]

        results, stats = engine.transform_documents(docs)

        assert len(results) == 3
        assert results[0] == {"name": "Alice", "status": "active"}
        assert results[1] == {"name": "Bob", "status": "existing"}  # Original
        assert results[2] == {"name": "Charlie", "status": "active"}
        assert stats.documents_processed == 2
        assert stats.documents_failed == 1


class TestBSONTypePreservation:
    """Tests for BSON type preservation."""

    def test_preserves_objectid(self):
        """Test that ObjectId is preserved through transforms."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        object_id = ObjectId()
        doc = {"_id": object_id, "name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["_id"] == object_id
        assert isinstance(result["_id"], ObjectId)

    def test_preserves_datetime(self):
        """Test that datetime is preserved through transforms."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        now = datetime.utcnow()
        doc = {"created_at": now, "name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["created_at"] == now
        assert isinstance(result["created_at"], datetime)

    def test_copy_preserves_objectid(self):
        """Test that copying ObjectId preserves type."""
        engine = TransformationEngine(
            transforms=[
                CopyFieldTransform(from_field="_id", to_field="original_id"),
            ]
        )

        object_id = ObjectId()
        doc = {"_id": object_id}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["_id"] == object_id
        assert result["original_id"] == object_id
        assert isinstance(result["_id"], ObjectId)
        assert isinstance(result["original_id"], ObjectId)

    def test_copy_preserves_datetime(self):
        """Test that copying datetime preserves type."""
        engine = TransformationEngine(
            transforms=[
                CopyFieldTransform(from_field="created_at", to_field="backup_created_at"),
            ]
        )

        now = datetime.utcnow()
        doc = {"created_at": now}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["created_at"] == now
        assert result["backup_created_at"] == now
        assert isinstance(result["created_at"], datetime)
        assert isinstance(result["backup_created_at"], datetime)

    def test_nested_bson_types_preserved(self):
        """Test that BSON types in nested objects are preserved."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        object_id = ObjectId()
        now = datetime.utcnow()
        doc = {
            "metadata": {
                "id": object_id,
                "timestamp": now,
            }
        }
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert isinstance(result["metadata"]["id"], ObjectId)
        assert isinstance(result["metadata"]["timestamp"], datetime)


class TestStatistics:
    """Tests for transformation statistics."""

    def test_stats_documents_processed(self):
        """Test statistics track documents processed."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        docs = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
        _, stats = engine.transform_documents(docs)

        assert stats.documents_processed == 3
        assert stats.documents_failed == 0

    def test_stats_documents_failed(self):
        """Test statistics track documents failed (error_mode=skip)."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ],
            error_mode="skip",
        )

        docs = [
            {"name": "Alice"},  # Will succeed
            {"name": "Bob", "status": "existing"},  # Will fail
        ]
        _, stats = engine.transform_documents(docs)

        assert stats.documents_processed == 1
        assert stats.documents_failed == 1

    def test_stats_transforms_applied(self):
        """Test statistics track transforms applied."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
                AddFieldTransform(field="processed", value=True),
            ]
        )

        docs = [{"name": "Alice"}, {"name": "Bob"}]
        _, stats = engine.transform_documents(docs)

        # Each document goes through all transforms (2 transforms * 2 docs = 4)
        assert stats.transforms_applied == 4

    def test_stats_initial_values(self):
        """Test that statistics start at zero."""
        stats = TransformResults()

        assert stats.documents_processed == 0
        assert stats.documents_failed == 0
        assert stats.transforms_applied == 0
        assert stats.transforms_skipped == 0
        assert stats.operations == {}

    def test_field_counts_per_transform_type(self):
        """Test that field counts are tracked correctly for each transform type."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="new_field", value="added"),
                SetFieldTransform(field="status", value="active"),
                RemoveFieldTransform(field=["temp", "cache"]),  # 2 fields removed
                RenameFieldTransform(from_field="old_name", to_field="new_name"),
                CopyFieldTransform(from_field="email", to_field="email_backup"),
                RegexReplaceTransform(field="phone", pattern=r"\D", replacement=""),
                AnonymizeTransform(field="ssn", operator="hash"),
            ]
        )

        documents = [
            {
                "status": "inactive",
                "temp": "xyz",
                "cache": "abc",
                "old_name": "test",
                "email": "test@example.com",
                "phone": "123-456-7890",
                "ssn": "123-45-6789",
            }
        ]

        result_docs, stats = engine.transform_documents(documents)

        # Verify operation results
        assert "add_field" in stats.operations
        assert stats.operations["add_field"].fields_processed == 1  # new_field

        assert "set_field" in stats.operations
        assert stats.operations["set_field"].fields_processed == 1  # status

        assert "remove_field" in stats.operations
        assert stats.operations["remove_field"].fields_processed == 2  # temp, cache

        assert "rename_field" in stats.operations
        assert stats.operations["rename_field"].fields_processed == 1  # old_name -> new_name

        assert "copy_field" in stats.operations
        assert stats.operations["copy_field"].fields_processed == 1  # email -> email_backup

        assert "regex_replace" in stats.operations
        assert stats.operations["regex_replace"].fields_processed == 1  # phone

        assert "anonymize" in stats.operations
        assert stats.operations["anonymize"].fields_processed == 1  # ssn (1 transform × 1 document)

    def test_field_counts_with_multiple_documents(self):
        """Test field counts with multiple documents and anonymize transforms."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="environment", value="prod"),
                AnonymizeTransform(field="email", operator="mask_email"),
                AnonymizeTransform(field="phone", operator="mask_phone"),
            ]
        )

        documents = [{"name": "Alice", "email": "alice@example.com", "phone": "123-456-7890"}] * 3

        result_docs, stats = engine.transform_documents(documents)

        # Verify operation results
        assert "add_field" in stats.operations
        assert (
            stats.operations["add_field"].fields_processed == 3
        )  # environment added to 3 documents

        assert "anonymize" in stats.operations
        assert (
            stats.operations["anonymize"].fields_processed == 6
        )  # 2 anonymize transforms × 3 documents


class TestTemplateResolution:
    """Tests for template value resolution."""

    def test_template_single_field_reference(self):
        """Test single field reference template."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="copy", value="$name"),
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["copy"] == "Alice"

    def test_template_nested_field_reference(self):
        """Test nested field reference template."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="city_copy", value="$address.city"),
            ]
        )

        doc = {"address": {"city": "NYC"}}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["city_copy"] == "NYC"

    def test_template_concatenation(self):
        """Test concatenation template."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="full_name", value="$first $last"),
            ]
        )

        doc = {"first": "Alice", "last": "Smith"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["full_name"] == "Alice Smith"

    def test_template_concatenation_with_literal(self):
        """Test concatenation with literal text (space-separated)."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="display", value="User: $username (admin)"),
            ]
        )

        doc = {"username": "alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["display"] == "User: alice (admin)"

    def test_template_missing_field_returns_none(self):
        """Test template with missing field reference."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="copy", value="$nonexistent"),
            ]
        )

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["copy"] is None

    def test_template_concatenation_missing_field(self):
        """Test concatenation template with missing field."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="full_name", value="$first $last"),
            ]
        )

        doc = {"first": "Alice"}  # Missing 'last'
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # Missing field should be empty string in concatenation
        assert result["full_name"] == "Alice "

    def test_template_special_value_now(self):
        """Test $now special value."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="timestamp", value="$now"),
            ]
        )

        doc = {}
        before = datetime.utcnow()
        result, _ = engine.transform_documents([doc])
        result = result[0]
        after = datetime.utcnow()

        assert isinstance(result["timestamp"], datetime)
        assert before <= result["timestamp"] <= after

    def test_template_special_value_null(self):
        """Test $null special value."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="deleted_at", value="$null"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["deleted_at"] is None

    def test_literal_dollar_sign_not_template(self):
        """Test that literal $ in middle of string is not treated as template."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="price", value="$100"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        # "$100" is treated as field reference "100", which doesn't exist
        assert result["price"] is None


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_empty_document(self):
        """Test transforming empty document."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"status": "active"}

    def test_empty_transform_list(self):
        """Test engine with no transforms."""
        engine = TransformationEngine(transforms=[])

        doc = {"name": "Alice"}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result == {"name": "Alice"}

    def test_empty_document_batch(self):
        """Test transforming empty batch."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        results, stats = engine.transform_documents([])

        assert results == []
        assert stats.documents_processed == 0

    def test_deeply_nested_operations(self):
        """Test operations on deeply nested fields."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="a.b.c.d.e.f", value="deep"),
                SetFieldTransform(field="x.y.z", value="$a.b.c.d.e.f"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["a"]["b"]["c"]["d"]["e"]["f"] == "deep"
        assert result["x"]["y"]["z"] == "deep"

    def test_field_path_with_special_characters(self):
        """Test field paths are simple strings (no special escaping needed)."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="field-with-dash", value="data"),
                AddFieldTransform(field="field_with_underscore", value="data2"),
            ]
        )

        doc = {}
        result, _ = engine.transform_documents([doc])
        result = result[0]

        assert result["field-with-dash"] == "data"
        assert result["field_with_underscore"] == "data2"

    def test_set_field_non_dict_parent_raises_error(self):
        """Test that setting nested field when parent is not dict raises error."""
        engine = TransformationEngine(
            transforms=[
                SetFieldTransform(field="name.first", value="Alice"),
            ],
            error_mode="fail",
        )

        doc = {"name": "Bob"}  # 'name' is a string, not dict
        with pytest.raises(TransformationError, match="is not a dictionary"):
            _, _ = engine.transform_documents([doc])

    def test_transform_preserves_original_document(self):
        """Test that transform doesn't mutate original document."""
        engine = TransformationEngine(
            transforms=[
                AddFieldTransform(field="status", value="active"),
            ]
        )

        original = {"name": "Alice"}
        original_copy = original.copy()

        result, _ = engine.transform_documents([original])
        result = result[0]

        # Original should be unchanged
        assert original == original_copy
        assert result != original
        assert result == {"name": "Alice", "status": "active"}
