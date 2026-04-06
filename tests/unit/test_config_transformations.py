"""Tests for transformation configuration loading and validation."""

import re

import pytest

from mongo_replication.config.loader import (
    FieldTransformConfig,
    CollectionConfig,
)


class TestFieldTransformConfig:
    """Tests for FieldTransformConfig validation."""

    def test_valid_regex_replace_transform(self):
        """Test creating a valid regex_replace transformation."""
        transform = FieldTransformConfig(
            field="email", type="regex_replace", pattern=r"@old\.com$", replacement="@new.com"
        )

        assert transform.field == "email"
        assert transform.type == "regex_replace"
        assert transform.pattern == r"@old\.com$"
        assert transform.replacement == "@new.com"

    def test_nested_field_path(self):
        """Test transformation with nested field path."""
        transform = FieldTransformConfig(
            field="company.domain",
            type="regex_replace",
            pattern=r"oldcompany\.com",
            replacement="newcompany.com",
        )

        assert transform.field == "company.domain"

    def test_invalid_transform_type(self):
        """Test that invalid transformation type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid transformation type.*unsupported"):
            FieldTransformConfig(
                field="email", type="unsupported", pattern="test", replacement="test"
            )

    def test_invalid_regex_pattern(self):
        """Test that invalid regex pattern raises ValueError at config load time."""
        with pytest.raises(ValueError, match="Invalid regex pattern"):
            FieldTransformConfig(
                field="email",
                type="regex_replace",
                pattern=r"[invalid(regex",  # Unclosed bracket
                replacement="test",
            )

    def test_complex_regex_pattern(self):
        """Test that complex but valid regex patterns work."""
        transform = FieldTransformConfig(
            field="phone",
            type="regex_replace",
            pattern=r"^\+1[- ]?\(?\d{3}\)?[- ]?\d{3}[- ]?\d{4}$",
            replacement="+1-XXX-XXX-XXXX",
        )

        # Verify the pattern compiles
        re.compile(transform.pattern)
        assert transform.pattern is not None


class TestCollectionConfigWithTransformations:
    """Tests for CollectionConfig with new transformation fields."""

    def test_collection_with_all_new_fields(self):
        """Test collection config with all new transformation fields."""
        transforms = [
            FieldTransformConfig(
                field="email", type="regex_replace", pattern=r"@old\.com$", replacement="@new.com"
            )
        ]

        config = CollectionConfig(
            name="users",
            cursor_field="updatedAt",
            write_disposition="merge",
            primary_key="_id",
            pii_fields={"ssn": "hash"},
            match={"status": {"$in": ["active", "pending"]}},
            field_transforms=transforms,
            fields_exclude=["internalNotes", "debugInfo"],
            transform_error_mode="fail",
        )

        assert config.name == "users"
        assert config.match == {"status": {"$in": ["active", "pending"]}}
        assert len(config.field_transforms) == 1
        assert config.field_transforms[0].field == "email"
        assert config.fields_exclude == ["internalNotes", "debugInfo"]
        assert config.transform_error_mode == "fail"

    def test_collection_with_default_transform_fields(self):
        """Test that new transformation fields have proper defaults."""
        config = CollectionConfig(
            name="users",
            cursor_field="updatedAt",
            write_disposition="merge",
            primary_key="_id",
            pii_fields={},
        )

        assert config.match is None
        assert config.field_transforms == []
        assert config.fields_exclude == []
        assert config.transform_error_mode == "skip"  # Default

    def test_invalid_transform_error_mode(self):
        """Test that invalid transform_error_mode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid transform_error_mode.*invalid_mode"):
            CollectionConfig(
                name="users",
                cursor_field="updatedAt",
                write_disposition="merge",
                primary_key="_id",
                pii_fields={},
                transform_error_mode="invalid_mode",
            )

    def test_nested_field_exclusion(self):
        """Test that nested fields can be excluded."""
        config = CollectionConfig(
            name="users",
            cursor_field="updatedAt",
            write_disposition="merge",
            primary_key="_id",
            pii_fields={},
            fields_exclude=["audit.raw", "debug.traces"],
        )

        assert "audit.raw" in config.fields_exclude
        assert "debug.traces" in config.fields_exclude
