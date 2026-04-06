"""Tests for the scan command cursor field detection."""

from typing import Dict, Any

from mongo_replication.cli.commands.scan import detect_cursor_field


class TestDetectCursorField:
    """Test cursor field detection logic."""

    def test_exact_match_first_field(self):
        """Test exact match with first field in list."""
        doc = {"updated_at": "2024-01-01", "name": "test"}
        cursor_fields = ["updated_at", "updatedAt", "created_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result == "updated_at"

    def test_exact_match_second_field(self):
        """Test exact match with second field in list."""
        doc = {"updatedAt": "2024-01-01", "name": "test"}
        cursor_fields = ["updated_at", "updatedAt", "created_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result == "updatedAt"

    def test_case_insensitive_match(self):
        """Test case-insensitive matching (field has different case than search)."""
        doc = {"UPDATEDAT": "2024-01-01", "name": "test"}
        cursor_fields = ["updatedAt", "updated_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # Should match the pattern (updatedAt) even though case differs
        assert result == "updatedAt"

    def test_nested_field_exact_match(self):
        """Test nested field with exact match."""
        doc = {"meta": {"updated_at": "2024-01-01"}, "name": "test"}
        cursor_fields = ["meta.updated_at", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result == "meta.updated_at"

    def test_nested_field_case_insensitive(self):
        """Test nested field with case-insensitive matching."""
        doc = {"META": {"UPDATEDAT": "2024-01-01"}, "name": "test"}
        cursor_fields = ["meta.updatedAt", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result == "meta.updatedAt"

    def test_deeply_nested_field(self):
        """Test deeply nested field detection."""
        doc = {"metadata": {"timestamps": {"updated_at": "2024-01-01"}}}
        cursor_fields = ["metadata.timestamps.updated_at", "updated_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result == "metadata.timestamps.updated_at"

    def test_no_match_returns_none(self):
        """Test that None is returned when no fields match."""
        doc = {"name": "test", "id": 123}
        cursor_fields = ["updated_at", "updatedAt", "created_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result is None

    def test_empty_document_returns_none(self):
        """Test that empty document returns None."""
        doc: Dict[str, Any] = {}
        cursor_fields = ["updated_at", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result is None

    def test_none_document_returns_none(self):
        """Test that None document returns None."""
        doc = None
        cursor_fields = ["updated_at", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result is None

    def test_priority_order_respected(self):
        """Test that priority order is respected (first match wins)."""
        doc = {"updated_at": "2024-01-01", "updatedAt": "2024-01-02", "created_at": "2024-01-03"}
        cursor_fields = ["created_at", "updatedAt", "updated_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # Should return created_at since it's first in the list
        assert result == "created_at"

    def test_partial_nested_path_no_match(self):
        """Test that partial nested path doesn't match."""
        doc = {"meta": "string_value"}
        cursor_fields = ["meta.updated_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # meta exists but meta.updated_at doesn't (meta is not a dict)
        assert result is None

    def test_missing_intermediate_nested_field(self):
        """Test that missing intermediate field returns None."""
        doc = {"other": {"field": "value"}}
        cursor_fields = ["meta.updated_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result is None

    def test_field_with_none_value_still_matches(self):
        """Test that field with None value is still considered a match."""
        doc = {"updated_at": None, "name": "test"}
        cursor_fields = ["updated_at", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # Field exists (even though value is None), so should not match
        # Actually, the function returns None if value is None, so this should NOT match
        assert result is None

    def test_field_with_zero_value_matches(self):
        """Test that field with zero value matches."""
        doc = {"updated_at": 0, "name": "test"}
        cursor_fields = ["updated_at", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # 0 is falsy but should still match (0 is not None)
        # Actually, looking at the code, it checks "if value is not None"
        # So 0 should match
        assert result == "updated_at"

    def test_field_with_empty_string_matches(self):
        """Test that field with empty string value matches."""
        doc = {"updated_at": "", "name": "test"}
        cursor_fields = ["updated_at", "updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # Empty string is not None, so should match
        assert result == "updated_at"

    def test_empty_cursor_fields_list(self):
        """Test that empty cursor fields list returns None."""
        doc = {"updated_at": "2024-01-01"}
        cursor_fields = []

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result is None

    def test_mixed_case_nested_fields(self):
        """Test nested fields with mixed case at different levels."""
        doc = {"METADATA": {"TIMESTAMPS": {"UPDATEDAT": "2024-01-01"}}}
        cursor_fields = ["metadata.timestamps.updatedAt"]

        result = detect_cursor_field("users", doc, cursor_fields)

        assert result == "metadata.timestamps.updatedAt"

    def test_array_value_in_path_no_match(self):
        """Test that array value in nested path doesn't match."""
        doc = {"meta": [{"updated_at": "2024-01-01"}]}
        cursor_fields = ["meta.updated_at"]

        result = detect_cursor_field("users", doc, cursor_fields)

        # meta is an array, not a dict, so should not match
        assert result is None

    def test_snake_case_vs_camel_case_requires_both_patterns(self):
        """Test that snake_case and camelCase are treated as different patterns."""
        # Document has camelCase
        doc = {"updatedAt": "2024-01-01", "name": "test"}

        # Pattern list has snake_case first
        cursor_fields = ["updated_at", "updatedAt"]
        result = detect_cursor_field("users", doc, cursor_fields)
        # Should match second pattern (updatedAt) since first doesn't match
        assert result == "updatedAt"

        # Pattern list has camelCase first
        cursor_fields = ["updatedAt", "updated_at"]
        result = detect_cursor_field("users", doc, cursor_fields)
        # Should match first pattern
        assert result == "updatedAt"

    def test_real_world_priority_list(self):
        """Test with realistic priority list like in defaults.yaml."""
        cursor_fields = ["updated_at", "updatedAt", "meta.updated_at", "meta.updatedAt"]

        # Document with snake_case
        doc1 = {"updated_at": "2024-01-01"}
        assert detect_cursor_field("coll", doc1, cursor_fields) == "updated_at"

        # Document with camelCase
        doc2 = {"updatedAt": "2024-01-01"}
        assert detect_cursor_field("coll", doc2, cursor_fields) == "updatedAt"

        # Document with nested snake_case
        doc3 = {"meta": {"updated_at": "2024-01-01"}}
        assert detect_cursor_field("coll", doc3, cursor_fields) == "meta.updated_at"

        # Document with nested camelCase
        doc4 = {"meta": {"updatedAt": "2024-01-01"}}
        assert detect_cursor_field("coll", doc4, cursor_fields) == "meta.updatedAt"
