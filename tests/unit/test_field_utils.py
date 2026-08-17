"""Unit tests for field_utils module (pattern matching and field extraction)."""

from mongo_replication.engine.pii.field_utils import (
    get_all_field_paths,
    matches_pattern,
    normalize_array_path,
)


class TestMatchesPattern:
    """Test the matches_pattern function for wildcard matching."""

    def test_exact_match(self):
        """Test exact field path matching."""
        assert matches_pattern("user.email", "user.email")
        assert matches_pattern("_id", "_id")
        assert matches_pattern("nested.field.path", "nested.field.path")

    def test_no_match(self):
        """Test non-matching patterns."""
        assert not matches_pattern("user.email", "user.phone")
        assert not matches_pattern("user.email", "email")
        assert not matches_pattern("user.profile.email", "user.email")

    def test_suffix_wildcard(self):
        """Test patterns with suffix wildcard (e.g., 'metadata.*')."""
        assert matches_pattern("metadata.created_at", "metadata.*")
        assert matches_pattern("metadata.updated_at", "metadata.*")
        assert matches_pattern("metadata.user_id", "metadata.*")
        assert matches_pattern("metadata.nested.field", "metadata.*")

        # Should not match
        assert not matches_pattern("meta.created_at", "metadata.*")
        assert not matches_pattern("user.metadata", "metadata.*")

    def test_prefix_wildcard(self):
        """Test patterns with prefix wildcard (e.g., '*.email').

        Note: This uses substring matching, so '*.email' will match 'user.emailAddress'
        because '.email' is a substring of '.emailAddress'. This is the current behavior
        from the original implementation.
        """
        assert matches_pattern("user.email", "*.email")
        assert matches_pattern("contact.email", "*.email")
        assert matches_pattern("profile.email", "*.email")
        assert matches_pattern("nested.user.email", "*.email")

        # Should not match (no .email substring at all)
        assert not matches_pattern("email", "*.email")
        assert not matches_pattern("user.mail", "*.email")

    def test_middle_wildcard(self):
        """Test patterns with wildcard in the middle (e.g., 'user.*.email')."""
        assert matches_pattern("user.profile.email", "user.*.email")
        assert matches_pattern("user.contact.email", "user.*.email")
        assert matches_pattern("user.forms.email", "user.*.email")
        assert matches_pattern("user.deeply.nested.email", "user.*.email")

        # Should not match
        assert not matches_pattern("user.email", "user.*.email")
        assert not matches_pattern("profile.user.email", "user.*.email")
        assert not matches_pattern("user.profile.phone", "user.*.email")

    def test_multiple_wildcards(self):
        """Test patterns with multiple wildcards."""
        assert matches_pattern("user.profile.contact.email", "user.*.*.email")
        assert matches_pattern("data.user.forms.sinNumber", "*.*.sinNumber")
        assert matches_pattern("root.nested.deep.value", "root.*.*")

    def test_wildcard_only(self):
        """Test single wildcard pattern."""
        # Single wildcard should match everything (starts with empty string and ends with empty string)
        assert matches_pattern("any.field", "*")
        assert matches_pattern("user.email", "*")
        assert matches_pattern("_id", "*")


class TestGetAllFieldPaths:
    """Test the get_all_field_paths function for field extraction."""

    def test_flat_document(self):
        """Test field extraction from flat document."""
        doc = {"name": "John", "age": 30, "email": "john@example.com"}
        paths = get_all_field_paths(doc)
        assert paths == {"name", "age", "email"}

    def test_nested_document(self):
        """Test field extraction from nested document."""
        doc = {
            "user": {
                "name": "John",
                "email": "john@example.com",
                "profile": {"age": 30, "city": "NYC"},
            }
        }
        paths = get_all_field_paths(doc)
        assert paths == {
            "user",
            "user.name",
            "user.email",
            "user.profile",
            "user.profile.age",
            "user.profile.city",
        }

    def test_array_document(self):
        """Test field extraction from document with arrays."""
        doc = {
            "contacts": [
                {"email": "a@test.com", "phone": "123"},
                {"email": "b@test.com", "phone": "456"},
            ]
        }
        paths = get_all_field_paths(doc)
        # Should include array container and fields from first element
        assert "contacts" in paths
        assert "contacts.email" in paths
        assert "contacts.phone" in paths

    def test_deeply_nested_with_arrays(self):
        """Test field extraction from complex nested document with arrays."""
        doc = {
            "user": {
                "name": "John",
                "forms": [{"sinNumber": "123-456-789", "type": "tax"}],
                "profile": {"sinNumber": "987-654-321", "address": {"street": "123 Main St"}},
            }
        }
        paths = get_all_field_paths(doc)

        assert "user" in paths
        assert "user.name" in paths
        assert "user.forms" in paths
        assert "user.forms.sinNumber" in paths
        assert "user.forms.type" in paths
        assert "user.profile" in paths
        assert "user.profile.sinNumber" in paths
        assert "user.profile.address" in paths
        assert "user.profile.address.street" in paths

    def test_empty_document(self):
        """Test field extraction from empty document."""
        doc = {}
        paths = get_all_field_paths(doc)
        assert paths == set()

    def test_empty_arrays(self):
        """Test field extraction with empty arrays."""
        doc = {"name": "John", "contacts": []}
        paths = get_all_field_paths(doc)
        assert paths == {"name", "contacts"}


class TestNormalizeArrayPath:
    """Test the normalize_array_path function for array index removal."""

    def test_simple_array_path(self):
        """Test normalizing simple array paths."""
        assert normalize_array_path("contacts[0].email") == "contacts.email"
        assert normalize_array_path("contacts[5].name") == "contacts.name"

    def test_nested_array_path(self):
        """Test normalizing nested array paths."""
        assert normalize_array_path("invitations[0].invitee.email") == "invitations.invitee.email"
        assert normalize_array_path("users[2].forms[1].sinNumber") == "users.forms.sinNumber"

    def test_no_array_indices(self):
        """Test paths without array indices."""
        assert normalize_array_path("user.email") == "user.email"
        assert normalize_array_path("simple.field") == "simple.field"

    def test_multiple_indices(self):
        """Test paths with multiple array indices."""
        assert normalize_array_path("root[0].nested[1].field[2].value") == "root.nested.field.value"

    def test_edge_cases(self):
        """Test edge cases."""
        assert normalize_array_path("") == ""
        assert normalize_array_path("field") == "field"
        assert normalize_array_path("[0]") == ""
        assert normalize_array_path("array[0]") == "array"
