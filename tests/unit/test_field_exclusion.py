"""Tests for field exclusion engine."""

from mongo_replication.engine.field_exclusion import FieldExcluder


class TestFieldExcluderBasic:
    """Basic tests for field exclusion."""

    def test_exclude_top_level_field(self):
        """Test excluding a single top-level field."""
        excluder = FieldExcluder(["internalNotes"])

        doc = {"_id": 1, "name": "John", "internalNotes": "Confidential info"}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        assert len(processed_docs) == 1
        assert "name" in processed_docs[0]
        assert "_id" in processed_docs[0]
        assert "internalNotes" not in processed_docs[0]
        assert stats.documents_processed == 1
        assert stats.fields_excluded == 1

    def test_exclude_nested_field_keeps_parent(self):
        """Test excluding nested field keeps parent with remaining fields."""
        excluder = FieldExcluder(["audit.raw"])

        doc = {
            "_id": 1,
            "name": "John",
            "audit": {"raw": "detailed_logs", "timestamp": "2024-01-01", "user": "admin"},
        }
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # audit.raw removed but audit object remains with other fields
        assert "audit" in processed_docs[0]
        assert "raw" not in processed_docs[0]["audit"]
        assert processed_docs[0]["audit"]["timestamp"] == "2024-01-01"
        assert processed_docs[0]["audit"]["user"] == "admin"
        assert stats.fields_excluded == 1

    def test_exclude_nested_field_removes_empty_parent(self):
        """Test that empty parent is removed after nested field exclusion."""
        excluder = FieldExcluder(["debug.traces"])

        doc = {"_id": 1, "name": "John", "debug": {"traces": "detailed_traces"}}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # debug object should be removed since it's now empty
        assert "debug" not in processed_docs[0]
        assert "name" in processed_docs[0]

    def test_exclude_multiple_fields(self):
        """Test excluding multiple fields at once."""
        excluder = FieldExcluder(["internalNotes", "debugInfo", "tempData"])

        doc = {
            "_id": 1,
            "name": "John",
            "internalNotes": "note1",
            "debugInfo": "debug1",
            "tempData": "temp1",
            "email": "john@example.com",
        }
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        assert "internalNotes" not in processed_docs[0]
        assert "debugInfo" not in processed_docs[0]
        assert "tempData" not in processed_docs[0]
        assert processed_docs[0]["name"] == "John"
        assert processed_docs[0]["email"] == "john@example.com"
        assert stats.fields_excluded == 3

    def test_no_exclusions_returns_unchanged(self):
        """Test that documents pass through unchanged when no exclusions configured."""
        excluder = FieldExcluder([])

        doc = {"_id": 1, "name": "John", "email": "john@example.com"}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        assert processed_docs[0] == doc
        assert stats.documents_processed == 1
        assert stats.fields_excluded == 0

    def test_field_not_found_skipped_silently(self):
        """Test that exclusion is skipped if field doesn't exist."""
        excluder = FieldExcluder(["nonexistent"])

        doc = {"_id": 1, "name": "John"}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # Document unchanged
        assert processed_docs[0] == doc
        # No fields excluded (field didn't exist)
        assert stats.fields_excluded == 0


class TestFieldExcluderBatchProcessing:
    """Tests for batch document processing."""

    def test_batch_exclude_multiple_documents(self):
        """Test excluding fields from multiple documents in a batch."""
        excluder = FieldExcluder(["internalNotes"])

        docs = [
            {"_id": 1, "name": "John", "internalNotes": "note1"},
            {"_id": 2, "name": "Jane", "internalNotes": "note2"},
            {"_id": 3, "name": "Bob", "internalNotes": "note3"},
        ]
        processed_docs, stats = excluder.exclude_fields_from_documents(docs)

        assert len(processed_docs) == 3
        assert all("internalNotes" not in doc for doc in processed_docs)
        assert all("name" in doc for doc in processed_docs)
        assert stats.documents_processed == 3
        assert stats.fields_excluded == 3

    def test_batch_with_mixed_presence(self):
        """Test batch where some documents have the field and some don't."""
        excluder = FieldExcluder(["internalNotes"])

        docs = [
            {"_id": 1, "name": "John", "internalNotes": "note1"},  # Has field
            {"_id": 2, "name": "Jane"},  # Doesn't have field
            {"_id": 3, "name": "Bob", "internalNotes": "note3"},  # Has field
        ]
        processed_docs, stats = excluder.exclude_fields_from_documents(docs)

        assert "internalNotes" not in processed_docs[0]
        assert "name" in processed_docs[1]
        assert "internalNotes" not in processed_docs[2]
        assert stats.documents_processed == 3
        assert stats.fields_excluded == 2  # Only 2 docs had the field


class TestFieldExcluderNestedFieldLogic:
    """Tests for nested field exclusion logic."""

    def test_deeply_nested_field_exclusion(self):
        """Test excluding deeply nested field."""
        excluder = FieldExcluder(["a.b.c.d.e"])

        doc = {"_id": 1, "a": {"b": {"c": {"d": {"e": "to_exclude", "f": "to_keep"}}}}}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # e removed, f kept, parent d kept
        assert "e" not in processed_docs[0]["a"]["b"]["c"]["d"]
        assert processed_docs[0]["a"]["b"]["c"]["d"]["f"] == "to_keep"

    def test_deeply_nested_field_removes_empty_parents(self):
        """Test that deeply nested exclusion removes all empty parents."""
        excluder = FieldExcluder(["a.b.c.d.e"])

        doc = {"_id": 1, "name": "test", "a": {"b": {"c": {"d": {"e": "to_exclude"}}}}}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # Entire a.b.c.d.e chain should be removed since all become empty
        assert "a" not in processed_docs[0]
        assert processed_docs[0]["name"] == "test"

    def test_exclude_multiple_nested_fields_same_parent(self):
        """Test excluding multiple fields from same parent object."""
        excluder = FieldExcluder(["audit.raw", "audit.debug"])

        doc = {
            "_id": 1,
            "audit": {
                "raw": "raw_data",
                "debug": "debug_data",
                "timestamp": "2024-01-01",
                "user": "admin",
            },
        }
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # Both raw and debug removed, but timestamp and user kept
        assert "audit" in processed_docs[0]
        assert "raw" not in processed_docs[0]["audit"]
        assert "debug" not in processed_docs[0]["audit"]
        assert processed_docs[0]["audit"]["timestamp"] == "2024-01-01"
        assert processed_docs[0]["audit"]["user"] == "admin"
        assert stats.fields_excluded == 2

    def test_exclude_parent_and_child_removes_both(self):
        """Test that excluding both parent and child works correctly."""
        excluder = FieldExcluder(["audit", "audit.raw"])

        doc = {"_id": 1, "audit": {"raw": "raw_data", "timestamp": "2024-01-01"}}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # Parent audit removed (takes precedence)
        assert "audit" not in processed_docs[0]
        # Second exclusion attempt has no effect (parent already gone)
        # But both exclusions are attempted, so stats may vary based on order

    def test_nested_field_with_partial_path_invalid(self):
        """Test that partial path match doesn't affect exclusion."""
        excluder = FieldExcluder(["audit.raw"])

        doc = {
            "_id": 1,
            "audit": "simple_string",  # Not an object
            "auditRaw": "different_field",
        }
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # audit.raw doesn't exist (audit is not a dict), so nothing excluded
        assert processed_docs[0]["audit"] == "simple_string"
        assert processed_docs[0]["auditRaw"] == "different_field"
        assert stats.fields_excluded == 0


class TestFieldExcluderEdgeCases:
    """Edge case tests."""

    def test_empty_document_list(self):
        """Test excluding fields from empty document list."""
        excluder = FieldExcluder(["internalNotes"])

        processed_docs, stats = excluder.exclude_fields_from_documents([])

        assert processed_docs == []
        assert stats.documents_processed == 0
        assert stats.fields_excluded == 0

    def test_document_with_no_fields_to_exclude(self):
        """Test document that doesn't have any of the excluded fields."""
        excluder = FieldExcluder(["field1", "field2", "field3"])

        doc = {"_id": 1, "name": "John", "email": "john@example.com"}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # Document unchanged
        assert processed_docs[0] == doc
        assert stats.fields_excluded == 0

    def test_exclude_id_field(self):
        """Test that _id field can be excluded (though not recommended)."""
        excluder = FieldExcluder(["_id"])

        doc = {"_id": 1, "name": "John"}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # _id removed
        assert "_id" not in processed_docs[0]
        assert processed_docs[0]["name"] == "John"
        assert stats.fields_excluded == 1

    def test_exclude_with_array_values(self):
        """Test that exclusion works with documents containing arrays."""
        excluder = FieldExcluder(["tags", "metadata.flags"])

        doc = {
            "_id": 1,
            "name": "John",
            "tags": ["tag1", "tag2"],
            "metadata": {"flags": ["flag1", "flag2"], "version": "1.0"},
        }
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        assert "tags" not in processed_docs[0]
        assert "flags" not in processed_docs[0]["metadata"]
        assert processed_docs[0]["metadata"]["version"] == "1.0"
        assert stats.fields_excluded == 2

    def test_original_document_not_mutated(self):
        """Test that original document is not modified."""
        excluder = FieldExcluder(["internalNotes"])

        original_doc = {"_id": 1, "name": "John", "internalNotes": "note"}
        original_notes = original_doc["internalNotes"]

        processed_docs, stats = excluder.exclude_fields_from_documents([original_doc])

        # Original unchanged
        assert original_doc["internalNotes"] == original_notes
        assert "internalNotes" in original_doc

        # Processed has field removed
        assert "internalNotes" not in processed_docs[0]

    def test_exclude_field_with_none_value(self):
        """Test excluding field that has None value."""
        excluder = FieldExcluder(["optionalField"])

        doc = {"_id": 1, "name": "John", "optionalField": None}
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        # Field with None value is still excluded
        assert "optionalField" not in processed_docs[0]
        assert stats.fields_excluded == 1

    def test_complex_nested_structure_with_multiple_exclusions(self):
        """Test complex document with multiple nested exclusions."""
        excluder = FieldExcluder(
            ["internal.notes", "internal.temp", "debug.traces", "metadata.raw.logs"]
        )

        doc = {
            "_id": 1,
            "name": "John",
            "internal": {"notes": "note1", "temp": "temp1", "id": "internal123"},
            "debug": {"traces": "trace1", "level": "info"},
            "metadata": {"raw": {"logs": "log_data", "metrics": "metric_data"}, "version": "1.0"},
        }
        processed_docs, stats = excluder.exclude_fields_from_documents([doc])

        result = processed_docs[0]

        # Check internal object
        assert "notes" not in result["internal"]
        assert "temp" not in result["internal"]
        assert result["internal"]["id"] == "internal123"

        # Check debug object
        assert "traces" not in result["debug"]
        assert result["debug"]["level"] == "info"

        # Check metadata object
        assert "logs" not in result["metadata"]["raw"]
        assert result["metadata"]["raw"]["metrics"] == "metric_data"
        assert result["metadata"]["version"] == "1.0"

        assert stats.fields_excluded == 4
