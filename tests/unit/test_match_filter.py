"""Tests for MongoDB match filter integration in CollectionReplicator."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from mongo_replication.engine.indexes import IndexManager
from mongo_replication.engine.replicator import CollectionReplicator
from mongo_replication.engine.state import StateManager
from mongo_replication.engine.validation import CursorValidator


class TestMatchFilterQueryBuilding:
    """Tests for _build_query method with match filter."""

    def setup_method(self):
        """Set up test fixtures."""
        self.source_collection = MagicMock()
        self.source_collection.name = "test_collection"
        self.dest_collection = MagicMock()
        self.state_manager = MagicMock(spec=StateManager)
        self.cursor_validator = MagicMock(spec=CursorValidator)
        self.index_manager = MagicMock(spec=IndexManager)

        self.replicator = CollectionReplicator(
            source_collection=self.source_collection,
            dest_collection=self.dest_collection,
            state_manager=self.state_manager,
            cursor_validator=self.cursor_validator,
            index_manager=self.index_manager,
        )

    def test_build_query_no_cursor_no_match(self):
        """Test query building with no cursor and no match filter."""
        self.replicator._match_filter = {}
        self.state_manager.get_last_cursor_value.return_value = None

        query = self.replicator._build_query("")

        assert query == {}

    def test_build_query_with_cursor_only(self):
        """Test query building with cursor but no match filter."""
        self.replicator._match_filter = {}
        last_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.state_manager.get_last_cursor_value.return_value = last_date

        query = self.replicator._build_query("updatedAt")

        assert query == {"updatedAt": {"$gt": last_date}}

    def test_build_query_with_match_filter_only(self):
        """Test query building with match filter but no cursor."""
        match_filter = {"status": "active", "type": "premium"}
        self.replicator._match_filter = match_filter
        self.state_manager.get_last_cursor_value.return_value = None

        query = self.replicator._build_query("")

        assert query == match_filter

    def test_build_query_with_cursor_and_match_filter(self):
        """Test query building with both cursor and match filter (combined with $and)."""
        match_filter = {"status": "active"}
        self.replicator._match_filter = match_filter
        last_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.state_manager.get_last_cursor_value.return_value = last_date

        query = self.replicator._build_query("updatedAt")

        # Should combine with $and
        assert "$and" in query
        assert len(query["$and"]) == 2
        assert {"updatedAt": {"$gt": last_date}} in query["$and"]
        assert match_filter in query["$and"]

    def test_build_query_with_complex_match_filter(self):
        """Test query building with complex match filter using operators."""
        match_filter = {
            "status": {"$in": ["active", "pending"]},
            "email": {"$regex": ".*@acme\\.com$"},
            "$or": [{"accountType": "premium"}, {"credits": {"$gt": 100}}],
        }
        self.replicator._match_filter = match_filter
        self.state_manager.get_last_cursor_value.return_value = None

        query = self.replicator._build_query("")

        assert query == match_filter

    def test_build_query_cursor_with_complex_match_and_combines(self):
        """Test that complex match filter properly combines with cursor using $and."""
        match_filter = {"$or": [{"status": "active"}, {"status": "pending"}]}
        self.replicator._match_filter = match_filter
        last_value = 100
        self.state_manager.get_last_cursor_value.return_value = last_value

        query = self.replicator._build_query("counter")

        # Must use $and to avoid conflicts
        assert "$and" in query
        assert {"counter": {"$gt": last_value}} in query["$and"]
        assert match_filter in query["$and"]


class TestMatchFilterIntegration:
    """Integration tests for match filter in replication flow."""

    def setup_method(self):
        """Set up test fixtures."""
        self.source_collection = MagicMock()
        self.source_collection.name = "users"
        self.dest_collection = MagicMock()
        self.state_manager = MagicMock(spec=StateManager)
        self.cursor_validator = MagicMock(spec=CursorValidator)
        self.index_manager = MagicMock(spec=IndexManager)

        # Mock index replication to return success
        self.index_manager.replicate_indexes.return_value = (0, 0, [])

        self.replicator = CollectionReplicator(
            source_collection=self.source_collection,
            dest_collection=self.dest_collection,
            state_manager=self.state_manager,
            cursor_validator=self.cursor_validator,
            index_manager=self.index_manager,
        )

    def test_replicate_stores_match_filter(self):
        """Test that replicate() stores match_filter for use in _build_query."""
        match_filter = {"status": "active"}

        # Mock necessary methods to avoid full replication
        self.cursor_validator.validate_cursor_field.return_value = "updatedAt"
        self.state_manager.create_run.return_value = "run_123"
        self.state_manager.get_last_cursor_value.return_value = None

        # Mock source.find() to return empty cursor
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__iter__.return_value = iter([])  # Empty result
        self.source_collection.find.return_value = mock_cursor

        # Call replicate with match_filter
        self.replicator.replicate(
            state_id="state_123",
            cursor_field="updatedAt",
            write_disposition="merge",
            primary_key="_id",
            match_filter=match_filter,
        )

        # Verify match_filter was stored
        assert self.replicator._match_filter == match_filter

        # Verify source.find() was called (which uses _build_query internally)
        assert self.source_collection.find.called

        # Verify the query passed to find() includes the match filter
        call_args = self.source_collection.find.call_args
        query_used = call_args[0][0] if call_args[0] else {}
        assert query_used == match_filter  # First call should be with just match filter

    def test_replicate_without_match_filter_backward_compatible(self):
        """Test that replicate() works without match_filter (backward compatible)."""
        # Mock necessary methods
        self.cursor_validator.validate_cursor_field.return_value = "updatedAt"
        self.state_manager.create_run.return_value = "run_123"
        self.state_manager.get_last_cursor_value.return_value = None

        # Mock source.find()
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__iter__.return_value = iter([])
        self.source_collection.find.return_value = mock_cursor

        # Call replicate WITHOUT match_filter
        self.replicator.replicate(
            state_id="state_123",
            cursor_field="updatedAt",
            write_disposition="merge",
            primary_key="_id",
        )

        # Verify match_filter defaults to empty dict
        assert self.replicator._match_filter == {}

        # Verify query is empty (no cursor value, no match filter)
        call_args = self.source_collection.find.call_args
        query_used = call_args[0][0] if call_args[0] else {}
        assert query_used == {}

    def test_fetch_batch_uses_match_filter(self):
        """Test that _fetch_batch uses match filter via _build_query."""
        match_filter = {"status": {"$in": ["active", "pending"]}}
        self.replicator._match_filter = match_filter
        self.state_manager.get_last_cursor_value.return_value = None

        # Mock source.find()
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__iter__.return_value = iter([{"_id": 1, "status": "active"}])
        self.source_collection.find.return_value = mock_cursor

        # Call _fetch_batch
        documents = self.replicator._fetch_batch(cursor_field="updatedAt", batch_size=100, skip=0)

        # Verify find() was called with match filter
        self.source_collection.find.assert_called_once_with(match_filter)
        assert len(documents) == 1
        assert documents[0]["status"] == "active"


class TestMatchFilterEdgeCases:
    """Edge case tests for match filter."""

    def setup_method(self):
        """Set up test fixtures."""
        self.source_collection = MagicMock()
        self.source_collection.name = "test_collection"
        self.dest_collection = MagicMock()
        self.state_manager = MagicMock(spec=StateManager)
        self.cursor_validator = MagicMock(spec=CursorValidator)
        self.index_manager = MagicMock(spec=IndexManager)

        self.replicator = CollectionReplicator(
            source_collection=self.source_collection,
            dest_collection=self.dest_collection,
            state_manager=self.state_manager,
            cursor_validator=self.cursor_validator,
            index_manager=self.index_manager,
        )

    def test_match_filter_with_dollar_operators(self):
        """Test match filter with various MongoDB $ operators."""
        match_filter = {
            "age": {"$gte": 18, "$lt": 65},
            "email": {"$exists": True},
            "tags": {"$all": ["premium", "verified"]},
        }
        self.replicator._match_filter = match_filter
        self.state_manager.get_last_cursor_value.return_value = None

        query = self.replicator._build_query("")

        assert query == match_filter

    def test_match_filter_none_treated_as_empty(self):
        """Test that None match_filter is treated as empty dict."""
        self.replicator._match_filter = None
        self.state_manager.get_last_cursor_value.return_value = None

        query = self.replicator._build_query("")

        assert query == {}

    def test_match_filter_with_nested_documents(self):
        """Test match filter with nested document queries."""
        match_filter = {"address.city": "San Francisco", "address.zipCode": {"$regex": "^94"}}
        self.replicator._match_filter = match_filter
        self.state_manager.get_last_cursor_value.return_value = None

        query = self.replicator._build_query("")

        assert query == match_filter
