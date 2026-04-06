"""Unit tests for IndexManager."""

from unittest.mock import MagicMock
from pymongo.errors import OperationFailure

from mongo_replication.engine.indexes import IndexInfo, IndexManager


class TestIndexInfo:
    """Tests for IndexInfo dataclass."""

    def test_basic_index_info(self):
        """Test creating basic index info."""
        info = IndexInfo(
            name="email_1",
            keys=[("email", 1)],
        )

        assert info.name == "email_1"
        assert info.keys == [("email", 1)]
        assert info.unique is False
        assert info.sparse is False
        assert info.expire_after_seconds is None

    def test_unique_index_info(self):
        """Test creating unique index info."""
        info = IndexInfo(
            name="email_1",
            keys=[("email", 1)],
            unique=True,
        )

        assert info.unique is True

    def test_compound_index_info(self):
        """Test creating compound index info."""
        info = IndexInfo(
            name="user_created_1",
            keys=[("user_id", 1), ("created_at", -1)],
        )

        assert len(info.keys) == 2
        assert info.keys[0] == ("user_id", 1)
        assert info.keys[1] == ("created_at", -1)

    def test_ttl_index_info(self):
        """Test creating TTL index info."""
        info = IndexInfo(
            name="created_at_1",
            keys=[("created_at", 1)],
            expire_after_seconds=3600,
        )

        assert info.expire_after_seconds == 3600

    def test_partial_index_info(self):
        """Test creating partial index info."""
        filter_expr = {"status": {"$eq": "active"}}
        info = IndexInfo(
            name="active_users_1",
            keys=[("user_id", 1)],
            partial_filter_expression=filter_expr,
        )

        assert info.partial_filter_expression == filter_expr


class TestIndexManager:
    """Tests for IndexManager."""

    def test_get_indexes_empty_collection(self):
        """Test getting indexes from collection with only _id index."""
        # Mock collection that returns only _id index
        mock_collection = MagicMock()
        mock_collection.name = "test_collection"
        mock_collection.list_indexes.return_value = [
            {
                "name": "_id_",
                "key": {"_id": 1},
                "v": 2,
            }
        ]

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        # Should return empty list (excludes _id index)
        assert indexes == []

    def test_get_indexes_single_field(self):
        """Test getting single field index."""
        mock_collection = MagicMock()
        mock_collection.name = "test_collection"
        mock_collection.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "email_1",
                "key": {"email": 1},
                "v": 2,
                "unique": True,
            },
        ]

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        assert len(indexes) == 1
        assert indexes[0].name == "email_1"
        assert indexes[0].keys == [("email", 1)]
        assert indexes[0].unique is True

    def test_get_indexes_compound(self):
        """Test getting compound index."""
        mock_collection = MagicMock()
        mock_collection.name = "test_collection"
        mock_collection.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "user_created_1",
                "key": {"user_id": 1, "created_at": -1},
                "v": 2,
            },
        ]

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        assert len(indexes) == 1
        assert indexes[0].name == "user_created_1"
        assert len(indexes[0].keys) == 2
        assert indexes[0].keys[0] == ("user_id", 1)
        assert indexes[0].keys[1] == ("created_at", -1)

    def test_get_indexes_with_ttl(self):
        """Test getting TTL index."""
        mock_collection = MagicMock()
        mock_collection.name = "sessions"
        mock_collection.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "expires_at_1",
                "key": {"expires_at": 1},
                "v": 2,
                "expireAfterSeconds": 3600,
            },
        ]

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        assert len(indexes) == 1
        assert indexes[0].name == "expires_at_1"
        assert indexes[0].expire_after_seconds == 3600

    def test_get_indexes_with_partial_filter(self):
        """Test getting partial index."""
        filter_expr = {"status": {"$eq": "active"}}
        mock_collection = MagicMock()
        mock_collection.name = "users"
        mock_collection.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "active_users_1",
                "key": {"email": 1},
                "v": 2,
                "partialFilterExpression": filter_expr,
            },
        ]

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        assert len(indexes) == 1
        assert indexes[0].name == "active_users_1"
        assert indexes[0].partial_filter_expression == filter_expr

    def test_get_indexes_with_collation(self):
        """Test getting index with collation."""
        collation = {"locale": "en", "strength": 2}
        mock_collection = MagicMock()
        mock_collection.name = "users"
        mock_collection.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "name_1",
                "key": {"name": 1},
                "v": 2,
                "collation": collation,
            },
        ]

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        assert len(indexes) == 1
        assert indexes[0].collation == collation

    def test_get_indexes_handles_operation_failure(self):
        """Test that get_indexes handles permission errors gracefully."""
        mock_collection = MagicMock()
        mock_collection.name = "test_collection"
        mock_collection.list_indexes.side_effect = OperationFailure("not authorized")

        manager = IndexManager()
        indexes = manager.get_indexes(mock_collection)

        # Should return empty list on failure
        assert indexes == []

    def test_replicate_indexes_no_indexes(self):
        """Test replicating when source has no indexes."""
        mock_source = MagicMock()
        mock_source.name = "source_coll"
        mock_source.list_indexes.return_value = [{"name": "_id_", "key": {"_id": 1}, "v": 2}]

        mock_dest = MagicMock()

        manager = IndexManager()
        replicated, failed, errors = manager.replicate_indexes(mock_source, mock_dest)

        assert replicated == 0
        assert failed == 0
        assert errors == []
        # Destination create_index should not be called
        mock_dest.create_index.assert_not_called()

    def test_replicate_indexes_success(self):
        """Test successful index replication."""
        mock_source = MagicMock()
        mock_source.name = "source_coll"
        mock_source.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "email_1",
                "key": {"email": 1},
                "v": 2,
                "unique": True,
            },
        ]

        mock_dest = MagicMock()
        mock_dest.create_index.return_value = "email_1"

        manager = IndexManager()
        replicated, failed, errors = manager.replicate_indexes(mock_source, mock_dest)

        assert replicated == 1
        assert failed == 0
        assert errors == []

        # Verify create_index was called correctly
        mock_dest.create_index.assert_called_once()
        call_args = mock_dest.create_index.call_args
        assert call_args[0][0] == [("email", 1)]
        assert call_args[1]["name"] == "email_1"
        assert call_args[1]["unique"] is True

    def test_replicate_indexes_multiple(self):
        """Test replicating multiple indexes."""
        mock_source = MagicMock()
        mock_source.name = "source_coll"
        mock_source.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "email_1",
                "key": {"email": 1},
                "v": 2,
                "unique": True,
            },
            {
                "name": "created_at_1",
                "key": {"created_at": 1},
                "v": 2,
            },
        ]

        mock_dest = MagicMock()
        mock_dest.create_index.return_value = "index_name"

        manager = IndexManager()
        replicated, failed, errors = manager.replicate_indexes(mock_source, mock_dest)

        assert replicated == 2
        assert failed == 0
        assert errors == []
        assert mock_dest.create_index.call_count == 2

    def test_replicate_indexes_with_failure(self):
        """Test index replication with one failure."""
        mock_source = MagicMock()
        mock_source.name = "source_coll"
        mock_source.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}, "v": 2},
            {
                "name": "email_1",
                "key": {"email": 1},
                "v": 2,
                "unique": True,
            },
            {
                "name": "text_idx",
                "key": {"content": "text"},
                "v": 2,
            },
        ]

        mock_dest = MagicMock()
        # First call succeeds, second fails
        mock_dest.create_index.side_effect = [
            "email_1",
            OperationFailure("text index requires specific version"),
        ]

        manager = IndexManager()
        replicated, failed, errors = manager.replicate_indexes(mock_source, mock_dest)

        assert replicated == 1
        assert failed == 1
        assert len(errors) == 1
        assert "text_idx" in errors[0]

    def test_get_index_type_description_unique(self):
        """Test index type description for unique index."""
        manager = IndexManager()
        info = IndexInfo(
            name="email_1",
            keys=[("email", 1)],
            unique=True,
        )

        desc = manager._get_index_type_description(info)
        assert "unique" in desc

    def test_get_index_type_description_compound(self):
        """Test index type description for compound index."""
        manager = IndexManager()
        info = IndexInfo(
            name="user_created",
            keys=[("user_id", 1), ("created_at", -1)],
        )

        desc = manager._get_index_type_description(info)
        assert "compound" in desc

    def test_get_index_type_description_ttl(self):
        """Test index type description for TTL index."""
        manager = IndexManager()
        info = IndexInfo(
            name="expires_1",
            keys=[("expires_at", 1)],
            expire_after_seconds=3600,
        )

        desc = manager._get_index_type_description(info)
        assert "TTL" in desc
        assert "3600" in desc

    def test_get_index_type_description_text(self):
        """Test index type description for text index."""
        manager = IndexManager()
        info = IndexInfo(
            name="content_text",
            keys=[("content", "text")],
        )

        desc = manager._get_index_type_description(info)
        assert "text" in desc

    def test_get_index_type_description_geospatial(self):
        """Test index type description for geospatial index."""
        manager = IndexManager()
        info = IndexInfo(
            name="location_2dsphere",
            keys=[("location", "2dsphere")],
        )

        desc = manager._get_index_type_description(info)
        assert "2dsphere" in desc

    def test_get_index_type_description_hashed(self):
        """Test index type description for hashed index."""
        manager = IndexManager()
        info = IndexInfo(
            name="user_id_hashed",
            keys=[("user_id", "hashed")],
        )

        desc = manager._get_index_type_description(info)
        assert "hashed" in desc

    def test_create_single_index_with_ttl(self):
        """Test creating TTL index."""
        mock_collection = MagicMock()

        info = IndexInfo(
            name="expires_1",
            keys=[("expires_at", 1)],
            expire_after_seconds=3600,
        )

        manager = IndexManager()
        success, error = manager._create_single_index(mock_collection, info)

        assert success is True
        assert error is None

        # Verify create_index was called with expireAfterSeconds
        call_args = mock_collection.create_index.call_args
        assert call_args[1]["expireAfterSeconds"] == 3600

    def test_create_single_index_with_partial_filter(self):
        """Test creating partial index."""
        mock_collection = MagicMock()

        filter_expr = {"status": {"$eq": "active"}}
        info = IndexInfo(
            name="active_1",
            keys=[("user_id", 1)],
            partial_filter_expression=filter_expr,
        )

        manager = IndexManager()
        success, error = manager._create_single_index(mock_collection, info)

        assert success is True
        assert error is None

        # Verify create_index was called with partialFilterExpression
        call_args = mock_collection.create_index.call_args
        assert call_args[1]["partialFilterExpression"] == filter_expr
