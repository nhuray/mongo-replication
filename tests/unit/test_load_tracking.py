"""Tests for load tracking in StateManager."""

import time
from datetime import datetime

import mongomock
import pytest
from bson import ObjectId

from mongo_replication.engine.state import StateManager


@pytest.fixture
def mock_db():
    """Create a mock MongoDB database."""
    client = mongomock.MongoClient()
    return client.get_database("test_db")


@pytest.fixture
def state_manager(mock_db):
    """Create a StateManager with mock database."""
    return StateManager(mock_db)


class TestLoadTracking:
    """Test suite for load tracking functionality."""
    
    def test_create_load_id(self, state_manager):
        """Test that load IDs are timestamp-based."""
        load_id = state_manager.create_load_id()
        
        # Should be a string representation of a float timestamp
        assert isinstance(load_id, str)
        float_value = float(load_id)
        assert float_value > 1700000000  # Some reasonable timestamp
    
    def test_start_load(self, state_manager):
        """Test starting a load creates a record in _rep_loads."""
        load_id = state_manager.create_load_id()
        
        state_manager.start_load(
            load_id=load_id,
            pipeline_name="test_pipeline",
            config_hash="abc123",
            config_path="config/test.yaml",
        )
        
        # Verify load document created
        load_doc = state_manager.loads_collection.find_one({"_id": load_id})
        
        assert load_doc is not None
        assert load_doc["pipeline_name"] == "test_pipeline"
        assert load_doc["status"] == 1  # running
        assert load_doc["config_hash"] == "abc123"
        assert load_doc["config_path"] == "config/test.yaml"
        assert load_doc["engine_version"] == "2.1"
        assert load_doc["collections_processed"] == 0
        assert load_doc["total_documents"] == 0
        assert load_doc["failed_collections"] == []
    
    def test_complete_load(self, state_manager):
        """Test completing a load updates status and statistics."""
        load_id = state_manager.create_load_id()
        
        # Start load
        state_manager.start_load(load_id, "test_pipeline", "hash123")
        
        # Small delay to ensure duration > 0
        time.sleep(0.01)
        
        # Complete load
        state_manager.complete_load(
            load_id=load_id,
            collections_processed=10,
            collections_succeeded=9,
            collections_failed=1,
            total_documents=5000,
            failed_collections=["users"],
            error_summary={"users": "PII redaction failed"},
        )
        
        # Verify load document updated
        load_doc = state_manager.loads_collection.find_one({"_id": load_id})
        
        assert load_doc["status"] == 0  # completed
        assert load_doc["collections_processed"] == 10
        assert load_doc["collections_succeeded"] == 9
        assert load_doc["collections_failed"] == 1
        assert load_doc["total_documents"] == 5000
        assert load_doc["failed_collections"] == ["users"]
        assert load_doc["error_summary"] == {"users": "PII redaction failed"}
        assert load_doc["completed_at"] is not None
        assert load_doc["duration_seconds"] > 0
    
    def test_fail_load(self, state_manager):
        """Test failing a load sets status to 2."""
        load_id = state_manager.create_load_id()
        
        # Start load
        state_manager.start_load(load_id, "test_pipeline", "hash123")
        
        # Fail load
        state_manager.fail_load(load_id, "Connection timeout")
        
        # Verify load marked as failed
        load_doc = state_manager.loads_collection.find_one({"_id": load_id})
        
        assert load_doc["status"] == 2  # failed
        assert load_doc["error_message"] == "Connection timeout"
        assert load_doc["completed_at"] is not None
    
    def test_get_last_successful_load(self, state_manager):
        """Test retrieving the most recent successful load."""
        # Create multiple loads
        load_id_1 = state_manager.create_load_id()
        time.sleep(0.02)  # Longer delay to ensure different timestamps
        load_id_2 = state_manager.create_load_id()
        time.sleep(0.02)
        load_id_3 = state_manager.create_load_id()
        
        # Complete first two, fail third
        state_manager.start_load(load_id_1, "test", "hash1")
        time.sleep(0.01)
        state_manager.complete_load(load_id_1, 5, 5, 0, 1000, [])
        
        time.sleep(0.02)  # Ensure different insertion times
        state_manager.start_load(load_id_2, "test", "hash2")
        time.sleep(0.01)
        state_manager.complete_load(load_id_2, 10, 10, 0, 2000, [])
        
        time.sleep(0.02)
        state_manager.start_load(load_id_3, "test", "hash3")
        state_manager.fail_load(load_id_3, "Error")
        
        # Get last successful load
        last_load = state_manager.get_last_successful_load()
        
        # Should be load_id_2 (most recent successful)
        # Check by document count since load IDs are timestamp-based
        assert last_load["status"] == 0
        assert last_load["total_documents"] == 2000  # This identifies load_id_2
    
    def test_get_running_loads(self, state_manager):
        """Test retrieving currently running loads."""
        load_id_1 = state_manager.create_load_id()
        load_id_2 = state_manager.create_load_id()
        
        # Start two loads
        state_manager.start_load(load_id_1, "test", "hash1")
        state_manager.start_load(load_id_2, "test", "hash2")
        
        # Complete one
        state_manager.complete_load(load_id_1, 5, 5, 0, 1000, [])
        
        # Get running loads
        running = state_manager.get_running_loads()
        
        # Should only have load_id_2
        assert len(running) == 1
        assert running[0]["_id"] == load_id_2
        assert running[0]["status"] == 1


class TestStateManagerWithSourceID:
    """Test suite for state management with source_id support."""
    
    def test_state_id(self, state_manager):
        """Test that state IDs use collection name."""
        state_id = state_manager._get_state_id("users")
        
        assert state_id == "users"
    
    def test_update_state_with_load_id(self, state_manager):
        """Test that state updates include load_id."""
        load_id = "test_load_456"
        cursor_value = datetime(2024, 3, 30, 12, 0, 0)
        
        state_manager.update_state(
            collection_name="orders",
            last_cursor_value=cursor_value,
            cursor_field="meta.updatedAt",
            documents_processed=500,
            write_disposition="merge",
            load_id=load_id,
            config_hash="config123",
        )
        
        # Verify state document
        state = state_manager.get_state("orders")
        
        assert state["_id"] == "orders"
        assert state["collection_name"] == "orders"
        assert state["last_load_id"] == load_id
        assert state["last_cursor_value"] == cursor_value  # Native datetime
        assert state["last_cursor_field"] == "meta.updatedAt"
        assert state["documents_processed_last_run"] == 500
        assert state["total_documents_processed"] == 500
        assert state["config_hash"] == "config123"
    
    def test_reset_state_uses_collection_name(self, state_manager):
        """Test that reset_state deletes the correct state."""
        load_id = "test_load"
        
        # Create state
        state_manager.update_state(
            collection_name="products",
            last_cursor_value=ObjectId(),
            cursor_field="_id",
            documents_processed=100,
            write_disposition="merge",
            load_id=load_id,
        )
        
        # Verify state exists
        assert state_manager.get_state("products") is not None
        
        # Reset state
        state_manager.reset_state("products")
        
        # Verify state deleted
        assert state_manager.get_state("products") is None
