"""State management for MongoDB replication with native BSON type preservation.

This module manages replication state in dedicated MongoDB collections in the
destination database:
- _rep_state: Per-collection replication state (one doc per collection-run)
- _rep_runs: Job run history (one doc per job run)

All cursor values are stored as native BSON types (datetime, ObjectId, Decimal128, etc.)
to preserve type fidelity across replication runs.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId
from pymongo.database import Database

logger = logging.getLogger(__name__)


class StateManager:
    """Manages replication state and run tracking for MongoDB replication.

    State is stored in two collections (configurable via defaults):
    1. _rep_runs: Job run history (one doc per complete replication job)
    2. _rep_state: Per-collection replication state (one doc per collection-run)

    All BSON types (ObjectId, datetime, Decimal128) are preserved natively.

    Schema for _rep_runs:
        _id: ObjectId                           # Run ID
        status: String                          # "running", "completed", or "failed"
        startedAt: Date                         # When job started
        endedAt: Date                           # When job ended
        durationSeconds: Double                 # Total duration
        collections.processed: Int32            # Total collections processed
        collections.succeeded: Int32            # Collections succeeded
        collections.failed: Int32               # Collections failed
        documents.processed: Int32              # Total documents processed
        documents.succeeded: Int32              # Documents succeeded
        documents.failed: Int32                 # Documents failed
        errors.summary: Object                  # Error summary by collection
        errors.collections: Array               # List of failed collection names

    Schema for _rep_state:
        _id: ObjectId                           # State ID
        runId: ObjectId                         # Reference to _rep_runs._id
        collection: String                      # Collection name
        status: String                          # "running", "completed", "failed", or "skipped"
        startedAt: Date                         # When collection replication started
        endedAt: Date                           # When collection replication ended
        durationSeconds: Double                 # Duration for this collection
        documents.processed: Int32              # Documents processed
        documents.succeeded: Int32              # Documents succeeded
        documents.failed: Int32                 # Documents failed
        lastCursorValue: Any                    # Last cursor value (native BSON type)
        lastCursorField: String                 # Cursor field name
        error: Object                           # Error details if failed
    """

    def __init__(
        self,
        destination_db: Database,
        runs_collection: str = "_rep_runs",
        state_collection: str = "_rep_state",
    ):
        """Initialize the state manager.

        Args:
            destination_db: PyMongo Database instance for the destination
            runs_collection: Name of the runs tracking collection
            state_collection: Name of the state tracking collection
        """
        self.db = destination_db
        self.runs_collection_name = runs_collection
        self.state_collection_name = state_collection
        self.runs_collection = self.db[runs_collection]
        self.state_collection = self.db[state_collection]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """Ensure required indexes exist on state and runs collections."""
        try:
            # Drop old indexes if they exist (from previous schema)
            try:
                self.state_collection.drop_index("collection_name_unique")
            except Exception as e:
                logger.warning(f"Failed to drop old collection_name_unique index: {e}")
                pass  # Index doesn't exist, that's fine

            # Runs collection indexes
            self.runs_collection.create_index("status", name="status_idx")
            self.runs_collection.create_index("startedAt", name="startedAt_idx")

            # State collection indexes
            self.state_collection.create_index("runId", name="runId_idx")
            self.state_collection.create_index("collection", name="collection_idx")
            self.state_collection.create_index("status", name="status_idx")

            # Compound index for incremental loading lookups
            self.state_collection.create_index(
                [("collection", 1), ("endedAt", -1)], name="collection_endedAt_idx"
            )

        except Exception as e:
            # Silently ignore index creation errors (may not have permissions)
            # Indexes are a nice-to-have for performance, not required
            logger.debug(f"Index creation skipped: {e}")

    # ============================================================
    # Run Tracking Methods
    # ============================================================

    def create_run(self) -> ObjectId:
        """Create a new replication run and return its ID.

        Returns:
            ObjectId for the new run
        """
        run_id = ObjectId()
        self.runs_collection.insert_one(
            {
                "_id": run_id,
                "status": "running",
                "startedAt": datetime.utcnow(),
                "endedAt": None,
                "durationSeconds": None,
                "collections": {
                    "processed": 0,
                    "succeeded": 0,
                    "failed": 0,
                },
                "documents": {
                    "processed": 0,
                    "succeeded": 0,
                    "failed": 0,
                },
                "errors": {
                    "summary": {},
                    "collections": [],
                },
            }
        )
        logger.debug(f"Created run {run_id}")
        return run_id

    def complete_run(
        self,
        run_id: ObjectId,
        collections_processed: int,
        collections_succeeded: int,
        collections_failed: int,
        documents_processed: int,
        documents_succeeded: int,
        documents_failed: int,
        error_summary: Optional[Dict[str, str]] = None,
        failed_collections: Optional[List[str]] = None,
    ) -> None:
        """Mark a run as completed successfully.

        Args:
            run_id: Run identifier
            collections_processed: Total collections processed
            collections_succeeded: Number of successful collections
            collections_failed: Number of failed collections
            documents_processed: Total documents processed
            documents_succeeded: Documents successfully processed
            documents_failed: Documents that failed
            error_summary: Optional dict mapping collection name to error message
            failed_collections: Optional list of collection names that failed
        """
        started = self.runs_collection.find_one({"_id": run_id})
        if not started:
            logger.warning(f"Run {run_id} not found in runs collection")
            return

        end_time = datetime.utcnow()
        duration = (end_time - started["startedAt"]).total_seconds()

        self.runs_collection.update_one(
            {"_id": run_id},
            {
                "$set": {
                    "status": "completed",
                    "endedAt": end_time,
                    "durationSeconds": duration,
                    "collections": {
                        "processed": collections_processed,
                        "succeeded": collections_succeeded,
                        "failed": collections_failed,
                    },
                    "documents": {
                        "processed": documents_processed,
                        "succeeded": documents_succeeded,
                        "failed": documents_failed,
                    },
                    "errors": {
                        "summary": error_summary or {},
                        "collections": failed_collections or [],
                    },
                }
            },
        )
        logger.info(
            f"Completed run {run_id}: "
            f"{collections_succeeded}/{collections_processed} collections succeeded, "
            f"{documents_processed:,} documents"
        )

    def fail_run(
        self,
        run_id: ObjectId,
        error_message: str,
    ) -> None:
        """Mark a run as failed.

        Args:
            run_id: Run identifier
            error_message: Error description
        """
        started = self.runs_collection.find_one({"_id": run_id})
        if started:
            end_time = datetime.utcnow()
            duration = (end_time - started["startedAt"]).total_seconds()
        else:
            end_time = datetime.utcnow()
            duration = 0

        self.runs_collection.update_one(
            {"_id": run_id},
            {
                "$set": {
                    "status": "failed",
                    "endedAt": end_time,
                    "durationSeconds": duration,
                    "errors": {
                        "summary": {"_global": error_message},
                        "collections": [],
                    },
                }
            },
            upsert=True,
        )
        logger.error(f"Failed run {run_id}: {error_message}")

    def get_last_successful_run(self) -> Optional[Dict[str, Any]]:
        """Get the most recent successful run.

        Returns:
            Run document if found, None otherwise
        """
        return self.runs_collection.find_one({"status": "completed"}, sort=[("startedAt", -1)])

    def get_running_runs(self) -> List[Dict[str, Any]]:
        """Get list of currently running runs.

        Useful for detecting stuck runs.

        Returns:
            List of run documents with status="running"
        """
        return list(self.runs_collection.find({"status": "running"}))

    # ============================================================
    # Collection State Methods
    # ============================================================

    def start_collection(
        self,
        run_id: ObjectId,
        collection_name: str,
    ) -> ObjectId:
        """Mark a collection replication as started.

        Args:
            run_id: ID of the parent run
            collection_name: Name of the collection

        Returns:
            ObjectId for the collection state document
        """
        state_id = ObjectId()
        self.state_collection.insert_one(
            {
                "_id": state_id,
                "runId": run_id,
                "collection": collection_name,
                "status": "running",
                "startedAt": datetime.utcnow(),
                "endedAt": None,
                "durationSeconds": None,
                "documents": {
                    "processed": 0,
                    "succeeded": 0,
                    "failed": 0,
                },
                "lastCursorValue": None,
                "lastCursorField": None,
                "error": None,
            }
        )
        return state_id

    def update_collection_state(
        self,
        state_id: ObjectId,
        last_cursor_value: Any,
        cursor_field: str,
        documents_processed: int,
        documents_succeeded: int,
        documents_failed: int,
    ) -> None:
        """Update collection state after processing a batch.

        This is called after each successful batch to ensure safe resume.

        Args:
            state_id: ID of the state document
            last_cursor_value: Latest cursor value (native BSON type preserved!)
            cursor_field: Field name used as cursor
            documents_processed: Total docs processed so far
            documents_succeeded: Total docs succeeded so far
            documents_failed: Total docs failed so far
        """
        self.state_collection.update_one(
            {"_id": state_id},
            {
                "$set": {
                    "lastCursorValue": last_cursor_value,  # Native BSON type!
                    "lastCursorField": cursor_field,
                    "documents.processed": documents_processed,
                    "documents.succeeded": documents_succeeded,
                    "documents.failed": documents_failed,
                }
            },
        )

    def complete_collection(
        self,
        state_id: ObjectId,
        documents_processed: int,
        documents_succeeded: int,
        documents_failed: int,
    ) -> None:
        """Mark a collection replication as completed.

        Args:
            state_id: ID of the state document
            documents_processed: Total documents processed
            documents_succeeded: Documents successfully processed
            documents_failed: Documents that failed
        """
        state = self.state_collection.find_one({"_id": state_id})
        if not state:
            logger.warning(f"State {state_id} not found")
            return

        end_time = datetime.utcnow()
        duration = (end_time - state["startedAt"]).total_seconds()

        self.state_collection.update_one(
            {"_id": state_id},
            {
                "$set": {
                    "status": "completed",
                    "endedAt": end_time,
                    "durationSeconds": duration,
                    "documents.processed": documents_processed,
                    "documents.succeeded": documents_succeeded,
                    "documents.failed": documents_failed,
                }
            },
        )

    def fail_collection(
        self,
        state_id: ObjectId,
        error_message: str,
        documents_processed: int = 0,
        documents_succeeded: int = 0,
        documents_failed: int = 0,
    ) -> None:
        """Mark a collection replication as failed.

        Args:
            state_id: ID of the state document
            error_message: Error description
            documents_processed: Total documents processed before failure
            documents_succeeded: Documents successfully processed before failure
            documents_failed: Documents that failed
        """
        state = self.state_collection.find_one({"_id": state_id})
        if state:
            end_time = datetime.utcnow()
            duration = (end_time - state["startedAt"]).total_seconds()
        else:
            end_time = datetime.utcnow()
            duration = 0

        self.state_collection.update_one(
            {"_id": state_id},
            {
                "$set": {
                    "status": "failed",
                    "endedAt": end_time,
                    "durationSeconds": duration,
                    "documents.processed": documents_processed,
                    "documents.succeeded": documents_succeeded,
                    "documents.failed": documents_failed,
                    "error": {
                        "message": error_message,
                        "timestamp": end_time,
                    },
                }
            },
            upsert=True,
        )

    def skip_collection(
        self,
        run_id: ObjectId,
        collection_name: str,
        reason: str,
    ) -> None:
        """Mark a collection as skipped.

        Args:
            run_id: ID of the parent run
            collection_name: Name of the collection
            reason: Reason for skipping
        """
        now = datetime.utcnow()
        self.state_collection.insert_one(
            {
                "_id": ObjectId(),
                "runId": run_id,
                "collection": collection_name,
                "status": "skipped",
                "startedAt": now,
                "endedAt": now,
                "durationSeconds": 0,
                "documents": {
                    "processed": 0,
                    "succeeded": 0,
                    "failed": 0,
                },
                "lastCursorValue": None,
                "lastCursorField": None,
                "error": {
                    "message": reason,
                    "timestamp": now,
                },
            }
        )

    def get_last_cursor_value(self, collection_name: str) -> Optional[Any]:
        """Get the last cursor value for incremental loading.

        Looks up the most recent completed state for this collection.

        Args:
            collection_name: Name of the collection

        Returns:
            Last cursor value (as native BSON type: datetime, ObjectId, etc.)
            or None if no previous state exists
        """
        state = self.state_collection.find_one(
            {
                "collection": collection_name,
                "status": "completed",
                "lastCursorValue": {"$ne": None},
            },
            sort=[("endedAt", -1)],
        )
        if state:
            return state.get("lastCursorValue")
        return None

    def get_running_collections(self, run_id: ObjectId) -> List[str]:
        """Get list of collections currently marked as running for a specific run.

        Useful for detecting stuck replications.

        Args:
            run_id: ID of the run

        Returns:
            List of collection names
        """
        cursor = self.state_collection.find(
            {"runId": run_id, "status": "running"}, {"collection": 1}
        )
        return [doc["collection"] for doc in cursor]

    def get_failed_collections(self, run_id: ObjectId) -> List[Dict[str, Any]]:
        """Get list of collections that failed in a specific run.

        Args:
            run_id: ID of the run

        Returns:
            List of state documents for failed collections
        """
        return list(self.state_collection.find({"runId": run_id, "status": "failed"}))

    def reset_collection_state(self, collection_name: str) -> None:
        """Reset all state for a collection (for manual recovery).

        Deletes all state documents for this collection across all runs.

        Args:
            collection_name: Name of the collection
        """
        result = self.state_collection.delete_many({"collection": collection_name})
        logger.info(f"Reset state for {collection_name} ({result.deleted_count} documents deleted)")
