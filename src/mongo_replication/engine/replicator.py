"""Collection replicator for MongoDB with BSON type preservation.

This module handles replication of a single collection from source to destination
with support for three write strategies (merge, append, replace), PII redaction
(manual and automatic), and batch processing with progress logging.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from pymongo import ReplaceOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from mongo_replication.engine.indexes import IndexManager
from mongo_replication.engine.state import StateManager
from mongo_replication.engine.transformations import (
    TransformationEngine,
    TransformOperationResults,
    TransformResults,
)
from mongo_replication.engine.validation import CursorValidator

logger = logging.getLogger(__name__)


def _summarize_bulk_write_error(error: BulkWriteError, collection_name: str) -> str:
    """Create a concise error summary without including full document data.

    Args:
        error: The BulkWriteError from pymongo
        collection_name: Name of the collection

    Returns:
        Summarized error message without document contents
    """
    import re

    details = error.details
    write_errors = details.get("writeErrors", [])
    write_concern_errors = details.get("writeConcernErrors", [])

    n_inserted = details.get("nInserted", 0)
    n_upserted = details.get("nUpserted", 0)
    n_modified = details.get("nModified", 0)

    error_summary = []

    # Regex to remove document data from error messages
    # Matches patterns like: dup key: { ... }
    doc_pattern = re.compile(r"dup key: \{[^}]*\}")

    # Summarize write errors
    if write_errors:
        # Group errors by code
        error_codes = {}
        for err in write_errors:
            code = err.get("code", "unknown")
            if code not in error_codes:
                errmsg = err.get("errmsg", "No message")
                # Remove document data from error message
                errmsg = doc_pattern.sub("dup key: { ... }", errmsg)
                # Get first line only and truncate
                errmsg = errmsg.split("\n")[0][:200]
                error_codes[code] = {"count": 0, "sample_msg": errmsg}
            error_codes[code]["count"] += 1

        for code, info in error_codes.items():
            count = info["count"]
            sample = info["sample_msg"]
            error_summary.append(f"{count} error(s) with code {code}: {sample}")

    # Summarize write concern errors
    if write_concern_errors:
        for err in write_concern_errors:
            msg = err.get("errmsg", "Unknown write concern error")[:200]
            error_summary.append(f"Write concern error: {msg}")

    # Build final message
    parts = [
        f"Bulk write failed for {collection_name}:",
        f"  Inserted: {n_inserted}, Upserted: {n_upserted}, Modified: {n_modified}",
    ]

    if error_summary:
        parts.append("  Errors:")
        for summary in error_summary:
            parts.append(f"    - {summary}")

    return "\n".join(parts)


class ReplicationResult(BaseModel):
    """Result of replicating a single collection."""

    collection_name: str
    status: str  # "completed", "failed", "skipped"
    documents_processed: int
    batches_processed: int
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    cursor_field_used: Optional[str] = None
    write_disposition: Optional[str] = None

    # Transformation statistics
    documents_transformed: int = 0
    transforms_applied: int = 0

    # Per-operation transform results
    # e.g. {"add_field": TransformOperationResults(...), "anonymize": TransformOperationResults(...)}
    transform_operations: Dict[str, TransformOperationResults] = Field(default_factory=dict)

    # Index replication statistics
    indexes_replicated: int = 0
    indexes_failed: int = 0
    index_errors: List[str] = Field(default_factory=list)


class ReplicationError(Exception):
    """Custom exception for replication errors."""

    pass


class CollectionReplicator:
    """Replicates a single MongoDB collection with BSON type preservation.

    Supports three write strategies:
    - merge: Upsert documents by primary key (incremental)
    - append: Insert only new documents (fail on duplicates)
    - replace: Drop collection and reload all data (full refresh)
    """

    def __init__(
        self,
        source_collection: Collection,
        dest_collection: Collection,
        state_manager: StateManager,
        cursor_validator: CursorValidator,
        index_manager: IndexManager,
    ):
        """Initialize the collection replicator.

        Args:
            source_collection: PyMongo Collection instance for source
            dest_collection: PyMongo Collection instance for destination
            state_manager: State manager for tracking replication progress
            cursor_validator: Validator for cursor fields
            index_manager: Manager for index replication
        """
        self.source = source_collection
        self.dest = dest_collection
        self.state_mgr = state_manager
        self.validator = cursor_validator
        self.index_mgr = index_manager
        self.collection_name = source_collection.name

    @staticmethod
    def _convert_operations_to_dict(
        operations: Dict[str, TransformOperationResults],
    ) -> Dict[str, Dict[str, Any]]:
        """Convert TransformOperationResults to dict for storage.

        Args:
            operations: Dictionary of operation results

        Returns:
            Dictionary suitable for MongoDB storage
        """
        return {
            op_type: {
                "type": op_result.type,
                "fieldsConfigured": op_result.fields_configured,
                "fieldsProcessed": op_result.fields_processed,
                "durationSeconds": op_result.duration_seconds,
            }
            for op_type, op_result in operations.items()
        }

    def replicate(
        self,
        state_id,
        cursor_field: Optional[str],
        write_disposition: str,
        primary_key: str = "_id",
        transformation_engine: Optional[TransformationEngine] = None,
        batch_size: int = 1000,
        match_filter: Optional[Dict[str, Any]] = None,
        cursor_initial_value: Optional[datetime] = None,
    ) -> ReplicationResult:
        """Replicate the collection from source to destination.

        All BSON types (ObjectId, datetime, Decimal128) are preserved throughout.

        Processing pipeline:
        1. Fetch from MongoDB (with match filter applied at query level)
        2. Apply transformations (unified pipeline with all transform types)
        3. Write to destination

        Args:
            state_id: ObjectId of the collection state document
            cursor_field: Field to use for incremental loading (None for replace mode)
            write_disposition: Strategy - "merge", "append", or "replace"
            primary_key: Primary key field for merge operations (default: _id)
            transformation_engine: TransformationEngine instance for document transformations (optional)
            batch_size: Number of documents per batch (default: 1000)
            match_filter: MongoDB match filter to apply at query time (optional)
            cursor_initial_value: Initial cursor value for first-time replication (datetime object, optional)

        Returns:
            ReplicationResult with statistics and status

        Raises:
            ReplicationError: On transformation failure, duplicate keys, or data conflicts
        """
        start_time = time.time()
        match_filter = match_filter or {}

        # Store transformation engine and match_filter for use in processing pipeline
        self._match_filter = match_filter
        self._transformation_engine = transformation_engine
        self._state_id = state_id
        self._cursor_initial_value = cursor_initial_value

        logger.info(f"🔄 Starting replication for collection: {self.collection_name}")
        logger.info(f"   State ID: {state_id}")
        logger.info(f"   Write disposition: {write_disposition}")
        logger.info(f"   Batch size: {batch_size}")
        if transformation_engine and transformation_engine.transforms:
            logger.info(f"   Transforms configured: {len(transformation_engine.transforms)}")
        if match_filter:
            logger.info(f"   Match filter applied: {match_filter}")

        try:
            # Validate cursor field and get actual field to use
            actual_cursor_field = self.validator.validate_cursor_field(
                collection=self.source,
                collection_name=self.collection_name,
                cursor_field=cursor_field,
                write_disposition=write_disposition,
            )

            # Replicate indexes BEFORE data replication (except for replace mode)
            # In replace mode, indexes are created after drop
            indexes_replicated = 0
            indexes_failed = 0
            index_errors = []

            if write_disposition != "replace":
                indexes_replicated, indexes_failed, index_errors = self.index_mgr.replicate_indexes(
                    self.source, self.dest
                )

            # Execute appropriate replication strategy
            if write_disposition == "replace":
                result = self._replicate_replace(
                    batch_size=batch_size,
                )
            elif write_disposition == "append":
                result = self._replicate_append(
                    cursor_field=actual_cursor_field,
                    batch_size=batch_size,
                )
            elif write_disposition == "merge":
                result = self._replicate_merge(
                    cursor_field=actual_cursor_field,
                    primary_key=primary_key,
                    batch_size=batch_size,
                )
            else:
                raise ValueError(f"Invalid write_disposition: {write_disposition}")

            # Mark as completed
            duration = time.time() - start_time
            self.state_mgr.complete_collection(
                state_id=state_id,
                documents_processed=result.documents_processed,
                documents_succeeded=result.documents_processed,  # TODO: Track failed docs
                documents_failed=0,
                documents_transformed=result.documents_transformed,
                transforms_applied=result.transforms_applied,
                transform_operations=self._convert_operations_to_dict(result.transform_operations),
            )

            logger.info(
                f"✅ {self.collection_name}: Completed - {result.documents_processed} docs in {duration:.1f}s"
            )

            # For replace mode, indexes come from result; for others, from variables
            final_indexes_replicated = (
                result.indexes_replicated if write_disposition == "replace" else indexes_replicated
            )
            final_indexes_failed = (
                result.indexes_failed if write_disposition == "replace" else indexes_failed
            )
            final_index_errors = (
                result.index_errors if write_disposition == "replace" else index_errors
            )

            return ReplicationResult(
                collection_name=self.collection_name,
                status="completed",
                documents_processed=result.documents_processed,
                batches_processed=result.batches_processed,
                duration_seconds=duration,
                cursor_field_used=actual_cursor_field,
                write_disposition=write_disposition,
                documents_transformed=result.documents_transformed,
                transforms_applied=result.transforms_applied,
                transform_operations=result.transform_operations,
                indexes_replicated=final_indexes_replicated,
                indexes_failed=final_indexes_failed,
                index_errors=final_index_errors,
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"❌ {self.collection_name}: Failed - {error_msg}")

            # Mark as failed in state
            self.state_mgr.fail_collection(
                state_id=state_id,
                error_message=error_msg,
                documents_processed=0,  # TODO: Track partial progress
                documents_succeeded=0,
                documents_failed=0,
            )

            return ReplicationResult(
                collection_name=self.collection_name,
                status="failed",
                documents_processed=0,
                batches_processed=0,
                duration_seconds=duration,
                error_message=error_msg,
                write_disposition=write_disposition,
            )

    def _build_query(self, cursor_field: str) -> Dict[str, Any]:
        """Build query for incremental loading based on last cursor value and match filter.

        Combines incremental cursor filter with user-defined match filter using $and.
        For documents where cursor_field doesn't exist, includes them with $or to avoid data loss.

        Args:
            cursor_field: Field to use as cursor

        Returns:
            Query dict for MongoDB find() (combines cursor + match filter)
        """
        filters = []

        # Add cursor filter for incremental loading
        if cursor_field:
            last_value = self.state_mgr.get_last_cursor_value(self.collection_name)

            # If no previous state exists, use cursor_initial_value
            if (
                last_value is None
                and hasattr(self, "_cursor_initial_value")
                and self._cursor_initial_value is not None
            ):
                last_value = self._cursor_initial_value
                logger.info(
                    f"   Using cursor_initial_value for first-time replication: {last_value}"
                )

            if last_value is not None:
                # Include docs where cursor field > last_value OR cursor field doesn't exist
                # This ensures we don't lose documents missing the cursor field
                filters.append(
                    {
                        "$or": [
                            {cursor_field: {"$exists": False}},
                            {cursor_field: {"$gt": last_value}},
                        ]
                    }
                )

        # Add user-defined match filter
        match_filter = getattr(self, "_match_filter", None)
        if match_filter:
            filters.append(match_filter)

        # Combine filters with $and if multiple exist
        if len(filters) == 0:
            return {}
        elif len(filters) == 1:
            return filters[0]
        else:
            return {"$and": filters}

    def _fetch_batch(
        self,
        cursor_field: str,
        batch_size: int,
        skip: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch a batch of documents from source.

        All BSON types are preserved in the returned documents.

        Args:
            cursor_field: Field to sort by (empty string for no sorting)
            batch_size: Number of documents to fetch
            skip: Number of documents to skip

        Returns:
            List of documents with native BSON types
        """
        query = self._build_query(cursor_field)

        # Sort by cursor field for consistent ordering (if specified)
        if cursor_field:
            cursor = self.source.find(query).sort(cursor_field, 1).skip(skip).limit(batch_size)
        else:
            cursor = self.source.find(query).skip(skip).limit(batch_size)

        return list(cursor)

    def _apply_transformations(
        self,
        documents: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], TransformResults]:
        """Apply transformation pipeline to documents.

        Args:
            documents: List of documents to process

        Returns:
            Tuple of (processed documents, TransformResults)

        Raises:
            ReplicationError: If transformation fails
        """
        if not self._transformation_engine:
            return documents, TransformResults()

        try:
            processed_docs, results = self._transformation_engine.transform_documents(documents)
            return processed_docs, results
        except Exception as e:
            raise ReplicationError(f"Transformation failed for {self.collection_name}: {e}") from e

    def _aggregate_operation_results(
        self,
        accumulated: Dict[str, TransformOperationResults],
        new_results: Dict[str, TransformOperationResults],
    ) -> None:
        """Aggregate operation results from a batch into accumulated totals.

        Args:
            accumulated: Accumulated operation results across batches (modified in place)
            new_results: New operation results from current batch
        """
        for op_type, op_result in new_results.items():
            if op_type not in accumulated:
                # Create new entry with same type
                accumulated[op_type] = TransformOperationResults(
                    type=op_type,
                    fields_configured=op_result.fields_configured,
                    fields_processed=op_result.fields_processed,
                    duration_seconds=op_result.duration_seconds,
                )
            else:
                # Aggregate into existing entry
                accumulated[op_type].fields_processed += op_result.fields_processed
                accumulated[op_type].duration_seconds += op_result.duration_seconds
                # fields_configured should be the same across batches, so we can just keep it

    def _write_batch_merge(
        self,
        documents: List[Dict[str, Any]],
        primary_key: str,
    ) -> int:
        """Write batch using merge strategy (upsert by primary key).

        Args:
            documents: Documents to write (with native BSON types)
            primary_key: Field to use as primary key for upsert

        Returns:
            Number of documents written
        """
        if not documents:
            return 0

        # Build bulk upsert operations
        operations = [
            ReplaceOne(
                filter={primary_key: doc[primary_key]},
                replacement=doc,  # ✅ Native BSON types preserved!
                upsert=True,
            )
            for doc in documents
        ]

        try:
            self.dest.bulk_write(operations, ordered=False)
            # Return number of documents processed (batch size)
            # Note: modified_count can be 0 if data is identical, so we count operations
            return len(documents)
        except BulkWriteError as e:
            # Fail fast on write errors (per requirements)
            error_msg = _summarize_bulk_write_error(e, self.collection_name)
            raise ReplicationError(error_msg) from e

    def _write_batch_append(
        self,
        documents: List[Dict[str, Any]],
    ) -> int:
        """Write batch using append strategy (insert only).

        Args:
            documents: Documents to insert (with native BSON types)

        Returns:
            Number of documents written

        Raises:
            ReplicationError: On duplicate key errors (fail fast per requirements)
        """
        if not documents:
            return 0

        try:
            result = self.dest.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
        except BulkWriteError as e:
            # Check for duplicate key errors
            details = e.details
            write_errors = details.get("writeErrors", [])

            if any(err["code"] == 11000 for err in write_errors):
                # Count duplicate key errors
                dup_count = sum(1 for err in write_errors if err["code"] == 11000)
                raise ReplicationError(
                    f"Duplicate key error in append mode for {self.collection_name}: "
                    f"{dup_count} duplicate(s) found. "
                    f"Append mode requires unique documents."
                ) from e

            # Other bulk write error
            error_msg = _summarize_bulk_write_error(e, self.collection_name)
            raise ReplicationError(error_msg) from e

    def _write_batch_replace(
        self,
        documents: List[Dict[str, Any]],
        is_first_batch: bool,
    ) -> tuple[int, int, int, List[str]]:
        """Write batch using replace strategy (drop + insert on first batch).

        Args:
            documents: Documents to insert (with native BSON types)
            is_first_batch: If True, drop collection and recreate indexes first

        Returns:
            Tuple of (docs_written, indexes_replicated, indexes_failed, index_errors)
        """
        if not documents:
            return 0, 0, 0, []

        indexes_replicated = 0
        indexes_failed = 0
        index_errors = []

        # Drop collection and recreate indexes on first batch only
        if is_first_batch:
            logger.info(f"   Dropping collection {self.collection_name} (replace mode)")
            self.dest.drop()

            # Recreate indexes after drop
            indexes_replicated, indexes_failed, index_errors = self.index_mgr.replicate_indexes(
                self.source, self.dest
            )

        result = self.dest.insert_many(documents, ordered=False)
        return len(result.inserted_ids), indexes_replicated, indexes_failed, index_errors

    def _replicate_replace(
        self,
        batch_size: int,
    ) -> ReplicationResult:
        """Execute replace strategy (full refresh).

        Args:
            batch_size: Documents per batch

        Returns:
            Intermediate result with stats
        """
        total_docs = 0
        batch_num = 0
        is_first_batch = True

        # Initialize aggregate statistics
        total_documents_transformed = 0
        total_transforms_applied = 0

        # Aggregate operation results across batches
        aggregated_operations: Dict[str, TransformOperationResults] = {}

        # Index statistics (captured from first batch)
        indexes_replicated = 0
        indexes_failed = 0
        index_errors = []

        while True:
            # Fetch batch (no cursor filtering in replace mode)
            batch = self._fetch_batch(
                cursor_field="",
                batch_size=batch_size,
                skip=total_docs,
            )

            if not batch:
                break

            batch_num += 1
            batch_start = time.time()

            # Apply transformation pipeline
            processed, results = self._apply_transformations(batch)

            # Aggregate statistics
            total_documents_transformed += results.documents_processed
            total_transforms_applied += results.transforms_applied
            self._aggregate_operation_results(aggregated_operations, results.operations)

            # Write batch (and handle indexes on first batch)
            written, idx_rep, idx_fail, idx_errs = self._write_batch_replace(
                processed, is_first_batch
            )

            # Capture index stats from first batch
            if is_first_batch:
                indexes_replicated = idx_rep
                indexes_failed = idx_fail
                index_errors = idx_errs
                is_first_batch = False

            total_docs += written
            batch_duration = time.time() - batch_start

            logger.info(
                f"📦 {self.collection_name}: Batch {batch_num} - "
                f"{total_docs} docs - {batch_duration:.1f}s"
            )

        return ReplicationResult(
            collection_name=self.collection_name,
            status="completed",
            documents_processed=total_docs,
            batches_processed=batch_num,
            documents_transformed=total_documents_transformed,
            transforms_applied=total_transforms_applied,
            transform_operations=aggregated_operations,
            indexes_replicated=indexes_replicated,
            indexes_failed=indexes_failed,
            index_errors=index_errors,
        )

    def _replicate_append(
        self,
        cursor_field: str,
        batch_size: int,
    ) -> ReplicationResult:
        """Execute append strategy (insert new documents only).

        Args:
            cursor_field: Field to use for incremental loading
            batch_size: Documents per batch

        Returns:
            Intermediate result with stats
        """
        total_docs = 0
        batch_num = 0
        last_cursor_value = None

        # Initialize aggregate statistics
        total_documents_transformed = 0
        total_transforms_applied = 0

        # Aggregate operation results across batches
        aggregated_operations: Dict[str, TransformOperationResults] = {}

        # Get starting cursor value from state
        # After this initial query, we track cursor locally for this run
        query = self._build_query(cursor_field)

        while True:
            # Fetch batch using current query (doesn't re-read state)
            if cursor_field:
                cursor = self.source.find(query).sort(cursor_field, 1).limit(batch_size)
            else:
                cursor = self.source.find(query).limit(batch_size)

            batch = list(cursor)

            if not batch:
                break

            batch_num += 1
            batch_start = time.time()

            # Apply transformation pipeline
            processed, results = self._apply_transformations(batch)

            # Aggregate statistics
            total_documents_transformed += results.documents_processed
            total_transforms_applied += results.transforms_applied
            self._aggregate_operation_results(aggregated_operations, results.operations)

            # Write batch
            written = self._write_batch_append(processed)

            # Track last cursor value for this run
            if cursor_field:
                last_cursor_value = self.validator.get_field_value(batch[-1], cursor_field)
                # Update query for next batch to continue from where we left off
                # Only query for docs with cursor field > last value (not docs without cursor field)
                query = {cursor_field: {"$gt": last_cursor_value}}

            total_docs += written
            batch_duration = time.time() - batch_start

            logger.info(
                f"📦 {self.collection_name}: Batch {batch_num} - "
                f"{total_docs} docs - {batch_duration:.1f}s"
            )

        # Update state once at the end with final cursor value
        # Always update state with document counts, cursor tracking is optional
        if cursor_field and last_cursor_value is not None:
            self.state_mgr.update_collection_state(
                state_id=self._state_id,
                last_cursor_value=last_cursor_value,  # ✅ Native BSON type!
                cursor_field=cursor_field,
                documents_processed=total_docs,
                documents_succeeded=total_docs,  # TODO: Track separately
                documents_failed=0,
            )
        elif total_docs > 0:
            # No cursor field, but we still need to update document counts
            self.state_mgr.update_collection_state(
                state_id=self._state_id,
                last_cursor_value=None,
                cursor_field="",
                documents_processed=total_docs,
                documents_succeeded=total_docs,
                documents_failed=0,
            )

        return ReplicationResult(
            collection_name=self.collection_name,
            status="completed",
            documents_processed=total_docs,
            batches_processed=batch_num,
            duration_seconds=0,  # Set by caller
            documents_transformed=total_documents_transformed,
            transforms_applied=total_transforms_applied,
            transform_operations=aggregated_operations,
        )

    def _replicate_merge(
        self,
        cursor_field: str,
        primary_key: str,
        batch_size: int,
    ) -> ReplicationResult:
        """Execute merge strategy (upsert by primary key).

        Args:
            cursor_field: Field to use for incremental loading
            primary_key: Primary key field for upsert operations
            batch_size: Documents per batch

        Returns:
            Intermediate result with stats
        """
        total_docs = 0
        batch_num = 0
        last_cursor_value = None

        # Initialize aggregate statistics
        total_documents_transformed = 0
        total_transforms_applied = 0

        # Aggregate operation results across batches
        aggregated_operations: Dict[str, TransformOperationResults] = {}

        # Get starting cursor value from state
        query = self._build_query(cursor_field)

        while True:
            # Fetch batch using current query
            if cursor_field:
                cursor = self.source.find(query).sort(cursor_field, 1).limit(batch_size)
            else:
                cursor = self.source.find(query).limit(batch_size)

            batch = list(cursor)

            if not batch:
                break

            batch_num += 1
            batch_start = time.time()

            # Apply transformation pipeline
            processed, results = self._apply_transformations(batch)

            # Aggregate statistics
            total_documents_transformed += results.documents_processed
            total_transforms_applied += results.transforms_applied
            self._aggregate_operation_results(aggregated_operations, results.operations)

            # Write batch
            written = self._write_batch_merge(processed, primary_key)

            # Track last cursor value for this run
            if cursor_field:
                last_cursor_value = self.validator.get_field_value(batch[-1], cursor_field)
                # Update query for next batch to continue from where we left off
                # Only query for docs with cursor field > last value (not docs without cursor field)
                query = {cursor_field: {"$gt": last_cursor_value}}

            total_docs += written
            batch_duration = time.time() - batch_start

            logger.info(
                f"📦 {self.collection_name}: Batch {batch_num} - "
                f"{total_docs} docs - {batch_duration:.1f}s"
            )

        # Update state once at the end with final cursor value
        # Always update state with document counts, cursor tracking is optional
        if cursor_field and last_cursor_value is not None:
            self.state_mgr.update_collection_state(
                state_id=self._state_id,
                last_cursor_value=last_cursor_value,
                cursor_field=cursor_field,
                documents_processed=total_docs,
                documents_succeeded=total_docs,  # TODO: Track separately
                documents_failed=0,
            )
        elif total_docs > 0:
            # No cursor field, but we still need to update document counts
            self.state_mgr.update_collection_state(
                state_id=self._state_id,
                last_cursor_value=None,
                cursor_field="",
                documents_processed=total_docs,
                documents_succeeded=total_docs,
                documents_failed=0,
            )

        return ReplicationResult(
            collection_name=self.collection_name,
            status="completed",
            documents_processed=total_docs,
            batches_processed=batch_num,
            duration_seconds=0,  # Set by caller
            documents_transformed=total_documents_transformed,
            transforms_applied=total_transforms_applied,
            transform_operations=aggregated_operations,
        )
