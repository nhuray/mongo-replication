"""Replication orchestrator for parallel MongoDB collection replication.

This module coordinates the entire replication process:
- Auto-discovers collections from source database
- Builds configurations (explicit + defaults for auto-discovered)
- Validates cursor fields
- Processes collections in parallel (configurable workers)
- Aggregates and reports results
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional

from pydantic import BaseModel

from mongo_replication.config import CollectionConfig, ReplicationConfig
from mongo_replication.engine.connection import ConnectionManager
from mongo_replication.engine.discovery import CollectionDiscovery, DiscoveryResult
from mongo_replication.engine.field_exclusion import FieldExcluder
from mongo_replication.engine.indexes import IndexManager
from mongo_replication.engine.pii import create_pii_handler_from_config
from mongo_replication.engine.replicator import CollectionReplicator, ReplicationResult
from mongo_replication.engine.state import StateManager
from mongo_replication.engine.transformations import FieldTransformer
from mongo_replication.engine.validation import CursorValidator

logger = logging.getLogger(__name__)


# Progress callback type: (collection_name, status, result_or_error)
ProgressCallback = Callable[[str, str, Optional[ReplicationResult]], None]


class OrchestrationResult(BaseModel):
    """Result of orchestrating replication for all collections."""

    discovery: DiscoveryResult
    collection_results: Dict[str, ReplicationResult]
    total_duration_seconds: float

    @property
    def total_collections_processed(self) -> int:
        """Total number of collections processed."""
        return len(self.collection_results)

    @property
    def successful_collections(self) -> List[str]:
        """Collections that completed successfully."""
        return [
            name for name, result in self.collection_results.items() if result.status == "completed"
        ]

    @property
    def failed_collections(self) -> List[str]:
        """Collections that failed."""
        return [
            name for name, result in self.collection_results.items() if result.status == "failed"
        ]

    @property
    def total_documents_processed(self) -> int:
        """Total documents processed across all collections."""
        return sum(result.documents_processed for result in self.collection_results.values())

    def log_summary(self) -> None:
        """Log a summary of the orchestration results."""
        logger.info("=" * 60)
        logger.info("REPLICATION SUMMARY")
        logger.info("=" * 60)
        logger.info(
            f"✅ Successfully replicated: {len(self.successful_collections)}/{self.total_collections_processed} collections"
        )
        logger.info(f"   Total documents: {self.total_documents_processed:,}")
        logger.info(f"   Total duration: {self.total_duration_seconds:.1f}s")
        logger.info("")
        logger.info(f"   Configured collections: {len(self.discovery.configured_collections)}")
        logger.info(
            f"   Auto-discovered collections: {len(self.discovery.auto_discovered_collections)}"
        )

        # Count collections with special features
        pii_count = sum(1 for r in self.collection_results.values() if r.pii_fields_redacted > 0)
        logger.info(f"   Collections with PII redaction: {pii_count}")

        # Index replication summary
        total_indexes = sum(r.indexes_replicated for r in self.collection_results.values())
        failed_indexes = sum(r.indexes_failed for r in self.collection_results.values())
        if total_indexes > 0 or failed_indexes > 0:
            logger.info(f"   Indexes replicated: {total_indexes}")
            if failed_indexes > 0:
                logger.info(f"   ⚠️  Indexes failed: {failed_indexes}")

        if self.failed_collections:
            logger.info("")
            logger.info(f"❌ Failed collections ({len(self.failed_collections)}):")
            for coll_name in self.failed_collections:
                result = self.collection_results[coll_name]
                logger.info(f"   - {coll_name}: {result.error_message}")

        logger.info("=" * 60)


class ReplicationOrchestrator:
    """Orchestrates parallel replication of MongoDB collections.

    This is the main entry point for the replication pipeline. It:
    1. Discovers collections from source
    2. Builds configurations (explicit + defaults)
    3. Validates cursor fields
    4. Replicates collections in parallel
    5. Reports results
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        config: ReplicationConfig,
    ):
        """Initialize the orchestrator.

        Args:
            connection_manager: MongoDB connection manager
            config: Replication configuration
        """
        self.conn_mgr = connection_manager
        self.config = config

        # Initialize components
        self.source_db = connection_manager.get_source_db()
        self.dest_db = connection_manager.get_dest_db()

        # Get state collection names from config (with defaults)
        self.runs_collection = config.state_management.runs_collection
        self.state_collection = config.state_management.state_collection

        self.state_mgr = StateManager(
            self.dest_db,
            runs_collection=self.runs_collection,
            state_collection=self.state_collection,
        )
        self.validator = CursorValidator(fallback_cursor=config.defaults.cursor_fallback_field)
        self.index_mgr = IndexManager()

    def _build_collection_config(
        self,
        collection_name: str,
        explicit_config: Optional[CollectionConfig] = None,
    ) -> CollectionConfig:
        """Build configuration for a collection.

        If explicit config exists, use it with defaults merged.
        Otherwise, create config from defaults for auto-discovered collection.

        Args:
            collection_name: Name of the collection
            explicit_config: Explicit configuration if exists

        Returns:
            Complete CollectionConfig
        """
        if explicit_config:
            # Use explicit config with defaults as fallback
            return explicit_config

        # Auto-discovered collection - create config from defaults
        return CollectionConfig(
            name=collection_name,
            cursor_field=self.config.defaults.cursor_field,
            write_disposition=self.config.defaults.write_disposition,
            primary_key=self.config.defaults.primary_key,
            pii_anonymized_fields={},  # No PII redaction for auto-discovered
            transform_error_mode=self.config.defaults.transform_error_mode,
        )

    def _replicate_single_collection(
        self,
        run_id,
        collection_name: str,
        config: CollectionConfig,
        batch_size: int,
    ) -> ReplicationResult:
        """Replicate a single collection.

        This method is called in parallel by ThreadPoolExecutor.

        Args:
            run_id: ObjectId of the parent run
            collection_name: Name of the collection
            config: Collection configuration
            batch_size: Batch size for processing

        Returns:
            ReplicationResult
        """
        try:
            # Create collection state
            state_id = self.state_mgr.start_collection(run_id, collection_name)

            # Create replicator for this collection
            source_coll = self.source_db[collection_name]
            dest_coll = self.dest_db[collection_name]

            replicator = CollectionReplicator(
                source_collection=source_coll,
                dest_collection=dest_coll,
                state_manager=self.state_mgr,
                cursor_validator=self.validator,
                index_manager=self.index_mgr,
            )

            # Create transformation engines if configured
            field_transformer = None
            if config.field_transforms:
                field_transformer = FieldTransformer(
                    transforms=config.field_transforms,
                    error_mode=config.transform_error_mode,
                )

            field_excluder = None
            if config.fields_exclude:
                field_excluder = FieldExcluder(
                    fields_to_exclude=config.fields_exclude,
                )

            # Create PII handler from collection config
            pii_handler = create_pii_handler_from_config(config)

            # Debug: Log match filter being used
            logger.info(
                f"🔍 Orchestrator - Replicating {collection_name} with match_filter: {config.match}"
            )

            # Replicate
            result = replicator.replicate(
                state_id=state_id,
                cursor_field=config.cursor_field,
                write_disposition=config.write_disposition,
                primary_key=config.primary_key,
                pii_handler=pii_handler,
                batch_size=batch_size,
                match_filter=config.match,
                field_transformer=field_transformer,
                field_excluder=field_excluder,
            )

            return result

        except Exception as e:
            logger.error(f"❌ {collection_name}: Unexpected error - {e}")
            import traceback

            traceback.print_exc()

            return ReplicationResult(
                collection_name=collection_name,
                status="failed",
                documents_processed=0,
                batches_processed=0,
                duration_seconds=0,
                error_message=f"Unexpected error: {type(e).__name__}: {str(e)}",
            )

    def replicate(
        self,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> OrchestrationResult:
        """Execute replication for all collections.

        This is the main entry point for the replication pipeline.

        Args:
            progress_callback: Optional callback for progress updates.
                              Called with (collection_name, status, result_or_none)
                              where status is "started", "completed", or "failed"

        Returns:
            OrchestrationResult with complete statistics
        """
        start_time = time.time()

        logger.info("\n" + "=" * 60)
        logger.info("🚀 STARTING MONGODB REPLICATION PIPELINE")
        logger.info("=" * 60)

        # Create a new run and track it
        run_id = self.state_mgr.create_run()
        logger.info(f"📝 Run ID: {run_id}")

        try:
            # Step 1: Discover collections
            logger.info("\n📊 Step 1: Discovering collections...")
            discovery = CollectionDiscovery(
                source_db=self.source_db,
                replicate_all=self.config.discovery.replicate_all,
                include_patterns=self.config.discovery.include_patterns,
                exclude_patterns=self.config.discovery.exclude_patterns,
                state_collections=[self.runs_collection, self.state_collection],
            )

            # Get configured collection names
            configured_names = set(self.config.collections.keys())

            # Run discovery
            discovery_result = discovery.discover_collections(configured_names)

            # Step 2: Build configurations
            logger.info("\n⚙️  Step 2: Building collection configurations...")
            collection_configs: Dict[str, CollectionConfig] = {}

            for coll_name in discovery_result.included_collections:
                explicit_config = self.config.collections.get(coll_name)
                config = self._build_collection_config(coll_name, explicit_config)
                collection_configs[coll_name] = config

                if coll_name in discovery_result.auto_discovered_collections:
                    logger.info(f"   🔍 {coll_name}: Auto-discovered (using defaults)")
                else:
                    logger.info(
                        f"   ⚙️  {coll_name}: Configured (PII: {len(config.pii_anonymized_fields)} fields)"
                    )

            # Step 3: Replicate collections in parallel
            max_workers = self.config.performance.max_parallel_collections
            batch_size = self.config.performance.batch_size

            logger.info(
                f"\n🔄 Step 3: Replicating {len(collection_configs)} collections (max {max_workers} parallel)..."
            )

            collection_results: Dict[str, ReplicationResult] = {}

            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all replication tasks with run_id
                future_to_collection = {
                    executor.submit(
                        self._replicate_single_collection,
                        run_id,
                        coll_name,
                        config,
                        batch_size,
                    ): coll_name
                    for coll_name, config in collection_configs.items()
                }

                # Notify start for all collections
                if progress_callback:
                    for coll_name in collection_configs.keys():
                        progress_callback(coll_name, "started", None)

                # Process results as they complete
                for future in as_completed(future_to_collection):
                    coll_name = future_to_collection[future]
                    try:
                        result = future.result()
                        collection_results[coll_name] = result

                        # Notify completion
                        if progress_callback:
                            progress_callback(coll_name, "completed", result)

                    except Exception as e:
                        logger.error(f"❌ {coll_name}: Task failed - {e}")
                        error_result = ReplicationResult(
                            collection_name=coll_name,
                            status="failed",
                            documents_processed=0,
                            batches_processed=0,
                            duration_seconds=0,
                            error_message=str(e),
                        )
                        collection_results[coll_name] = error_result

                        # Notify failure
                        if progress_callback:
                            progress_callback(coll_name, "failed", error_result)

            # Step 4: Complete the run with final statistics
            total_duration = time.time() - start_time

            # Calculate run statistics
            collections_processed = len(collection_results)
            collections_succeeded = sum(
                1 for r in collection_results.values() if r.status == "completed"
            )
            collections_failed = sum(1 for r in collection_results.values() if r.status == "failed")

            documents_processed = sum(r.documents_processed for r in collection_results.values())
            # Note: We don't track succeeded/failed docs separately yet, so we'll use processed count
            documents_succeeded = documents_processed
            documents_failed = 0

            error_summary = {
                name: result.error_message
                for name, result in collection_results.items()
                if result.status == "failed" and result.error_message
            }
            failed_collections = [
                name for name, result in collection_results.items() if result.status == "failed"
            ]

            # Complete the run
            self.state_mgr.complete_run(
                run_id=run_id,
                collections_processed=collections_processed,
                collections_succeeded=collections_succeeded,
                collections_failed=collections_failed,
                documents_processed=documents_processed,
                documents_succeeded=documents_succeeded,
                documents_failed=documents_failed,
                error_summary=error_summary,
                failed_collections=failed_collections,
            )

            result = OrchestrationResult(
                discovery=discovery_result,
                collection_results=collection_results,
                total_duration_seconds=total_duration,
            )

            result.log_summary()

            return result

        except Exception as e:
            # Mark run as failed
            logger.error(f"Run {run_id} failed: {e}")
            self.state_mgr.fail_run(run_id, str(e))
            raise
