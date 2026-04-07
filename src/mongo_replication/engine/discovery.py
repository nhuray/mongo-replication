"""Collection discovery and filtering for MongoDB replication.

This module handles auto-discovery of collections from the source database
and applies include/exclude regex patterns for filtering.
"""

import logging
import re
from typing import List, Set

from pydantic import BaseModel
from pymongo.database import Database

logger = logging.getLogger(__name__)


class DiscoveryResult(BaseModel):
    """Result of collection discovery process."""

    all_collections: List[str]
    included_collections: List[str]
    excluded_collections: List[str]
    configured_collections: Set[str]
    auto_discovered_collections: Set[str]

    @property
    def total_found(self) -> int:
        """Total number of collections found in source."""
        return len(self.all_collections)

    @property
    def total_included(self) -> int:
        """Number of collections to be replicated."""
        return len(self.included_collections)

    @property
    def total_excluded(self) -> int:
        """Number of collections excluded from replication."""
        return len(self.excluded_collections)

    def log_summary(self) -> None:
        """Log a summary of discovery results."""
        logger.info("=" * 60)
        logger.info("COLLECTION DISCOVERY SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📊 Total collections found: {self.total_found}")
        logger.info(f"✅ Collections to replicate: {self.total_included}")
        logger.info(f"❌ Collections excluded: {self.total_excluded}")
        logger.info(f"⚙️  Configured collections: {len(self.configured_collections)}")
        logger.info(f"🔍 Auto-discovered collections: {len(self.auto_discovered_collections)}")
        logger.info("=" * 60)

        if self.excluded_collections:
            logger.info(f"Excluded: {', '.join(sorted(self.excluded_collections))}")


class CollectionDiscovery:
    """Discovers and filters collections from source MongoDB database.

    Supports two modes:
    1. replicate_all=true: Include all except excluded patterns
    2. replicate_all=false: Only include matching include patterns
    """

    def __init__(
        self,
        source_db: Database,
        replicate_all: bool = True,
        include_patterns: List[str] | None = None,
        exclude_patterns: List[str] | None = None,
        state_collections: List[str] | None = None,
    ):
        """Initialize collection discovery.

        Args:
            source_db: PyMongo Database instance for source
            replicate_all: If True, replicate all except excluded. If False, only included.
            include_patterns: List of regex patterns for collections to include (when replicate_all=False)
            exclude_patterns: List of regex patterns for collections to exclude
            state_collections: List of state management collection names to exclude
        """
        self.source_db = source_db
        self.replicate_all = replicate_all
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []

        # Always exclude the state management collections
        state_collections = state_collections or []
        for state_coll in state_collections:
            if state_coll and state_coll not in self.exclude_patterns:
                self.exclude_patterns.append(f"^{state_coll}$")

        # Compile regex patterns for performance
        self._include_regex = [re.compile(pattern) for pattern in self.include_patterns]
        self._exclude_regex = [re.compile(pattern) for pattern in self.exclude_patterns]

    def _matches_any_pattern(self, collection_name: str, patterns: List[re.Pattern]) -> bool:
        """Check if collection name matches any regex pattern.

        Args:
            collection_name: Name of the collection
            patterns: List of compiled regex patterns

        Returns:
            True if any pattern matches
        """
        return any(pattern.search(collection_name) for pattern in patterns)

    def _should_include(self, collection_name: str) -> bool:
        """Determine if a collection should be included in replication.

        Args:
            collection_name: Name of the collection

        Returns:
            True if collection should be replicated
        """
        # Check exclude patterns first (takes precedence)
        if self._matches_any_pattern(collection_name, self._exclude_regex):
            return False

        # If replicate_all mode, include everything not excluded
        if self.replicate_all:
            return True

        # Otherwise, only include if matches include patterns
        return self._matches_any_pattern(collection_name, self._include_regex)

    def discover_collections(self, configured_collections: Set[str]) -> DiscoveryResult:
        """Discover collections from source database and apply filters.

        Args:
            configured_collections: Set of collection names that have explicit config

        Returns:
            DiscoveryResult with categorized collections
        """
        logger.info(f"🔍 Discovering collections from source database: {self.source_db.name}")

        # Get all collection names from source
        all_collections = self.source_db.list_collection_names()
        logger.info(f"Found {len(all_collections)} collections in source")

        # Apply filtering
        included = []
        excluded = []

        for coll_name in all_collections:
            if self._should_include(coll_name):
                included.append(coll_name)
            else:
                excluded.append(coll_name)

        # Categorize included collections
        included_set = set(included)
        auto_discovered = included_set - configured_collections

        result = DiscoveryResult(
            all_collections=all_collections,
            included_collections=sorted(included),
            excluded_collections=sorted(excluded),
            configured_collections=configured_collections,
            auto_discovered_collections=auto_discovered,
        )

        result.log_summary()

        return result

    def get_excluded_collections(self, all_collections: List[str]) -> List[str]:
        """Get list of collections that would be excluded.

        Useful for validation and debugging.

        Args:
            all_collections: List of all collection names

        Returns:
            List of excluded collection names
        """
        return [coll for coll in all_collections if not self._should_include(coll)]
