"""MongoDB document sampling for PII analysis.

This module provides intelligent sampling strategies to efficiently
analyze large collections while maintaining representative coverage.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection
from pymongo.database import Database

logger = logging.getLogger(__name__)


@dataclass
class SamplingResult:
    """Result of sampling a collection."""
    
    collection_name: str
    total_documents: int
    sampled_documents: int
    sample_docs: List[Dict[str, Any]]
    sampling_strategy: str  # "all", "random", "stratified"
    date_field_used: Optional[str] = None


class CollectionSampler:
    """
    Samples documents from MongoDB collections for PII analysis.
    
    Uses stratified sampling by date when possible to ensure coverage
    of both recent and historical documents (schema evolution).
    """
    
    # Common date field names to try for stratified sampling
    DATE_FIELD_CANDIDATES = [
        "meta.updatedAt",
        "meta.createdAt",
        "updatedAt",
        "createdAt",
        "created_at",
        "updated_at",
        "timestamp",
        "date",
    ]
    
    def __init__(
        self,
        database: Database,
        sample_size: int = 100,
        exclude_patterns: Optional[List[str]] = None,
    ):
        """
        Initialize collection sampler.
        
        Args:
            database: MongoDB database instance
            sample_size: Number of documents to sample per collection
            exclude_patterns: Regex patterns for collections to exclude
        """
        self.database = database
        self.sample_size = sample_size
        self.exclude_patterns = exclude_patterns or []
    
    def sample_collection(
        self,
        collection_name: str,
    ) -> SamplingResult:
        """
        Sample documents from a single collection.
        
        Strategy:
        1. If collection has <= sample_size docs, return all
        2. If date field exists, use stratified sampling (50% recent, 50% older)
        3. Otherwise, use MongoDB $sample for random sampling
        
        Args:
            collection_name: Name of collection to sample
            
        Returns:
            SamplingResult with sampled documents and metadata
        """
        collection = self.database[collection_name]
        total_docs = collection.count_documents({})
        
        logger.info(f"📊 Sampling {collection_name}: {total_docs:,} total documents")
        
        # Small collection - get all documents
        if total_docs == 0:
            logger.info(f"   ⚠️  Empty collection - no samples")
            return SamplingResult(
                collection_name=collection_name,
                total_documents=0,
                sampled_documents=0,
                sample_docs=[],
                sampling_strategy="empty",
            )
        
        if total_docs <= self.sample_size:
            logger.info(f"   📥 Small collection - sampling all {total_docs} documents")
            docs = list(collection.find().limit(self.sample_size))
            return SamplingResult(
                collection_name=collection_name,
                total_documents=total_docs,
                sampled_documents=len(docs),
                sample_docs=docs,
                sampling_strategy="all",
            )
        
        # Try stratified sampling by date
        date_field = self._find_date_field(collection)
        if date_field:
            logger.info(f"   📅 Using stratified sampling on '{date_field}'")
            docs = self._stratified_sample(collection, date_field)
            return SamplingResult(
                collection_name=collection_name,
                total_documents=total_docs,
                sampled_documents=len(docs),
                sample_docs=docs,
                sampling_strategy="stratified",
                date_field_used=date_field,
            )
        
        # Fallback to random sampling
        logger.info(f"   🎲 Using random sampling (no date field found)")
        docs = self._random_sample(collection)
        return SamplingResult(
            collection_name=collection_name,
            total_documents=total_docs,
            sampled_documents=len(docs),
            sample_docs=docs,
            sampling_strategy="random",
        )
    
    def sample_all_collections(
        self,
        collection_names: Optional[List[str]] = None,
    ) -> Dict[str, SamplingResult]:
        """
        Sample documents from multiple collections.
        
        Args:
            collection_names: Specific collections to sample (None = all collections)
            
        Returns:
            Dict mapping collection name to SamplingResult
        """
        if collection_names is None:
            # Auto-discover collections (excluding system collections)
            all_collections = self.database.list_collection_names()
            collection_names = [
                name for name in all_collections
                if not self._is_excluded(name)
            ]
        
        logger.info(f"\n📚 Sampling {len(collection_names)} collections...")
        
        results = {}
        for i, coll_name in enumerate(collection_names, 1):
            logger.info(f"\n[{i}/{len(collection_names)}] {coll_name}")
            try:
                result = self.sample_collection(coll_name)
                results[coll_name] = result
            except Exception as e:
                logger.error(f"   ❌ Failed to sample {coll_name}: {e}")
                # Continue with other collections
                continue
        
        return results
    
    def _find_date_field(self, collection: Collection) -> Optional[str]:
        """
        Find a date field suitable for stratified sampling.
        
        Checks common date field names and returns the first one found
        with datetime values.
        
        Args:
            collection: MongoDB collection
            
        Returns:
            Field name if found, None otherwise
        """
        # Try each candidate field
        for field_name in self.DATE_FIELD_CANDIDATES:
            try:
                # Check if field exists and has datetime values
                sample_doc = collection.find_one(
                    {field_name: {"$type": "date"}},
                    {field_name: 1}
                )
                if sample_doc and self._get_nested_field(sample_doc, field_name):
                    return field_name
            except Exception:
                continue
        
        return None
    
    def _stratified_sample(
        self,
        collection: Collection,
        date_field: str,
    ) -> List[Dict[str, Any]]:
        """
        Perform stratified sampling: 50% recent, 50% older documents.
        
        Args:
            collection: MongoDB collection
            date_field: Field name to use for stratification
            
        Returns:
            List of sampled documents
        """
        half_size = self.sample_size // 2
        
        # Get recent documents (sorted descending, take first half)
        recent_docs = list(
            collection.find()
            .sort(date_field, -1)  # Descending (newest first)
            .limit(half_size)
        )
        
        # Get older documents (sorted ascending, take first half)
        older_docs = list(
            collection.find()
            .sort(date_field, 1)  # Ascending (oldest first)
            .limit(half_size)
        )
        
        # Combine samples
        all_docs = recent_docs + older_docs
        
        logger.info(
            f"      Recent: {len(recent_docs)} docs, "
            f"Older: {len(older_docs)} docs, "
            f"Total: {len(all_docs)} docs"
        )
        
        return all_docs
    
    def _random_sample(self, collection: Collection) -> List[Dict[str, Any]]:
        """
        Perform random sampling using MongoDB $sample aggregation.
        
        Args:
            collection: MongoDB collection
            
        Returns:
            List of sampled documents
        """
        docs = list(collection.aggregate([
            {"$sample": {"size": self.sample_size}}
        ]))
        
        logger.info(f"      Sampled: {len(docs)} random documents")
        
        return docs
    
    def _is_excluded(self, collection_name: str) -> bool:
        """
        Check if collection should be excluded based on patterns.
        
        Args:
            collection_name: Name of collection to check
            
        Returns:
            True if collection should be excluded
        """
        import re
        
        # Always exclude system collections
        if collection_name.startswith("system."):
            return True
        
        # Check user-defined exclude patterns
        for pattern in self.exclude_patterns:
            if re.match(pattern, collection_name):
                return True
        
        return False
    
    def _get_nested_field(self, doc: Dict, field_path: str) -> Any:
        """
        Get value from nested field using dot notation.
        
        Args:
            doc: Document to extract from
            field_path: Field path (e.g., "meta.updatedAt")
            
        Returns:
            Field value or None if not found
        """
        parts = field_path.split(".")
        value = doc
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        
        return value
