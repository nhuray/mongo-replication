"""Index management for MongoDB replication.

This module handles discovery and replication of indexes from source to
destination collections to maintain performance characteristics.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection
from pymongo.errors import OperationFailure

logger = logging.getLogger(__name__)


@dataclass
class IndexInfo:
    """Information about a MongoDB index."""
    
    name: str
    """Index name."""
    
    keys: List[tuple]
    """Index key specification as list of (field, direction) tuples."""
    
    unique: bool = False
    """Whether index enforces uniqueness."""
    
    sparse: bool = False
    """Whether index is sparse (only indexes documents with the field)."""
    
    background: bool = False
    """Whether index was created in background (deprecated in MongoDB 4.2+)."""
    
    expire_after_seconds: Optional[int] = None
    """TTL in seconds for TTL indexes."""
    
    partial_filter_expression: Optional[Dict[str, Any]] = None
    """Partial filter expression for partial indexes."""
    
    collation: Optional[Dict[str, Any]] = None
    """Collation options for index."""
    
    version: Optional[int] = None
    """Index version."""
    
    extra_options: Dict[str, Any] = field(default_factory=dict)
    """Any other index options not explicitly captured."""


class IndexManager:
    """Manages index discovery and replication for MongoDB collections.
    
    Handles all index types including:
    - Single field indexes
    - Compound indexes
    - Unique indexes
    - Sparse indexes
    - TTL indexes
    - Partial indexes
    - Text indexes
    - Geospatial indexes (2d, 2dsphere)
    - Hashed indexes
    """
    
    def get_indexes(self, collection: Collection) -> List[IndexInfo]:
        """Get all indexes from a collection.
        
        Args:
            collection: PyMongo Collection instance
            
        Returns:
            List of IndexInfo objects (excluding _id index)
        """
        try:
            indexes = []
            
            # list_indexes() returns a cursor of index documents
            for index_doc in collection.list_indexes():
                # Skip the automatic _id index (MongoDB creates it automatically)
                if index_doc['name'] == '_id_':
                    continue
                
                # Extract key specification
                # Format: {'field1': 1, 'field2': -1} -> [('field1', 1), ('field2', -1)]
                keys = list(index_doc['key'].items())
                
                # Extract common options
                index_info = IndexInfo(
                    name=index_doc['name'],
                    keys=keys,
                    unique=index_doc.get('unique', False),
                    sparse=index_doc.get('sparse', False),
                    background=index_doc.get('background', False),
                    expire_after_seconds=index_doc.get('expireAfterSeconds'),
                    partial_filter_expression=index_doc.get('partialFilterExpression'),
                    collation=index_doc.get('collation'),
                    version=index_doc.get('v'),
                )
                
                # Capture any other options not explicitly handled
                known_keys = {
                    'name', 'key', 'unique', 'sparse', 'background',
                    'expireAfterSeconds', 'partialFilterExpression', 'collation',
                    'v', 'ns'  # ns is collection namespace, we can ignore it
                }
                index_info.extra_options = {
                    k: v for k, v in index_doc.items()
                    if k not in known_keys
                }
                
                indexes.append(index_info)
            
            return indexes
            
        except OperationFailure as e:
            logger.warning(
                f"Failed to list indexes for {collection.name}: {e}. "
                f"Continuing without index replication."
            )
            return []
    
    def replicate_indexes(
        self,
        source_collection: Collection,
        dest_collection: Collection,
    ) -> tuple[int, int, List[str]]:
        """Replicate all indexes from source to destination collection.
        
        Creates indexes on destination to match source. Index creation is
        best-effort - failures are logged but don't stop the process.
        
        Args:
            source_collection: Source collection to read indexes from
            dest_collection: Destination collection to create indexes on
            
        Returns:
            Tuple of (indexes_replicated, indexes_failed, error_messages)
        """
        # Get indexes from source
        source_indexes = self.get_indexes(source_collection)
        
        if not source_indexes:
            logger.debug(f"No indexes to replicate for {source_collection.name}")
            return 0, 0, []
        
        logger.info(
            f"📑 Replicating {len(source_indexes)} index(es) for collection: {source_collection.name}"
        )
        
        replicated = 0
        failed = 0
        error_messages = []
        
        for index_info in source_indexes:
            success, error = self._create_single_index(dest_collection, index_info)
            
            if success:
                replicated += 1
                # Log index type for clarity
                index_type = self._get_index_type_description(index_info)
                logger.info(f"   ✅ Created index '{index_info.name}' ({index_type})")
            else:
                failed += 1
                error_msg = f"Index '{index_info.name}': {error}"
                error_messages.append(error_msg)
                logger.warning(f"   ⚠️  Failed to create index '{index_info.name}': {error}")
        
        return replicated, failed, error_messages
    
    def _create_single_index(
        self,
        collection: Collection,
        index_info: IndexInfo,
    ) -> tuple[bool, Optional[str]]:
        """Create a single index on a collection.
        
        Args:
            collection: Collection to create index on
            index_info: Index specification
            
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            # Build index creation kwargs
            kwargs = {
                'name': index_info.name,
                'unique': index_info.unique,
                'sparse': index_info.sparse,
            }
            
            # Add background option (deprecated in MongoDB 4.2+ but still supported)
            if index_info.background:
                kwargs['background'] = True
            
            # Add TTL if specified
            if index_info.expire_after_seconds is not None:
                kwargs['expireAfterSeconds'] = index_info.expire_after_seconds
            
            # Add partial filter if specified
            if index_info.partial_filter_expression:
                kwargs['partialFilterExpression'] = index_info.partial_filter_expression
            
            # Add collation if specified
            if index_info.collation:
                kwargs['collation'] = index_info.collation
            
            # Add any extra options
            kwargs.update(index_info.extra_options)
            
            # Create the index
            # Keys are in format [('field1', 1), ('field2', -1)]
            collection.create_index(index_info.keys, **kwargs)
            
            return True, None
            
        except OperationFailure as e:
            return False, str(e)
        except Exception as e:
            return False, f"{type(e).__name__}: {str(e)}"
    
    def _get_index_type_description(self, index_info: IndexInfo) -> str:
        """Get a human-readable description of the index type.
        
        Args:
            index_info: Index information
            
        Returns:
            String description like "unique", "compound", "text", etc.
        """
        descriptions = []
        
        # Check for unique
        if index_info.unique:
            descriptions.append("unique")
        
        # Check for sparse
        if index_info.sparse:
            descriptions.append("sparse")
        
        # Check for TTL
        if index_info.expire_after_seconds is not None:
            descriptions.append(f"TTL {index_info.expire_after_seconds}s")
        
        # Check for partial
        if index_info.partial_filter_expression:
            descriptions.append("partial")
        
        # Check index type from keys
        if len(index_info.keys) > 1:
            descriptions.append("compound")
        
        # Check for special index types (text, geospatial, hashed)
        for _, index_type in index_info.keys:
            if index_type == 'text':
                descriptions.append("text")
                break
            elif index_type == '2d':
                descriptions.append("2d geospatial")
                break
            elif index_type == '2dsphere':
                descriptions.append("2dsphere geospatial")
                break
            elif index_type == 'hashed':
                descriptions.append("hashed")
                break
        
        if not descriptions:
            # Default to "single field" for simple ascending/descending indexes
            if len(index_info.keys) == 1:
                return "single field"
            return "standard"
        
        return ", ".join(descriptions)
