"""Cascade filter building for selective replication."""

import logging
from typing import Dict, List, Any, Set
from pymongo.database import Database
from bson import ObjectId

from mongo_replication.engine.relationships import RelationshipGraph

logger = logging.getLogger(__name__)


class CascadeResult:
    """Result of cascade filter building with metadata."""
    
    def __init__(self):
        self.filters: Dict[str, Dict[str, Any]] = {}
        self.doc_counts: Dict[str, int] = {}
        self.skipped: Set[str] = set()
    
    def add_collection(self, collection: str, match_filter: Dict[str, Any], doc_count: int):
        """Add a collection's filter and document count."""
        self.filters[collection] = match_filter
        self.doc_counts[collection] = doc_count
        if doc_count == 0:
            self.skipped.add(collection)
    
    def get_total_documents(self) -> int:
        """Get total document count across all collections."""
        return sum(self.doc_counts.values())
    
    @property
    def skipped_collections(self) -> Set[str]:
        """Alias for skipped for backward compatibility."""
        return self.skipped


class CascadeFilterBuilder:
    """
    Builds MongoDB match filters by cascading through collection relationships.
    
    Example:
        Given: customers=[id1, id2]
        Builds:
            - customers: {_id: {$in: [id1, id2]}}
            - orders: {customerId: {$in: [id1, id2]}}
            - order_items: {orderId: {$in: [order_ids...]}}
    """
    
    def __init__(self, source_db: Database, graph: RelationshipGraph):
        """
        Initialize builder.
        
        Args:
            source_db: Source MongoDB database
            graph: RelationshipGraph with collection dependencies
        """
        self.source_db = source_db
        self.graph = graph
    
    def build_filters(
        self,
        root_collection: str,
        root_ids: List[str]
    ) -> CascadeResult:
        """
        Build match filters for all collections in relationship chain.
        
        Args:
            root_collection: Starting collection (e.g., "customers")
            root_ids: List of IDs to filter by (as strings)
            
        Returns:
            CascadeResult with filters, document counts, and skipped collections
            
        Example:
            build_filters("customers", ["id1", "id2"])
            => CascadeResult with:
                filters = {
                    "customers": {"_id": {"$in": [ObjectId("id1"), ObjectId("id2")]}},
                    "orders": {"customerId": {"$in": [ObjectId("id1"), ObjectId("id2")]}},
                    "order_items": {"orderId": {"$in": [ObjectId("ord1"), ...]}}
                }
                doc_counts = {"customers": 2, "orders": 5, "order_items": 12}
                skipped = set()  # or {"addresses"} if 0 addresses found
            
        Raises:
            ValueError: If ObjectId conversion fails or collections don't exist
        """
        result = CascadeResult()
        id_cache = {}  # Cache IDs per collection: {collection: [ids...]}
        
        # Get all descendants in order
        collections = self.graph.get_descendants(root_collection)
        
        logger.info(f"Building cascade filters for {len(collections)} collections: {collections}")
        
        # Validate all collections exist in source DB
        existing_collections = set(self.source_db.list_collection_names())
        self.graph.validate_collections(existing_collections)
        
        # Process root collection
        logger.info(f"Processing root collection: {root_collection}")
        
        # Convert root IDs to ObjectId if needed
        root_object_ids = self._convert_to_object_ids(root_ids, root_collection, "_id")
        
        # Build root filter
        root_filter = {"_id": {"$in": root_object_ids}}
        
        # Query and cache root IDs
        root_count = self.source_db[root_collection].count_documents(root_filter)
        result.add_collection(root_collection, root_filter, root_count)
        
        if root_count == 0:
            logger.warning(
                f"No documents found in {root_collection} matching filter. "
                f"Cascade will replicate zero records."
            )
            # Mark all descendants as skipped too
            for collection in collections[1:]:
                result.add_collection(collection, {"_id": {"$in": []}}, 0)
            return result
        else:
            logger.info(f"Found {root_count} documents in {root_collection}")
        
        # Extract actual IDs from root collection (in case some IDs don't exist)
        id_cache[root_collection] = self._query_field_values(
            root_collection,
            root_filter,
            "_id"
        )
        
        # Process each descendant collection
        for collection in collections[1:]:  # Skip root (already processed)
            logger.debug(f"Processing child collection: {collection}")
            
            # Find parent relationship
            parent_rel = self.graph.get_parent_relationship(collection)
            
            if not parent_rel:
                raise ValueError(
                    f"No parent relationship found for collection '{collection}' "
                    f"in cascade chain from '{root_collection}'"
                )
            
            # Get parent IDs from cache
            parent_ids = id_cache.get(parent_rel.parent, [])
            
            if not parent_ids:
                logger.warning(
                    f"No parent IDs found for {collection}, "
                    f"will skip this collection"
                )
                result.add_collection(collection, {"_id": {"$in": []}}, 0)
                id_cache[collection] = []
                continue
            
            # Build filter for child using parent IDs
            child_filter = {
                parent_rel.child_field: {"$in": parent_ids}
            }
            
            # Count documents matching filter
            doc_count = self.source_db[collection].count_documents(child_filter)
            result.add_collection(collection, child_filter, doc_count)
            
            if doc_count == 0:
                logger.info(
                    f"No documents in {collection} related to {parent_rel.parent}, "
                    f"collection will be skipped"
                )
                id_cache[collection] = []
                continue
            
            logger.info(
                f"Found {doc_count} documents in {collection} "
                f"related to {parent_rel.parent}"
            )
            
            # Query child collection and cache primary key values for next level
            # We need the primary key for the NEXT level of cascading
            primary_key_field = parent_rel.parent_field  # Use parent_field as PK for this collection
            
            id_cache[collection] = self._query_field_values(
                collection,
                child_filter,
                primary_key_field
            )
        
        return result
    
    def _convert_to_object_ids(
        self,
        ids: List[str],
        collection: str,
        field: str
    ) -> List[ObjectId]:
        """
        Convert string IDs to ObjectIds with validation.
        
        Args:
            ids: List of ID strings
            collection: Collection name (for error messages)
            field: Field name (for error messages)
            
        Returns:
            List of ObjectIds
            
        Raises:
            ValueError: If any ID is not a valid ObjectId
        """
        object_ids = []
        
        for id_str in ids:
            try:
                object_ids.append(ObjectId(id_str))
            except Exception as e:
                raise ValueError(
                    f"Invalid ObjectId '{id_str}' for {collection}.{field}: {e}. "
                    f"All IDs must be valid 24-character hex strings."
                )
        
        return object_ids
    
    def _query_field_values(
        self,
        collection: str,
        match_filter: Dict[str, Any],
        field: str
    ) -> List[Any]:
        """
        Query source DB and extract unique field values.
        
        Args:
            collection: Collection name
            match_filter: MongoDB query filter
            field: Field name to extract
            
        Returns:
            List of unique field values
        """
        try:
            logger.debug(
                f"Querying {collection} with filter {match_filter} to extract {field} values"
            )
            
            cursor = self.source_db[collection].find(
                match_filter,
                {field: 1}
            )
            
            values = []
            for doc in cursor:
                if field in doc:
                    values.append(doc[field])
            
            # Return unique values
            unique_values = list(set(values))
            logger.debug(f"Extracted {len(unique_values)} unique values for {collection}.{field}")
            return unique_values
            
        except Exception as e:
            raise ValueError(
                f"Failed to query {collection} with filter {match_filter}: {e}"
            )
