"""Relationship graph management for cascading replication."""

from typing import List, Dict, Set, Optional
from collections import defaultdict, deque

from pydantic import BaseModel


class Relationship(BaseModel):
    """Represents a parent-child relationship between collections."""

    parent: str
    child: str
    parent_field: str
    child_field: str


class RelationshipGraph:
    """
    Builds and traverses collection dependency graph.

    Example:
        customers (root)
          ├─> orders
          │    └─> order_items
          └─> addresses
    """

    def __init__(self, relationships: List[Relationship]):
        """
        Initialize graph from relationships.

        Args:
            relationships: List of parent-child relationships
        """
        self.relationships = relationships
        self._children_map: Dict[str, List[Relationship]] = defaultdict(list)
        self._parent_map: Dict[str, Relationship] = {}
        self._build_maps()

    def _build_maps(self) -> None:
        """Build parent->children and child->parent lookup maps."""
        for rel in self.relationships:
            self._children_map[rel.parent].append(rel)

            # Validate: each child can only have one parent
            if rel.child in self._parent_map:
                existing = self._parent_map[rel.child]
                raise ValueError(
                    f"Collection '{rel.child}' has multiple parent relationships: "
                    f"'{existing.parent}' and '{rel.parent}'. "
                    f"Each collection can only have one parent."
                )
            self._parent_map[rel.child] = rel

    def get_descendants(self, root_collection: str) -> List[str]:
        """
        Get all descendant collections in breadth-first order.

        Args:
            root_collection: Starting collection name

        Returns:
            List of collection names [root, children, grandchildren, ...]

        Example:
            get_descendants("customers")
            => ["customers", "orders", "addresses", "order_items"]
        """
        descendants = []
        visited = set()
        queue = deque([root_collection])

        while queue:
            collection = queue.popleft()

            if collection in visited:
                continue

            visited.add(collection)
            descendants.append(collection)

            # Add children to queue
            for rel in self._children_map.get(collection, []):
                if rel.child not in visited:
                    queue.append(rel.child)

        return descendants

    def get_parent_relationship(self, child_collection: str) -> Optional[Relationship]:
        """
        Get the parent relationship for a child collection.

        Args:
            child_collection: Child collection name

        Returns:
            Relationship if exists, None otherwise
        """
        return self._parent_map.get(child_collection)

    def get_children_relationships(self, parent_collection: str) -> List[Relationship]:
        """
        Get all direct child relationships for a parent.

        Args:
            parent_collection: Parent collection name

        Returns:
            List of relationships where this collection is the parent
        """
        return self._children_map.get(parent_collection, [])

    def validate_collections(self, existing_collections: Set[str]) -> None:
        """
        Validate that all collections in relationships exist.

        Args:
            existing_collections: Set of collection names that exist in source DB

        Raises:
            ValueError: If any collection in relationships doesn't exist
        """
        for rel in self.relationships:
            if rel.parent not in existing_collections:
                raise ValueError(
                    f"Relationship references non-existent parent collection: '{rel.parent}'"
                )
            if rel.child not in existing_collections:
                raise ValueError(
                    f"Relationship references non-existent child collection: '{rel.child}'"
                )

    def has_cycles(self) -> bool:
        """
        Check if graph has cycles (circular dependencies).

        Returns:
            True if cycles detected, False otherwise
        """
        visited = set()
        rec_stack = set()

        def has_cycle_util(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for rel in self._children_map.get(node, []):
                if rel.child not in visited:
                    if has_cycle_util(rel.child):
                        return True
                elif rel.child in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        # Check all nodes
        all_nodes = set(self._children_map.keys()) | set(self._parent_map.keys())
        for node in all_nodes:
            if node not in visited:
                if has_cycle_util(node):
                    return True

        return False

    def get_tree_structure(self, root_collection: str) -> Dict[str, any]:
        """
        Get tree structure for visualization.

        Args:
            root_collection: Root collection name

        Returns:
            Nested dict representing tree structure
            {
                'name': 'customers',
                'children': [
                    {'name': 'orders', 'children': [...]},
                    {'name': 'addresses', 'children': []}
                ]
            }
        """

        def build_tree(collection: str, visited: Set[str]) -> Dict[str, any]:
            if collection in visited:
                return {"name": collection, "children": [], "is_cycle": True}

            visited.add(collection)

            children = []
            for rel in self._children_map.get(collection, []):
                child_tree = build_tree(rel.child, visited.copy())
                children.append(child_tree)

            return {"name": collection, "children": children}

        return build_tree(root_collection, set())


class SchemaRelationshipAnalyzer:
    """
    Analyzes document samples to infer relationships between collections.

    Detects relationships by matching collection names with field names.
    For example:
    - Collection 'customers' with _id field
    - Collection 'orders' with 'customer_id' or 'customerId' field
    → Inferred relationship: customers → orders
    """

    def __init__(self, collection_samples: Dict[str, List[Dict]]):
        """
        Initialize analyzer with document samples.

        Args:
            collection_samples: Dict mapping collection names to lists of sample documents
                                Example: {"customers": [{...}, {...}], "orders": [{...}]}
        """
        self.collection_samples = collection_samples
        self.collection_names = set(collection_samples.keys())

    def infer_relationships(self) -> List[Relationship]:
        """
        Infer relationships between collections based on field names.

        Returns:
            List of inferred Relationship objects

        Algorithm:
        1. For each collection, extract all field names from samples
        2. For each field, try to match it to another collection name
        3. Match patterns:
           - Exact match: field "customer_id" → collection "customers"
           - Camel case: field "customerId" → collection "customers"
           - Plural to singular: field "customer_id" → collection "customers"
           - Nested fields: field "meta.customer_id" → collection "customers"
        """
        relationships = []

        for child_collection, samples in self.collection_samples.items():
            if not samples:
                continue

            # Extract all fields from samples
            fields = self._extract_fields_from_samples(samples)

            # Try to find parent relationships
            for field_path in fields:
                parent_collection = self._match_field_to_collection(field_path)

                if parent_collection and parent_collection != child_collection:
                    # Infer that this is a parent-child relationship
                    relationship = Relationship(
                        parent=parent_collection,
                        child=child_collection,
                        parent_field="_id",  # Assume primary key is _id
                        child_field=field_path,
                    )
                    relationships.append(relationship)

        # Deduplicate relationships (same parent-child pair)
        unique_relationships = self._deduplicate_relationships(relationships)

        return unique_relationships

    def _extract_fields_from_samples(self, samples: List[Dict]) -> Set[str]:
        """
        Extract all field paths from sample documents.

        Args:
            samples: List of sample documents

        Returns:
            Set of field paths (supports nested fields with dot notation)
        """
        fields = set()

        for doc in samples:
            fields.update(self._extract_fields_recursive(doc))

        return fields

    def _extract_fields_recursive(self, obj: any, prefix: str = "") -> Set[str]:
        """
        Recursively extract field paths from a document.

        Args:
            obj: Document or nested object
            prefix: Current field path prefix

        Returns:
            Set of field paths
        """
        fields = set()

        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{prefix}.{key}" if prefix else key
                fields.add(field_path)

                # Recurse into nested objects (but not too deep)
                if isinstance(value, dict) and prefix.count(".") < 2:
                    fields.update(self._extract_fields_recursive(value, field_path))

        return fields

    def _match_field_to_collection(self, field_path: str) -> Optional[str]:
        """
        Try to match a field name to a collection name.

        Args:
            field_path: Field path (e.g., "customer_id", "customerId", "meta.customerId")

        Returns:
            Matched collection name, or None if no match

        Matching logic:
        1. Extract base field name (last part of path)
        2. Normalize field name (remove _id, Id suffixes)
        3. Try to match against collection names (singular/plural variants)
        """
        # Get the last part of the path (e.g., "meta.customer_id" → "customer_id")
        field_name = field_path.split(".")[-1]

        # Skip common non-relationship fields
        if field_name in {"_id", "id", "created_at", "updated_at", "createdAt", "updatedAt"}:
            return None

        # Extract potential collection reference from field name
        # Handle patterns: customer_id, customerId, customer_ids, customerIds
        potential_names = self._extract_collection_references(field_name)

        # Try to find matching collection
        for name in potential_names:
            if name in self.collection_names:
                return name

        return None

    def _extract_collection_references(self, field_name: str) -> List[str]:
        """
        Extract potential collection names from a field name.

        Args:
            field_name: Field name (e.g., "customer_id", "customerId")

        Returns:
            List of potential collection names to try
        """
        potential_names = []

        # Pattern 1: snake_case with _id or _ids suffix
        # "customer_id" → "customer"
        # "customer_ids" → "customers"
        if "_id" in field_name:
            base = field_name.replace("_ids", "").replace("_id", "")
            potential_names.append(base)
            potential_names.append(base + "s")  # Try plural

            # If base ends with 'y', try replacing with 'ies'
            # "category_id" → "categories"
            if base.endswith("y"):
                potential_names.append(base[:-1] + "ies")

        # Pattern 2: camelCase with Id or Ids suffix
        # "customerId" → "customer"
        # "customerIds" → "customers"
        if field_name.endswith("Ids"):
            base = field_name[:-3]  # Remove "Ids"
            # Convert camelCase to snake_case
            snake_case = self._camel_to_snake(base)
            potential_names.append(snake_case + "s")  # Plural
            potential_names.append(snake_case)
        elif field_name.endswith("Id") and not field_name.endswith("_id"):
            base = field_name[:-2]  # Remove "Id"
            # Convert camelCase to snake_case
            snake_case = self._camel_to_snake(base)
            potential_names.append(snake_case)
            potential_names.append(snake_case + "s")  # Try plural

            # If base ends with 'y', try replacing with 'ies'
            if snake_case.endswith("y"):
                potential_names.append(snake_case[:-1] + "ies")

        return potential_names

    def _camel_to_snake(self, name: str) -> str:
        """
        Convert camelCase to snake_case.

        Args:
            name: camelCase string

        Returns:
            snake_case string
        """
        import re

        # Insert underscore before uppercase letters
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        return snake

    def _deduplicate_relationships(self, relationships: List[Relationship]) -> List[Relationship]:
        """
        Remove duplicate relationships (same parent-child pair).

        If multiple fields point to the same parent, keep the first one found.

        Args:
            relationships: List of relationships (may contain duplicates)

        Returns:
            Deduplicated list of relationships
        """
        seen = set()
        unique = []

        for rel in relationships:
            key = (rel.parent, rel.child)
            if key not in seen:
                seen.add(key)
                unique.append(rel)

        return unique
