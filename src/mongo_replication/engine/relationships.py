"""Relationship graph management for cascading replication."""

from dataclasses import dataclass
from typing import List, Dict, Set, Optional
from collections import defaultdict, deque


@dataclass
class Relationship:
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
                return {'name': collection, 'children': [], 'is_cycle': True}
            
            visited.add(collection)
            
            children = []
            for rel in self._children_map.get(collection, []):
                child_tree = build_tree(rel.child, visited.copy())
                children.append(child_tree)
            
            return {
                'name': collection,
                'children': children
            }
        
        return build_tree(root_collection, set())
