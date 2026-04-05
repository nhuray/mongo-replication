"""Tree visualization for cascade replication."""

from typing import Dict, List, Optional, Set
from rich.tree import Tree
from rich.text import Text


class CascadeTreeBuilder:
    """Builds Rich Tree visualizations for cascade replication."""
    
    @staticmethod
    def build_dry_run_tree(
        tree_structure: Dict,
        doc_counts: Dict[str, int],
        skipped: Set[str]
    ) -> Tree:
        """
        Build tree for dry-run showing what will be replicated.
        
        Args:
            tree_structure: Tree structure from RelationshipGraph.get_tree_structure()
            doc_counts: Map of collection name to document count
            skipped: Set of collection names that will be skipped (0 docs)
            
        Returns:
            Rich Tree object for display
        """
        def build_node(node_data: Dict, parent_tree: Optional[Tree] = None) -> Tree:
            collection = node_data['name']
            count = doc_counts.get(collection, 0)
            is_skipped = collection in skipped
            
            # Build label with status
            if is_skipped:
                label = Text(f"{collection} ", style="dim")
                label.append("(0 docs - will skip)", style="yellow")
            else:
                label = Text(f"{collection} ", style="cyan bold")
                label.append(f"({count:,} docs)", style="green")
            
            # Create tree node
            if parent_tree is None:
                tree = Tree(label)
            else:
                tree = parent_tree.add(label)
            
            # Add children
            for child_data in node_data.get('children', []):
                build_node(child_data, tree)
            
            return tree
        
        return build_node(tree_structure)
    
    @staticmethod
    def build_progress_tree(
        tree_structure: Dict,
        doc_counts: Dict[str, int],
        statuses: Dict[str, str],  # collection -> 'pending'|'replicating'|'completed'|'failed'|'skipped'
        errors: Dict[str, str] = None  # collection -> error message
    ) -> Tree:
        """
        Build tree for progress tracking during replication.
        
        Args:
            tree_structure: Tree structure from RelationshipGraph.get_tree_structure()
            doc_counts: Map of collection name to document count
            statuses: Map of collection name to status
            errors: Map of collection name to error message (optional)
            
        Returns:
            Rich Tree object for display
        """
        errors = errors or {}
        
        def build_node(node_data: Dict, parent_tree: Optional[Tree] = None) -> Tree:
            collection = node_data['name']
            count = doc_counts.get(collection, 0)
            status = statuses.get(collection, 'pending')
            error = errors.get(collection)
            
            # Build label with status indicator
            if status == 'pending':
                icon = "⏸️"
                label = Text(f"{icon} {collection} ", style="dim")
                label.append(f"({count:,} docs)", style="dim")
            elif status == 'replicating':
                icon = "⏳"
                label = Text(f"{icon} {collection} ", style="yellow bold")
                label.append(f"({count:,} docs)", style="yellow")
            elif status == 'completed':
                icon = "✅"
                label = Text(f"{icon} {collection} ", style="green bold")
                label.append(f"({count:,} docs)", style="green")
            elif status == 'failed':
                icon = "❌"
                label = Text(f"{icon} {collection} ", style="red bold")
                label.append(f"({count:,} docs)", style="red")
                if error:
                    error_msg = error[:50] + "..." if len(error) > 50 else error
                    label.append(f" - {error_msg}", style="red dim")
            elif status == 'skipped':
                icon = "⊘"
                label = Text(f"{icon} {collection} ", style="dim")
                label.append("(0 docs - skipped)", style="yellow dim")
            else:
                label = Text(f"{collection}", style="white")
            
            # Create tree node
            if parent_tree is None:
                tree = Tree(label)
            else:
                tree = parent_tree.add(label)
            
            # Add children
            for child_data in node_data.get('children', []):
                build_node(child_data, tree)
            
            return tree
        
        return build_node(tree_structure)
    
    @staticmethod
    def build_summary_tree(
        root_collection: str,
        tree_structure: Dict,
        doc_counts: Dict[str, int],
        successful: Set[str],
        failed: Set[str],
        skipped: Set[str]
    ) -> Tree:
        """
        Build tree for final summary after replication.
        
        Args:
            root_collection: Root collection name
            tree_structure: Tree structure from RelationshipGraph.get_tree_structure()
            doc_counts: Map of collection name to document count
            successful: Set of successfully replicated collections
            failed: Set of failed collections
            skipped: Set of skipped collections
            
        Returns:
            Rich Tree object for display
        """
        def build_node(node_data: Dict, parent_tree: Optional[Tree] = None) -> Tree:
            collection = node_data['name']
            count = doc_counts.get(collection, 0)
            
            # Determine status
            if collection in successful:
                icon = "✅"
                label = Text(f"{icon} {collection} ", style="green bold")
                label.append(f"({count:,} docs replicated)", style="green")
            elif collection in failed:
                icon = "❌"
                label = Text(f"{icon} {collection} ", style="red bold")
                label.append(f"(failed)", style="red")
            elif collection in skipped:
                icon = "⊘"
                label = Text(f"{icon} {collection} ", style="dim")
                label.append("(skipped - 0 docs)", style="yellow dim")
            else:
                label = Text(f"{collection}", style="white")
            
            # Create tree node
            if parent_tree is None:
                tree = Tree(label)
            else:
                tree = parent_tree.add(label)
            
            # Add children
            for child_data in node_data.get('children', []):
                build_node(child_data, tree)
            
            return tree
        
        return build_node(tree_structure)
