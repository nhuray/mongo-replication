"""Field exclusion engine for MongoDB documents.

Removes specified fields from documents before writing to destination.
Supports top-level and nested fields with "keep parent with remaining fields" logic.
"""

import logging
from typing import Any, Dict, List

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ExclusionStats(BaseModel):
    """Statistics for field exclusions."""

    documents_processed: int = 0
    fields_excluded: int = Field(default=0)  # Total number of field exclusions applied


class FieldExcluder:
    """Removes specified fields from MongoDB documents.

    Supports top-level and nested fields using dot notation.
    Implements "keep parent with remaining fields" logic for nested field exclusions.
    """

    def __init__(self, fields_to_exclude: List[str]):
        """Initialize the field excluder.

        Args:
            fields_to_exclude: List of field paths to exclude (supports dot notation)
        """
        self.fields_to_exclude = fields_to_exclude or []

    def exclude_fields_from_documents(
        self, documents: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], ExclusionStats]:
        """Exclude fields from a batch of documents.

        Args:
            documents: List of documents to process

        Returns:
            Tuple of (processed documents, statistics)
        """
        if not self.fields_to_exclude:
            # No exclusions configured - return documents unchanged
            stats = ExclusionStats(documents_processed=len(documents))
            return documents, stats

        stats = ExclusionStats()
        processed_docs = []

        for doc in documents:
            processed_doc, exclusions_count = self.exclude_fields(doc)
            processed_docs.append(processed_doc)
            stats.fields_excluded += exclusions_count
            stats.documents_processed += 1

        return processed_docs, stats

    def exclude_fields(self, doc: Dict[str, Any]) -> tuple[Dict[str, Any], int]:
        """Exclude fields from a single document.

        Args:
            doc: Document to process

        Returns:
            Tuple of (processed document, number of fields excluded)
        """
        # Work on a copy to avoid mutating the original
        processed = self._deep_copy(doc)
        exclusions_count = 0

        for field_path in self.fields_to_exclude:
            if self._remove_nested_field(processed, field_path):
                exclusions_count += 1

        return processed, exclusions_count

    def _remove_nested_field(self, doc: Dict[str, Any], field_path: str) -> bool:
        """Remove a nested field from a document.

        Implements "keep parent with remaining fields" logic:
        - Only removes the specified field
        - Keeps parent object if other fields exist
        - Removes parent only if it becomes empty after removal

        Args:
            doc: Document to modify (modified in-place)
            field_path: Field path to remove (e.g., "audit.raw")

        Returns:
            True if field was removed, False if field didn't exist
        """
        parts = field_path.split(".")

        # Navigate to parent of target field
        current = doc
        parent_chain = [(doc, None)]  # List of (parent, key) tuples

        for i, part in enumerate(parts[:-1]):
            if not isinstance(current, dict) or part not in current:
                # Path doesn't exist
                return False

            parent_chain.append((current, part))
            current = current[part]

        # Remove the target field
        final_key = parts[-1]
        if not isinstance(current, dict) or final_key not in current:
            return False

        del current[final_key]

        # Clean up empty parent objects (from bottom up)
        # Only remove parents that become empty after our deletion
        for i in range(len(parent_chain) - 1, 0, -1):
            parent, key = parent_chain[i]
            child = parent[key]

            # Only remove if the child is now an empty dict
            if isinstance(child, dict) and len(child) == 0:
                del parent[key]
            else:
                # Stop climbing if we hit a non-empty parent
                break

        return True

    def _deep_copy(self, obj: Any) -> Any:
        """Create a deep copy of a document.

        Args:
            obj: Object to copy

        Returns:
            Deep copy of the object
        """
        if isinstance(obj, dict):
            return {k: self._deep_copy(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_copy(item) for item in obj]
        else:
            # Primitive types and other objects are returned as-is
            # (MongoDB BSON types like ObjectId are immutable)
            return obj
