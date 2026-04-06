"""Field transformation engine for MongoDB documents.

Supports regex-based transformations on document fields with statistics tracking
and configurable error handling.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from mongo_replication.config.models import FieldTransformConfig

logger = logging.getLogger(__name__)


@dataclass
class TransformStats:
    """Statistics for field transformations."""

    documents_processed: int = 0
    total_transforms: int = 0
    successful_transforms: int = 0
    failed_transforms: int = 0


class FieldTransformer:
    """Applies field transformations to MongoDB documents.

    Supports regex-based replacements on top-level and nested fields.
    Transformations are applied in order, allowing for chained transforms.
    """

    def __init__(self, transforms: List[FieldTransformConfig], error_mode: str = "skip"):
        """Initialize the field transformer.

        Args:
            transforms: List of field transformation configurations
            error_mode: Error handling mode - "skip" or "fail"
        """
        self.transforms = transforms
        self.error_mode = error_mode

        # Pre-compile regex patterns for performance
        self._compiled_patterns = {}
        for transform in transforms:
            key = (transform.field, transform.pattern)
            if key not in self._compiled_patterns:
                self._compiled_patterns[key] = re.compile(transform.pattern)

    def transform_documents(
        self, documents: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], TransformStats]:
        """Transform a batch of documents.

        Args:
            documents: List of documents to transform

        Returns:
            Tuple of (transformed documents, statistics)

        Raises:
            ValueError: If error_mode is "fail" and a transformation fails
        """
        if not self.transforms:
            # No transforms configured - return documents unchanged
            stats = TransformStats(documents_processed=len(documents))
            return documents, stats

        stats = TransformStats()
        transformed_docs = []

        for doc in documents:
            try:
                transformed_doc, doc_stats = self.transform_document(doc)
                transformed_docs.append(transformed_doc)

                # Aggregate statistics
                stats.total_transforms += doc_stats.total_transforms
                stats.successful_transforms += doc_stats.successful_transforms
                stats.failed_transforms += doc_stats.failed_transforms

            except Exception as e:
                if self.error_mode == "fail":
                    raise ValueError(
                        f"Field transformation failed for document {doc.get('_id')}: {e}"
                    ) from e
                else:
                    # Skip mode - log warning and use original document
                    logger.warning(
                        f"Field transformation failed for document {doc.get('_id')}, "
                        f"using original document: {e}"
                    )
                    transformed_docs.append(doc)
                    stats.failed_transforms += len(self.transforms)

            stats.documents_processed += 1

        return transformed_docs, stats

    def transform_document(self, doc: Dict[str, Any]) -> Tuple[Dict[str, Any], TransformStats]:
        """Transform a single document.

        Args:
            doc: Document to transform

        Returns:
            Tuple of (transformed document, statistics)
        """
        # Work on a copy to avoid mutating the original
        transformed = doc.copy()
        stats = TransformStats()

        # Apply each transformation in order
        for transform in self.transforms:
            stats.total_transforms += 1

            try:
                success = self._apply_regex_replace(
                    transformed, transform.field, transform.pattern, transform.replacement
                )

                if success:
                    stats.successful_transforms += 1
                else:
                    stats.failed_transforms += 1

            except Exception as e:
                stats.failed_transforms += 1
                if self.error_mode == "fail":
                    raise ValueError(
                        f"Failed to apply transform to field '{transform.field}': {e}"
                    ) from e
                else:
                    logger.warning(
                        f"Failed to apply transform to field '{transform.field}' "
                        f"in document {doc.get('_id')}: {e}"
                    )

        return transformed, stats

    def _apply_regex_replace(
        self, doc: Dict[str, Any], field_path: str, pattern: str, replacement: str
    ) -> bool:
        """Apply regex replacement to a field.

        Supports nested fields using dot notation (e.g., "company.domain").

        Args:
            doc: Document to modify (modified in-place)
            field_path: Path to field (supports dot notation)
            pattern: Regex pattern to match
            replacement: Replacement string

        Returns:
            True if transformation was applied, False if field not found or not a string
        """
        # Get current value
        value = self._get_nested_field(doc, field_path)

        if value is None:
            # Field doesn't exist - skip silently
            return False

        if not isinstance(value, str):
            # Can only transform string fields
            logger.debug(
                f"Skipping transformation for field '{field_path}': "
                f"value is {type(value).__name__}, not str"
            )
            return False

        # Apply regex replacement
        compiled_pattern = self._compiled_patterns.get((field_path, pattern))
        if not compiled_pattern:
            compiled_pattern = re.compile(pattern)

        new_value = compiled_pattern.sub(replacement, value)

        # Only update if value changed
        if new_value != value:
            self._set_nested_field(doc, field_path, new_value)
            return True

        return False

    def _get_nested_field(self, doc: Dict[str, Any], field_path: str) -> Any:
        """Get value of a nested field using dot notation.

        Args:
            doc: Document to read from
            field_path: Field path (e.g., "company.domain")

        Returns:
            Field value, or None if not found
        """
        parts = field_path.split(".")
        current = doc

        for part in parts:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None

        return current

    def _set_nested_field(self, doc: Dict[str, Any], field_path: str, value: Any) -> None:
        """Set value of a nested field using dot notation.

        Creates intermediate dictionaries if needed.

        Args:
            doc: Document to modify
            field_path: Field path (e.g., "company.domain")
            value: Value to set
        """
        parts = field_path.split(".")
        current = doc

        # Navigate to parent of target field, creating dicts as needed
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                # Can't traverse through non-dict value
                raise ValueError(
                    f"Cannot set nested field '{field_path}': '{part}' is not a dictionary"
                )
            current = current[part]

        # Set the final field
        current[parts[-1]] = value
