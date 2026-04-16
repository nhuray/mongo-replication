"""Unified transformation engine for MongoDB documents.

Supports multiple transformation types including field operations, regex replacements,
and PII anonymization with statistics tracking and configurable error handling.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from bson import ObjectId
from pydantic import BaseModel

from mongo_replication.config.models import (
    AddFieldTransform,
    AnonymizeTransform,
    ConditionConfig,
    CopyFieldTransform,
    RegexReplaceTransform,
    RemoveFieldTransform,
    RenameFieldTransform,
    SetFieldTransform,
    TransformConfig,
)

logger = logging.getLogger(__name__)


class TransformStats(BaseModel):
    """Statistics for transformations."""

    documents_processed: int = 0
    documents_failed: int = 0
    transforms_applied: int = 0
    transforms_skipped: int = 0


class TransformationError(Exception):
    """Error during transformation processing."""

    pass


class TransformationEngine:
    """Unified transformation engine for all document transformations.

    Processes documents through a pipeline of transformations including:
    - Field operations (add, set, remove, rename, copy)
    - Regex replacements
    - PII anonymization

    Transformations are applied sequentially in the order defined in configuration.
    """

    def __init__(
        self,
        transforms: List[TransformConfig],
        error_mode: str = "skip",
    ):
        """Initialize the transformation engine.

        Args:
            transforms: List of transformation configurations
            error_mode: Error handling mode - "skip" or "fail"
        """
        self.transforms = transforms
        self.error_mode = error_mode

        # Pre-compile regex patterns for performance
        self._compiled_patterns = {}
        for transform in transforms:
            if isinstance(transform, RegexReplaceTransform):
                key = (transform.field, transform.pattern)
                if key not in self._compiled_patterns:
                    self._compiled_patterns[key] = re.compile(transform.pattern)

        # Initialize PII handler if any anonymize transforms exist
        self.pii_handler = None
        anonymize_transforms = [t for t in transforms if isinstance(t, AnonymizeTransform)]
        if anonymize_transforms:
            self.pii_handler = self._create_pii_handler(anonymize_transforms)

    def _create_pii_handler(self, anonymize_transforms: List[AnonymizeTransform]):
        """Create PII handler from anonymize transforms.

        Args:
            anonymize_transforms: List of anonymize transform configs

        Returns:
            PIIHandler instance
        """
        from mongo_replication.engine.pii import create_pii_handler_from_config
        from mongo_replication.config.models import PIIFieldAnonymization

        # Convert AnonymizeTransform to PIIFieldAnonymization for compatibility
        pii_configs = [
            PIIFieldAnonymization(
                field=t.field,
                operator=t.operator,
                params=t.params or {},
            )
            for t in anonymize_transforms
        ]

        return create_pii_handler_from_config(pii_configs)

    def transform_documents(
        self, documents: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], TransformStats]:
        """Transform batch of documents.

        Args:
            documents: List of documents to transform

        Returns:
            Tuple of (transformed documents, statistics)
        """
        transformed = []
        stats = TransformStats()

        for doc in documents:
            try:
                transformed_doc = self.transform_document(doc)
                transformed.append(transformed_doc)
                stats.documents_processed += 1
                # Each document goes through all transforms (even if conditions skip some)
                stats.transforms_applied += len(self.transforms)
            except Exception as e:
                if self.error_mode == "fail":
                    raise TransformationError(f"Transform failed: {e}") from e
                logger.warning(f"Transform failed, using original document: {e}")
                transformed.append(doc)
                stats.documents_failed += 1

        return transformed, stats

    def transform_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Transform single document through all transformations.

        Args:
            doc: Document to transform

        Returns:
            Transformed document
        """
        # Deep copy to avoid mutating original
        result = self._deep_copy(doc)

        for transform in self.transforms:
            # Check condition if present
            if transform.condition:
                if not self._evaluate_condition(result, transform.condition):
                    continue

            # Apply transformation based on type
            result = self._apply_transform(result, transform)

        return result

    def _apply_transform(self, doc: Dict[str, Any], transform: TransformConfig) -> Dict[str, Any]:
        """Apply single transform to document.

        Args:
            doc: Document to transform
            transform: Transform configuration

        Returns:
            Transformed document
        """
        if isinstance(transform, AddFieldTransform):
            return self._add_field(doc, transform)
        elif isinstance(transform, SetFieldTransform):
            return self._set_field(doc, transform)
        elif isinstance(transform, RemoveFieldTransform):
            return self._remove_field(doc, transform)
        elif isinstance(transform, RenameFieldTransform):
            return self._rename_field(doc, transform)
        elif isinstance(transform, CopyFieldTransform):
            return self._copy_field(doc, transform)
        elif isinstance(transform, RegexReplaceTransform):
            return self._regex_replace(doc, transform)
        elif isinstance(transform, AnonymizeTransform):
            return self._anonymize(doc, transform)
        else:
            raise ValueError(f"Unknown transform type: {type(transform)}")

    # =============================================================================
    # CONDITION EVALUATION
    # =============================================================================

    def _evaluate_condition(self, doc: Dict[str, Any], condition: ConditionConfig) -> bool:
        """Evaluate if condition matches for document.

        Args:
            doc: Document to check
            condition: Condition configuration

        Returns:
            True if condition matches, False otherwise
        """
        field_value = self._get_nested_field(doc, condition.field)

        if condition.operator == "$exists":
            return (field_value is not None) == condition.value
        elif condition.operator == "$eq":
            return field_value == condition.value
        elif condition.operator == "$ne":
            return field_value != condition.value
        elif condition.operator == "$gt":
            return field_value is not None and field_value > condition.value
        elif condition.operator == "$gte":
            return field_value is not None and field_value >= condition.value
        elif condition.operator == "$lt":
            return field_value is not None and field_value < condition.value
        elif condition.operator == "$lte":
            return field_value is not None and field_value <= condition.value
        elif condition.operator == "$in":
            return field_value in condition.value
        elif condition.operator == "$nin":
            return field_value not in condition.value
        else:
            raise ValueError(f"Unknown condition operator: {condition.operator}")

    # =============================================================================
    # FIELD OPERATIONS
    # =============================================================================

    def _add_field(self, doc: Dict[str, Any], transform: AddFieldTransform) -> Dict[str, Any]:
        """Add new field with value (error if exists).

        Args:
            doc: Document to transform
            transform: Add field configuration

        Returns:
            Transformed document
        """
        # Check if field already exists
        existing_value = self._get_nested_field(doc, transform.field)
        if existing_value is not None:
            raise TransformationError(f"Cannot add field '{transform.field}': field already exists")

        # Resolve value from template or literal
        resolved_value = self._resolve_value(doc, transform.value)

        # Set field
        self._set_nested_field(doc, transform.field, resolved_value)

        return doc

    def _set_field(self, doc: Dict[str, Any], transform: SetFieldTransform) -> Dict[str, Any]:
        """Set field value (overwrites if exists).

        Args:
            doc: Document to transform
            transform: Set field configuration

        Returns:
            Transformed document
        """
        # Resolve value from template or literal
        resolved_value = self._resolve_value(doc, transform.value)

        # Set field (overwrites if exists)
        self._set_nested_field(doc, transform.field, resolved_value)

        return doc

    def _remove_field(self, doc: Dict[str, Any], transform: RemoveFieldTransform) -> Dict[str, Any]:
        """Remove one or more fields.

        Args:
            doc: Document to transform
            transform: Remove field configuration

        Returns:
            Transformed document
        """
        fields = transform.field if isinstance(transform.field, list) else [transform.field]

        for field in fields:
            self._delete_nested_field(doc, field)

        return doc

    def _rename_field(self, doc: Dict[str, Any], transform: RenameFieldTransform) -> Dict[str, Any]:
        """Rename a field.

        Args:
            doc: Document to transform
            transform: Rename field configuration

        Returns:
            Transformed document
        """
        # Get source value
        source_value = self._get_nested_field(doc, transform.from_field)
        if source_value is None:
            # Source field doesn't exist, nothing to rename
            return doc

        # Check if target exists
        target_value = self._get_nested_field(doc, transform.to_field)
        if target_value is not None and not transform.overwrite:
            raise TransformationError(
                f"Cannot rename '{transform.from_field}' to '{transform.to_field}': "
                f"target field already exists"
            )

        # Set target field
        self._set_nested_field(doc, transform.to_field, source_value)

        # Delete source field
        self._delete_nested_field(doc, transform.from_field)

        return doc

    def _copy_field(self, doc: Dict[str, Any], transform: CopyFieldTransform) -> Dict[str, Any]:
        """Copy field value to another field.

        Args:
            doc: Document to transform
            transform: Copy field configuration

        Returns:
            Transformed document
        """
        # Get source value
        source_value = self._get_nested_field(doc, transform.from_field)
        if source_value is None:
            # Source field doesn't exist, nothing to copy
            return doc

        # Check if target exists
        target_value = self._get_nested_field(doc, transform.to_field)
        if target_value is not None and not transform.overwrite:
            raise TransformationError(
                f"Cannot copy '{transform.from_field}' to '{transform.to_field}': "
                f"target field already exists"
            )

        # Copy value to target field
        self._set_nested_field(doc, transform.to_field, self._deep_copy(source_value))

        return doc

    # =============================================================================
    # REGEX REPLACEMENT
    # =============================================================================

    def _regex_replace(
        self, doc: Dict[str, Any], transform: RegexReplaceTransform
    ) -> Dict[str, Any]:
        """Apply regex pattern replacement to field.

        Args:
            doc: Document to transform
            transform: Regex replace configuration

        Returns:
            Transformed document
        """
        field_value = self._get_nested_field(doc, transform.field)

        # Skip if field doesn't exist or isn't a string
        if field_value is None or not isinstance(field_value, str):
            return doc

        # Get pre-compiled pattern
        pattern_key = (transform.field, transform.pattern)
        compiled_pattern = self._compiled_patterns[pattern_key]

        # Apply replacement
        new_value = compiled_pattern.sub(transform.replacement, field_value)

        # Set new value if changed
        if new_value != field_value:
            self._set_nested_field(doc, transform.field, new_value)

        return doc

    # =============================================================================
    # PII ANONYMIZATION
    # =============================================================================

    def _anonymize(self, doc: Dict[str, Any], transform: AnonymizeTransform) -> Dict[str, Any]:
        """Apply PII anonymization to field.

        Args:
            doc: Document to transform
            transform: Anonymize configuration

        Returns:
            Transformed document
        """
        if not self.pii_handler:
            raise TransformationError("PII handler not initialized")

        # Process entire document through PII handler
        # The handler will only anonymize fields it's configured for
        processed = self.pii_handler.process_documents([doc])

        return processed[0] if processed else doc

    # =============================================================================
    # VALUE RESOLUTION
    # =============================================================================

    def _resolve_value(self, doc: Dict[str, Any], value: Any) -> Any:
        """Resolve value from template or literal.

        Args:
            doc: Document context for field references
            value: Value to resolve (literal or template)

        Returns:
            Resolved value
        """
        # Special values
        if value == "$now":
            return datetime.utcnow()
        elif value == "$null":
            return None

        # Template strings (contain $)
        if isinstance(value, str) and "$" in value:
            return self._resolve_template(doc, value)

        # Literal values
        return value

    def _resolve_template(self, doc: Dict[str, Any], template: str) -> Any:
        """Resolve template string with field references.

        Supports:
        - Single field reference: "$fieldName"
        - Nested field reference: "$address.city"
        - Concatenation: "$firstName $lastName"

        Args:
            doc: Document context for field references
            template: Template string

        Returns:
            Resolved value
        """
        # Single field reference: "$fieldName" (no spaces)
        if template.startswith("$") and " " not in template:
            field_name = template[1:]
            return self._get_nested_field(doc, field_name)

        # Concatenation: "$field1 $field2"
        parts = template.split()
        resolved_parts = []
        for part in parts:
            if part.startswith("$"):
                field_name = part[1:]
                field_value = self._get_nested_field(doc, field_name)
                resolved_parts.append(str(field_value) if field_value is not None else "")
            else:
                resolved_parts.append(part)
        return " ".join(resolved_parts)

    # =============================================================================
    # NESTED FIELD UTILITIES
    # =============================================================================

    def _get_nested_field(self, doc: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path using dot notation.

        Args:
            doc: Document to navigate
            field_path: Field path (e.g., "address.city")

        Returns:
            Field value or None if not found
        """
        parts = field_path.split(".")
        current = doc

        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]

        return current

    def _set_nested_field(self, doc: Dict[str, Any], field_path: str, value: Any) -> None:
        """Set value at nested field path, creating intermediate dicts as needed.

        Args:
            doc: Document to modify
            field_path: Field path (e.g., "address.city")
            value: Value to set
        """
        parts = field_path.split(".")
        current = doc

        # Navigate to parent, creating intermediate dicts
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                # Can't navigate further, parent is not a dict
                raise TransformationError(
                    f"Cannot set field '{field_path}': "
                    f"'{'.'.join(parts[: parts.index(part) + 1])}' is not a dictionary"
                )
            current = current[part]

        # Set final value
        current[parts[-1]] = value

    def _delete_nested_field(self, doc: Dict[str, Any], field_path: str) -> None:
        """Delete field at nested path, cleaning up empty parent objects.

        Args:
            doc: Document to modify
            field_path: Field path (e.g., "address.city")
        """
        parts = field_path.split(".")
        current = doc

        # Navigate to parent
        parents = []
        for part in parts[:-1]:
            if not isinstance(current, dict) or part not in current:
                # Field doesn't exist, nothing to delete
                return
            parents.append((current, part))
            current = current[part]

        # Delete field
        if isinstance(current, dict) and parts[-1] in current:
            del current[parts[-1]]

            # Clean up empty parents
            for parent, key in reversed(parents):
                if not parent[key]:  # Empty dict
                    del parent[key]
                else:
                    break

    def _deep_copy(self, value: Any) -> Any:
        """Create deep copy of value.

        Handles BSON types (ObjectId, datetime) which are immutable.

        Args:
            value: Value to copy

        Returns:
            Deep copy of value
        """
        if isinstance(value, (ObjectId, datetime)):
            # BSON types are immutable, return as-is
            return value
        elif isinstance(value, dict):
            return {k: self._deep_copy(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._deep_copy(item) for item in value]
        else:
            # Primitives and other immutable types
            return value
