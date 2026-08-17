"""PII handler for manual PII field anonymization.

This module provides PII handling with manual field configuration using Presidio.
Use the scan command to generate PII field configurations.

Supports wildcard patterns in field specifications:
- "metadata.*" matches all fields under metadata
- "*.email" matches all fields ending with email
- "user.*.sinNumber" matches user.<anything>.sinNumber
"""

import logging
from collections import defaultdict
from typing import Any

from mongo_replication.engine.pii.field_utils import get_all_field_paths, matches_pattern

logger = logging.getLogger(__name__)


class PIIHandler:
    """
    PII handler for manual field-based anonymization using Presidio.

    Supports multi-entity anonymization where a single field can have multiple
    entity types (e.g., a field containing both PERSON and EMAIL_ADDRESS).

    Also supports wildcard patterns in field specifications:
    - "metadata.*" matches all fields under metadata
    - "*.email" matches all fields ending with email
    - "user.*.sinNumber" matches user.<anything>.sinNumber
    """

    def __init__(
        self,
        pii_anonymization: list | dict[str, str] | None = None,
    ):
        """
        Initialize PII handler.

        Args:
            pii_anonymization: Either:
                - List[PIIFieldAnonymization]: New format supporting multi-entity (preferred)
                - Dict[str, str]: Legacy format (field->operator mapping)

                Field paths can include wildcard patterns using '*':
                - "user.email" - exact match
                - "user.*" - all fields under user
                - "*.email" - all fields ending with .email
                - "user.*.email" - user.<anything>.email
        """
        # Normalize to internal format: Dict[field_path, List[Dict[operator, params]]]
        # This stores both exact matches and wildcard patterns
        self.field_operators: dict[str, list[dict[str, Any]]] = defaultdict(list)

        # Track which patterns contain wildcards for optimization
        self._has_wildcard_patterns = False

        if pii_anonymization:
            if isinstance(pii_anonymization, dict):
                # Legacy dict format: field -> operator
                for field, operator in pii_anonymization.items():
                    if "*" in field:
                        self._has_wildcard_patterns = True
                    self.field_operators[field].append(
                        {
                            "operator": operator,
                            "params": None,  # No params in legacy format
                        }
                    )
            elif isinstance(pii_anonymization, list):
                # New list format: List[PIIFieldAnonymization]
                # Sort by confidence (highest first) - assuming list is pre-sorted from scan
                for item in pii_anonymization:
                    # Handle both PIIFieldAnonymization objects and dicts
                    if hasattr(item, "field"):
                        field = item.field
                        operator = item.operator
                        params = getattr(item, "params", None)
                    else:
                        field = item["field"]
                        operator = item["operator"]
                        params = item.get("params")

                    if "*" in field:
                        self._has_wildcard_patterns = True
                    self.field_operators[field].append({"operator": operator, "params": params})

    @property
    def pii_field_count(self) -> int:
        """Return the number of unique fields being anonymized."""
        return len(self.field_operators)

    def process_documents(
        self,
        documents: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Process documents with PII anonymization.

        If wildcard patterns are present, this method will expand them to concrete
        field paths for each document based on the document's schema.

        Args:
            documents: List of documents to process

        Returns:
            List of documents with PII anonymized
        """
        if not documents:
            return documents

        if not self.field_operators:
            return documents

        from mongo_replication.engine.pii.presidio_anonymizer import get_anonymizer

        # Get anonymizer instance
        anonymizer = get_anonymizer()

        redacted = []
        for doc in documents:
            # Expand wildcard patterns to concrete field paths for this document
            expanded_operators = (
                self._expand_patterns(doc) if self._has_wildcard_patterns else self.field_operators
            )

            # Apply multi-entity anonymization
            redacted_doc = anonymizer.apply_multi_entity_anonymization(
                document=doc,
                field_operators=expanded_operators,
            )
            redacted.append(redacted_doc)

        return redacted

    def _expand_patterns(self, document: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        """
        Expand wildcard patterns to concrete field paths for a document.

        This method examines the document's schema and matches wildcard patterns
        against all available field paths, expanding them to concrete paths.

        Args:
            document: The document to expand patterns for

        Returns:
            Dict mapping concrete field paths to operator configurations

        Example:
            If field_operators contains {"user.*.email": [{"operator": "mask_email"}]}
            and document has fields ["user.profile.email", "user.account.email"],
            this returns:
            {
                "user.profile.email": [{"operator": "mask_email"}],
                "user.account.email": [{"operator": "mask_email"}]
            }
        """
        expanded = defaultdict(list)

        # Get all field paths in the document
        all_fields = get_all_field_paths(document)

        # Process each pattern
        for pattern, operators in self.field_operators.items():
            if "*" in pattern:
                # Wildcard pattern - match against all fields
                for field in all_fields:
                    if matches_pattern(field, pattern):
                        # Add operators for this matched field
                        expanded[field].extend(operators)
            else:
                # Exact match - use as-is (fast path)
                expanded[pattern].extend(operators)

        return dict(expanded)


def create_pii_handler_from_config(pii_anonymization: list | dict[str, str]) -> PIIHandler:
    """
    Create a PII handler from collection configuration.

    Args:
        pii_anonymization: Either:
            - List[PIIFieldAnonymization]: New format supporting multi-entity
            - Dict[str, str]: Legacy format (field->operator mapping)

    Returns:
        Configured PIIHandler instance
    """
    return PIIHandler(pii_anonymization=pii_anonymization)
