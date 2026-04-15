"""PII handler for manual PII field anonymization.

This module provides PII handling with manual field configuration using Presidio.
Use the scan command to generate PII field configurations.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class PIIHandler:
    """
    PII handler for manual field-based anonymization using Presidio.

    Supports multi-entity anonymization where a single field can have multiple
    entity types (e.g., a field containing both PERSON and EMAIL_ADDRESS).
    """

    def __init__(
        self,
        pii_anonymization: Optional[Union[List, Dict[str, str]]] = None,
    ):
        """
        Initialize PII handler.

        Args:
            pii_anonymization: Either:
                - List[PIIFieldAnonymization]: New format supporting multi-entity (preferred)
                - Dict[str, str]: Legacy format (field->operator mapping)
        """
        # Normalize to internal format: Dict[field_path, List[Dict[operator, params]]]
        self.field_operators: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        if pii_anonymization:
            if isinstance(pii_anonymization, dict):
                # Legacy dict format: field -> operator
                for field, operator in pii_anonymization.items():
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

                    self.field_operators[field].append({"operator": operator, "params": params})

    @property
    def pii_field_count(self) -> int:
        """Return the number of unique fields being anonymized."""
        return len(self.field_operators)

    def process_documents(
        self,
        documents: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Process documents with PII anonymization.

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
            # Apply multi-entity anonymization
            redacted_doc = anonymizer.apply_multi_entity_anonymization(
                document=doc,
                field_operators=self.field_operators,
            )
            redacted.append(redacted_doc)

        return redacted


def create_pii_handler_from_config(pii_anonymization: Union[List, Dict[str, str]]) -> PIIHandler:
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
