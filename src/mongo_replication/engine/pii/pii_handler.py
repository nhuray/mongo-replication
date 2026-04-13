"""PII handler for manual PII field anonymization.

This module provides PII handling with manual field configuration using Presidio.
Use the scan command to generate PII field configurations.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PIIHandler:
    """
    PII handler for manual field-based anonymization using Presidio.
    """

    def __init__(
        self,
        pii_fields: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize PII handler.

        Args:
            pii_fields: Manual field->strategy mappings for anonymization
        """
        self.pii_fields = pii_fields or {}

    def process_documents(
        self,
        documents: List[Dict[str, Any]],
        pii_fields: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Process documents with PII anonymization.

        Args:
            documents: List of documents to process
            pii_fields: Manual field->strategy mappings (overrides instance manual_pii_fields if provided)

        Returns:
            List of documents with PII anonymized
        """
        if not documents:
            return documents

        # Use provided manual fields, or fall back to instance manual fields
        pii_fields = pii_fields if pii_fields is not None else self.pii_fields

        return self._apply_manual_redaction(documents, pii_fields)

    def _apply_manual_redaction(
        self,
        documents: List[Dict[str, Any]],
        manual_pii_fields: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Apply manual PII redaction using Presidio anonymizer."""
        if not manual_pii_fields:
            return documents

        from mongo_replication.engine.pii.presidio_anonymizer import apply_anonymization

        redacted = []
        for doc in documents:
            # Use apply_anonymization with PII field strategy
            redacted_doc = apply_anonymization(
                document=doc,
                pii_field_strategy=manual_pii_fields,
            )
            redacted.append(redacted_doc)

        return redacted


def create_pii_handler_from_config(pii_fields: Dict[str, str]) -> PIIHandler:
    """
    Create a PII handler from collection configuration.

    Args:
        pii_fields: PII Fields

    Returns:
        Configured PIIHandler instance
    """
    return PIIHandler(
        pii_fields=pii_fields,
    )
