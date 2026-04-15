"""PII anonymization using Microsoft Presidio with custom operators.

This module provides anonymization capabilities that integrate Presidio's
AnonymizerEngine with custom operators for:
- Built-in Presidio operators (mask, hash, redact, replace, etc.)
- Custom Mimesis-based operators for realistic synthetic data generation
- Smart redaction that preserves format
- Multi-entity anonymization (applying multiple operators to same field)

The anonymization operators are configured via YAML (presidio.yaml).
"""

import copy
import logging
from typing import Any, Dict, List, Optional

from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig, RecognizerResult

from mongo_replication.config.presidio_config import PresidioConfig
from mongo_replication.engine.pii.custom_operators import CUSTOM_OPERATORS

logger = logging.getLogger(__name__)


class PresidioAnonymizer:
    """
    Handles PII anonymization using Presidio's AnonymizerEngine with custom operators.

    This class integrates Presidio's anonymization capabilities with custom operators
    for MongoDB document anonymization. It loads operator configurations from YAML
    and applies them to detected PII fields.
    """

    def __init__(
        self,
        presidio_config_path: Optional[str] = None,
        operator_overrides: Optional[Dict[str, OperatorConfig]] = None,
    ):
        """
        Initialize the anonymizer.

        Args:
            presidio_config_path: Optional path to custom Presidio YAML config.
                                 If None, uses bundled default configuration.
            operator_overrides: Optional operator config overrides (for testing).
        """
        # Initialize Presidio AnonymizerEngine
        self.anonymizer_engine = AnonymizerEngine()

        # Register custom operators
        for operator_class in CUSTOM_OPERATORS:
            self.anonymizer_engine.add_anonymizer(operator_class)
            logger.debug(f"Registered custom operator: {operator_class.__name__}")

        # Load operator configurations from YAML
        self.presidio_config = PresidioConfig(presidio_config_path)
        self.operator_configs = operator_overrides or self.presidio_config.get_operator_configs()

        logger.info(
            f"PresidioAnonymizer initialized with {len(self.operator_configs)} operator configs"
        )

    def apply_anonymization(
        self,
        document: Dict[str, Any],
        pii_field_strategy: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Apply anonymization to a document based on PII field strategy mappings.

        Args:
            document: The MongoDB document to anonymize
            pii_field_strategy: Field path to anonymization strategy mapping

        Returns:
            Anonymized document with PII fields redacted

        Example:
            >>> anonymizer = PresidioAnonymizer()
            >>> doc = {"email": "john@example.com", "ssn": "123-45-6789"}
            >>> pii_field_strategy = {"email": "smart_mask", "ssn": "hash"}
            >>> anonymized = anonymizer.apply_anonymization(doc, pii_field_strategy)
        """
        # Deep copy to avoid modifying original
        anonymized = copy.deepcopy(document)

        # Determine which fields to anonymize and with what operators
        field_operators = self._build_field_operators(pii_field_strategy)

        # Apply anonymization to each field
        for field_path, operator_config in field_operators.items():
            self._anonymize_field(anonymized, field_path, operator_config)

        return anonymized

    def apply_multi_entity_anonymization(
        self,
        document: Dict[str, Any],
        field_operators: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """
        Apply multi-entity anonymization to a document.

        Supports fields with multiple entity types by applying operators sequentially
        in order of detection confidence (operators should be pre-sorted).

        Args:
            document: The MongoDB document to anonymize
            field_operators: Dict mapping field paths to list of operator configs.
                Format: {
                    "field_path": [
                        {"operator": "mask_person", "params": {"entity_type": "PERSON"}},
                        {"operator": "mask_email", "params": {"entity_type": "EMAIL_ADDRESS"}}
                    ]
                }

        Returns:
            Anonymized document with PII fields redacted

        Example:
            >>> anonymizer = PresidioAnonymizer()
            >>> doc = {"contact": "John Smith john@example.com"}
            >>> field_operators = {
            ...     "contact": [
            ...         {"operator": "mask_person", "params": {"entity_type": "PERSON"}},
            ...         {"operator": "mask_email", "params": {"entity_type": "EMAIL_ADDRESS"}}
            ...     ]
            ... }
            >>> anonymized = anonymizer.apply_multi_entity_anonymization(doc, field_operators)
        """
        # Deep copy to avoid modifying original
        anonymized = copy.deepcopy(document)

        # Apply operators to each field (in order for multi-entity fields)
        for field_path, operators_list in field_operators.items():
            for operator_info in operators_list:
                operator_name = operator_info["operator"]
                params = operator_info.get("params")

                # Convert to OperatorConfig
                operator_config = self._build_operator_config(operator_name, params)
                if operator_config:
                    self._anonymize_field(anonymized, field_path, operator_config)

        return anonymized

    def anonymize_text(
        self,
        text: str,
        operator_name: str,
        entity_type: Optional[str] = None,
    ) -> str:
        """
        Anonymize a text value using a specific operator.

        This is a convenience method for anonymizing individual text values,
        primarily used for generating examples in scan reports.

        Args:
            text: The text to anonymize
            operator_name: The operator to use (e.g., 'mask_email', 'smart_mask')
            entity_type: Optional entity type (used by some operators)

        Returns:
            Anonymized text

        Example:
            >>> anonymizer = PresidioAnonymizer()
            >>> anonymizer.anonymize_text("john@example.com", "mask_email")
            'jo*@example.com'
        """
        if not text or not text.strip():
            return text

        # Build params with entity_type if provided
        params = {"entity_type": entity_type} if entity_type else None

        # Convert strategy name to operator config
        operator_config = self._build_operator_config(operator_name, params)
        if not operator_config:
            logger.warning(
                f"No operator config found for '{operator_name}', returning original text"
            )
            return text

        # Use the internal method to anonymize
        return self._anonymize_value(str(text), operator_config)
        return self._anonymize_value(str(text), operator_config)

    def _build_field_operators(
        self,
        pii_field_strategy: Optional[Dict[str, str]],
    ) -> Dict[str, OperatorConfig]:
        """
        Build mapping of field paths to OperatorConfig objects.

        Args:
            pii_field_strategy: Field path to strategy mapping

        Returns:
            Dictionary mapping field paths to OperatorConfig objects
        """
        field_operators: Dict[str, OperatorConfig] = {}

        # Apply PII field strategies
        if pii_field_strategy:
            for field_path, strategy_name in pii_field_strategy.items():
                if strategy_name is not None:
                    # Convert strategy name to operator config
                    operator_config = self._build_operator_config(strategy_name)
                    if operator_config:
                        field_operators[field_path] = operator_config

        return field_operators

    def _build_operator_config(
        self,
        operator_name: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[OperatorConfig]:
        """
        Convert a strategy name to an OperatorConfig.

        Strategy names can be:
        1. Operator names directly (e.g., "mask", "hash", "redact")
        2. Entity types from YAML config (e.g., "EMAIL_ADDRESS")

        Args:
            operator_name: The strategy name to convert
            params: Optional parameters dict to pass to the operator (may include 'entity_type')

        Returns:
            OperatorConfig object, or None if strategy not found
        """
        # Check if it's an entity type with configured operator
        if operator_name in self.operator_configs:
            return self.operator_configs[operator_name]

        # Use provided params or empty dict
        final_params = params.copy() if params else {}

        return OperatorConfig(operator_name, final_params)

    def _anonymize_field(
        self, document: Dict[str, Any], field_path: str, operator_config: OperatorConfig
    ) -> None:
        """
        Anonymize a specific field in a document using Presidio.

        This method treats each field value as text and uses Presidio's
        AnonymizerEngine to anonymize it.

        Args:
            document: The document to modify
            field_path: Dot-notation path to the field
            operator_config: The OperatorConfig to apply
        """
        # Handle nested fields with dot notation
        if "." in field_path:
            self._anonymize_nested_field(document, field_path, operator_config)
        elif field_path in document:
            # Get the field value
            value = document[field_path]
            # Anonymize it
            anonymized_value = self._anonymize_value(value, operator_config)
            # Update the document
            document[field_path] = anonymized_value

    def _anonymize_nested_field(
        self, document: Dict[str, Any], field_path: str, operator_config: OperatorConfig
    ) -> None:
        """
        Anonymize a nested field using dot notation.

        Supports array fields - e.g., "contacts.email" will anonymize email in all array elements.

        Args:
            document: The document containing the nested field
            field_path: Dot-separated path to the field (e.g., "address.street" or "contacts.email")
            operator_config: The OperatorConfig to apply
        """
        parts = field_path.split(".")
        self._anonymize_nested_recursive(document, parts, operator_config)

    def _anonymize_nested_recursive(
        self, current: Any, remaining_parts: list, operator_config: OperatorConfig
    ) -> None:
        """
        Recursively anonymize a nested field, handling arrays.

        Args:
            current: Current position in the document
            remaining_parts: Remaining parts of the field path
            operator_config: The OperatorConfig to apply
        """
        if not remaining_parts:
            return

        # Base case: we've reached the final field
        if len(remaining_parts) == 1:
            final_field = remaining_parts[0]

            if isinstance(current, dict) and final_field in current:
                current[final_field] = self._anonymize_value(current[final_field], operator_config)
            elif isinstance(current, list):
                # Apply to all elements in the array
                for item in current:
                    if isinstance(item, dict) and final_field in item:
                        item[final_field] = self._anonymize_value(
                            item[final_field], operator_config
                        )
            return

        # Recursive case: navigate deeper
        next_part = remaining_parts[0]
        rest = remaining_parts[1:]

        if isinstance(current, dict) and next_part in current:
            next_value = current[next_part]

            if isinstance(next_value, list):
                # Apply to all elements in the array
                for item in next_value:
                    self._anonymize_nested_recursive(item, rest, operator_config)
            else:
                # Continue with single object
                self._anonymize_nested_recursive(next_value, rest, operator_config)

    def _anonymize_value(self, value: Any, operator_config: OperatorConfig) -> Any:
        """
        Anonymize a single value using Presidio's AnonymizerEngine.

        This method treats the value as text and creates a synthetic RecognizerResult
        that spans the entire value, then uses Presidio to anonymize it.

        Args:
            value: The value to anonymize
            operator_config: The OperatorConfig to apply

        Returns:
            Anonymized value
        """
        if value is None:
            return None

        # Convert value to string for anonymization
        text = str(value)

        if not text.strip():
            return value

        # Create a synthetic RecognizerResult that spans the entire text
        # This allows us to use Presidio's anonymize() method for field-level anonymization
        recognizer_result = RecognizerResult(
            entity_type="PII",  # Generic entity type
            start=0,
            end=len(text),
            score=1.0,
        )

        try:
            # Use Presidio's anonymize method with our operator config
            result = self.anonymizer_engine.anonymize(
                text=text,
                analyzer_results=[recognizer_result],
                operators={"PII": operator_config},  # Map our generic entity to the operator
            )

            return result.text

        except Exception as e:
            logger.error(
                f"Error anonymizing value with operator '{operator_config.operator_name}': {e}"
            )
            # Return original value on error to avoid data loss
            return value


# Singleton instance for easy import (uses default config)
_default_anonymizer = None


def get_anonymizer(presidio_config_path: Optional[str] = None) -> PresidioAnonymizer:
    """
    Get or create a PresidioAnonymizer instance.

    Args:
        presidio_config_path: Optional path to custom Presidio configuration

    Returns:
        PresidioAnonymizer instance
    """
    global _default_anonymizer

    if presidio_config_path:
        # Return new instance with custom config
        return PresidioAnonymizer(presidio_config_path=presidio_config_path)

    # Return singleton with default config
    if _default_anonymizer is None:
        _default_anonymizer = PresidioAnonymizer()

    return _default_anonymizer


def apply_anonymization(
    document: Dict[str, Any],
    pii_field_strategy: Optional[Dict[str, str]] = None,
    presidio_config_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convenience function to anonymize a document.

    Args:
        document: The MongoDB document to anonymize
        pii_field_strategy: Optional field path to strategy mapping
        presidio_config_path: Optional path to custom Presidio configuration

    Returns:
        Anonymized document
    """
    anonymizer = get_anonymizer(presidio_config_path)
    return anonymizer.apply_anonymization(document, pii_field_strategy)
