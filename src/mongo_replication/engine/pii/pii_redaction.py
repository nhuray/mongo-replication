"""PII redaction utilities using Mimesis for synthetic data generation."""

import hashlib
from typing import Any, Callable, Dict

from mimesis import Person, Address

# Initialize Mimesis providers
_person = Person()
_address = Address()


class PIIRedactor:
    """Handles PII redaction using various strategies."""

    def __init__(self):
        """Initialize the PII redactor with Mimesis providers."""
        self.person = Person()
        self.address = Address()
        self._strategy_map: Dict[str, Callable[[Any], Any]] = {
            "fake_email": self._fake_email,
            "fake_phone": self._fake_phone,
            "fake_name": self._fake_name,
            "fake_address": self._fake_address,
            "redact": self._redact,
            "hash": self._hash,
            "null": self._null,
        }

    def redact_document(self, document: Dict[str, Any], pii_fields: Dict[str, str]) -> Dict[str, Any]:
        """
        Redact PII fields in a document according to the specified strategies.

        Args:
            document: The document to redact
            pii_fields: Dictionary mapping field names to redaction strategies

        Returns:
            Document with PII fields redacted
        """
        redacted = document.copy()

        for field, strategy in pii_fields.items():
            if field in redacted:
                redacted[field] = self.apply_strategy(redacted[field], strategy)
            # Handle nested fields with dot notation (e.g., "address.street")
            elif "." in field:
                self._redact_nested_field(redacted, field, strategy)

        return redacted

    def apply_strategy(self, value: Any, strategy: str) -> Any:
        """
        Apply a redaction strategy to a value.

        Args:
            value: The value to redact
            strategy: The redaction strategy to apply

        Returns:
            Redacted value

        Raises:
            ValueError: If strategy is not recognized
        """
        if strategy not in self._strategy_map:
            raise ValueError(
                f"Unknown PII redaction strategy: {strategy}. "
                f"Available strategies: {', '.join(self._strategy_map.keys())}"
            )

        strategy_func = self._strategy_map[strategy]
        return strategy_func(value)

    def _redact_nested_field(self, document: Dict[str, Any], field_path: str, strategy: str) -> None:
        """
        Redact a nested field using dot notation.
        
        Supports array fields - e.g., "contacts.email" will redact email in all array elements.

        Args:
            document: The document containing the nested field
            field_path: Dot-separated path to the field (e.g., "address.street" or "contacts.email")
            strategy: The redaction strategy to apply
        """
        parts = field_path.split(".")
        self._redact_nested_field_recursive(document, parts, strategy)

    def _redact_nested_field_recursive(
        self, 
        current: Any, 
        remaining_parts: list, 
        strategy: str
    ) -> None:
        """
        Recursively redact a nested field, handling arrays.

        Args:
            current: Current position in the document
            remaining_parts: Remaining parts of the field path
            strategy: The redaction strategy to apply
        """
        if not remaining_parts:
            return

        # Base case: we've reached the final field
        if len(remaining_parts) == 1:
            final_field = remaining_parts[0]
            
            if isinstance(current, dict) and final_field in current:
                current[final_field] = self.apply_strategy(current[final_field], strategy)
            elif isinstance(current, list):
                # Apply to all elements in the array
                for item in current:
                    if isinstance(item, dict) and final_field in item:
                        item[final_field] = self.apply_strategy(item[final_field], strategy)
            return

        # Recursive case: navigate deeper
        next_part = remaining_parts[0]
        rest = remaining_parts[1:]

        if isinstance(current, dict) and next_part in current:
            next_value = current[next_part]
            
            if isinstance(next_value, list):
                # Apply to all elements in the array
                for item in next_value:
                    self._redact_nested_field_recursive(item, rest, strategy)
            else:
                # Continue with single object
                self._redact_nested_field_recursive(next_value, rest, strategy)

    # Redaction strategy implementations

    def _fake_email(self, value: Any) -> str:
        """Generate a fake email address."""
        return self.person.email()

    def _fake_phone(self, value: Any) -> str:
        """Generate a fake phone number."""
        return self.person.phone_number()

    def _fake_name(self, value: Any) -> str:
        """Generate a fake full name."""
        return self.person.full_name()

    def _fake_address(self, value: Any) -> str:
        """Generate a fake address."""
        return self.address.address()

    def _redact(self, value: Any) -> str:
        """
        Partially redact a value, showing only the first few characters.

        For strings, shows first 4 chars followed by asterisks.
        """
        if value is None:
            return "***"

        value_str = str(value)
        if len(value_str) <= 4:
            return "***"

        return value_str[:4] + "***"

    def _hash(self, value: Any) -> str:
        """
        Hash a value using SHA-256.

        Preserves referential integrity - same input always produces same hash.
        """
        if value is None:
            return hashlib.sha256(b"").hexdigest()

        value_str = str(value)
        return hashlib.sha256(value_str.encode("utf-8")).hexdigest()

    def _null(self, value: Any) -> None:
        """Replace value with None."""
        return None


# Singleton instance for easy import
redactor = PIIRedactor()


def redact_document(document: Dict[str, Any], pii_fields: Dict[str, str]) -> Dict[str, Any]:
    """
    Convenience function to redact a document.

    Args:
        document: The document to redact
        pii_fields: Dictionary mapping field names to redaction strategies

    Returns:
        Document with PII fields redacted
    """
    return redactor.redact_document(document, pii_fields)
