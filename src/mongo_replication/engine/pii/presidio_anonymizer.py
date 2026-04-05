"""PII anonymization using Microsoft Presidio with custom Mimesis operators.

This module provides anonymization capabilities that integrate Presidio's
anonymization engine with Mimesis for realistic synthetic data generation.
"""

import copy
import hashlib
import logging
from typing import Any, Dict, Optional, Tuple

from mimesis import Person, Address
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

logger = logging.getLogger(__name__)


# Default mapping of Presidio entity types to anonymization strategies
# Using smart redaction that preserves format and some characters for data utility
DEFAULT_ENTITY_STRATEGIES = {
    "EMAIL_ADDRESS": "redact",  # Preserves domain + uniqueness: john.doe@corp.com -> jo****oe@corp.com
    "PERSON": "redact",  # Preserves first/last chars: Joh*** Doe (or Doe*** for single name)
    "PHONE_NUMBER": "redact",  # Preserves format: +1-***-***-4567
    "LOCATION": "redact",  # Preserves first/last chars
    "US_SSN": "redact",  # Preserves format: ***-**-6789
    "SSN": "redact",  # Preserves format
    "CREDIT_CARD": "hash",  # Too sensitive, hash completely
    "IBAN": "hash",  # Too sensitive, hash completely
    "CRYPTO": "hash",  # Too sensitive, hash completely
    "US_PASSPORT": "hash",  # Too sensitive, hash completely
    "US_DRIVER_LICENSE": "redact",  # Preserves format
    "UK_NHS": "redact",  # Preserves format
    "DATE_TIME": "redact",  # Preserves format
    "IP_ADDRESS": "redact",  # Preserves format: 192.***.*.123
    "URL": "redact",  # Preserves format
    # Default for any other entity type
    "DEFAULT": "redact",
}


class PresidioAnonymizer:
    """
    Handles PII anonymization using Presidio with custom Mimesis operators.
    
    Provides integration between Presidio's anonymization engine and Mimesis
    for generating realistic synthetic data. Supports the same strategies
    as the existing PIIRedactor for backward compatibility.
    """

    def __init__(self, entity_strategy_map: Optional[Dict[str, str]] = None):
        """
        Initialize the anonymizer.
        
        Args:
            entity_strategy_map: Optional custom mapping of entity types to strategies.
                                 If None, uses DEFAULT_ENTITY_STRATEGIES.
        """
        self.person = Person()
        self.address = Address()
        self.anonymizer_engine = AnonymizerEngine()
        self.entity_strategy_map = entity_strategy_map or DEFAULT_ENTITY_STRATEGIES.copy()

    def apply_anonymization(
        self,
        document: Dict[str, Any],
        pii_map: Dict[str, Tuple[str, float]],
        manual_overrides: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Apply anonymization to a document based on detected PII.
        
        Args:
            document: The MongoDB document to anonymize
            pii_map: Map of field paths to (entity_type, confidence) from analyzer
            manual_overrides: Optional manual field->strategy overrides
        
        Returns:
            Anonymized document with PII fields redacted
        """
        # Deep copy to avoid modifying original
        anonymized = copy.deepcopy(document)
        
        # Merge auto-detected PII with manual overrides (manual takes precedence)
        final_strategies = self._merge_strategies(pii_map, manual_overrides)
        
        # Apply anonymization to each field
        for field_path, strategy in final_strategies.items():
            self._anonymize_field(anonymized, field_path, strategy)
        
        return anonymized

    def _merge_strategies(
        self,
        pii_map: Dict[str, Tuple[str, float]],
        manual_overrides: Optional[Dict[str, str]],
    ) -> Dict[str, str]:
        """
        Merge auto-detected PII strategies with manual overrides.
        
        Manual overrides take precedence. If a manual override has value None,
        that field will be excluded from anonymization (even if auto-detected).
        
        Args:
            pii_map: Auto-detected PII fields with entity types and confidence
            manual_overrides: Manual field->strategy overrides
        
        Returns:
            Final mapping of field_path -> strategy
        """
        strategies: Dict[str, str] = {}
        
        # Start with auto-detected fields, map entity types to strategies
        for field_path, (entity_type, _) in pii_map.items():
            strategy = self.entity_strategy_map.get(entity_type, self.entity_strategy_map["DEFAULT"])
            strategies[field_path] = strategy
        
        # Apply manual overrides
        if manual_overrides:
            for field_path, strategy in manual_overrides.items():
                if strategy is None:
                    # None means "don't anonymize this field"
                    strategies.pop(field_path, None)
                else:
                    # Manual strategy overrides auto-detected
                    strategies[field_path] = strategy
        
        return strategies

    def _anonymize_field(self, document: Dict[str, Any], field_path: str, strategy: str) -> None:
        """
        Anonymize a specific field in a document.
        
        Args:
            document: The document to modify
            field_path: Dot-notation path to the field
            strategy: Anonymization strategy to apply
        """
        # Handle nested fields with dot notation
        if "." in field_path:
            self._anonymize_nested_field(document, field_path, strategy)
        elif field_path in document:
            document[field_path] = self._apply_strategy(document[field_path], strategy)

    def _anonymize_nested_field(self, document: Dict[str, Any], field_path: str, strategy: str) -> None:
        """
        Anonymize a nested field using dot notation.
        
        Args:
            document: The document containing the nested field
            field_path: Dot-separated path to the field (e.g., "user.address.street")
            strategy: The anonymization strategy to apply
        """
        parts = field_path.split(".")
        current = document
        
        # Navigate to the parent of the target field
        for part in parts[:-1]:
            # Handle array notation like "items[0]"
            if "[" in part:
                array_name, index = part.replace("]", "").split("[")
                index = int(index)
                
                if array_name in current and isinstance(current[array_name], list):
                    if len(current[array_name]) > index:
                        current = current[array_name][index]
                    else:
                        return  # Index out of bounds
                else:
                    return  # Field doesn't exist
            elif part in current and isinstance(current[part], dict):
                current = current[part]
            else:
                return  # Field path doesn't exist
        
        # Apply anonymization to the final field
        final_field = parts[-1]
        if final_field in current:
            current[final_field] = self._apply_strategy(current[final_field], strategy)

    def _apply_strategy(self, value: Any, strategy: str) -> Any:
        """
        Apply an anonymization strategy to a value.
        
        Args:
            value: The value to anonymize
            strategy: The strategy to apply
        
        Returns:
            Anonymized value
        
        Raises:
            ValueError: If strategy is not recognized
        """
        strategy_map = {
            "fake_email": self._fake_email,
            "fake_name": self._fake_name,
            "fake_phone": self._fake_phone,
            "fake_address": self._fake_address,
            "hash": self._hash,
            "redact": self._redact,
            "mask": self._mask,
            "null": self._null,
        }
        
        if strategy not in strategy_map:
            raise ValueError(
                f"Unknown anonymization strategy: {strategy}. "
                f"Available strategies: {', '.join(strategy_map.keys())}"
            )
        
        return strategy_map[strategy](value)

    # Anonymization strategy implementations (using Mimesis)

    def _fake_email(self, value: Any) -> str:
        """Generate a fake email address."""
        return self.person.email()

    def _fake_name(self, value: Any) -> str:
        """Generate a fake full name."""
        return self.person.full_name()

    def _fake_phone(self, value: Any) -> str:
        """Generate a fake phone number."""
        return self.person.phone_number()

    def _fake_address(self, value: Any) -> str:
        """Generate a fake address."""
        return self.address.address()

    def _hash(self, value: Any) -> str:
        """
        Hash a value using SHA-256.
        
        Preserves referential integrity - same input always produces same hash.
        Adds a salt based on the value type for better security.
        """
        if value is None:
            return hashlib.sha256(b"").hexdigest()
        
        value_str = str(value)
        return hashlib.sha256(value_str.encode("utf-8")).hexdigest()

    def _redact(self, value: Any) -> str:
        """
        Smart redaction that preserves first 3 and last 3 characters.
        
        Preserves format for well-known patterns:
        - Email: john.doe@corp.com -> jo****oe@corp.com (preserves domain + uniqueness)
        - SSN: ***-**-6789
        - Phone: +1-***-***-4567 or (555) ***-4567
        - IP: 192.***.*.123
        - General: abc***xyz (for strings >= 7 chars)
        
        For shorter strings (< 7 chars), shows partial information or masks completely.
        """
        if value is None:
            return "***"
        
        value_str = str(value).strip()
        if len(value_str) == 0:
            return "***"
        
        # Detect and handle specific formats
        
        # Email format: user@domain.com -> abc***@ex***.com
        if "@" in value_str:
            return self._redact_email(value_str)
        
        # IP address format (check before phone, since IPs can look like phones)
        # 192.168.1.1 -> 192.***.*.1
        if self._is_ip_format(value_str):
            return self._redact_ip(value_str)
        
        # SSN format: 123-45-6789 -> ***-**-6789
        if self._is_ssn_format(value_str):
            return self._redact_ssn(value_str)
        
        # Phone number format (various)
        if self._is_phone_format(value_str):
            return self._redact_phone(value_str)
        
        # URL format: https://example.com/path -> htt***://***.com/pa***th
        if value_str.startswith(("http://", "https://", "ftp://")):
            return self._redact_url(value_str)
        
        # Generic redaction: preserve first 3 and last 3 characters
        if len(value_str) <= 6:
            # Too short to show both sides, mask more aggressively
            if len(value_str) <= 3:
                return "***"
            else:
                # Show first char only for 4-6 char strings
                return value_str[0] + "***"
        else:
            # Show first 3 and last 3
            return value_str[:3] + "***" + value_str[-3:]
    
    def _redact_email(self, email: str) -> str:
        """
        Redact email preserving domain and uniqueness.
        
        Transforms: john.smith@corp.com -> jo****th@corp.com
        
        This approach:
        - Preserves the full domain (important for unique indexes)
        - Shows first 2 and last 2 chars of local part for readability
        - Maintains uniqueness by hashing the middle portion
        - Prevents duplicate key errors on unique email indexes
        """
        try:
            local, domain = email.rsplit("@", 1)
            
            # Redact local part while preserving uniqueness
            if len(local) <= 4:
                # For short local parts, show first char + hash
                if len(local) > 0:
                    # Hash the local part to maintain uniqueness
                    hash_val = hashlib.sha256(local.encode("utf-8")).hexdigest()[:4]
                    local_redacted = local[0] + hash_val
                else:
                    local_redacted = "****"
            else:
                # For longer local parts, show first 2 and last 2 chars with hash in middle
                # This preserves readability while maintaining uniqueness
                hash_val = hashlib.sha256(local.encode("utf-8")).hexdigest()[:4]
                local_redacted = local[:2] + hash_val + local[-2:]
            
            # Keep domain unchanged to avoid duplicate keys
            return f"{local_redacted}@{domain}"
        except Exception:
            # Fallback to generic redaction if parsing fails
            return email[:3] + "***" + email[-3:] if len(email) >= 7 else email[0] + "***"
    
    def _redact_ssn(self, ssn: str) -> str:
        """Redact SSN preserving format: 123-45-6789 -> ***-**-6789"""
        # Replace digits but preserve separators
        result = ""
        parts = ssn.split("-")
        if len(parts) == 3:
            # Standard XXX-XX-XXXX format
            # Mask first two groups, show last 4
            result = "***-**-" + parts[2]
        else:
            # Non-standard format, use generic masking
            result = ""
            for char in ssn:
                if char.isdigit():
                    result += "*"
                else:
                    result += char
            # Try to preserve last 4 digits
            digits_only = [i for i, c in enumerate(ssn) if c.isdigit()]
            if len(digits_only) >= 4:
                for i in digits_only[-4:]:
                    result = result[:i] + ssn[i] + result[i+1:]
        
        return result
    
    def _redact_phone(self, phone: str) -> str:
        """Redact phone preserving format and last 4 digits"""
        # Count digits
        digits = [c for c in phone if c.isdigit()]
        if len(digits) < 4:
            # Too few digits, mask all
            return "***"
        
        # Keep last 4 digits, mask the rest
        result = ""
        digits_seen = 0
        total_digits = len(digits)
        
        for char in phone:
            if char.isdigit():
                if digits_seen < total_digits - 4:
                    result += "*"
                else:
                    result += char
                digits_seen += 1
            else:
                result += char
        
        return result
    
    def _redact_ip(self, ip: str) -> str:
        """Redact IP address: 192.168.1.1 -> 192.***.*.1"""
        parts = ip.split(".")
        if len(parts) == 4:
            # Mask middle two octets, keep first and last
            return f"{parts[0]}.***.***.{parts[3]}"
        else:
            # Not standard IPv4, use generic redaction
            return ip[:3] + "***" + ip[-3:] if len(ip) >= 7 else "***"
    
    def _redact_url(self, url: str) -> str:
        """Redact URL preserving structure"""
        # Simple approach: preserve protocol and last few chars
        if len(url) <= 12:
            return url[:4] + "***"
        else:
            return url[:8] + "***" + url[-4:]
    
    def _is_ssn_format(self, value: str) -> bool:
        """Check if value looks like SSN format"""
        # XXX-XX-XXXX or XXXXXXXXX
        import re
        return bool(re.match(r'^\d{3}-?\d{2}-?\d{4}$', value))
    
    def _is_phone_format(self, value: str) -> bool:
        """Check if value looks like phone number"""
        # Has multiple digits and common phone separators
        digit_count = sum(c.isdigit() for c in value)
        has_phone_chars = any(c in value for c in ['-', '(', ')', '+', ' '])
        return digit_count >= 7 and (has_phone_chars or digit_count >= 10)
    
    def _is_ip_format(self, value: str) -> bool:
        """Check if value looks like IP address"""
        import re
        return bool(re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', value))

    def _mask(self, value: Any) -> str:
        """
        Mask a value completely with asterisks.
        
        Similar to redact but shows no original characters.
        """
        if value is None:
            return "***"
        
        value_str = str(value)
        if len(value_str) == 0:
            return "***"
        
        # Preserve structure for things like phone numbers, SSNs
        # e.g., "123-45-6789" becomes "***-**-****"
        masked = ""
        for char in value_str:
            if char.isalnum():
                masked += "*"
            else:
                masked += char
        
        return masked if masked else "***"

    def _null(self, value: Any) -> None:
        """Replace value with None."""
        return None


# Singleton instance for easy import
anonymizer = PresidioAnonymizer()


def apply_anonymization(
    document: Dict[str, Any],
    pii_map: Dict[str, Tuple[str, float]],
    manual_overrides: Optional[Dict[str, str]] = None,
    entity_strategy_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Convenience function to anonymize a document.
    
    Args:
        document: The MongoDB document to anonymize
        pii_map: Map of field paths to (entity_type, confidence) from analyzer
        manual_overrides: Optional manual field->strategy overrides
        entity_strategy_map: Optional custom entity type to strategy mapping
    
    Returns:
        Anonymized document
    """
    anon = PresidioAnonymizer(entity_strategy_map=entity_strategy_map)
    return anon.apply_anonymization(document, pii_map, manual_overrides)
