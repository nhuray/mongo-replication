"""Custom Presidio anonymization operators for mongo-replication.

This module implements custom Presidio operators that extend the built-in
anonymization capabilities with:
- Mimesis-based fake data generation (realistic synthetic data)
- Stripe test credit card numbers
- Smart redaction that preserves format
"""

import logging
import re
from typing import Dict, Optional

from mimesis import Address, Internet, Person, Payment
from presidio_anonymizer.operators import Operator, OperatorType

logger = logging.getLogger(__name__)


class FakeEmailOperator(Operator):
    """Generate realistic fake email addresses using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake email address.

        Args:
            text: Original email (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake email address
        """
        person = Person()
        return person.email()

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_email"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeNameOperator(Operator):
    """Generate realistic fake names using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake full name.

        Args:
            text: Original name (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake full name
        """
        person = Person()
        return person.full_name()

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_name"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakePhoneOperator(Operator):
    """Generate realistic fake phone numbers using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake phone number.

        Args:
            text: Original phone (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake phone number
        """
        person = Person()
        return person.phone_number()

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_phone"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeAddressOperator(Operator):
    """Generate realistic fake addresses using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake address.

        Args:
            text: Original address (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake address
        """
        address = Address()
        return address.address()

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_address"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeSSNOperator(Operator):
    """Generate realistic fake SSN using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake SSN.

        Args:
            text: Original SSN (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake SSN in format XXX-XX-XXXX
        """
        person = Person()
        # Generate SSN format: XXX-XX-XXXX
        return person.identifier(mask="###-##-####")

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_ssn"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeCreditCardOperator(Operator):
    """Generate realistic fake credit card numbers using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake credit card number.

        Args:
            text: Original credit card (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake credit card number
        """
        payment = Payment()
        return payment.credit_card_number()

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_credit_card"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeIBANOperator(Operator):
    """Generate realistic fake IBAN.

    Generates a valid IBAN format with random country codes.
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake IBAN.

        Args:
            text: Original IBAN (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake IBAN in valid format
        """
        person = Person()
        # Common IBAN country codes
        country_codes = ["GB", "DE", "FR", "IT", "ES", "NL", "BE", "CH"]
        # Pick random country code from original or list
        if text and len(text) >= 2:
            country = text[:2]
        else:
            import random

            country = random.choice(country_codes)

        # Generate check digits (2 digits)
        check_digits = person.identifier(mask="##")
        # Generate account number (varies by country, use 18 chars as standard)
        account = person.identifier(mask="##################")

        return f"{country}{check_digits}{account}"

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_iban"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeUSBankAccountOperator(Operator):
    """Generate realistic fake US bank account numbers using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake US bank account number.

        Args:
            text: Original account number (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake US bank account number (routing + account)
        """
        person = Person()
        # Generate a 9-digit routing number and 10-digit account number
        routing = person.identifier(mask="#########")
        account = person.identifier(mask="##########")
        return f"Routing: {routing}, Account: {account}"

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_us_bank_account"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeCABankAccountOperator(Operator):
    """Generate realistic fake Canadian bank account numbers using Mimesis.

    Canadian bank accounts typically have:
    - 5-digit transit number
    - 3-digit institution number
    - 7-12 digit account number
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake Canadian bank account number.

        Args:
            text: Original account number (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake Canadian bank account number
        """
        person = Person()
        # Generate Canadian banking numbers
        transit = person.identifier(mask="#####")
        institution = person.identifier(mask="###")
        account = person.identifier(mask="##########")
        return f"{transit}-{institution}-{account}"

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_ca_bank_account"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class FakeIPAddressOperator(Operator):
    """Generate realistic fake IP addresses using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake IP address.

        Args:
            text: Original IP (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake IPv4 address
        """
        internet = Internet()
        # Generate a random IPv4 address
        return internet.ip_v4()

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "fake_ip_address"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class SmartMaskOperator(Operator):
    """Entity-aware masking operator that delegates to entity-specific mask operators.

    This operator provides intelligent masking by:
    1. Detecting the entity type from the text format (or accepting it as a parameter)
    2. Delegating to the appropriate mask_<entity> operator
    3. Applying entity-specific masking rules

    Supported entity types:
    - EMAIL_ADDRESS -> mask_email
    - PHONE_NUMBER -> mask_phone
    - CREDIT_CARD -> mask_credit_card
    - US_SSN, SSN -> mask_ssn
    - IP_ADDRESS -> mask_ip_address
    - IBAN_CODE -> mask_iban
    - PERSON -> mask_person
    - LOCATION -> mask_location
    - US_BANK_ACCOUNT -> mask_us_bank_account
    - CA_BANK_ACCOUNT -> mask_ca_bank_account

    This operator is primarily used during scan-time to suggest
    appropriate masking strategies for detected PII.
    """

    # Map entity types to mask operator names
    ENTITY_TO_OPERATOR = {
        "EMAIL_ADDRESS": "mask_email",
        "PHONE_NUMBER": "mask_phone",
        "CREDIT_CARD": "mask_credit_card",
        "US_SSN": "mask_ssn",
        "SSN": "mask_ssn",
        "IP_ADDRESS": "mask_ip_address",
        "IBAN_CODE": "mask_iban",
        "PERSON": "mask_person",
        "LOCATION": "mask_location",
        "US_BANK_ACCOUNT": "mask_us_bank_account",
        "CA_BANK_ACCOUNT": "mask_ca_bank_account",
    }

    # Map entity types to operator instances (lazy-loaded)
    _operator_cache: Dict[str, Operator] = {}

    def _get_operator(self, operator_name: str) -> Optional[Operator]:
        """Get or create operator instance."""
        if operator_name not in self._operator_cache:
            operator_map = {
                "mask_email": MaskEmailOperator,
                "mask_phone": MaskPhoneOperator,
                "mask_credit_card": MaskCreditCardOperator,
                "mask_ssn": MaskSSNOperator,
                "mask_ip_address": MaskIPAddressOperator,
                "mask_iban": MaskIBANOperator,
                "mask_person": MaskPersonOperator,
                "mask_location": MaskLocationOperator,
                "mask_us_bank_account": MaskUSBankAccountOperator,
                "mask_ca_bank_account": MaskCABankAccountOperator,
            }
            operator_class = operator_map.get(operator_name)
            if operator_class:
                self._operator_cache[operator_name] = operator_class()

        return self._operator_cache.get(operator_name)

    def _detect_entity_type(self, text: str) -> str:
        """Detect entity type from text format."""
        if not text:
            return "GENERIC"

        text_str = str(text).strip()

        # Email detection
        if "@" in text_str and "." in text_str.split("@")[-1]:
            return "EMAIL_ADDRESS"

        # IP address detection
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", text_str):
            return "IP_ADDRESS"

        # SSN detection (with or without dashes)
        if re.match(r"^\d{3}-?\d{2}-?\d{4}$", text_str):
            return "US_SSN"

        # Credit card detection (16 digits with optional spaces/dashes)
        digits_only = re.sub(r"[^\d]", "", text_str)
        if len(digits_only) == 16:
            return "CREDIT_CARD"

        # Phone number detection
        digit_count = sum(c.isdigit() for c in text_str)
        has_phone_chars = any(c in text_str for c in ["-", "(", ")", "+", " "])
        if digit_count >= 7 and (has_phone_chars or digit_count >= 10):
            return "PHONE_NUMBER"

        # IBAN detection (starts with 2 letters)
        if (
            len(text_str) > 15
            and text_str[:2].isalpha()
            and text_str[2:].replace(" ", "").isalnum()
        ):
            return "IBAN_CODE"

        return "GENERIC"

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Apply smart masking based on detected or provided entity type.

        Args:
            text: Text to mask
            params: Optional parameters including:
                   - entity_type: Override automatic detection
                   - keep_first: Number of characters to keep at the beginning (for fallback, default: 3)
                   - keep_last: Number of characters to keep at the end (for fallback, default: 3)

        Returns:
            Masked text using appropriate entity-specific operator
        """
        if not text:
            return "***"

        # Check if entity_type is provided in params
        entity_type = None
        keep_first = 3  # Default
        keep_last = 3  # Default

        if params and isinstance(params, dict):
            entity_type = params.get("entity_type")
            keep_first = params.get("keep_first", 3)
            keep_last = params.get("keep_last", 3)

        # If no entity type provided, try to detect it
        if not entity_type:
            entity_type = self._detect_entity_type(text)

        # Map entity type to operator name using class-level mapping
        operator_name = self.ENTITY_TO_OPERATOR.get(entity_type)
        if operator_name:
            operator = self._get_operator(operator_name)
            if operator:
                return operator.operate(text, params)

        # Fallback: generic masking with configurable keep_first and keep_last
        text_str = str(text)
        total_keep = keep_first + keep_last

        if len(text_str) <= total_keep:
            return "*" * len(text_str)

        masked_length = len(text_str) - total_keep
        return (
            text_str[:keep_first] + "*" * masked_length + text_str[-keep_last:]
            if keep_last > 0
            else text_str[:keep_first] + "*" * masked_length
        )

    def validate(self, params: Dict = None) -> None:
        """Validate parameters."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "smart_mask"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class SmartFakeOperator(Operator):
    """Entity-aware fake data generation that delegates to entity-specific fake operators.

    This operator provides intelligent fake data generation by:
    1. Detecting the entity type from the text format
    2. Delegating to the appropriate fake_<entity> operator
    3. Generating realistic synthetic data specific to that entity

    Supported entity types:
    - EMAIL_ADDRESS -> fake_email
    - PHONE_NUMBER -> fake_phone
    - CREDIT_CARD -> fake_credit_card
    - US_SSN, SSN -> fake_ssn
    - IP_ADDRESS -> fake_ip_address
    - IBAN_CODE -> fake_iban
    - PERSON -> fake_name
    - LOCATION -> fake_address
    - US_BANK_ACCOUNT -> fake_us_bank_account
    - CA_BANK_ACCOUNT -> fake_ca_bank_account

    This operator is primarily used during scan-time to suggest
    appropriate fake data strategies for detected PII.
    """

    # Map entity types to fake operator names
    ENTITY_TO_OPERATOR = {
        "EMAIL_ADDRESS": "fake_email",
        "PHONE_NUMBER": "fake_phone",
        "CREDIT_CARD": "fake_credit_card",
        "US_SSN": "fake_ssn",
        "SSN": "fake_ssn",
        "IP_ADDRESS": "fake_ip_address",
        "IBAN_CODE": "fake_iban",
        "PERSON": "fake_name",
        "LOCATION": "fake_address",
        "US_BANK_ACCOUNT": "fake_us_bank_account",
        "CA_BANK_ACCOUNT": "fake_ca_bank_account",
    }

    # Map entity types to operator instances (lazy-loaded)
    _operator_cache: Dict[str, Operator] = {}

    def _get_operator(self, operator_name: str) -> Optional[Operator]:
        """Get or create operator instance."""
        if operator_name not in self._operator_cache:
            operator_map = {
                "fake_email": FakeEmailOperator,
                "fake_phone": FakePhoneOperator,
                "fake_credit_card": FakeCreditCardOperator,
                "fake_ssn": FakeSSNOperator,
                "fake_ip_address": FakeIPAddressOperator,
                "fake_iban": FakeIBANOperator,
                "fake_name": FakeNameOperator,
                "fake_address": FakeAddressOperator,
                "fake_us_bank_account": FakeUSBankAccountOperator,
                "fake_ca_bank_account": FakeCABankAccountOperator,
            }
            operator_class = operator_map.get(operator_name)
            if operator_class:
                self._operator_cache[operator_name] = operator_class()

        return self._operator_cache.get(operator_name)

    def _detect_entity_type(self, text: str) -> str:
        """Detect entity type from text format."""
        if not text:
            return "GENERIC"

        text_str = str(text).strip()

        # Email detection
        if "@" in text_str and "." in text_str.split("@")[-1]:
            return "EMAIL_ADDRESS"

        # IP address detection
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", text_str):
            return "IP_ADDRESS"

        # SSN detection (with or without dashes)
        if re.match(r"^\d{3}-?\d{2}-?\d{4}$", text_str):
            return "US_SSN"

        # Credit card detection (16 digits with optional spaces/dashes)
        digits_only = re.sub(r"[^\d]", "", text_str)
        if len(digits_only) == 16:
            return "CREDIT_CARD"

        # Phone number detection
        digit_count = sum(c.isdigit() for c in text_str)
        has_phone_chars = any(c in text_str for c in ["-", "(", ")", "+", " "])
        if digit_count >= 7 and (has_phone_chars or digit_count >= 10):
            return "PHONE_NUMBER"

        # IBAN detection (starts with 2 letters)
        if (
            len(text_str) > 15
            and text_str[:2].isalpha()
            and text_str[2:].replace(" ", "").isalnum()
        ):
            return "IBAN_CODE"

        return "GENERIC"

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate fake data based on detected or provided entity type.

        Args:
            text: Original text (used for entity type detection)
            params: Optional parameters including:
                   - entity_type: Override automatic detection

        Returns:
            Fake data using appropriate entity-specific operator
        """
        # Check if entity_type is provided in params
        entity_type = None
        if params and isinstance(params, dict):
            entity_type = params.get("entity_type")

        # If no entity type provided, try to detect it from text
        if not entity_type and text:
            entity_type = self._detect_entity_type(text)

        # Map entity type to operator name using class-level mapping
        operator_name = self.ENTITY_TO_OPERATOR.get(entity_type)
        if operator_name:
            operator = self._get_operator(operator_name)
            if operator:
                return operator.operate(text, params)

        # Fallback: return generic fake text
        person = Person()
        return person.identifier(mask="##########")

    def validate(self, params: Dict = None) -> None:
        """Validate parameters."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "smart_fake"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


def resolve_smart_operator(operator_name: str, entity_type: str) -> str:
    """Resolve a smart operator to a concrete operator based on entity type.

    This function is used during scan time to convert smart operators (smart_mask, smart_fake)
    into concrete entity-specific operators that will be stored in the replication config.

    Args:
        operator_name: The smart operator name ("smart_mask" or "smart_fake")
        entity_type: The detected PII entity type (e.g., "EMAIL_ADDRESS", "PHONE_NUMBER")

    Returns:
        The resolved concrete operator name (e.g., "mask_email", "fake_phone").
        Falls back to "mask" if resolution fails.

    Examples:
        >>> resolve_smart_operator("smart_mask", "EMAIL_ADDRESS")
        'mask_email'
        >>> resolve_smart_operator("smart_fake", "PHONE_NUMBER")
        'fake_phone'
        >>> resolve_smart_operator("smart_mask", "UNKNOWN_TYPE")
        'mask'
    """
    import logging

    logger = logging.getLogger(__name__)

    # Handle non-smart operators - return as-is
    if operator_name not in ("smart_mask", "smart_fake"):
        return operator_name

    # Resolve based on operator type
    if operator_name == "smart_mask":
        resolved = SmartMaskOperator.ENTITY_TO_OPERATOR.get(entity_type)
        if resolved:
            return resolved
        logger.warning(
            f"Failed to resolve smart_mask for entity_type '{entity_type}'. "
            f"Falling back to generic 'mask' operator."
        )
        return "mask"

    elif operator_name == "smart_fake":
        resolved = SmartFakeOperator.ENTITY_TO_OPERATOR.get(entity_type)
        if resolved:
            return resolved
        logger.warning(
            f"Failed to resolve smart_fake for entity_type '{entity_type}'. "
            f"Falling back to generic 'mask' operator."
        )
        return "mask"

    # Should never reach here, but provide fallback
    return "mask"


class MaskEmailOperator(Operator):
    """Mask email addresses while preserving domain.

    Masks the local part (before @) while keeping the domain visible.
    Preserves first 2 and last 2 characters of local part for uniqueness.

    Example: john.smith@example.com -> jo******th@example.com
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask email address.

        Args:
            text: Email address to mask
            params: Optional parameters (not used)

        Returns:
            Masked email address
        """
        if not text or "@" not in text:
            return "***@***.com"

        try:
            local, domain = text.rsplit("@", 1)

            if len(local) <= 4:
                # Short local part: show first char only
                masked_local = local[0] + "*" * (len(local) - 1) if local else "***"
            else:
                # Longer local part: show first 2 and last 2 chars
                masked_local = local[:2] + "*" * (len(local) - 4) + local[-2:]

            return f"{masked_local}@{domain}"
        except Exception:
            return "***@***.com"

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_email"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskPhoneOperator(Operator):
    """Mask phone numbers while preserving format and last 4 digits.

    Preserves all non-digit characters and shows only the last 4 digits.

    Example: +1 (555) 123-4567 -> +* (***) ***-4567
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask phone number.

        Args:
            text: Phone number to mask
            params: Optional parameters (not used)

        Returns:
            Masked phone number
        """
        if not text:
            return "***-****"

        # Extract digits
        digits = [c for c in text if c.isdigit()]
        if len(digits) < 4:
            return "*" * len(text)

        # Build masked version preserving format
        result = []
        digit_index = 0
        total_digits = len(digits)

        for char in text:
            if char.isdigit():
                # Mask all except last 4 digits
                if digit_index < total_digits - 4:
                    result.append("*")
                else:
                    result.append(char)
                digit_index += 1
            else:
                result.append(char)

        return "".join(result)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_phone"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskCreditCardOperator(Operator):
    """Mask credit card numbers showing only last 4 digits.

    Preserves spacing/formatting and shows last 4 digits only.

    Example: 4242 4242 4242 4242 -> **** **** **** 4242
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask credit card number.

        Args:
            text: Credit card number to mask
            params: Optional parameters (not used)

        Returns:
            Masked credit card number
        """
        if not text:
            return "************"

        # Extract digits
        digits = [c for c in text if c.isdigit()]
        if len(digits) < 4:
            return "*" * len(text)

        # Build masked version preserving format
        result = []
        digit_index = 0
        total_digits = len(digits)

        for char in text:
            if char.isdigit():
                # Show only last 4 digits
                if digit_index < total_digits - 4:
                    result.append("*")
                else:
                    result.append(char)
                digit_index += 1
            else:
                result.append(char)

        return "".join(result)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_credit_card"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskSSNOperator(Operator):
    """Mask SSN showing only last 4 digits.

    Preserves format (with or without dashes) and shows last 4 digits.

    Example: 123-45-6789 -> ***-**-6789
    Example: 123456789 -> *****6789
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask SSN.

        Args:
            text: SSN to mask
            params: Optional parameters (not used)

        Returns:
            Masked SSN
        """
        if not text:
            return "***-**-****"

        # Extract digits
        digits = [c for c in text if c.isdigit()]
        if len(digits) < 4:
            return "*" * len(text)

        # Build masked version preserving format
        result = []
        digit_index = 0
        total_digits = len(digits)

        for char in text:
            if char.isdigit():
                # Show only last 4 digits
                if digit_index < total_digits - 4:
                    result.append("*")
                else:
                    result.append(char)
                digit_index += 1
            else:
                result.append(char)

        return "".join(result)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_ssn"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskIPAddressOperator(Operator):
    """Mask IP addresses showing first and last octet.

    Example: 192.168.1.1 -> 192.***.*.1
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask IP address.

        Args:
            text: IP address to mask
            params: Optional parameters (not used)

        Returns:
            Masked IP address
        """
        if not text:
            return "***.***.***.***"

        parts = text.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.***.*.{parts[3]}"
        else:
            # Fallback for invalid IPs
            return "***.***.***.***"

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_ip_address"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskIBANOperator(Operator):
    """Mask IBAN showing country code and last 4 digits.

    Example: GB82WEST12345698765432 -> GB******************5432
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask IBAN.

        Args:
            text: IBAN to mask
            params: Optional parameters (not used)

        Returns:
            Masked IBAN
        """
        if not text or len(text) < 6:
            return "********************"

        # IBAN format: 2-letter country code + check digits + account number
        # Show country code (first 2) and last 4 digits
        country_code = text[:2]
        last_four = text[-4:]
        masked_middle = "*" * (len(text) - 6)

        return f"{country_code}{masked_middle}{last_four}"

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_iban"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskPersonOperator(Operator):
    """Mask person names showing first and last character.

    For multi-word names, masks each word separately.

    Example: John Smith -> J*** S****
    Example: Mary -> M***
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask person name.

        Args:
            text: Person name to mask
            params: Optional parameters (not used)

        Returns:
            Masked person name
        """
        if not text:
            return "***"

        words = text.split()
        masked_words = []

        for word in words:
            if len(word) <= 2:
                masked_words.append(word[0] + "*" if word else "*")
            else:
                masked_words.append(word[0] + "*" * (len(word) - 1))

        return " ".join(masked_words)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_person"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskLocationOperator(Operator):
    """Mask location/address showing first 3 characters of each word.

    Example: San Francisco -> San Fra******
    Example: 123 Main Street -> 123 Mai*** Str****
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask location/address.

        Args:
            text: Location to mask
            params: Optional parameters (not used)

        Returns:
            Masked location
        """
        if not text:
            return "***"

        words = text.split()
        masked_words = []

        for word in words:
            if len(word) <= 3:
                masked_words.append(word)
            else:
                masked_words.append(word[:3] + "*" * (len(word) - 3))

        return " ".join(masked_words)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_location"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskUSBankAccountOperator(Operator):
    """Mask US bank account numbers showing last 4 digits.

    Works with various formats including routing + account number.

    Example: Routing: 123456789, Account: 9876543210 -> Routing: *****6789, Account: ******3210
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask US bank account.

        Args:
            text: Bank account info to mask
            params: Optional parameters (not used)

        Returns:
            Masked bank account info
        """
        if not text:
            return "***********"

        # Extract all digit sequences
        result = []
        i = 0
        while i < len(text):
            if text[i].isdigit():
                # Found start of digit sequence
                j = i
                while j < len(text) and text[j].isdigit():
                    j += 1

                # Mask this digit sequence (show last 4)
                digit_sequence = text[i:j]
                if len(digit_sequence) <= 4:
                    result.append(digit_sequence)
                else:
                    result.append("*" * (len(digit_sequence) - 4) + digit_sequence[-4:])

                i = j
            else:
                result.append(text[i])
                i += 1

        return "".join(result)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_us_bank_account"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class MaskCABankAccountOperator(Operator):
    """Mask Canadian bank account numbers showing last 4 digits.

    Similar to US bank account masking.

    Example: 12345-678-9012345 -> *****-***-***2345
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Mask Canadian bank account.

        Args:
            text: Bank account info to mask
            params: Optional parameters (not used)

        Returns:
            Masked bank account info
        """
        if not text:
            return "***********"

        # Extract all digit sequences
        result = []
        i = 0
        while i < len(text):
            if text[i].isdigit():
                # Found start of digit sequence
                j = i
                while j < len(text) and text[j].isdigit():
                    j += 1

                # Mask this digit sequence (show last 4)
                digit_sequence = text[i:j]
                if len(digit_sequence) <= 4:
                    result.append(digit_sequence)
                else:
                    result.append("*" * (len(digit_sequence) - 4) + digit_sequence[-4:])

                i = j
            else:
                result.append(text[i])
                i += 1

        return "".join(result)

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "mask_ca_bank_account"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


# Registry of all custom operators (classes, not instances)
CUSTOM_OPERATORS = [
    # Fake data operators (synthetic data generation)
    FakeEmailOperator,
    FakeNameOperator,
    FakePhoneOperator,
    FakeAddressOperator,
    FakeSSNOperator,
    FakeCreditCardOperator,
    FakeIBANOperator,
    FakeUSBankAccountOperator,
    FakeCABankAccountOperator,
    FakeIPAddressOperator,
    # Masking operators (format-preserving redaction)
    MaskEmailOperator,
    MaskPhoneOperator,
    MaskCreditCardOperator,
    MaskSSNOperator,
    MaskIPAddressOperator,
    MaskIBANOperator,
    MaskPersonOperator,
    MaskLocationOperator,
    MaskUSBankAccountOperator,
    MaskCABankAccountOperator,
    # Smart operators (entity-aware delegation)
    SmartMaskOperator,
    SmartFakeOperator,
]
