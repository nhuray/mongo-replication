"""Custom Presidio anonymization operators for mongo-replication.

This module implements custom Presidio operators that extend the built-in
anonymization capabilities with:
- Mimesis-based fake data generation (realistic synthetic data)
- Stripe test credit card numbers
- Smart redaction that preserves format
"""

import hashlib
import logging
import re
from typing import Dict

from mimesis import Address, Person, Payment
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
        return person.ssn()

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
    """Generate realistic fake IBAN using Mimesis."""

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Generate a fake IBAN.

        Args:
            text: Original IBAN (ignored)
            params: Optional parameters (not used)

        Returns:
            Fake IBAN
        """
        payment = Payment()
        return payment.bitcoin_address()  # Mimesis doesn't have IBAN, use placeholder

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


class StripeTestingCCOperator(Operator):
    """Use Stripe test credit card numbers.

    Reference: https://docs.stripe.com/testing
    """

    # Stripe test card numbers that always succeed
    STRIPE_TEST_CARDS = [
        "4242424242424242",  # Visa
        "5555555555554444",  # Mastercard
        "378282246310005",  # American Express
        "6011111111111117",  # Discover
        "3056930009020004",  # Diners Club
        "3566002020360505",  # JCB
    ]

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Return a Stripe test credit card number.

        Args:
            text: Original credit card (used to determine card type if possible)
            params: Optional parameters (not used)

        Returns:
            Stripe test credit card number
        """
        # Try to detect card type from original text to return matching test card
        if text and text.strip():
            first_digit = text.strip()[0]
            # Match first digit to card type
            if first_digit == "4":
                return self.STRIPE_TEST_CARDS[0]  # Visa
            elif first_digit == "5":
                return self.STRIPE_TEST_CARDS[1]  # Mastercard
            elif first_digit == "3":
                return self.STRIPE_TEST_CARDS[2]  # Amex
            elif first_digit == "6":
                return self.STRIPE_TEST_CARDS[3]  # Discover

        # Default to Visa test card
        return self.STRIPE_TEST_CARDS[0]

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "stripe_testing_cc"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


class SmartRedactOperator(Operator):
    """Smart redaction that preserves format for various PII types.

    This operator implements format-preserving redaction for:
    - Emails: Preserves domain and shows partial local part
    - SSN: Shows last 4 digits
    - Phone: Shows last 4 digits
    - IP addresses: Shows first and last octet
    - URLs: Shows protocol and partial path
    - Generic: Shows first 3 and last 3 characters
    """

    def operate(self, text: str = None, params: Dict = None) -> str:
        """Apply smart redaction to text.

        Args:
            text: Text to redact
            params: Optional parameters (not used)

        Returns:
            Redacted text with format preserved
        """
        if text is None:
            return "***"

        value_str = str(text).strip()
        if len(value_str) == 0:
            return "***"

        # Detect and handle specific formats

        # Email format: user@domain.com -> abc***@domain.com
        if "@" in value_str:
            return self._redact_email(value_str)

        # IP address format
        if self._is_ip_format(value_str):
            return self._redact_ip(value_str)

        # SSN format: 123-45-6789 -> ***-**-6789
        if self._is_ssn_format(value_str):
            return self._redact_ssn(value_str)

        # Phone number format
        if self._is_phone_format(value_str):
            return self._redact_phone(value_str)

        # URL format
        if value_str.startswith(("http://", "https://", "ftp://")):
            return self._redact_url(value_str)

        # Generic redaction: preserve first 3 and last 3 characters
        if len(value_str) <= 6:
            if len(value_str) <= 3:
                return "***"
            else:
                return value_str[0] + "***"
        else:
            return value_str[:3] + "***" + value_str[-3:]

    def _redact_email(self, email: str) -> str:
        """Redact email preserving domain and uniqueness."""
        try:
            local, domain = email.rsplit("@", 1)

            if len(local) <= 4:
                if len(local) > 0:
                    hash_val = hashlib.sha256(local.encode("utf-8")).hexdigest()[:4]
                    local_redacted = local[0] + hash_val
                else:
                    local_redacted = "****"
            else:
                hash_val = hashlib.sha256(local.encode("utf-8")).hexdigest()[:4]
                local_redacted = local[:2] + hash_val + local[-2:]

            return f"{local_redacted}@{domain}"
        except Exception:
            return email[:3] + "***" + email[-3:] if len(email) >= 7 else email[0] + "***"

    def _redact_ssn(self, ssn: str) -> str:
        """Redact SSN preserving format: 123-45-6789 -> ***-**-6789"""
        parts = ssn.split("-")
        if len(parts) == 3:
            return "***-**-" + parts[2]
        else:
            result = ""
            digits_only = [i for i, c in enumerate(ssn) if c.isdigit()]
            for i, char in enumerate(ssn):
                if char.isdigit() and i not in digits_only[-4:]:
                    result += "*"
                else:
                    result += char
            return result

    def _redact_phone(self, phone: str) -> str:
        """Redact phone preserving format and last 4 digits"""
        digits = [c for c in phone if c.isdigit()]
        if len(digits) < 4:
            return "***"

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
            return f"{parts[0]}.***.***.{parts[3]}"
        else:
            return ip[:3] + "***" + ip[-3:] if len(ip) >= 7 else "***"

    def _redact_url(self, url: str) -> str:
        """Redact URL preserving structure"""
        if len(url) <= 12:
            return url[:4] + "***"
        else:
            return url[:8] + "***" + url[-4:]

    def _is_ssn_format(self, value: str) -> bool:
        """Check if value looks like SSN format"""
        return bool(re.match(r"^\d{3}-?\d{2}-?\d{4}$", value))

    def _is_phone_format(self, value: str) -> bool:
        """Check if value looks like phone number"""
        digit_count = sum(c.isdigit() for c in value)
        has_phone_chars = any(c in value for c in ["-", "(", ")", "+", " "])
        return digit_count >= 7 and (has_phone_chars or digit_count >= 10)

    def _is_ip_format(self, value: str) -> bool:
        """Check if value looks like IP address"""
        return bool(re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", value))

    def validate(self, params: Dict = None) -> None:
        """Validate parameters (no params needed)."""
        pass

    def operator_name(self) -> str:
        """Return operator name."""
        return "smart_redact"

    def operator_type(self) -> OperatorType:
        """Return operator type."""
        return OperatorType.Anonymize


# Registry of all custom operators (classes, not instances)
CUSTOM_OPERATORS = [
    FakeEmailOperator,
    FakeNameOperator,
    FakePhoneOperator,
    FakeAddressOperator,
    FakeSSNOperator,
    FakeCreditCardOperator,
    FakeIBANOperator,
    FakeUSBankAccountOperator,
    StripeTestingCCOperator,
    SmartRedactOperator,
]
