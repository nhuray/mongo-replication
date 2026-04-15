"""Unit tests for custom Presidio anonymization operators."""

import re


from mongo_replication.engine.pii.custom_operators import (
    FakeCABankAccountOperator,
    FakeIPAddressOperator,
    MaskCABankAccountOperator,
    MaskCreditCardOperator,
    MaskEmailOperator,
    MaskIBANOperator,
    MaskIPAddressOperator,
    MaskLocationOperator,
    MaskPersonOperator,
    MaskPhoneOperator,
    MaskSINOperator,
    MaskSSNOperator,
    MaskTINOperator,
    MaskUSBankAccountOperator,
    SmartFakeOperator,
    SmartMaskOperator,
    resolve_smart_operator,
)


# =============================================================================
# MASK OPERATORS TESTS
# =============================================================================


class TestMaskEmailOperator:
    """Tests for MaskEmailOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskEmailOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_email"

    def test_mask_standard_email(self):
        """Test masking standard email address with default behavior.

        With default min_local_part=5 and min_domain_part=5:
        - 'john.smith' (10 chars) > 5, so partially masked (show first 2 and last 2)
        - 'example.com' (11 chars) > 5, so preserved
        """
        email = "john.smith@example.com"
        masked = self.operator.operate(email)

        # Should preserve domain
        assert "@example.com" in masked
        # Should mask local part showing first 2 and last 2
        assert "john.smith" not in masked
        assert masked.startswith("jo")
        assert masked.endswith("th@example.com")

    def test_mask_short_email(self):
        """Test masking short email address.

        'ab' (2 chars) <= 5, so fully masked
        'test.com' (8 chars) > 5, so preserved
        """
        email = "ab@test.com"
        masked = self.operator.operate(email)

        # Should preserve domain
        assert "@test.com" in masked
        # Should fully mask local part since it's at or below threshold
        assert masked == "**@test.com"

    def test_mask_long_email(self):
        """Test masking long email address.

        Both parts meet thresholds, so partial masking applies.
        """
        email = "verylongemailaddress@company.org"
        masked = self.operator.operate(email)

        # Should preserve domain
        assert "@company.org" in masked
        # Should show first 2 and last 2
        assert masked.startswith("ve")
        assert masked.endswith("ss@company.org")
        # Should have asterisks in middle
        assert "*" in masked

    def test_mask_empty_email(self):
        """Test masking empty or None email."""
        assert self.operator.operate(None) == "***@***.com"
        assert self.operator.operate("") == "***@***.com"

    def test_mask_invalid_email(self):
        """Test masking invalid email format."""
        result = self.operator.operate("notanemail")
        assert result == "***@***.com"

    def test_mask_email_with_keep_domain_false(self):
        """Test masking email with keep_domain=False."""
        email = "john.smith@example.com"
        masked = self.operator.operate(email, params={"keep_domain": False})

        # Should mask both local and domain
        assert "@" in masked
        assert "example.com" not in masked
        assert "*" in masked.split("@")[1]  # Domain should be masked

    def test_mask_email_below_min_local_part(self):
        """Test masking email where local part is below min_local_part threshold."""
        email = "joe@example.com"  # local part is 3 chars, below default of 5
        masked = self.operator.operate(email)

        # Should fully mask local part
        assert masked.startswith("***@")
        # Should preserve domain
        assert masked.endswith("@example.com")

    def test_mask_email_at_min_local_part(self):
        """Test masking email where local part equals min_local_part threshold."""
        email = "alice@example.com"  # local part is 5 chars, equals default of 5
        masked = self.operator.operate(email)

        # Should fully mask local part since it equals threshold (<=)
        assert masked == "*****@example.com"

    def test_mask_email_above_min_local_part(self):
        """Test masking email where local part is just above min_local_part threshold."""
        email = "robert@example.com"  # local part is 6 chars, above default of 5
        masked = self.operator.operate(email)

        # Should partially mask local part (show first 2 and last 2)
        assert masked.startswith("ro")
        assert masked.endswith("rt@example.com")
        assert "robert" not in masked

    def test_mask_email_custom_min_local_part(self):
        """Test masking email with custom min_local_part."""
        email = "john@example.com"  # local part is 4 chars
        # Set min to 5, so 4 < 5, should fully mask
        masked = self.operator.operate(email, params={"min_local_part": 5})

        # Should fully mask local part since 4 < 5
        assert masked.startswith("****@")
        assert masked.endswith("@example.com")

    def test_mask_email_custom_min_domain_part(self):
        """Test masking email with custom min_domain_part."""
        email = "john@ex.co"  # domain is 5 chars
        # Set min to 6, so 5 <= 6, should fully mask domain
        masked = self.operator.operate(email, params={"min_domain_part": 6})

        # Should fully mask domain since 5 < 6
        assert "@ex.co" not in masked
        assert "@*****" in masked

    def test_mask_email_domain_at_threshold(self):
        """Test masking email where domain equals min_domain_part threshold."""
        email = "john@ex.co"  # domain is 5 chars, equals default of 5
        masked = self.operator.operate(email)

        # Should fully mask domain since it equals threshold (<=)
        assert "@ex.co" not in masked
        assert "@*****" in masked

    def test_mask_email_domain_above_threshold(self):
        """Test masking email where domain is just above min_domain_part threshold."""
        email = "john@test.io"  # domain is 7 chars, above default of 5
        masked = self.operator.operate(email)

        # Should preserve domain since it's above threshold
        assert "@test.io" in masked

    def test_mask_email_all_params_combined(self):
        """Test masking email with all parameters combined."""
        email = "johnsmith@company.org"
        masked = self.operator.operate(
            email,
            params={
                "keep_domain": False,
                "min_local_part": 10,
                "min_domain_part": 10,
            },
        )

        # keep_domain=False should always mask domain regardless of length
        assert "company.org" not in masked
        # local part is 9 chars, below 10, so fully masked
        assert masked.startswith("*********@")

    def test_validate_params_valid(self):
        """Test parameter validation with valid params."""
        # Should not raise
        self.operator.validate({"keep_domain": True, "min_local_part": 5, "min_domain_part": 3})

    def test_validate_params_invalid_keep_domain(self):
        """Test parameter validation with invalid keep_domain type."""
        import pytest

        with pytest.raises(ValueError, match="keep_domain must be a boolean"):
            self.operator.validate({"keep_domain": "yes"})

    def test_validate_params_invalid_min_local_part(self):
        """Test parameter validation with invalid min_local_part type."""
        import pytest

        with pytest.raises(ValueError, match="min_local_part must be an integer"):
            self.operator.validate({"min_local_part": "5"})

    def test_validate_params_invalid_min_domain_part(self):
        """Test parameter validation with invalid min_domain_part type."""
        import pytest

        with pytest.raises(ValueError, match="min_domain_part must be an integer"):
            self.operator.validate({"min_domain_part": 3.5})


class TestMaskPhoneOperator:
    """Tests for MaskPhoneOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskPhoneOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_phone"

    def test_mask_us_phone_with_dashes(self):
        """Test masking US phone with dashes."""
        phone = "555-123-4567"
        masked = self.operator.operate(phone)

        # Should preserve format
        assert "-" in masked
        # Should show last 4 digits
        assert masked.endswith("4567")
        # Should mask other digits
        assert "555" not in masked
        assert "123" not in masked

    def test_mask_international_phone(self):
        """Test masking international phone."""
        phone = "+1 (555) 123-4567"
        masked = self.operator.operate(phone)

        # Should preserve format characters
        assert "+" in masked
        assert "(" in masked
        assert ")" in masked
        # Should show last 4 digits
        assert masked.endswith("4567")

    def test_mask_phone_numbers_only(self):
        """Test masking phone with numbers only."""
        phone = "5551234567"
        masked = self.operator.operate(phone)

        # Should show last 4
        assert masked.endswith("4567")
        # Should mask first 6
        assert masked.startswith("******")

    def test_mask_short_phone(self):
        """Test masking short phone number."""
        phone = "123"
        masked = self.operator.operate(phone)

        # Should mask all if less than 4 digits
        assert masked == "***"

    def test_mask_empty_phone(self):
        """Test masking empty phone."""
        assert self.operator.operate(None) == "***-****"
        assert self.operator.operate("") == "***-****"


class TestMaskCreditCardOperator:
    """Tests for MaskCreditCardOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskCreditCardOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_credit_card"

    def test_mask_card_with_spaces(self):
        """Test masking card with spaces."""
        card = "4242 4242 4242 4242"
        masked = self.operator.operate(card)

        # Should preserve spaces
        assert " " in masked
        # Should show last 4 digits
        assert masked.endswith("4242")
        # Should mask other digits
        assert masked.startswith("****")

    def test_mask_card_no_spaces(self):
        """Test masking card without spaces."""
        card = "4242424242424242"
        masked = self.operator.operate(card)

        # Should show last 4
        assert masked.endswith("4242")
        # Should mask first 12
        assert masked.startswith("************")

    def test_mask_card_with_dashes(self):
        """Test masking card with dashes."""
        card = "4242-4242-4242-4242"
        masked = self.operator.operate(card)

        # Should preserve dashes
        assert "-" in masked
        # Should show last 4
        assert masked.endswith("4242")

    def test_mask_empty_card(self):
        """Test masking empty card."""
        assert self.operator.operate(None) == "************"


class TestMaskSSNOperator:
    """Tests for MaskSSNOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskSSNOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_ssn"

    def test_mask_ssn_with_dashes(self):
        """Test masking SSN with dashes."""
        ssn = "123-45-6789"
        masked = self.operator.operate(ssn)

        # Should preserve dashes
        assert masked.count("-") == 2
        # Should show last 4
        assert masked.endswith("6789")
        # Should mask first 5 digits
        assert masked.startswith("***-**-")

    def test_mask_ssn_without_dashes(self):
        """Test masking SSN without dashes."""
        ssn = "123456789"
        masked = self.operator.operate(ssn)

        # Should show last 4
        assert masked.endswith("6789")
        # Should mask first 5
        assert masked.startswith("*****")

    def test_mask_empty_ssn(self):
        """Test masking empty SSN."""
        assert self.operator.operate(None) == "***-**-****"


class TestMaskSINOperator:
    """Tests for MaskSINOperator (Canadian Social Insurance Number)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskSINOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_sin"

    def test_mask_sin_with_dashes(self):
        """Test masking Canadian SIN with dashes."""
        sin = "123-456-789"
        masked = self.operator.operate(sin)

        # Should preserve dashes
        assert masked.count("-") == 2
        # Should show last 3
        assert masked.endswith("789")
        # Should mask first 6 digits
        assert masked.startswith("***-***-")
        assert masked == "***-***-789"

    def test_mask_sin_without_dashes(self):
        """Test masking Canadian SIN without dashes."""
        sin = "123456789"
        masked = self.operator.operate(sin)

        # Should show last 3
        assert masked.endswith("789")
        # Should mask first 6
        assert masked.startswith("******")
        assert masked == "******789"

    def test_mask_sin_partial_format(self):
        """Test masking SIN with spaces."""
        sin = "123 456 789"
        masked = self.operator.operate(sin)

        # Should preserve spaces
        assert masked.count(" ") == 2
        # Should show last 3
        assert masked.endswith("789")
        # Should mask first 6 digits
        assert masked == "*** *** 789"

    def test_mask_empty_sin(self):
        """Test masking empty SIN."""
        assert self.operator.operate(None) == "***-***-***"
        assert self.operator.operate("") == "***-***-***"

    def test_mask_short_sin(self):
        """Test masking SIN with fewer than 3 digits."""
        sin = "12"
        masked = self.operator.operate(sin)
        assert masked == "**"


class TestMaskTINOperator:
    """Tests for MaskTINOperator (Canadian Tax Identification Number)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskTINOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_tin"

    def test_mask_tin_basic(self):
        """Test masking basic Canadian TIN (9 digits)."""
        tin = "123456789"
        masked = self.operator.operate(tin)

        # Should show last 4
        assert masked.endswith("6789")
        # Should mask first 5
        assert masked.startswith("*****")
        assert masked == "*****6789"

    def test_mask_tin_with_program_identifier(self):
        """Test masking TIN with program identifier (RC, RM, RP, RT)."""
        tin = "123456789RC0001"
        masked = self.operator.operate(tin)

        # Should show last 4 digits of all digits (which includes program identifier digits)
        # 9 digits + 4 program identifier digits = 13 total digits
        # Last 4 of 13 digits = "0001"
        assert masked == "*********RC0001"
        assert "RC0001" in masked
        assert masked.endswith("RC0001")

    def test_mask_tin_with_dashes(self):
        """Test masking TIN with dashes."""
        tin = "12345-6789"
        masked = self.operator.operate(tin)

        # Should preserve dash
        assert "-" in masked
        # Should show last 4 digits
        assert masked.endswith("6789")
        # Should mask first 5
        assert masked.startswith("*****-")
        assert masked == "*****-6789"

    def test_mask_tin_rm_program(self):
        """Test masking TIN with RM program identifier."""
        tin = "987654321RM0002"
        masked = self.operator.operate(tin)

        # Should preserve RM program identifier
        # Last 4 of 13 total digits = "0002"
        assert "RM0002" in masked
        assert masked.endswith("RM0002")
        assert masked == "*********RM0002"

    def test_mask_empty_tin(self):
        """Test masking empty TIN."""
        assert self.operator.operate(None) == "*********"
        assert self.operator.operate("") == "*********"

    def test_mask_short_tin(self):
        """Test masking TIN with fewer than 4 digits."""
        tin = "123"
        masked = self.operator.operate(tin)
        assert masked == "***"


class TestMaskIPAddressOperator:
    """Tests for MaskIPAddressOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskIPAddressOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_ip_address"

    def test_mask_ipv4(self):
        """Test masking IPv4 address."""
        ip = "192.168.1.1"
        masked = self.operator.operate(ip)

        # Should show first and last octet
        assert masked.startswith("192.")
        assert masked.endswith(".1")
        # Should mask middle octets
        assert "*" in masked

    def test_mask_different_ip(self):
        """Test masking different IP address."""
        ip = "10.20.30.40"
        masked = self.operator.operate(ip)

        # Should show first and last octet, mask middle
        assert masked.startswith("10.")
        assert masked.endswith(".40")
        assert "*" in masked

    def test_mask_empty_ip(self):
        """Test masking empty IP."""
        assert self.operator.operate(None) == "***.***.***.***"

    def test_mask_invalid_ip(self):
        """Test masking invalid IP format."""
        result = self.operator.operate("not.an.ip")
        # Should return default mask since it doesn't have 4 parts
        assert result == "***.***.***.***"


class TestMaskIBANOperator:
    """Tests for MaskIBANOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskIBANOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_iban"

    def test_mask_iban(self):
        """Test masking IBAN."""
        iban = "GB82WEST12345698765432"
        masked = self.operator.operate(iban)

        # Should show country code (first 2)
        assert masked.startswith("GB")
        # Should show last 4
        assert masked.endswith("5432")
        # Should mask middle
        assert "*" in masked

    def test_mask_different_iban(self):
        """Test masking different IBAN."""
        iban = "DE89370400440532013000"
        masked = self.operator.operate(iban)

        assert masked.startswith("DE")
        assert masked.endswith("3000")

    def test_mask_short_iban(self):
        """Test masking short IBAN."""
        iban = "GB82"
        masked = self.operator.operate(iban)

        # Too short, should return default
        assert masked == "********************"


class TestMaskPersonOperator:
    """Tests for MaskPersonOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskPersonOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_person"

    def test_mask_full_name(self):
        """Test masking full name."""
        name = "John Smith"
        masked = self.operator.operate(name)

        # Should show first char of each word
        assert masked.startswith("J")
        assert "S" in masked
        # Should mask rest
        assert "*" in masked

    def test_mask_single_name(self):
        """Test masking single name."""
        name = "Madonna"
        masked = self.operator.operate(name)

        assert masked.startswith("M")
        assert "*" in masked

    def test_mask_three_names(self):
        """Test masking three names."""
        name = "John Paul Smith"
        masked = self.operator.operate(name)

        # Each word should start with original letter
        words = masked.split()
        assert len(words) == 3
        assert words[0].startswith("J")
        assert words[1].startswith("P")
        assert words[2].startswith("S")

    def test_mask_empty_name(self):
        """Test masking empty name."""
        assert self.operator.operate(None) == "***"


class TestMaskLocationOperator:
    """Tests for MaskLocationOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskLocationOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_location"

    def test_mask_city(self):
        """Test masking city name."""
        location = "San Francisco"
        masked = self.operator.operate(location)

        # Should show first 3 chars of each word
        assert masked.startswith("San")
        assert "Fra" in masked
        # Should mask rest
        assert "*" in masked

    def test_mask_address(self):
        """Test masking street address."""
        location = "123 Main Street"
        masked = self.operator.operate(location)

        # Should preserve short words (3 chars or less)
        assert "123" in masked
        # Should show first 3 of longer words
        assert "Mai" in masked
        assert "Str" in masked

    def test_mask_empty_location(self):
        """Test masking empty location."""
        assert self.operator.operate(None) == "***"


class TestMaskUSBankAccountOperator:
    """Tests for MaskUSBankAccountOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskUSBankAccountOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_us_bank_account"

    def test_mask_bank_account(self):
        """Test masking bank account."""
        account = "Routing: 123456789, Account: 9876543210"
        masked = self.operator.operate(account)

        # Should preserve format
        assert "Routing:" in masked
        assert "Account:" in masked
        # Should show last 4 of each number
        assert "6789" in masked
        assert "3210" in masked
        # Should mask other digits
        assert "*" in masked

    def test_mask_simple_account(self):
        """Test masking simple account number."""
        account = "1234567890"
        masked = self.operator.operate(account)

        # Should show last 4
        assert masked.endswith("7890")
        # Should mask first 6
        assert masked.startswith("******")


class TestMaskCABankAccountOperator:
    """Tests for MaskCABankAccountOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = MaskCABankAccountOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "mask_ca_bank_account"

    def test_mask_ca_account(self):
        """Test masking Canadian bank account."""
        account = "12345-678-9012345"
        masked = self.operator.operate(account)

        # Should preserve dashes
        assert "-" in masked
        # Should show last 4 of each number group
        assert "*2345" in masked or "2345" in masked
        # Should mask other digits
        assert "*" in masked


# =============================================================================
# FAKE OPERATORS TESTS
# =============================================================================


class TestFakeIPAddressOperator:
    """Tests for FakeIPAddressOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = FakeIPAddressOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "fake_ip_address"

    def test_generate_fake_ip(self):
        """Test generating fake IP address."""
        fake_ip = self.operator.operate()

        # Should match IPv4 format
        assert re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", fake_ip)

        # Parse octets
        octets = [int(x) for x in fake_ip.split(".")]

        # All octets should be in valid range
        for octet in octets:
            assert 0 <= octet <= 255

    def test_generates_different_ips(self):
        """Test that multiple calls generate different IPs."""
        ips = [self.operator.operate() for _ in range(10)]

        # Should have at least some variety (not all identical)
        unique_ips = set(ips)
        assert len(unique_ips) > 1


class TestFakeCABankAccountOperator:
    """Tests for FakeCABankAccountOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = FakeCABankAccountOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "fake_ca_bank_account"

    def test_generate_fake_account(self):
        """Test generating fake Canadian bank account."""
        fake_account = self.operator.operate()

        # Should match format: XXXXX-XXX-XXXXXXXXXX
        assert re.match(r"^\d{5}-\d{3}-\d{10}$", fake_account)

        # Should have dashes in right places
        parts = fake_account.split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 5  # Transit
        assert len(parts[1]) == 3  # Institution
        assert len(parts[2]) == 10  # Account

    def test_generates_different_accounts(self):
        """Test that multiple calls generate different accounts."""
        accounts = [self.operator.operate() for _ in range(10)]

        # Should have variety
        unique_accounts = set(accounts)
        assert len(unique_accounts) > 1


# =============================================================================
# SMART OPERATORS TESTS
# =============================================================================


class TestSmartMaskOperator:
    """Tests for SmartMaskOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = SmartMaskOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "smart_mask"

    def test_auto_detect_email(self):
        """Test auto-detection and masking of email."""
        email = "john.smith@example.com"
        masked = self.operator.operate(email)

        # Should detect as email and delegate to mask_email
        assert "@example.com" in masked
        assert "john.smith" not in masked

    def test_auto_detect_phone(self):
        """Test auto-detection and masking of phone."""
        phone = "+1 (555) 123-4567"
        masked = self.operator.operate(phone)

        # Should detect as phone and delegate to mask_phone
        assert masked.endswith("4567")

    def test_auto_detect_credit_card(self):
        """Test auto-detection and masking of credit card."""
        card = "4242 4242 4242 4242"
        masked = self.operator.operate(card)

        # Should detect as credit card and delegate to mask_credit_card
        assert masked.endswith("4242")
        assert "*" in masked

    def test_auto_detect_ssn(self):
        """Test auto-detection and masking of SSN."""
        ssn = "123-45-6789"
        masked = self.operator.operate(ssn)

        # Should detect as SSN and delegate to mask_ssn
        assert masked.endswith("6789")
        assert masked.startswith("***-**-")

    def test_auto_detect_ip(self):
        """Test auto-detection and masking of IP address."""
        ip = "192.168.1.1"
        masked = self.operator.operate(ip)

        # Should detect as IP and delegate to mask_ip_address
        assert masked == "192.***.*.1"

    def test_explicit_entity_type(self):
        """Test providing explicit entity type via params."""
        text = "Some person name"
        masked = self.operator.operate(text, params={"entity_type": "PERSON"})

        # Should use PERSON masking even though auto-detection might not catch it
        assert masked.startswith("S")
        assert "*" in masked

    def test_fallback_generic_masking(self):
        """Test fallback to generic masking for unknown types."""
        text = "UnknownDataType123"
        masked = self.operator.operate(text)

        # Should use generic masking (first 3 + last 3)
        # The actual output depends on length
        assert len(masked) == len(text)
        assert "*" in masked
        # Should preserve some of original text
        assert masked != text

    def test_empty_text(self):
        """Test masking empty text."""
        assert self.operator.operate(None) == "***"
        assert self.operator.operate("") == "***"

    def test_fallback_with_custom_keep_first(self):
        """Test fallback masking with custom keep_first parameter."""
        text = "some_random_data_12345"  # Won't be detected as any specific entity
        masked = self.operator.operate(text, params={"keep_first": 5, "keep_last": 3})

        # Should keep first 5 chars and last 3 chars
        assert masked.startswith("some_")
        assert masked.endswith("345")
        assert "*" in masked
        assert len(masked) == len(text)

    def test_fallback_with_custom_keep_last(self):
        """Test fallback masking with custom keep_last parameter."""
        text = "random_string_value"
        masked = self.operator.operate(text, params={"keep_first": 2, "keep_last": 5})

        # Should keep first 2 chars and last 5 chars
        assert masked.startswith("ra")
        assert masked.endswith("value")
        assert "*" in masked
        assert len(masked) == len(text)

    def test_fallback_with_keep_first_only(self):
        """Test fallback masking with keep_first=0."""
        text = "random_value_string"
        masked = self.operator.operate(text, params={"keep_first": 0, "keep_last": 4})

        # Should keep no chars at beginning and last 4 chars
        assert masked.endswith("ring")
        assert masked.startswith("*")
        assert len(masked) == len(text)

    def test_fallback_with_keep_last_zero(self):
        """Test fallback masking with keep_last=0."""
        text = "another_random_value"
        masked = self.operator.operate(text, params={"keep_first": 4, "keep_last": 0})

        # Should keep first 4 chars and no chars at end
        assert masked.startswith("anot")
        assert masked.endswith("*")
        assert len(masked) == len(text)

    def test_fallback_with_short_text(self):
        """Test fallback masking when text is shorter than keep_first + keep_last."""
        text = "Short"
        masked = self.operator.operate(text, params={"keep_first": 3, "keep_last": 3})

        # Should mask entire string when length <= keep_first + keep_last
        assert masked == "*" * len(text)

    def test_fallback_params_dont_affect_entity_specific_masking(self):
        """Test that keep_first/keep_last don't affect entity-specific operators."""
        email = "john.smith@example.com"
        # Even with custom keep_first/keep_last, email masking should use its own logic
        masked = self.operator.operate(email, params={"keep_first": 10, "keep_last": 10})

        # Should still use email-specific masking
        assert "@example.com" in masked
        assert "john.smith" not in masked


class TestSmartFakeOperator:
    """Tests for SmartFakeOperator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operator = SmartFakeOperator()

    def test_operator_name(self):
        """Test operator name is correct."""
        assert self.operator.operator_name() == "smart_fake"

    def test_auto_detect_email(self):
        """Test auto-detection and fake data for email."""
        email = "john.smith@example.com"
        faked = self.operator.operate(email)

        # Should generate a fake email
        assert "@" in faked
        assert "." in faked
        # Should not be the same as input
        assert faked != email

    def test_auto_detect_phone(self):
        """Test auto-detection and fake data for phone."""
        phone = "+1 (555) 123-4567"
        faked = self.operator.operate(phone)

        # Should generate a fake phone
        # Should not be the same as input
        assert faked != phone

    def test_auto_detect_credit_card(self):
        """Test auto-detection and fake data for credit card."""
        card = "4242 4242 4242 4242"
        faked = self.operator.operate(card)

        # Should generate fake card with digits
        digits = re.sub(r"[^\d]", "", faked)
        assert len(digits) >= 13  # Valid card length
        # Should not be the same as input
        assert faked != card

    def test_auto_detect_ssn(self):
        """Test auto-detection and fake data for SSN."""
        ssn = "123-45-6789"
        faked = self.operator.operate(ssn)

        # Should generate fake SSN in format XXX-XX-XXXX
        assert re.match(r"^\d{3}-\d{2}-\d{4}$", faked)
        # Should not be the same as input
        assert faked != ssn

    def test_auto_detect_ip(self):
        """Test auto-detection and fake data for IP."""
        ip = "192.168.1.1"
        faked = self.operator.operate(ip)

        # Should generate fake IP
        assert re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", faked)
        # Should not be the same as input
        assert faked != ip

    def test_explicit_entity_type(self):
        """Test providing explicit entity type via params."""
        text = "original@email.com"
        faked = self.operator.operate(text, params={"entity_type": "PHONE_NUMBER"})

        # Should generate phone even though text looks like email
        # We can't easily verify it's a phone, but we can check it's not an email
        # (unless the phone generator happens to include @, which it shouldn't)
        assert faked != text

    def test_generates_different_values(self):
        """Test that multiple calls generate different values."""
        email = "test@example.com"
        fakes = [self.operator.operate(email) for _ in range(10)]

        # Should have some variety
        unique_fakes = set(fakes)
        assert len(unique_fakes) > 1


# =============================================================================
# RESOLVE SMART OPERATOR TESTS
# =============================================================================


class TestResolveSmartOperator:
    """Tests for resolve_smart_operator function."""

    def test_resolve_smart_mask_email(self):
        """Test resolving smart_mask for EMAIL_ADDRESS."""
        resolved = resolve_smart_operator("smart_mask", "EMAIL_ADDRESS")
        assert resolved == "mask_email"

    def test_resolve_smart_mask_phone(self):
        """Test resolving smart_mask for PHONE_NUMBER."""
        resolved = resolve_smart_operator("smart_mask", "PHONE_NUMBER")
        assert resolved == "mask_phone"

    def test_resolve_smart_mask_ssn(self):
        """Test resolving smart_mask for US_SSN."""
        resolved = resolve_smart_operator("smart_mask", "US_SSN")
        assert resolved == "mask_ssn"

    def test_resolve_smart_mask_ip(self):
        """Test resolving smart_mask for IP_ADDRESS."""
        resolved = resolve_smart_operator("smart_mask", "IP_ADDRESS")
        assert resolved == "mask_ip_address"

    def test_resolve_smart_mask_person(self):
        """Test resolving smart_mask for PERSON."""
        resolved = resolve_smart_operator("smart_mask", "PERSON")
        assert resolved == "mask_person"

    def test_resolve_smart_mask_location(self):
        """Test resolving smart_mask for LOCATION."""
        resolved = resolve_smart_operator("smart_mask", "LOCATION")
        assert resolved == "mask_location"

    def test_resolve_smart_mask_unknown_entity(self):
        """Test resolving smart_mask for unknown entity type falls back to mask."""
        resolved = resolve_smart_operator("smart_mask", "UNKNOWN_TYPE")
        assert resolved == "mask"

    def test_resolve_smart_fake_email(self):
        """Test resolving smart_fake for EMAIL_ADDRESS."""
        resolved = resolve_smart_operator("smart_fake", "EMAIL_ADDRESS")
        assert resolved == "fake_email"

    def test_resolve_smart_fake_phone(self):
        """Test resolving smart_fake for PHONE_NUMBER."""
        resolved = resolve_smart_operator("smart_fake", "PHONE_NUMBER")
        assert resolved == "fake_phone"

    def test_resolve_smart_fake_person(self):
        """Test resolving smart_fake for PERSON."""
        resolved = resolve_smart_operator("smart_fake", "PERSON")
        assert resolved == "fake_name"

    def test_resolve_smart_fake_location(self):
        """Test resolving smart_fake for LOCATION."""
        resolved = resolve_smart_operator("smart_fake", "LOCATION")
        assert resolved == "fake_address"

    def test_resolve_smart_fake_unknown_entity(self):
        """Test resolving smart_fake for unknown entity type falls back to mask."""
        resolved = resolve_smart_operator("smart_fake", "UNKNOWN_TYPE")
        assert resolved == "mask"

    def test_resolve_non_smart_operator_returns_as_is(self):
        """Test that non-smart operators are returned unchanged."""
        resolved = resolve_smart_operator("mask_email", "EMAIL_ADDRESS")
        assert resolved == "mask_email"

        resolved = resolve_smart_operator("fake_phone", "PHONE_NUMBER")
        assert resolved == "fake_phone"

        resolved = resolve_smart_operator("hash", "EMAIL_ADDRESS")
        assert resolved == "hash"

        resolved = resolve_smart_operator("redact", "PERSON")
        assert resolved == "redact"

    def test_resolve_all_smart_mask_entities(self):
        """Test that all entities in SmartMaskOperator.ENTITY_TO_OPERATOR resolve correctly."""
        for entity_type, expected_operator in SmartMaskOperator.ENTITY_TO_OPERATOR.items():
            resolved = resolve_smart_operator("smart_mask", entity_type)
            assert resolved == expected_operator, (
                f"Failed to resolve smart_mask for {entity_type}. "
                f"Expected {expected_operator}, got {resolved}"
            )

    def test_resolve_all_smart_fake_entities(self):
        """Test that all entities in SmartFakeOperator.ENTITY_TO_OPERATOR resolve correctly."""
        for entity_type, expected_operator in SmartFakeOperator.ENTITY_TO_OPERATOR.items():
            resolved = resolve_smart_operator("smart_fake", entity_type)
            assert resolved == expected_operator, (
                f"Failed to resolve smart_fake for {entity_type}. "
                f"Expected {expected_operator}, got {resolved}"
            )
