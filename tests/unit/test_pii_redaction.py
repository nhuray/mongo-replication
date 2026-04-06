"""Tests for PII redaction with array field support."""

from mongo_replication.engine.pii import PIIRedactor


class TestPIIRedactor:
    """Test PII redaction functionality."""
    
    def test_simple_field_redaction(self):
        """Test redacting a simple top-level field."""
        redactor = PIIRedactor()
        doc = {"name": "John Doe", "age": 30}
        pii_config = {"name": "redact"}
        
        result = redactor.redact_document(doc, pii_config)
        
        assert result["name"] == "John***"
        assert result["age"] == 30
    
    def test_nested_field_redaction(self):
        """Test redacting a nested field using dot notation."""
        redactor = PIIRedactor()
        doc = {
            "user": {
                "profile": {
                    "email": "john@example.com"
                }
            }
        }
        pii_config = {"user.profile.email": "redact"}
        
        result = redactor.redact_document(doc, pii_config)
        
        assert result["user"]["profile"]["email"] == "john***"
    
    def test_array_field_redaction(self):
        """Test redacting fields in array elements."""
        redactor = PIIRedactor()
        doc = {
            "contacts": [
                {"name": "Alice", "email": "alice@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
                {"name": "Charlie", "email": "charlie@example.com"}
            ]
        }
        pii_config = {
            "contacts.email": "redact",
            "contacts.name": "hash"
        }
        
        result = redactor.redact_document(doc, pii_config)
        
        # Check all array elements were redacted
        for contact in result["contacts"]:
            assert "***" in contact["email"]
            assert "@example.com" not in contact["email"]
            # Hash should be 64 chars (SHA-256)
            assert len(contact["name"]) == 64
    
    def test_deeply_nested_array_redaction(self):
        """Test redacting fields in deeply nested arrays."""
        redactor = PIIRedactor()
        doc = {
            "departments": [
                {
                    "name": "Engineering",
                    "employees": [
                        {"name": "Alice", "email": "alice@acme.com"},
                        {"name": "Bob", "email": "bob@acme.com"}
                    ]
                },
                {
                    "name": "Sales",
                    "employees": [
                        {"name": "Charlie", "email": "charlie@acme.com"}
                    ]
                }
            ]
        }
        pii_config = {
            "departments.employees.email": "redact",
            "departments.employees.name": "hash"
        }
        
        result = redactor.redact_document(doc, pii_config)
        
        # Check all nested array elements were redacted
        for dept in result["departments"]:
            for emp in dept["employees"]:
                assert "***" in emp["email"]
                assert "@acme.com" not in emp["email"]
                assert len(emp["name"]) == 64
    
    def test_array_with_nested_objects(self):
        """Test redacting nested objects within arrays (like invitations.invitee.email)."""
        redactor = PIIRedactor()
        doc = {
            "invitations": [
                {
                    "invitee": {
                        "email": "alice@example.com",
                        "name": "Alice Johnson"
                    },
                    "token": "secret123"
                },
                {
                    "invitee": {
                        "email": "bob@example.com",
                        "name": "Bob Smith"
                    },
                    "token": "secret456"
                }
            ]
        }
        pii_config = {
            "invitations.invitee.email": "redact",
            "invitations.invitee.name": "redact"
        }
        
        result = redactor.redact_document(doc, pii_config)
        
        for invitation in result["invitations"]:
            assert "***" in invitation["invitee"]["email"]
            assert "@example.com" not in invitation["invitee"]["email"]
            assert "***" in invitation["invitee"]["name"]
            # Token should not be affected
            assert "secret" in invitation["token"]
    
    def test_mixed_array_and_non_array_fields(self):
        """Test redacting both array and non-array fields in same document."""
        redactor = PIIRedactor()
        doc = {
            "orgName": "Acme Corp",
            "primaryEmail": "info@acme.com",
            "contacts": [
                {"email": "alice@acme.com"},
                {"email": "bob@acme.com"}
            ]
        }
        pii_config = {
            "orgName": "hash",
            "primaryEmail": "redact",
            "contacts.email": "redact"
        }
        
        result = redactor.redact_document(doc, pii_config)
        
        # Non-array fields
        assert len(result["orgName"]) == 64  # Hash
        assert "***" in result["primaryEmail"]
        
        # Array fields
        for contact in result["contacts"]:
            assert "***" in contact["email"]
            assert "@acme.com" not in contact["email"]
    
    def test_nonexistent_field_path(self):
        """Test that redacting a nonexistent path doesn't raise errors."""
        redactor = PIIRedactor()
        doc = {"name": "John"}
        pii_config = {"contacts.email": "redact"}
        
        # Should not raise any errors
        result = redactor.redact_document(doc, pii_config)
        
        assert result["name"] == "John"
    
    def test_empty_array(self):
        """Test redacting fields in an empty array."""
        redactor = PIIRedactor()
        doc = {"contacts": []}
        pii_config = {"contacts.email": "redact"}
        
        result = redactor.redact_document(doc, pii_config)
        
        assert result["contacts"] == []
    
    def test_hash_strategy(self):
        """Test hash strategy produces consistent hashes."""
        redactor = PIIRedactor()
        doc = {"ssn": "123-45-6789"}
        pii_config = {"ssn": "hash"}
        
        result1 = redactor.redact_document(doc, pii_config)
        result2 = redactor.redact_document(doc, pii_config)
        
        # Same input should produce same hash
        assert result1["ssn"] == result2["ssn"]
        assert len(result1["ssn"]) == 64
