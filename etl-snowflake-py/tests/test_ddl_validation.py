"""
Unit tests for SQL identifier validation (Issue #1 fix)

Add these tests to: etl-snowflake-py/tests/test_ddl_validation.py
"""

import pytest
from etl_snowflake.ddl import validate_identifier


class TestIdentifierValidation:
    """Test SQL identifier validation to prevent injection attacks."""

    def test_valid_identifiers(self):
        """Test that valid identifiers are accepted."""
        assert validate_identifier("table_name") == "table_name"
        assert validate_identifier("_private") == "_private"
        assert validate_identifier("Table123") == "Table123"
        assert validate_identifier("my_table_2") == "my_table_2"
        assert validate_identifier("a" * 255) == "a" * 255  # Max length

    def test_reject_empty_identifier(self):
        """Test that empty identifiers are rejected."""
        with pytest.raises(ValueError, match="Empty"):
            validate_identifier("")

        with pytest.raises(ValueError, match="Empty"):
            validate_identifier("   ")

    def test_reject_invalid_start_character(self):
        """Test that identifiers starting with numbers are rejected."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("123table")

        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("9table")

    def test_reject_special_characters(self):
        """Test that special characters are rejected."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("table-name")

        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("table.name")

        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("table;name")

        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("table'name")

        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("table name")  # Space

    def test_reject_sql_injection_attempts(self):
        """Test that SQL injection attempts are blocked."""
        with pytest.raises(ValueError):
            validate_identifier("table'; DROP TABLE users--")

        with pytest.raises(ValueError):
            validate_identifier('table"; DELETE FROM data;--')

        with pytest.raises(ValueError):
            validate_identifier("'; OR '1'='1")

    def test_reject_dangerous_keywords(self):
        """Test that dangerous SQL keywords are rejected."""
        dangerous = ["DROP", "DELETE", "TRUNCATE", "ALTER", "GRANT", "REVOKE"]

        for keyword in dangerous:
            with pytest.raises(ValueError, match="dangerous SQL keyword"):
                validate_identifier(keyword)

            with pytest.raises(ValueError, match="dangerous SQL keyword"):
                validate_identifier(keyword.lower())

    def test_reject_too_long_identifier(self):
        """Test that identifiers over 255 characters are rejected."""
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("a" * 256)

    def test_identifier_type_in_error_message(self):
        """Test that error messages include identifier type."""
        with pytest.raises(ValueError, match="table_name"):
            validate_identifier("invalid!", "table_name")

        with pytest.raises(ValueError, match="column_name"):
            validate_identifier("123col", "column_name")


class TestDDLWithValidation:
    """Test DDL operations use validation."""

    def test_create_landing_table_validates_table_name(self):
        """Test that create_landing_table validates table name."""
        # This requires mocking SnowflakeDDL
        # Placeholder for integration test
        pass

    def test_create_landing_table_validates_column_names(self):
        """Test that create_landing_table validates column names."""
        # This requires mocking SnowflakeDDL
        # Placeholder for integration test
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
