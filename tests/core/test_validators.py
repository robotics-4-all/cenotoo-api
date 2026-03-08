"""Tests for core.validators module."""

import pytest

from core.exceptions import ValidationError
from core.validators import contains_special_characters, validate_cql_identifier

# ──────────────────────────────────────────────────────────────────────────────
# validate_cql_identifier
# ──────────────────────────────────────────────────────────────────────────────


class TestValidateCqlIdentifier:
    """validate_cql_identifier accepts safe names and rejects unsafe ones."""

    def test_valid_alphanumeric(self):
        """Verify that alphanumeric identifiers are accepted."""
        assert validate_cql_identifier("my_table_1") == "my_table_1"

    def test_valid_hyphen(self):
        """Verify that identifiers with hyphens are accepted."""
        assert validate_cql_identifier("my-table") == "my-table"

    def test_valid_uppercase(self):
        """Verify that identifiers with uppercase letters are accepted."""
        assert validate_cql_identifier("MyTable") == "MyTable"

    def test_valid_pure_digits(self):
        """Verify that identifiers consisting only of digits are accepted."""
        assert validate_cql_identifier("123") == "123"

    def test_rejects_empty_string(self):
        """Verify that an empty string identifier raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("")

    def test_rejects_whitespace_only(self):
        """Verify that a whitespace-only identifier raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("   ")

    def test_rejects_semicolon(self):
        """Verify that an identifier containing a semicolon raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("table; DROP")

    def test_rejects_single_quote(self):
        """Verify that an identifier containing a single quote raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("table'name")

    def test_rejects_double_quote(self):
        """Verify that an identifier containing a double quote raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier('table"name')

    def test_rejects_space(self):
        """Verify that an identifier containing a space raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("my table")

    def test_rejects_dot(self):
        """Verify that an identifier containing a dot raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("keyspace.table")

    def test_rejects_dollar_sign(self):
        """Verify that an identifier containing a dollar sign raises a ValidationError."""
        with pytest.raises(ValidationError):
            validate_cql_identifier("price$field")

    def test_custom_label_in_error(self):
        """Verify that a custom label is included in the ValidationError message."""
        with pytest.raises(ValidationError, match="keyspace"):
            validate_cql_identifier("bad name", label="keyspace")


# ──────────────────────────────────────────────────────────────────────────────
# contains_special_characters
# ──────────────────────────────────────────────────────────────────────────────


class TestContainsSpecialCharacters:
    """contains_special_characters blocks $ and empty strings."""

    def test_empty_string_returns_true(self):
        """Verify that an empty string is considered to contain special characters."""
        assert contains_special_characters("") is True

    def test_whitespace_only_returns_true(self):
        """Verify that a whitespace-only string is considered to contain special characters."""
        assert contains_special_characters("   ") is True

    def test_dollar_sign_returns_true(self):
        """Verify that a string with a dollar sign is considered to have special characters."""
        assert contains_special_characters("price$amount") is True

    def test_plain_text_returns_false(self):
        """Verify that plain text is not considered to have special characters."""
        assert contains_special_characters("hello") is False

    def test_special_chars_allowed_by_default(self):
        """Verify that special characters are allowed by default."""
        # allow_special_chars=True means anything goes (except $)
        assert contains_special_characters("hello@world") is False

    def test_special_chars_disallowed(self):
        """Verify that special characters are correctly identified when disallowed."""
        assert (
            contains_special_characters(
                "hello@world",
                allow_special_chars=False,
            )
            is True
        )

    def test_underscores_allowed(self):
        """Verify that underscores are allowed when explicitly permitted."""
        assert (
            contains_special_characters(
                "hello_world",
                allow_special_chars=False,
                allow_underscores=True,
            )
            is False
        )

    def test_spaces_allowed(self):
        """Verify that spaces are allowed when explicitly permitted."""
        assert (
            contains_special_characters(
                "hello world",
                allow_special_chars=False,
                allow_spaces=True,
                allow_underscores=True,
            )
            is False
        )

    def test_spaces_disallowed(self):
        """Verify that spaces are correctly identified when disallowed."""
        assert (
            contains_special_characters(
                "hello world",
                allow_special_chars=False,
                allow_spaces=False,
                allow_underscores=True,
            )
            is True
        )
