"""Tests for core.filters module."""

import pytest

from core.exceptions import ValidationError
from core.filters import escape_cql_string, generate_filter_condition


class TestEscapeCqlString:
    """Tests for escape_cql_string."""

    def test_no_quotes(self):
        """Verify that a string without quotes is returned unchanged."""
        assert escape_cql_string("hello") == "hello"

    def test_single_quote_doubled(self):
        """Verify that a single quote is doubled for CQL escaping."""
        assert escape_cql_string("it's") == "it''s"

    def test_multiple_quotes(self):
        """Verify that multiple single quotes are all doubled."""
        assert escape_cql_string("it's a 'test'") == "it''s a ''test''"

    def test_empty_string(self):
        """Verify that an empty string is returned unchanged."""
        assert escape_cql_string("") == ""


class TestGenerateFilterCondition:
    """generate_filter_condition produces safe CQL WHERE fragments."""

    # ── Equality ──────────────────────────────────────────────────────────

    def test_eq_string(self):
        """Verify that the 'eq' operator generates a correct equality condition for strings."""
        result = generate_filter_condition("name", "eq", "Alice")
        assert result == "\"name\" = 'Alice'"

    def test_eq_integer(self):
        """Verify that the 'eq' operator generates a correct equality condition for integers."""
        result = generate_filter_condition("age", "eq", 42)
        assert result == '"age" = 42'

    def test_eq_float(self):
        """Verify that the 'eq' operator generates a correct equality condition for floats."""
        result = generate_filter_condition("score", "eq", 3.14)
        assert result == '"score" = 3.14'

    # ── Inequality ────────────────────────────────────────────────────────

    def test_ne_string(self):
        """Verify that the 'ne' operator generates a correct inequality condition for strings."""
        result = generate_filter_condition("status", "ne", "active")
        assert result == "\"status\" != 'active'"

    # ── Comparison ────────────────────────────────────────────────────────

    def test_lt_integer(self):
        """Verify that the 'lt' operator generates a correct less-than condition for integers."""
        result = generate_filter_condition("count", "lt", 10)
        assert result == '"count" < 10'

    def test_less_alias(self):
        """Verify that the 'less' alias generates a correct less-than condition."""
        result = generate_filter_condition("count", "less", 10)
        assert result == '"count" < 10'

    def test_lte_integer(self):
        """Verify that the 'lte' operator generates a correct less-than-or-equal condition."""
        result = generate_filter_condition("count", "lte", 10)
        assert result == '"count" <= 10'

    def test_le_alias(self):
        """Verify that the 'le' alias generates a correct less-than-or-equal condition."""
        result = generate_filter_condition("count", "le", 10)
        assert result == '"count" <= 10'

    def test_gt_integer(self):
        """Verify that the 'gt' operator generates a correct greater-than condition."""
        result = generate_filter_condition("count", "gt", 5)
        assert result == '"count" > 5'

    def test_greater_alias(self):
        """Verify that the 'greater' alias generates a correct greater-than condition."""
        result = generate_filter_condition("count", "greater", 5)
        assert result == '"count" > 5'

    def test_gte_integer(self):
        """Verify that the 'gte' operator generates a correct greater-than-or-equal condition."""
        result = generate_filter_condition("count", "gte", 5)
        assert result == '"count" >= 5'

    def test_ge_alias(self):
        """Verify that the 'ge' alias generates a correct greater-than-or-equal condition."""
        result = generate_filter_condition("count", "ge", 5)
        assert result == '"count" >= 5'

    # ── IN ────────────────────────────────────────────────────────────────

    def test_in_integers(self):
        """Verify that the 'in' operator generates a correct IN condition for integers."""
        result = generate_filter_condition("id", "in", [1, 2, 3])
        assert result == '"id" IN (1, 2, 3)'

    def test_in_strings(self):
        """Verify that the 'in' operator generates a correct IN condition for strings."""
        result = generate_filter_condition("status", "in", ["a", "b"])
        assert result == "\"status\" IN ('a', 'b')"

    # ── CONTAINS / NOT CONTAINS ───────────────────────────────────────────

    def test_contains(self):
        """Verify that the 'contains' operator generates a correct CONTAINS condition."""
        result = generate_filter_condition("tags", "contains", "iot")
        assert result == "\"tags\" CONTAINS 'iot'"

    def test_not_contains(self):
        """Verify that the 'not_contains' operator generates a correct NOT CONTAINS condition."""
        result = generate_filter_condition("tags", "not_contains", "old")
        assert result == "\"tags\" NOT CONTAINS 'old'"

    # ── Invalid operator → empty string ───────────────────────────────────

    def test_unknown_operator_returns_empty(self):
        """Verify that an unknown operator returns an empty string."""
        assert generate_filter_condition("x", "LIKE", "foo") == ""

    # ── SQL injection prevention ──────────────────────────────────────────

    def test_string_value_escaped(self):
        """Verify that string values are properly escaped to prevent SQL injection."""
        result = generate_filter_condition("name", "eq", "O'Brien")
        assert result == "\"name\" = 'O''Brien'"

    def test_invalid_column_name_rejected(self):
        """Verify that an invalid column name raises a ValidationError."""
        with pytest.raises(ValidationError):
            generate_filter_condition('"injected"', "eq", "val")

    def test_empty_column_name_rejected(self):
        """Verify that an empty column name raises a ValidationError."""
        with pytest.raises(ValidationError):
            generate_filter_condition("", "eq", "val")
