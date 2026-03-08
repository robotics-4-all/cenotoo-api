from typing import Any

from core.exceptions import ValidationError

_VALID_OPERATORS = {
    "eq",
    "ne",
    "lt",
    "less",
    "lte",
    "le",
    "gt",
    "greater",
    "gte",
    "ge",
    "in",
    "contains",
    "not_contains",
}

_COMPARISON_SYMBOLS = {
    "eq": "=",
    "ne": "!=",
    "lt": "<",
    "less": "<",
    "lte": "<=",
    "le": "<=",
    "gt": ">",
    "greater": ">",
    "gte": ">=",
    "ge": ">=",
}


def escape_cql_string(value: str) -> str:
    """Escape single quotes in a string for safe CQL execution."""
    return value.replace("'", "''")


def _validate_column_name(name: str) -> str:
    if not name or '"' in name:
        raise ValidationError(detail=f"Invalid filter property name: '{name}'")
    return name


def generate_filter_condition(prop_name: str, operator: str, prop_value: Any) -> str:
    """Generate a CQL filter condition string."""
    if operator not in _VALID_OPERATORS:
        return ""

    _validate_column_name(prop_name)
    quoted_name = f'"{prop_name}"'

    if operator in _COMPARISON_SYMBOLS:
        symbol = _COMPARISON_SYMBOLS[operator]
        if isinstance(prop_value, (int, float)):
            return f"{quoted_name} {symbol} {prop_value}"
        return f"{quoted_name} {symbol} '{escape_cql_string(str(prop_value))}'"

    if operator == "in":
        if all(isinstance(v, (int, float)) for v in prop_value):
            values = ", ".join([str(v) for v in prop_value])
        else:
            values = ", ".join([f"'{escape_cql_string(str(v))}'" for v in prop_value])
        return f"{quoted_name} IN ({values})"

    if operator == "contains":
        return f"{quoted_name} CONTAINS '{escape_cql_string(str(prop_value))}'"

    if operator == "not_contains":
        return f"{quoted_name} NOT CONTAINS '{escape_cql_string(str(prop_value))}'"

    return ""
