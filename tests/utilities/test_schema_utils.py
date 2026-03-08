import pytest
from fastapi import HTTPException

from utilities.schema_utils import (
    flatten_object,
    is_list_of_same_schema,
    unflatten_data,
    unflatten_schema,
)


class TestFlattenObject:
    """Tests for flatten_object."""

    def test_flat_dict(self):
        """Verify flatten_object correctly flattens a flat dictionary."""
        result = flatten_object({"temp": 25.0, "name": "sensor1"})
        assert result == {"temp": "decimal", "name": "text"}

    def test_nested_dict(self):
        """Verify flatten_object correctly flattens a nested dictionary."""
        result = flatten_object({"location": {"lat": 1.0, "lon": 2.0}})
        assert result == {"location$lat": "decimal", "location$lon": "decimal"}

    def test_deeply_nested(self):
        """Verify flatten_object correctly flattens a deeply nested dictionary."""
        result = flatten_object({"a": {"b": {"c": 1}}})
        assert result == {"a$b$c": "int"}

    def test_boolean_type(self):
        """Verify flatten_object correctly maps boolean type."""
        result = flatten_object({"active": True})
        assert result == {"active": "boolean"}

    def test_integer_type(self):
        """Verify flatten_object correctly maps integer type."""
        result = flatten_object({"count": 42})
        assert result == {"count": "int"}

    def test_list_of_ints(self):
        """Verify flatten_object correctly maps list of integers."""
        result = flatten_object({"values": [1, 2, 3]})
        assert result == {"values": "list<int>"}

    def test_list_of_strings(self):
        """Verify flatten_object correctly maps list of strings."""
        result = flatten_object({"tags": ["a", "b"]})
        assert result == {"tags": "list<text>"}

    def test_empty_list(self):
        """Verify flatten_object correctly maps an empty list."""
        result = flatten_object({"empty": []})
        assert result == {"empty": "list<text>"}

    def test_return_value_mode(self):
        """Verify flatten_object returns values instead of types when return_value is True."""
        result = flatten_object({"temp": 25.0}, return_value=True)
        assert result == {"temp": 25.0}

    def test_return_value_mode_list(self):
        """Verify flatten_object returns list values when return_value is True."""
        result = flatten_object({"values": [1, 2, 3]}, return_value=True)
        assert result == {"values": [1, 2, 3]}

    def test_return_value_mode_empty_list(self):
        """Verify flatten_object returns empty list when return_value is True."""
        result = flatten_object({"empty": []}, return_value=True)
        assert result == {"empty": []}

    def test_nested_return_value_flat_only(self):
        """Verify flatten_object returns flat values when return_value is True."""
        result = flatten_object({"x": 42}, return_value=True)
        assert result == {"x": 42}

    def test_dollar_sign_in_key_raises(self):
        """Verify flatten_object raises HTTP 400 when key contains a dollar sign."""
        with pytest.raises(HTTPException) as exc_info:
            flatten_object({"price$amount": 10})
        assert exc_info.value.status_code == 400
        assert "dollar sign" in exc_info.value.detail.lower()

    def test_mixed_type_list_raises(self):
        """Verify flatten_object raises HTTP 400 for mixed type lists."""
        with pytest.raises(HTTPException) as exc_info:
            flatten_object({"mixed": [1, "two"]})
        assert exc_info.value.status_code == 400

    def test_list_of_dicts_raises(self):
        """Verify flatten_object raises HTTP 400 for lists of dictionaries."""
        with pytest.raises(HTTPException) as exc_info:
            flatten_object({"items": [{"a": 1}]})
        assert exc_info.value.status_code == 400


class TestUnflattenSchema:
    """Tests for unflatten_schema."""

    def test_flat_keys_unchanged(self):
        """Verify unflatten_schema leaves flat keys unchanged."""
        result = unflatten_schema({"temp": "decimal", "name": "text"})
        assert result == {"temp": "decimal", "name": "text"}

    def test_nested_keys(self):
        """Verify unflatten_schema correctly unflattens nested keys."""
        result = unflatten_schema({"location$lat": "decimal", "location$lon": "decimal"})
        assert result == {"location": {"lat": "decimal", "lon": "decimal"}}

    def test_deep_nesting(self):
        """Verify unflatten_schema correctly unflattens deeply nested keys."""
        result = unflatten_schema({"a$b$c": "int"})
        assert result == {"a": {"b": {"c": "int"}}}


class TestUnflattenData:
    """Tests for unflatten_data."""

    def test_flat_data_unchanged(self):
        """Verify unflatten_data leaves flat data unchanged."""
        result = unflatten_data({"temp": 25.0, "name": "sensor"})
        assert result == {"temp": 25.0, "name": "sensor"}

    def test_nested_data(self):
        """Verify unflatten_data correctly unflattens nested data."""
        result = unflatten_data({"location$lat": 1.0, "location$lon": 2.0})
        assert result == {"location": {"lat": 1.0, "lon": 2.0}}

    def test_timestamp_preserved(self):
        """Verify unflatten_data preserves timestamp values."""
        result = unflatten_data({"timestamp": "2024-01-01T00:00:00Z"})
        assert result["timestamp"] == "2024-01-01T00:00:00Z"

    def test_decimal_converted_to_float(self):
        """Verify unflatten_data converts Decimal to float."""
        from decimal import Decimal

        result = unflatten_data({"value": Decimal("25.5")})
        assert result["value"] == 25.5
        assert isinstance(result["value"], float)

    def test_cassandra_date_format_converted(self):
        """Verify unflatten_data converts Cassandra date format to string."""
        result = unflatten_data({"day": {"days_from_epoch": 19723}})
        assert result["day"] == "2024-01-01"

    def test_nested_decimal_converted(self):
        """Verify unflatten_data converts nested Decimal to float."""
        from decimal import Decimal

        result = unflatten_data({"reading": Decimal("10.3")})
        assert result["reading"] == 10.3
        assert isinstance(result["reading"], float)


class TestIsListOfSameSchema:
    """Tests for is_list_of_same_schema."""

    def test_empty_list(self):
        """Verify is_list_of_same_schema returns True for an empty list."""
        assert is_list_of_same_schema([]) is True

    def test_single_item(self):
        """Verify is_list_of_same_schema returns True for a single item list."""
        assert is_list_of_same_schema([{"a": 1}]) is True

    def test_same_schema(self):
        """Verify is_list_of_same_schema returns True for items with the same schema."""
        assert is_list_of_same_schema([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]) is True

    def test_different_schema_raises(self):
        """Verify is_list_of_same_schema raises HTTP 400 for items with different schemas."""
        with pytest.raises(HTTPException) as exc_info:
            is_list_of_same_schema([{"a": 1}, {"a": 1, "b": 2}])
        assert exc_info.value.status_code == 400
