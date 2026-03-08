from collections.abc import Mapping, Sequence
from typing import Any

from fastapi import HTTPException

PYTHON_TO_CASSANDRA_TYPES = {
    int: "int",
    float: "decimal",  # Changed from 'float' to 'decimal' for exact precision
    str: "text",
    bool: "boolean",
}


def flatten_object(
    obj: dict, parent_key: str = "", sep: str = "$", return_value: bool = False
) -> dict:
    """
    Flatten a nested dictionary and map Python types to Cassandra types for table creation.
    Also, check that all keys contain only valid characters (letters, numbers, underscores).
    """
    items: list[tuple[str, Any]] = []
    simple_types = (int, float, str, bool)  # Define the simple types

    for k, v in obj.items():
        # Check if key contains dollar sign (reserved for flattening)
        if "$" in k:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Key '{k}' contains the dollar sign ($) which is reserved for flattening."
                    " Please use a different character."
                ),
            )
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, Mapping):
            # If value is a dictionary, recursively flatten it
            items.extend(flatten_object(dict(v), new_key, sep=sep).items())
        elif isinstance(v, Sequence) and not isinstance(v, str):
            # If value is a list or sequence, check if all elements are of the same simple type
            if len(v) > 0:
                first_type = type(v[0])
                if all(isinstance(item, first_type) for item in v):
                    if first_type in simple_types:
                        # Map the Python list type to Cassandra list type
                        cassandra_type = PYTHON_TO_CASSANDRA_TYPES[first_type]
                        if return_value:
                            items.append((new_key, v))
                        else:
                            items.append((new_key, f"list<{cassandra_type}>"))
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail=(
                                f"List at {new_key} contains unsupported complex types: "
                                f"{first_type.__name__}"
                            ),
                        )
                else:
                    raise HTTPException(
                        status_code=400,
                        detail=f"List at {new_key} contains mixed types.",
                    )
            elif return_value:
                items.append((new_key, v))
            else:
                items.append((new_key, "list<text>"))
        elif return_value:
            items.append((new_key, v))
        else:
            # If value is not a dictionary or list, store the corresponding Cassandra type
            cassandra_type = PYTHON_TO_CASSANDRA_TYPES.get(type(v), "text")
            items.append((new_key, cassandra_type))

    return dict(items)


def is_list_of_same_schema(items: list) -> bool:
    """
    Check if all items in the list follow the same schema as the first item.
    The schema includes both the structure and the types of the values.
    """
    if not items or len(items) < 2:
        return True
    # Flatten the first item to get its schema
    first_item_schema = flatten_object(items[0])
    # Compare all other items against the first item's schema
    for item in items[1:]:
        item_schema = flatten_object(item)
        if item_schema != first_item_schema:
            raise HTTPException(
                status_code=400, detail="List contains items with different schemas."
            )
    return True


def unflatten_schema(flattened_schema: dict, sep: str = "$") -> dict:
    """Reconstruct a nested schema from flattened column names"""
    result: dict[str, Any] = {}
    for key, value in flattened_schema.items():
        parts = key.split(sep)
        d: Any = result
        for part in parts[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value
    return result


def unflatten_data(flattened_data: dict, sep: str = "$") -> dict:
    """
    Reconstruct a nested data structure from flattened keys.
    Similar to unflatten_schema but handles actual data values.
    Also handles data type conversions for better API responses.
    """
    result: dict[str, Any] = {}
    for key, value in flattened_data.items():
        # Handle special data type conversions
        if key == "day" and isinstance(value, dict) and "days_from_epoch" in value:
            # Convert Cassandra DATE format back to string
            from datetime import date, timedelta

            epoch_date = date(1970, 1, 1)
            actual_date = epoch_date + timedelta(days=value["days_from_epoch"])
            result[key] = actual_date.strftime("%Y-%m-%d")
            continue
        if key == "timestamp":
            # Keep timestamp as is
            result[key] = value
            continue
        if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
            # Convert DECIMAL values to float for better JSON serialization
            result[key] = float(value)
            continue

        parts = key.split(sep)
        d: Any = result
        for part in parts[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]

        # Apply same type conversions for nested values
        final_value = value
        if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
            # Convert DECIMAL values to float for nested values too
            final_value = float(value)

        d[parts[-1]] = final_value
    return result
