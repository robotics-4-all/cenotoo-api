import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from core.filters import escape_cql_string
from dependencies import (
    aggregate_data,
    check_project_exists,
    generate_filter_condition,
    get_organization_id,
    verify_write_access,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

logger = logging.getLogger(__name__)

session = get_cassandra_session()

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Get Data Statistics"


@router.get(
    "/projects/{project_id}/collections/{collection_id}/statistics",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_write_access)],
)
async def get_collection_statistics(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    attribute: str = Query(None, description="Attribute to perform statistics on"),
    stat: str = Query(
        "avg",
        enum=["avg", "max", "min", "sum", "count", "distinct"],
        description="Statistical operation to perform",
    ),
    interval: str = "every_2_days",
    start_time: str = Query(None, description="Start time in format YYYY-MM-DDTHH:MM:SSZ"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DDTHH:MM:SSZ"),
    filters: str | None = Query(None),
    order: str | None = Query(None, enum=["asc", "desc"]),
    group_by: str | None = Query(None),
    limit: int | None = Query(None, description="Maximum number of results to return"),
):
    """Retrieve aggregated statistics for a collection's data."""
    del limit
    organization_id = get_organization_id()
    # Get names from IDs
    organization = get_organization_by_id(organization_id)
    organization_name = organization.organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = get_collection_by_id(
        collection_id, project_id, organization_id
    ).collection_name

    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    # Handle distinct operation differently
    if stat == "distinct":
        if not attribute:
            raise HTTPException(status_code=422, detail="attribute is required when stat=distinct")

        try:
            # For distinct operations, we return distinct values with min/max timestamps per key
            # Get key, attribute, and timestamp for grouping and analysis

            query = f'SELECT "key", "{attribute}", "timestamp" FROM {keyspace_name}.{table_name}'

            conditions = []
            params: list[Any] = []

            # Add other filters if provided
            if filters:
                filters_list = json.loads(filters)
                for f in filters_list:
                    if f["operator"] == "or":
                        or_conditions = []
                        for operand in f["operands"]:
                            filter_condition = generate_filter_condition(
                                operand["property_name"],
                                operand["operator"],
                                operand["property_value"],
                            )
                            if filter_condition:
                                or_conditions.append(filter_condition)
                        if or_conditions:
                            conditions.append(f"({' OR '.join(or_conditions)})")
                    else:
                        filter_condition = generate_filter_condition(
                            f["property_name"], f["operator"], f["property_value"]
                        )
                        if filter_condition:
                            conditions.append(filter_condition)

            # Add time conditions
            if start_time and end_time:
                conditions.append(
                    f"\"timestamp\" >= '{start_time}' AND \"timestamp\" <= '{end_time}'"
                )
            elif start_time:
                conditions.append(f"\"timestamp\" >= '{escape_cql_string(start_time)}'")
            elif end_time:
                conditions.append(f"\"timestamp\" <= '{end_time}'")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            # Don't use LIMIT for distinct operations to ensure we get all data
            # We need to process all records to find accurate distinct values
            # per key
            query += " ALLOW FILTERING"

            # Execute query and get values
            results = session.execute(query, params)

            # Group data by key and track distinct values with timestamps
            key_data = {}

            for row in results:
                # Handle column name conversion
                column_name = (
                    attribute.replace("-", "_")
                    .replace("@", "_")
                    .replace("%", "")
                    .replace(".", "_")
                    .replace("/", "_")
                    .replace(":", "_")
                    .replace(" ", "_")
                    .replace("$", "_")
                )
                value = getattr(row, column_name)
                key = row.key
                timestamp = row.timestamp

                # Convert DECIMAL values to float for better JSON serialization
                if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
                    value = float(value)

                # Convert to string for comparison (handles None values too)
                value_str = str(value) if value is not None else "null"

                # Initialize key data if not exists
                if key not in key_data:
                    key_data[key] = {
                        "distinct_values": {},
                        "timestamps": [],
                        "min_timestamp": timestamp,
                        "max_timestamp": timestamp,
                    }

                # Track distinct values and their counts
                if value_str not in key_data[key]["distinct_values"]:
                    key_data[key]["distinct_values"][value_str] = {
                        "value": value,
                        "count": 1,
                    }
                else:
                    key_data[key]["distinct_values"][value_str]["count"] += 1

                # Update min/max timestamps for this key
                key_data[key]["min_timestamp"] = min(key_data[key]["min_timestamp"], timestamp)
                key_data[key]["max_timestamp"] = max(key_data[key]["max_timestamp"], timestamp)

                key_data[key]["timestamps"].append(timestamp)

            # Build response with statistics for each key
            key_stats = []
            for key, data in key_data.items():
                key_stats.append(
                    {
                        "key": key,
                        "min_timestamp": data["min_timestamp"].isoformat(),
                        "max_timestamp": data["max_timestamp"].isoformat(),
                        "total_records": len(data["timestamps"]),
                    }
                )

            # Sort keys if needed
            if order:
                key_stats.sort(key=lambda x: x["key"], reverse=order == "desc")

            return {
                "collection_name": collection_name,
                "stat": "distinct",
                "attribute": attribute,
                "total_keys": len(key_stats),
                "key_statistics": key_stats,
            }

        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to retrieve distinct values: {str(e)}"
            ) from e

    # For regular statistics, attribute is required
    if not attribute:
        raise HTTPException(
            status_code=422, detail="Attribute is required for non-distinct statistics"
        )

    # Get schema information

    # For regular statistics, attribute is required
    if not attribute:
        raise HTTPException(
            status_code=422, detail="Attribute is required for non-distinct statistics"
        )

    # Get schema information
    schema_query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name=%s AND table_name=%s
    """
    # Try with exact case first
    rows = session.execute(schema_query, (organization_name, f"{project_name}_{collection_name}"))
    schema = {row.column_name: row.type for row in rows}

    # If no schema found, try with lowercase as fallback
    if not schema:
        rows = session.execute(
            schema_query,
            (organization_name.lower(), f"{project_name.lower()}_{collection_name.lower()}"),
        )
        schema = {row.column_name: row.type for row in rows}

    # For the actual query, use proper quoting for case sensitivity
    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    # Validate attribute
    if attribute not in schema:
        raise HTTPException(
            status_code=422,
            detail=f"Attribute '{attribute}' does not exist in the collection schema.",
        )

    # Validate group_by if provided
    if group_by and group_by not in schema:
        raise HTTPException(
            status_code=422,
            detail=f"Group by field '{group_by}' does not exist in the collection schema.",
        )

    # Default group_by to 'key' if not provided
    final_group_by = group_by or "key"

    # Build the SELECT statement with quoted column names
    query = f'SELECT "{final_group_by}", timestamp, "{attribute}" FROM {keyspace_name}.{table_name}'

    # Add filters to the query
    conditions = []
    if filters:
        filters_list = json.loads(filters)
        for f in filters_list:
            if f["operator"] == "or":
                or_conditions = []
                for operand in f["operands"]:
                    filter_condition = generate_filter_condition(
                        operand["property_name"], operand["operator"], operand["property_value"]
                    )
                    if filter_condition:
                        or_conditions.append(filter_condition)
                if or_conditions:
                    conditions.append(f"({' OR '.join(or_conditions)})")
            else:
                filter_condition = generate_filter_condition(
                    f["property_name"], f["operator"], f["property_value"]
                )
                if filter_condition:
                    conditions.append(filter_condition)

    # Add WHERE conditions if there are any
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Add time conditions
    if start_time and end_time:
        if not conditions:
            query += (
                f" WHERE timestamp >= '{escape_cql_string(start_time)}'"
                f" AND timestamp <= '{escape_cql_string(end_time)}'"
            )
        else:
            query += (
                f" AND timestamp >= '{escape_cql_string(start_time)}' "
                f"AND timestamp <= '{escape_cql_string(end_time)}'"
            )

    query += " ALLOW FILTERING"

    # Execute the query and fetch data
    try:
        results = session.execute(query)
        # Instead of using _asdict() which converts column names to Python identifiers,
        # manually construct the dictionary using the original column names
        # from the schema
        results_list = []
        for row in results:
            row_dict = {}
            for i, column_name in enumerate(row._fields):
                # Find the original column name from schema that matches this
                # Python identifier
                original_name = column_name
                for schema_col in schema:
                    # Convert schema column name to Python identifier format for comparison
                    # Replace all special characters with underscores (except %
                    # which gets removed)
                    python_identifier = (
                        schema_col.replace("-", "_")
                        .replace("@", "_")
                        # % gets completely removed, not replaced with _
                        .replace("%", "")
                        .replace(".", "_")
                        .replace("/", "_")
                        .replace(":", "_")  # Add colon support
                        .replace(" ", "_")  # Add space support
                        .replace("$", "_")
                    )
                    if python_identifier == column_name:
                        original_name = schema_col
                        break
                # Convert DECIMAL values to float for proper processing
                value = row[i]
                if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
                    value = float(value)
                row_dict[original_name] = value
            results_list.append(row_dict)

        # Group and aggregate data based on the specified interval
        if interval:
            _, every_n, units = interval.split("_")
            every_n_int = int(every_n)
            if units not in ["minutes", "hours", "days", "weeks", "months"]:
                raise HTTPException(
                    status_code=422, detail="The unit for the interval isn't supported"
                )
            aggregated_data = aggregate_data(
                results_list, every_n_int, units, stat, attribute, final_group_by
            )
        else:
            aggregated_data = results_list

    except Exception:
        logger.error("Query execution failed", exc_info=True)
        return []

    if order:
        aggregated_data.sort(key=lambda x: x.get(f"{stat}_{attribute}"), reverse=order == "desc")

    return aggregated_data
