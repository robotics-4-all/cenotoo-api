import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from core.validators import validate_cql_identifier
from dependencies import (
    check_project_exists,
    generate_filter_condition,
    get_organization_id,
    verify_write_access,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.schema_utils import unflatten_data

logger = logging.getLogger(__name__)

session = get_cassandra_session()


router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Get Data"


class Filter(BaseModel):
    """Filter condition for querying data."""

    property_name: str
    operator: str
    property_value: Any
    operands: list["Filter"] | None = None


class OrderBy(BaseModel):
    """Ordering specification for query results."""

    field: str
    order: str  # 'asc' or 'desc'


@router.get(
    "/projects/{project_id}/collections/{collection_id}/get_data",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_write_access)],
)
async def get_data_from_collection(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    attributes: list[str] = Query(None),
    filters: str | None = Query(None),
    order_by: str | None = Query(None),
    nested: bool = Query(True),
    offset: int = Query(0, ge=0),
    limit: int | None = Query(None, description="Maximum number of records to return"),
):
    """Retrieve data from a collection with optional filtering and pagination."""
    organization_id = get_organization_id()
    # Get names from IDs
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = get_collection_by_id(
        collection_id, project_id, organization_id
    ).collection_name

    # Form the keyspace and table names
    validate_cql_identifier(organization_name, "keyspace")
    validate_cql_identifier(project_name, "project")
    validate_cql_identifier(collection_name, "collection")
    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    # Get schema information
    schema_query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name=%s AND table_name=%s
    """

    # Debug information
    print(
        f"Looking for schema with: keyspace={organization_name}, "
        f"table={project_name}_{collection_name}"
    )

    # IMPORTANT: For system_schema queries, Cassandra requires EXACT case
    # matching
    rows = session.execute(schema_query, (organization_name, f"{project_name}_{collection_name}"))
    schema = {row.column_name: row.type for row in rows}

    # If no schema found, try fallbacks with different case combinations
    if not schema:
        print("No schema found with exact case, trying alternative cases...")

        # Try all lowercase
        rows = session.execute(
            schema_query,
            (organization_name.lower(), f"{project_name.lower()}_{collection_name.lower()}"),
        )
        schema = {row.column_name: row.type for row in rows}

        # If still empty, check if table exists at all
        if not schema:
            table_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s"
            tables = [r.table_name for r in session.execute(table_query, (organization_name,))]
            print(f"Tables in keyspace: {tables}")

            # Only raise error if we need to validate attributes
            if attributes:
                raise HTTPException(
                    status_code=422,
                    detail=f"Collection '{collection_name}' does not have a defined schema",
                )

    # Validate attributes
    if attributes:
        invalid_attrs = [attr for attr in attributes if attr not in schema]
        if invalid_attrs:
            raise HTTPException(
                status_code=422, detail=f"Attributes not in schema: {', '.join(invalid_attrs)}"
            )

    # Build query
    if attributes:
        # Quote each attribute name to handle special characters
        quoted_attributes = [f'"{attr}"' for attr in attributes]
        select_clause = ", ".join(quoted_attributes)
    else:
        select_clause = "*"
    query = f"SELECT {select_clause} FROM {keyspace_name}.{table_name}"

    # Handle filters
    conditions = []
    if filters:
        try:
            filters_list = json.loads(filters)
            for f in filters_list:
                if f["operator"].lower() == "or":
                    or_conditions = []
                    for operand in f["operands"]:
                        filter_condition = generate_filter_condition(
                            operand["property_name"], operand["operator"], operand["property_value"]
                        )
                        # Only add non-empty filter conditions
                        if filter_condition:
                            or_conditions.append(filter_condition)
                    # Only add OR clause if there are valid conditions
                    if or_conditions:
                        conditions.append(f"({' OR '.join(or_conditions)})")
                else:
                    filter_condition = generate_filter_condition(
                        f["property_name"], f["operator"], f["property_value"]
                    )
                    # Only add non-empty filter conditions
                    if filter_condition:
                        conditions.append(filter_condition)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=422, detail="Invalid filter format") from exc

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ALLOW FILTERING"

    # Execute query
    try:
        results = session.execute(query)
        # Convert results to list
        all_results = []
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
                # Convert DECIMAL values to float for better JSON serialization
                value = row[i]
                if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
                    value = float(value)
                row_dict[original_name] = value
            all_results.append(row_dict)

        # Handle ordering in Python BEFORE applying limit
        if order_by:
            try:
                order_by_dict = json.loads(order_by)
                if order_by_dict["field"] not in schema:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Order field '{order_by_dict['field']}' not in schema",
                    )
                field = order_by_dict["field"]

                def sort_key(x: dict, f: str = field):  # type: ignore[assignment]
                    val = x.get(f)
                    return val if val is not None else ""

                all_results.sort(
                    key=sort_key,  # type: ignore[arg-type]
                    reverse=(order_by_dict["order"].lower() == "desc"),
                )
            except json.JSONDecodeError as exc:
                raise HTTPException(status_code=422, detail="Invalid order_by format") from exc

        total_count = len(all_results)
        all_results = all_results[offset:]
        results_list = all_results[:limit] if limit else all_results

        if nested:
            results_list = [unflatten_data(item) for item in results_list]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}") from e

    response = {"data": results_list, "total_count": total_count}

    return response
