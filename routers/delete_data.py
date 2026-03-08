# pylint: disable=too-few-public-methods
"""Data deletion endpoints.

This module provides API endpoints for deleting data from collections
based on time ranges and specific keys.
"""

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel

from dependencies import check_project_exists, get_organization_id, verify_master_access
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

session = get_cassandra_session()
router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Delete Data"


class DeleteDataRequest(BaseModel):
    """Request model for deleting data from a collection.

    Attributes:
        key: Optional specific key to delete. If not provided, deletes all keys for the time period.
        timestamp_from: Optional start timestamp for deletion range.
        timestamp_to: Optional end timestamp for deletion range.
    """

    # Optional: if not provided, delete all keys for the time period
    key: str | None = None
    timestamp_from: str | None = None
    timestamp_to: str | None = None


@router.delete(
    "/projects/{project_id}/collections/{collection_id}/delete_data",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def delete_data_from_collection(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    delete_request: DeleteDataRequest = Body(...),
):
    """
    Delete data from a collection based on various criteria.

    Deletion scenarios:
    - Delete all data for a specific key (station)
    - Delete data for a key within a timestamp range
    - Delete all data within a timestamp range (all keys)

    Request body should contain:
    {
        "key": "key1",  // Optional: specific station name. If not provided, deletes all keys
        "timestamp_from": "2024-01-01T00:00:00Z",  // Optional: start timestamp
        "timestamp_to": "2024-01-02T00:00:00Z"     // Optional: end timestamp
    }

    Note: This endpoint requires a master API key for authorization.
    """
    organization_id = get_organization_id()

    # Get names from IDs
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = get_collection_by_id(
        collection_id, project_id, organization_id
    ).collection_name

    # Form the keyspace and table names
    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    # Validate that at least one criteria is provided
    if (
        not delete_request.key
        and not delete_request.timestamp_from
        and not delete_request.timestamp_to
    ):
        raise HTTPException(
            status_code=400,
            detail=("At least one deletion criteria must be provided (key or timestamp range)"),
        )

    try:
        # Convert timestamps to datetime objects if provided
        ts_from = None
        ts_to = None
        if delete_request.timestamp_from:
            try:
                ts_from = datetime.fromisoformat(
                    delete_request.timestamp_from.replace("Z", "+00:00")
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail="Invalid timestamp_from format. Use ISO format."
                ) from e
        if delete_request.timestamp_to:
            try:
                ts_to = datetime.fromisoformat(delete_request.timestamp_to.replace("Z", "+00:00"))
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail="Invalid timestamp_to format. Use ISO format."
                ) from e

        # Handle different deletion scenarios
        if delete_request.key:
            # Case 1: Specific key deletion
            if not ts_from and not ts_to:
                # Delete all data for this key across all days
                # We need to select both day and key for DISTINCT to work with
                # WHERE clause
                days_query = (
                    f'SELECT DISTINCT "day", "key" FROM {keyspace_name}.{table_name} '
                    'WHERE "key" = %s ALLOW FILTERING'
                )
                days_result = session.execute(days_query, [delete_request.key])
                days = [row.day for row in days_result]

                if not days:
                    return {
                        "message": "No data found for the specified key",
                        "criteria": {
                            "key": delete_request.key,
                            "timestamp_from": delete_request.timestamp_from,
                            "timestamp_to": delete_request.timestamp_to,
                        },
                    }

                # Delete data for each day
                for day in days:
                    day_delete_query = (
                        f'DELETE FROM {keyspace_name}.{table_name} WHERE "day" = %s AND "key" = %s'
                    )
                    session.execute(day_delete_query, [day, delete_request.key])

            else:
                # Delete data for specific key with timestamp filters
                # Since Cassandra doesn't allow DISTINCT with WHERE on clustering columns,
                # we scan for this specific key and extract unique days
                time_conditions = []
                time_params: list[Any] = [delete_request.key]

                if ts_from and ts_to:
                    time_conditions.append('"timestamp" >= %s AND "timestamp" <= %s')
                    time_params.extend([ts_from, ts_to])
                elif ts_from:
                    time_conditions.append('"timestamp" >= %s')
                    time_params.append(ts_from)
                elif ts_to:
                    time_conditions.append('"timestamp" <= %s')
                    time_params.append(ts_to)

                time_where = " AND ".join(time_conditions)
                # Use regular SELECT to get all matching records for this key
                scan_query = (
                    f'SELECT "day", "key" FROM {keyspace_name}.{table_name} '
                    f'WHERE "key" = %s AND {time_where} ALLOW FILTERING'
                )
                scan_result = session.execute(scan_query, time_params)

                # Extract unique days for this key
                unique_days = set()
                for row in scan_result:
                    unique_days.add(row.day)

                days = list(unique_days)

                if not days:
                    return {
                        "message": "No data found for the specified criteria",
                        "criteria": {
                            "key": delete_request.key,
                            "timestamp_from": delete_request.timestamp_from,
                            "timestamp_to": delete_request.timestamp_to,
                        },
                    }

                # Delete data for each day with timestamp conditions
                for day in days:
                    day_conditions = ['"day" = %s', '"key" = %s']
                    day_params = [day, delete_request.key]

                    if ts_from and ts_to:
                        day_conditions.append('"timestamp" >= %s AND "timestamp" <= %s')
                        day_params.extend([ts_from, ts_to])
                    elif ts_from:
                        day_conditions.append('"timestamp" >= %s')
                        day_params.append(ts_from)
                    elif ts_to:
                        day_conditions.append('"timestamp" <= %s')
                        day_params.append(ts_to)

                    day_where_clause = " AND ".join(day_conditions)
                    day_delete_query = (
                        f"DELETE FROM {keyspace_name}.{table_name} WHERE {day_where_clause}"
                    )
                    session.execute(day_delete_query, day_params)

        else:
            # Case 2: Delete data across ALL keys within timestamp range
            # This requires finding all (day, key) combinations within the
            # timestamp range
            if not ts_from and not ts_to:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "When no key is specified, at least one timestamp filter must be provided"
                    ),
                )

            # Since Cassandra doesn't allow DISTINCT with WHERE on clustering columns,
            # we need to use a different approach: scan all data and filter by timestamp
            # This will be less efficient but works within Cassandra's
            # constraints

            # Build query to scan all data within timestamp range
            time_conditions = []
            time_params = []

            if ts_from and ts_to:
                time_conditions.append('"timestamp" >= %s AND "timestamp" <= %s')
                time_params.extend([ts_from, ts_to])
            elif ts_from:
                time_conditions.append('"timestamp" >= %s')
                time_params.append(ts_from)
            elif ts_to:
                time_conditions.append('"timestamp" <= %s')
                time_params.append(ts_to)

            time_where = " AND ".join(time_conditions)
            # Use regular SELECT to get all matching records, then extract
            # unique partitions
            scan_query = (
                f'SELECT "day", "key" FROM {keyspace_name}.{table_name} '
                f"WHERE {time_where} ALLOW FILTERING"
            )
            scan_result = session.execute(scan_query, time_params)

            # Extract unique (day, key) combinations
            unique_partitions = set()
            for row in scan_result:
                unique_partitions.add((row.day, row.key))

            partitions = list(unique_partitions)

            if not partitions:
                return {
                    "message": "No data found for the specified time range",
                    "criteria": {
                        "key": delete_request.key,
                        "timestamp_from": delete_request.timestamp_from,
                        "timestamp_to": delete_request.timestamp_to,
                    },
                }

            # Delete data for each (day, key) partition with timestamp
            # conditions
            for day, key in partitions:
                day_conditions = ['"day" = %s', '"key" = %s']
                day_params = [day, key]

                if ts_from and ts_to:
                    day_conditions.append('"timestamp" >= %s AND "timestamp" <= %s')
                    day_params.extend([ts_from, ts_to])
                elif ts_from:
                    day_conditions.append('"timestamp" >= %s')
                    day_params.append(ts_from)
                elif ts_to:
                    day_conditions.append('"timestamp" <= %s')
                    day_params.append(ts_to)

                day_where_clause = " AND ".join(day_conditions)
                day_delete_query = (
                    f"DELETE FROM {keyspace_name}.{table_name} WHERE {day_where_clause}"
                )
                session.execute(day_delete_query, day_params)

        return {
            "message": "Data deleted successfully",
            "criteria": {
                "key": delete_request.key,
                "timestamp_from": delete_request.timestamp_from,
                "timestamp_to": delete_request.timestamp_to,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete data: {str(e)}") from e
