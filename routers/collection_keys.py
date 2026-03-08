"""Collection key statistics endpoints.

This module provides API endpoints for retrieving statistics about specific
keys (stations) within collections.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException

from dependencies import check_project_exists, get_organization_id, verify_endpoint_access
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

session = get_cassandra_session()
router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Collection Keys"


@router.get(
    "/projects/{project_id}/collections/{collection_id}/keys/{key}/stats",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def get_key_statistics(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    key: str,
):
    """Retrieve statistics for a specific key in a collection."""
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

    try:
        # Get basic statistics
        stats_query = f"""
            SELECT COUNT(*) as record_count,
                   MIN("timestamp") as first_timestamp,
                   MAX("timestamp") as last_timestamp
            FROM {keyspace_name}.{table_name}
            WHERE "key" = %s
            ALLOW FILTERING
        """

        stats_result = session.execute(stats_query, [key])
        stats = stats_result.one()

        if stats.record_count == 0:
            raise HTTPException(status_code=404, detail=f"No data found for key '{key}'")

        # Get daily record counts
        daily_query = f"""
            SELECT "day", COUNT(*) as daily_count
            FROM {keyspace_name}.{table_name}
            WHERE "key" = %s
            GROUP BY "day"
            ALLOW FILTERING
        """

        daily_results = session.execute(daily_query, [key])
        daily_counts = [
            {
                "date": row.day.strftime("%Y-%m-%d"),
                "record_count": row.daily_count,
            }
            for row in daily_results
        ]

        return {
            "key": key,
            "collection_name": collection_name,
            "total_records": stats.record_count,
            "first_timestamp": stats.first_timestamp.isoformat() if stats.first_timestamp else None,
            "last_timestamp": stats.last_timestamp.isoformat() if stats.last_timestamp else None,
            "daily_breakdown": daily_counts,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve statistics for key '{key}': {str(e)}"
        ) from e
