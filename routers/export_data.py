import datetime
import io
import logging
import uuid

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from core.validators import validate_cql_identifier
from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Data Export"
logger = logging.getLogger(__name__)
session = get_cassandra_session()


@router.get(
    "/projects/{project_id}/collections/{collection_id}/export",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists)],
)
def export_data(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    format: str = Query("csv", description="Export format: csv or parquet"),
    limit: int = Query(10000, le=100000, description="Max rows to export"),
    offset: int = Query(0, description="Rows to skip"),
    organization_id: uuid.UUID = Depends(get_organization_id),
    _=Depends(verify_endpoint_access),
):
    if format not in ("csv", "parquet"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Format must be csv or parquet"
        )

    collection = get_collection_by_id(collection_id, project_id, organization_id)
    if not collection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found")

    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = collection.collection_name

    validate_cql_identifier(organization_name, "keyspace")
    validate_cql_identifier(project_name, "project")
    validate_cql_identifier(collection_name, "collection")

    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    query = f"SELECT * FROM {keyspace_name}.{table_name} LIMIT {limit + offset} ALLOW FILTERING"

    try:
        results = session.execute(query)
        column_names = list(results.column_names)
        all_results = []
        for row in results:
            row_dict = {}
            for i, column_name in enumerate(row._fields):
                value = row[i]
                if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
                    value = float(value)
                elif isinstance(value, uuid.UUID):
                    value = str(value)
                elif isinstance(value, (datetime.datetime, datetime.date)):
                    value = value.isoformat()
                elif (
                    not isinstance(value, (bool, int, float, str, type(None)))
                    and hasattr(value, "__class__")
                    and "Date" in value.__class__.__name__
                ):
                    value = str(value)
                row_dict[column_name] = value
            all_results.append(row_dict)

        all_results = all_results[offset:]

        df = pd.DataFrame(all_results, columns=column_names if not all_results else None)

        if format == "csv":
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            return StreamingResponse(
                iter([buf.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{collection_name}.csv"'},
            )
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{collection_name}.parquet"'},
        )

    except Exception as e:
        logger.error("Export failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}") from e
