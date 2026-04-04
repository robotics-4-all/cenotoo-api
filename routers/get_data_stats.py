import json
import logging
import re
import time
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from config import settings
from core.aggregation import aggregate_data
from core.filters import generate_filter_condition_parameterized
from dependencies import (
    check_project_exists,
    generate_filter_condition,
    get_organization_id,
    verify_endpoint_access,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

logger = logging.getLogger(__name__)

session = get_cassandra_session()

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Get Data Statistics"

_INTERVAL_RE = re.compile(r"^every_(\d+)_(minutes|hours|days|weeks|months)$")

_schema_cache: dict[tuple, tuple[dict, float]] = {}
_SCHEMA_CACHE_TTL = 60.0


def _get_schema(session, org_name: str, project_name: str, collection_name: str) -> dict:
    cache_key = (org_name, project_name, collection_name)
    cached = _schema_cache.get(cache_key)
    if cached and (time.monotonic() - cached[1]) < _SCHEMA_CACHE_TTL:
        return cached[0]

    schema_query = """
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (org_name, f"{project_name}_{collection_name}"))
    schema = {row.column_name: row.type for row in rows}

    if not schema:
        rows = session.execute(
            schema_query,
            (org_name.lower(), f"{project_name.lower()}_{collection_name.lower()}"),
        )
        schema = {row.column_name: row.type for row in rows}

    _schema_cache[cache_key] = (schema, time.monotonic())
    return schema


@router.get(
    "/projects/{project_id}/collections/{collection_id}/statistics",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def get_collection_statistics(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    attribute: str = Query(None, description="Attribute to perform statistics on"),
    stat: str = Query(
        "avg",
        enum=["avg", "max", "min", "sum", "count", "distinct", "p50", "p90", "p95", "p99"],
        description="Statistical operation to perform",
    ),
    interval: str | None = Query(
        None, description="Interval for time bucketing, e.g. every_1_hours"
    ),
    start_time: str = Query(None, description="Start time in format YYYY-MM-DDTHH:MM:SSZ"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DDTHH:MM:SSZ"),
    filters: str | None = Query(None),
    order: str | None = Query(None, enum=["asc", "desc"]),
    group_by: str | None = Query(None),
    limit: int | None = Query(None, description="Maximum number of results to return"),
):
    """Retrieve aggregated statistics for a collection's data."""
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

    ts_from: datetime | None = None
    ts_to: datetime | None = None
    if start_time:
        try:
            ts_from = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail="Invalid start_time format. Use ISO 8601."
            ) from exc
    if end_time:
        try:
            ts_to = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail="Invalid end_time format. Use ISO 8601."
            ) from exc

    if interval and not _INTERVAL_RE.match(interval):
        raise HTTPException(
            status_code=422,
            detail="interval must be in format 'every_N_<minutes|hours|days|weeks|months>'",
        )

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

            if ts_from and ts_to:
                conditions.append('"timestamp" >= %s AND "timestamp" <= %s')
                params.extend([ts_from, ts_to])
            elif ts_from:
                conditions.append('"timestamp" >= %s')
                params.append(ts_from)
            elif ts_to:
                conditions.append('"timestamp" <= %s')
                params.append(ts_to)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += f" LIMIT {settings.max_stats_rows}"

            query += " ALLOW FILTERING"

            results = session.execute(query, params)

            results_list = list(results)

            if len(results_list) >= settings.max_stats_rows:
                raise HTTPException(
                    status_code=400,
                    detail=f"Result set too large (≥{settings.max_stats_rows} rows). Narrow your time range or add filters.",
                )

            key_data: dict[str, Any] = {}

            for row in results_list:
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

                if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
                    value = float(value)

                value_str = str(value) if value is not None else "null"

                if key not in key_data:
                    key_data[key] = {
                        "distinct_values": {},
                        "timestamps": [],
                        "min_timestamp": timestamp,
                        "max_timestamp": timestamp,
                    }

                if value_str not in key_data[key]["distinct_values"]:
                    key_data[key]["distinct_values"][value_str] = {
                        "value": value,
                        "count": 1,
                    }
                else:
                    key_data[key]["distinct_values"][value_str]["count"] += 1

                key_data[key]["min_timestamp"] = min(key_data[key]["min_timestamp"], timestamp)
                key_data[key]["max_timestamp"] = max(key_data[key]["max_timestamp"], timestamp)

                key_data[key]["timestamps"].append(timestamp)

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

            if order:
                key_stats.sort(key=lambda x: x["key"], reverse=order == "desc")

            if limit is not None:
                key_stats = key_stats[:limit]

            interval_buckets = None
            if interval:
                m = _INTERVAL_RE.match(interval)
                if m:
                    every_n_int = int(m.group(1))
                    units = m.group(2)
                    attr_column_name = (
                        attribute.replace("-", "_")
                        .replace("@", "_")
                        .replace("%", "")
                        .replace(".", "_")
                        .replace("/", "_")
                        .replace(":", "_")
                        .replace(" ", "_")
                        .replace("$", "_")
                    )
                    flat = [
                        {
                            "key": r.key,
                            "timestamp": r.timestamp,
                            attribute: getattr(r, attr_column_name),
                        }
                        for r in results_list
                    ]
                    if flat:
                        interval_buckets = aggregate_data(
                            flat, every_n_int, units, "distinct", attribute, "key"
                        )

            return {
                "collection_name": collection_name,
                "stat": "distinct",
                "attribute": attribute,
                "total_keys": len(key_stats),
                "key_statistics": key_stats,
                "interval_buckets": interval_buckets,
            }

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to retrieve distinct values: {str(e)}"
            ) from e

    if not attribute:
        raise HTTPException(
            status_code=422, detail="Attribute is required for non-distinct statistics"
        )

    schema = _get_schema(session, organization_name, project_name, collection_name)

    keyspace_name = f'"{organization_name}"'
    table_name = f'"{project_name}_{collection_name}"'

    if attribute not in schema:
        raise HTTPException(
            status_code=422,
            detail=f"Attribute '{attribute}' does not exist in the collection schema.",
        )

    if group_by and group_by not in schema:
        raise HTTPException(
            status_code=422,
            detail=f"Group by field '{group_by}' does not exist in the collection schema.",
        )

    final_group_by = group_by or "key"

    if stat == "count":
        query = f'SELECT "{final_group_by}", timestamp FROM {keyspace_name}.{table_name}'
    else:
        query = (
            f'SELECT "{final_group_by}", timestamp, "{attribute}" FROM {keyspace_name}.{table_name}'
        )

    conditions = []
    filter_params: list[Any] = []
    if filters:
        filters_list = json.loads(filters)
        for f in filters_list:
            if f["operator"] == "or":
                or_conditions = []
                for operand in f["operands"]:
                    frag, fparams = generate_filter_condition_parameterized(
                        operand["property_name"], operand["operator"], operand["property_value"]
                    )
                    if frag:
                        or_conditions.append(frag)
                        filter_params.extend(fparams)
                if or_conditions:
                    conditions.append(f"({' OR '.join(or_conditions)})")
            else:
                frag, fparams = generate_filter_condition_parameterized(
                    f["property_name"], f["operator"], f["property_value"]
                )
                if frag:
                    conditions.append(frag)
                    filter_params.extend(fparams)

    time_params: list[Any] = []
    if ts_from and ts_to:
        conditions.append('"timestamp" >= %s AND "timestamp" <= %s')
        time_params.extend([ts_from, ts_to])
    elif ts_from:
        conditions.append('"timestamp" >= %s')
        time_params.append(ts_from)
    elif ts_to:
        conditions.append('"timestamp" <= %s')
        time_params.append(ts_to)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += f" LIMIT {settings.max_stats_rows}"

    query += " ALLOW FILTERING"

    params: list[Any] = filter_params + time_params

    try:
        results = session.execute(query, params)
        results_list = []

        if stat == "count":
            for row in results:
                row_dict = {
                    final_group_by: getattr(row, final_group_by),
                    "timestamp": row.timestamp,
                    attribute: 1,
                }
                results_list.append(row_dict)
        else:
            for row in results:
                row_dict = {}
                for i, column_name in enumerate(row._fields):
                    original_name = column_name
                    for schema_col in schema:
                        python_identifier = (
                            schema_col.replace("-", "_")
                            .replace("@", "_")
                            .replace("%", "")
                            .replace(".", "_")
                            .replace("/", "_")
                            .replace(":", "_")
                            .replace(" ", "_")
                            .replace("$", "_")
                        )
                        if python_identifier == column_name:
                            original_name = schema_col
                            break
                    value = row[i]
                    if hasattr(value, "__class__") and "Decimal" in value.__class__.__name__:
                        value = float(value)
                    row_dict[original_name] = value
                results_list.append(row_dict)

        if len(results_list) >= settings.max_stats_rows:
            raise HTTPException(
                status_code=400,
                detail=f"Result set too large (≥{settings.max_stats_rows} rows). Narrow your time range or add filters.",
            )

        if interval:
            m = _INTERVAL_RE.match(interval)
            assert m is not None
            every_n_int = int(m.group(1))
            units = m.group(2)
            aggregated_data = aggregate_data(
                results_list, every_n_int, units, stat, attribute, final_group_by
            )
        else:
            aggregated_data = results_list

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Query execution failed", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}") from e

    if order:
        stat_key = f"{stat}_{attribute}"

        def _sort_key(x: dict) -> tuple:
            val = x.get(stat_key)
            return (val is None, val if val is not None else 0)

        aggregated_data.sort(key=_sort_key, reverse=order == "desc")

    if limit is not None:
        aggregated_data = aggregated_data[:limit]

    return aggregated_data
