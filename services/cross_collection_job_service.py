import json
import logging
import uuid

from fastapi import HTTPException

from models.flink_job_models import (
    CROSS_COLLECTION_SENTINEL,
    CrossCollectionJobRequest,
    CrossCollectionJobResponse,
    CrossCollectionSchemaResponse,
    CrossCollectionSourceSchema,
    FlinkJobResult,
    FlinkJobResultsResponse,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import SYSTEM_FIELDS, fetch_all_collections, get_collection_by_id
from utilities.flink_utilities import (
    gateway_cancel_session,
    gateway_create_session,
    gateway_get_operation_status,
    gateway_submit_statement,
    generate_cross_collection_job_statements,
    get_collection_ddl_display,
)
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.schema_utils import CASSANDRA_TO_USER_TYPES

logger = logging.getLogger(__name__)

session = get_cassandra_session()


def _flat_schema(org_name: str, project_name: str, collection_name: str) -> dict[str, str]:
    rows = session.execute(
        "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s",
        (org_name, f"{project_name}_{collection_name}"),
    )
    result: dict[str, str] = {}
    for row in rows:
        if row.column_name not in SYSTEM_FIELDS:
            result[row.column_name] = CASSANDRA_TO_USER_TYPES.get(row.type) or row.type
    return result


def _row_to_response(row) -> CrossCollectionJobResponse:
    config = json.loads(row.config)
    return CrossCollectionJobResponse(
        id=row.id,
        project_id=row.project_id,
        name=config.get("name", ""),
        collection_ids=[uuid.UUID(cid) for cid in config.get("collection_ids", [])],
        config=config,
        sink_topic=row.sink_topic,
        status=row.status,
        created_at=str(row.created_at),
    )


async def get_cross_collection_schema_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
) -> CrossCollectionSchemaResponse:
    org = get_organization_by_id(organization_id)
    project = get_project_by_id(project_id, organization_id)
    org_name = org.organization_name
    project_name = project.project_name

    collections = fetch_all_collections(organization_id, project_id)
    sources: list[CrossCollectionSourceSchema] = []
    for col in collections:
        fields = _flat_schema(org_name, project_name, col.collection_name)
        table_name = f"KafkaSource_{col.collection_name}"
        sources.append(
            CrossCollectionSourceSchema(
                collection_id=str(col.id),
                collection_name=col.collection_name,
                table_name=table_name,
                source_ddl=get_collection_ddl_display(table_name, fields),
            )
        )
    return CrossCollectionSchemaResponse(
        collections=sources,
        sink_columns=[
            "`key` STRING",
            "window_start TIMESTAMP(3)",
            "window_end TIMESTAMP(3)",
            "record_count BIGINT",
            "value DOUBLE",
        ],
    )


async def create_cross_collection_job_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    job: CrossCollectionJobRequest,
) -> CrossCollectionJobResponse:
    org = get_organization_by_id(organization_id)
    project = get_project_by_id(project_id, organization_id)
    org_name = org.organization_name
    project_name = project.project_name

    source_collections: list[dict] = []
    for cid in job.collection_ids:
        col = get_collection_by_id(cid, project_id, organization_id)
        if not col:
            raise HTTPException(status_code=404, detail=f"Collection {cid} not found")
        fields = _flat_schema(org_name, project_name, col.collection_name)
        source_collections.append(
            {
                "topic": f"{org_name}.{project_name}.{col.collection_name}",
                "fields": fields,
                "table_name": f"KafkaSource_{col.collection_name}",
            }
        )

    job_id = uuid.uuid4()
    sink_topic = f"{org_name}.{project_name}.cross.{job_id}"
    statements = generate_cross_collection_job_statements(source_collections, sink_topic, job.sql)

    try:
        session_handle = await gateway_create_session()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Flink SQL Gateway unavailable: {exc}",
        ) from exc

    operation_handle: str | None = None
    try:
        for stmt in statements:
            operation_handle = await gateway_submit_statement(session_handle, stmt)
    except Exception as exc:
        await gateway_cancel_session(session_handle)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit Flink SQL statements: {exc}",
        ) from exc

    if operation_handle is None:
        await gateway_cancel_session(session_handle)
        raise HTTPException(status_code=500, detail="No operation handle returned from SQL Gateway")

    config_json = json.dumps(
        {
            "name": job.name,
            "sql": job.sql,
            "collection_ids": [str(cid) for cid in job.collection_ids],
        }
    )
    session.execute(
        """
        INSERT INTO flink_jobs
            (id, collection_id, project_id, session_handle, operation_handle,
             job_type, config, sink_topic, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(now()))
        """,
        (
            job_id,
            CROSS_COLLECTION_SENTINEL,
            project_id,
            session_handle,
            operation_handle,
            "cross",
            config_json,
            sink_topic,
            "RUNNING",
        ),
    )

    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s", (job_id,)).one()
    return _row_to_response(row)


async def list_cross_collection_jobs_service(
    project_id: uuid.UUID,
) -> list[CrossCollectionJobResponse]:
    rows = session.execute(
        "SELECT * FROM flink_jobs WHERE project_id=%s AND job_type=%s ALLOW FILTERING",
        (project_id, "cross"),
    )
    return [_row_to_response(r) for r in rows]


async def get_cross_collection_job_service(
    project_id: uuid.UUID,
    job_id: uuid.UUID,
) -> CrossCollectionJobResponse:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.project_id != project_id or row.job_type != "cross":
        raise HTTPException(status_code=404, detail="Cross-collection job not found")

    if row.status == "RUNNING":
        try:
            status_resp = await gateway_get_operation_status(
                row.session_handle, row.operation_handle
            )
            gw_status = status_resp.get("status", "RUNNING")
            if gw_status in ("ERROR", "CANCELED"):
                new_status = "ERROR" if gw_status == "ERROR" else "CANCELLED"
                session.execute(
                    "UPDATE flink_jobs SET status=%s WHERE id=%s",
                    (new_status, job_id),
                )
                row = session.execute(
                    "SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)
                ).one()
        except Exception:
            logger.warning("Could not refresh status for cross job %s", job_id)

    return _row_to_response(row)


async def stop_cross_collection_job_service(
    project_id: uuid.UUID,
    job_id: uuid.UUID,
) -> dict:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.project_id != project_id or row.job_type != "cross":
        raise HTTPException(status_code=404, detail="Cross-collection job not found")
    if row.status not in ("RUNNING", "PENDING"):
        raise HTTPException(status_code=400, detail="Job is not running")

    await gateway_cancel_session(row.session_handle)
    session.execute("UPDATE flink_jobs SET status=%s WHERE id=%s", ("CANCELLED", job_id))
    return {"message": f"Job {job_id} stopped"}


async def delete_cross_collection_job_service(
    project_id: uuid.UUID,
    job_id: uuid.UUID,
) -> dict:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.project_id != project_id or row.job_type != "cross":
        raise HTTPException(status_code=404, detail="Cross-collection job not found")

    if row.status in ("RUNNING", "PENDING"):
        await gateway_cancel_session(row.session_handle)

    session.execute("DELETE FROM flink_jobs WHERE id=%s", (job_id,))
    return {"message": f"Job {job_id} deleted"}


async def restart_cross_collection_job_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    job_id: uuid.UUID,
) -> CrossCollectionJobResponse:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.project_id != project_id or row.job_type != "cross":
        raise HTTPException(status_code=404, detail="Cross-collection job not found")

    if row.status in ("RUNNING", "PENDING"):
        await gateway_cancel_session(row.session_handle)

    config = json.loads(row.config)
    job = CrossCollectionJobRequest(
        name=config["name"],
        collection_ids=[uuid.UUID(cid) for cid in config["collection_ids"]],
        sql=config["sql"],
    )
    return await create_cross_collection_job_service(organization_id, project_id, job)


async def get_cross_collection_job_results_service(
    project_id: uuid.UUID,
    job_id: uuid.UUID,
    limit: int,
) -> FlinkJobResultsResponse:
    from utilities.kafka_consumer import read_topic_messages

    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.project_id != project_id or row.job_type != "cross":
        raise HTTPException(status_code=404, detail="Cross-collection job not found")

    config = json.loads(row.config)
    raw = read_topic_messages(row.sink_topic, limit=limit)
    items: list[FlinkJobResult] = []
    for msg in raw:
        raw_value = msg.get("value")
        items.append(
            FlinkJobResult(
                key=str(msg.get("key", "")),
                window_start=str(msg.get("window_start", "")),
                window_end=str(msg.get("window_end", "")),
                record_count=int(msg.get("record_count", 0)),
                value=float(raw_value) if raw_value is not None else None,
            )
        )

    return FlinkJobResultsResponse(
        items=items,
        total=len(items),
        metric="cross",
        attribute=config.get("name", "query"),
        sink_topic=row.sink_topic,
    )
