import json
import logging
import uuid

from fastapi import HTTPException

from models.flink_job_models import (
    CustomJobRequest,
    CustomSchemaResponse,
    FlinkJobResponse,
    FlinkJobResult,
    FlinkJobResultsResponse,
    GuidedJobRequest,
)
from utilities.cassandra_connector import get_cassandra_session
from utilities.collection_utils import SYSTEM_FIELDS, get_collection_by_id
from utilities.flink_utilities import (
    build_sink_topic,
    gateway_cancel_session,
    gateway_create_session,
    gateway_get_operation_status,
    gateway_submit_statement,
    generate_custom_job_statements,
    generate_guided_job_statements,
    get_source_ddl_display,
)
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.schema_utils import CASSANDRA_TO_USER_TYPES

logger = logging.getLogger(__name__)

session = get_cassandra_session()


def _row_to_response(row) -> FlinkJobResponse:
    return FlinkJobResponse(
        id=row.id,
        collection_id=row.collection_id,
        project_id=row.project_id,
        job_type=row.job_type,
        config=json.loads(row.config),
        sink_topic=row.sink_topic,
        status=row.status,
        created_at=str(row.created_at),
    )


async def create_guided_job_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job: GuidedJobRequest,
) -> FlinkJobResponse:
    org = get_organization_by_id(organization_id)
    project = get_project_by_id(project_id, organization_id)
    collection = get_collection_by_id(collection_id, project_id, organization_id)

    org_name = org.organization_name
    project_name = project.project_name
    collection_name = collection.collection_name

    sink_topic = build_sink_topic(org_name, project_name, collection_name, job)
    statements = generate_guided_job_statements(
        org_name, project_name, collection_name, job, sink_topic
    )

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

    job_id = uuid.uuid4()
    config_json = json.dumps(job.model_dump())

    session.execute(
        """
        INSERT INTO flink_jobs
            (id, collection_id, project_id, session_handle, operation_handle,
             job_type, config, sink_topic, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(now()))
        """,
        (
            job_id,
            collection_id,
            project_id,
            session_handle,
            operation_handle,
            "guided",
            config_json,
            sink_topic,
            "RUNNING",
        ),
    )

    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s", (job_id,)).one()
    return _row_to_response(row)


async def list_jobs_service(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
) -> list[FlinkJobResponse]:
    rows = session.execute(
        "SELECT * FROM flink_jobs WHERE collection_id=%s AND project_id=%s ALLOW FILTERING",
        (collection_id, project_id),
    )
    return [_row_to_response(r) for r in rows]


async def get_job_service(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
) -> FlinkJobResponse:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.collection_id != collection_id or row.project_id != project_id:
        raise HTTPException(status_code=404, detail="Flink job not found")

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
            logger.warning("Could not refresh job status for job %s", job_id)

    return _row_to_response(row)


async def stop_job_service(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
) -> dict:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.collection_id != collection_id or row.project_id != project_id:
        raise HTTPException(status_code=404, detail="Flink job not found")
    if row.status not in ("RUNNING", "PENDING"):
        raise HTTPException(status_code=400, detail="Job is not running")

    await gateway_cancel_session(row.session_handle)
    session.execute("UPDATE flink_jobs SET status=%s WHERE id=%s", ("CANCELLED", job_id))
    return {"message": f"Job {job_id} stopped"}


async def delete_job_service(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
) -> dict:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.collection_id != collection_id or row.project_id != project_id:
        raise HTTPException(status_code=404, detail="Flink job not found")

    if row.status in ("RUNNING", "PENDING"):
        await gateway_cancel_session(row.session_handle)

    session.execute("DELETE FROM flink_jobs WHERE id=%s", (job_id,))
    return {"message": f"Job {job_id} deleted"}


async def restart_job_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
) -> FlinkJobResponse:
    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.collection_id != collection_id or row.project_id != project_id:
        raise HTTPException(status_code=404, detail="Flink job not found")

    if row.status in ("RUNNING", "PENDING"):
        await gateway_cancel_session(row.session_handle)
        session.execute("UPDATE flink_jobs SET status=%s WHERE id=%s", ("CANCELLED", job_id))

    config = json.loads(row.config)
    job = GuidedJobRequest(**config)
    return await create_guided_job_service(organization_id, project_id, collection_id, job)


def _flat_collection_schema(
    org_name: str, project_name: str, collection_name: str
) -> dict[str, str]:
    rows = session.execute(
        "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s",
        (org_name, f"{project_name}_{collection_name}"),
    )
    result: dict[str, str] = {}
    for row in rows:
        if row.column_name not in SYSTEM_FIELDS:
            result[row.column_name] = CASSANDRA_TO_USER_TYPES.get(row.type) or row.type
    return result


async def create_custom_job_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job: CustomJobRequest,
) -> FlinkJobResponse:
    org = get_organization_by_id(organization_id)
    project = get_project_by_id(project_id, organization_id)
    collection = get_collection_by_id(collection_id, project_id, organization_id)

    org_name = org.organization_name
    project_name = project.project_name
    collection_name = collection.collection_name

    fields = _flat_collection_schema(org_name, project_name, collection_name)
    source_topic = f"{org_name}.{project_name}.{collection_name}"
    job_id = uuid.uuid4()
    sink_topic = f"{org_name}.{project_name}.{collection_name}.custom.{job_id}"

    statements = generate_custom_job_statements(source_topic, fields, sink_topic, job.sql)

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

    config_json = json.dumps({"name": job.name, "sql": job.sql})
    session.execute(
        """
        INSERT INTO flink_jobs
            (id, collection_id, project_id, session_handle, operation_handle,
             job_type, config, sink_topic, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(now()))
        """,
        (
            job_id,
            collection_id,
            project_id,
            session_handle,
            operation_handle,
            "custom",
            config_json,
            sink_topic,
            "RUNNING",
        ),
    )

    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s", (job_id,)).one()
    return _row_to_response(row)


async def get_custom_schema_service(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
) -> CustomSchemaResponse:
    org = get_organization_by_id(organization_id)
    project = get_project_by_id(project_id, organization_id)
    collection = get_collection_by_id(collection_id, project_id, organization_id)

    fields = _flat_collection_schema(
        org.organization_name, project.project_name, collection.collection_name
    )
    source_ddl = get_source_ddl_display(fields)
    sink_columns = [
        "`key` STRING",
        "window_start TIMESTAMP(3)",
        "window_end TIMESTAMP(3)",
        "record_count BIGINT",
        "value DOUBLE",
    ]
    return CustomSchemaResponse(source_ddl=source_ddl, sink_columns=sink_columns)


async def list_project_jobs_service(project_id: uuid.UUID) -> list[FlinkJobResponse]:
    rows = session.execute(
        "SELECT * FROM flink_jobs WHERE project_id=%s ALLOW FILTERING", (project_id,)
    )
    return [_row_to_response(r) for r in rows]


async def get_job_results_service(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
    limit: int,
) -> FlinkJobResultsResponse:
    from utilities.kafka_consumer import read_topic_messages

    row = session.execute("SELECT * FROM flink_jobs WHERE id=%s ALLOW FILTERING", (job_id,)).one()
    if not row or row.collection_id != collection_id or row.project_id != project_id:
        raise HTTPException(status_code=404, detail="Flink job not found")

    config = json.loads(row.config)
    if row.job_type == "guided":
        metric = config["metric"]
        attribute = config["attribute"]
        value_key = f"{metric}_{attribute}"
    else:
        metric = "custom"
        attribute = config.get("name", "query")
        value_key = "value"

    raw = read_topic_messages(row.sink_topic, limit=limit)
    items: list[FlinkJobResult] = []
    for msg in raw:
        raw_value = msg.get(value_key)
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
        metric=metric,
        attribute=attribute,
        sink_topic=row.sink_topic,
    )
