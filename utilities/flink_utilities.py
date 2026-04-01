import logging
from typing import Any

import httpx

from config import settings
from models.flink_job_models import GuidedJobRequest

logger = logging.getLogger(__name__)

_METRIC_SQL: dict[str, str] = {
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "sum": "SUM",
    "count": "COUNT(*)",
    "stddev": "STDDEV_POP",
    "stddev_samp": "STDDEV_SAMP",
    "var_pop": "VAR_POP",
    "var_samp": "VAR_SAMP",
    "first_value": "FIRST_VALUE",
    "last_value": "LAST_VALUE",
}

_USER_TO_FLINK_TYPES: dict[str, str] = {
    "text": "STRING",
    "float": "DOUBLE",
    "int": "INT",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP(3)",
}

_UNIT_FLINK: dict[str, str] = {
    "second": "SECOND",
    "minute": "MINUTE",
    "hour": "HOUR",
    "day": "DAY",
}


def build_sink_topic(org: str, project: str, collection: str, job: GuidedJobRequest) -> str:
    return f"{org}.{project}.{collection}.stats.{job.window_size}{job.unit}.{job.metric}.{job.attribute}"


def _kafka_auth_props(indent: str = "  ") -> str:
    if not settings.kafka_username:
        return ""
    return (
        f"\n{indent}'properties.security.protocol' = '{settings.kafka_security_protocol}',"
        f"\n{indent}'properties.sasl.mechanism' = '{settings.kafka_sasl_mechanism}',"
        f"\n{indent}'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule"
        f' required username="{settings.kafka_username}" password="{settings.kafka_password}";\','
    )


def _source_ddl(topic: str, attribute: str) -> str:
    auth = _kafka_auth_props()
    return (
        f"CREATE TABLE KafkaSource (\n"
        f"  `key` STRING,\n"
        f"  `{attribute}` DOUBLE,\n"
        f"  `event_time` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP() * 1000, 3),\n"
        f"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND\n"
        f") WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{topic}',\n"
        f"  'properties.bootstrap.servers' = '{settings.kafka_brokers}',{auth}\n"
        f"  'value.format' = 'json',\n"
        f"  'scan.startup.mode' = 'earliest-offset'\n"
        f")"
    )


def _sink_ddl(sink_topic: str, attribute: str, metric: str) -> str:
    auth = _kafka_auth_props()
    return (
        f"CREATE TABLE KafkaSink (\n"
        f"  `key` STRING,\n"
        f"  window_start TIMESTAMP(3),\n"
        f"  window_end TIMESTAMP(3),\n"
        f"  record_count BIGINT,\n"
        f"  {metric}_{attribute} DOUBLE\n"
        f") WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{sink_topic}',\n"
        f"  'properties.bootstrap.servers' = '{settings.kafka_brokers}',{auth}\n"
        f"  'format' = 'json'\n"
        f")"
    )


def _insert_dml(
    attribute: str,
    metric: str,
    window_size: int,
    unit: str,
    window_type: str,
    sliding_step: int | None,
) -> str:
    flink_unit = _UNIT_FLINK[unit]
    value_expr = "COUNT(*)" if metric == "count" else f"{_METRIC_SQL[metric]}(`{attribute}`)"

    if window_type == "tumbling":
        window_expr = f"TUMBLE(`event_time`, INTERVAL '{window_size}' {flink_unit})"
        start_expr = f"TUMBLE_START(`event_time`, INTERVAL '{window_size}' {flink_unit})"
        end_expr = f"TUMBLE_END(`event_time`, INTERVAL '{window_size}' {flink_unit})"
    else:
        step = sliding_step or 1
        window_expr = f"HOP(`event_time`, INTERVAL '{step}' {flink_unit}, INTERVAL '{window_size}' {flink_unit})"
        start_expr = f"HOP_START(`event_time`, INTERVAL '{step}' {flink_unit}, INTERVAL '{window_size}' {flink_unit})"
        end_expr = f"HOP_END(`event_time`, INTERVAL '{step}' {flink_unit}, INTERVAL '{window_size}' {flink_unit})"

    return (
        f"INSERT INTO KafkaSink\n"
        f"SELECT\n"
        f"  `key`,\n"
        f"  {start_expr} AS window_start,\n"
        f"  {end_expr} AS window_end,\n"
        f"  COUNT(*) AS record_count,\n"
        f"  {value_expr} AS {metric}_{attribute}\n"
        f"FROM KafkaSource\n"
        f"GROUP BY `key`, {window_expr}"
    )


def _source_ddl_full(topic: str, fields: dict[str, str]) -> str:
    auth = _kafka_auth_props()
    col_lines = "\n".join(
        f"  `{name}` {_USER_TO_FLINK_TYPES.get(ftype, 'STRING')}," for name, ftype in fields.items()
    )
    return (
        f"CREATE TABLE KafkaSource (\n"
        f"  `key` STRING,\n"
        f"{col_lines}\n"
        f"  `event_time` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP() * 1000, 3),\n"
        f"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND\n"
        f") WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{topic}',\n"
        f"  'properties.bootstrap.servers' = '{settings.kafka_brokers}',{auth}\n"
        f"  'value.format' = 'json',\n"
        f"  'scan.startup.mode' = 'earliest-offset'\n"
        f")"
    )


def _sink_ddl_custom(sink_topic: str) -> str:
    auth = _kafka_auth_props()
    return (
        f"CREATE TABLE KafkaSink (\n"
        f"  `key` STRING,\n"
        f"  window_start TIMESTAMP(3),\n"
        f"  window_end TIMESTAMP(3),\n"
        f"  record_count BIGINT,\n"
        f"  value DOUBLE\n"
        f") WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{sink_topic}',\n"
        f"  'properties.bootstrap.servers' = '{settings.kafka_brokers}',{auth}\n"
        f"  'format' = 'json'\n"
        f")"
    )


def get_source_ddl_display(fields: dict[str, str]) -> str:
    col_lines = "\n".join(
        f"  `{name}` {_USER_TO_FLINK_TYPES.get(ftype, 'STRING')}," for name, ftype in fields.items()
    )
    return (
        f"CREATE TABLE KafkaSource (\n"
        f"  `key` STRING,\n"
        f"{col_lines}\n"
        f"  `event_time` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP() * 1000, 3),\n"
        f"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND\n"
        f")"
    )


def _source_ddl_named(topic: str, fields: dict[str, str], table_name: str) -> str:
    auth = _kafka_auth_props()
    col_lines = "\n".join(
        f"  `{name}` {_USER_TO_FLINK_TYPES.get(ftype, 'STRING')}," for name, ftype in fields.items()
    )
    return (
        f"CREATE TABLE {table_name} (\n"
        f"  `key` STRING,\n"
        f"{col_lines}\n"
        f"  `event_time` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP() * 1000, 3),\n"
        f"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND\n"
        f") WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{topic}',\n"
        f"  'properties.bootstrap.servers' = '{settings.kafka_brokers}',{auth}\n"
        f"  'value.format' = 'json',\n"
        f"  'scan.startup.mode' = 'earliest-offset'\n"
        f")"
    )


def get_collection_ddl_display(table_name: str, fields: dict[str, str]) -> str:
    col_lines = "\n".join(
        f"  `{name}` {_USER_TO_FLINK_TYPES.get(ftype, 'STRING')}," for name, ftype in fields.items()
    )
    return (
        f"CREATE TABLE {table_name} (\n"
        f"  `key` STRING,\n"
        f"{col_lines}\n"
        f"  `event_time` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP() * 1000, 3),\n"
        f"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND\n"
        f")"
    )


def generate_cross_collection_job_statements(
    source_collections: list[dict[str, Any]],
    sink_topic: str,
    user_sql: str,
) -> list[str]:
    stmts: list[str] = [f"ADD JAR '{_KAFKA_CONNECTOR_JAR}'"]
    for col in source_collections:
        stmts.append(_source_ddl_named(col["topic"], col["fields"], col["table_name"]))
    stmts.append(_sink_ddl_custom(sink_topic))
    stmts.append(f"INSERT INTO KafkaSink\n{user_sql}")
    return stmts


def generate_custom_job_statements(
    source_topic: str,
    fields: dict[str, str],
    sink_topic: str,
    user_sql: str,
) -> list[str]:
    return [
        f"ADD JAR '{_KAFKA_CONNECTOR_JAR}'",
        _source_ddl_full(source_topic, fields),
        _sink_ddl_custom(sink_topic),
        f"INSERT INTO KafkaSink\n{user_sql}",
    ]


_KAFKA_CONNECTOR_JAR = "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar"


def generate_guided_job_statements(
    org: str,
    project: str,
    collection: str,
    job: GuidedJobRequest,
    sink_topic: str,
) -> list[str]:
    source_topic = f"{org}.{project}.{collection}"
    return [
        f"ADD JAR '{_KAFKA_CONNECTOR_JAR}'",
        _source_ddl(source_topic, job.attribute),
        _sink_ddl(sink_topic, job.attribute, job.metric),
        _insert_dml(
            job.attribute,
            job.metric,
            job.window_size,
            job.unit,
            job.window_type,
            job.sliding_step,
        ),
    ]


async def gateway_create_session() -> str:
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{settings.flink_sql_gateway_url}/v1/sessions")
        resp.raise_for_status()
        return resp.json()["sessionHandle"]


async def gateway_submit_statement(session_handle: str, statement: str) -> str:
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{settings.flink_sql_gateway_url}/v1/sessions/{session_handle}/statements",
            json={"statement": statement},
        )
        resp.raise_for_status()
        return resp.json()["operationHandle"]


async def gateway_get_operation_status(
    session_handle: str, operation_handle: str
) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{settings.flink_sql_gateway_url}/v1/sessions/{session_handle}"
            f"/operations/{operation_handle}/status"
        )
        resp.raise_for_status()
        return resp.json()


async def gateway_cancel_session(session_handle: str) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.delete(f"{settings.flink_sql_gateway_url}/v1/sessions/{session_handle}")
        except httpx.HTTPError:
            logger.warning("Failed to cancel SQL Gateway session %s", session_handle)
