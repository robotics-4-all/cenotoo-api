import httpx
import pytest
import respx

from models.flink_job_models import GuidedJobRequest
from utilities.flink_utilities import (
    build_sink_topic,
    gateway_cancel_session,
    gateway_create_session,
    gateway_get_operation_status,
    gateway_submit_statement,
    generate_guided_job_statements,
)

GATEWAY_URL = "http://localhost:8083"


class TestBuildSinkTopic:
    def test_tumbling_topic_format(self):
        job = GuidedJobRequest(attribute="temperature", metric="avg", window_size=5, unit="minute")
        topic = build_sink_topic("org", "proj", "coll", job)
        assert topic == "org.proj.coll.stats.5minute.avg.temperature"

    def test_sliding_topic_format(self):
        job = GuidedJobRequest(
            attribute="humidity",
            metric="sum",
            window_type="sliding",
            window_size=10,
            unit="second",
            sliding_step=2,
        )
        topic = build_sink_topic("myorg", "myproj", "mycoll", job)
        assert topic == "myorg.myproj.mycoll.stats.10second.sum.humidity"


class TestGenerateGuidedJobStatements:
    def test_returns_four_statements(self):
        job = GuidedJobRequest(attribute="temp", metric="avg", window_size=5, unit="minute")
        stmts = generate_guided_job_statements(
            "org", "proj", "coll", job, "org.proj.coll.stats.5minute.avg.temp"
        )
        assert len(stmts) == 4

    def test_first_statement_is_add_jar(self):
        job = GuidedJobRequest(attribute="temp", metric="avg", window_size=5, unit="minute")
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "sink-topic")
        assert stmts[0].startswith("ADD JAR")
        assert "flink-sql-connector-kafka" in stmts[0]

    def test_source_ddl_contains_kafka_topic(self):
        job = GuidedJobRequest(attribute="temp", metric="avg", window_size=5, unit="minute")
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "sink-topic")
        assert "org.proj.coll" in stmts[1]
        assert "KafkaSource" in stmts[1]

    def test_sink_ddl_contains_sink_topic(self):
        job = GuidedJobRequest(attribute="temp", metric="avg", window_size=5, unit="minute")
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "my-sink-topic")
        assert "my-sink-topic" in stmts[2]
        assert "KafkaSink" in stmts[2]

    def test_insert_dml_tumbling(self):
        job = GuidedJobRequest(attribute="temp", metric="avg", window_size=5, unit="minute")
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "sink-topic")
        assert "TUMBLE" in stmts[3]
        assert "INSERT INTO KafkaSink" in stmts[3]
        assert "avg_temp" in stmts[3]

    def test_insert_dml_sliding(self):
        job = GuidedJobRequest(
            attribute="temp",
            metric="min",
            window_type="sliding",
            window_size=10,
            unit="minute",
            sliding_step=2,
        )
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "sink-topic")
        assert "HOP" in stmts[3]
        assert "min_temp" in stmts[3]

    def test_count_metric_uses_count_star(self):
        job = GuidedJobRequest(attribute="temp", metric="count", window_size=1, unit="hour")
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "sink-topic")
        assert "COUNT(*)" in stmts[3]

    def test_stddev_metric(self):
        job = GuidedJobRequest(attribute="temp", metric="stddev", window_size=1, unit="hour")
        stmts = generate_guided_job_statements("org", "proj", "coll", job, "sink-topic")
        assert "STDDEV_POP" in stmts[3]


class TestGatewayCreateSession:
    @pytest.mark.asyncio
    async def test_returns_session_handle(self):
        with respx.mock(base_url=GATEWAY_URL) as mock:
            mock.post("/v1/sessions").mock(
                return_value=httpx.Response(200, json={"sessionHandle": "abc-123"})
            )
            handle = await gateway_create_session()
        assert handle == "abc-123"

    @pytest.mark.asyncio
    async def test_raises_on_gateway_error(self):
        with respx.mock(base_url=GATEWAY_URL) as mock:
            mock.post("/v1/sessions").mock(return_value=httpx.Response(500))
            with pytest.raises(httpx.HTTPStatusError):
                await gateway_create_session()


class TestGatewaySubmitStatement:
    @pytest.mark.asyncio
    async def test_returns_operation_handle(self):
        with respx.mock(base_url=GATEWAY_URL) as mock:
            mock.post("/v1/sessions/sess-1/statements").mock(
                return_value=httpx.Response(200, json={"operationHandle": "op-42"})
            )
            handle = await gateway_submit_statement("sess-1", "SELECT 1")
        assert handle == "op-42"


class TestGatewayGetOperationStatus:
    @pytest.mark.asyncio
    async def test_returns_status_dict(self):
        with respx.mock(base_url=GATEWAY_URL) as mock:
            mock.get("/v1/sessions/sess-1/operations/op-1/status").mock(
                return_value=httpx.Response(200, json={"status": "RUNNING"})
            )
            result = await gateway_get_operation_status("sess-1", "op-1")
        assert result["status"] == "RUNNING"


class TestGatewayCancelSession:
    @pytest.mark.asyncio
    async def test_cancels_successfully(self):
        with respx.mock(base_url=GATEWAY_URL) as mock:
            mock.delete("/v1/sessions/sess-1").mock(return_value=httpx.Response(200))
            await gateway_cancel_session("sess-1")

    @pytest.mark.asyncio
    async def test_swallows_http_error(self):
        with respx.mock(base_url=GATEWAY_URL) as mock:
            mock.delete("/v1/sessions/sess-bad").mock(return_value=httpx.Response(404))
            await gateway_cancel_session("sess-bad")
