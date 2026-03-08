import logging
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.responses import Response

from config import settings
from models.common import HealthResponse, ReadyResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description="Returns 200 if the API process is running. No dependency checks.",
)
async def health() -> dict[str, str]:
    """Return a simple status indicating the API is running."""
    return {"status": "ok"}


@router.get(
    "/ready",
    response_model=ReadyResponse,
    summary="Readiness probe",
    description=(
        "Checks connectivity to Cassandra and Kafka. "
        "Returns 200 with all checks passing, or 503 if any dependency is unreachable."
    ),
)
async def ready() -> dict[str, Any] | Response:
    """Check and return the readiness status of dependencies."""
    checks: dict[str, str] = {}
    all_ok = True

    try:
        from utilities.cassandra_connector import get_cassandra_session

        session = get_cassandra_session()
        session.execute("SELECT release_version FROM system.local")
        checks["cassandra"] = "ok"
    except Exception as e:
        logger.warning("Readiness: Cassandra check failed: %s", e)
        checks["cassandra"] = f"error: {e}"
        all_ok = False

    try:
        from confluent_kafka import Consumer

        conf = {"bootstrap.servers": settings.kafka_brokers, "group.id": "cenotoo-health"}
        if settings.kafka_username:
            conf["security.protocol"] = settings.kafka_security_protocol
            conf["sasl.mechanism"] = settings.kafka_sasl_mechanism
            conf["sasl.username"] = settings.kafka_username
            conf["sasl.password"] = settings.kafka_password
        consumer = Consumer(conf)
        consumer.list_topics(timeout=5)
        consumer.close()
        checks["kafka"] = "ok"
    except Exception as e:
        logger.warning("Readiness: Kafka check failed: %s", e)
        checks["kafka"] = f"degraded: {e}"

    if not all_ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "checks": checks},
        )

    return {"status": "ok", "checks": checks}
