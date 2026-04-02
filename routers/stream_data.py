import asyncio
import json
import logging
import uuid

from confluent_kafka import OFFSET_END as KAFKA_OFFSET_END
from confluent_kafka import Consumer, KafkaError, TopicPartition
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from config import settings
from dependencies import check_project_exists, get_organization_id, verify_endpoint_access
from utilities.collection_utils import check_collection_exists, get_collection_by_id
from utilities.kafka_connector import _apply_sasl_config
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Stream Data"

_KEEPALIVE_TICKS = 150


async def _sse_event_generator(topic: str):
    conf = {
        "bootstrap.servers": settings.kafka_brokers,
        "group.id": f"cenotoo-sse-{uuid.uuid4()}",
        "enable.auto.commit": False,
    }
    _apply_sasl_config(conf)
    consumer = Consumer(conf)
    consumer.assign([TopicPartition(topic, 0, KAFKA_OFFSET_END)])
    loop = asyncio.get_event_loop()
    tick = 0
    try:
        while True:
            msg = await loop.run_in_executor(None, lambda: consumer.poll(0.1))
            if msg is None:
                tick += 1
                if tick >= _KEEPALIVE_TICKS:
                    yield ": keepalive\n\n"
                    tick = 0
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    tick += 1
                    if tick >= _KEEPALIVE_TICKS:
                        yield ": keepalive\n\n"
                        tick = 0
                    continue
                logger.warning("Kafka error on SSE stream for topic %s: %s", topic, msg.error())
                break
            tick = 0
            try:
                data = json.loads(msg.value().decode("utf-8"))
                yield f"data: {json.dumps(data)}\n\n"
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
    finally:
        consumer.close()


@router.get(
    "/projects/{project_id}/collections/{collection_id}/stream",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def stream_collection_data(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
):
    organization_id = get_organization_id()
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id, organization_id).project_name
    collection_name = get_collection_by_id(
        collection_id, project_id, organization_id
    ).collection_name
    topic = f"{organization_name}.{project_name}.{collection_name}"
    return StreamingResponse(
        _sse_event_generator(topic),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
