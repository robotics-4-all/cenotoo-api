import json
import logging
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka import OFFSET_BEGINNING

from config import settings
from utilities.kafka_connector import _apply_sasl_config

logger = logging.getLogger(__name__)


def read_topic_messages(topic: str, limit: int = 100) -> list[dict]:
    conf: dict = {
        "bootstrap.servers": settings.kafka_brokers,
        "group.id": f"cenotoo-api-reader-{uuid.uuid4()}",
        "enable.auto.commit": False,
    }
    _apply_sasl_config(conf)

    consumer = Consumer(conf)
    messages: list[dict] = []

    try:
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
        empty_polls = 0
        while empty_polls < 3 and len(messages) < limit:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.warning("Kafka error reading topic %s: %s", topic, msg.error())
                break
            empty_polls = 0
            try:
                messages.append(json.loads(msg.value().decode("utf-8")))
            except (json.JSONDecodeError, AttributeError, UnicodeDecodeError):
                continue
    except Exception as exc:
        logger.warning("Failed to read messages from topic %s: %s", topic, exc)
    finally:
        consumer.close()

    return messages[-limit:]
