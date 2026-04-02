import asyncio
import datetime
import logging
import uuid

import httpx
from fastapi import HTTPException, status

from models.rule_models import RuleCreateRequest, RuleUpdateRequest
from utilities.cassandra_connector import get_cassandra_session

logger = logging.getLogger(__name__)
session = get_cassandra_session()


def insert_rule(
    project_id: uuid.UUID, collection_id: uuid.UUID, data: RuleCreateRequest
) -> uuid.UUID:
    rule_id = uuid.uuid4()
    now = datetime.datetime.now(datetime.UTC)
    query = """
        INSERT INTO metadata.rules (
            id, project_id, collection_id, name, description, field, operator,
            threshold, webhook_url, cooldown_seconds, enabled, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(
        query,
        (
            rule_id,
            project_id,
            collection_id,
            data.name,
            data.description,
            data.field,
            data.operator,
            data.threshold,
            data.webhook_url,
            data.cooldown_seconds,
            data.enabled,
            now,
        ),
    )
    return rule_id


def get_rule_by_id(rule_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID):
    query = "SELECT * FROM metadata.rules WHERE project_id=%s AND collection_id=%s AND id=%s LIMIT 1 ALLOW FILTERING"
    return session.execute(query, (project_id, collection_id, rule_id)).one()


def fetch_all_rules(project_id: uuid.UUID, collection_id: uuid.UUID):
    query = "SELECT * FROM metadata.rules WHERE project_id=%s AND collection_id=%s ALLOW FILTERING"
    return list(session.execute(query, (project_id, collection_id)))


def fetch_enabled_rules(project_id: uuid.UUID, collection_id: uuid.UUID):
    query = "SELECT * FROM metadata.rules WHERE project_id=%s AND collection_id=%s AND enabled=True ALLOW FILTERING"
    return list(session.execute(query, (project_id, collection_id)))


def update_rule_in_db(
    rule_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID, data: RuleUpdateRequest
):
    check_rule_exists(rule_id, project_id, collection_id)

    updates = []
    values = []

    if data.name is not None:
        updates.append("name=%s")
        values.append(data.name)
    if data.description is not None:
        updates.append("description=%s")
        values.append(data.description)
    if data.threshold is not None:
        updates.append("threshold=%s")
        values.append(data.threshold)
    if data.webhook_url is not None:
        updates.append("webhook_url=%s")
        values.append(data.webhook_url)
    if data.cooldown_seconds is not None:
        updates.append("cooldown_seconds=%s")
        values.append(data.cooldown_seconds)
    if data.enabled is not None:
        updates.append("enabled=%s")
        values.append(data.enabled)

    if not updates:
        return

    query = f"UPDATE metadata.rules SET {', '.join(updates)} WHERE project_id=%s AND collection_id=%s AND id=%s"
    values.extend([project_id, collection_id, rule_id])
    session.execute(query, tuple(values))


def delete_rule_from_db(rule_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID):
    check_rule_exists(rule_id, project_id, collection_id)
    query = "DELETE FROM metadata.rules WHERE project_id=%s AND collection_id=%s AND id=%s"
    session.execute(query, (project_id, collection_id, rule_id))


def update_rule_last_fired(rule_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID):
    now = datetime.datetime.now(datetime.UTC)
    query = "UPDATE metadata.rules SET last_fired_at=%s WHERE project_id=%s AND collection_id=%s AND id=%s"
    session.execute(query, (now, project_id, collection_id, rule_id))


def check_rule_exists(rule_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID):
    rule = get_rule_by_id(rule_id, project_id, collection_id)
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return rule


def evaluate_rule(record: dict, rule_row) -> bool:
    field = rule_row.field
    if field not in record:
        return False

    val = record[field]
    if not isinstance(val, (int, float)):
        return False

    threshold = rule_row.threshold
    op = rule_row.operator

    if op == "gt":
        return val > threshold
    if op == "lt":
        return val < threshold
    if op == "gte":
        return val >= threshold
    if op == "lte":
        return val <= threshold
    if op == "eq":
        return val == threshold
    if op == "neq":
        return val != threshold
    return False


def _within_cooldown(rule_row) -> bool:
    if not rule_row.last_fired_at:
        return False
    now = datetime.datetime.now(datetime.UTC)
    last_fired = rule_row.last_fired_at
    if last_fired.tzinfo is None:
        last_fired = last_fired.replace(tzinfo=datetime.UTC)

    delta = (now - last_fired).total_seconds()
    return delta < rule_row.cooldown_seconds


async def evaluate_and_fire_rules(
    project_id: uuid.UUID, collection_id: uuid.UUID, organization_id: uuid.UUID, records: list[dict]
):
    rules = fetch_enabled_rules(project_id, collection_id)
    for rule in rules:
        if _within_cooldown(rule):
            continue
        for record in records:
            if evaluate_rule(record, rule):
                update_rule_last_fired(rule.id, project_id, collection_id)
                asyncio.create_task(_fire_webhook(rule.webhook_url, rule, record))
                break  # one fire per rule per batch


async def _fire_webhook(url: str, rule, record: dict):
    payload = {
        "rule_id": str(rule.id),
        "rule_name": rule.name,
        "field": rule.field,
        "operator": rule.operator,
        "threshold": rule.threshold,
        "triggered_value": record.get(rule.field),
        "record": record,
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(url, json=payload)
    except Exception as e:
        logger.warning("Webhook fire failed for rule %s: %s", rule.id, e)
