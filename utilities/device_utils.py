import datetime
import json
import logging
import uuid
from typing import Any

from fastapi import HTTPException, status

from dependencies import get_organization_id
from models.device_models import DeviceCreateRequest, DeviceUpdateRequest
from utilities.cassandra_connector import get_cassandra_session

logger = logging.getLogger(__name__)

session = get_cassandra_session()


def get_device_by_id(device_id: uuid.UUID, project_id: uuid.UUID, organization_id: uuid.UUID):
    query = (
        "SELECT id, project_id, organization_id, name, description, tags, status, "
        "last_seen, created_at FROM device "
        "WHERE id=%s AND project_id=%s AND organization_id=%s LIMIT 1 ALLOW FILTERING"
    )
    return session.execute(query, (device_id, project_id, organization_id)).one()


def check_device_exists(device_id: uuid.UUID, project_id: uuid.UUID):
    device = get_device_by_id(device_id, project_id, get_organization_id())
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found.")
    return device


def fetch_all_devices(organization_id: uuid.UUID, project_id: uuid.UUID):
    query = (
        "SELECT id, project_id, organization_id, name, description, tags, status, "
        "last_seen, created_at FROM device "
        "WHERE organization_id=%s AND project_id=%s ALLOW FILTERING"
    )
    return session.execute(query, (organization_id, project_id)).all()


def insert_device(
    organization_id: uuid.UUID, project_id: uuid.UUID, data: DeviceCreateRequest
) -> uuid.UUID:
    device_id = uuid.uuid4()
    now = datetime.datetime.now(datetime.UTC)
    query = (
        "INSERT INTO device (id, project_id, organization_id, name, description, tags, "
        "status, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    session.execute(
        query,
        (
            device_id,
            project_id,
            organization_id,
            data.name,
            data.description,
            data.tags,
            "active",
            now,
        ),
    )
    return device_id


def update_device_in_db(device_id: uuid.UUID, data: DeviceUpdateRequest):
    update_query = "UPDATE device SET "
    update_params: list[Any] = []

    if data.description is not None:
        update_query += "description=%s, "
        update_params.append(data.description)
    if data.tags is not None:
        update_query += "tags=%s, "
        update_params.append(data.tags)
    if data.status is not None:
        update_query += "status=%s, "
        update_params.append(data.status)

    if not update_params:
        return

    update_query = update_query.rstrip(", ") + " WHERE id=%s"
    update_params.append(device_id)
    session.execute(update_query, tuple(update_params))


def delete_device_from_db(device_id: uuid.UUID):
    session.execute("DELETE FROM device WHERE id=%s", (device_id,))
    session.execute("DELETE FROM device_shadow WHERE device_id=%s", (device_id,))


def get_device_shadow(device_id: uuid.UUID):
    query = (
        "SELECT device_id, reported_state, desired_state, reported_at, desired_at "
        "FROM device_shadow WHERE device_id=%s LIMIT 1 ALLOW FILTERING"
    )
    return session.execute(query, (device_id,)).one()


def upsert_reported_state(device_id: uuid.UUID, state_dict: dict[str, Any]):
    now = datetime.datetime.now(datetime.UTC)
    session.execute(
        "UPDATE device_shadow SET reported_state=%s, reported_at=%s WHERE device_id=%s",
        (json.dumps(state_dict), now, device_id),
    )
    session.execute(
        "UPDATE device SET last_seen=%s WHERE id=%s",
        (now, device_id),
    )


def upsert_desired_state(device_id: uuid.UUID, state_dict: dict[str, Any]):
    now = datetime.datetime.now(datetime.UTC)
    session.execute(
        "UPDATE device_shadow SET desired_state=%s, desired_at=%s WHERE device_id=%s",
        (json.dumps(state_dict), now, device_id),
    )


def compute_shadow_delta(reported: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in desired.items() if reported.get(k) != v}
