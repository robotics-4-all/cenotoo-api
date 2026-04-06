import datetime
import json
import logging
import uuid
from typing import Any

from fastapi import HTTPException, status

from dependencies import get_organization_id
from models.device_models import DeviceCreateRequest, DeviceUpdateRequest
from utilities.postgres_connector import pg_execute, pg_fetchall, pg_fetchone

logger = logging.getLogger(__name__)


def get_device_by_id(device_id: uuid.UUID, project_id: uuid.UUID, organization_id: uuid.UUID):
    return pg_fetchone(
        "SELECT id, project_id, organization_id, name, description, tags, status, "
        "last_seen, created_at FROM device "
        "WHERE id=%s AND project_id=%s AND organization_id=%s",
        (device_id, project_id, organization_id),
    )


def check_device_exists(device_id: uuid.UUID, project_id: uuid.UUID):
    device = get_device_by_id(device_id, project_id, get_organization_id())
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found.")
    return device


def fetch_all_devices(organization_id: uuid.UUID, project_id: uuid.UUID):
    return pg_fetchall(
        "SELECT id, project_id, organization_id, name, description, tags, status, "
        "last_seen, created_at FROM device "
        "WHERE organization_id=%s AND project_id=%s",
        (organization_id, project_id),
    )


def insert_device(
    organization_id: uuid.UUID, project_id: uuid.UUID, data: DeviceCreateRequest
) -> uuid.UUID:
    device_id = uuid.uuid4()
    now = datetime.datetime.now(datetime.UTC)
    pg_execute(
        "INSERT INTO device (id, project_id, organization_id, name, description, tags, "
        "status, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
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
    updates: list[str] = []
    params: list[Any] = []

    if data.description is not None:
        updates.append("description=%s")
        params.append(data.description)
    if data.tags is not None:
        updates.append("tags=%s")
        params.append(data.tags)
    if data.status is not None:
        updates.append("status=%s")
        params.append(data.status)

    if not updates:
        return

    params.append(device_id)
    pg_execute(f"UPDATE device SET {', '.join(updates)} WHERE id=%s", tuple(params))


def delete_device_from_db(device_id: uuid.UUID):
    pg_execute("DELETE FROM device WHERE id=%s", (device_id,))


def get_device_shadow(device_id: uuid.UUID):
    return pg_fetchone(
        "SELECT device_id, reported_state, desired_state, reported_at, desired_at "
        "FROM device_shadow WHERE device_id=%s",
        (device_id,),
    )


def upsert_reported_state(device_id: uuid.UUID, state_dict: dict[str, Any]):
    now = datetime.datetime.now(datetime.UTC)
    pg_execute(
        "INSERT INTO device_shadow (device_id, reported_state, reported_at) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (device_id) DO UPDATE "
        "SET reported_state=EXCLUDED.reported_state, reported_at=EXCLUDED.reported_at",
        (device_id, json.dumps(state_dict), now),
    )
    pg_execute("UPDATE device SET last_seen=%s WHERE id=%s", (now, device_id))


def upsert_desired_state(device_id: uuid.UUID, state_dict: dict[str, Any]):
    now = datetime.datetime.now(datetime.UTC)
    pg_execute(
        "INSERT INTO device_shadow (device_id, desired_state, desired_at) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (device_id) DO UPDATE "
        "SET desired_state=EXCLUDED.desired_state, desired_at=EXCLUDED.desired_at",
        (device_id, json.dumps(state_dict), now),
    )


def compute_shadow_delta(reported: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in desired.items() if reported.get(k) != v}
