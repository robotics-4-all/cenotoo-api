import uuid
from typing import Any

from pydantic import BaseModel


class DeviceCreateRequest(BaseModel):
    name: str
    description: str = ""
    tags: list[str] = []


class DeviceUpdateRequest(BaseModel):
    description: str | None = None
    tags: list[str] | None = None
    status: str | None = None


class DeviceResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    description: str
    tags: list[str]
    status: str
    last_seen: str | None
    created_at: str


class ShadowUpdateRequest(BaseModel):
    state: dict[str, Any]


class DeviceShadowResponse(BaseModel):
    device_id: uuid.UUID
    reported: dict[str, Any]
    desired: dict[str, Any]
    delta: dict[str, Any]
    reported_at: str | None
    desired_at: str | None
