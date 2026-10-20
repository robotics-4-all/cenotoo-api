import json
import uuid

from fastapi import APIRouter, Depends, Query

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
    verify_master_access,
    verify_write_access,
)
from models.common import PaginatedResponse
from models.device_models import (
    DeviceCreateRequest,
    DeviceResponse,
    DeviceShadowResponse,
    DeviceUpdateRequest,
    ShadowUpdateRequest,
)
from utilities.device_utils import (
    check_device_exists,
    compute_shadow_delta,
    delete_device_from_db,
    fetch_all_devices,
    get_device_by_id,
    get_device_shadow,
    insert_device,
    update_device_in_db,
    upsert_desired_state,
    upsert_reported_state,
)

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Device Management"


def _device_row_to_response(row) -> DeviceResponse:
    return DeviceResponse(
        id=row.id,
        project_id=row.project_id,
        organization_id=row.organization_id,
        name=row.name,
        description=row.description or "",
        tags=list(row.tags or []),
        status=row.status or "active",
        last_seen=str(row.last_seen) if row.last_seen else None,
        created_at=str(row.created_at),
    )


@router.post(
    "/projects/{project_id}/devices",
    tags=[TAG],
    dependencies=[Depends(verify_master_access)],
)
async def register_device(project_id: uuid.UUID, data: DeviceCreateRequest):
    organization_id = get_organization_id()
    device_id = insert_device(organization_id, project_id, data)
    row = get_device_by_id(device_id, project_id, organization_id)
    return _device_row_to_response(row)


@router.get(
    "/projects/{project_id}/devices",
    tags=[TAG],
    dependencies=[Depends(verify_endpoint_access)],
)
async def list_devices(
    project_id: uuid.UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    organization_id = get_organization_id()
    rows = fetch_all_devices(organization_id, project_id)
    items = [_device_row_to_response(r) for r in rows]
    page = items[offset : offset + limit]
    return PaginatedResponse(items=page, total=len(items), offset=offset, limit=limit)


@router.get(
    "/projects/{project_id}/devices/{device_id}",
    tags=[TAG],
    response_model=DeviceResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_device(
    project_id: uuid.UUID,
    device_id: uuid.UUID,
):
    row = check_device_exists(device_id, project_id)
    return _device_row_to_response(row)


@router.put(
    "/projects/{project_id}/devices/{device_id}",
    tags=[TAG],
    dependencies=[Depends(verify_master_access)],
)
async def update_device(
    project_id: uuid.UUID,
    device_id: uuid.UUID,
    data: DeviceUpdateRequest,
):
    row = check_device_exists(device_id, project_id)
    update_device_in_db(device_id, data)
    organization_id = get_organization_id()
    updated = get_device_by_id(device_id, project_id, organization_id)
    return _device_row_to_response(updated or row)


@router.delete(
    "/projects/{project_id}/devices/{device_id}",
    tags=[TAG],
    dependencies=[Depends(verify_master_access)],
)
async def delete_device(
    project_id: uuid.UUID,
    device_id: uuid.UUID,
):
    check_device_exists(device_id, project_id)
    delete_device_from_db(device_id)
    return {"message": f"Device {device_id} deleted."}


@router.get(
    "/projects/{project_id}/devices/{device_id}/shadow",
    tags=[TAG],
    response_model=DeviceShadowResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_shadow(
    project_id: uuid.UUID,
    device_id: uuid.UUID,
):
    check_device_exists(device_id, project_id)
    shadow = get_device_shadow(device_id)
    reported: dict = json.loads(shadow.reported_state) if shadow and shadow.reported_state else {}
    desired: dict = json.loads(shadow.desired_state) if shadow and shadow.desired_state else {}
    delta = compute_shadow_delta(reported, desired)
    return DeviceShadowResponse(
        device_id=device_id,
        reported=reported,
        desired=desired,
        delta=delta,
        reported_at=str(shadow.reported_at) if shadow and shadow.reported_at else None,
        desired_at=str(shadow.desired_at) if shadow and shadow.desired_at else None,
    )


@router.put(
    "/projects/{project_id}/devices/{device_id}/shadow/desired",
    tags=[TAG],
    dependencies=[Depends(verify_write_access)],
)
async def set_desired_state(
    project_id: uuid.UUID,
    device_id: uuid.UUID,
    data: ShadowUpdateRequest,
):
    check_device_exists(device_id, project_id)
    upsert_desired_state(device_id, data.state)
    return {"message": "Desired state updated."}


@router.put(
    "/projects/{project_id}/devices/{device_id}/shadow/reported",
    tags=[TAG],
    dependencies=[Depends(verify_write_access)],
)
async def update_reported_state(
    project_id: uuid.UUID,
    device_id: uuid.UUID,
    data: ShadowUpdateRequest,
):
    check_device_exists(device_id, project_id)
    upsert_reported_state(device_id, data.state)
    return {"message": "Reported state updated."}
