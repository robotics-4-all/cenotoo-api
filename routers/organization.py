"""Organization management endpoints."""

from fastapi import APIRouter, Depends

from dependencies import (
    check_organization_exists,
    get_organization_id,
    verify_superadmin,
    verify_user_belongs_to_organization,
)
from models.organization_models import (
    OrganizationCreateRequest,
    OrganizationResponse,
    OrganizationUpdateRequest,
)
from services.organization_service import (
    create_organization_service,
    delete_organization_service,
    get_all_organizations_service,
    get_organization_info_service,
    update_organization_service,
)

router = APIRouter()
TAG = "Organization Management"


@router.post(
    "/organizations",
    tags=[TAG],
    status_code=201,
    dependencies=[Depends(verify_superadmin)],
)
async def create_organization(data: OrganizationCreateRequest):
    """Create a new organization and its associated Cassandra keyspace."""
    return await create_organization_service(data)


@router.get(
    "/organizations",
    tags=[TAG],
    response_model=list[OrganizationResponse],
    dependencies=[Depends(verify_superadmin)],
)
async def list_organizations():
    """List all organizations."""
    return await get_all_organizations_service()


@router.get(
    "/organization",
    tags=[TAG],
    response_model=OrganizationResponse,
    dependencies=[Depends(verify_user_belongs_to_organization)],
)
async def get_organization_info():
    """Retrieve information for the current organization."""
    organization = check_organization_exists(get_organization_id())
    return await get_organization_info_service(organization)


@router.put("/organization", tags=[TAG])
async def update_organization(data: OrganizationUpdateRequest):
    """Update the current organization."""
    organization_id = get_organization_id()
    return await update_organization_service(organization_id, data)


@router.delete(
    "/organization",
    tags=[TAG],
    dependencies=[Depends(verify_superadmin)],
)
async def delete_organization():
    """Delete the current organization and its Cassandra keyspace."""
    organization = check_organization_exists(get_organization_id())
    return await delete_organization_service(organization)
