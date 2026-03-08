"""Organization management endpoints.

This module provides API endpoints for retrieving and updating organization information.
"""

from fastapi import APIRouter, Depends

from dependencies import (
    check_organization_exists,
    get_organization_id,
    verify_user_belongs_to_organization,
)
from models.organization_models import OrganizationResponse, OrganizationUpdateRequest
from services.organization_service import get_organization_info_service, update_organization_service

router = APIRouter()
TAG = "Organization Management"

# GET: Get information of the current organization


@router.get(
    "/organization/nostradamus",
    tags=[TAG],
    response_model=OrganizationResponse,
    dependencies=[Depends(verify_user_belongs_to_organization)],
)
async def get_organization_info():
    """Retrieve information for the current organization.

    Returns:
        Organization information.
    """
    organization = check_organization_exists(get_organization_id())
    return await get_organization_info_service(organization)


# PUT: Update the current organization


@router.put("/organization/nostradamus", tags=[TAG])
async def update_organization(data: OrganizationUpdateRequest):
    """Update the current organization.

    Args:
        data: Organization update request with new description and/or tags.

    Returns:
        Success message.
    """
    organization_id = get_organization_id()
    return await update_organization_service(organization_id, data)
