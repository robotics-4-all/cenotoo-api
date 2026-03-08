"""Organization data models.

This module defines Pydantic models for organization-related requests and responses.
"""

import uuid

from pydantic import BaseModel


class OrganizationCreateRequest(BaseModel):
    """Request model for creating a new organization.

    Attributes:
        organization_name: The organization name.
        description: Description of the organization.
        tags: List of tags for categorization.
    """

    organization_name: str
    description: str
    tags: list[str] = []


class OrganizationUpdateRequest(BaseModel):
    """Request model for updating an existing organization.

    Attributes:
        description: Updated description (optional).
        tags: Updated tags list (optional).
    """

    description: str | None = None
    tags: list[str] | None = None


class OrganizationResponse(BaseModel):
    """Response model for organization information.

    Contains complete organization details including ID, name, and metadata.
    """

    organization_id: uuid.UUID
    organization_name: str
    description: str
    creation_date: str
    tags: list[str] | None = None
