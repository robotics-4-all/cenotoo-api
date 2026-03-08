# pylint: disable=too-few-public-methods
"""Project data models.

This module defines Pydantic models for project-related requests and responses.
"""

import uuid

from pydantic import BaseModel


class ProjectCreateRequest(BaseModel):
    """Request model for creating a new project.

    Attributes:
        project_name: The project name.
        description: Project description (optional).
        tags: List of tags for categorization.
    """

    project_name: str
    description: str | None
    tags: list[str]


class ProjectUpdateRequest(BaseModel):
    """Request model for updating an existing project.

    Attributes:
        description: Updated description (optional).
        tags: Updated tags list (optional).
    """

    description: str | None
    tags: list[str] | None


class ProjectResponse(BaseModel):
    """Response model for project information.

    Contains complete project details including IDs, names, and metadata.
    """

    organization_id: uuid.UUID
    project_id: uuid.UUID
    organization_name: str
    project_name: str
    description: str | None
    tags: list[str]
    creation_date: str
