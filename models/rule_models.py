import uuid

from pydantic import BaseModel


class RuleCreateRequest(BaseModel):
    name: str
    description: str = ""
    field: str
    operator: str
    threshold: float
    webhook_url: str
    cooldown_seconds: int = 60
    enabled: bool = True


class RuleUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    threshold: float | None = None
    webhook_url: str | None = None
    cooldown_seconds: int | None = None
    enabled: bool | None = None


class RuleResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    collection_id: uuid.UUID
    name: str
    description: str
    field: str
    operator: str
    threshold: float
    webhook_url: str
    cooldown_seconds: int
    enabled: bool
    last_fired_at: str | None
    created_at: str
