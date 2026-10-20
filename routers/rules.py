import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException

from dependencies import (
    check_project_exists,
    verify_endpoint_access,
    verify_master_access,
)
from models.rule_models import RuleCreateRequest, RuleResponse, RuleUpdateRequest
from utilities.collection_utils import check_collection_exists
from utilities.rule_utils import (
    check_rule_exists,
    delete_rule_from_db,
    fetch_all_rules,
    get_rule_by_id,
    insert_rule,
    update_rule_in_db,
)

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Alert Rules"


@router.post(
    "/projects/{project_id}/collections/{collection_id}/rules",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
    response_model=RuleResponse,
)
def create_rule(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    data: RuleCreateRequest,
):
    if data.operator not in ("gt", "lt", "gte", "lte", "eq", "neq"):
        raise HTTPException(status_code=400, detail="Invalid operator")

    rule_id = insert_rule(project_id, collection_id, data)
    rule = get_rule_by_id(rule_id, project_id, collection_id)
    return _format_rule_response(rule)


@router.get(
    "/projects/{project_id}/collections/{collection_id}/rules",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
    response_model=list[RuleResponse],
)
def list_rules(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
):
    rules = fetch_all_rules(project_id, collection_id)
    return [_format_rule_response(r) for r in rules]


@router.get(
    "/projects/{project_id}/collections/{collection_id}/rules/{rule_id}",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
    response_model=RuleResponse,
)
def get_rule(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    rule_id: uuid.UUID,
):
    rule = check_rule_exists(rule_id, project_id, collection_id)
    return _format_rule_response(rule)


@router.put(
    "/projects/{project_id}/collections/{collection_id}/rules/{rule_id}",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
    response_model=RuleResponse,
)
def update_rule(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    rule_id: uuid.UUID,
    data: RuleUpdateRequest,
):
    update_rule_in_db(rule_id, project_id, collection_id, data)
    rule = get_rule_by_id(rule_id, project_id, collection_id)
    return _format_rule_response(rule)


@router.delete(
    "/projects/{project_id}/collections/{collection_id}/rules/{rule_id}",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
def delete_rule(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    rule_id: uuid.UUID,
):
    delete_rule_from_db(rule_id, project_id, collection_id)
    return {"message": "Rule deleted successfully"}


def _format_rule_response(rule_row) -> dict:
    return {
        "id": rule_row.id,
        "project_id": rule_row.project_id,
        "collection_id": rule_row.collection_id,
        "name": rule_row.name,
        "description": rule_row.description,
        "field": rule_row.field,
        "operator": rule_row.operator,
        "threshold": rule_row.threshold,
        "webhook_url": rule_row.webhook_url,
        "cooldown_seconds": rule_row.cooldown_seconds,
        "enabled": rule_row.enabled,
        "last_fired_at": rule_row.last_fired_at.isoformat() if rule_row.last_fired_at else None,
        "created_at": rule_row.created_at.isoformat() if rule_row.created_at else None,
    }
