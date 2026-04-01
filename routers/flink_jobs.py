import uuid

from fastapi import APIRouter, Depends

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
    verify_master_access,
)
from models.flink_job_models import (
    FlinkJobListResponse,
    FlinkJobResponse,
    FlinkJobResultsResponse,
    GuidedJobRequest,
)
from services.flink_job_service import (
    create_guided_job_service,
    delete_job_service,
    get_job_results_service,
    get_job_service,
    list_jobs_service,
    list_project_jobs_service,
    restart_job_service,
    stop_job_service,
)
from utilities.collection_utils import check_collection_exists

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Flink Jobs"


@router.post(
    "/projects/{project_id}/collections/{collection_id}/jobs",
    tags=[TAG],
    response_model=FlinkJobResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def create_guided_job(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job: GuidedJobRequest,
):
    organization_id = get_organization_id()
    return await create_guided_job_service(organization_id, project_id, collection_id, job)


@router.get(
    "/projects/{project_id}/collections/{collection_id}/jobs",
    tags=[TAG],
    response_model=FlinkJobListResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def list_jobs(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
):
    items = await list_jobs_service(project_id, collection_id)
    return FlinkJobListResponse(items=items, total=len(items))


@router.get(
    "/projects/{project_id}/collections/{collection_id}/jobs/{job_id}",
    tags=[TAG],
    response_model=FlinkJobResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def get_job(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
):
    return await get_job_service(project_id, collection_id, job_id)


@router.post(
    "/projects/{project_id}/collections/{collection_id}/jobs/{job_id}/cancel",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def stop_job(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
):
    return await stop_job_service(project_id, collection_id, job_id)


@router.delete(
    "/projects/{project_id}/collections/{collection_id}/jobs/{job_id}",
    tags=[TAG],
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def delete_job(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
):
    return await delete_job_service(project_id, collection_id, job_id)


@router.post(
    "/projects/{project_id}/collections/{collection_id}/jobs/{job_id}/restart",
    tags=[TAG],
    response_model=FlinkJobResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_master_access)],
)
async def restart_job(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
):
    organization_id = get_organization_id()
    return await restart_job_service(organization_id, project_id, collection_id, job_id)


@router.get(
    "/projects/{project_id}/collections/{collection_id}/jobs/{job_id}/results",
    tags=[TAG],
    response_model=FlinkJobResultsResponse,
    dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)],
)
async def get_job_results(
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    job_id: uuid.UUID,
    limit: int = 100,
):
    return await get_job_results_service(project_id, collection_id, job_id, limit)


@router.get(
    "/projects/{project_id}/jobs",
    tags=[TAG],
    response_model=FlinkJobListResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def list_project_jobs(project_id: uuid.UUID):
    items = await list_project_jobs_service(project_id)
    return FlinkJobListResponse(items=items, total=len(items))
