import uuid

from fastapi import APIRouter, Depends

from dependencies import (
    check_project_exists,
    get_organization_id,
    verify_endpoint_access,
    verify_master_access,
)
from models.flink_job_models import (
    CrossCollectionJobListResponse,
    CrossCollectionJobRequest,
    CrossCollectionJobResponse,
    CrossCollectionSchemaResponse,
    FlinkJobResultsResponse,
)
from services.cross_collection_job_service import (
    create_cross_collection_job_service,
    delete_cross_collection_job_service,
    get_cross_collection_job_results_service,
    get_cross_collection_job_service,
    get_cross_collection_schema_service,
    list_cross_collection_jobs_service,
    restart_cross_collection_job_service,
    stop_cross_collection_job_service,
)

router = APIRouter(dependencies=[Depends(check_project_exists)])
TAG = "Cross-Collection Jobs"


@router.get(
    "/projects/{project_id}/cross-collection-jobs/schema",
    tags=[TAG],
    response_model=CrossCollectionSchemaResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_schema(project_id: uuid.UUID):
    return await get_cross_collection_schema_service(get_organization_id(), project_id)


@router.post(
    "/projects/{project_id}/cross-collection-jobs",
    tags=[TAG],
    response_model=CrossCollectionJobResponse,
    dependencies=[Depends(verify_master_access)],
)
async def create_job(project_id: uuid.UUID, job: CrossCollectionJobRequest):
    return await create_cross_collection_job_service(get_organization_id(), project_id, job)


@router.get(
    "/projects/{project_id}/cross-collection-jobs",
    tags=[TAG],
    response_model=CrossCollectionJobListResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def list_jobs(project_id: uuid.UUID):
    items = await list_cross_collection_jobs_service(project_id)
    return CrossCollectionJobListResponse(items=items, total=len(items))


@router.get(
    "/projects/{project_id}/cross-collection-jobs/{job_id}",
    tags=[TAG],
    response_model=CrossCollectionJobResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_job(project_id: uuid.UUID, job_id: uuid.UUID):
    return await get_cross_collection_job_service(project_id, job_id)


@router.post(
    "/projects/{project_id}/cross-collection-jobs/{job_id}/cancel",
    tags=[TAG],
    dependencies=[Depends(verify_master_access)],
)
async def stop_job(project_id: uuid.UUID, job_id: uuid.UUID):
    return await stop_cross_collection_job_service(project_id, job_id)


@router.delete(
    "/projects/{project_id}/cross-collection-jobs/{job_id}",
    tags=[TAG],
    dependencies=[Depends(verify_master_access)],
)
async def delete_job(project_id: uuid.UUID, job_id: uuid.UUID):
    return await delete_cross_collection_job_service(project_id, job_id)


@router.post(
    "/projects/{project_id}/cross-collection-jobs/{job_id}/restart",
    tags=[TAG],
    response_model=CrossCollectionJobResponse,
    dependencies=[Depends(verify_master_access)],
)
async def restart_job(project_id: uuid.UUID, job_id: uuid.UUID):
    return await restart_cross_collection_job_service(get_organization_id(), project_id, job_id)


@router.get(
    "/projects/{project_id}/cross-collection-jobs/{job_id}/results",
    tags=[TAG],
    response_model=FlinkJobResultsResponse,
    dependencies=[Depends(verify_endpoint_access)],
)
async def get_results(project_id: uuid.UUID, job_id: uuid.UUID, limit: int = 100):
    return await get_cross_collection_job_results_service(project_id, job_id, limit)
