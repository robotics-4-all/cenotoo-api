"""
API v1 router assembly.

All v1 endpoints are mounted here. When v2 is needed, create api/v2.py
with its own router and mount it at /api/v2 in main.py.
"""

from fastapi import APIRouter

from routers import (
    auth,
    collection_keys,
    collections,
    cross_collection_jobs,
    delete_data,
    devices,
    export_data,
    flink_jobs,
    get_data,
    get_data_stats,
    import_data,
    organization,
    project,
    project_keys,
    rules,
    send_data,
    store_data,
    stream_data,
    users,
)

router = APIRouter()

router.include_router(auth.router)
router.include_router(organization.router)
router.include_router(project.router)
router.include_router(project_keys.router)
router.include_router(collections.router)
router.include_router(collection_keys.router)
router.include_router(send_data.router)
router.include_router(store_data.router)
router.include_router(get_data.router)
router.include_router(delete_data.router)
router.include_router(get_data_stats.router)
router.include_router(flink_jobs.router)
router.include_router(cross_collection_jobs.router)
router.include_router(users.router)
router.include_router(stream_data.router)
router.include_router(devices.router)
router.include_router(export_data.router)
router.include_router(import_data.router)
router.include_router(rules.router)
