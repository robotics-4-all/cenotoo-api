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
    delete_data,
    get_data,
    get_data_stats,
    organization,
    project,
    project_keys,
    send_data,
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
router.include_router(get_data.router)
router.include_router(delete_data.router)
router.include_router(get_data_stats.router)
router.include_router(users.router)
