"""
Config API routes.

Exposes system configuration for frontend and debugging.
"""
from fastapi import APIRouter
from services.config_service import get_config_service

router = APIRouter()


@router.get("")
async def get_config():
    """Get all system configuration."""
    service = get_config_service()
    return {
        "global": service.get_all_global(),
        "product_types": service.get_all_product_types(),
    }


@router.post("/reload")
async def reload_config():
    """Force reload config from database."""
    service = get_config_service()
    service.reload()
    return {"status": "reloaded"}
