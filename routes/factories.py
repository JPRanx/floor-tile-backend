"""
Factory API routes.

Simple read-only endpoints for factory lookup.
"""

from fastapi import APIRouter, HTTPException
import structlog

from models.factory import FactoryResponse
from services.factory_service import get_factory_service

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("", response_model=list[FactoryResponse])
async def list_factories():
    """
    Get all factories ordered by sort_order.

    Returns list of all factories (small table, no pagination needed).
    """
    try:
        service = get_factory_service()
        return service.get_all()
    except Exception as e:
        logger.error("list_factories_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch factories")


@router.get("/active", response_model=list[FactoryResponse])
async def list_active_factories():
    """
    Get only active factories ordered by sort_order.

    Returns list of active factories for dropdowns and selectors.
    """
    try:
        service = get_factory_service()
        return service.get_active()
    except Exception as e:
        logger.error("list_active_factories_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch active factories")


@router.get("/{factory_id}", response_model=FactoryResponse)
async def get_factory(factory_id: str):
    """
    Get a single factory by ID.

    Args:
        factory_id: Factory UUID

    Returns:
        Factory details

    Raises:
        404: Factory not found
    """
    try:
        service = get_factory_service()
        factory = service.get_by_id(factory_id)

        if not factory:
            raise HTTPException(status_code=404, detail="Factory not found")

        return factory
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_factory_failed", factory_id=factory_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch factory")
