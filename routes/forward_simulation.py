"""
Forward simulation API routes.

Planning horizon endpoints for factory-level forward projections.
"""

from fastapi import APIRouter, HTTPException, Query
import structlog

from models.forward_simulation import PlanningHorizonResponse
from services.forward_simulation_service import get_forward_simulation_service
from services.factory_service import get_factory_service

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/horizon", response_model=PlanningHorizonResponse)
async def get_default_planning_horizon(
    months: int = Query(3, ge=1, le=12, description="Months to project ahead"),
):
    """
    Get planning horizon for the default (active) factory.

    Uses the first active factory. Returns 404 if no active factory exists.

    Args:
        months: Number of months to project ahead (1-12, default 3)

    Returns:
        Planning horizon with projected inventory and order signals
    """
    try:
        factory_service = get_factory_service()
        active_factories = factory_service.get_active()

        if not active_factories:
            raise HTTPException(status_code=404, detail="No active factory found")

        service = get_forward_simulation_service()
        return service.get_planning_horizon(active_factories[0].id, months)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_default_planning_horizon_failed", error=str(e))
        raise HTTPException(
            status_code=500, detail="Failed to compute planning horizon"
        )


@router.get("/horizon/{factory_id}", response_model=PlanningHorizonResponse)
async def get_planning_horizon(
    factory_id: str,
    months: int = Query(3, ge=1, le=12, description="Months to project ahead"),
):
    """
    Get planning horizon for a specific factory.

    Args:
        factory_id: Factory UUID
        months: Number of months to project ahead (1-12, default 3)

    Returns:
        Planning horizon with projected inventory and order signals
    """
    try:
        service = get_forward_simulation_service()
        return service.get_planning_horizon(factory_id, months)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "get_planning_horizon_failed",
            factory_id=factory_id,
            months=months,
            error=str(e),
        )
        raise HTTPException(
            status_code=500, detail="Failed to compute planning horizon"
        )
