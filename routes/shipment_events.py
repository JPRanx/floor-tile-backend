"""
Shipment Event Routes - API endpoints for shipment event history.

Read-only endpoints. Events are created internally by shipment_service.
"""

from fastapi import APIRouter, HTTPException
import structlog

from models.shipment_event import (
    ShipmentEventResponse,
    ShipmentEventListResponse
)
from services.shipment_event_service import get_shipment_event_service
from exceptions import ShipmentEventNotFoundError

logger = structlog.get_logger(__name__)

router = APIRouter(
    prefix="/api/shipments",
    tags=["shipment-events"]
)


def handle_error(e: Exception):
    """Convert service exceptions to HTTP responses."""
    if isinstance(e, ShipmentEventNotFoundError):
        raise HTTPException(status_code=404, detail=str(e))
    raise HTTPException(status_code=500, detail="Internal server error")


# ===================
# SHIPMENT EVENT ROUTES
# ===================

@router.get("/{shipment_id}/events", response_model=ShipmentEventListResponse)
async def get_shipment_events(shipment_id: str):
    """
    Get all events for a shipment.

    Returns events ordered by occurred_at DESC (newest first).
    """
    try:
        service = get_shipment_event_service()
        return service.get_by_shipment(shipment_id)
    except Exception as e:
        logger.error("get_shipment_events_failed", shipment_id=shipment_id, error=str(e))
        return handle_error(e)


@router.get("/{shipment_id}/events/latest", response_model=ShipmentEventResponse)
async def get_latest_shipment_event(shipment_id: str):
    """
    Get the most recent event for a shipment.

    Useful for checking current status without loading full history.
    """
    try:
        service = get_shipment_event_service()
        return service.get_latest(shipment_id)
    except ShipmentEventNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        logger.error("get_latest_shipment_event_failed", shipment_id=shipment_id, error=str(e))
        return handle_error(e)
