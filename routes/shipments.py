"""
Shipment API routes.

See BUILDER_BLUEPRINT.md for endpoint specifications.
See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.shipment import (
    ShipmentCreate,
    ShipmentUpdate,
    ShipmentStatusUpdate,
    ShipmentResponse,
    ShipmentListResponse,
    ShipmentStatus,
)
from services.shipment_service import get_shipment_service
from exceptions import (
    AppError,
    ShipmentNotFoundError,
    ShipmentBookingExistsError,
    ShipmentSHPExistsError,
    InvalidStatusTransitionError,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/shipments", tags=["Shipments"])


# ===================
# EXCEPTION HANDLER
# ===================

def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
    # Unexpected error
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


# ===================
# ROUTES
# ===================

@router.get("", response_model=ShipmentListResponse)
async def list_shipments(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: Optional[ShipmentStatus] = Query(None, description="Filter by status"),
    factory_order_id: Optional[str] = Query(None, description="Filter by factory order"),
    include_inactive: bool = Query(False, description="Include inactive shipments")
):
    """
    List all shipments with optional filters.

    Returns paginated list of shipments.
    """
    try:
        service = get_shipment_service()

        shipments, total = service.get_all(
            page=page,
            page_size=page_size,
            status=status,
            factory_order_id=factory_order_id,
            active_only=not include_inactive
        )

        total_pages = (total + page_size - 1) // page_size

        return ShipmentListResponse(
            data=shipments,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        return handle_error(e)


@router.get("/status/{status}", response_model=list[ShipmentResponse])
async def get_shipments_by_status(status: ShipmentStatus):
    """
    Get all shipments with a specific status.

    Useful for workflow views (e.g., all IN_TRANSIT shipments).
    """
    try:
        service = get_shipment_service()
        return service.get_by_status(status)

    except Exception as e:
        return handle_error(e)


@router.get("/booking/{booking_number}", response_model=ShipmentResponse)
async def get_shipment_by_booking(booking_number: str):
    """
    Get a shipment by booking number.

    Raises:
        404: Shipment not found
    """
    try:
        service = get_shipment_service()
        shipment = service.get_by_booking_number(booking_number)

        if not shipment:
            raise ShipmentNotFoundError(booking_number)

        return shipment

    except ShipmentNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.get("/shp/{shp_number}", response_model=ShipmentResponse)
async def get_shipment_by_shp(shp_number: str):
    """
    Get a shipment by SHP number (TIBA reference).

    Raises:
        404: Shipment not found
    """
    try:
        service = get_shipment_service()
        shipment = service.get_by_shp_number(shp_number)

        if not shipment:
            raise ShipmentNotFoundError(shp_number)

        return shipment

    except ShipmentNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.get("/{shipment_id}", response_model=ShipmentResponse)
async def get_shipment(shipment_id: str):
    """
    Get a single shipment by ID.

    Raises:
        404: Shipment not found
    """
    try:
        service = get_shipment_service()
        return service.get_by_id(shipment_id)

    except ShipmentNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.post("", response_model=ShipmentResponse, status_code=201)
async def create_shipment(data: ShipmentCreate):
    """
    Create a new shipment.

    Raises:
        409: Booking number or SHP number already exists
        422: Validation error
    """
    try:
        service = get_shipment_service()
        return service.create(data)

    except (ShipmentBookingExistsError, ShipmentSHPExistsError) as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{shipment_id}", response_model=ShipmentResponse)
async def update_shipment(shipment_id: str, data: ShipmentUpdate):
    """
    Update an existing shipment.

    Only provided fields are updated.
    Use PATCH /{id}/status to update status.

    Raises:
        404: Shipment not found
        409: New booking/SHP number already exists
        422: Validation error
    """
    try:
        service = get_shipment_service()
        return service.update(shipment_id, data)

    except (ShipmentNotFoundError, ShipmentBookingExistsError, ShipmentSHPExistsError) as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{shipment_id}/status", response_model=ShipmentResponse)
async def update_shipment_status(shipment_id: str, data: ShipmentStatusUpdate):
    """
    Update shipment status.

    Validates status transitions:
    - Can skip forward (AT_FACTORY -> IN_TRANSIT is OK)
    - Cannot go backward (IN_TRANSIT -> AT_ORIGIN_PORT is NOT OK)
    - DELIVERED is terminal (cannot change from DELIVERED)

    Raises:
        404: Shipment not found
        422: Invalid status transition
    """
    try:
        service = get_shipment_service()
        return service.update_status(shipment_id, data)

    except (ShipmentNotFoundError, InvalidStatusTransitionError) as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.delete("/{shipment_id}", status_code=204)
async def delete_shipment(shipment_id: str):
    """
    Delete a shipment (soft delete).

    Sets active=False rather than removing from database.

    Raises:
        404: Shipment not found
    """
    try:
        service = get_shipment_service()
        service.delete(shipment_id)
        return None  # 204 No Content

    except ShipmentNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# UTILITY ROUTES
# ===================

@router.get("/count/total")
async def count_shipments(
    status: Optional[ShipmentStatus] = Query(None, description="Filter by status"),
    include_inactive: bool = Query(False, description="Include inactive")
):
    """Get total shipment count."""
    try:
        service = get_shipment_service()
        count = service.count(
            status=status,
            active_only=not include_inactive
        )
        return {"count": count}

    except Exception as e:
        return handle_error(e)