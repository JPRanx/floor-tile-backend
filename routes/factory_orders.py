"""
Factory order API routes.

See BUILDER_BLUEPRINT.md for endpoint specifications.
See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.factory_order import (
    FactoryOrderCreate,
    FactoryOrderUpdate,
    FactoryOrderStatusUpdate,
    FactoryOrderResponse,
    FactoryOrderWithItemsResponse,
    FactoryOrderItemResponse,
    FactoryOrderListResponse,
    OrderStatus,
)
from services.factory_order_service import get_factory_order_service
from exceptions import (
    AppError,
    FactoryOrderNotFoundError,
    FactoryOrderPVExistsError,
    InvalidStatusTransitionError,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/factory-orders", tags=["Factory Orders"])


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

@router.get("", response_model=FactoryOrderListResponse)
async def list_factory_orders(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: Optional[OrderStatus] = Query(None, description="Filter by status"),
    include_inactive: bool = Query(False, description="Include inactive orders")
):
    """
    List all factory orders with optional filters.

    Returns paginated list of factory orders.
    """
    try:
        service = get_factory_order_service()

        orders, total = service.get_all(
            page=page,
            page_size=page_size,
            status=status,
            active_only=not include_inactive
        )

        total_pages = (total + page_size - 1) // page_size

        return FactoryOrderListResponse(
            data=orders,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        return handle_error(e)


@router.get("/status/{status}", response_model=list[FactoryOrderResponse])
async def get_orders_by_status(status: OrderStatus):
    """
    Get all orders with a specific status.

    Useful for workflow views (e.g., all PENDING orders).
    """
    try:
        service = get_factory_order_service()
        return service.get_by_status(status)

    except Exception as e:
        return handle_error(e)


@router.get("/pv/{pv_number}", response_model=FactoryOrderWithItemsResponse)
async def get_factory_order_by_pv(pv_number: str):
    """
    Get a factory order by PV number.

    Raises:
        404: Order not found
    """
    try:
        service = get_factory_order_service()
        order = service.get_by_pv_number(pv_number)

        if not order:
            raise FactoryOrderNotFoundError(pv_number)

        return order

    except FactoryOrderNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.get("/{order_id}", response_model=FactoryOrderWithItemsResponse)
async def get_factory_order(order_id: str):
    """
    Get a single factory order by ID.

    Includes line items.

    Raises:
        404: Order not found
    """
    try:
        service = get_factory_order_service()
        return service.get_by_id(order_id)

    except FactoryOrderNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.get("/{order_id}/items", response_model=list[FactoryOrderItemResponse])
async def get_factory_order_items(order_id: str):
    """
    Get all line items for a factory order.

    Raises:
        404: Order not found
    """
    try:
        service = get_factory_order_service()
        return service.get_items(order_id)

    except FactoryOrderNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.post("", response_model=FactoryOrderWithItemsResponse, status_code=201)
async def create_factory_order(data: FactoryOrderCreate):
    """
    Create a new factory order with items.

    Raises:
        409: PV number already exists
        422: Validation error
    """
    try:
        service = get_factory_order_service()
        return service.create(data)

    except FactoryOrderPVExistsError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{order_id}", response_model=FactoryOrderWithItemsResponse)
async def update_factory_order(order_id: str, data: FactoryOrderUpdate):
    """
    Update an existing factory order.

    Only provided fields are updated.
    Use PATCH /{id}/status to update status.

    Raises:
        404: Order not found
        409: New PV number already exists
        422: Validation error
    """
    try:
        service = get_factory_order_service()
        return service.update(order_id, data)

    except (FactoryOrderNotFoundError, FactoryOrderPVExistsError) as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{order_id}/status", response_model=FactoryOrderWithItemsResponse)
async def update_factory_order_status(order_id: str, data: FactoryOrderStatusUpdate):
    """
    Update factory order status.

    Validates status transitions:
    - Can skip forward (PENDING -> READY is OK)
    - Cannot go backward (READY -> CONFIRMED is NOT OK)
    - SHIPPED is terminal (cannot change from SHIPPED)

    Raises:
        404: Order not found
        422: Invalid status transition
    """
    try:
        service = get_factory_order_service()
        return service.update_status(order_id, data)

    except (FactoryOrderNotFoundError, InvalidStatusTransitionError) as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.delete("/{order_id}", status_code=204)
async def delete_factory_order(order_id: str):
    """
    Delete a factory order (soft delete).

    Sets active=False rather than removing from database.

    Raises:
        404: Order not found
    """
    try:
        service = get_factory_order_service()
        service.delete(order_id)
        return None  # 204 No Content

    except FactoryOrderNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# UTILITY ROUTES
# ===================

@router.get("/count/total")
async def count_factory_orders(
    status: Optional[OrderStatus] = Query(None, description="Filter by status"),
    include_inactive: bool = Query(False, description="Include inactive")
):
    """Get total factory order count."""
    try:
        service = get_factory_order_service()
        count = service.count(
            status=status,
            active_only=not include_inactive
        )
        return {"count": count}

    except Exception as e:
        return handle_error(e)
