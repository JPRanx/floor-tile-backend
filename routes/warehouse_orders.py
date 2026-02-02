"""
Warehouse order API routes.

Tracks Order Builder exports - SIESA stock selected for shipment on specific boats.
Used to prevent double-ordering and calculate pending coverage.

See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional

import structlog

from models.warehouse_order import (
    WarehouseOrderCreate,
    WarehouseOrderStatusUpdate,
    WarehouseOrderResponse,
    WarehouseOrderWithItemsResponse,
    WarehouseOrderItemResponse,
    WarehouseOrderListResponse,
    WarehouseOrderStatus,
    PendingOrdersForBoat,
    PendingOrdersBySku,
)
from services.warehouse_order_service import get_warehouse_order_service
from exceptions import AppError, WarehouseOrderNotFoundError

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/warehouse-orders", tags=["Warehouse Orders"])


# ===================
# EXCEPTION HANDLER
# ===================


def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(status_code=e.status_code, content=e.to_dict())
    # Unexpected error
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "INTERNAL_ERROR", "message": "An unexpected error occurred"}},
    )


# ===================
# LIST ROUTES
# ===================


@router.get("", response_model=WarehouseOrderListResponse)
async def list_warehouse_orders(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: Optional[WarehouseOrderStatus] = Query(None, description="Filter by status"),
    boat_id: Optional[str] = Query(None, description="Filter by boat ID"),
):
    """
    List all warehouse orders with optional filters.

    Returns paginated list of warehouse orders.
    """
    try:
        service = get_warehouse_order_service()

        orders, total = service.get_all(
            page=page,
            page_size=page_size,
            status=status,
            boat_id=boat_id,
        )

        total_pages = (total + page_size - 1) // page_size

        return WarehouseOrderListResponse(
            data=orders,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    except Exception as e:
        return handle_error(e)


@router.get("/status/{status}", response_model=list[WarehouseOrderResponse])
async def get_orders_by_status(status: WarehouseOrderStatus):
    """
    Get all orders with a specific status.

    Useful for workflow views (e.g., all PENDING orders).
    """
    try:
        service = get_warehouse_order_service()
        orders, _ = service.get_all(page=1, page_size=1000, status=status)
        return orders

    except Exception as e:
        return handle_error(e)


@router.get("/boat/{boat_id}", response_model=list[WarehouseOrderResponse])
async def get_orders_by_boat(
    boat_id: str,
    status: Optional[WarehouseOrderStatus] = Query(None, description="Filter by status"),
):
    """
    Get all orders for a specific boat.

    Used to see what has been ordered for an upcoming boat.
    """
    try:
        service = get_warehouse_order_service()
        return service.get_by_boat_id(boat_id, status=status)

    except Exception as e:
        return handle_error(e)


# ===================
# PENDING QUERIES (for coverage calculation)
# ===================


@router.get("/pending/by-boat/{boat_id}", response_model=Optional[PendingOrdersForBoat])
async def get_pending_for_boat(boat_id: str):
    """
    Get aggregated pending order info for a specific boat.

    Used in Order Builder to show what's already ordered for this boat.
    Returns None if no pending orders exist.
    """
    try:
        service = get_warehouse_order_service()
        return service.get_pending_for_boat(boat_id)

    except Exception as e:
        return handle_error(e)


@router.get("/pending/by-sku", response_model=list[PendingOrdersBySku])
async def get_pending_by_sku():
    """
    Get pending order quantities grouped by SKU.

    Used in Order Builder coverage calculation:
    coverage_gap = adjusted_need - warehouse_m2 - in_transit_m2 - pending_order_m2
    """
    try:
        service = get_warehouse_order_service()
        return service.get_pending_by_sku()

    except Exception as e:
        return handle_error(e)


# ===================
# SINGLE ORDER ROUTES
# ===================


@router.get("/{order_id}", response_model=WarehouseOrderWithItemsResponse)
async def get_warehouse_order(order_id: str):
    """
    Get a single warehouse order by ID.

    Includes line items.

    Raises:
        404: Order not found
    """
    try:
        service = get_warehouse_order_service()
        return service.get_by_id(order_id)

    except WarehouseOrderNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.get("/{order_id}/items", response_model=list[WarehouseOrderItemResponse])
async def get_warehouse_order_items(order_id: str):
    """
    Get all line items for a warehouse order.

    Raises:
        404: Order not found
    """
    try:
        service = get_warehouse_order_service()
        order = service.get_by_id(order_id, include_items=True)
        return order.items

    except WarehouseOrderNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# CREATE / UPDATE ROUTES
# ===================


@router.post("", response_model=WarehouseOrderWithItemsResponse, status_code=201)
async def create_warehouse_order(data: WarehouseOrderCreate):
    """
    Create a new warehouse order with items.

    Re-export logic: If a PENDING order already exists for the same boat,
    it will be automatically cancelled and replaced by this new order.

    Raises:
        422: Validation error
    """
    try:
        service = get_warehouse_order_service()
        return service.create(data)

    except Exception as e:
        return handle_error(e)


@router.patch("/{order_id}/status", response_model=WarehouseOrderWithItemsResponse)
async def update_warehouse_order_status(order_id: str, data: WarehouseOrderStatusUpdate):
    """
    Update warehouse order status.

    Valid transitions:
    - pending -> shipped
    - pending -> cancelled
    - shipped -> received

    Raises:
        404: Order not found
        422: Invalid status transition
    """
    try:
        service = get_warehouse_order_service()
        return service.update_status(order_id, data)

    except WarehouseOrderNotFoundError as e:
        return handle_error(e)
    except ValueError as e:
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "INVALID_STATUS_TRANSITION", "message": str(e)}},
        )
    except Exception as e:
        return handle_error(e)


@router.post("/{order_id}/cancel", status_code=204)
async def cancel_warehouse_order(
    order_id: str,
    reason: Optional[str] = Query(None, description="Cancellation reason"),
):
    """
    Cancel a warehouse order.

    Only PENDING orders can be cancelled.

    Raises:
        404: Order not found
        422: Order is not PENDING
    """
    try:
        service = get_warehouse_order_service()
        service.cancel(order_id, reason=reason)
        return None  # 204 No Content

    except WarehouseOrderNotFoundError as e:
        return handle_error(e)
    except ValueError as e:
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "CANNOT_CANCEL", "message": str(e)}},
        )
    except Exception as e:
        return handle_error(e)


# ===================
# UTILITY ROUTES
# ===================


@router.get("/count/total")
async def count_warehouse_orders(
    status: Optional[WarehouseOrderStatus] = Query(None, description="Filter by status"),
    boat_id: Optional[str] = Query(None, description="Filter by boat ID"),
):
    """Get total warehouse order count."""
    try:
        service = get_warehouse_order_service()
        count = service.count(status=status, boat_id=boat_id)
        return {"count": count}

    except Exception as e:
        return handle_error(e)
