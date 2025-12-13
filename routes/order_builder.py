"""
Order Builder API routes.

GET /api/order-builder — Get Order Builder data for the hero feature.
"""

from typing import Optional
from fastapi import APIRouter, Query
import structlog

from models.order_builder import OrderBuilderMode, OrderBuilderResponse
from services.order_builder_service import get_order_builder_service

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/order-builder", tags=["Order Builder"])


@router.get("", response_model=OrderBuilderResponse)
def get_order_builder(
    boat_id: Optional[str] = Query(
        None,
        description="Specific boat ID. If not provided, uses next available boat."
    ),
    mode: OrderBuilderMode = Query(
        OrderBuilderMode.STANDARD,
        description="Optimization mode: minimal (3 cnt), standard (4 cnt), optimal (5 cnt)"
    ),
) -> OrderBuilderResponse:
    """
    Get Order Builder data.

    Returns everything needed for the Order Builder hero page:
    - Target boat information
    - Products grouped by priority (HIGH_PRIORITY, CONSIDER, WELL_COVERED, YOUR_CALL)
    - Pre-selected products based on mode
    - Order summary with capacity checks
    - Alerts for issues

    Mode determines container target:
    - minimal: 3 containers (42 pallets) — only HIGH_PRIORITY
    - standard: 4 containers (56 pallets) — HIGH_PRIORITY + CONSIDER
    - optimal: 5 containers (70 pallets) — fill boat with WELL_COVERED
    """
    logger.info(
        "order_builder_request",
        boat_id=boat_id,
        mode=mode.value
    )

    service = get_order_builder_service()
    return service.get_order_builder(boat_id=boat_id, mode=mode)
