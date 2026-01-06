"""
Order Builder API routes.

GET /api/order-builder — Get Order Builder data for the hero feature.
POST /api/order-builder/export — Export order to factory Excel format.
"""

from datetime import date
from typing import Optional, List
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import structlog

from models.order_builder import OrderBuilderMode, OrderBuilderResponse
from services.order_builder_service import get_order_builder_service
from services.export_service import get_export_service, MONTHS_ES

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/order-builder", tags=["Order Builder"])


# Request models for export
class ExportProductItem(BaseModel):
    """Single product in export request."""
    sku: str
    pallets: int


class ExportOrderRequest(BaseModel):
    """Request body for order export."""
    products: List[ExportProductItem]
    boat_departure: date


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


@router.post("/export")
def export_order(request: ExportOrderRequest) -> StreamingResponse:
    """
    Generate Excel file for factory order.

    Request body:
    {
        "products": [
            {"sku": "ALMENDRO BEIGE BTE", "pallets": 14},
            {"sku": "CEIBA GRIS CLARO BTE", "pallets": 7}
        ],
        "boat_departure": "2026-01-15"
    }

    Returns: Excel file download
    """
    logger.info(
        "export_order_request",
        product_count=len(request.products),
        boat_departure=str(request.boat_departure),
    )

    # Convert request to dict format expected by service
    products_data = [
        {"sku": p.sku, "pallets": p.pallets}
        for p in request.products
    ]

    export_service = get_export_service()
    excel_file = export_service.generate_factory_order_excel(
        products=products_data,
        boat_departure=request.boat_departure,
    )

    # Generate filename with month and year
    production_month = request.boat_departure.month + 1
    production_year = request.boat_departure.year
    if production_month > 12:
        production_month = 1
        production_year += 1

    month_name = MONTHS_ES[production_month]
    filename = f"PEDIDO_FABRICA_{month_name}_{production_year}.xlsx"

    return StreamingResponse(
        excel_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )
