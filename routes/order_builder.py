"""
Order Builder API routes.

GET /api/order-builder — Get Order Builder data for the hero feature.
POST /api/order-builder/confirm — Confirm order and create factory_order.
POST /api/order-builder/export — Export order to factory Excel format.
"""

from datetime import date, timedelta
from typing import Optional, List
from decimal import Decimal
from fastapi import APIRouter, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import structlog

from models.order_builder import (
    OrderBuilderMode,
    OrderBuilderResponse,
    ConfirmOrderRequest,
    ConfirmOrderResponse,
)
from models.factory_order import FactoryOrderCreate, FactoryOrderItemCreate
from services.order_builder_service import get_order_builder_service
from services.export_service import get_export_service, MONTHS_ES
from services.factory_order_service import get_factory_order_service
from exceptions import FactoryOrderPVExistsError, DatabaseError

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/order-builder", tags=["Order Builder"])

# Factory constant for pallet conversion
M2_PER_PALLET_FACTORY = Decimal("134.4")


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


@router.post("/confirm", response_model=ConfirmOrderResponse, status_code=status.HTTP_201_CREATED)
def confirm_order(request: ConfirmOrderRequest) -> ConfirmOrderResponse:
    """
    Confirm order and create factory_order record.

    This creates a persistent factory_order that can be:
    - Tracked through production (PENDING → CONFIRMED → READY → SHIPPED)
    - Linked to shipments later
    - Referenced by PV number

    Request body:
    {
        "boat_id": "uuid",
        "boat_name": "MSC Mediterranean",
        "boat_departure": "2026-02-15",
        "products": [
            {"product_id": "uuid", "sku": "ALMENDRO BEIGE BTE", "pallets": 14},
            {"product_id": "uuid", "sku": "CEIBA GRIS CLARO BTE", "pallets": 7}
        ],
        "pv_number": "PV-20260108-001",  // Optional - auto-generated if not provided
        "notes": "Urgent order for February shipment"
    }

    Returns: Created factory_order with PV number and details
    """
    logger.info(
        "confirm_order_request",
        boat_id=request.boat_id,
        product_count=len(request.products),
        pv_number=request.pv_number
    )

    factory_order_service = get_factory_order_service()

    # Auto-generate PV number if not provided
    pv_number = request.pv_number
    if not pv_number:
        today = date.today()
        daily_count = factory_order_service.count_by_date(today)
        pv_number = f"PV-{today.strftime('%Y%m%d')}-{daily_count + 1:03d}"

        logger.info(
            "pv_number_generated",
            pv_number=pv_number,
            daily_count=daily_count
        )

    # Convert Order Builder products to factory_order_items
    # Pallets → m² conversion: pallets × 134.4
    items = []
    total_pallets = 0

    for product in request.products:
        quantity_m2 = Decimal(product.pallets) * M2_PER_PALLET_FACTORY
        total_pallets += product.pallets

        # Estimated ready date: ~30 days before boat departure
        estimated_ready = request.boat_departure - timedelta(days=30)

        items.append(FactoryOrderItemCreate(
            product_id=product.product_id,
            quantity_ordered=quantity_m2,
            estimated_ready_date=estimated_ready
        ))

    # Build notes with boat reference
    notes_parts = []
    if request.notes:
        notes_parts.append(request.notes)
    notes_parts.append(f"Created from Order Builder for boat {request.boat_name} departing {request.boat_departure.strftime('%Y-%m-%d')}")
    combined_notes = " | ".join(notes_parts)

    # Create factory order
    factory_order_data = FactoryOrderCreate(
        pv_number=pv_number,
        order_date=date.today(),
        items=items,
        notes=combined_notes
    )

    try:
        factory_order = factory_order_service.create(factory_order_data)

        logger.info(
            "factory_order_confirmed",
            factory_order_id=factory_order.id,
            pv_number=factory_order.pv_number,
            items_count=len(items),
            total_m2=float(factory_order.total_m2)
        )

        return ConfirmOrderResponse(
            factory_order_id=factory_order.id,
            pv_number=factory_order.pv_number,
            status=factory_order.status.value,
            order_date=factory_order.order_date,
            items_count=factory_order.item_count,
            total_m2=factory_order.total_m2,
            total_pallets=total_pallets,
            created_at=factory_order.created_at.isoformat()
        )

    except FactoryOrderPVExistsError as e:
        logger.warning("pv_number_exists", pv_number=pv_number)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"PV number {pv_number} already exists. Please use a different PV number."
        )
    except DatabaseError as e:
        logger.error("confirm_order_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create factory order. Please try again."
        )


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
