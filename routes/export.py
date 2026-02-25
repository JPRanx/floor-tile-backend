"""
Atomic export endpoint — creates factory_order + warehouse_order in one request.
If either fails, the other is cleaned up.

See STANDARDS_ERRORS.md for error response format.
"""

from datetime import date
from decimal import Decimal
from typing import Optional

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from exceptions import AppError
from models.factory_order import FactoryOrderCreate, FactoryOrderItemCreate
from models.warehouse_order import (
    WarehouseOrderCreate,
    WarehouseOrderItemCreate,
    Priority,
)
from services.factory_order_service import get_factory_order_service
from services.warehouse_order_service import get_warehouse_order_service
from services.inventory_ledger_service import get_ledger_service

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/export", tags=["Export"])


# ===================
# REQUEST / RESPONSE MODELS
# ===================


class ExportItem(BaseModel):
    """A single item in the export request."""

    product_id: str
    sku: str
    description: Optional[str] = None
    pallets: int = Field(ge=0)
    m2: Decimal = Field(ge=0)
    weight_kg: Decimal = Field(default=Decimal("0"), ge=0)
    quantity_ordered: Optional[Decimal] = Field(
        None,
        gt=0,
        description="m2 for factory order item (defaults to m2 if not set)",
    )
    score: Optional[int] = Field(None, ge=0, le=100)
    priority: Optional[str] = None
    is_critical: bool = False
    primary_customer: Optional[str] = None
    bl_number: Optional[int] = Field(None, ge=1, le=5)
    estimated_ready_date: Optional[date] = None


class ExportRequest(BaseModel):
    """
    Atomic export request.

    Creates a factory order and optionally a warehouse order in a single
    transactional call.  If `boat_id` is provided a warehouse order is
    also created; otherwise only a factory order is produced.
    """

    # Factory order fields
    order_date: date = Field(..., description="Factory order date")
    pv_number: Optional[str] = Field(None, description="PV number (auto-generated if omitted)")
    factory_notes: Optional[str] = Field(None, max_length=500)

    # Warehouse / boat fields (optional — only needed when exporting to a boat)
    boat_id: Optional[str] = None
    boat_departure_date: Optional[date] = None
    boat_arrival_date: Optional[date] = None
    estimated_warehouse_date: Optional[date] = None
    boat_name: Optional[str] = None
    exported_by: str = "Ashley"
    excel_filename: Optional[str] = None

    # Items
    items: list[ExportItem] = Field(..., min_length=1)

    # General
    notes: Optional[str] = Field(None, max_length=1000)


class ExportResponse(BaseModel):
    """Atomic export response."""

    success: bool
    pv_number: Optional[str] = None
    factory_order_id: str
    warehouse_order_id: Optional[str] = None
    message: str


# ===================
# EXCEPTION HANDLER
# ===================


def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(status_code=e.status_code, content=e.to_dict())
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
            }
        },
    )


# ===================
# ROUTE
# ===================


@router.post("/atomic", response_model=ExportResponse, status_code=201)
async def atomic_export(request: ExportRequest):
    """
    Create factory_order and warehouse_order atomically.

    1. Creates a factory order with all items.
    2. If `boat_id` is provided, creates a warehouse order for the same items.
    3. If step 2 fails, the factory order is soft-deleted (rollback).
    """
    factory_order_id: Optional[str] = None

    try:
        # ---- Step 1: Create factory order ----
        factory_svc = get_factory_order_service()

        factory_create = FactoryOrderCreate(
            pv_number=request.pv_number,
            order_date=request.order_date,
            notes=request.factory_notes or request.notes,
            items=[
                FactoryOrderItemCreate(
                    product_id=item.product_id,
                    quantity_ordered=item.quantity_ordered or item.m2,
                    estimated_ready_date=item.estimated_ready_date,
                )
                for item in request.items
            ],
        )

        factory_order = factory_svc.create(factory_create)
        factory_order_id = factory_order.id
        pv_number = factory_order.pv_number or "Unknown"

        logger.info(
            "atomic_export_factory_order_created",
            factory_order_id=factory_order_id,
            pv_number=pv_number,
        )

        # ---- Step 2: Create warehouse order (if boat_id provided) ----
        warehouse_order_id: Optional[str] = None

        if request.boat_id:
            warehouse_svc = get_warehouse_order_service()

            # Map priority string to enum safely
            def _parse_priority(val: Optional[str]) -> Optional[Priority]:
                if val is None:
                    return None
                try:
                    return Priority(val)
                except ValueError:
                    return None

            warehouse_create = WarehouseOrderCreate(
                boat_id=request.boat_id,
                boat_departure_date=request.boat_departure_date,
                boat_arrival_date=request.boat_arrival_date,
                estimated_warehouse_date=request.estimated_warehouse_date,
                boat_name=request.boat_name,
                exported_by=request.exported_by,
                excel_filename=request.excel_filename or f"PEDIDO_{pv_number}.xlsx",
                notes=request.notes or f"PV: {pv_number}",
                items=[
                    WarehouseOrderItemCreate(
                        product_id=item.product_id,
                        sku=item.sku,
                        description=item.description,
                        pallets=item.pallets,
                        m2=item.m2,
                        weight_kg=item.weight_kg,
                        score=item.score,
                        priority=_parse_priority(item.priority),
                        is_critical=item.is_critical,
                        primary_customer=item.primary_customer,
                        bl_number=item.bl_number,
                    )
                    for item in request.items
                ],
            )

            warehouse_order = warehouse_svc.create(warehouse_create)
            warehouse_order_id = warehouse_order.id

            logger.info(
                "atomic_export_warehouse_order_created",
                warehouse_order_id=warehouse_order_id,
                boat_id=request.boat_id,
            )

        logger.info(
            "atomic_export_success",
            factory_order_id=factory_order_id,
            warehouse_order_id=warehouse_order_id,
            pv_number=pv_number,
        )

        # --- Ledger: record warehouse order export (Section 1) ---
        if warehouse_order_id:
            try:
                ledger = get_ledger_service()
                for item in request.items:
                    ledger.record_warehouse_order_exported(
                        product_id=item.product_id,
                        ordered_m2=item.m2,
                        source_id=warehouse_order_id,
                    )
            except Exception as ledger_err:
                logger.warning("ledger_warehouse_export_hook_failed", error=str(ledger_err))

        return ExportResponse(
            success=True,
            pv_number=pv_number,
            factory_order_id=factory_order_id,
            warehouse_order_id=warehouse_order_id,
            message=f"Pedido #{pv_number} exportado exitosamente",
        )

    except Exception as e:
        # ---- Rollback: soft-delete factory order if it was created ----
        if factory_order_id:
            try:
                factory_svc = get_factory_order_service()
                factory_svc.delete(factory_order_id)
                logger.info(
                    "atomic_export_factory_order_rolled_back",
                    factory_order_id=factory_order_id,
                )
            except Exception as rollback_err:
                logger.error(
                    "atomic_export_rollback_failed",
                    factory_order_id=factory_order_id,
                    error=str(rollback_err),
                )

        logger.error("atomic_export_failed", error=str(e))
        return handle_error(e)
