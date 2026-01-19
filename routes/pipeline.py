"""
Pipeline API routes.

Provides Kanban-style overview of orders flowing through the system.
Maps factory orders and shipments to pipeline stages:
- ORDERED: Factory orders without shipments
- SHIPPED: Shipments at factory or origin port
- IN_TRANSIT: Shipments in transit
- DELIVERED: Completed shipments (last 30 days)
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
import structlog

from config import get_supabase_client
from exceptions import AppError

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["Pipeline"])


def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
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


@router.get("/overview")
async def get_pipeline_overview():
    """
    Get all orders grouped by pipeline stage.

    Stages:
    - ordered: Factory orders without linked shipments (PENDING, CONFIRMED, IN_PRODUCTION, READY)
    - shipped: Orders with shipments at AT_FACTORY or AT_ORIGIN_PORT
    - in_transit: Shipments with status IN_TRANSIT
    - delivered: Shipments with status DELIVERED (last 30 days)

    Returns:
        stages: Dict with arrays for each stage
        counts: Summary counts per stage
    """
    try:
        db = get_supabase_client()

        # Get all factory orders
        orders_result = db.table("factory_orders").select(
            "id, pv_number, order_date, status, created_at, notes"
        ).eq("active", True).order("created_at", desc=True).execute()

        # Get all active shipments with factory order info
        shipments_result = db.table("shipments").select(
            "id, factory_order_id, booking_number, vessel_name, voyage_number, etd, eta, status, actual_arrival"
        ).eq("active", True).execute()

        # Get factory order items for m2 totals
        items_result = db.table("factory_order_items").select(
            "factory_order_id, quantity_ordered, product_id"
        ).execute()

        # Get products for SKU lookup
        products_result = db.table("products").select("id, sku").execute()
        products_map = {p["id"]: p["sku"] for p in products_result.data}

        # Build items map (order_id -> {total_m2, item_count, products_preview})
        items_map = {}
        for item in items_result.data:
            order_id = item["factory_order_id"]
            qty = Decimal(str(item.get("quantity_ordered", 0)))
            sku = products_map.get(item.get("product_id"), "")

            if order_id not in items_map:
                items_map[order_id] = {
                    "total_m2": Decimal("0"),
                    "item_count": 0,
                    "skus": []
                }
            items_map[order_id]["total_m2"] += qty
            items_map[order_id]["item_count"] += 1
            if sku and len(items_map[order_id]["skus"]) < 3:
                items_map[order_id]["skus"].append(sku)

        # Build shipments map (factory_order_id -> shipment)
        shipments_map = {}
        for s in shipments_result.data:
            if s.get("factory_order_id"):
                shipments_map[s["factory_order_id"]] = s

        # Classify orders into stages
        ordered = []
        shipped = []
        in_transit = []
        delivered = []

        # Cutoff for delivered (last 30 days)
        thirty_days_ago = (date.today() - timedelta(days=30)).isoformat()

        for order in orders_result.data:
            order_id = order["id"]
            order_items = items_map.get(order_id, {"total_m2": Decimal("0"), "item_count": 0, "skus": []})
            shipment = shipments_map.get(order_id)

            base_order_data = {
                "id": order_id,
                "pv_number": order.get("pv_number"),
                "order_date": order.get("order_date"),
                "status": order.get("status"),
                "created_at": order.get("created_at"),
                "total_m2": float(order_items["total_m2"]),
                "item_count": order_items["item_count"],
                "products_preview": ", ".join(order_items["skus"]) if order_items["skus"] else None,
            }

            if not shipment:
                # No shipment linked - ORDERED stage
                if order.get("status") in ["PENDING", "CONFIRMED", "IN_PRODUCTION", "READY"]:
                    ordered.append(base_order_data)
            else:
                # Has shipment - classify by shipment status
                shipment_status = shipment.get("status")

                shipment_data = {
                    **base_order_data,
                    "factory_order_id": order_id,
                    "shipment_id": shipment.get("id"),
                    "booking_number": shipment.get("booking_number"),
                    "vessel_name": shipment.get("vessel_name"),
                    "voyage_number": shipment.get("voyage_number"),
                    "etd": shipment.get("etd"),
                    "eta": shipment.get("eta"),
                    "shipment_status": shipment_status,
                }

                if shipment_status in ["AT_FACTORY", "AT_ORIGIN_PORT"]:
                    shipped.append(shipment_data)
                elif shipment_status == "IN_TRANSIT":
                    in_transit.append(shipment_data)
                elif shipment_status in ["AT_DESTINATION_PORT", "IN_CUSTOMS", "IN_TRUCK", "DELIVERED"]:
                    # Check if recently delivered
                    arrival = shipment.get("actual_arrival") or shipment.get("eta")
                    if arrival and arrival >= thirty_days_ago:
                        shipment_data["delivered_date"] = arrival
                        delivered.append(shipment_data)

        # Sort each stage appropriately
        ordered.sort(key=lambda x: x.get("created_at") or "", reverse=True)
        shipped.sort(key=lambda x: x.get("etd") or "9999", reverse=False)
        in_transit.sort(key=lambda x: x.get("eta") or "9999", reverse=False)
        delivered.sort(key=lambda x: x.get("delivered_date") or "", reverse=True)

        logger.info(
            "pipeline_overview_retrieved",
            ordered=len(ordered),
            shipped=len(shipped),
            in_transit=len(in_transit),
            delivered=len(delivered)
        )

        return {
            "stages": {
                "ordered": ordered,
                "shipped": shipped,
                "in_transit": in_transit,
                "delivered": delivered
            },
            "counts": {
                "ordered": len(ordered),
                "shipped": len(shipped),
                "in_transit": len(in_transit),
                "delivered": len(delivered),
                "total": len(ordered) + len(shipped) + len(in_transit) + len(delivered)
            }
        }

    except Exception as e:
        return handle_error(e)
