"""
Factory Request Service — thin wrapper over Forward Simulation.

Consumes FS planning horizon data and identifies products that need
factory production (where warehouse/SIESA can't cover the gap).
Zero database queries of its own.
"""

import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog

from config.shipping import M2_PER_PALLET

logger = structlog.get_logger()

PALLETS_PER_CONTAINER = 13  # ~13.73 by weight, floor to be safe
URGENCY_ORDER = {"overdue": 0, "order_now": 1, "upcoming": 2}


def _parse_date(d: str) -> date:
    return datetime.fromisoformat(d).date() if "T" in d else date.fromisoformat(d)


class FactoryRequestService:
    """Aggregates FS projections into factory production requests."""

    def get_horizon(self, factory_id: str) -> dict:
        from services.forward_simulation_service import get_forward_simulation_service

        fs = get_forward_simulation_service()
        planning = fs.get_planning_horizon(factory_id, months=3)

        today = date.today()
        lead_days = planning.get("production_lead_days", 0) + planning.get("transport_to_port_days", 0)
        ready_date = today + timedelta(days=lead_days)

        # Find "ships on" boat: first boat departing after estimated_ready_date
        ships_on_boat: Optional[dict] = None
        for boat in planning.get("projections", []):
            dep = _parse_date(boat["departure_date"])
            if dep > ready_date:
                ships_on_boat = boat
                break

        # Aggregate per product across ALL boats
        product_agg: dict[str, dict] = {}

        for boat in planning.get("projections", []):
            for p in boat.get("product_details", []):
                need = p.get("suggested_pallets", 0) - p.get("shippable_pallets", 0)
                if need <= 0:
                    continue

                pid = p["product_id"]
                if pid not in product_agg:
                    product_agg[pid] = {
                        "product_id": pid,
                        "sku": p.get("sku", ""),
                        "daily_velocity_m2": Decimal(str(p.get("daily_velocity_m2", 0))),
                        "days_of_stock_at_first_gap": int(p.get("days_of_stock_at_arrival", 0)),
                        "trend_direction": p.get("trend_direction", "stable"),
                        "trend_adjustment_pct": Decimal(str(p.get("trend_adjustment_pct", 0))),
                        "first_gap_boat": boat.get("boat_name", ""),
                        "first_gap_boat_id": boat.get("boat_id", ""),
                        "first_gap_departure": boat.get("departure_date", ""),
                        "total_need_pallets": 0,
                    }
                product_agg[pid]["total_need_pallets"] += need

        # Build product list with urgency and container math
        products = []
        for agg in product_agg.values():
            total_pallets = agg["total_need_pallets"]
            total_m2 = Decimal(str(total_pallets)) * M2_PER_PALLET

            # Urgency from when the first gap appears vs lead time
            first_gap_dep = _parse_date(agg["first_gap_departure"])
            days_until_gap = (first_gap_dep - today).days
            if days_until_gap < lead_days:
                urgency = "overdue"
            elif days_until_gap < lead_days + 30:
                urgency = "order_now"
            else:
                urgency = "upcoming"

            products.append({
                "product_id": agg["product_id"],
                "sku": agg["sku"],
                "total_factory_need_pallets": total_pallets,
                "total_factory_need_m2": round(total_m2, 2),
                "first_gap_boat": agg["first_gap_boat"],
                "first_gap_boat_id": agg["first_gap_boat_id"],
                "first_gap_departure": agg["first_gap_departure"],
                "ships_on_boat": ships_on_boat.get("boat_name") if ships_on_boat else None,
                "ships_on_boat_id": ships_on_boat.get("boat_id") if ships_on_boat else None,
                "ships_on_departure": ships_on_boat.get("departure_date") if ships_on_boat else None,
                "estimated_ready_date": ready_date.isoformat(),
                "daily_velocity_m2": agg["daily_velocity_m2"],
                "days_of_stock_at_first_gap": agg["days_of_stock_at_first_gap"],
                "urgency": urgency,
                "trend_direction": agg["trend_direction"],
                "trend_adjustment_pct": agg["trend_adjustment_pct"],
            })

        # Sort by urgency, then by days_of_stock ascending (worst first)
        products.sort(key=lambda p: (
            URGENCY_ORDER.get(p["urgency"], 9),
            p["days_of_stock_at_first_gap"],
        ))

        # Summary
        total_pallets = sum(p["total_factory_need_pallets"] for p in products)
        total_m2 = sum(p["total_factory_need_m2"] for p in products)
        overdue = sum(1 for p in products if p["urgency"] == "overdue")
        order_now = sum(1 for p in products if p["urgency"] == "order_now")

        logger.info(
            "factory_request_horizon",
            factory_id=factory_id,
            products=len(products),
            total_pallets=total_pallets,
            overdue=overdue,
            order_now=order_now,
        )

        return {
            "factory_id": factory_id,
            "factory_name": planning.get("factory_name", ""),
            "production_lead_days": planning.get("production_lead_days", 0),
            "transport_to_port_days": planning.get("transport_to_port_days", 0),
            "estimated_ready_date": ready_date.isoformat(),
            "products": products,
            "factory_order_signal": planning.get("factory_order_signal"),
            "summary": {
                "total_products": len(products),
                "total_pallets": total_pallets,
                "total_m2": round(total_m2, 2),
                "total_containers": math.ceil(total_pallets / PALLETS_PER_CONTAINER) if total_pallets > 0 else 0,
                "overdue_count": overdue,
                "order_now_count": order_now,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


_factory_request_service: Optional[FactoryRequestService] = None


def get_factory_request_service() -> FactoryRequestService:
    global _factory_request_service
    if not _factory_request_service:
        _factory_request_service = FactoryRequestService()
    return _factory_request_service
