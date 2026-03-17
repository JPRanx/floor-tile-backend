"""
Factory Request Service — thin wrapper over Forward Simulation.

Consumes FS planning horizon data and identifies products that need
factory production (where warehouse/SIESA can't cover the gap).
Integrates with production_schedule table to separate blind spots
from in-production items.
"""

import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog

from config.shipping import M2_PER_PALLET

logger = structlog.get_logger()

PALLETS_PER_CONTAINER = 13  # ~13.73 by weight, floor to be safe
URGENCY_ORDER = {"sin_stock": 0, "critico": 1, "pedir_ahora": 2, "planificar": 3}


def _parse_date(d: str) -> date:
    return datetime.fromisoformat(d).date() if "T" in d else date.fromisoformat(d)


class FactoryRequestService:
    """Aggregates FS projections into factory production requests."""

    def _get_production_schedule(self) -> list[dict]:
        """Query production_schedule table joined with products for SKU.

        Returns list of dicts with production schedule data + product SKU.
        Returns empty list on failure (graceful degradation).
        """
        try:
            from config import get_supabase_client
            db = get_supabase_client()
            result = db.table("production_schedule").select(
                "*, products(sku)"
            ).execute()
            return result.data or []
        except Exception as e:
            logger.warning("production_schedule_fetch_failed", error=str(e))
            return []

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

            # Urgency from stock coverage vs production lead time
            # (not boat timing — that's a warehouse concern, not a factory concern)
            stock_days = agg["days_of_stock_at_first_gap"]
            if stock_days < 0:
                urgency = "sin_stock"       # Already stocked out — order immediately
            elif stock_days < lead_days:
                urgency = "critico"         # Will stock out before production arrives
            elif stock_days < lead_days + 30:
                urgency = "pedir_ahora"     # Tight — order now
            else:
                urgency = "planificar"      # Comfortable — plan ahead

            # act_by_date = first gap departure minus lead time
            # If you order by this date, production arrives before the gap
            first_gap_dep = _parse_date(agg["first_gap_departure"])
            act_by = first_gap_dep - timedelta(days=lead_days)
            act_by_str = act_by.isoformat() if act_by >= today else None  # past = overdue, show null

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
                "act_by_date": act_by_str,
            })

        # --- Production schedule integration ---
        # Partition products into in_production vs blind_spots
        schedule_rows = self._get_production_schedule()

        # Build SKU -> [schedule items] lookup
        sku_schedule_map: dict[str, list[dict]] = {}
        for row in schedule_rows:
            sku = (row.get("products") or {}).get("sku", "")
            if sku:
                sku_schedule_map.setdefault(sku, []).append(row)

        in_production_items: list[dict] = []
        blind_spots: list[dict] = []

        for p in products:
            sku = p["sku"]
            schedule_entries = sku_schedule_map.get(sku)
            if schedule_entries:
                # This product has production schedule entries — it's in production
                total_scheduled_m2 = sum(
                    Decimal(str(s.get("requested_m2", 0))) for s in schedule_entries
                )
                piggyback = p["total_factory_need_m2"] - total_scheduled_m2
                piggyback_m2 = piggyback if piggyback > 0 else None

                # Use the first schedule entry for status/date info
                first_entry = schedule_entries[0]

                in_production_items.append({
                    "product_id": p["product_id"],
                    "sku": sku,
                    "production_status": first_entry.get("status", "scheduled"),
                    "requested_m2": total_scheduled_m2,
                    "completed_m2": sum(
                        Decimal(str(s.get("completed_m2", 0))) for s in schedule_entries
                    ),
                    "scheduled_date": first_entry.get("scheduled_date"),
                    "target_boat": p.get("ships_on_boat"),
                    "target_boat_departure": p.get("ships_on_departure"),
                    "daily_velocity_m2": p.get("daily_velocity_m2"),
                    "days_of_stock_at_first_gap": p.get("days_of_stock_at_first_gap"),
                    "urgency": p.get("urgency"),
                    "piggyback_m2": piggyback_m2,
                })
            else:
                # No production schedule entry — blind spot
                blind_spots.append(p)

        # Build by_boat groups: group all products by their target boat
        boat_product_map: dict[str, dict] = {}
        # Include blind spots
        for p in blind_spots:
            boat_key = p.get("ships_on_boat") or p.get("first_gap_boat", "Unassigned")
            if boat_key not in boat_product_map:
                # Find boat info from upcoming projections
                dep = p.get("ships_on_departure") or p.get("first_gap_departure", "")
                boat_product_map[boat_key] = {
                    "boat_name": boat_key,
                    "departure_date": dep,
                    "arrival_date": "",  # Will be enriched below
                    "products": [],
                    "total_m2": Decimal("0"),
                }
            boat_product_map[boat_key]["products"].append({
                "product_id": p["product_id"],
                "sku": p["sku"],
                "need_m2": p["total_factory_need_m2"],
                "type": "blind_spot",
            })
            boat_product_map[boat_key]["total_m2"] += p["total_factory_need_m2"]

        # Include in_production items
        for ip in in_production_items:
            boat_key = ip.get("target_boat") or "Unassigned"
            if boat_key not in boat_product_map:
                dep = ip.get("target_boat_departure", "")
                boat_product_map[boat_key] = {
                    "boat_name": boat_key,
                    "departure_date": dep,
                    "arrival_date": "",
                    "products": [],
                    "total_m2": Decimal("0"),
                }
            boat_product_map[boat_key]["products"].append({
                "product_id": ip["product_id"],
                "sku": ip["sku"],
                "need_m2": ip["requested_m2"],
                "type": "in_production",
            })
            boat_product_map[boat_key]["total_m2"] += ip["requested_m2"]

        # Enrich by_boat with arrival dates from planning projections
        projection_boat_map: dict[str, str] = {}
        for boat in planning.get("projections", []):
            projection_boat_map[boat.get("boat_name", "")] = boat.get("arrival_date", "")

        by_boat: list[dict] = []
        for boat_group in boat_product_map.values():
            if not boat_group["arrival_date"] and boat_group["boat_name"] in projection_boat_map:
                boat_group["arrival_date"] = projection_boat_map[boat_group["boat_name"]]
            boat_group["total_m2"] = round(boat_group["total_m2"], 2)
            by_boat.append(boat_group)

        # Sort by_boat by departure date
        by_boat.sort(key=lambda b: b.get("departure_date", "9999"))

        # Replace products with blind_spots only (breaking change:
        # products field now only contains items NOT in production schedule)
        products = blind_spots

        # Sort by urgency, then by days_of_stock ascending (worst first)
        products.sort(key=lambda p: (
            URGENCY_ORDER.get(p["urgency"], 9),
            p["days_of_stock_at_first_gap"],
        ))

        # Summary (covers blind spots only — in_production tracked separately)
        total_pallets = sum(p["total_factory_need_pallets"] for p in products)
        total_m2 = sum(p["total_factory_need_m2"] for p in products)
        sin_stock = sum(1 for p in products if p["urgency"] == "sin_stock")
        critico = sum(1 for p in products if p["urgency"] == "critico")

        logger.info(
            "factory_request_horizon",
            factory_id=factory_id,
            products=len(products),
            total_pallets=total_pallets,
            sin_stock=sin_stock,
            critico=critico,
        )

        # Build upcoming boats with production eligibility
        upcoming_boats = [
            {
                "boat_name": b.get("boat_name", ""),
                "departure_date": b.get("departure_date", ""),
                "arrival_date": b.get("arrival_date", ""),
                "days_until_departure": b.get("days_until_departure", 0),
                "is_estimated": b.get("is_estimated", False),
                "can_receive_production": b.get("days_until_departure", 0) > lead_days,
            }
            for b in planning.get("projections", [])[:6]
        ]

        return {
            "factory_id": factory_id,
            "factory_name": planning.get("factory_name", ""),
            "production_lead_days": planning.get("production_lead_days", 0),
            "transport_to_port_days": planning.get("transport_to_port_days", 0),
            "monthly_quota_m2": planning.get("monthly_quota_m2", 0),
            "estimated_ready_date": ready_date.isoformat(),
            "products": products,
            "in_production": in_production_items,
            "by_boat": by_boat,
            "upcoming_boats": upcoming_boats,
            "factory_order_signal": planning.get("factory_order_signal"),
            "summary": {
                "total_products": len(products),
                "total_pallets": total_pallets,
                "total_m2": round(total_m2, 2),
                "total_containers": math.ceil(total_pallets / PALLETS_PER_CONTAINER) if total_pallets > 0 else 0,
                "sin_stock_count": sin_stock,
                "critico_count": critico,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


_factory_request_service: Optional[FactoryRequestService] = None


def get_factory_request_service() -> FactoryRequestService:
    global _factory_request_service
    if not _factory_request_service:
        _factory_request_service = FactoryRequestService()
    return _factory_request_service
