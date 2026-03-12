"""
Factory Request Horizon Service.

Computes production request data grouped by monthly cycle for a given factory.
Independent service — does NOT call forward_simulation_service internals.
"""

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog

from config import get_supabase_client
from config.shipping import M2_PER_PALLET, ORDERING_CYCLE_DAYS

logger = structlog.get_logger(__name__)

VELOCITY_LOOKBACK_DAYS = 90
PALLETS_PER_CONTAINER = 14
MIN_CONTAINER_M2 = M2_PER_PALLET * PALLETS_PER_CONTAINER  # 1881.6
LOW_VOLUME_THRESHOLD_DAYS = 365
MIN_PRODUCTION_GAP_M2 = Decimal("1200")


def _parse_date(d: object) -> date:
    """Parse a date from string or date object."""
    if isinstance(d, date):
        return d
    return date.fromisoformat(str(d)[:10])


class FactoryRequestService:
    def __init__(self) -> None:
        self.db = get_supabase_client()

    # ------------------------------------------------------------------
    # Data fetching helpers
    # ------------------------------------------------------------------

    def _get_factory(self, factory_id: str) -> dict:
        """Fetch a single factory by ID."""
        logger.debug("fetching_factory", factory_id=factory_id)
        try:
            result = (
                self.db.table("factories")
                .select("*")
                .eq("id", factory_id)
                .execute()
            )
            if not result.data:
                raise ValueError(f"Factory not found: {factory_id}")
            return result.data[0]
        except ValueError:
            raise
        except Exception as e:
            logger.error("fetch_factory_failed", factory_id=factory_id, error=str(e))
            raise

    def _get_active_products(self, factory_id: str) -> list[dict]:
        """Fetch active products for the factory."""
        logger.debug("fetching_products", factory_id=factory_id)
        try:
            result = (
                self.db.table("products")
                .select("id, sku, description, units_per_pallet")
                .eq("factory_id", factory_id)
                .eq("active", True)
                .execute()
            )
            logger.debug("products_found", count=len(result.data))
            return result.data
        except Exception as e:
            logger.error("fetch_products_failed", factory_id=factory_id, error=str(e))
            raise

    def _get_latest_inventory(self, products: list[dict]) -> dict[str, dict]:
        """Get latest inventory per product from inventory_current view."""
        if not products:
            return {}
        logger.debug("fetching_inventory", product_count=len(products))
        try:
            result = (
                self.db.table("inventory_current")
                .select("product_id, warehouse_qty, in_transit_qty, factory_available_m2")
                .execute()
            )
            inventory_map: dict[str, dict] = {
                row["product_id"]: row for row in result.data
            }
            logger.debug("inventory_loaded", unique_products=len(inventory_map))
            return inventory_map
        except Exception as e:
            logger.error("fetch_inventory_failed", error=str(e))
            return {}

    def _get_daily_velocities(
        self, products: list[dict], today: date
    ) -> dict[str, Decimal]:
        """Compute daily sales velocity per product over the last 90 days."""
        if not products:
            return {}
        ninety_days_ago = (today - timedelta(days=VELOCITY_LOOKBACK_DAYS)).isoformat()
        logger.debug("computing_velocities", since=ninety_days_ago)
        try:
            result = (
                self.db.table("sales")
                .select("product_id, quantity_m2")
                .gte("week_start", ninety_days_ago)
                .execute()
            )
            totals: dict[str, Decimal] = defaultdict(Decimal)
            for row in result.data:
                totals[row["product_id"]] += Decimal(str(row["quantity_m2"]))

            lookback = Decimal(str(VELOCITY_LOOKBACK_DAYS))
            velocity_map: dict[str, Decimal] = {}
            for pid, total_m2 in totals.items():
                velocity_map[pid] = (total_m2 / lookback).quantize(Decimal("0.01"))

            logger.debug("velocities_computed", products_with_sales=len(velocity_map))
            return velocity_map
        except Exception as e:
            logger.error("fetch_velocities_failed", error=str(e))
            return {}

    def _get_production_pipeline(
        self, products: list[dict], horizon_end: date, today: date
    ) -> dict[str, list[dict]]:
        """Get production schedule items within the horizon."""
        if not products:
            return {}
        product_ids = [p["id"] for p in products]
        logger.debug("fetching_production_pipeline", product_count=len(product_ids))
        try:
            result = (
                self.db.table("production_schedule")
                .select("id, product_id, status, requested_m2, completed_m2, estimated_delivery_date")
                .in_("status", ["scheduled", "in_progress"])
                .not_.is_("product_id", "null")
                .not_.is_("estimated_delivery_date", "null")
                .gte("estimated_delivery_date", today.isoformat())
                .lte("estimated_delivery_date", horizon_end.isoformat())
                .execute()
            )
            pipeline: dict[str, list[dict]] = defaultdict(list)
            product_set = set(product_ids)
            for row in result.data:
                if row["product_id"] in product_set:
                    pipeline[row["product_id"]].append(row)
            logger.debug("production_pipeline_loaded", rows=sum(len(v) for v in pipeline.values()))
            return dict(pipeline)
        except Exception as e:
            logger.error("fetch_production_pipeline_failed", error=str(e))
            return {}

    def _get_in_transit_drafts(self, factory_id: str) -> dict[str, list[dict]]:
        """Get per-product in-transit quantities from ordered/confirmed drafts."""
        logger.debug("fetching_in_transit_drafts", factory_id=factory_id)
        try:
            drafts_result = (
                self.db.table("boat_factory_drafts")
                .select("id, boat_id, status")
                .eq("factory_id", factory_id)
                .in_("status", ["ordered", "confirmed"])
                .execute()
            )
            if not drafts_result.data:
                return {}

            boat_ids = [d["boat_id"] for d in drafts_result.data]
            boats_result = (
                self.db.table("boat_schedules")
                .select("id, arrival_date")
                .in_("id", boat_ids)
                .execute()
            )
            arrival_by_boat = {b["id"]: b["arrival_date"] for b in boats_result.data}

            draft_ids = [d["id"] for d in drafts_result.data]
            items_result = (
                self.db.table("draft_items")
                .select("draft_id, product_id, selected_pallets")
                .in_("draft_id", draft_ids)
                .execute()
            )

            draft_to_boat = {d["id"]: d["boat_id"] for d in drafts_result.data}

            in_transit: dict[str, list[dict]] = defaultdict(list)
            for item in items_result.data:
                boat_id = draft_to_boat.get(item["draft_id"])
                if not boat_id:
                    continue
                arrival = arrival_by_boat.get(boat_id)
                if not arrival:
                    continue
                pallets_m2 = Decimal(str(item["selected_pallets"])) * M2_PER_PALLET
                in_transit[item["product_id"]].append({
                    "arrival_date": arrival,
                    "pallets_m2": pallets_m2,
                })
            logger.debug("in_transit_drafts_loaded", products=len(in_transit))
            return dict(in_transit)
        except Exception as e:
            logger.error("fetch_in_transit_drafts_failed", error=str(e))
            return {}

    def _get_committed_to_ship(self, factory_id: str) -> dict[str, Decimal]:
        """Get total m2 committed to ordered/confirmed boats per product."""
        try:
            drafts_result = (
                self.db.table("boat_factory_drafts")
                .select("id")
                .eq("factory_id", factory_id)
                .in_("status", ["ordered", "confirmed"])
                .execute()
            )
            if not drafts_result.data:
                return {}

            draft_ids = [d["id"] for d in drafts_result.data]
            items_result = (
                self.db.table("draft_items")
                .select("product_id, selected_pallets")
                .in_("draft_id", draft_ids)
                .execute()
            )

            committed: dict[str, Decimal] = defaultdict(Decimal)
            for item in items_result.data:
                committed[item["product_id"]] += Decimal(str(item["selected_pallets"])) * M2_PER_PALLET
            return dict(committed)
        except Exception as e:
            logger.error("fetch_committed_to_ship_failed", error=str(e))
            return {}

    def _get_upcoming_boats(
        self, origin_port: str, start: date, end: date
    ) -> list[dict]:
        """Fetch boats departing from the factory's port within the horizon."""
        logger.debug(
            "fetching_upcoming_boats",
            origin_port=origin_port,
            start=start.isoformat(),
            end=end.isoformat(),
        )
        try:
            result = (
                self.db.table("boat_schedules")
                .select("*")
                .eq("origin_port", origin_port)
                .gt("departure_date", start.isoformat())
                .lt("departure_date", end.isoformat())
                .in_("status", ["available", "booked"])
                .order("departure_date")
                .execute()
            )
            logger.debug("upcoming_boats_found", count=len(result.data))
            return result.data
        except Exception as e:
            logger.error("fetch_boats_failed", error=str(e))
            return []

    # ------------------------------------------------------------------
    # Main computation
    # ------------------------------------------------------------------

    def get_horizon(self, factory_id: str) -> dict:
        """
        Compute factory request horizon grouped by monthly cycle.

        Returns dict matching FactoryRequestHorizonResponse schema.
        """
        today = date.today()
        horizon_end = today + timedelta(days=90)

        # Load all data
        factory = self._get_factory(factory_id)
        products = self._get_active_products(factory_id)
        if not products:
            return {
                "factory_id": factory_id,
                "factory_name": factory.get("name", ""),
                "cycles": [],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        inventory_map = self._get_latest_inventory(products)
        velocity_map = self._get_daily_velocities(products, today)
        production_pipeline = self._get_production_pipeline(products, horizon_end, today)
        in_transit_drafts = self._get_in_transit_drafts(factory_id)
        committed_map = self._get_committed_to_ship(factory_id)

        origin_port = factory.get("origin_port", "")
        boats = self._get_upcoming_boats(origin_port, today, horizon_end) if origin_port else []

        production_lead = int(factory.get("production_lead_days", 0))
        transport_to_port = int(factory.get("transport_to_port_days", 0))
        lead_time = production_lead + transport_to_port

        # Per-product analysis
        items_by_month: dict[str, list[dict]] = defaultdict(list)

        for product in products:
            pid = product["id"]
            daily_vel = velocity_map.get(pid, Decimal("0"))
            if daily_vel <= 0:
                continue

            inv = inventory_map.get(pid, {})
            siesa_stock = Decimal(str(inv.get("factory_available_m2", 0) or 0))

            # In production remaining
            in_production = Decimal("0")
            product_in_production = False
            earliest_delivery: Optional[date] = None
            for prow in production_pipeline.get(pid, []):
                if prow["status"] in ("scheduled", "in_progress"):
                    req = Decimal(str(prow["requested_m2"] or 0))
                    comp = Decimal(str(prow["completed_m2"] or 0))
                    in_production += max(Decimal("0"), req - comp)
                    product_in_production = True
                    del_date = prow.get("estimated_delivery_date")
                    if del_date:
                        d = _parse_date(del_date)
                        if earliest_delivery is None or d < earliest_delivery:
                            earliest_delivery = d

            committed = committed_map.get(pid, Decimal("0"))
            in_transit_bulk = Decimal(str(inv.get("in_transit_qty", 0) or 0))
            effective = max(Decimal("0"), siesa_stock + in_production + in_transit_bulk - committed)

            # Preliminary coverage and runs_out
            coverage_days_raw = int(effective / daily_vel) if daily_vel > 0 else 9999
            runs_out = today + timedelta(days=coverage_days_raw)

            # Draft-level in-transit (only count arrivals before runs_out)
            in_transit_draft_total = Decimal("0")
            if in_transit_drafts:
                for entry in in_transit_drafts.get(pid, []):
                    entry_arrival = _parse_date(entry["arrival_date"])
                    if entry_arrival <= runs_out:
                        in_transit_draft_total += entry["pallets_m2"]
            if in_transit_draft_total > 0:
                effective = max(
                    Decimal("0"),
                    siesa_stock + in_production + in_transit_bulk + in_transit_draft_total - committed,
                )
                coverage_days_raw = int(effective / daily_vel) if daily_vel > 0 else 9999
                runs_out = today + timedelta(days=coverage_days_raw)

            order_by = runs_out - timedelta(days=lead_time)

            # Gap computation
            coverage_target_days = coverage_days_raw + lead_time + ORDERING_CYCLE_DAYS
            total_need = daily_vel * coverage_target_days
            gap = max(Decimal("0"), total_need - effective)

            if gap <= MIN_PRODUCTION_GAP_M2:
                continue  # Not enough gap to warrant production

            # Container minimum enforcement
            gap_pallets = int(gap / M2_PER_PALLET)
            request_m2 = gap
            request_pallets = gap_pallets
            is_low_volume = False
            low_volume_reason: Optional[str] = None
            should_request = True

            if gap < MIN_CONTAINER_M2:
                days_to_consume = int(MIN_CONTAINER_M2 / daily_vel) if daily_vel > 0 else 9999
                if days_to_consume > LOW_VOLUME_THRESHOLD_DAYS:
                    is_low_volume = True
                    low_volume_reason = f">{LOW_VOLUME_THRESHOLD_DAYS}d to consume 1 container"
                    should_request = False
                else:
                    # Round up to 1 container
                    request_m2 = MIN_CONTAINER_M2
                    request_pallets = PALLETS_PER_CONTAINER
            else:
                # Round up to whole containers
                containers = int(gap / MIN_CONTAINER_M2) + (1 if gap % MIN_CONTAINER_M2 > 0 else 0)
                request_m2 = MIN_CONTAINER_M2 * containers
                request_pallets = PALLETS_PER_CONTAINER * containers

            # Estimated ready date
            if product_in_production and earliest_delivery:
                estimated_ready = earliest_delivery
            else:
                # Use factory lead time (NOT get_average_production_time fallback)
                estimated_ready = today + timedelta(days=lead_time)

            # Target boat for this product
            target_boat_name: Optional[str] = None
            target_boat_departure: Optional[str] = None
            for boat in boats:
                boat_dep = _parse_date(boat["departure_date"])
                if boat_dep > estimated_ready:
                    target_boat_name = boat.get("vessel_name", "")
                    target_boat_departure = boat["departure_date"]
                    break

            # Urgency based on coverage
            if coverage_days_raw < 0:
                urgency = "critical"
            elif coverage_days_raw < 15:
                urgency = "urgent"
            elif coverage_days_raw < 30:
                urgency = "soon"
            else:
                urgency = "ok"

            # Group by month
            month_key = estimated_ready.strftime("%Y-%m")

            items_by_month[month_key].append({
                "product_id": pid,
                "sku": product.get("sku", ""),
                "description": product.get("description"),
                "gap_m2": round(gap, 2),
                "gap_pallets": gap_pallets,
                "request_m2": round(request_m2, 2),
                "request_pallets": request_pallets,
                "velocity_m2_day": round(daily_vel, 2),
                "coverage_days": coverage_days_raw,
                "estimated_ready_date": estimated_ready.isoformat(),
                "target_boat": target_boat_name,
                "target_boat_departure": target_boat_departure,
                "urgency": urgency,
                "should_request": should_request,
                "is_low_volume": is_low_volume,
                "low_volume_reason": low_volume_reason,
                "order_by": order_by,  # internal, for signal computation
            })

        # Build cycles from grouped items
        from services.production_schedule_service import get_production_schedule_service
        prod_service = get_production_schedule_service()
        capacity = prod_service.get_production_capacity()
        monthly_limit = Decimal(str(factory.get("monthly_quota_m2", 60000)))
        current_month = today.strftime("%Y-%m")

        cycles = []
        for month_key in sorted(items_by_month.keys()):
            month_items = items_by_month[month_key]
            requestable = [i for i in month_items if i["should_request"]]

            total_m2 = sum(Decimal(str(i["request_m2"])) for i in requestable)
            total_pallets = sum(i["request_pallets"] for i in requestable)

            # Capacity: current month uses real data, future months assume empty
            if month_key == current_month:
                used = capacity.already_requested_m2 if hasattr(capacity, "already_requested_m2") else Decimal("0")
            else:
                used = Decimal("0")

            remaining = max(Decimal("0"), monthly_limit - used)
            util_pct = round((used / monthly_limit) * 100, 1) if monthly_limit > 0 else Decimal("0")

            # Signal type from earliest order_by product
            earliest_order_by = min((i["order_by"] for i in month_items), default=today)
            days_until = (earliest_order_by - today).days
            if days_until <= 10:
                signal_type = "order_today"
            else:
                signal_type = "on_track"

            # Target boats (unique, from requestable items)
            target_boats = list(dict.fromkeys(
                i["target_boat"] for i in requestable if i["target_boat"]
            ))

            # Month display (raw month key — frontend formats)
            month_display = month_key

            # Clean items (remove internal fields)
            clean_items: list[dict] = []
            for i in month_items:
                item = {k: v for k, v in i.items() if k != "order_by"}
                clean_items.append(item)

            # Sort: should_request first, then by urgency priority, then by gap descending
            urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}
            clean_items.sort(key=lambda x: (
                0 if x["should_request"] else 1,
                urgency_order.get(x["urgency"], 4),
                -float(x["gap_m2"]),
            ))

            cycles.append({
                "month": month_key,
                "month_display": month_display,
                "product_count": len(requestable),
                "total_m2": round(total_m2, 2),
                "total_pallets": total_pallets,
                "capacity_limit_m2": monthly_limit,
                "capacity_used_m2": round(used, 2),
                "capacity_remaining_m2": round(remaining, 2),
                "utilization_pct": util_pct,
                "deadline": earliest_order_by.isoformat() if earliest_order_by else None,
                "days_until_deadline": days_until,
                "signal_type": signal_type,
                "target_boats": target_boats,
                "items": clean_items,
            })

        return {
            "factory_id": factory_id,
            "factory_name": factory.get("name", ""),
            "cycles": cycles,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


# ------------------------------------------------------------------
# Singleton
# ------------------------------------------------------------------

_factory_request_service: Optional[FactoryRequestService] = None


def get_factory_request_service() -> FactoryRequestService:
    global _factory_request_service
    if _factory_request_service is None:
        _factory_request_service = FactoryRequestService()
    return _factory_request_service
