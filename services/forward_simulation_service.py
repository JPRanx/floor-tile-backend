"""
Forward simulation service for projecting future orders across boats.

Implements the 3-month planning horizon for Order Builder V2.
For each upcoming boat tied to a factory's origin port, projects inventory
depletion using daily velocity, identifies replenishment needs, and
estimates pallet/container counts with confidence scoring.
"""

import math
import uuid
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import structlog

from config import get_supabase_client, DatabaseError
from config.shipping import (
    M2_PER_PALLET,
    WAREHOUSE_BUFFER_DAYS,
    ORDERING_CYCLE_DAYS,
)
from models.boat_schedule import ORDER_DEADLINE_DAYS
from services.unit_config_service import get_unit_config

logger = structlog.get_logger(__name__)

# Urgency thresholds in days of stock at arrival
URGENCY_CRITICAL_DAYS = 7
URGENCY_URGENT_DAYS = 14
URGENCY_SOON_DAYS = 30

# Confidence bands by days out from today
CONFIDENCE_BANDS = [
    (14, "very_high", 90, 100),
    (30, "high", 70, 89),
    (60, "medium", 50, 69),
    (90, "low", 30, 49),
]
CONFIDENCE_DEFAULT = ("very_low", 10, 29)

VELOCITY_LOOKBACK_DAYS = 90


class ForwardSimulationService:
    """
    Project future orders for upcoming boats by factory.

    Uses daily sales velocity and current inventory to simulate
    stock depletion over a multi-boat planning horizon, producing
    pallet estimates with confidence scoring for each boat.
    """

    def __init__(self):
        self.db = get_supabase_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_planning_horizon(self, factory_id: str, months: int = 3) -> dict:
        """
        Project orders for upcoming boats for a specific factory.

        Args:
            factory_id: Factory UUID
            months: How many months to look ahead (default 3)

        Returns:
            Dict matching PlanningHorizonResponse schema
        """
        logger.info(
            "planning_horizon_start",
            factory_id=factory_id,
            months=months,
        )

        try:
            today = date.today()
            horizon_end = today + timedelta(days=months * 30)

            # Fetch all required data
            factory = self._get_factory(factory_id)
            unit_config = get_unit_config(self.db, factory_id)
            real_boats = self._get_upcoming_boats(factory["origin_port"], today, horizon_end)
            routes = self._get_shipping_routes(factory["origin_port"])
            boats = _merge_with_phantom_boats(real_boats, routes, today, horizon_end)
            products = self._get_active_products(factory_id)
            inventory_map = self._get_latest_inventory(products)
            velocity_map = self._get_daily_velocities(products, today)
            drafts_map = self._get_existing_drafts(factory_id, boats)

            # NEW: Fetch enriched supply sources
            production_pipeline = self._get_production_pipeline(products, horizon_end)
            # Exclude horizon boats — their drafts are already handled via drafts_map cascade
            horizon_boat_ids = {b["id"] for b in boats}
            in_transit_drafts = self._get_in_transit_drafts(factory_id, horizon_boat_ids)

            # Build running stock tracker — warehouse only (no lump-sum in_transit)
            current_stock: dict[str, Decimal] = {}
            factory_siesa_map: dict[str, Decimal] = {}
            for p in products:
                pid = p["id"]
                inv = inventory_map.get(pid, {})
                warehouse = Decimal(str(inv.get("warehouse_qty", 0)))
                current_stock[pid] = warehouse
                # Factory SIESA finished goods
                siesa = inv.get("factory_available_m2") or 0
                if Decimal(str(siesa)) > 0:
                    factory_siesa_map[pid] = Decimal(str(siesa))

            # Track one-time supply consumption across boats
            factory_siesa_consumed: set[str] = set()
            production_consumed: set[str] = set()

            # Simulate each boat in departure order
            boat_projections: list[dict] = []
            for boat in boats:
                projection = self._simulate_boat(
                    boat=boat,
                    factory=factory,
                    products=products,
                    current_stock=current_stock,
                    velocity_map=velocity_map,
                    drafts_map=drafts_map,
                    today=today,
                    factory_siesa_map=factory_siesa_map,
                    factory_siesa_consumed=factory_siesa_consumed,
                    production_pipeline=production_pipeline,
                    production_consumed=production_consumed,
                    in_transit_drafts=in_transit_drafts,
                    unit_config=unit_config,
                )
                boat_projections.append(projection)

            # Post-processing: compute draft lock, review flags, and dependency context
            for i, proj in enumerate(boat_projections):
                # Draft lock: locked if any LATER boat has a draft
                if proj["draft_id"] is not None:
                    for j in range(i + 1, len(boat_projections)):
                        later = boat_projections[j]
                        if later["draft_id"] is not None:
                            proj["is_draft_locked"] = True
                            proj["blocking_boat_name"] = later["boat_name"]
                            break

                # Earlier draft dependency: check if any EARLIER boat has a draft
                earlier_draft_names: list[str] = []
                earlier_total_pallets = 0
                for j in range(0, i):
                    earlier = boat_projections[j]
                    if earlier["draft_id"] is not None:
                        earlier_pallets = sum(
                            d.get("suggested_pallets", 0) for d in earlier["product_details"]
                        )
                        earlier_draft_names.append(earlier["boat_name"])
                        earlier_total_pallets += earlier_pallets

                if earlier_draft_names:
                    proj["has_earlier_drafts"] = True
                    if len(earlier_draft_names) == 1:
                        proj["earlier_draft_context"] = (
                            f"based_on_single:{earlier_draft_names[0]}:{earlier_total_pallets}"
                        )
                    else:
                        proj["earlier_draft_context"] = (
                            f"based_on_multiple:{len(earlier_draft_names)}:{earlier_total_pallets}"
                        )

                # Review flags: needs_review if status is action_needed
                if proj["draft_status"] == "action_needed":
                    proj["needs_review"] = True
                    # Use the draft's notes as the review reason (set by cascade)
                    draft = drafts_map.get(proj["boat_id"])
                    if draft and draft.get("notes"):
                        proj["review_reason"] = draft["notes"]
                    else:
                        proj["review_reason"] = "draft_needs_review"

            # Stability impact: classify products as stabilized, recovering, or blocked
            STABILITY_THRESHOLD = URGENCY_SOON_DAYS  # 30 days
            for i, proj in enumerate(boat_projections):
                details = proj["product_details"]
                total = len(details)
                if total == 0:
                    proj["stability_impact"] = {
                        "stabilizes_count": 0, "stabilizes_products": [],
                        "recovering_count": 0, "recovering_products": [],
                        "blocked_count": 0, "blocked_products": [],
                        "progress_before_pct": 100, "progress_after_pct": 100,
                    }
                    continue

                stabilizes: list[str] = []
                recovering: list[str] = []
                blocked: list[str] = []
                stable_before = 0
                stable_after = 0

                # Pre-compute: which product IDs have supply on any later boat
                later_supply_pids: set[str] = set()
                for j in range(i + 1, len(boat_projections)):
                    for pd in boat_projections[j]["product_details"]:
                        if pd["suggested_pallets"] > 0:
                            later_supply_pids.add(pd["product_id"])

                for pd in details:
                    before = pd["days_of_stock_at_arrival"]
                    after = pd.get("days_of_stock_after_fill", before)

                    if before >= STABILITY_THRESHOLD:
                        stable_before += 1
                        stable_after += 1
                    else:
                        # Unstable before this boat
                        if after >= STABILITY_THRESHOLD:
                            # This boat stabilizes this product
                            stabilizes.append(pd["sku"])
                            stable_after += 1
                        else:
                            # Still unstable after this boat
                            if pd["product_id"] in later_supply_pids:
                                recovering.append(pd["sku"])
                            else:
                                blocked.append(pd["sku"])

                proj["stability_impact"] = {
                    "stabilizes_count": len(stabilizes),
                    "stabilizes_products": stabilizes,
                    "recovering_count": len(recovering),
                    "recovering_products": recovering,
                    "blocked_count": len(blocked),
                    "blocked_products": blocked,
                    "progress_before_pct": round(stable_before / total * 100) if total > 0 else 100,
                    "progress_after_pct": round(stable_after / total * 100) if total > 0 else 100,
                }

            # Factory order signal
            factory_order_signal = self._compute_factory_order_signal(
                factory=factory,
                products=products,
                inventory_map=inventory_map,
                velocity_map=velocity_map,
                production_pipeline=production_pipeline,
                factory_id=factory_id,
                today=today,
                boats=boats,
                in_transit_drafts=in_transit_drafts,
            )

            result = {
                "factory_id": factory_id,
                "factory_name": factory.get("name", ""),
                "horizon_months": months,
                "generated_at": today.isoformat(),
                "projections": boat_projections,
                "factory_order_signal": factory_order_signal,
            }

            logger.info(
                "planning_horizon_complete",
                factory_id=factory_id,
                boats=len(boat_projections),
            )
            return result

        except DatabaseError:
            raise
        except Exception as e:
            logger.error(
                "planning_horizon_failed",
                factory_id=factory_id,
                error=str(e),
            )
            raise DatabaseError("select", str(e))

    def get_projection_for_boat(
        self, factory_id: str, boat_id: str
    ) -> Optional[dict[str, dict]]:
        """
        Run forward simulation and extract per-product projections for a specific boat.

        Returns a dict keyed by product_id with projected stock, supply breakdown,
        and earlier-draft consumption data. Returns None if the boat is not found
        in the simulation horizon or if the simulation fails.
        """
        try:
            horizon = self.get_planning_horizon(factory_id)
            projections = horizon.get("projections", [])
            if not projections:
                return None

            # Find the target boat
            target_proj: Optional[dict] = None
            for proj in projections:
                if proj["boat_id"] == boat_id:
                    target_proj = proj
                    break

            if target_proj is None:
                return None

            # Build first boat's warehouse values (pristine stock before any cascade)
            first_warehouse_by_pid: dict[str, Decimal] = {}
            for pd in projections[0]["product_details"]:
                first_warehouse_by_pid[pd["product_id"]] = Decimal(
                    str(pd["supply_breakdown"]["warehouse_m2"])
                )

            # Build result dict keyed by product_id
            result: dict[str, dict] = {}
            for pd in target_proj["product_details"]:
                pid = pd["product_id"]
                original_warehouse = first_warehouse_by_pid.get(pid, Decimal("0"))
                current_warehouse = Decimal(str(pd["supply_breakdown"]["warehouse_m2"]))
                earlier_consumed = max(Decimal("0"), original_warehouse - current_warehouse)

                result[pid] = {
                    "projected_stock_m2": Decimal(str(pd["projected_stock_m2"])),
                    "daily_velocity_m2": Decimal(str(pd["daily_velocity_m2"])),
                    "supply_breakdown": {
                        "warehouse_m2": current_warehouse,
                        "factory_siesa_m2": Decimal(str(pd["supply_breakdown"]["factory_siesa_m2"])),
                        "production_pipeline_m2": Decimal(str(pd["supply_breakdown"]["production_pipeline_m2"])),
                        "in_transit_m2": Decimal(str(pd["supply_breakdown"]["in_transit_m2"])),
                    },
                    "earlier_drafts_consumed_m2": earlier_consumed,
                    "coverage_gap_m2": Decimal(str(pd["coverage_gap_m2"])),
                    "days_of_stock_at_arrival": pd["days_of_stock_at_arrival"],
                }

            return result

        except Exception as e:
            logger.warning(
                "get_projection_for_boat_failed",
                factory_id=factory_id,
                boat_id=boat_id,
                error=str(e),
            )
            return None

    # ------------------------------------------------------------------
    # Boat simulation
    # ------------------------------------------------------------------

    def _simulate_boat(
        self,
        boat: dict,
        factory: dict,
        products: list[dict],
        current_stock: dict[str, Decimal],
        velocity_map: dict[str, Decimal],
        drafts_map: dict[str, dict],
        today: date,
        factory_siesa_map: dict[str, Decimal],
        factory_siesa_consumed: set[str],
        production_pipeline: dict[str, list[dict]],
        production_consumed: set[str],
        in_transit_drafts: dict[str, list[dict]],
        unit_config: Optional[dict] = None,
    ) -> dict:
        """
        Simulate a single boat: project stock at arrival, compute needs.

        Mutates *current_stock*, *factory_siesa_consumed*, *production_consumed*,
        and *in_transit_drafts* in place so subsequent boats see consumed supply.

        Args:
            unit_config: Factory unit configuration from get_unit_config().
                When the factory is unit-based, pallet conversions use
                per-product units_per_pallet instead of M2_PER_PALLET.
        """
        boat_id = boat["id"]
        arrival_date = _parse_date(boat["arrival_date"])
        departure_date = _parse_date(boat["departure_date"])

        days_until_arrival = (arrival_date - today).days + WAREHOUSE_BUFFER_DAYS
        if days_until_arrival < 1:
            days_until_arrival = 1

        coverage_target = ORDERING_CYCLE_DAYS + days_until_arrival
        transport_to_port = int(factory.get("transport_to_port_days", 0))

        # Existing draft for this boat
        draft = drafts_map.get(boat_id)

        # Per-product projections
        product_details: list[dict] = []
        total_pallets = 0
        urgency_counts = {"critical": 0, "urgent": 0, "soon": 0, "ok": 0}

        # Determine if unit-based factory
        is_unit_based = unit_config is not None and not unit_config.get("is_m2_based", True)

        for product in products:
            pid = product["id"]
            daily_vel = velocity_map.get(pid, Decimal("0"))
            stock = current_stock.get(pid, Decimal("0"))

            # Per-product pallet divisor: units_per_pallet for furniture, M2_PER_PALLET for tiles
            product_units_per_pallet = product.get("units_per_pallet")
            if is_unit_based and product_units_per_pallet and product_units_per_pallet > 0:
                pallet_divisor = Decimal(str(product_units_per_pallet))
            else:
                pallet_divisor = M2_PER_PALLET

            # --- SUPPLY EVENTS ---
            siesa_supply = Decimal("0")
            prod_supply = Decimal("0")
            transit_supply = Decimal("0")

            # A. Factory SIESA (one-time, first eligible boat)
            if pid not in factory_siesa_consumed:
                siesa_m2 = factory_siesa_map.get(pid, Decimal("0"))
                if siesa_m2 > 0:
                    factory_ready_by = today + timedelta(days=transport_to_port)
                    if departure_date >= factory_ready_by:
                        siesa_supply = siesa_m2
                        factory_siesa_consumed.add(pid)

            # B. Production pipeline (one-time per row)
            for prow in production_pipeline.get(pid, []):
                if prow["id"] in production_consumed:
                    continue
                est_delivery = _parse_date(prow["estimated_delivery_date"])
                prod_ready_by = est_delivery + timedelta(days=transport_to_port)
                if prod_ready_by <= departure_date:
                    if prow["status"] == "completed":
                        contrib = Decimal(str(prow["completed_m2"] or 0))
                    else:
                        req = Decimal(str(prow["requested_m2"] or 0))
                        comp = Decimal(str(prow["completed_m2"] or 0))
                        contrib = max(Decimal("0"), req - comp)
                    if contrib > 0:
                        prod_supply += contrib
                        production_consumed.add(prow["id"])

            # C. In-transit from ordered/confirmed drafts
            # Available when earlier boat's goods are in warehouse before
            # this boat departs (per spec: entry_arrival + BUFFER <= departure)
            remaining_transit = []
            for entry in in_transit_drafts.get(pid, []):
                entry_arrival = _parse_date(entry["arrival_date"])
                entry_warehouse = entry_arrival + timedelta(days=WAREHOUSE_BUFFER_DAYS)
                if entry_warehouse <= departure_date:
                    transit_supply += entry["pallets_m2"]
                else:
                    remaining_transit.append(entry)
            # Remove consumed entries so later boats don't double-count
            if remaining_transit != in_transit_drafts.get(pid, []):
                in_transit_drafts[pid] = remaining_transit

            effective_stock = stock + siesa_supply + prod_supply + transit_supply
            projected_stock = effective_stock - (daily_vel * days_until_arrival)

            # --- DEMAND: use draft or compute suggestion ---
            # Use draft selections for cascade so Ashley can plan multiple
            # boats at once.  Committed drafts (ordered/confirmed) are
            # authoritative; tentative drafts (drafting/action_needed)
            # cascade their user-selected quantities so later boats see
            # reduced inventory, but are marked non-committed for display.
            draft_status = draft.get("status", "") if draft else ""
            is_committed_draft = draft_status in ("ordered", "confirmed")
            has_draft_items = bool(draft and draft.get("items"))

            is_committed = False
            if has_draft_items and is_committed_draft:
                # Committed draft — authoritative, locked quantities
                draft_item = next((i for i in draft["items"] if i["product_id"] == pid), None)
                if draft_item:
                    suggested_pallets = draft_item["selected_pallets"]
                    is_committed = True
                else:
                    suggested_pallets = 0
                    is_committed = True
            elif has_draft_items:
                # Tentative draft — use selections for cascade so later
                # boats see updated inventory, but don't mark as committed
                draft_item = next((i for i in draft["items"] if i["product_id"] == pid), None)
                if draft_item and draft_item["selected_pallets"] > 0:
                    suggested_pallets = draft_item["selected_pallets"]
                else:
                    # Product not selected in tentative draft — compute fresh
                    coverage_gap_val = max(
                        Decimal("0"),
                        (daily_vel * coverage_target) - projected_stock,
                    )
                    suggested_pallets = math.ceil(coverage_gap_val / pallet_divisor) if coverage_gap_val > 0 else 0
            else:
                # No draft — compute suggestion normally
                coverage_gap_val = max(
                    Decimal("0"),
                    (daily_vel * coverage_target) - projected_stock,
                )
                suggested_pallets = math.ceil(coverage_gap_val / pallet_divisor) if coverage_gap_val > 0 else 0

            total_pallets += suggested_pallets

            # Urgency based on days of stock at arrival
            if daily_vel > 0:
                days_of_stock = float(projected_stock / daily_vel)
            else:
                days_of_stock = 999.0

            urgency = _classify_urgency(days_of_stock)
            urgency_counts[urgency] += 1

            # Days of stock AFTER this boat fills the order
            if suggested_pallets > 0 and daily_vel > 0:
                filled_m2 = Decimal(suggested_pallets) * pallet_divisor
                days_after_fill = float((projected_stock + filled_m2) / daily_vel)
            else:
                days_after_fill = days_of_stock

            # Coverage gap (for display — always computed even if draft committed)
            coverage_gap = max(
                Decimal("0"),
                (daily_vel * coverage_target) - projected_stock,
            )

            product_details.append({
                "product_id": pid,
                "sku": product.get("sku", ""),
                "daily_velocity_m2": float(daily_vel.quantize(Decimal("0.01"))),
                "current_stock_m2": float(effective_stock.quantize(Decimal("0.01"))),
                "projected_stock_m2": float(projected_stock.quantize(Decimal("0.01"))),
                "days_of_stock_at_arrival": round(days_of_stock, 1),
                "days_of_stock_after_fill": round(days_after_fill, 1),
                "urgency": urgency,
                "coverage_gap_m2": float(coverage_gap.quantize(Decimal("0.01"))),
                "suggested_pallets": suggested_pallets,
                "supply_breakdown": {
                    "warehouse_m2": float(stock.quantize(Decimal("0.01"))),
                    "factory_siesa_m2": float(siesa_supply.quantize(Decimal("0.01"))),
                    "production_pipeline_m2": float(prod_supply.quantize(Decimal("0.01"))),
                    "in_transit_m2": float(transit_supply.quantize(Decimal("0.01"))),
                },
                "is_draft_committed": is_committed,
            })

            # Cascade: update running stock for next boat
            if suggested_pallets > 0:
                filled_qty = Decimal(suggested_pallets) * pallet_divisor
                current_stock[pid] = projected_stock + filled_qty
            else:
                current_stock[pid] = projected_stock

        # Confidence
        days_out = (departure_date - today).days
        confidence_label, confidence_score = _compute_confidence(days_out)

        # Pallet range (uncertainty band)
        estimated_pallets_min = int(total_pallets * (confidence_score / 100))
        estimated_pallets_max = int(total_pallets * (2 - confidence_score / 100))

        # Deadlines
        production_lead = int(factory.get("production_lead_days", 0))
        # Factory order deadline: latest date to tell factory to start producing
        factory_order_by = departure_date - timedelta(days=production_lead + transport_to_port)
        # Shipping booking deadline: latest date to book container space
        shipping_book_by = departure_date - timedelta(days=transport_to_port)

        # SIESA order deadline: finalize what to pick from SIESA warehouse (departure - 20d)
        # Unit-based factories (e.g. Muebles) have no SIESA step — skip deadline
        siesa_order_by = None if is_unit_based else departure_date - timedelta(days=ORDER_DEADLINE_DAYS)

        # Sort product details by urgency (critical first)
        urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}
        product_details.sort(key=lambda d: urgency_order.get(d["urgency"], 99))

        # Build BL items from draft if available
        draft_bl_items: list[dict] = []
        has_bl_allocation = False
        if draft and draft.get("items"):
            products_by_id = {p["id"]: p for p in products}
            bl_items = [i for i in draft["items"] if i.get("bl_number") is not None]
            if bl_items:
                has_bl_allocation = True
                for item in bl_items:
                    prod = products_by_id.get(item["product_id"], {})
                    draft_bl_items.append({
                        "product_id": item["product_id"],
                        "sku": prod.get("sku", ""),
                        "selected_pallets": item["selected_pallets"],
                        "bl_number": item["bl_number"],
                    })
                draft_bl_items.sort(key=lambda x: (x["bl_number"], x["sku"]))

        is_estimated = boat.get("_is_estimated", False)
        route_carrier = boat.get("_route_carrier")

        return {
            "boat_id": boat_id,
            "boat_name": boat.get("vessel_name", ""),
            "departure_date": boat["departure_date"],
            "arrival_date": boat["arrival_date"],
            "days_until_departure": (departure_date - today).days,
            "origin_port": factory["origin_port"],
            "confidence": confidence_label,
            "projected_pallets_min": estimated_pallets_min,
            "projected_pallets_max": estimated_pallets_max,
            "urgency_breakdown": urgency_counts,
            "draft_status": draft.get("status") if draft else None,
            "draft_id": draft["id"] if draft else None,
            "is_active": draft is not None,
            "order_by_date": factory_order_by.isoformat(),
            "days_until_order_deadline": (factory_order_by - today).days,
            "shipping_book_by_date": shipping_book_by.isoformat(),
            "days_until_shipping_deadline": (shipping_book_by - today).days,
            "siesa_order_date": siesa_order_by.isoformat() if siesa_order_by else None,
            "days_until_siesa_deadline": (siesa_order_by - today).days if siesa_order_by else None,
            "production_request_date": None,
            "days_until_production_deadline": None,
            "product_details": product_details,
            "draft_bl_items": draft_bl_items,
            "has_bl_allocation": has_bl_allocation,
            "is_estimated": is_estimated,
            "carrier": route_carrier or boat.get("shipping_line"),
            "is_draft_locked": False,
            "blocking_boat_name": None,
            "has_earlier_drafts": False,
            "needs_review": False,
            "review_reason": None,
            "earlier_draft_context": None,
            "has_factory_siesa_supply": any(
                d.get("supply_breakdown", {}).get("factory_siesa_m2", 0) > 0
                for d in product_details
            ),
            "has_production_supply": any(
                d.get("supply_breakdown", {}).get("production_pipeline_m2", 0) > 0
                for d in product_details
            ),
            "factory_siesa_total_m2": float(sum(
                Decimal(str(d.get("supply_breakdown", {}).get("factory_siesa_m2", 0)))
                for d in product_details
            )),
            "production_total_m2": float(sum(
                Decimal(str(d.get("supply_breakdown", {}).get("production_pipeline_m2", 0)))
                for d in product_details
            )),
            "has_in_transit_supply": any(
                d.get("supply_breakdown", {}).get("in_transit_m2", 0) > 0
                for d in product_details
            ),
            "in_transit_total_m2": float(sum(
                Decimal(str(d.get("supply_breakdown", {}).get("in_transit_m2", 0)))
                for d in product_details
            )),
        }

    # ------------------------------------------------------------------
    # Factory order signal
    # ------------------------------------------------------------------

    def _get_committed_to_ship(self, factory_id: str) -> dict[str, Decimal]:
        """
        Get total m² committed to ordered/confirmed boats per product.

        These are goods already "spoken for" — picked from SIESA for specific boats.
        """
        try:
            # Get all ordered/confirmed drafts for this factory
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

            # Get all items for those drafts
            items_result = (
                self.db.table("draft_items")
                .select("product_id, selected_pallets")
                .in_("draft_id", draft_ids)
                .execute()
            )

            # Sum m² per product
            committed: dict[str, Decimal] = defaultdict(Decimal)
            for item in items_result.data:
                committed[item["product_id"]] += Decimal(str(item["selected_pallets"])) * M2_PER_PALLET

            return dict(committed)
        except Exception as e:
            logger.error("fetch_committed_to_ship_failed", error=str(e))
            return {}

    def _compute_factory_order_signal(
        self,
        factory: dict,
        products: list[dict],
        inventory_map: dict[str, dict],
        velocity_map: dict[str, Decimal],
        production_pipeline: dict[str, list[dict]],
        factory_id: str,
        today: date,
        boats: list[dict] | None = None,
        in_transit_drafts: dict[str, list[dict]] | None = None,
    ) -> Optional[dict]:
        """
        Compute when the next factory production order needs to be placed.

        Formula per product:
            in_production_remaining = SUM(requested_m2 - completed_m2)
                                      WHERE status IN ('scheduled', 'in_progress')
            in_transit = bulk in_transit_qty + draft-level in-transit arriving before runs_out
            effective_siesa = siesa_stock + in_production_remaining + in_transit - committed_to_ship
            siesa_coverage_days = effective_siesa / daily_velocity
            siesa_runs_out = today + siesa_coverage_days
            lead_time = production_lead + transport_to_port
            next_factory_order = siesa_runs_out - lead_time

        Products with gap <= 1200 m² are excluded from needing production
        (minimum worthwhile production run ~9 pallets).

        Return the earliest date across all products.
        """
        committed_map = self._get_committed_to_ship(factory_id)

        production_lead = int(factory.get("production_lead_days", 0))
        transport_to_port = int(factory.get("transport_to_port_days", 0))
        lead_time = production_lead + transport_to_port

        # Minimum production threshold: ~9 pallets worth of m²
        MIN_PRODUCTION_GAP_M2 = Decimal("1200")

        earliest_order_date: Optional[date] = None
        limiting_sku: Optional[str] = None
        limiting_pid: Optional[str] = None
        min_coverage_days: Optional[int] = None

        # Track products that actually need a production run
        products_needing_production: list[dict] = []

        for product in products:
            pid = product["id"]
            daily_vel = velocity_map.get(pid, Decimal("0"))
            if daily_vel <= 0:
                continue  # No velocity — can't compute coverage

            # SIESA stock
            inv = inventory_map.get(pid, {})
            siesa_stock = Decimal(str(inv.get("factory_available_m2", 0) or 0))

            # In production remaining
            in_production = Decimal("0")
            for prow in production_pipeline.get(pid, []):
                if prow["status"] in ("scheduled", "in_progress"):
                    req = Decimal(str(prow["requested_m2"] or 0))
                    comp = Decimal(str(prow["completed_m2"] or 0))
                    remaining = max(Decimal("0"), req - comp)
                    in_production += remaining

            # Committed to ship (ordered/confirmed drafts)
            committed = committed_map.get(pid, Decimal("0"))

            # In-transit: bulk from inventory_current view
            in_transit_bulk = Decimal(str(inv.get("in_transit_qty", 0) or 0))

            # Effective supply (before draft-level transit — need runs_out first)
            effective = siesa_stock + in_production + in_transit_bulk - committed
            if effective < 0:
                effective = Decimal("0")

            # Preliminary coverage days and runs_out (used as cutoff for draft transit)
            coverage_days = int(effective / daily_vel)
            runs_out = today + timedelta(days=coverage_days)

            # In-transit: per-product arrival-date-aware from ordered/confirmed drafts
            # Only count entries arriving before SIESA runs out
            in_transit_draft_total = Decimal("0")
            if in_transit_drafts:
                for entry in in_transit_drafts.get(pid, []):
                    entry_arrival = _parse_date(entry["arrival_date"])
                    if entry_arrival <= runs_out:
                        in_transit_draft_total += entry["pallets_m2"]

            # Recompute effective supply with draft transit included
            if in_transit_draft_total > 0:
                effective = siesa_stock + in_production + in_transit_bulk + in_transit_draft_total - committed
                if effective < 0:
                    effective = Decimal("0")
                coverage_days = int(effective / daily_vel)
                runs_out = today + timedelta(days=coverage_days)

            # When to order: runs_out - lead_time
            order_by = runs_out - timedelta(days=lead_time)

            # Compute gap for production threshold: how much would we need to produce?
            # gap = demand over coverage horizon minus available supply
            coverage_target_days = coverage_days + lead_time + ORDERING_CYCLE_DAYS
            total_need = daily_vel * coverage_target_days
            gap = max(Decimal("0"), total_need - effective)

            if gap > MIN_PRODUCTION_GAP_M2:
                products_needing_production.append({
                    "product_id": pid,
                    "sku": product.get("sku", ""),
                    "gap_m2": gap,
                })

            if earliest_order_date is None or order_by < earliest_order_date:
                earliest_order_date = order_by
                limiting_sku = product.get("sku", "")
                limiting_pid = pid
                min_coverage_days = coverage_days

        if earliest_order_date is None:
            return None

        days_until = (earliest_order_date - today).days

        # Target boat: first boat departing after production lead time would complete
        target_boat_name: Optional[str] = None
        target_boat_departure: Optional[str] = None
        if boats:
            production_ready_by = today + timedelta(days=production_lead + transport_to_port)
            for boat in boats:
                boat_dep = _parse_date(boat["departure_date"])
                if boat_dep > production_ready_by:
                    target_boat_name = boat.get("vessel_name", "")
                    target_boat_departure = boat["departure_date"]
                    break

        # Quantity summary from products needing production
        product_count = len(products_needing_production)
        estimated_pallets: Optional[int] = None
        if product_count > 0:
            total_gap_m2 = sum(p["gap_m2"] for p in products_needing_production)
            estimated_pallets = int(total_gap_m2 / M2_PER_PALLET)

        # Production-aware signal classification
        is_overdue = days_until < 0
        signal_type = "on_track"
        limiting_production_delivery_str: Optional[str] = None
        can_make_boat = True

        if is_overdue and limiting_pid:
            # Check if limiting product has active production rows
            limiting_prod_rows = [
                prow for prow in production_pipeline.get(limiting_pid, [])
                if prow["status"] in ("scheduled", "in_progress")
                and prow.get("estimated_delivery_date")
            ]

            if limiting_prod_rows:
                earliest_delivery = min(
                    _parse_date(prow["estimated_delivery_date"])
                    for prow in limiting_prod_rows
                )
                limiting_production_delivery_str = earliest_delivery.isoformat()
                ready_at_port = earliest_delivery + timedelta(days=transport_to_port)

                if target_boat_departure:
                    target_dep = _parse_date(target_boat_departure)
                    if ready_at_port <= target_dep:
                        signal_type = "in_production"
                        can_make_boat = True
                    else:
                        signal_type = "production_delayed"
                        can_make_boat = False
                else:
                    signal_type = "in_production"
            else:
                # No production for limiting product
                if target_boat_name:
                    signal_type = "order_today"
                    can_make_boat = True
                else:
                    signal_type = "no_production"
                    can_make_boat = False

        return {
            "next_order_date": earliest_order_date.isoformat(),
            "days_until_order": days_until,
            "is_overdue": is_overdue,
            "limiting_product_sku": limiting_sku,
            "effective_coverage_days": min_coverage_days,
            "target_boat_name": target_boat_name,
            "target_boat_departure": target_boat_departure,
            "estimated_pallets": estimated_pallets,
            "product_count": product_count if product_count > 0 else None,
            "signal_type": signal_type,
            "limiting_production_delivery": limiting_production_delivery_str,
            "can_make_target_boat": can_make_boat,
        }

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
            raise DatabaseError("select", str(e))

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
            raise DatabaseError("select", str(e))

    def _get_active_products(self, factory_id: str) -> list[dict]:
        """Fetch active products for the factory."""
        logger.debug("fetching_products", factory_id=factory_id)
        try:
            result = (
                self.db.table("products")
                .select("id, sku, units_per_pallet")
                .eq("factory_id", factory_id)
                .eq("active", True)
                .execute()
            )
            logger.debug("products_found", count=len(result.data))
            return result.data
        except Exception as e:
            logger.error("fetch_products_failed", factory_id=factory_id, error=str(e))
            raise DatabaseError("select", str(e))

    def _get_latest_inventory(self, products: list[dict]) -> dict[str, dict]:
        """
        Get latest inventory per product from inventory_current view.

        The view composes the latest value from each independent source
        (warehouse_snapshots, factory_snapshots, transit_snapshots).

        Returns:
            Mapping of product_id -> {warehouse_qty, in_transit_qty, factory_available_m2}
        """
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
            raise DatabaseError("select", str(e))

    def _get_production_pipeline(self, products: list[dict], horizon_end: date) -> dict[str, list[dict]]:
        """
        Get production schedule items that could contribute supply within the horizon.

        Returns:
            Mapping of product_id -> list of production rows
        """
        if not products:
            return {}

        product_ids = [p["id"] for p in products]
        logger.debug("fetching_production_pipeline", product_count=len(product_ids))

        try:
            result = (
                self.db.table("production_schedule")
                .select("id, product_id, status, requested_m2, completed_m2, estimated_delivery_date")
                .in_("status", ["scheduled", "in_progress", "completed"])
                .not_.is_("product_id", "null")
                .not_.is_("estimated_delivery_date", "null")
                .lte("estimated_delivery_date", horizon_end.isoformat())
                .execute()
            )

            # Filter to our products and group by product_id
            pipeline: dict[str, list[dict]] = defaultdict(list)
            product_set = set(product_ids)
            for row in result.data:
                if row["product_id"] in product_set:
                    pipeline[row["product_id"]].append(row)

            logger.debug("production_pipeline_loaded", rows=sum(len(v) for v in pipeline.values()))
            return dict(pipeline)

        except Exception as e:
            logger.error("fetch_production_pipeline_failed", error=str(e))
            return {}  # Non-fatal: fall back to no production data

    def _get_in_transit_drafts(
        self, factory_id: str, exclude_boat_ids: Optional[set[str]] = None
    ) -> dict[str, list[dict]]:
        """
        Get per-product in-transit quantities from ordered/confirmed drafts.

        These are goods on the water whose per-boat breakdown we know
        because the draft tells us what was ordered on each boat.

        Args:
            factory_id: Factory UUID
            exclude_boat_ids: Boat IDs to exclude (horizon boats whose drafts
                are already handled via the running stock cascade in _simulate_boat)

        Returns:
            Mapping of product_id -> list of {arrival_date, pallets_m2}
        """
        logger.debug("fetching_in_transit_drafts", factory_id=factory_id)

        try:
            # Get all ordered/confirmed drafts for this factory
            drafts_result = (
                self.db.table("boat_factory_drafts")
                .select("id, boat_id, status")
                .eq("factory_id", factory_id)
                .in_("status", ["ordered", "confirmed"])
                .execute()
            )

            if not drafts_result.data:
                return {}

            # Filter out horizon boats whose drafts are handled via running stock cascade
            _exclude = exclude_boat_ids or set()
            filtered_drafts = [d for d in drafts_result.data if d["boat_id"] not in _exclude]
            if not filtered_drafts:
                return {}

            # Get boat arrival dates
            boat_ids = [d["boat_id"] for d in filtered_drafts]
            boats_result = (
                self.db.table("boat_schedules")
                .select("id, arrival_date")
                .in_("id", boat_ids)
                .execute()
            )
            arrival_by_boat = {b["id"]: b["arrival_date"] for b in boats_result.data}

            # Get draft items
            draft_ids = [d["id"] for d in filtered_drafts]
            items_result = (
                self.db.table("draft_items")
                .select("draft_id, product_id, selected_pallets")
                .in_("draft_id", draft_ids)
                .execute()
            )

            # Map draft_id -> boat_id
            draft_to_boat = {d["id"]: d["boat_id"] for d in filtered_drafts}

            # Build per-product in-transit list
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
            return {}  # Non-fatal

    def _get_daily_velocities(
        self, products: list[dict], today: date
    ) -> dict[str, Decimal]:
        """
        Compute daily sales velocity per product over the last 90 days.

        Returns:
            Mapping of product_id -> Decimal m2 per day
        """
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

            # Group by product_id, sum quantity_m2
            totals: dict[str, Decimal] = defaultdict(Decimal)
            for row in result.data:
                totals[row["product_id"]] += Decimal(str(row["quantity_m2"]))

            # Convert to daily velocity
            lookback = Decimal(str(VELOCITY_LOOKBACK_DAYS))
            velocity_map: dict[str, Decimal] = {}
            for pid, total_m2 in totals.items():
                velocity_map[pid] = (total_m2 / lookback).quantize(Decimal("0.01"))

            logger.debug("velocities_computed", products_with_sales=len(velocity_map))
            return velocity_map

        except Exception as e:
            logger.error("fetch_sales_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def _get_shipping_routes(self, origin_port: str) -> list[dict]:
        """Fetch active shipping routes for the given origin port."""
        logger.debug("fetching_shipping_routes", origin_port=origin_port)
        try:
            result = (
                self.db.table("shipping_routes")
                .select("*")
                .eq("origin_port", origin_port)
                .eq("active", True)
                .execute()
            )
            logger.debug("shipping_routes_found", count=len(result.data))
            return result.data
        except Exception as e:
            logger.error("fetch_shipping_routes_failed", error=str(e))
            return []  # Non-fatal: fall back to real boats only

    def _get_existing_drafts(
        self, factory_id: str, boats: list[dict]
    ) -> dict[str, dict]:
        """
        Fetch existing drafts for these boats and factory.

        Returns:
            Mapping of boat_id -> draft dict
        """
        if not boats:
            return {}

        boat_ids = [b["id"] for b in boats]
        logger.debug("fetching_drafts", factory_id=factory_id, boat_count=len(boat_ids))

        try:
            result = (
                self.db.table("boat_factory_drafts")
                .select("*")
                .eq("factory_id", factory_id)
                .in_("boat_id", boat_ids)
                .execute()
            )

            drafts_map: dict[str, dict] = {}
            for row in result.data:
                drafts_map[row["boat_id"]] = row

            # Fetch draft items with BL assignments
            if drafts_map:
                draft_ids = [d["id"] for d in drafts_map.values()]
                items_result = (
                    self.db.table("draft_items")
                    .select("*")
                    .in_("draft_id", draft_ids)
                    .execute()
                )
                # Group items by draft_id
                items_by_draft: dict[str, list[dict]] = {}
                for item in items_result.data:
                    items_by_draft.setdefault(item["draft_id"], []).append(item)

                for draft in drafts_map.values():
                    draft["items"] = items_by_draft.get(draft["id"], [])

            logger.debug("drafts_found", count=len(drafts_map))
            return drafts_map

        except Exception as e:
            logger.error("fetch_drafts_failed", error=str(e))
            raise DatabaseError("select", str(e))


# ----------------------------------------------------------------------
# Pure helpers (no DB access)
# ----------------------------------------------------------------------


def _merge_with_phantom_boats(
    real_boats: list[dict],
    routes: list[dict],
    start: date,
    end: date,
) -> list[dict]:
    """
    Fill gaps in the real boat schedule with phantom (estimated) boats
    generated from shipping route patterns.

    For each route, generates expected departure dates within the horizon.
    If a real boat already departs within +/- 2 days of an expected date,
    the phantom is skipped (real boat takes priority).
    """
    if not routes:
        return real_boats

    # Build set of real departure dates for quick lookup
    real_departure_dates = set()
    for boat in real_boats:
        real_departure_dates.add(_parse_date(boat["departure_date"]))

    phantoms: list[dict] = []

    for route in routes:
        db_dow = route["departure_day_of_week"]  # DB uses 0=Sun, 4=Thu
        python_dow = (db_dow - 1) % 7  # Convert to Python's 0=Mon convention
        transit = route["transit_days"]
        freq = route["frequency_weeks"]
        carrier = route.get("carrier", "")
        shipping_line = route.get("shipping_line", "")
        route_name = route.get("name", carrier)
        dest_port = route.get("destination_port", "Miami")

        # Find first matching day-of-week after start
        cursor = start + timedelta(days=1)  # start from tomorrow
        days_ahead = (python_dow - cursor.weekday()) % 7
        cursor = cursor + timedelta(days=days_ahead)

        while cursor <= end:
            # Check if any real boat departs within +/- 2 days
            has_real = any(
                abs((cursor - rd).days) <= 2 for rd in real_departure_dates
            )

            if not has_real:
                arrival = cursor + timedelta(days=transit)
                phantom_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"phantom-{route['id']}-{cursor.isoformat()}"))
                phantoms.append({
                    "id": phantom_id,
                    "vessel_name": f"{route_name} (est.)",
                    "shipping_line": shipping_line,
                    "departure_date": cursor.isoformat(),
                    "arrival_date": arrival.isoformat(),
                    "transit_days": transit,
                    "origin_port": route["origin_port"],
                    "destination_port": dest_port,
                    "status": "estimated",
                    "_is_estimated": True,
                    "_route_carrier": carrier,
                })

            cursor += timedelta(weeks=freq)

    # Merge and sort by departure date
    all_boats = real_boats + phantoms
    all_boats.sort(key=lambda b: b["departure_date"])
    return all_boats


def _parse_date(value: str) -> date:
    """Parse an ISO date string to a date object."""
    return date.fromisoformat(value)


def _classify_urgency(days_of_stock: float) -> str:
    """Classify urgency based on projected days of stock at arrival."""
    if days_of_stock < URGENCY_CRITICAL_DAYS:
        return "critical"
    if days_of_stock < URGENCY_URGENT_DAYS:
        return "urgent"
    if days_of_stock < URGENCY_SOON_DAYS:
        return "soon"
    return "ok"


def _compute_confidence(days_out: int) -> tuple[str, int]:
    """
    Compute confidence label and score based on how far out the boat is.

    Returns:
        (label, score) where score is the midpoint of the band.
    """
    for max_days, label, score_min, score_max in CONFIDENCE_BANDS:
        if days_out <= max_days:
            score = (score_min + score_max) // 2
            return label, score

    default_label, default_min, default_max = CONFIDENCE_DEFAULT
    return default_label, (default_min + default_max) // 2


# ----------------------------------------------------------------------
# Singleton
# ----------------------------------------------------------------------

_service: Optional[ForwardSimulationService] = None


def get_forward_simulation_service() -> ForwardSimulationService:
    """Get or create ForwardSimulationService instance."""
    global _service
    if _service is None:
        _service = ForwardSimulationService()
    return _service
