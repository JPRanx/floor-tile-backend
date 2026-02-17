"""
Forward simulation service for projecting future orders across boats.

Implements the 3-month planning horizon for Order Builder V2.
For each upcoming boat tied to a factory's origin port, projects inventory
depletion using daily velocity, identifies replenishment needs, and
estimates pallet/container counts with confidence scoring.
"""

import math
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import structlog

from config import get_supabase_client, DatabaseError
from config.shipping import (
    M2_PER_PALLET,
    CONTAINER_MAX_PALLETS,
    WAREHOUSE_BUFFER_DAYS,
    ORDERING_CYCLE_DAYS,
)

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
            boats = self._get_upcoming_boats(factory["origin_port"], today, horizon_end)
            products = self._get_active_products(factory_id)
            inventory_map = self._get_latest_inventory(products)
            velocity_map = self._get_daily_velocities(products, today)
            drafts_map = self._get_existing_drafts(factory_id, boats)

            # Build running stock tracker from current inventory
            current_stock: dict[str, Decimal] = {}
            for p in products:
                pid = p["id"]
                inv = inventory_map.get(pid, {})
                warehouse = Decimal(str(inv.get("warehouse_qty", 0)))
                in_transit = Decimal(str(inv.get("in_transit_qty", 0)))
                current_stock[pid] = warehouse + in_transit

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
                )
                boat_projections.append(projection)

            result = {
                "factory_id": factory_id,
                "factory_name": factory.get("name", ""),
                "horizon_months": months,
                "generated_at": today.isoformat(),
                "projections": boat_projections,
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
    ) -> dict:
        """
        Simulate a single boat: project stock at arrival, compute needs.

        Mutates *current_stock* in place to subtract the suggested order
        quantities so subsequent boats see the replenished inventory.
        """
        boat_id = boat["id"]
        arrival_date = _parse_date(boat["arrival_date"])
        departure_date = _parse_date(boat["departure_date"])

        days_until_arrival = (arrival_date - today).days + WAREHOUSE_BUFFER_DAYS
        if days_until_arrival < 1:
            days_until_arrival = 1

        coverage_target = ORDERING_CYCLE_DAYS + days_until_arrival

        # Per-product projections
        product_details: list[dict] = []
        total_pallets = 0
        urgency_counts = {"critical": 0, "urgent": 0, "soon": 0, "ok": 0}

        for product in products:
            pid = product["id"]
            daily_vel = velocity_map.get(pid, Decimal("0"))
            stock = current_stock.get(pid, Decimal("0"))

            projected_stock = stock - (daily_vel * days_until_arrival)

            # Urgency based on days of stock at arrival
            if daily_vel > 0:
                days_of_stock = float(projected_stock / daily_vel)
            else:
                days_of_stock = 999.0  # no demand -- effectively infinite

            urgency = _classify_urgency(days_of_stock)
            urgency_counts[urgency] += 1

            # Replenishment need
            coverage_gap = max(
                Decimal("0"),
                (daily_vel * coverage_target) - projected_stock,
            )
            suggested_pallets = math.ceil(coverage_gap / M2_PER_PALLET) if coverage_gap > 0 else 0
            total_pallets += suggested_pallets

            product_details.append({
                "product_id": pid,
                "product_name": product.get("sku", ""),
                "daily_velocity_m2": float(daily_vel.quantize(Decimal("0.01"))),
                "current_stock_m2": float(stock.quantize(Decimal("0.01"))),
                "projected_stock_m2": float(projected_stock.quantize(Decimal("0.01"))),
                "days_of_stock_at_arrival": round(days_of_stock, 1),
                "urgency": urgency,
                "coverage_gap_m2": float(coverage_gap.quantize(Decimal("0.01"))),
                "suggested_pallets": suggested_pallets,
            })

            # Subtract ordered quantity from running stock so next boat sees it
            if suggested_pallets > 0:
                filled_m2 = Decimal(suggested_pallets) * M2_PER_PALLET
                current_stock[pid] = projected_stock + filled_m2

        # Confidence
        days_out = (departure_date - today).days
        confidence_label, confidence_score = _compute_confidence(days_out)

        # Pallet range (uncertainty band)
        estimated_pallets_min = int(total_pallets * (confidence_score / 100))
        estimated_pallets_max = int(total_pallets * (2 - confidence_score / 100))

        # Containers
        estimated_containers = math.ceil(total_pallets / CONTAINER_MAX_PALLETS) if total_pallets > 0 else 0

        # Window opens
        production_lead = int(factory.get("production_lead_days", 0))
        transport_to_port = int(factory.get("transport_to_port_days", 0))
        window_opens = departure_date - timedelta(days=production_lead + transport_to_port)

        # Existing draft
        draft = drafts_map.get(boat_id)

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
                .select("id, sku")
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
        Get latest inventory snapshot per product.

        Returns:
            Mapping of product_id -> {warehouse_qty, in_transit_qty}
        """
        if not products:
            return {}

        logger.debug("fetching_inventory", product_count=len(products))
        try:
            result = (
                self.db.table("inventory_snapshots")
                .select("product_id, warehouse_qty, in_transit_qty")
                .order("created_at", desc=True)
                .execute()
            )

            # Deduplicate: keep first (latest) per product_id
            inventory_map: dict[str, dict] = {}
            for row in result.data:
                pid = row["product_id"]
                if pid not in inventory_map:
                    inventory_map[pid] = row

            logger.debug("inventory_loaded", unique_products=len(inventory_map))
            return inventory_map

        except Exception as e:
            logger.error("fetch_inventory_failed", error=str(e))
            raise DatabaseError("select", str(e))

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

            logger.debug("drafts_found", count=len(drafts_map))
            return drafts_map

        except Exception as e:
            logger.error("fetch_drafts_failed", error=str(e))
            raise DatabaseError("select", str(e))


# ----------------------------------------------------------------------
# Pure helpers (no DB access)
# ----------------------------------------------------------------------


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
