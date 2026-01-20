"""
Order Builder service — Hero feature business logic.

Answers: "What should I order for the next boat?"
Combines coverage gap, confidence, and 4-level optimization.

See BUILDER_BLUEPRINT.md for algorithm details.
"""

from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
import math
import structlog

from config import settings
from services.boat_schedule_service import get_boat_schedule_service
from services.recommendation_service import get_recommendation_service
from services.inventory_service import get_inventory_service
from models.order_builder import (
    OrderBuilderMode,
    OrderBuilderProduct,
    OrderBuilderBoat,
    OrderBuilderAlert,
    OrderBuilderAlertType,
    OrderBuilderSummary,
    OrderBuilderResponse,
)
from models.recommendation import RecommendationPriority

logger = structlog.get_logger(__name__)

# Constants (actual factory pallet dimensions)
M2_PER_PALLET = Decimal("134.4")
PALLETS_PER_CONTAINER = 14
WAREHOUSE_CAPACITY = 740  # pallets


class OrderBuilderService:
    """
    Order Builder business logic.

    Calculates:
    1. Which boat to target
    2. Products grouped by priority
    3. Pre-selection based on mode
    4. Summary with capacity checks
    5. Alerts for issues
    """

    def __init__(self):
        self.boat_service = get_boat_schedule_service()
        self.recommendation_service = get_recommendation_service()
        self.inventory_service = get_inventory_service()

    def get_order_builder(
        self,
        boat_id: Optional[str] = None,
        mode: OrderBuilderMode = OrderBuilderMode.STANDARD,
    ) -> OrderBuilderResponse:
        """
        Get complete Order Builder data.

        Args:
            boat_id: Optional specific boat ID. If None, uses next available.
            mode: Optimization mode (minimal/standard/optimal)

        Returns:
            OrderBuilderResponse with all data needed for the UI
        """
        logger.info(
            "getting_order_builder",
            boat_id=boat_id,
            mode=mode.value
        )

        # Step 1: Get boat info
        boat, next_boat = self._get_boats(boat_id)

        # Default lead time for "no boat" mode (45 days)
        DEFAULT_LEAD_TIME_DAYS = 45

        if not boat:
            # No boats available - use default lead time and create dummy boat
            logger.info("no_boats_available_using_default_lead_time", days=DEFAULT_LEAD_TIME_DAYS)
            today = date.today()
            default_departure = today + timedelta(days=DEFAULT_LEAD_TIME_DAYS)
            default_arrival = default_departure + timedelta(days=25)  # ~25 days transit

            boat = OrderBuilderBoat(
                boat_id="",
                name="",
                departure_date=default_departure,
                arrival_date=default_arrival,
                days_until_departure=DEFAULT_LEAD_TIME_DAYS,
                booking_deadline=today,
                days_until_deadline=0,
                max_containers=5,
            )

        # Step 2: Get recommendations (has coverage gap, confidence, priority)
        recommendations = self.recommendation_service.get_recommendations()

        # Step 3: Convert to OrderBuilderProducts grouped by priority
        products_by_priority = self._group_products_by_priority(
            recommendations.recommendations,
            boat.days_until_departure
        )

        # Step 4: Apply mode logic (pre-select products)
        all_products = self._apply_mode(products_by_priority, mode, boat.max_containers)

        # Step 5: Calculate summary
        summary = self._calculate_summary(all_products, boat.max_containers)

        # Step 6: Generate alerts
        alerts = self._generate_alerts(all_products, summary, boat)
        summary.alerts = alerts

        # Re-group after mode application
        high_priority = [p for p in all_products if p.priority == "HIGH_PRIORITY"]
        consider = [p for p in all_products if p.priority == "CONSIDER"]
        well_covered = [p for p in all_products if p.priority == "WELL_COVERED"]
        your_call = [p for p in all_products if p.priority == "YOUR_CALL"]

        result = OrderBuilderResponse(
            boat=boat,
            next_boat=next_boat,
            mode=mode,
            high_priority=high_priority,
            consider=consider,
            well_covered=well_covered,
            your_call=your_call,
            summary=summary,
        )

        logger.info(
            "order_builder_generated",
            mode=mode.value,
            high_priority=len(high_priority),
            consider=len(consider),
            well_covered=len(well_covered),
            your_call=len(your_call),
            total_selected=summary.total_pallets,
        )

        return result

    def _get_boats(
        self,
        boat_id: Optional[str]
    ) -> tuple[Optional[OrderBuilderBoat], Optional[OrderBuilderBoat]]:
        """Get target boat and next boat after that."""
        today = date.today()

        if boat_id:
            # Get specific boat
            try:
                boat_data = self.boat_service.get_by_id(boat_id)
                boat = self._to_order_builder_boat(boat_data, today)
            except Exception:
                logger.warning("boat_not_found", boat_id=boat_id)
                boat = None
        else:
            # Get next available boat
            boat_data = self.boat_service.get_next_available()
            boat = self._to_order_builder_boat(boat_data, today) if boat_data else None

        # Get the next boat after this one
        available_boats = self.boat_service.get_available(limit=2)
        next_boat = None

        if len(available_boats) > 1:
            # If we got a specific boat, find the one after it
            if boat:
                for b in available_boats:
                    if b.departure_date > boat.departure_date:
                        next_boat = self._to_order_builder_boat(b, today)
                        break
            else:
                # Use second available
                next_boat = self._to_order_builder_boat(available_boats[1], today)

        return boat, next_boat

    def _to_order_builder_boat(self, boat_data, today: date) -> OrderBuilderBoat:
        """Convert BoatScheduleResponse to OrderBuilderBoat."""
        days_until_departure = (boat_data.departure_date - today).days
        days_until_deadline = (boat_data.booking_deadline - today).days

        return OrderBuilderBoat(
            boat_id=boat_data.id,
            name=boat_data.vessel_name or f"Boat {boat_data.departure_date}",
            departure_date=boat_data.departure_date,
            arrival_date=boat_data.arrival_date,
            days_until_departure=max(0, days_until_departure),
            booking_deadline=boat_data.booking_deadline,
            days_until_deadline=max(0, days_until_deadline),
            max_containers=5,  # Default, could be configurable per boat
        )

    def _group_products_by_priority(
        self,
        recommendations: list,
        days_to_cover: int
    ) -> dict[str, list[OrderBuilderProduct]]:
        """Convert recommendations to OrderBuilderProducts grouped by priority."""
        groups = {
            "HIGH_PRIORITY": [],
            "CONSIDER": [],
            "WELL_COVERED": [],
            "YOUR_CALL": [],
        }

        for rec in recommendations:
            # Calculate coverage gap pallets (ensure non-negative)
            coverage_gap_pallets = max(0, rec.coverage_gap_pallets or 0)

            product = OrderBuilderProduct(
                product_id=rec.product_id,
                sku=rec.sku,
                description=None,  # Could add from product service if needed
                priority=rec.priority.value,
                action_type=rec.action_type.value,
                current_stock_m2=rec.warehouse_m2,
                in_transit_m2=rec.in_transit_m2,
                days_to_cover=days_to_cover,
                total_demand_m2=rec.total_demand_m2 or Decimal("0"),
                coverage_gap_m2=rec.coverage_gap_m2 or Decimal("0"),
                coverage_gap_pallets=coverage_gap_pallets,
                suggested_pallets=coverage_gap_pallets,
                confidence=rec.confidence.value,
                confidence_reason=rec.confidence_reason,
                unique_customers=rec.unique_customers,
                top_customer_name=rec.top_customer_name,
                top_customer_share=rec.top_customer_share,
                factory_available=None,
                factory_status="unknown",
                is_selected=False,
                selected_pallets=0,
            )

            priority_key = rec.priority.value
            if priority_key in groups:
                groups[priority_key].append(product)
            else:
                groups["YOUR_CALL"].append(product)

        return groups

    def _apply_mode(
        self,
        products_by_priority: dict[str, list[OrderBuilderProduct]],
        mode: OrderBuilderMode,
        boat_max_containers: int
    ) -> list[OrderBuilderProduct]:
        """
        Apply mode logic to pre-select products.

        Mode determines container limit:
        - minimal: 3 containers (42 pallets)
        - standard: 4 containers (56 pallets)
        - optimal: 5 containers (70 pallets)
        """
        max_pallets = {
            OrderBuilderMode.MINIMAL: 3 * PALLETS_PER_CONTAINER,   # 42
            OrderBuilderMode.STANDARD: 4 * PALLETS_PER_CONTAINER,  # 56
            OrderBuilderMode.OPTIMAL: 5 * PALLETS_PER_CONTAINER,   # 70
        }[mode]

        # Cap at boat's actual capacity
        max_pallets = min(max_pallets, boat_max_containers * PALLETS_PER_CONTAINER)

        total_selected = 0
        all_products = []

        # First pass: HIGH_PRIORITY (always include if room)
        for p in products_by_priority.get("HIGH_PRIORITY", []):
            pallets_needed = p.coverage_gap_pallets
            if pallets_needed > 0 and total_selected + pallets_needed <= max_pallets:
                p.is_selected = True
                p.selected_pallets = pallets_needed
                total_selected += pallets_needed
            elif pallets_needed > 0:
                # Partial fill if there's room
                remaining = max_pallets - total_selected
                if remaining > 0:
                    p.is_selected = True
                    p.selected_pallets = remaining
                    total_selected += remaining
            all_products.append(p)

        # Second pass: CONSIDER (if mode >= standard)
        if mode in [OrderBuilderMode.STANDARD, OrderBuilderMode.OPTIMAL]:
            for p in products_by_priority.get("CONSIDER", []):
                pallets_needed = p.coverage_gap_pallets
                if pallets_needed > 0 and total_selected + pallets_needed <= max_pallets:
                    p.is_selected = True
                    p.selected_pallets = pallets_needed
                    total_selected += pallets_needed
                elif pallets_needed > 0:
                    # Partial fill
                    remaining = max_pallets - total_selected
                    if remaining > 0:
                        p.is_selected = True
                        p.selected_pallets = remaining
                        total_selected += remaining
                all_products.append(p)
        else:
            # Still include CONSIDER products, just not selected
            all_products.extend(products_by_priority.get("CONSIDER", []))

        # Third pass: WELL_COVERED (only if mode == optimal and room left)
        for p in products_by_priority.get("WELL_COVERED", []):
            if mode == OrderBuilderMode.OPTIMAL:
                remaining = max_pallets - total_selected
                if remaining > 0:
                    # Add partial to help fill containers
                    pallets_to_add = min(PALLETS_PER_CONTAINER, remaining, p.coverage_gap_pallets or PALLETS_PER_CONTAINER)
                    if pallets_to_add > 0:
                        p.is_selected = True
                        p.selected_pallets = pallets_to_add
                        total_selected += pallets_to_add
            all_products.append(p)

        # YOUR_CALL products - never auto-select
        all_products.extend(products_by_priority.get("YOUR_CALL", []))

        logger.debug(
            "mode_applied",
            mode=mode.value,
            max_pallets=max_pallets,
            total_selected=total_selected,
            products_count=len(all_products)
        )

        return all_products

    def _calculate_summary(
        self,
        products: list[OrderBuilderProduct],
        boat_max_containers: int
    ) -> OrderBuilderSummary:
        """Calculate order summary from selected products."""
        # Get current warehouse level
        inventory_snapshots = self.inventory_service.get_latest()
        warehouse_current_m2 = sum(
            Decimal(str(inv.warehouse_qty))
            for inv in inventory_snapshots
        )
        warehouse_current_pallets = int(warehouse_current_m2 / M2_PER_PALLET)

        # Calculate selection totals
        selected = [p for p in products if p.is_selected]
        total_pallets = sum(p.selected_pallets for p in selected)
        total_m2 = Decimal(total_pallets) * M2_PER_PALLET
        total_containers = math.ceil(total_pallets / PALLETS_PER_CONTAINER) if total_pallets > 0 else 0

        # Warehouse after delivery
        warehouse_after = warehouse_current_pallets + total_pallets
        utilization_after = Decimal(warehouse_after) / Decimal(WAREHOUSE_CAPACITY) * 100

        return OrderBuilderSummary(
            total_pallets=total_pallets,
            total_containers=total_containers,
            total_m2=total_m2,
            boat_max_containers=boat_max_containers,
            boat_remaining_containers=max(0, boat_max_containers - total_containers),
            warehouse_current_pallets=warehouse_current_pallets,
            warehouse_capacity=WAREHOUSE_CAPACITY,
            warehouse_after_delivery=warehouse_after,
            warehouse_utilization_after=round(utilization_after, 1),
            alerts=[],  # Populated later
        )

    def _generate_alerts(
        self,
        products: list[OrderBuilderProduct],
        summary: OrderBuilderSummary,
        boat: OrderBuilderBoat
    ) -> list[OrderBuilderAlert]:
        """Generate alerts based on current selection."""
        alerts = []

        # 1. Warehouse capacity exceeded
        if summary.warehouse_after_delivery > WAREHOUSE_CAPACITY:
            over = summary.warehouse_after_delivery - WAREHOUSE_CAPACITY
            alerts.append(OrderBuilderAlert(
                type=OrderBuilderAlertType.BLOCKED,
                icon="🚫",
                message=f"Exceeds warehouse by {over} pallets. Remove some items."
            ))

        # 2. Warehouse near capacity (>95%)
        elif summary.warehouse_utilization_after > Decimal("95"):
            alerts.append(OrderBuilderAlert(
                type=OrderBuilderAlertType.WARNING,
                icon="⚠️",
                message=f"Warehouse will be at {summary.warehouse_utilization_after:.0f}% after delivery"
            ))

        # 3. Boat capacity exceeded
        if summary.total_containers > boat.max_containers:
            alerts.append(OrderBuilderAlert(
                type=OrderBuilderAlertType.BLOCKED,
                icon="🚫",
                message=f"Exceeds boat capacity ({summary.total_containers}/{boat.max_containers} containers)"
            ))

        # 4. Room for more
        if (summary.boat_remaining_containers > 0 and
            summary.warehouse_utilization_after < Decimal("90")):
            alerts.append(OrderBuilderAlert(
                type=OrderBuilderAlertType.SUGGESTION,
                icon="💡",
                message=f"Room for {summary.boat_remaining_containers} more container(s)"
            ))

        # 5. HIGH_PRIORITY items not selected
        for p in products:
            if p.priority == "HIGH_PRIORITY" and not p.is_selected:
                alerts.append(OrderBuilderAlert(
                    type=OrderBuilderAlertType.WARNING,
                    icon="⚠️",
                    product_sku=p.sku,
                    message=f"{p.sku}: HIGH_PRIORITY but not selected — stockout risk"
                ))

        # 6. LOW confidence items selected
        for p in products:
            if p.is_selected and p.confidence == "LOW":
                alerts.append(OrderBuilderAlert(
                    type=OrderBuilderAlertType.WARNING,
                    icon="⚠️",
                    product_sku=p.sku,
                    message=f"{p.sku}: {p.confidence_reason}"
                ))

        # 7. Booking deadline warning
        if boat.days_until_deadline <= 3:
            alerts.insert(0, OrderBuilderAlert(
                type=OrderBuilderAlertType.WARNING,
                icon="⏰",
                message=f"Booking deadline in {boat.days_until_deadline} days!"
            ))

        return alerts

    def _empty_response(self, mode: OrderBuilderMode) -> OrderBuilderResponse:
        """Return empty response when no boats available."""
        today = date.today()
        dummy_boat = OrderBuilderBoat(
            boat_id="",
            name="No boats scheduled",
            departure_date=today,
            arrival_date=today,
            days_until_departure=0,
            booking_deadline=today,
            days_until_deadline=0,
            max_containers=5,
        )

        summary = OrderBuilderSummary(
            alerts=[
                OrderBuilderAlert(
                    type=OrderBuilderAlertType.WARNING,
                    icon="⚠️",
                    message="No boats available. Upload a boat schedule first."
                )
            ]
        )

        return OrderBuilderResponse(
            boat=dummy_boat,
            next_boat=None,
            mode=mode,
            high_priority=[],
            consider=[],
            well_covered=[],
            your_call=[],
            summary=summary,
        )


# Singleton instance
_order_builder_service: Optional[OrderBuilderService] = None


def get_order_builder_service() -> OrderBuilderService:
    """Get or create OrderBuilderService instance."""
    global _order_builder_service
    if _order_builder_service is None:
        _order_builder_service = OrderBuilderService()
    return _order_builder_service
