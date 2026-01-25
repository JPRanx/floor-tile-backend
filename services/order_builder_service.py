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
from config.shipping import (
    CONTAINER_MAX_WEIGHT_KG,
    CONTAINER_MAX_PALLETS,
    DEFAULT_WEIGHT_PER_M2_KG,
    M2_PER_PALLET as SHIPPING_M2_PER_PALLET,
)
from services.boat_schedule_service import get_boat_schedule_service
from services.recommendation_service import get_recommendation_service
from services.inventory_service import get_inventory_service
from services.trend_service import get_trend_service
from models.order_builder import (
    OrderBuilderMode,
    OrderBuilderProduct,
    OrderBuilderBoat,
    OrderBuilderAlert,
    OrderBuilderAlertType,
    OrderBuilderSummary,
    OrderBuilderResponse,
    CalculationBreakdown,
    Urgency,
    # Reasoning models
    ProductReasoning,
    StockAnalysis,
    DemandAnalysis,
    QuantityReasoning,
    OrderSummaryReasoning,
    ExcludedProduct,
    PrimaryFactor,
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
        self.trend_service = get_trend_service()

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

        # Step 2b: Get trend data for products
        trend_data = self._get_product_trends()

        # Step 3: Convert to OrderBuilderProducts grouped by priority
        products_by_priority = self._group_products_by_priority(
            recommendations.recommendations,
            boat.days_until_departure,
            trend_data
        )

        # Step 4: Apply mode logic (pre-select products)
        all_products = self._apply_mode(products_by_priority, mode, boat.max_containers)

        # Step 5: Calculate summary
        summary = self._calculate_summary(all_products, boat.max_containers)

        # Step 6: Generate alerts
        alerts = self._generate_alerts(all_products, summary, boat)
        summary.alerts = alerts

        # Step 7: Generate summary reasoning
        summary_reasoning = self._generate_summary_reasoning(all_products, boat, summary)

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
            summary_reasoning=summary_reasoning,
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

    def _get_product_trends(self) -> dict[str, dict]:
        """
        Fetch trend data from Intelligence system.

        Returns dict keyed by SKU with trend metrics.
        """
        try:
            trends = self.trend_service.get_product_trends(
                period_days=90,
                comparison_period_days=90,
                limit=200  # Get all products
            )

            return {
                t.sku: {
                    "direction": t.direction.value if hasattr(t.direction, 'value') else str(t.direction),
                    "strength": t.strength.value if hasattr(t.strength, 'value') else str(t.strength),
                    "velocity_change_pct": t.velocity_change_pct,
                    "daily_velocity_m2": t.current_velocity_m2_day,
                    "days_of_stock": t.days_of_stock,
                    "confidence": t.confidence.value if hasattr(t.confidence, 'value') else str(t.confidence),
                }
                for t in trends
            }
        except Exception as e:
            logger.warning("trend_fetch_failed", error=str(e))
            return {}

    def _calculate_urgency(self, days_of_stock: Optional[int]) -> str:
        """Classify urgency based on days of stock."""
        if days_of_stock is None:
            return Urgency.OK.value
        if days_of_stock < 7:
            return Urgency.CRITICAL.value
        if days_of_stock < 14:
            return Urgency.URGENT.value
        if days_of_stock < 30:
            return Urgency.SOON.value
        return Urgency.OK.value

    def _calculate_trend_adjustment(
        self,
        direction: str,
        strength: str,
        base_quantity_m2: Decimal
    ) -> tuple[Decimal, Decimal]:
        """
        Calculate trend-based adjustment to order quantity.

        Returns (adjustment_m2, adjustment_pct)
        """
        # Only adjust for upward trends (increase order)
        if direction != "up":
            return Decimal("0"), Decimal("0")

        # Adjustment percentages based on strength
        pct_by_strength = {
            "strong": Decimal("0.20"),   # +20% for strong uptrend
            "moderate": Decimal("0.10"), # +10% for moderate uptrend
            "weak": Decimal("0.05"),     # +5% for weak uptrend
        }

        adjustment_pct = pct_by_strength.get(strength, Decimal("0"))
        adjustment_m2 = base_quantity_m2 * adjustment_pct

        return adjustment_m2, adjustment_pct * 100  # Return as percentage

    def _determine_primary_factor(
        self,
        days_of_stock: Optional[int],
        trend_pct: Decimal,
        velocity: Decimal,
        days_to_boat: int
    ) -> str:
        """
        Determine the primary factor driving this product's recommendation.

        Returns one of: LOW_STOCK, TRENDING_UP, OVERSTOCKED, DECLINING, NO_SALES, NO_DATA, STABLE
        """
        # No sales data
        if velocity is None or velocity == 0:
            return PrimaryFactor.NO_SALES.value

        # No stock data
        if days_of_stock is None:
            return PrimaryFactor.NO_DATA.value

        # Overstocked with declining demand
        if days_of_stock > 180 and trend_pct < -25:
            return PrimaryFactor.OVERSTOCKED.value

        # Significant demand decline even with moderate stock
        if days_of_stock > 90 and trend_pct < -50:
            return PrimaryFactor.DECLINING.value

        # Low stock - will stockout before boat or within 14 days
        if days_of_stock < 14 or days_of_stock < days_to_boat:
            return PrimaryFactor.LOW_STOCK.value

        # Strong upward trend
        if trend_pct > 30:
            return PrimaryFactor.TRENDING_UP.value

        return PrimaryFactor.STABLE.value

    def _group_products_by_priority(
        self,
        recommendations: list,
        days_to_cover: int,
        trend_data: dict[str, dict]
    ) -> dict[str, list[OrderBuilderProduct]]:
        """Convert recommendations to OrderBuilderProducts grouped by priority."""
        groups = {
            "HIGH_PRIORITY": [],
            "CONSIDER": [],
            "WELL_COVERED": [],
            "YOUR_CALL": [],
        }

        SAFETY_STOCK_DAYS = 14

        for rec in recommendations:
            # Get trend data for this product
            trend = trend_data.get(rec.sku, {})
            direction = trend.get("direction", "stable")
            strength = trend.get("strength", "weak")
            velocity_change_pct = Decimal(str(trend.get("velocity_change_pct", 0)))
            daily_velocity_m2 = Decimal(str(trend.get("daily_velocity_m2", 0)))
            days_of_stock = trend.get("days_of_stock")

            # Calculate urgency based on days of stock
            urgency = self._calculate_urgency(days_of_stock)

            # Calculate base quantity (lead time + safety stock) × velocity
            total_coverage_days = days_to_cover + SAFETY_STOCK_DAYS
            base_quantity_m2 = daily_velocity_m2 * Decimal(total_coverage_days)

            # Calculate trend adjustment
            trend_adjustment_m2, trend_adjustment_pct = self._calculate_trend_adjustment(
                direction, strength, base_quantity_m2
            )

            # Calculate adjusted requirement
            adjusted_quantity_m2 = base_quantity_m2 + trend_adjustment_m2

            # Subtract current stock and incoming
            minus_current = rec.warehouse_m2 or Decimal("0")
            minus_incoming = rec.in_transit_m2 or Decimal("0")

            final_suggestion_m2 = max(
                Decimal("0"),
                adjusted_quantity_m2 - minus_current - minus_incoming
            )

            # Convert to pallets
            final_suggestion_pallets = max(0, math.ceil(float(final_suggestion_m2 / M2_PER_PALLET)))

            # Build calculation breakdown
            breakdown = CalculationBreakdown(
                lead_time_days=days_to_cover,
                safety_stock_days=SAFETY_STOCK_DAYS,
                daily_velocity_m2=daily_velocity_m2,
                base_quantity_m2=round(base_quantity_m2, 2),
                trend_adjustment_m2=round(trend_adjustment_m2, 2),
                trend_adjustment_pct=round(trend_adjustment_pct, 1),
                minus_current_stock_m2=minus_current,
                minus_incoming_m2=minus_incoming,
                final_suggestion_m2=round(final_suggestion_m2, 2),
                final_suggestion_pallets=final_suggestion_pallets,
            )

            # Use the calculated suggestion if we have trend data, otherwise fall back to original
            coverage_gap_pallets = max(0, rec.coverage_gap_pallets or 0)
            suggested = final_suggestion_pallets if daily_velocity_m2 > 0 else coverage_gap_pallets

            # Determine primary factor for reasoning
            primary_factor = self._determine_primary_factor(
                days_of_stock=days_of_stock,
                trend_pct=velocity_change_pct,
                velocity=daily_velocity_m2,
                days_to_boat=days_to_cover
            )

            # Calculate gap days (negative = stockout before boat)
            gap_days = None
            if days_of_stock is not None:
                gap_days = Decimal(str(days_of_stock)) - Decimal(str(days_to_cover))

            # Determine exclusion reason if suggested is 0
            exclusion_reason = None
            if suggested == 0:
                if primary_factor == PrimaryFactor.OVERSTOCKED.value:
                    exclusion_reason = "OVERSTOCKED"
                elif primary_factor == PrimaryFactor.NO_SALES.value:
                    exclusion_reason = "NO_SALES"
                elif primary_factor == PrimaryFactor.DECLINING.value:
                    exclusion_reason = "DECLINING"
                elif primary_factor == PrimaryFactor.NO_DATA.value:
                    exclusion_reason = "NO_DATA"

            # Build reasoning object
            reasoning = ProductReasoning(
                primary_factor=primary_factor,
                stock=StockAnalysis(
                    current_m2=minus_current,
                    days_of_stock=Decimal(str(days_of_stock)) if days_of_stock is not None else None,
                    days_to_boat=days_to_cover,
                    gap_days=gap_days,
                ),
                demand=DemandAnalysis(
                    velocity_m2_day=daily_velocity_m2,
                    trend_pct=velocity_change_pct,
                    trend_direction=direction,
                    sales_rank=None,  # Will be populated later with ranking
                ),
                quantity=QuantityReasoning(
                    target_coverage_days=total_coverage_days,
                    m2_needed=round(adjusted_quantity_m2, 2),
                    m2_in_transit=minus_incoming,
                    m2_in_stock=minus_current,
                    m2_to_order=round(final_suggestion_m2, 2),
                ),
                exclusion_reason=exclusion_reason,
            )

            product = OrderBuilderProduct(
                product_id=rec.product_id,
                sku=rec.sku,
                description=None,
                priority=rec.priority.value,
                action_type=rec.action_type.value,
                current_stock_m2=rec.warehouse_m2,
                in_transit_m2=rec.in_transit_m2,
                days_to_cover=days_to_cover,
                total_demand_m2=rec.total_demand_m2 or Decimal("0"),
                coverage_gap_m2=rec.coverage_gap_m2 or Decimal("0"),
                coverage_gap_pallets=coverage_gap_pallets,
                suggested_pallets=suggested,
                confidence=rec.confidence.value,
                confidence_reason=rec.confidence_reason,
                unique_customers=rec.unique_customers,
                top_customer_name=rec.top_customer_name,
                top_customer_share=rec.top_customer_share,
                factory_available=None,
                factory_status="unknown",
                # Trend fields
                urgency=urgency,
                days_of_stock=days_of_stock,
                trend_direction=direction,
                trend_strength=strength,
                velocity_change_pct=velocity_change_pct,
                daily_velocity_m2=daily_velocity_m2,
                calculation_breakdown=breakdown if daily_velocity_m2 > 0 else None,
                # Reasoning
                reasoning=reasoning,
                # Selection
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
        """Calculate order summary from selected products with weight-based container limits."""
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

        # Calculate weight-based container requirements
        # Each product may have different weight per m² (future support)
        total_weight_kg = Decimal("0")
        for p in selected:
            product_m2 = Decimal(p.selected_pallets) * M2_PER_PALLET
            weight_per_m2 = p.weight_per_m2_kg or DEFAULT_WEIGHT_PER_M2_KG
            product_weight = product_m2 * weight_per_m2
            total_weight_kg += product_weight
            # Update product's total_weight_kg for UI display
            p.total_weight_kg = product_weight

        # Containers by pallet count (physical limit)
        containers_by_pallets = math.ceil(total_pallets / PALLETS_PER_CONTAINER) if total_pallets > 0 else 0

        # Containers by weight (27,500 kg limit per container)
        containers_by_weight = math.ceil(float(total_weight_kg) / CONTAINER_MAX_WEIGHT_KG) if total_weight_kg > 0 else 0

        # Total containers = max of both (weight is typically the constraint)
        # With standard tiles: 14 pallets × 134.4 m² × 14.90 kg/m² = 28,036 kg > 27,500 kg
        total_containers = max(containers_by_pallets, containers_by_weight)
        weight_is_limiting = containers_by_weight > containers_by_pallets

        # Warehouse after delivery
        warehouse_after = warehouse_current_pallets + total_pallets
        utilization_after = Decimal(warehouse_after) / Decimal(WAREHOUSE_CAPACITY) * 100

        return OrderBuilderSummary(
            total_pallets=total_pallets,
            total_containers=total_containers,
            total_m2=total_m2,
            # Weight-based calculations
            total_weight_kg=round(total_weight_kg, 2),
            containers_by_pallets=containers_by_pallets,
            containers_by_weight=containers_by_weight,
            weight_is_limiting=weight_is_limiting,
            # Capacity
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

        # 4. Weight is limiting factor
        if summary.weight_is_limiting and summary.containers_by_weight > summary.containers_by_pallets:
            extra = summary.containers_by_weight - summary.containers_by_pallets
            alerts.append(OrderBuilderAlert(
                type=OrderBuilderAlertType.WARNING,
                icon="⚖️",
                message=f"Weight adds {extra} container(s) ({summary.total_weight_kg:,.0f} kg exceeds {CONTAINER_MAX_WEIGHT_KG:,} kg limit)"
            ))

        # 5. Room for more
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

    def _generate_summary_reasoning(
        self,
        all_products: list[OrderBuilderProduct],
        boat: OrderBuilderBoat,
        summary: OrderBuilderSummary
    ) -> OrderSummaryReasoning:
        """
        Generate order-level reasoning with strategy, counts, insights, and excluded products.
        """
        # Count by urgency
        critical_count = sum(1 for p in all_products if p.urgency == "critical")
        urgent_count = sum(1 for p in all_products if p.urgency == "urgent")
        stable_count = sum(1 for p in all_products if p.urgency in ["ok", "soon"])

        # Build excluded products list (products with suggested_pallets = 0)
        excluded_products = []
        for p in all_products:
            if p.suggested_pallets == 0 and p.reasoning and p.reasoning.exclusion_reason:
                excluded_products.append(ExcludedProduct(
                    sku=p.sku,
                    product_name=p.description,
                    reason=p.reasoning.exclusion_reason,
                    days_of_stock=p.reasoning.stock.days_of_stock,
                    trend_pct=p.reasoning.demand.trend_pct,
                    last_sale_days_ago=None,  # Could be populated from trend data
                ))

        excluded_count = len(excluded_products)

        # Determine overall strategy
        if critical_count > 0:
            strategy = "STOCKOUT_PREVENTION"
        elif urgent_count > 0:
            strategy = "DEMAND_CAPTURE"
        else:
            strategy = "BALANCED"

        # Generate key insights
        key_insights = []

        # Insight 1: Stockout risk count
        stockout_risk_count = sum(
            1 for p in all_products
            if p.reasoning and p.reasoning.stock.gap_days is not None
            and p.reasoning.stock.gap_days < 0
        )
        if stockout_risk_count > 0:
            key_insights.append(
                f"{stockout_risk_count} product(s) will stockout before boat arrives"
            )

        # Insight 2: Highest risk product
        products_with_stock = [
            p for p in all_products
            if p.reasoning and p.reasoning.stock.days_of_stock is not None
        ]
        if products_with_stock:
            most_critical = min(
                products_with_stock,
                key=lambda p: p.reasoning.stock.days_of_stock
            )
            days = most_critical.reasoning.stock.days_of_stock
            key_insights.append(
                f"{most_critical.sku} is highest-risk ({days:.0f} days of stock)"
            )

        # Insight 3: Container utilization
        if summary.total_containers > 0:
            weight_util = float(summary.total_weight_kg) / (summary.total_containers * CONTAINER_MAX_WEIGHT_KG) * 100
            key_insights.append(
                f"Container weight utilization: {weight_util:.0f}%"
            )

        # Insight 4: Excluded products summary
        if excluded_count > 0:
            overstocked = sum(1 for e in excluded_products if e.reason == "OVERSTOCKED")
            no_sales = sum(1 for e in excluded_products if e.reason == "NO_SALES")
            declining = sum(1 for e in excluded_products if e.reason == "DECLINING")

            reasons = []
            if overstocked > 0:
                reasons.append(f"{overstocked} overstocked")
            if no_sales > 0:
                reasons.append(f"{no_sales} with no sales")
            if declining > 0:
                reasons.append(f"{declining} with declining demand")

            if reasons:
                key_insights.append(
                    f"{excluded_count} product(s) excluded: {', '.join(reasons)}"
                )

        # Insight 5: Trending products
        trending_up_count = sum(
            1 for p in all_products
            if p.reasoning and p.reasoning.primary_factor == PrimaryFactor.TRENDING_UP.value
        )
        if trending_up_count > 0:
            key_insights.append(
                f"{trending_up_count} product(s) with strong upward demand trend"
            )

        return OrderSummaryReasoning(
            strategy=strategy,
            days_to_boat=boat.days_until_departure,
            boat_date=boat.departure_date.isoformat(),
            boat_name=boat.name,
            critical_count=critical_count,
            urgent_count=urgent_count,
            stable_count=stable_count,
            excluded_count=excluded_count,
            key_insights=key_insights[:5],  # Top 5 insights
            excluded_products=excluded_products[:10],  # Top 10 excluded
        )

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

        empty_reasoning = OrderSummaryReasoning(
            strategy="BALANCED",
            days_to_boat=0,
            boat_date=today.isoformat(),
            boat_name="No boats scheduled",
            critical_count=0,
            urgent_count=0,
            stable_count=0,
            excluded_count=0,
            key_insights=["No boat schedule available. Upload a boat schedule to get recommendations."],
            excluded_products=[],
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
            summary_reasoning=empty_reasoning,
        )


# Singleton instance
_order_builder_service: Optional[OrderBuilderService] = None


def get_order_builder_service() -> OrderBuilderService:
    """Get or create OrderBuilderService instance."""
    global _order_builder_service
    if _order_builder_service is None:
        _order_builder_service = OrderBuilderService()
    return _order_builder_service
