"""
Recommendation service — Core "what to order" business logic.

Calculates warehouse allocations and generates order recommendations
based on sales velocity, safety stock, and current inventory levels.

See BUILDER_BLUEPRINT.md for algorithm details.
"""

from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
from math import sqrt
import structlog

from config import settings
from services.product_service import get_product_service
from services.inventory_service import get_inventory_service
from services.sales_service import get_sales_service
from services.stockout_service import get_stockout_service, StockoutStatus
from models.recommendation import (
    ProductAllocation,
    ProductRecommendation,
    RecommendationWarning,
    RecommendationPriority,
    ActionType,
    WarningType,
    WarehouseStatus,
    OrderRecommendations,
    ConfidenceLevel,
)
from services.boat_schedule_service import get_boat_schedule_service

logger = structlog.get_logger(__name__)


# Constants
M2_PER_PALLET = Decimal("135")
Z_SCORE = Decimal("1.645")  # 95% service level
OVER_STOCK_THRESHOLD = Decimal("-5")  # Pallets over target to warn


class RecommendationService:
    """
    Order recommendation business logic.

    Calculates:
    1. Warehouse allocation per product (target stock levels)
    2. Order recommendations based on gap between current and target
    """

    def __init__(self):
        self.product_service = get_product_service()
        self.inventory_service = get_inventory_service()
        self.sales_service = get_sales_service()
        self.stockout_service = get_stockout_service()
        self.boat_service = get_boat_schedule_service()

        # Settings (from environment config, not database)
        self.lead_time = settings.lead_time_days  # 45 days
        self.warehouse_capacity_pallets = settings.warehouse_max_pallets  # 740
        self.warehouse_capacity_m2 = Decimal(str(self.warehouse_capacity_pallets)) * M2_PER_PALLET
        self.sales_weeks = settings.velocity_window_weeks  # 12 weeks default
        self.low_volume_min_records = settings.low_volume_min_records  # 2 records minimum

    def allocate_warehouse_slots(self) -> tuple[list[ProductAllocation], Decimal]:
        """
        Calculate target warehouse allocation for each product.

        Uses formula:
            target = base_stock + safety_stock
            base_stock = daily_velocity × lead_time
            safety_stock = std_dev × Z_SCORE × √lead_time

        If total exceeds capacity, scale down proportionally.

        Optimized: Uses batch query for sales instead of N queries.

        Returns:
            Tuple of (allocations list, scale_factor)
        """
        logger.info("calculating_warehouse_allocations")

        # Get all active products (1 query)
        products, _ = self.product_service.get_all(
            page=1,
            page_size=1000,
            active_only=True
        )

        # Get recent sales for ALL products in ONE query (was N queries!)
        sales_by_product = self.sales_service.get_recent_sales_all(
            weeks=self.sales_weeks
        )

        allocations = []
        lead_time_sqrt = Decimal(str(sqrt(self.lead_time)))

        for product in products:
            # Get pre-fetched sales history for this product
            sales_history = sales_by_product.get(product.id, [])

            weeks_of_data = len(sales_history)

            if weeks_of_data == 0:
                # No sales data — skip allocation
                allocations.append(ProductAllocation(
                    product_id=product.id,
                    sku=product.sku,
                    category=product.category.value if product.category else None,
                    rotation=product.rotation.value if product.rotation else None,
                    daily_velocity=Decimal("0"),
                    weekly_velocity=Decimal("0"),
                    velocity_std_dev=Decimal("0"),
                    weeks_of_data=0,
                    base_stock_m2=Decimal("0"),
                    safety_stock_m2=Decimal("0"),
                    target_m2=Decimal("0"),
                    target_pallets=Decimal("0"),
                ))
                continue

            # Calculate weekly velocity and std_dev
            weekly_sales = [Decimal(str(s.quantity_m2)) for s in sales_history]
            total_sales = sum(weekly_sales)
            weekly_velocity = total_sales / weeks_of_data
            daily_velocity = weekly_velocity / 7

            # Standard deviation of weekly sales
            if weeks_of_data > 1:
                mean = weekly_velocity
                variance = sum((x - mean) ** 2 for x in weekly_sales) / weeks_of_data
                std_dev = Decimal(str(sqrt(float(variance))))
            else:
                std_dev = Decimal("0")

            # Calculate target stock
            base_stock = daily_velocity * self.lead_time
            safety_stock = std_dev * Z_SCORE * lead_time_sqrt
            target_m2 = base_stock + safety_stock
            target_pallets = target_m2 / M2_PER_PALLET

            allocations.append(ProductAllocation(
                product_id=product.id,
                sku=product.sku,
                category=product.category.value if product.category else None,
                rotation=product.rotation.value if product.rotation else None,
                daily_velocity=round(daily_velocity, 2),
                weekly_velocity=round(weekly_velocity, 2),
                velocity_std_dev=round(std_dev, 2),
                weeks_of_data=weeks_of_data,
                base_stock_m2=round(base_stock, 2),
                safety_stock_m2=round(safety_stock, 2),
                target_m2=round(target_m2, 2),
                target_pallets=round(target_pallets, 2),
            ))

        # Check if total exceeds capacity
        total_target_pallets = sum(a.target_pallets for a in allocations)
        capacity = Decimal(str(self.warehouse_capacity_pallets))

        if total_target_pallets > capacity:
            # Scale down proportionally
            scale_factor = capacity / total_target_pallets
            logger.info(
                "scaling_allocations",
                total_target=float(total_target_pallets),
                capacity=self.warehouse_capacity_pallets,
                scale_factor=float(scale_factor)
            )

            for alloc in allocations:
                alloc.scaled_target_pallets = round(alloc.target_pallets * scale_factor, 2)
                alloc.scaled_target_m2 = round(alloc.scaled_target_pallets * M2_PER_PALLET, 2)
                alloc.scale_factor = round(scale_factor, 4)
        else:
            scale_factor = Decimal("1")

        logger.info(
            "allocations_calculated",
            products=len(allocations),
            total_pallets=float(total_target_pallets),
            scale_factor=float(scale_factor)
        )

        return allocations, scale_factor

    def get_recommendations(self) -> OrderRecommendations:
        """
        Generate order recommendations for all products.

        Compares current stock against target allocation and
        recommends orders for products below target.

        Returns:
            OrderRecommendations with all recommendations and warnings
        """
        logger.info("generating_order_recommendations")

        # Step 1: Get allocations
        allocations, scale_factor = self.allocate_warehouse_slots()
        allocation_map = {a.product_id: a for a in allocations}

        # Step 2: Get current stockout status for all products
        stockout_summary = self.stockout_service.calculate_all()
        stockout_map = {s.product_id: s for s in stockout_summary.products}

        # Step 3: Get latest inventory for all products
        inventory_snapshots = self.inventory_service.get_latest()
        inventory_map = {inv.product_id: inv for inv in inventory_snapshots}

        # Step 4: Get boat arrival info for coverage gap calculation
        next_boat_arrival, _ = self.boat_service.get_next_two_arrivals()
        today = date.today()

        if next_boat_arrival:
            days_to_cover = (next_boat_arrival - today).days
        else:
            # Fallback to lead time if no boat scheduled
            days_to_cover = self.lead_time
            next_boat_arrival = None

        # Step 5: Get sales data for confidence calculation
        sales_by_product = self.sales_service.get_recent_sales_all(weeks=self.sales_weeks)

        # Step 6: Get customer analysis for all products (for confidence calculation)
        product_ids = [a.product_id for a in allocations if a.weeks_of_data > 0]
        customer_analysis_map = self.sales_service.get_customer_analysis_batch(
            product_ids=product_ids,
            weeks=self.sales_weeks
        )

        recommendations = []
        warnings = []
        order_arrives = today + timedelta(days=self.lead_time)

        for alloc in allocations:
            stockout = stockout_map.get(alloc.product_id)
            inventory = inventory_map.get(alloc.product_id)

            # Get effective target (scaled if necessary)
            target_pallets = alloc.scaled_target_pallets or alloc.target_pallets
            target_m2 = alloc.scaled_target_m2 or alloc.target_m2

            # Skip products with no sales data
            if alloc.weeks_of_data == 0:
                warnings.append(RecommendationWarning(
                    product_id=alloc.product_id,
                    sku=alloc.sku,
                    type=WarningType.NO_SALES_DATA,
                    action_type=ActionType.REVIEW,
                    message=f"No sales history available — cannot calculate allocation",
                ))
                continue

            # Flag low volume products (< min records) but still calculate
            if alloc.weeks_of_data < self.low_volume_min_records:
                warnings.append(RecommendationWarning(
                    product_id=alloc.product_id,
                    sku=alloc.sku,
                    type=WarningType.LOW_VELOCITY,
                    action_type=ActionType.REVIEW,
                    message=f"Low volume — only {alloc.weeks_of_data} week(s) of sales data",
                    details={"weeks_of_data": alloc.weeks_of_data}
                ))

            # Get current inventory
            if inventory:
                warehouse_m2 = Decimal(str(inventory.warehouse_qty))
                in_transit_m2 = Decimal(str(inventory.in_transit_qty))
            else:
                warehouse_m2 = Decimal("0")
                in_transit_m2 = Decimal("0")

            warehouse_pallets = warehouse_m2 / M2_PER_PALLET
            in_transit_pallets = in_transit_m2 / M2_PER_PALLET
            current_pallets = warehouse_pallets + in_transit_pallets
            current_m2 = warehouse_m2 + in_transit_m2

            # Calculate gap
            gap_pallets = target_pallets - current_pallets
            gap_m2 = gap_pallets * M2_PER_PALLET

            # Check if over-stocked
            if gap_pallets < OVER_STOCK_THRESHOLD:
                warnings.append(RecommendationWarning(
                    product_id=alloc.product_id,
                    sku=alloc.sku,
                    type=WarningType.OVER_STOCKED,
                    action_type=ActionType.SKIP_ORDER,
                    message=f"{abs(int(gap_pallets))} pallets above target — skip this cycle",
                    details={
                        "current_pallets": float(current_pallets),
                        "target_pallets": float(target_pallets),
                        "excess_pallets": float(abs(gap_pallets)),
                    }
                ))
                continue

            # Get timing info from stockout
            days_until_empty = None
            stockout_date = None
            arrives_before_stockout = True

            if stockout and stockout.days_to_stockout is not None:
                days_until_empty = stockout.days_to_stockout
                stockout_date = today + timedelta(days=int(days_until_empty))
                arrives_before_stockout = order_arrives < stockout_date

            # Get health status
            health_status = stockout.status if stockout else StockoutStatus.YOUR_CALL

            # Determine priority (based on boat arrivals via stockout status)
            priority = self._determine_priority(
                stockout_status=health_status,
            )

            # Determine action type based on health status
            action_type = self._determine_action_type(health_status, gap_pallets)

            # Move WELL_STOCKED to warnings (Skip This Cycle)
            if action_type == ActionType.WELL_STOCKED:
                warnings.append(RecommendationWarning(
                    product_id=alloc.product_id,
                    sku=alloc.sku,
                    type=WarningType.WELL_STOCKED,
                    action_type=ActionType.WELL_STOCKED,
                    message=f"Well stocked — {int(days_until_empty) if days_until_empty else '?'} days of inventory",
                    details={
                        "current_pallets": float(current_pallets),
                        "target_pallets": float(target_pallets),
                        "days_until_empty": float(days_until_empty) if days_until_empty else None,
                    }
                ))
                continue

            # Generate action message based on action type
            action, reason = self._generate_action_message(
                action_type=action_type,
                gap_pallets=gap_pallets,
                gap_m2=gap_m2,
                days_until_empty=days_until_empty,
                arrives_before_stockout=arrives_before_stockout,
                rotation=alloc.rotation,
                sku=alloc.sku,
            )

            # Calculate coverage gap (demand until next boat - available)
            total_demand_m2, coverage_gap_m2, coverage_gap_pallets = self._calculate_coverage_gap(
                daily_velocity=alloc.daily_velocity,
                available_m2=current_m2,
                days_to_cover=days_to_cover,
            )

            # Calculate confidence score (using customer data)
            product_sales = sales_by_product.get(alloc.product_id, [])
            weekly_quantities = [s.quantity_m2 for s in product_sales]
            customer_analysis = customer_analysis_map.get(alloc.product_id)
            confidence, confidence_reason, velocity_cv, customer_metrics = self._calculate_confidence(
                weekly_sales=weekly_quantities,
                weeks_of_data=alloc.weeks_of_data,
                customer_analysis=customer_analysis,
            )

            recommendations.append(ProductRecommendation(
                product_id=alloc.product_id,
                sku=alloc.sku,
                category=alloc.category,
                rotation=alloc.rotation,
                target_pallets=round(target_pallets, 2),
                target_m2=round(target_m2, 2),
                warehouse_pallets=round(warehouse_pallets, 2),
                warehouse_m2=round(warehouse_m2, 2),
                in_transit_pallets=round(in_transit_pallets, 2),
                in_transit_m2=round(in_transit_m2, 2),
                current_pallets=round(current_pallets, 2),
                current_m2=round(current_m2, 2),
                gap_pallets=round(gap_pallets, 2),
                gap_m2=round(gap_m2, 2),
                # Coverage gap fields
                days_to_cover=days_to_cover,
                total_demand_m2=total_demand_m2,
                coverage_gap_m2=coverage_gap_m2,
                coverage_gap_pallets=coverage_gap_pallets,
                # Timing
                daily_velocity=alloc.daily_velocity,
                days_until_empty=round(days_until_empty, 1) if days_until_empty else None,
                stockout_date=stockout_date,
                order_arrives_date=order_arrives,
                arrives_before_stockout=arrives_before_stockout,
                # Confidence fields
                confidence=confidence,
                confidence_reason=confidence_reason,
                weeks_of_data=alloc.weeks_of_data,
                velocity_cv=velocity_cv,
                # Customer analysis fields
                unique_customers=customer_metrics.get("unique_customers", 0),
                top_customer_name=customer_metrics.get("top_customer_name"),
                top_customer_share=customer_metrics.get("top_customer_share"),
                recurring_customers=customer_metrics.get("recurring_customers", 0),
                recurring_share=customer_metrics.get("recurring_share"),
                # Priority and action
                priority=priority,
                action_type=action_type,
                action=action,
                reason=reason,
            ))

        # Sort by action urgency, then alphabetically by SKU
        # WELL_STOCKED comes last (no action needed)
        action_order = {
            ActionType.ORDER_NOW: 0,
            ActionType.ORDER_SOON: 1,
            ActionType.SKIP_ORDER: 2,
            ActionType.REVIEW: 3,
            ActionType.WELL_STOCKED: 4,
        }
        recommendations.sort(key=lambda r: (
            action_order.get(r.action_type, 5),
            r.sku
        ))

        # Calculate totals
        total_allocated = sum(
            (a.scaled_target_pallets or a.target_pallets)
            for a in allocations
        )
        # Calculate warehouse stock and in-transit separately
        # Utilization is warehouse-only (matches Dashboard)
        total_warehouse_m2 = sum(
            Decimal(str(inv.warehouse_qty))
            for inv in inventory_snapshots
        )
        total_in_transit_m2 = sum(
            Decimal(str(inv.in_transit_qty))
            for inv in inventory_snapshots
        )
        total_current = total_warehouse_m2 / M2_PER_PALLET  # Warehouse only for utilization
        total_in_transit = total_in_transit_m2 / M2_PER_PALLET
        total_recommended_pallets = sum(r.gap_pallets for r in recommendations)
        total_recommended_m2 = sum(r.gap_m2 for r in recommendations)

        # Calculate coverage gap totals (only positive gaps = need to order)
        total_coverage_gap_pallets = sum(
            r.coverage_gap_pallets for r in recommendations
            if r.coverage_gap_pallets and r.coverage_gap_pallets > 0
        )
        total_coverage_gap_m2 = sum(
            r.coverage_gap_m2 for r in recommendations
            if r.coverage_gap_m2 and r.coverage_gap_m2 > 0
        )

        # Count by priority (boat-based)
        high_priority_count = sum(1 for r in recommendations if r.priority == RecommendationPriority.HIGH_PRIORITY)
        consider_count = sum(1 for r in recommendations if r.priority == RecommendationPriority.CONSIDER)
        well_covered_count = sum(1 for r in recommendations if r.priority == RecommendationPriority.WELL_COVERED)
        your_call_count = sum(1 for r in recommendations if r.priority == RecommendationPriority.YOUR_CALL)
        # Add YOUR_CALL warnings too
        your_call_count += sum(1 for w in warnings if w.action_type == ActionType.REVIEW)

        # Count by action type (recommendations + warnings)
        action_counts = {a: 0 for a in ActionType}
        for rec in recommendations:
            action_counts[rec.action_type] += 1
        for warn in warnings:
            action_counts[warn.action_type] += 1

        # Build warehouse status
        warehouse_status = WarehouseStatus(
            total_capacity_pallets=self.warehouse_capacity_pallets,
            total_capacity_m2=self.warehouse_capacity_m2,
            total_allocated_pallets=round(total_allocated, 2),
            total_allocated_m2=round(total_allocated * M2_PER_PALLET, 2),
            total_current_pallets=round(total_current, 2),
            total_current_m2=round(total_warehouse_m2, 2),
            total_in_transit_pallets=round(total_in_transit, 2),
            total_in_transit_m2=round(total_in_transit_m2, 2),
            utilization_percent=round(
                (total_current / Decimal(str(self.warehouse_capacity_pallets))) * 100, 1
            ) if self.warehouse_capacity_pallets > 0 else Decimal("0"),
            allocation_scaled=scale_factor < 1,
            scale_factor=round(scale_factor, 4) if scale_factor < 1 else None,
        )

        result = OrderRecommendations(
            warehouse_status=warehouse_status,
            lead_time_days=self.lead_time,
            calculation_date=today,
            # Boat arrival info
            next_boat_arrival=next_boat_arrival,
            days_to_next_boat=days_to_cover if next_boat_arrival else None,
            # Recommendations
            recommendations=recommendations,
            total_recommended_pallets=round(total_recommended_pallets, 2),
            total_recommended_m2=round(total_recommended_m2, 2),
            # Coverage gap totals
            total_coverage_gap_pallets=total_coverage_gap_pallets,
            total_coverage_gap_m2=round(total_coverage_gap_m2, 2) if total_coverage_gap_m2 else Decimal("0"),
            warnings=warnings,
            # Priority counts (boat-based)
            high_priority_count=high_priority_count,
            consider_count=consider_count,
            well_covered_count=well_covered_count,
            your_call_count=your_call_count,
            # Action counts
            order_now_count=action_counts.get(ActionType.ORDER_NOW, 0),
            order_soon_count=action_counts.get(ActionType.ORDER_SOON, 0),
            well_stocked_count=action_counts.get(ActionType.WELL_STOCKED, 0),
            skip_order_count=action_counts.get(ActionType.SKIP_ORDER, 0),
            review_count=action_counts.get(ActionType.REVIEW, 0),
        )

        logger.info(
            "recommendations_generated",
            total_recommendations=len(recommendations),
            total_warnings=len(warnings),
            high_priority=high_priority_count,
            consider=consider_count,
            well_covered=well_covered_count,
            your_call=your_call_count,
        )

        return result

    def _determine_priority(
        self,
        stockout_status: StockoutStatus,
    ) -> RecommendationPriority:
        """
        Determine recommendation priority based on stockout status.

        Priority is now tied to boat arrivals (via stockout status):
        - HIGH_PRIORITY: stockout before next boat
        - CONSIDER: stockout before second boat
        - WELL_COVERED: won't stock out for 2+ boat cycles
        - YOUR_CALL: no data / needs review
        """
        if stockout_status == StockoutStatus.HIGH_PRIORITY:
            return RecommendationPriority.HIGH_PRIORITY
        if stockout_status == StockoutStatus.CONSIDER:
            return RecommendationPriority.CONSIDER
        if stockout_status == StockoutStatus.WELL_COVERED:
            return RecommendationPriority.WELL_COVERED
        return RecommendationPriority.YOUR_CALL

    def _generate_action_message(
        self,
        action_type: ActionType,
        gap_pallets: Decimal,
        gap_m2: Decimal,
        days_until_empty: Optional[Decimal],
        arrives_before_stockout: bool,
        rotation: Optional[str],
        sku: str,
    ) -> tuple[str, str]:
        """
        Generate action message and reason based on action type.

        Returns:
            Tuple of (action, reason)
        """
        if action_type == ActionType.ORDER_NOW:
            pallets_needed = max(int(gap_pallets), 1)
            action = f"{pallets_needed} pallets needed — order immediately"
            reason = self._build_reason(
                days_until_empty=days_until_empty,
                arrives_before_stockout=arrives_before_stockout,
                rotation=rotation,
                gap_pallets=gap_pallets,
            )

        elif action_type == ActionType.ORDER_SOON:
            pallets_needed = max(int(gap_pallets), 1)
            action = f"{pallets_needed} pallets needed — plan for next cycle"
            reason = self._build_reason(
                days_until_empty=days_until_empty,
                arrives_before_stockout=arrives_before_stockout,
                rotation=rotation,
                gap_pallets=gap_pallets,
            )

        elif action_type == ActionType.WELL_STOCKED:
            weeks_of_stock = int(days_until_empty / 7) if days_until_empty else 0
            action = f"{weeks_of_stock} weeks of inventory — no action needed"
            reason = "Stock levels are healthy."

        elif action_type == ActionType.SKIP_ORDER:
            excess = abs(int(gap_pallets))
            action = f"{excess} pallets above target — skip this cycle"
            reason = "Excess inventory relative to target allocation."

        elif action_type == ActionType.REVIEW:
            action = "Needs manual review — limited sales data"
            reason = "Insufficient sales history for automated recommendation."

        else:
            action = "Unknown action"
            reason = "Unable to determine recommendation."

        return action, reason

    def _build_reason(
        self,
        days_until_empty: Optional[Decimal],
        arrives_before_stockout: bool,
        rotation: Optional[str],
        gap_pallets: Decimal,
    ) -> str:
        """Build reason string for ORDER_NOW/ORDER_SOON actions."""
        reasons = []

        if not arrives_before_stockout and days_until_empty is not None:
            reasons.append(f"Stockout in {int(days_until_empty)} days — order arrives too late")
        elif days_until_empty is not None and days_until_empty < self.lead_time:
            reasons.append(f"Only {int(days_until_empty)} days of stock remaining")

        if gap_pallets > 0:
            reasons.append(f"{int(gap_pallets)} pallets below target allocation")

        if rotation == "ALTA":
            reasons.append("High rotation product")
        elif rotation == "MEDIA-ALTA":
            reasons.append("Medium-high rotation product")

        return ". ".join(reasons) + "." if reasons else "Stock replenishment needed."

    def _determine_action_type(
        self,
        health_status: StockoutStatus,
        gap_pallets: Decimal
    ) -> ActionType:
        """
        Determine action type based on health status and gap.

        Mapping (boat-based):
        - HIGH_PRIORITY → ORDER_NOW
        - CONSIDER → ORDER_SOON
        - WELL_COVERED with excess (>5 pallets over) → SKIP_ORDER
        - WELL_COVERED at/near target → WELL_STOCKED
        - YOUR_CALL → REVIEW
        """
        if health_status == StockoutStatus.HIGH_PRIORITY:
            return ActionType.ORDER_NOW

        if health_status == StockoutStatus.CONSIDER:
            return ActionType.ORDER_SOON

        if health_status == StockoutStatus.WELL_COVERED:
            if gap_pallets < OVER_STOCK_THRESHOLD:  # -5 = excess inventory
                return ActionType.SKIP_ORDER
            return ActionType.WELL_STOCKED

        # YOUR_CALL or any unknown status
        return ActionType.REVIEW

    def _calculate_coverage_gap(
        self,
        daily_velocity: Decimal,
        available_m2: Decimal,
        days_to_cover: int,
    ) -> tuple[Decimal, int]:
        """
        Calculate coverage gap: how much stock needed to survive until next boat.

        Args:
            daily_velocity: Average daily sales (m²)
            available_m2: Current stock + in-transit (m²)
            days_to_cover: Days until next boat arrives

        Returns:
            Tuple of (coverage_gap_m2, coverage_gap_pallets)
            Positive = need to order, Negative/Zero = have buffer
        """
        # Total demand during coverage period
        total_demand_m2 = daily_velocity * days_to_cover

        # Gap = demand - available
        coverage_gap_m2 = total_demand_m2 - available_m2

        # Convert to pallets (only if positive, otherwise 0)
        if coverage_gap_m2 > 0:
            import math
            coverage_gap_pallets = math.ceil(float(coverage_gap_m2) / float(M2_PER_PALLET))
        else:
            coverage_gap_pallets = 0

        return round(total_demand_m2, 2), round(coverage_gap_m2, 2), coverage_gap_pallets

    def _calculate_confidence(
        self,
        weekly_sales: list,
        weeks_of_data: int,
        customer_analysis: Optional[dict] = None,
    ) -> tuple[ConfidenceLevel, str, Decimal, dict]:
        """
        Calculate confidence in velocity estimate using customer data.

        Args:
            weekly_sales: List of weekly sales quantities (Decimal)
            weeks_of_data: Number of weeks of data
            customer_analysis: Customer breakdown from sales_service.get_customer_analysis_batch()

        Returns:
            Tuple of (confidence_level, reason, coefficient_of_variation, customer_metrics)
        """
        # Default customer metrics
        customer_metrics = {
            "unique_customers": 0,
            "top_customer_name": None,
            "top_customer_share": None,
            "recurring_customers": 0,
            "recurring_share": None,
        }

        # No data case
        if weeks_of_data == 0 or not weekly_sales:
            return ConfidenceLevel.LOW, "No sales data", None, customer_metrics

        # Calculate coefficient of variation
        quantities = [Decimal(str(s)) for s in weekly_sales]
        avg = sum(quantities) / len(quantities)

        if len(quantities) > 1 and avg > 0:
            variance = sum((x - avg) ** 2 for x in quantities) / len(quantities)
            std_dev = Decimal(str(sqrt(float(variance))))
            cv = std_dev / avg
        else:
            cv = Decimal("0")

        # Extract customer metrics if available
        if customer_analysis:
            customer_metrics = {
                "unique_customers": customer_analysis.get("unique_customers", 0),
                "top_customer_name": customer_analysis.get("top_customer_name"),
                "top_customer_share": customer_analysis.get("top_customer_share"),
                "recurring_customers": customer_analysis.get("recurring_count", 0),
                "recurring_share": customer_analysis.get("recurring_share"),
            }

        unique_customers = customer_metrics["unique_customers"]
        top_customer_share = customer_metrics.get("top_customer_share") or Decimal("0")
        recurring_share = customer_metrics.get("recurring_share") or Decimal("0")
        top_customer_name = customer_metrics.get("top_customer_name")

        # Check recent activity (last 4 weeks)
        recent_count = min(4, len(quantities))
        recent_sum = sum(quantities[:recent_count])
        has_recent_sales = recent_sum > 0

        # Confidence rules (customer-based takes priority)
        # Rule 1: Too few weeks of data
        if weeks_of_data < 4:
            return ConfidenceLevel.LOW, f"Only {weeks_of_data} weeks of data", round(cv, 2), customer_metrics

        # Rule 2: No recent sales
        if not has_recent_sales:
            return ConfidenceLevel.LOW, "No sales in last 4 weeks", round(cv, 2), customer_metrics

        # Rule 3: Top customer dominates (>70% of sales)
        if top_customer_share > Decimal("0.7"):
            pct = int(float(top_customer_share) * 100)
            customer_display = top_customer_name or "1 customer"
            return ConfidenceLevel.LOW, f"{pct}% from {customer_display}", round(cv, 2), customer_metrics

        # Rule 4: Single customer
        if unique_customers == 1 and customer_analysis:
            customer_display = top_customer_name or "single customer"
            return ConfidenceLevel.LOW, f"Single customer: {customer_display}", round(cv, 2), customer_metrics

        # Rule 5: Top customer is significant (>50%)
        if top_customer_share > Decimal("0.5"):
            pct = int(float(top_customer_share) * 100)
            customer_display = top_customer_name or "top customer"
            return ConfidenceLevel.MEDIUM, f"{pct}% from {customer_display}", round(cv, 2), customer_metrics

        # Rule 6: Too few customers
        if unique_customers < 3 and unique_customers > 0 and customer_analysis:
            return ConfidenceLevel.MEDIUM, f"Only {unique_customers} customers", round(cv, 2), customer_metrics

        # Rule 7: Good recurring customer base (>70% from recurring)
        if recurring_share > Decimal("0.7") and unique_customers >= 3:
            recurring_count = customer_metrics.get("recurring_customers", 0)
            return ConfidenceLevel.HIGH, f"{unique_customers} customers, {recurring_count} recurring", round(cv, 2), customer_metrics

        # Rule 8: Limited data history
        if weeks_of_data < 8:
            return ConfidenceLevel.MEDIUM, f"Limited history ({weeks_of_data} weeks)", round(cv, 2), customer_metrics

        # Rule 9: Erratic sales pattern (high CV)
        if cv > Decimal("0.8"):
            return ConfidenceLevel.LOW, "Erratic sales pattern", round(cv, 2), customer_metrics

        if cv > Decimal("0.5"):
            return ConfidenceLevel.MEDIUM, "Variable sales pattern", round(cv, 2), customer_metrics

        # Rule 10: Good diverse customer base
        if unique_customers >= 3:
            return ConfidenceLevel.HIGH, f"{unique_customers} customers, stable demand", round(cv, 2), customer_metrics

        # Default: MEDIUM with data quality note
        return ConfidenceLevel.MEDIUM, f"{weeks_of_data} weeks history", round(cv, 2), customer_metrics

    def get_allocation_details(self) -> list[ProductAllocation]:
        """Get detailed allocation breakdown for all products."""
        allocations, _ = self.allocate_warehouse_slots()
        return allocations


# Singleton instance
_recommendation_service: Optional[RecommendationService] = None


def get_recommendation_service() -> RecommendationService:
    """Get or create RecommendationService instance."""
    global _recommendation_service
    if _recommendation_service is None:
        _recommendation_service = RecommendationService()
    return _recommendation_service
