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
    WarningType,
    WarehouseStatus,
    OrderRecommendations,
)

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

        # Settings (from environment config, not database)
        self.lead_time = settings.lead_time_days  # 45 days
        self.warehouse_capacity_pallets = settings.warehouse_max_pallets  # 740
        self.warehouse_capacity_m2 = Decimal(str(self.warehouse_capacity_pallets)) * M2_PER_PALLET
        self.sales_weeks = 4  # Weeks of sales data for velocity/std_dev

    def allocate_warehouse_slots(self) -> tuple[list[ProductAllocation], Decimal]:
        """
        Calculate target warehouse allocation for each product.

        Uses formula:
            target = base_stock + safety_stock
            base_stock = daily_velocity × lead_time
            safety_stock = std_dev × Z_SCORE × √lead_time

        If total exceeds capacity, scale down proportionally.

        Returns:
            Tuple of (allocations list, scale_factor)
        """
        logger.info("calculating_warehouse_allocations")

        # Get all active products
        products, _ = self.product_service.get_all(
            page=1,
            page_size=1000,
            active_only=True
        )

        allocations = []
        lead_time_sqrt = Decimal(str(sqrt(self.lead_time)))

        for product in products:
            # Get sales history (last 4 weeks)
            sales_history = self.sales_service.get_history(
                product.id,
                limit=self.sales_weeks
            )

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

        recommendations = []
        warnings = []
        today = date.today()
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
                    message=f"No sales history available — cannot calculate allocation",
                ))
                continue

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
                    message=f"{abs(int(gap_pallets))} pallets over target — do not order",
                    details={
                        "current_pallets": float(current_pallets),
                        "target_pallets": float(target_pallets),
                        "excess_pallets": float(abs(gap_pallets)),
                    }
                ))
                continue

            # No order needed if at or above target
            if gap_pallets <= 0:
                continue

            # Get timing info from stockout
            days_until_empty = None
            stockout_date = None
            arrives_before_stockout = True

            if stockout and stockout.days_to_stockout is not None:
                days_until_empty = stockout.days_to_stockout
                stockout_date = today + timedelta(days=int(days_until_empty))
                arrives_before_stockout = order_arrives < stockout_date

            # Determine priority
            priority = self._determine_priority(
                stockout_status=stockout.status if stockout else StockoutStatus.NO_SALES,
                arrives_before_stockout=arrives_before_stockout,
                rotation=alloc.rotation
            )

            # Generate action and reason
            action = f"Order {int(gap_pallets)} pallets ({int(gap_m2):,} m²) of {alloc.sku}"
            reason = self._generate_reason(
                gap_pallets=gap_pallets,
                days_until_empty=days_until_empty,
                arrives_before_stockout=arrives_before_stockout,
                rotation=alloc.rotation
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
                daily_velocity=alloc.daily_velocity,
                days_until_empty=round(days_until_empty, 1) if days_until_empty else None,
                stockout_date=stockout_date,
                order_arrives_date=order_arrives,
                arrives_before_stockout=arrives_before_stockout,
                priority=priority,
                action=action,
                reason=reason,
            ))

        # Sort by priority, then by days_until_empty
        priority_order = {
            RecommendationPriority.CRITICAL: 0,
            RecommendationPriority.HIGH: 1,
            RecommendationPriority.MEDIUM: 2,
            RecommendationPriority.LOW: 3,
        }
        recommendations.sort(key=lambda r: (
            priority_order[r.priority],
            r.days_until_empty if r.days_until_empty is not None else 9999
        ))

        # Calculate totals
        total_allocated = sum(
            (a.scaled_target_pallets or a.target_pallets)
            for a in allocations
        )
        total_current = sum(r.current_pallets for r in recommendations)
        total_recommended_pallets = sum(r.gap_pallets for r in recommendations)
        total_recommended_m2 = sum(r.gap_m2 for r in recommendations)

        # Count by priority
        priority_counts = {p: 0 for p in RecommendationPriority}
        for rec in recommendations:
            priority_counts[rec.priority] += 1

        # Build warehouse status
        warehouse_status = WarehouseStatus(
            total_capacity_pallets=self.warehouse_capacity_pallets,
            total_capacity_m2=self.warehouse_capacity_m2,
            total_allocated_pallets=round(total_allocated, 2),
            total_allocated_m2=round(total_allocated * M2_PER_PALLET, 2),
            total_current_pallets=round(total_current, 2),
            total_current_m2=round(total_current * M2_PER_PALLET, 2),
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
            recommendations=recommendations,
            total_recommended_pallets=round(total_recommended_pallets, 2),
            total_recommended_m2=round(total_recommended_m2, 2),
            warnings=warnings,
            critical_count=priority_counts[RecommendationPriority.CRITICAL],
            high_count=priority_counts[RecommendationPriority.HIGH],
            medium_count=priority_counts[RecommendationPriority.MEDIUM],
            low_count=priority_counts[RecommendationPriority.LOW],
        )

        logger.info(
            "recommendations_generated",
            total_recommendations=len(recommendations),
            total_warnings=len(warnings),
            critical=result.critical_count,
            high=result.high_count,
            medium=result.medium_count,
            low=result.low_count,
        )

        return result

    def _determine_priority(
        self,
        stockout_status: StockoutStatus,
        arrives_before_stockout: bool,
        rotation: Optional[str]
    ) -> RecommendationPriority:
        """Determine recommendation priority based on urgency and rotation."""

        # Critical if stockout imminent or order arrives too late
        if stockout_status == StockoutStatus.CRITICAL or not arrives_before_stockout:
            return RecommendationPriority.CRITICAL

        # High for ALTA rotation products
        if rotation == "ALTA":
            return RecommendationPriority.HIGH

        # Medium for MEDIA-ALTA rotation
        if rotation == "MEDIA-ALTA":
            return RecommendationPriority.MEDIUM

        # Low for everything else
        return RecommendationPriority.LOW

    def _generate_reason(
        self,
        gap_pallets: Decimal,
        days_until_empty: Optional[Decimal],
        arrives_before_stockout: bool,
        rotation: Optional[str]
    ) -> str:
        """Generate human-readable reason for recommendation."""

        reasons = []

        if not arrives_before_stockout and days_until_empty is not None:
            reasons.append(f"Stockout in {int(days_until_empty)} days — order arrives too late")
        elif days_until_empty is not None and days_until_empty < self.lead_time:
            reasons.append(f"Only {int(days_until_empty)} days of stock remaining")

        reasons.append(f"{int(gap_pallets)} pallets below target allocation")

        if rotation == "ALTA":
            reasons.append("High rotation product")
        elif rotation == "MEDIA-ALTA":
            reasons.append("Medium-high rotation product")

        return ". ".join(reasons) + "."

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
