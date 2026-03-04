from typing import Optional
from decimal import Decimal
from datetime import date
import math
import structlog

from models.order_builder import (
    OrderBuilderProduct,
    OrderBuilderBoat,
    OrderBuilderAlert,
    OrderBuilderAlertType,
    OrderBuilderSummary,
    ConstraintAnalysis,
    OrderSummaryReasoning,
    OrderReasoning,
    StockAnalysis,
    DemandAnalysis,
    QuantityReasoning,
    ProductReasoning,
    UnableToShipItem,
    UnableToShipSummary,
    ExcludedProduct,
    PrimaryFactor,
)
from services.order_builder.constants import PALLETS_PER_CONTAINER, WAREHOUSE_CAPACITY
from config.shipping import M2_PER_PALLET, CONTAINER_MAX_WEIGHT_KG

logger = structlog.get_logger(__name__)


class AlertsMixin:
    """Alert generation, reasoning, and unable-to-ship analysis."""

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

        # 8. Order deadline warning (recommended date to place factory order)
        if boat.past_order_deadline:
            alerts.insert(0, OrderBuilderAlert(
                type=OrderBuilderAlertType.WARNING,
                icon="⏰",
                message=f"Past recommended order deadline ({abs(boat.days_until_order_deadline)}d ago). Order soon to catch this boat."
            ))
        elif 0 < boat.days_until_order_deadline <= 7:
            alerts.insert(0, OrderBuilderAlert(
                type=OrderBuilderAlertType.WARNING,
                icon="⏰",
                message=f"Order deadline in {boat.days_until_order_deadline} days!"
            ))

        return alerts

    def _generate_summary_reasoning(
        self,
        all_products: list[OrderBuilderProduct],
        boat: OrderBuilderBoat,
        summary: OrderBuilderSummary,
        constraint_analysis: Optional[ConstraintAnalysis] = None
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

        # Insight 1: Stockout risk count (cascade-aware)
        # Use priority + suggested_pallets as source of truth.
        # Products whose gaps are already covered by earlier boats
        # have suggested_pallets=0 and priority=WELL_COVERED — excluded here.
        stockout_risk_count = sum(
            1 for p in all_products
            if p.priority in ("HIGH_PRIORITY", "CONSIDER")
            and p.suggested_pallets > 0
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

        # Generate structured reasoning narrative
        reasoning = self._generate_order_reasoning(
            all_products=all_products,
            boat=boat,
            summary=summary,
            constraint_analysis=constraint_analysis,
            critical_count=critical_count,
            stockout_risk_count=stockout_risk_count,
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
            key_insights=key_insights[:5],  # Top 5 insights (legacy)
            excluded_products=excluded_products[:10],  # Top 10 excluded
            reasoning=reasoning,
        )

    def _generate_order_reasoning(
        self,
        all_products: list[OrderBuilderProduct],
        boat: OrderBuilderBoat,
        summary: OrderBuilderSummary,
        constraint_analysis: Optional[ConstraintAnalysis],
        critical_count: int,
        stockout_risk_count: int,
    ) -> OrderReasoning:
        """
        Generate structured reasoning narrative with 4 sentences.

        Template-based approach for consistent, translatable output.
        """
        boat_date_str = boat.departure_date.strftime("%b %d")  # e.g., "Feb 15"

        # === STRATEGY SENTENCE ===
        # Why are we ordering? Include actionable context about shippability.
        if stockout_risk_count > 0:
            # Count how many at-risk products can actually ship (have SIESA stock)
            # Uses same cascade-aware criteria as stockout_risk_count above.
            at_risk = [
                p for p in all_products
                if p.priority in ("HIGH_PRIORITY", "CONSIDER")
                and p.suggested_pallets > 0
            ]
            shippable = [p for p in at_risk if float(p.factory_available_m2 or 0) > 0]
            unshippable = [p for p in at_risk if float(p.factory_available_m2 or 0) <= 0]

            if shippable and unshippable:
                skus_no_stock = ", ".join(p.sku for p in unshippable[:3])
                strategy_sentence = (
                    f"{stockout_risk_count} products at stockout risk. "
                    f"{len(shippable)} can ship now, {len(unshippable)} have no SIESA stock "
                    f"({skus_no_stock})."
                )
            elif unshippable and not shippable:
                skus_no_stock = ", ".join(p.sku for p in unshippable[:3])
                strategy_sentence = (
                    f"{stockout_risk_count} products at stockout risk but no SIESA stock to ship "
                    f"({skus_no_stock}). Check production schedule."
                )
            else:
                strategy_sentence = (
                    f"{stockout_risk_count} products at stockout risk — "
                    f"all have SIESA stock ready to ship."
                )
        elif critical_count > 0:
            strategy_sentence = (
                f"Addressing {critical_count} critical products "
                f"for the {boat_date_str} shipment."
            )
        else:
            strategy_sentence = (
                f"Replenishing inventory for the {boat_date_str} shipment."
            )

        # === RISK SENTENCE ===
        # What's the biggest risk?
        products_with_stock = [
            p for p in all_products
            if p.reasoning and p.reasoning.stock.days_of_stock is not None
        ]

        highest_risk_sku = None
        highest_risk_days = None

        if products_with_stock:
            most_critical = min(
                products_with_stock,
                key=lambda p: p.reasoning.stock.days_of_stock
            )
            highest_risk_sku = most_critical.sku
            highest_risk_days = int(most_critical.reasoning.stock.days_of_stock)

            if highest_risk_days <= 0:
                risk_sentence = f"{highest_risk_sku} is out of stock now."
            elif highest_risk_days < 7:
                risk_sentence = (
                    f"{highest_risk_sku} is most critical "
                    f"({highest_risk_days} days of stock)."
                )
            elif highest_risk_days < boat.days_until_warehouse:
                risk_sentence = (
                    f"{highest_risk_sku} will stockout before boat arrives "
                    f"({highest_risk_days} days vs {boat.days_until_warehouse} day lead time)."
                )
            else:
                risk_sentence = "All products have adequate coverage until boat arrives."
        else:
            risk_sentence = "Unable to assess risk due to insufficient sales data."

        # === CONSTRAINT SENTENCE ===
        # What's limiting the order?
        limiting_factor = "none"
        deferred_count = 0

        if constraint_analysis:
            limiting_factor = constraint_analysis.limiting_factor
            deferred_count = constraint_analysis.deferred_pallets

        if limiting_factor == "warehouse":
            constraint_sentence = (
                f"Warehouse space is the limiting factor. "
                f"{deferred_count} pallets deferred to next boat."
            )
        elif limiting_factor == "boat":
            constraint_sentence = (
                f"Boat capacity is the limiting factor. "
                f"{deferred_count} pallets deferred to next boat."
            )
        elif limiting_factor == "mode":
            constraint_sentence = (
                f"Mode limit reached. "
                f"Switch to Optimal mode to order {deferred_count} more pallets."
            )
        elif deferred_count > 0:
            constraint_sentence = f"{deferred_count} pallets deferred to next boat."
        else:
            constraint_sentence = "No constraints — all recommended products fit."

        # === CUSTOMER SENTENCE ===
        # Who's waiting? Count products with customer demand signals
        products_with_customer_demand = sum(
            1 for p in all_products
            if p.customers_expecting_count and p.customers_expecting_count > 0
        )

        if products_with_customer_demand >= 3:
            customer_sentence = (
                f"{products_with_customer_demand} products have customers expected to order soon "
                f"based on purchase patterns."
            )
        elif products_with_customer_demand > 0:
            customer_sentence = (
                f"{products_with_customer_demand} product(s) with customers expected to order soon."
            )
        else:
            customer_sentence = None  # No customer signal to report

        customers_expecting = products_with_customer_demand  # For the badge

        return OrderReasoning(
            strategy_sentence=strategy_sentence,
            risk_sentence=risk_sentence,
            constraint_sentence=constraint_sentence,
            customer_sentence=customer_sentence,
            limiting_factor=limiting_factor,
            deferred_count=deferred_count,
            customers_expecting=customers_expecting,
            critical_count=critical_count,
            highest_risk_sku=highest_risk_sku,
            highest_risk_days=highest_risk_days,
        )

    def _calculate_unable_to_ship_alerts(
        self,
        all_products: list[OrderBuilderProduct],
        order_deadline: date,
    ) -> UnableToShipSummary:
        """
        Find products that NEED to be ordered but CAN'T ship due to logistical issues.

        A product is "unable to ship" if:
        1. Has coverage gap > 0 (we need it)
        2. factory_available_m2 = 0 (nothing at SIESA)
        3. NOT production_completing_before_deadline (nothing coming)

        Args:
            all_products: All Order Builder products
            order_deadline: Boat order deadline date

        Returns:
            UnableToShipSummary with items and totals
        """
        items = []

        for p in all_products:
            # Skip if no coverage gap (don't need it)
            if p.coverage_gap_m2 <= 0:
                continue

            # Skip if has SIESA stock (can ship)
            if p.factory_available_m2 and p.factory_available_m2 > 0:
                continue

            # Skip if production completing before deadline
            if p.factory_ready_before_boat:
                continue

            # This product needs attention!
            reason, action = self._determine_unable_to_ship_reason(p, order_deadline)

            # Calculate priority score
            priority_score = 0
            if p.score:
                priority_score = p.score.total
            elif p.priority == "HIGH_PRIORITY":
                priority_score = 80
            elif p.priority == "CONSIDER":
                priority_score = 50

            items.append(UnableToShipItem(
                sku=p.sku,
                description=p.description,
                coverage_gap_m2=p.coverage_gap_m2,
                coverage_gap_pallets=p.coverage_gap_pallets,
                days_of_stock=p.days_of_stock,
                stockout_date=p.stockout_date if hasattr(p, 'stockout_date') else None,
                reason=reason,
                production_status=p.production_status,
                production_estimated_ready=p.production_estimated_ready,
                suggested_action=action,
                priority=p.priority,
                priority_score=priority_score,
            ))

        # Sort by priority score (most urgent first)
        items.sort(key=lambda x: x.priority_score, reverse=True)

        # Calculate totals
        total_gap_m2 = sum(item.coverage_gap_m2 for item in items)
        total_gap_pallets = sum(item.coverage_gap_pallets for item in items)

        # Generate summary message
        if items:
            message = f"{len(items)} products need attention ({total_gap_m2:,.0f} m²)"
        else:
            message = ""

        return UnableToShipSummary(
            count=len(items),
            total_gap_m2=total_gap_m2,
            total_gap_pallets=total_gap_pallets,
            message=message,
            items=items,
        )

    def _determine_unable_to_ship_reason(
        self,
        product: OrderBuilderProduct,
        order_deadline: date,
    ) -> tuple[str, str]:
        """Determine why product can't ship and what to do."""

        if product.production_status == 'completed':
            return (
                "Production completed but not at SIESA",
                "Check if already shipped or sync SIESA data"
            )

        if product.production_status == 'in_progress':
            ready_str = ""
            if product.production_estimated_ready:
                ready_str = f" (ready {product.production_estimated_ready})"
            return (
                f"Production in progress{ready_str}",
                "Wait for production or expedite if urgent"
            )

        if product.production_status == 'scheduled':
            ready_str = ""
            if product.production_estimated_ready:
                ready_str = f" (ready {product.production_estimated_ready})"
            return (
                f"Production scheduled{ready_str}",
                "Wait for production or request expedite"
            )

        # Not scheduled at all
        return (
            "No SIESA stock and no production scheduled",
            "Request new production immediately"
        )
