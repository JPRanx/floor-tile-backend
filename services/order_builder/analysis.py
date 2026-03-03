"""Product analysis mixins for Order Builder service."""

from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
import math
import structlog

from config.shipping import (
    M2_PER_PALLET,
    ORDERING_CYCLE_DAYS,
    WAREHOUSE_BUFFER_DAYS,
    SEASONAL_DAMPENING,
)
from services.order_builder.constants import ProductAnalysis
from models.order_builder import Urgency

logger = structlog.get_logger(__name__)


class AnalysisMixin:
    """Product analysis: FS projection and inventory fallback paths."""

    def _get_product_trends(self) -> dict[str, dict]:
        """
        Fetch trend data from Intelligence system with dual velocity calculation.

        Returns dict keyed by SKU with trend metrics including:
        - 90-day velocity (recent)
        - 180-day velocity (historical)
        - Trend signal (growing/stable/declining based on 90d vs 180d comparison)

        Trend signal thresholds:
        - growing: 90d velocity > 180d velocity by 20%+
        - declining: 90d velocity < 180d velocity by 20%+
        - stable: within 20%
        """
        try:
            # Get 90-day trends (existing)
            trends_90d = self.trend_service.get_product_trends(
                period_days=90,
                comparison_period_days=90,
                limit=200  # Get all products
            )

            # Get 180-day trends for longer-term comparison
            trends_180d = self.trend_service.get_product_trends(
                period_days=180,
                comparison_period_days=180,
                limit=200
            )

            # Build 180d velocity lookup by SKU
            velocity_180d_by_sku = {
                t.sku: t.current_velocity_m2_day
                for t in trends_180d
            }

            # Thresholds for trend signal
            GROWING_THRESHOLD = Decimal("1.20")   # 90d > 180d by 20%+
            DECLINING_THRESHOLD = Decimal("0.80") # 90d < 180d by 20%+

            # Get seasonal dampening factor for current month
            current_month = date.today().month
            seasonal_factor = SEASONAL_DAMPENING.get(current_month, 1.0)

            result = {}
            for t in trends_90d:
                velocity_90d = t.current_velocity_m2_day
                velocity_180d = velocity_180d_by_sku.get(t.sku, Decimal("0"))

                # Calculate trend signal with seasonal dampening
                if velocity_180d > 0:
                    trend_ratio_raw = velocity_90d / velocity_180d

                    # Apply seasonal dampening: pull ratio toward 1.0 (neutral)
                    # Formula: dampened = 1.0 + (raw - 1.0) * factor
                    # Factor of 0.5 means: +60% raw becomes +30% dampened
                    trend_ratio = Decimal("1.0") + (trend_ratio_raw - Decimal("1.0")) * Decimal(str(seasonal_factor))

                    # Determine signal from dampened ratio
                    if trend_ratio >= GROWING_THRESHOLD:
                        trend_signal = "growing"
                    elif trend_ratio <= DECLINING_THRESHOLD:
                        trend_signal = "declining"
                    else:
                        trend_signal = "stable"

                    # Log when dampening changes the outcome (DEBUG level)
                    if seasonal_factor < 1.0:
                        # What would signal have been without dampening?
                        if trend_ratio_raw >= GROWING_THRESHOLD:
                            raw_signal = "growing"
                        elif trend_ratio_raw <= DECLINING_THRESHOLD:
                            raw_signal = "declining"
                        else:
                            raw_signal = "stable"

                        if raw_signal != trend_signal:
                            logger.debug(
                                "seasonal_dampening_applied",
                                sku=t.sku,
                                month=current_month,
                                raw_ratio=float(trend_ratio_raw),
                                dampened_ratio=float(trend_ratio),
                                raw_signal=raw_signal,
                                dampened_signal=trend_signal,
                                seasonal_factor=seasonal_factor,
                            )
                else:
                    # No 180d data - use 90d direction (no dampening possible)
                    trend_ratio_raw = Decimal("1.0")
                    trend_ratio = Decimal("1.0")
                    if velocity_90d > 0:
                        trend_signal = "growing"  # New activity
                    else:
                        trend_signal = "stable"

                result[t.sku] = {
                    "direction": t.direction.value if hasattr(t.direction, 'value') else str(t.direction),
                    "strength": t.strength.value if hasattr(t.strength, 'value') else str(t.strength),
                    "velocity_change_pct": t.velocity_change_pct,
                    "daily_velocity_m2": velocity_90d,
                    "days_of_stock": t.days_of_stock,
                    "confidence": t.confidence.value if hasattr(t.confidence, 'value') else str(t.confidence),
                    # Dual velocity fields
                    "velocity_90d_m2": velocity_90d,
                    "velocity_180d_m2": velocity_180d,
                    "velocity_trend_signal": trend_signal,
                    "velocity_trend_ratio": round(trend_ratio, 2) if velocity_180d > 0 else Decimal("1.0"),
                    "velocity_trend_ratio_raw": round(trend_ratio_raw, 2) if velocity_180d > 0 else Decimal("1.0"),
                }

            return result
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

    @staticmethod
    def _derive_action_type(urgency: str, suggested_pallets: int) -> str:
        """Derive action_type from urgency and suggestion."""
        if suggested_pallets == 0:
            return "WELL_STOCKED"
        if urgency in ("critical", "urgent"):
            return "ORDER_NOW"
        if urgency == "soon":
            return "ORDER_SOON"
        return "REVIEW"

    def _get_customer_demand_scores(self) -> dict[str, dict]:
        """
        Calculate customer demand scores and expected orders for products.

        Returns dict keyed by SKU with:
        - score: int (0-300+ based on tier weights and overdue status)
        - customers_count: int (number of customers expecting this product)
        - expected_m2: Decimal (expected m² from customers due soon)
        - customer_names: list[str] (names of expecting customers)

        Tier weights:
        - A-tier: 100 points
        - B-tier: 50 points
        - C-tier: 25 points

        Overdue multiplier:
        - 0-14 days: 1.0x (due soon)
        - 15-30 days: 1.5x (moderately overdue)
        - 31-60 days: 2.0x (significantly overdue)
        - 60+ days: 2.5x (severely overdue)
        """
        try:
            # Get customer trends (includes pattern data and top_products)
            customer_trends = self.trend_service.get_customer_trends(
                period_days=90,
                comparison_period_days=90,
                limit=100
            )

            # Build SKU → demand info mapping
            sku_demand: dict[str, dict] = {}

            tier_weights = {"A": 100, "B": 50, "C": 25}

            for customer in customer_trends:
                # Skip if no pattern data
                if not customer.avg_days_between_orders or customer.order_count < 2:
                    continue

                days_overdue = customer.days_overdue

                # Only consider customers due within 14 days or overdue
                if days_overdue < -14:
                    continue

                # Calculate overdue multiplier
                if days_overdue <= 14:
                    overdue_multiplier = 1.0
                elif days_overdue <= 30:
                    overdue_multiplier = 1.5
                elif days_overdue <= 60:
                    overdue_multiplier = 2.0
                else:
                    overdue_multiplier = 2.5

                # Get tier weight
                tier = customer.tier.value if hasattr(customer.tier, 'value') else str(customer.tier)
                tier_weight = tier_weights.get(tier, 25)

                # Score for this customer
                customer_score = int(tier_weight * overdue_multiplier)

                # Add score and expected m² to each of their top products
                for prod in customer.top_products[:5]:  # Top 5 products per customer
                    sku = prod.sku
                    if sku not in sku_demand:
                        sku_demand[sku] = {
                            "score": 0,
                            "customers": set(),
                            "expected_m2": Decimal("0"),
                        }

                    sku_demand[sku]["score"] += customer_score
                    sku_demand[sku]["customers"].add(customer.customer_normalized)

                    # Calculate expected m² for this product from this customer
                    # Use customer's average order for this specific product
                    if customer.order_count > 0 and prod.total_m2:
                        avg_product_m2 = Decimal(str(prod.total_m2)) / customer.order_count
                        sku_demand[sku]["expected_m2"] += avg_product_m2

            # Convert sets to result
            result = {}
            for sku, data in sku_demand.items():
                result[sku] = {
                    "score": data["score"],
                    "customers_count": len(data["customers"]),
                    "expected_m2": round(data["expected_m2"], 2),
                    "customer_names": list(data["customers"])[:5],  # Top 5 names
                }

            total_expected = sum(d["expected_m2"] for d in result.values())
            logger.debug(
                "customer_demand_scores_calculated",
                products_with_demand=len(result),
                top_score=max((d["score"] for d in result.values()), default=0),
                total_expected_m2=float(total_expected),
            )

            return result

        except Exception as e:
            logger.warning("customer_demand_scores_failed", error=str(e))
            return {}

    def _calculate_trend_adjustment(
        self,
        direction: str,
        strength: str,
        base_quantity_m2: Decimal
    ) -> tuple[Decimal, Decimal]:
        """
        Calculate trend-based adjustment to order quantity.

        Growing: +5% to +20% (order more buffer for increasing demand)
        Stable: 0% (no adjustment)
        Declining: -5% to -20% (order less to avoid overstock)

        Returns (adjustment_m2, adjustment_pct)
        """
        if direction == "up":
            # Uptrend: increase order quantity
            pct_by_strength = {
                "strong": Decimal("0.20"),   # +20% for strong uptrend
                "moderate": Decimal("0.10"), # +10% for moderate uptrend
                "weak": Decimal("0.05"),     # +5% for weak uptrend
            }
            adjustment_pct = pct_by_strength.get(strength, Decimal("0"))

        elif direction == "down":
            # Downtrend: decrease order quantity to avoid overstock
            # Mirror the uptrend logic with negative values
            pct_by_strength = {
                "strong": Decimal("-0.20"),   # -20% for strong decline
                "moderate": Decimal("-0.10"), # -10% for moderate decline
                "weak": Decimal("-0.05"),     # -5% for weak decline
            }
            adjustment_pct = pct_by_strength.get(strength, Decimal("0"))

        else:
            # Stable: no adjustment
            adjustment_pct = Decimal("0")

        adjustment_m2 = base_quantity_m2 * adjustment_pct

        return adjustment_m2, adjustment_pct * 100  # Return as percentage

    def _analyze_from_projection(
        self,
        projection: dict,
        daily_velocity_m2: Decimal,
        buffer_days: int,
        pallet_factor: Decimal,
        factory_availability_map: dict,
        pending_orders_map: dict,
        sku: str,
    ) -> ProductAnalysis:
        """Build ProductAnalysis from Forward Simulation projection.

        FS already computed everything (trend, customer demand, cascade).
        We just read its values, then subtract pending warehouse orders.
        """
        # Days of stock at arrival (FS-projected)
        proj_days = projection.get("days_of_stock_at_arrival")
        days_of_stock = int(round(proj_days)) if proj_days is not None else None
        urgency = self._calculate_urgency(days_of_stock)

        # Trend from FS
        direction = projection.get("trend_direction", "stable")
        strength = projection.get("trend_strength", "weak")

        # Suggestion from FS
        final_suggestion_m2 = Decimal(str(projection.get("coverage_gap_m2", 0)))
        final_suggestion_pallets = projection.get("suggested_pallets", 0)

        # Breakdown display values
        buffer_days_from_fs = projection.get("buffer_days", buffer_days)
        trend_adjustment_pct = Decimal(str(projection.get("trend_adjustment_pct", 0)))
        base_quantity_m2 = daily_velocity_m2 * Decimal(buffer_days_from_fs)
        trend_adjustment_m2 = base_quantity_m2 * trend_adjustment_pct / Decimal("100")

        # Warehouse-projected for breakdown display
        supply = projection["supply_breakdown"]
        factory_supply_m2 = supply["factory_siesa_m2"] + supply["production_pipeline_m2"]
        warehouse_projected = projection["projected_stock_m2"] - factory_supply_m2
        minus_current = max(Decimal("0"), warehouse_projected)

        # Customer demand from FS
        customers_expecting_count = projection.get("customers_expecting_count", 0)
        expected_customer_orders_m2 = Decimal(str(projection.get("customer_demand_m2", 0)))
        customer_names = projection.get("customer_names", [])

        # Factory cascade-aware SIESA
        factory_cascade_m2 = supply["factory_siesa_m2"] + supply["production_pipeline_m2"]

        # Pending warehouse orders (FS doesn't know about these)
        pending_info = pending_orders_map.get(sku, {})
        pending_order_m2 = Decimal(str(pending_info.get("total_m2", 0)))
        pending_order_pallets = int(pending_info.get("total_pallets", 0))
        pending_order_boat = pending_info.get("boat_name")

        # Subtract pending from FS suggestion
        adj_suggestion_m2 = max(Decimal("0"), final_suggestion_m2 - pending_order_m2)
        adj_suggestion_pallets = max(0, math.ceil(float(adj_suggestion_m2 / pallet_factor)))

        return ProductAnalysis(
            uses_projection=True,
            days_of_stock=days_of_stock,
            urgency=urgency,
            trend_direction=direction,
            trend_strength=strength,
            base_quantity_m2=base_quantity_m2,
            trend_adjustment_m2=trend_adjustment_m2,
            trend_adjustment_pct=trend_adjustment_pct,
            adjusted_quantity_m2=base_quantity_m2 + trend_adjustment_m2,
            buffer_days=buffer_days_from_fs,
            total_coverage_days=buffer_days_from_fs,
            minus_current=minus_current,
            minus_incoming=Decimal("0"),
            pending_order_m2=pending_order_m2,
            pending_order_pallets=pending_order_pallets,
            pending_order_boat=pending_order_boat,
            final_suggestion_m2=adj_suggestion_m2,
            final_suggestion_pallets=adj_suggestion_pallets,
            adjusted_coverage_gap=Decimal(str(projection["coverage_gap_m2"])),
            customer_demand_score=projection.get("customer_demand_score", 0),
            customers_expecting_count=customers_expecting_count,
            expected_customer_orders_m2=expected_customer_orders_m2,
            customer_names=customer_names,
            factory_cascade_m2=factory_cascade_m2,
            projected_stock_m2=Decimal(str(projection["projected_stock_m2"])),
            earlier_drafts_consumed_m2=Decimal(str(projection["earlier_drafts_consumed_m2"])),
            lead_time_days_for_breakdown=0,
            ordering_cycle_days_for_breakdown=buffer_days_from_fs,
        )

    def _analyze_from_inventory(
        self,
        product,
        direction: str,
        strength: str,
        daily_velocity_m2: Decimal,
        days_of_stock: Optional[int],
        days_to_cover: int,
        buffer_days: int,
        pallet_factor: Decimal,
        pending_orders_map: dict,
        customer_demand_data: dict,
        factory_availability_map: dict,
    ) -> ProductAnalysis:
        """Build ProductAnalysis from current inventory + trend data.

        Fallback path when Forward Simulation is not available.
        """
        # Adjust days_of_stock for depletion during transit
        # Include SIESA so products with large factory stock aren't falsely critical
        if days_of_stock is not None and daily_velocity_m2 > 0 and days_to_cover > 0:
            warehouse_m2_val = product.warehouse_m2 or Decimal("0")
            in_transit_m2_val = product.in_transit_m2 or Decimal("0")
            fallback_siesa = factory_availability_map.get(
                product.product_id, {}
            ).get("factory_available_m2", Decimal("0"))
            available_m2 = warehouse_m2_val + in_transit_m2_val + fallback_siesa
            projected_m2 = available_m2 - (daily_velocity_m2 * days_to_cover)
            if projected_m2 > 0:
                days_of_stock = int(projected_m2 / daily_velocity_m2)
            else:
                days_of_stock = 0

        urgency = self._calculate_urgency(days_of_stock)

        # Quantity calculation
        total_coverage_days = days_to_cover + buffer_days
        base_quantity_m2 = daily_velocity_m2 * Decimal(total_coverage_days)
        trend_adjustment_m2, trend_adjustment_pct = self._calculate_trend_adjustment(
            direction, strength, base_quantity_m2
        )
        adjusted_quantity_m2 = base_quantity_m2 + trend_adjustment_m2

        minus_current = product.warehouse_m2 or Decimal("0")
        minus_incoming = product.in_transit_m2 or Decimal("0")

        # Pending orders
        pending_info = pending_orders_map.get(product.sku, {})
        pending_order_m2 = Decimal(str(pending_info.get("total_m2", 0)))
        pending_order_pallets = int(pending_info.get("total_pallets", 0))
        pending_order_boat = pending_info.get("boat_name")

        final_suggestion_m2 = max(
            Decimal("0"),
            adjusted_quantity_m2 - minus_current - minus_incoming - pending_order_m2
        )
        final_suggestion_pallets = max(0, math.ceil(float(final_suggestion_m2 / pallet_factor)))

        # Customer demand
        demand_info = customer_demand_data.get(product.sku, {
            "score": 0, "customers_count": 0,
            "expected_m2": Decimal("0"), "customer_names": []
        })
        customer_demand_score = demand_info["score"]
        customers_expecting_count = demand_info["customers_count"]
        expected_customer_orders_m2 = Decimal(str(demand_info.get("expected_m2", 0)))
        customer_names = demand_info.get("customer_names", [])

        # Coverage gap: OB-computed
        base_coverage_gap = adjusted_quantity_m2 - minus_current - minus_incoming - pending_order_m2
        adjusted_coverage_gap = base_coverage_gap + expected_customer_orders_m2

        if expected_customer_orders_m2 > 0:
            logger.debug(
                "expected_orders_added_to_gap",
                sku=product.sku,
                base_gap=float(base_coverage_gap),
                expected_orders=float(expected_customer_orders_m2),
                adjusted_gap=float(adjusted_coverage_gap),
            )

        # Factory: raw SIESA (no cascade without FS)
        factory_cascade_m2 = factory_availability_map.get(
            product.product_id, {}
        ).get("factory_available_m2", Decimal("0"))

        return ProductAnalysis(
            uses_projection=False,
            days_of_stock=days_of_stock,
            urgency=urgency,
            trend_direction=direction,
            trend_strength=strength,
            base_quantity_m2=base_quantity_m2,
            trend_adjustment_m2=trend_adjustment_m2,
            trend_adjustment_pct=trend_adjustment_pct,
            adjusted_quantity_m2=adjusted_quantity_m2,
            buffer_days=buffer_days,
            total_coverage_days=total_coverage_days,
            minus_current=minus_current,
            minus_incoming=minus_incoming,
            pending_order_m2=pending_order_m2,
            pending_order_pallets=pending_order_pallets,
            pending_order_boat=pending_order_boat,
            final_suggestion_m2=final_suggestion_m2,
            final_suggestion_pallets=final_suggestion_pallets,
            adjusted_coverage_gap=adjusted_coverage_gap,
            customer_demand_score=customer_demand_score,
            customers_expecting_count=customers_expecting_count,
            expected_customer_orders_m2=expected_customer_orders_m2,
            customer_names=customer_names,
            factory_cascade_m2=factory_cascade_m2,
            lead_time_days_for_breakdown=days_to_cover,
            ordering_cycle_days_for_breakdown=buffer_days,
        )
