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
import time
import structlog

from config import settings
from config.shipping import (
    CONTAINER_MAX_WEIGHT_KG,
    CONTAINER_MAX_PALLETS,
    DEFAULT_WEIGHT_PER_M2_KG,
    M2_PER_PALLET,
    WAREHOUSE_BUFFER_DAYS,
    ORDERING_CYCLE_DAYS,
    LIQUIDATION_DECLINING_TREND_PCT_MAX,
    LIQUIDATION_DECLINING_DAYS_MIN,
    LIQUIDATION_NO_SALES_DAYS,
    LIQUIDATION_EXTREME_DAYS_MIN,
)
from services.boat_schedule_service import get_boat_schedule_service
from services.recommendation_service import get_recommendation_service
from services.inventory_service import get_inventory_service
from services.trend_service import get_trend_service
from services.customer_pattern_service import get_customer_pattern_service
from services.production_schedule_service import get_production_schedule_service
from models.order_builder import (
    OrderBuilderMode,
    OrderBuilderProduct,
    OrderBuilderBoat,
    OrderBuilderAlert,
    OrderBuilderAlertType,
    OrderBuilderSummary,
    OrderBuilderResponse,
    CalculationBreakdown,
    ConstraintAnalysis,
    LiquidationCandidate,
    Urgency,
    # Reasoning models
    ProductReasoning,
    StockAnalysis,
    DemandAnalysis,
    QuantityReasoning,
    OrderSummaryReasoning,
    OrderReasoning,
    ExcludedProduct,
    PrimaryFactor,
    # Scoring models (Layer 2 & 4)
    ProductScore,
    ProductReasoningDisplay,
    DominantFactor,
    # Section summaries (Three-Section Order Builder)
    WarehouseOrderSummary,
    AddToProductionSummary,
    AddToProductionItem,
    FactoryRequestSummary,
    FactoryRequestItem,
)
from models.recommendation import RecommendationPriority

logger = structlog.get_logger(__name__)

# Use config.shipping constants via imports above
# M2_PER_PALLET, CONTAINER_MAX_PALLETS imported from config.shipping
PALLETS_PER_CONTAINER = CONTAINER_MAX_PALLETS  # Alias for readability
MAX_CONTAINERS_PER_BL = 5  # Each BL can hold up to 5 containers
WAREHOUSE_CAPACITY = settings.warehouse_max_pallets  # From config.settings

# Factory request constants
MIN_CONTAINER_M2 = M2_PER_PALLET * PALLETS_PER_CONTAINER  # 1,881.6 m²
LOW_VOLUME_THRESHOLD_DAYS = 365  # 1 year — products that take longer to consume 1 container are flagged


def _get_next_monday(from_date: date) -> date:
    """
    Get next Monday from a given date.
    Factory adds new items to production schedule on Mondays.
    """
    days_ahead = (7 - from_date.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7  # If today is Monday, get next Monday
    return from_date + timedelta(days=days_ahead)


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
        self.customer_pattern_service = get_customer_pattern_service()
        self.production_schedule_service = get_production_schedule_service()

    def get_order_builder(
        self,
        boat_id: Optional[str] = None,
        num_bls: int = 1,
        excluded_skus: Optional[list[str]] = None,
    ) -> OrderBuilderResponse:
        """
        Get complete Order Builder data.

        Args:
            boat_id: Optional specific boat ID. If None, uses next available.
            num_bls: Number of BLs (1-5). Determines capacity: num_bls × 5 × 14 pallets.
                     Default 1 (70 pallets) for backward compatibility.
            excluded_skus: Optional list of SKUs to exclude from optimization.
                          Used when user removes products and wants to recalculate.

        Returns:
            OrderBuilderResponse with all data needed for the UI
        """
        # Clamp num_bls to valid range
        num_bls = max(1, min(5, num_bls))

        # Normalize excluded_skus
        excluded_set = set(excluded_skus) if excluded_skus else set()

        timings = {}
        t0 = time.time()
        logger.info(
            "getting_order_builder",
            boat_id=boat_id,
            num_bls=num_bls,
            excluded_count=len(excluded_set)
        )

        # Step 1: Get boat info
        boat, next_boat = self._get_boats(boat_id)
        timings["1_boats"] = round(time.time() - t0, 2)

        # Default lead time for "no boat" mode (45 days)
        DEFAULT_LEAD_TIME_DAYS = 45

        if not boat:
            # No boats available - use default lead time and create dummy boat
            logger.info("no_boats_available_using_default_lead_time", days=DEFAULT_LEAD_TIME_DAYS)
            today = date.today()
            default_departure = today + timedelta(days=DEFAULT_LEAD_TIME_DAYS)
            default_arrival = default_departure + timedelta(days=25)  # ~25 days transit
            default_warehouse = DEFAULT_LEAD_TIME_DAYS + 25 + WAREHOUSE_BUFFER_DAYS  # departure + transit + buffer
            default_order_deadline = default_departure - timedelta(days=30)

            boat = OrderBuilderBoat(
                boat_id="",
                name="",
                departure_date=default_departure,
                arrival_date=default_arrival,
                days_until_departure=DEFAULT_LEAD_TIME_DAYS,
                days_until_arrival=DEFAULT_LEAD_TIME_DAYS + 25,  # departure + transit
                days_until_warehouse=default_warehouse,
                order_deadline=default_order_deadline,
                days_until_order_deadline=(default_order_deadline - today).days,
                past_order_deadline=today > default_order_deadline,
                booking_deadline=today,
                days_until_deadline=0,
                max_containers=5,
            )

        # Step 2: Get recommendations (has coverage gap, confidence, priority)
        t1 = time.time()
        recommendations = self.recommendation_service.get_recommendations()
        timings["2_recommendations"] = round(time.time() - t1, 2)

        # Step 2a: Filter out excluded SKUs (for recalculate)
        if excluded_set:
            original_count = len(recommendations.recommendations)
            recommendations.recommendations = [
                rec for rec in recommendations.recommendations
                if rec.sku not in excluded_set
            ]
            logger.info(
                "excluded_skus_filtered",
                excluded_count=len(excluded_set),
                original_count=original_count,
                filtered_count=len(recommendations.recommendations)
            )

        # Step 2b: Get trend data for products
        t2 = time.time()
        trend_data = self._get_product_trends()
        timings["3_trends"] = round(time.time() - t2, 2)

        # Step 3: Convert to OrderBuilderProducts grouped by priority
        t3 = time.time()
        products_by_priority = self._group_products_by_priority(
            recommendations.recommendations,
            boat.days_until_warehouse,  # Use warehouse arrival (not departure!)
            trend_data,
            boat_departure=boat.departure_date if boat.boat_id else None  # Pass departure for factory status
        )
        timings["4_grouping"] = round(time.time() - t3, 2)

        logger.info("order_builder_timings", **timings)

        # Step 4: Apply BL capacity logic (pre-select products) with constraint analysis
        all_products, constraint_analysis = self._apply_mode(
            products_by_priority, num_bls, boat.max_containers, trend_data
        )

        # Step 5: Calculate summary (use BL capacity, not boat capacity)
        bl_max_containers = num_bls * MAX_CONTAINERS_PER_BL
        summary = self._calculate_summary(all_products, bl_max_containers)

        # Step 6: Generate alerts
        alerts = self._generate_alerts(all_products, summary, boat)
        summary.alerts = alerts

        # Step 7: Generate summary reasoning
        summary_reasoning = self._generate_summary_reasoning(
            all_products, boat, summary, constraint_analysis
        )

        # Re-group after mode application
        high_priority = [p for p in all_products if p.priority == "HIGH_PRIORITY"]
        consider = [p for p in all_products if p.priority == "CONSIDER"]
        well_covered = [p for p in all_products if p.priority == "WELL_COVERED"]
        your_call = [p for p in all_products if p.priority == "YOUR_CALL"]

        # Step 8: Calculate three-section summaries
        warehouse_summary, add_to_production_summary, factory_request_summary = \
            self._calculate_section_summaries(all_products, boat, num_bls)

        # Step 9: Calculate recommended BL count (based on true need) and available BLs
        recommended_bls, available_bls, recommended_bls_reason = self._calculate_recommended_bls(all_products)

        result = OrderBuilderResponse(
            boat=boat,
            next_boat=next_boat,
            num_bls=num_bls,
            recommended_bls=recommended_bls,
            available_bls=available_bls,
            recommended_bls_reason=recommended_bls_reason,
            high_priority=high_priority,
            consider=consider,
            well_covered=well_covered,
            your_call=your_call,
            summary=summary,
            # Three-section summaries
            warehouse_order_summary=warehouse_summary,
            add_to_production_summary=add_to_production_summary,
            factory_request_summary=factory_request_summary,
            constraint_analysis=constraint_analysis,
            summary_reasoning=summary_reasoning,
        )

        logger.info(
            "order_builder_generated",
            num_bls=num_bls,
            bl_capacity=num_bls * MAX_CONTAINERS_PER_BL * PALLETS_PER_CONTAINER,
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
        days_until_arrival = (boat_data.arrival_date - today).days
        days_until_deadline = (boat_data.booking_deadline - today).days

        # Order deadline is 30 days before departure (from boat_data)
        order_deadline = boat_data.order_deadline
        days_until_order_deadline = (order_deadline - today).days  # Can be negative
        past_order_deadline = today > order_deadline

        # days_until_warehouse = arrival + port buffer + trucking
        # This is the TRUE lead time for coverage calculation
        days_until_warehouse = days_until_arrival + WAREHOUSE_BUFFER_DAYS

        return OrderBuilderBoat(
            boat_id=boat_data.id,
            name=boat_data.vessel_name or f"Boat {boat_data.departure_date}",
            departure_date=boat_data.departure_date,
            arrival_date=boat_data.arrival_date,
            days_until_departure=max(0, days_until_departure),
            days_until_arrival=max(0, days_until_arrival),
            days_until_warehouse=max(0, days_until_warehouse),
            order_deadline=order_deadline,
            days_until_order_deadline=days_until_order_deadline,
            past_order_deadline=past_order_deadline,
            booking_deadline=boat_data.booking_deadline,
            days_until_deadline=max(0, days_until_deadline),
            max_containers=5,  # Default, could be configurable per boat
        )

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

            result = {}
            for t in trends_90d:
                velocity_90d = t.current_velocity_m2_day
                velocity_180d = velocity_180d_by_sku.get(t.sku, Decimal("0"))

                # Calculate trend signal
                if velocity_180d > 0:
                    trend_ratio = velocity_90d / velocity_180d
                    if trend_ratio >= GROWING_THRESHOLD:
                        trend_signal = "growing"
                    elif trend_ratio <= DECLINING_THRESHOLD:
                        trend_signal = "declining"
                    else:
                        trend_signal = "stable"
                else:
                    # No 180d data - use 90d direction
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

    # ===================
    # PRIORITY SCORING (Layer 2)
    # ===================

    def _calculate_priority_score(self, product: OrderBuilderProduct) -> ProductScore:
        """
        Calculate weighted priority score (0-100).

        Components:
        - Stockout Risk (0-40): Based on days of stock
        - Customer Demand (0-30): Based on customer_demand_score
        - Growth Trend (0-20): Based on velocity change
        - Revenue Impact (0-10): Based on sales velocity
        """
        # === STOCKOUT RISK (0-40 points) ===
        # Most important factor — product availability
        days = product.days_of_stock
        if days is None or days <= 0:
            stockout = 40  # Critical - already out
        elif days < 7:
            stockout = 35  # Urgent - days away
        elif days < 14:
            stockout = 30  # Soon - 1-2 weeks
        elif days < 30:
            stockout = 20  # Monitor - under a month
        elif days < 60:
            stockout = 10  # OK - 1-2 months
        else:
            stockout = 0   # Well stocked

        # === CUSTOMER DEMAND (0-30 points) ===
        # Based on customer_demand_score from existing calculation
        # Score ranges 0-500+ depending on tier and overdue
        demand_raw = product.customer_demand_score or 0
        if demand_raw >= 200:
            customer = 30  # Multiple A-tier or very overdue
        elif demand_raw >= 100:
            customer = 25  # A-tier customer waiting
        elif demand_raw >= 50:
            customer = 15  # B-tier customer waiting
        elif demand_raw > 0:
            customer = 10  # C-tier customer waiting
        else:
            customer = 0   # No customers waiting

        # === GROWTH TREND (0-20 points) ===
        # Based on velocity_change_pct and trend_direction
        trend_pct = float(product.velocity_change_pct or 0)
        direction = product.trend_direction

        if direction == "up":
            if trend_pct >= 30:
                trend = 20  # Strong growth
            elif trend_pct >= 15:
                trend = 15  # Good growth
            else:
                trend = 10  # Mild growth
        elif direction == "down":
            trend = 0  # Declining - no bonus
        else:
            trend = 5  # Stable

        # === REVENUE IMPACT (0-10 points) ===
        # Based on velocity (fast sellers = more revenue impact)
        velocity = float(product.daily_velocity_m2 or 0)
        if velocity >= 50:
            revenue = 10  # Top seller
        elif velocity >= 30:
            revenue = 8
        elif velocity >= 15:
            revenue = 5
        elif velocity > 0:
            revenue = 3
        else:
            revenue = 0  # No sales

        total = stockout + customer + trend + revenue

        return ProductScore(
            total=min(100, total),  # Cap at 100
            stockout_risk=stockout,
            customer_demand=customer,
            growth_trend=trend,
            revenue_impact=revenue,
        )

    def _determine_dominant_factor(self, score: ProductScore) -> str:
        """Determine which factor contributed most to the score."""
        factors = {
            DominantFactor.STOCKOUT.value: score.stockout_risk,
            DominantFactor.CUSTOMER.value: score.customer_demand,
            DominantFactor.TREND.value: score.growth_trend,
            DominantFactor.REVENUE.value: score.revenue_impact,
        }
        return max(factors, key=factors.get)

    def _generate_why_product_sentence(
        self,
        product: OrderBuilderProduct,
        dominant: str
    ) -> str:
        """
        Generate one-sentence explanation of why this product is recommended.

        Format: "Main reason · secondary factor · tertiary factor"
        """
        parts = []

        # Lead with dominant factor
        if dominant == DominantFactor.STOCKOUT.value:
            days = product.days_of_stock
            if days is None or days <= 0:
                parts.append("Out of stock now")
            elif days < 7:
                parts.append(f"Only {days} days of stock")
            else:
                parts.append(f"{days} days of stock")

        elif dominant == DominantFactor.CUSTOMER.value:
            count = product.customers_expecting_count or 0
            if count == 1:
                parts.append("1 customer expected to order")
            else:
                parts.append(f"{count} customers expected to order")

        elif dominant == DominantFactor.TREND.value:
            pct = product.velocity_change_pct or 0
            parts.append(f"Demand growing {pct:+.0f}%")

        elif dominant == DominantFactor.REVENUE.value:
            velocity = float(product.daily_velocity_m2 or 0)
            parts.append(f"High-velocity product ({velocity:.0f} m²/day)")

        # Add secondary factors if significant (and not already the dominant)
        score = product.score

        if score and dominant != DominantFactor.STOCKOUT.value and score.stockout_risk >= 30:
            days = product.days_of_stock
            if days is not None:
                parts.append(f"{days}d stock")

        if score and dominant != DominantFactor.CUSTOMER.value and score.customer_demand >= 15:
            count = product.customers_expecting_count or 0
            if count > 0:
                parts.append(f"{count} customer{'s' if count > 1 else ''} waiting")

        if score and dominant != DominantFactor.TREND.value and score.growth_trend >= 15:
            pct = product.velocity_change_pct or 0
            if pct > 0:
                parts.append(f"+{pct:.0f}% trend")

        return " · ".join(parts) if parts else "Standard replenishment"

    def _generate_why_quantity_sentence(self, product: OrderBuilderProduct) -> str:
        """Generate one-sentence explanation of the quantity recommendation."""
        velocity = float(product.daily_velocity_m2 or 0)
        breakdown = product.calculation_breakdown

        if breakdown and velocity > 0:
            coverage_days = breakdown.lead_time_days + breakdown.ordering_cycle_days
            return f"{coverage_days}d coverage × {velocity:.1f} m²/day"
        elif product.suggested_pallets > 0:
            return f"{product.suggested_pallets} pallets to cover lead time"
        else:
            return "No order needed"

    def _generate_product_reasoning_display(
        self,
        product: OrderBuilderProduct
    ) -> ProductReasoningDisplay:
        """Generate complete display reasoning for a product."""
        # Calculate score if not already done
        if not product.score:
            product.score = self._calculate_priority_score(product)

        dominant = self._determine_dominant_factor(product.score)

        return ProductReasoningDisplay(
            why_product_sentence=self._generate_why_product_sentence(product, dominant),
            why_quantity_sentence=self._generate_why_quantity_sentence(product),
            dominant_factor=dominant,
            would_include_if=None,  # Phase 3
        )

    def _group_products_by_priority(
        self,
        recommendations: list,
        days_to_cover: int,
        trend_data: dict[str, dict],
        boat_departure: Optional[date] = None
    ) -> dict[str, list[OrderBuilderProduct]]:
        """Convert recommendations to OrderBuilderProducts grouped by priority."""
        groups = {
            "HIGH_PRIORITY": [],
            "CONSIDER": [],
            "WELL_COVERED": [],
            "YOUR_CALL": [],
        }

        # ORDERING_CYCLE_DAYS imported from config.shipping (default: 30)
        # Covers the gap until the NEXT boat arrives (monthly ordering cycle)

        # Get customer demand scores for priority ranking
        customer_demand_data = self._get_customer_demand_scores()

        # Get factory production status for all products
        factory_status_map = {}
        if boat_departure:
            product_ids = [rec.product_id for rec in recommendations]
            try:
                factory_status_map = self.production_schedule_service.get_factory_status(
                    product_ids=product_ids,
                    boat_departure=boat_departure,
                    buffer_days=3
                )
            except Exception as e:
                logger.warning("factory_status_lookup_failed", error=str(e))

        # Get factory availability (SIESA finished goods) for all products
        factory_availability_map = {}
        try:
            inventory_snapshots = self.inventory_service.get_latest()
            for inv in inventory_snapshots:
                factory_availability_map[inv.product_id] = {
                    "factory_available_m2": Decimal(str(inv.factory_available_m2 or 0)),
                    "factory_largest_lot_m2": Decimal(str(inv.factory_largest_lot_m2)) if inv.factory_largest_lot_m2 else None,
                    "factory_largest_lot_code": inv.factory_largest_lot_code,
                    "factory_lot_count": inv.factory_lot_count or 0,
                }
        except Exception as e:
            logger.warning("factory_availability_lookup_failed", error=str(e))

        # Get production schedule data (from Programa de Produccion Excel)
        # This shows what's scheduled/in_progress/completed at the factory
        production_schedule_map = {}
        try:
            production_schedule_map = self.production_schedule_service.get_production_by_sku()
            logger.debug(
                "production_schedule_loaded",
                products_with_production=len(production_schedule_map)
            )
        except Exception as e:
            logger.warning("production_schedule_lookup_failed", error=str(e))

        for rec in recommendations:
            # Get trend data for this product
            trend = trend_data.get(rec.sku, {})
            direction = trend.get("direction", "stable")
            strength = trend.get("strength", "weak")
            velocity_change_pct = Decimal(str(trend.get("velocity_change_pct", 0)))
            daily_velocity_m2 = Decimal(str(trend.get("daily_velocity_m2", 0)))
            days_of_stock = trend.get("days_of_stock")

            # Dual velocity fields (90-day vs 6-month comparison)
            velocity_90d_m2 = Decimal(str(trend.get("velocity_90d_m2", 0)))
            velocity_180d_m2 = Decimal(str(trend.get("velocity_180d_m2", 0)))
            velocity_trend_signal = trend.get("velocity_trend_signal", "stable")
            velocity_trend_ratio = Decimal(str(trend.get("velocity_trend_ratio", 1.0)))

            # Calculate urgency based on days of stock
            urgency = self._calculate_urgency(days_of_stock)

            # Calculate base quantity (lead time + ordering cycle) × velocity
            total_coverage_days = days_to_cover + ORDERING_CYCLE_DAYS
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
                ordering_cycle_days=ORDERING_CYCLE_DAYS,
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

            # Get customer demand score and expected orders for this product
            demand_info = customer_demand_data.get(rec.sku, {
                "score": 0,
                "customers_count": 0,
                "expected_m2": Decimal("0"),
                "customer_names": []
            })
            customer_demand_score = demand_info["score"]
            customers_expecting_count = demand_info["customers_count"]
            expected_customer_orders_m2 = Decimal(str(demand_info.get("expected_m2", 0)))

            # Build note for expected orders
            expected_orders_note = None
            if expected_customer_orders_m2 > 0 and customers_expecting_count > 0:
                customer_names = demand_info.get("customer_names", [])
                names_str = ", ".join(customer_names[:3])
                if len(customer_names) > 3:
                    names_str += f" +{len(customer_names) - 3}"
                expected_orders_note = (
                    f"Includes {int(expected_customer_orders_m2):,} m² expected from "
                    f"{customers_expecting_count} customer(s): {names_str}"
                )

            # Get factory production status
            factory_info = factory_status_map.get(rec.product_id)
            factory_status = "not_scheduled"
            factory_production_date = None
            factory_production_m2 = None
            days_until_factory_ready = None
            factory_ready_before_boat = None
            factory_timing_message = None

            if factory_info:
                factory_status = factory_info.status.value if hasattr(factory_info.status, 'value') else str(factory_info.status)
                factory_production_date = factory_info.production_date
                factory_production_m2 = factory_info.production_m2
                days_until_factory_ready = factory_info.days_until_ready
                factory_ready_before_boat = factory_info.ready_before_boat
                factory_timing_message = factory_info.timing_message

            # Get factory availability (SIESA finished goods)
            factory_avail = factory_availability_map.get(rec.product_id, {})
            factory_available_m2 = factory_avail.get("factory_available_m2", Decimal("0"))
            factory_largest_lot_m2 = factory_avail.get("factory_largest_lot_m2")
            factory_largest_lot_code = factory_avail.get("factory_largest_lot_code")
            factory_lot_count = factory_avail.get("factory_lot_count", 0)

            # Calculate factory fill status based on suggested quantity
            suggested_m2 = final_suggestion_m2
            if factory_available_m2 <= 0:
                factory_fill_status = "no_stock"
                factory_fill_message = "No stock at factory"
            elif suggested_m2 <= 0:
                factory_fill_status = "not_needed"
                factory_fill_message = None
            elif factory_largest_lot_m2 and suggested_m2 <= factory_largest_lot_m2:
                factory_fill_status = "single_lot"
                factory_fill_message = f"Can fill from single lot ({factory_largest_lot_code})"
            elif suggested_m2 <= factory_available_m2:
                factory_fill_status = "mixed_lots"
                largest_str = f"{int(factory_largest_lot_m2):,}" if factory_largest_lot_m2 else "?"
                factory_fill_message = f"Will need mixed lots (largest: {largest_str} m²)"
            else:
                shortfall = suggested_m2 - factory_available_m2
                factory_fill_status = "needs_production"
                factory_fill_message = f"Request production — need {int(shortfall):,} m² more"

            # Get production schedule status (from Programa de Produccion Excel)
            # This shows what's currently scheduled/in_progress/completed
            prod_schedule = production_schedule_map.get(rec.sku)
            production_status = "not_scheduled"
            production_requested_m2 = Decimal("0")
            production_completed_m2 = Decimal("0")
            production_can_add_more = False
            production_estimated_ready = None
            production_add_more_m2 = Decimal("0")
            production_add_more_alert = None

            if prod_schedule:
                production_status = prod_schedule.status.value if hasattr(prod_schedule.status, 'value') else str(prod_schedule.status)
                production_requested_m2 = prod_schedule.requested_m2 or Decimal("0")
                production_completed_m2 = prod_schedule.completed_m2 or Decimal("0")
                production_can_add_more = prod_schedule.can_add_more
                production_estimated_ready = prod_schedule.estimated_delivery_date

                # Calculate pre-production alert if:
                # 1. Status is 'scheduled' (production hasn't started)
                # 2. Suggested m² is greater than what's already requested
                if production_can_add_more and suggested_m2 > production_requested_m2:
                    gap_m2 = suggested_m2 - production_requested_m2
                    production_add_more_m2 = gap_m2
                    production_add_more_alert = f"Add {int(gap_m2):,} m² before production starts!"

            # Add expected customer orders to coverage gap
            # This accounts for predictable demand from customers due to order soon
            base_coverage_gap = rec.coverage_gap_m2 or Decimal("0")
            adjusted_coverage_gap = base_coverage_gap + expected_customer_orders_m2

            # Log if expected orders are adding to gap
            if expected_customer_orders_m2 > 0:
                logger.debug(
                    "expected_orders_added_to_gap",
                    sku=rec.sku,
                    base_gap=float(base_coverage_gap),
                    expected_orders=float(expected_customer_orders_m2),
                    adjusted_gap=float(adjusted_coverage_gap),
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
                coverage_gap_m2=adjusted_coverage_gap,  # Now includes expected customer orders
                coverage_gap_pallets=coverage_gap_pallets,
                suggested_pallets=suggested,
                confidence=rec.confidence.value,
                confidence_reason=rec.confidence_reason,
                unique_customers=rec.unique_customers,
                top_customer_name=rec.top_customer_name,
                top_customer_share=rec.top_customer_share,
                # Factory production status
                factory_status=factory_status,
                factory_production_date=factory_production_date,
                factory_production_m2=factory_production_m2,
                days_until_factory_ready=days_until_factory_ready,
                factory_ready_before_boat=factory_ready_before_boat,
                factory_timing_message=factory_timing_message,
                # Factory availability (SIESA finished goods)
                factory_available_m2=factory_available_m2,
                factory_largest_lot_m2=factory_largest_lot_m2,
                factory_largest_lot_code=factory_largest_lot_code,
                factory_lot_count=factory_lot_count,
                factory_fill_status=factory_fill_status,
                factory_fill_message=factory_fill_message,
                # Production schedule status (from Programa de Produccion Excel)
                production_status=production_status,
                production_requested_m2=production_requested_m2,
                production_completed_m2=production_completed_m2,
                production_can_add_more=production_can_add_more,
                production_estimated_ready=production_estimated_ready,
                production_add_more_m2=production_add_more_m2,
                production_add_more_alert=production_add_more_alert,
                # Trend fields
                urgency=urgency,
                days_of_stock=days_of_stock,
                trend_direction=direction,
                trend_strength=strength,
                velocity_change_pct=velocity_change_pct,
                daily_velocity_m2=daily_velocity_m2,
                # Dual velocity fields (90-day vs 6-month comparison)
                velocity_90d_m2=velocity_90d_m2,
                velocity_180d_m2=velocity_180d_m2,
                velocity_trend_signal=velocity_trend_signal,
                velocity_trend_ratio=velocity_trend_ratio,
                calculation_breakdown=breakdown if daily_velocity_m2 > 0 else None,
                # Reasoning
                reasoning=reasoning,
                # Customer demand signal
                customer_demand_score=customer_demand_score,
                customers_expecting_count=customers_expecting_count,
                expected_customer_orders_m2=expected_customer_orders_m2,
                expected_orders_note=expected_orders_note,
                # Selection
                is_selected=False,
                selected_pallets=0,
            )

            # Calculate priority score and display reasoning (Layer 2 & 4)
            product.score = self._calculate_priority_score(product)
            product.reasoning_display = self._generate_product_reasoning_display(product)

            priority_key = rec.priority.value
            if priority_key in groups:
                groups[priority_key].append(product)
            else:
                groups["YOUR_CALL"].append(product)

        # Sort each tier by:
        # 1. Urgency (critical → urgent → soon → ok)
        # 2. Customer demand score (higher = customers expecting this product)
        # 3. Days of stock (lower = more urgent)
        # 4. Velocity (higher = more important for revenue)
        urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}

        for priority_key in groups:
            groups[priority_key].sort(
                key=lambda p: (
                    urgency_order.get(p.urgency, 4),
                    -p.customer_demand_score,  # Higher score = higher priority
                    p.days_of_stock if p.days_of_stock is not None else 999,
                    -float(p.daily_velocity_m2),
                )
            )

        logger.debug(
            "products_sorted_by_urgency_and_demand",
            high_priority_order=[p.sku for p in groups["HIGH_PRIORITY"][:5]],
            top_demand_scores=[p.customer_demand_score for p in groups["HIGH_PRIORITY"][:5]],
        )

        return groups

    def _get_warehouse_available_pallets(self) -> int:
        """Get available space in warehouse (in pallets)."""
        inventory_snapshots = self.inventory_service.get_latest()
        warehouse_current_m2 = sum(
            Decimal(str(inv.warehouse_qty))
            for inv in inventory_snapshots
        )
        warehouse_current_pallets = int(warehouse_current_m2 / M2_PER_PALLET)
        available = max(0, WAREHOUSE_CAPACITY - warehouse_current_pallets)
        return available

    def _identify_liquidation_candidates(
        self,
        trend_data: dict[str, dict]
    ) -> list[LiquidationCandidate]:
        """
        Find slow movers that could be cleared to make room for fast movers.

        Uses LIQUIDATION_THRESHOLDS from config to identify:
        - declining_overstocked: Declining trend + high inventory
        - no_sales: No recent sales
        - extreme_overstock: Very high stock regardless of trend
        """
        candidates = []

        # Get inventory data
        inventory_snapshots = self.inventory_service.get_latest()

        for inv in inventory_snapshots:
            warehouse_m2 = Decimal(str(inv.warehouse_qty or 0))

            # Skip products with no warehouse stock
            if warehouse_m2 <= 0:
                continue

            product_id = inv.product_id
            sku = inv.sku if hasattr(inv, 'sku') else None

            # Get trend data for this product
            trend = trend_data.get(sku, {}) if sku else {}
            days_of_stock = trend.get("days_of_stock")
            trend_pct = Decimal(str(trend.get("velocity_change_pct", 0)))
            direction = trend.get("direction", "stable")
            daily_velocity_m2 = Decimal(str(trend.get("daily_velocity_m2", 0)))

            reason = None
            reason_display = ""

            # Check: Declining + Overstocked
            if (trend_pct <= LIQUIDATION_DECLINING_TREND_PCT_MAX and
                days_of_stock is not None and
                days_of_stock >= LIQUIDATION_DECLINING_DAYS_MIN):
                reason = "declining_overstocked"
                reason_display = f"Declining {trend_pct:+.0f}%, {days_of_stock} days stock"

            # Check: No sales in 90 days (days_of_stock is None or extremely high)
            elif days_of_stock is None or days_of_stock >= 365:
                reason = "no_sales"
                reason_display = "No sales in 90+ days"

            # Check: Extreme overstock (any trend)
            elif days_of_stock is not None and days_of_stock >= LIQUIDATION_EXTREME_DAYS_MIN:
                reason = "extreme_overstock"
                reason_display = f"{days_of_stock} days of stock"

            if reason:
                current_pallets = math.ceil(float(warehouse_m2 / M2_PER_PALLET))

                candidates.append(LiquidationCandidate(
                    product_id=product_id,
                    sku=sku or product_id,
                    description=None,
                    current_m2=warehouse_m2,
                    current_pallets=current_pallets,
                    days_of_stock=days_of_stock,
                    trend_direction=direction,
                    trend_pct=trend_pct,
                    daily_velocity_m2=daily_velocity_m2,
                    reason=reason,
                    reason_display=reason_display,
                    potential_space_freed_m2=warehouse_m2,
                    potential_space_freed_pallets=current_pallets,
                ))

        # Sort by most clearable (highest stock first, then most declining)
        candidates.sort(key=lambda c: (-c.current_pallets, float(c.trend_pct)))

        logger.debug(
            "liquidation_candidates_identified",
            count=len(candidates),
            total_pallets=sum(c.current_pallets for c in candidates),
        )

        return candidates

    def _apply_mode(
        self,
        products_by_priority: dict[str, list[OrderBuilderProduct]],
        num_bls: int,
        boat_max_containers: int,
        trend_data: dict[str, dict],
        warehouse_available_pallets: Optional[int] = None
    ) -> tuple[list[OrderBuilderProduct], ConstraintAnalysis]:
        """
        Apply BL capacity logic to pre-select products.

        BL count determines capacity:
        - 1 BL  =  5 containers =  70 pallets
        - 2 BLs = 10 containers = 140 pallets
        - 3 BLs = 15 containers = 210 pallets
        - 4 BLs = 20 containers = 280 pallets
        - 5 BLs = 25 containers = 350 pallets

        Returns tuple of (products, constraint_analysis)
        """
        # BL capacity: num_bls × 5 containers × 14 pallets
        bl_capacity = num_bls * MAX_CONTAINERS_PER_BL * PALLETS_PER_CONTAINER

        boat_capacity = boat_max_containers * PALLETS_PER_CONTAINER

        # Get warehouse available if not provided
        if warehouse_available_pallets is None:
            warehouse_available_pallets = self._get_warehouse_available_pallets()

        # Calculate total needed pallets (sum of all suggestions)
        total_needed = sum(
            p.suggested_pallets
            for group in products_by_priority.values()
            for p in group
            if p.suggested_pallets > 0
        )
        total_needed_m2 = Decimal(total_needed) * M2_PER_PALLET

        # Determine limiting factor and effective limit
        # Note: boat_capacity is NOT included as a separate constraint because
        # bl_capacity already represents the logical limit (num_bls × 5 containers).
        # The boat's physical capacity (25 containers max) is handled by limiting num_bls to 5.
        constraints = {
            "bl_capacity": bl_capacity,
            "warehouse": warehouse_available_pallets,
        }
        limiting_factor = min(constraints, key=constraints.get)
        effective_limit = constraints[limiting_factor]

        # If all constraints allow more than needed, no constraint is active
        if effective_limit >= total_needed:
            limiting_factor = "none"
            effective_limit = total_needed

        max_pallets = effective_limit

        total_selected = 0
        all_products = []

        # First pass: HIGH_PRIORITY (always include if room)
        for p in products_by_priority.get("HIGH_PRIORITY", []):
            pallets_needed = p.suggested_pallets  # Use suggestion (includes trend adjustment)
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

        # Second pass: CONSIDER (include if room available)
        for p in products_by_priority.get("CONSIDER", []):
            pallets_needed = p.suggested_pallets  # Use suggestion (includes trend adjustment)
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

        # Third pass: WELL_COVERED (include if room left)
        for p in products_by_priority.get("WELL_COVERED", []):
            remaining = max_pallets - total_selected
            if remaining > 0:
                # Add partial to help fill containers (use suggestion if available)
                pallets_to_add = min(PALLETS_PER_CONTAINER, remaining, p.suggested_pallets or PALLETS_PER_CONTAINER)
                if pallets_to_add > 0:
                    p.is_selected = True
                    p.selected_pallets = pallets_to_add
                    total_selected += pallets_to_add
            all_products.append(p)

        # YOUR_CALL products - never auto-select
        all_products.extend(products_by_priority.get("YOUR_CALL", []))

        # Track deferred SKUs (products that couldn't fully fit)
        deferred_skus = []
        for p in all_products:
            if p.suggested_pallets > 0:
                if not p.is_selected:
                    deferred_skus.append(p.sku)
                elif p.selected_pallets < p.suggested_pallets:
                    deferred_skus.append(p.sku)

        # Calculate deferred pallets
        deferred_pallets = max(0, total_needed - total_selected)

        # Calculate utilization percentage
        utilization_pct = Decimal("0")
        if effective_limit > 0:
            utilization_pct = round(Decimal(total_selected) / Decimal(effective_limit) * 100, 1)

        # Identify liquidation candidates (slow movers that could be cleared)
        liquidation_candidates = self._identify_liquidation_candidates(trend_data)
        total_liquidation_pallets = sum(c.current_pallets for c in liquidation_candidates)
        total_liquidation_m2 = sum(c.current_m2 for c in liquidation_candidates)

        # Determine if liquidation is needed and if it could help
        liquidation_needed = deferred_pallets > 0 and len(liquidation_candidates) > 0
        liquidation_could_fit = total_liquidation_pallets >= deferred_pallets

        # Build constraint analysis
        constraint_analysis = ConstraintAnalysis(
            total_needed_pallets=total_needed,
            total_needed_m2=total_needed_m2,
            warehouse_available_pallets=warehouse_available_pallets,
            boat_capacity_pallets=boat_capacity,
            bl_capacity_pallets=bl_capacity,
            limiting_factor=limiting_factor,
            effective_limit_pallets=effective_limit,
            can_order_pallets=total_selected,
            deferred_pallets=deferred_pallets,
            deferred_skus=deferred_skus[:10],  # Top 10 deferred
            constraint_utilization_pct=utilization_pct,
            # Liquidation insight
            liquidation_candidates=liquidation_candidates[:10],  # Top 10 candidates
            total_liquidation_potential_pallets=total_liquidation_pallets,
            total_liquidation_potential_m2=total_liquidation_m2,
            liquidation_needed=liquidation_needed,
            liquidation_could_fit_deferred=liquidation_could_fit,
        )

        logger.debug(
            "bl_capacity_applied",
            num_bls=num_bls,
            bl_capacity=bl_capacity,
            max_pallets=max_pallets,
            total_selected=total_selected,
            products_count=len(all_products),
            limiting_factor=limiting_factor,
            deferred_pallets=deferred_pallets,
            liquidation_candidates=len(liquidation_candidates),
            liquidation_potential=total_liquidation_pallets,
        )

        return all_products, constraint_analysis

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

    def _calculate_recommended_bls(
        self,
        products: list[OrderBuilderProduct]
    ) -> tuple[int, int, str]:
        """
        Calculate recommended BL count based on TRUE NEED and AVAILABLE stock.

        TRUE NEED = coverage_gap - in_transit - in_production
        (What you need, regardless of current factory stock)

        AVAILABLE = factory_available
        (What can ship right now from SIESA)

        Returns:
            tuple[int, int, str]: (recommended_bls, available_bls, reason_string)
        """
        # Calculate TRUE NEED: gap - transit - production
        total_true_need_m2 = Decimal("0")
        total_factory_available_m2 = Decimal("0")

        for p in products:
            # Coverage gap is the base need
            gap = Decimal(str(p.coverage_gap_m2 or 0))
            # Subtract what's already coming
            in_transit = Decimal(str(p.in_transit_m2 or 0))
            in_production = Decimal(str(p.production_requested_m2 or 0))
            # True need = gap - transit - production (floor at 0)
            true_need = max(Decimal("0"), gap - in_transit - in_production)
            total_true_need_m2 += true_need

            # Factory available is what can ship now
            factory_available = Decimal(str(p.factory_available_m2 or 0))
            total_factory_available_m2 += factory_available

        # Calculate recommended BLs based on TRUE NEED
        if total_true_need_m2 > 0:
            need_pallets = math.ceil(float(total_true_need_m2) / float(M2_PER_PALLET))
            need_containers = math.ceil(need_pallets / PALLETS_PER_CONTAINER)
            recommended_bls = max(1, min(5, math.ceil(need_containers / MAX_CONTAINERS_PER_BL)))
        else:
            recommended_bls = 1
            need_containers = 0

        # Calculate available BLs based on factory stock
        if total_factory_available_m2 > 0:
            available_pallets = math.ceil(float(total_factory_available_m2) / float(M2_PER_PALLET))
            available_containers = math.ceil(available_pallets / PALLETS_PER_CONTAINER)
            available_bls = max(1, min(5, math.ceil(available_containers / MAX_CONTAINERS_PER_BL)))
        else:
            available_bls = 0
            available_containers = 0

        # Build reason string showing BOTH need and available
        if total_true_need_m2 <= 0:
            reason = "No coverage gap (stock is adequate)"
        elif total_factory_available_m2 <= 0:
            reason = f"Need: {recommended_bls} BLs ({total_true_need_m2:,.0f} m²) • Available: 0 (SIESA empty)"
        elif available_bls >= recommended_bls:
            reason = f"Need: {recommended_bls} BLs ({total_true_need_m2:,.0f} m²) • Available: {available_bls} BLs ({total_factory_available_m2:,.0f} m²) ✓"
        else:
            reason = f"Need: {recommended_bls} BLs ({total_true_need_m2:,.0f} m²) • Available: {available_bls} BLs ({total_factory_available_m2:,.0f} m²)"

        return recommended_bls, available_bls, reason

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
        # Why are we ordering?
        if stockout_risk_count > 0:
            strategy_sentence = (
                f"Prioritizing {stockout_risk_count} products at stockout risk "
                f"before the {boat_date_str} boat."
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

    def _calculate_section_summaries(
        self,
        all_products: list[OrderBuilderProduct],
        boat: OrderBuilderBoat,
        num_bls: int,
    ) -> tuple[WarehouseOrderSummary, AddToProductionSummary, FactoryRequestSummary]:
        """
        Calculate summaries for the three-section Order Builder view.

        Section 1: Warehouse Order — Products with SIESA stock available now
        Section 2: Add to Production — Items in scheduled production that can have more added
        Section 3: Factory Request — Products needing new production requests

        Args:
            all_products: All Order Builder products with production/factory data
            boat: Target boat info
            num_bls: Number of BLs (determines capacity)

        Returns:
            Tuple of (warehouse_summary, add_to_production_summary, factory_request_summary)
        """
        logger.debug("calculating_section_summaries", product_count=len(all_products))

        # Get boat schedules for matching production ready dates
        available_boats = self.boat_service.get_available(limit=5)
        boat_schedules = [
            (b.vessel_name, b.departure_date, b.order_deadline)
            for b in available_boats
        ]

        # === SECTION 1: WAREHOUSE ORDER ===
        # Products where factory_available_m2 > 0 (can ship from SIESA now)
        warehouse_products = [
            p for p in all_products
            if p.factory_available_m2 and p.factory_available_m2 > 0
        ]

        selected_warehouse = [p for p in warehouse_products if p.is_selected]
        warehouse_total_pallets = sum(p.selected_pallets for p in selected_warehouse)
        warehouse_total_m2 = Decimal(str(warehouse_total_pallets)) * M2_PER_PALLET
        warehouse_total_containers = math.ceil(warehouse_total_pallets / PALLETS_PER_CONTAINER)
        warehouse_total_weight = warehouse_total_m2 * DEFAULT_WEIGHT_PER_M2_KG

        warehouse_summary = WarehouseOrderSummary(
            product_count=len(warehouse_products),
            selected_count=len(selected_warehouse),
            total_m2=warehouse_total_m2,
            total_pallets=warehouse_total_pallets,
            total_containers=warehouse_total_containers,
            total_weight_kg=warehouse_total_weight,
            bl_count=num_bls,
            boat_name=boat.name,
            boat_departure=boat.departure_date,
        )

        # === SECTION 2: ADD TO PRODUCTION ===
        # Products where production_can_add_more=True AND suggested > requested
        add_to_production_items: list[AddToProductionItem] = []

        for p in all_products:
            if not p.production_can_add_more:
                continue

            # Calculate how much more to add
            # suggested_pallets is what Order Builder recommends, convert to m2
            suggested_m2 = Decimal(str(p.suggested_pallets)) * M2_PER_PALLET
            requested_m2 = p.production_requested_m2 or Decimal("0")
            additional_m2 = suggested_m2 - requested_m2

            if additional_m2 <= 0:
                continue

            additional_pallets = int(additional_m2 / M2_PER_PALLET)
            if additional_pallets <= 0:
                continue

            # Find matching boat based on estimated ready date
            target_boat_name = None
            target_boat_departure = None
            estimated_ready = p.production_estimated_ready

            if estimated_ready and boat_schedules:
                # Find first boat whose order deadline is after the ready date
                for b_name, b_departure, b_deadline in boat_schedules:
                    if estimated_ready <= b_deadline:
                        target_boat_name = b_name
                        target_boat_departure = b_departure
                        break

            # Get score from product
            score = p.score.total if p.score else 0
            is_critical = score >= 85

            # Get referencia from production data (or use SKU)
            referencia = p.sku  # Default to SKU

            add_to_production_items.append(AddToProductionItem(
                product_id=p.product_id,
                sku=p.sku,
                description=p.description,
                referencia=referencia,
                current_requested_m2=requested_m2,
                suggested_total_m2=suggested_m2,
                suggested_additional_m2=additional_m2,
                suggested_additional_pallets=additional_pallets,
                estimated_ready_date=estimated_ready,
                target_boat=target_boat_name,
                target_boat_departure=target_boat_departure,
                score=score,
                is_critical=is_critical,
                is_selected=True,  # Pre-select all recommended items
            ))

        # Sort by score (critical first)
        add_to_production_items.sort(key=lambda x: x.score, reverse=True)

        total_additional_m2 = sum(item.suggested_additional_m2 for item in add_to_production_items)
        total_additional_pallets = sum(item.suggested_additional_pallets for item in add_to_production_items)
        has_critical = any(item.is_critical for item in add_to_production_items)

        # No deadline for Add to Production — user can add before production starts
        # The deadline concept was removed as production scheduling is flexible

        add_to_production_summary = AddToProductionSummary(
            product_count=len(add_to_production_items),
            total_additional_m2=total_additional_m2,
            total_additional_pallets=total_additional_pallets,
            items=add_to_production_items,
            estimated_ready_range="4-7 days",
            has_critical_items=has_critical,
            action_deadline=None,  # No deadline — can add before production starts
            action_deadline_display="",  # Empty — no deadline to display
        )

        # === SECTION 3: FACTORY REQUEST ===
        # Dynamic calculation: Project stock at arrival, determine if request needed
        # Enforce 1 container minimum PER PRODUCT with low-volume detection
        factory_request_items: list[FactoryRequestItem] = []

        today = date.today()

        # Get average production time from completed items (dynamic, not hardcoded)
        avg_production_days = self.production_schedule_service.get_average_production_time(fallback_days=7)

        # Calculate when production would be ready if requested now
        next_monday = _get_next_monday(today)  # Factory adds items on Mondays
        estimated_ready_date_global = next_monday + timedelta(days=avg_production_days)

        # Find target boat (first boat departing after production ready)
        target_boat_global = self.boat_service.get_first_boat_after(estimated_ready_date_global)

        # Get boats after target for buffer calculation
        boats_after_target = []
        if target_boat_global:
            boats_after_target = self.boat_service.get_boats_after(target_boat_global.arrival_date, limit=2)

        for p in all_products:
            # Skip items that are in scheduled production (they go in Section 2)
            if p.production_status == "scheduled" and p.production_can_add_more:
                continue

            # Get product data
            warehouse_m2 = p.current_stock_m2 or Decimal("0")
            in_transit_m2 = p.in_transit_m2 or Decimal("0")
            factory_available_m2 = p.factory_available_m2 or Decimal("0")
            in_production_m2 = p.production_requested_m2 or Decimal("0")
            velocity_m2_day = p.daily_velocity_m2 or Decimal("0")
            score = p.score.total if p.score else 0

            # If no target boat, use fallback calculation
            if not target_boat_global:
                # Fallback: Use simple gap calculation
                suggested_m2 = Decimal(str(p.suggested_pallets)) * M2_PER_PALLET
                total_available = warehouse_m2 + in_transit_m2 + factory_available_m2 + in_production_m2
                gap_m2 = suggested_m2 - total_available

                if gap_m2 <= 0:
                    continue

                gap_pallets = int(gap_m2 / M2_PER_PALLET)
                if gap_pallets <= 0:
                    continue

                # Apply minimum with low-volume check
                request_pallets, request_m2, minimum_applied, minimum_note, is_low_volume, low_volume_reason, should_request, skip_reason, days_to_consume = self._apply_container_minimum(
                    gap_m2=gap_m2,
                    gap_pallets=gap_pallets,
                    velocity_m2_day=velocity_m2_day
                )

                factory_request_items.append(FactoryRequestItem(
                    product_id=p.product_id,
                    sku=p.sku,
                    description=p.description,
                    warehouse_m2=warehouse_m2,
                    in_transit_m2=in_transit_m2,
                    factory_available_m2=factory_available_m2,
                    in_production_m2=in_production_m2,
                    suggested_m2=suggested_m2,
                    gap_m2=gap_m2,
                    gap_pallets=gap_pallets,
                    request_m2=request_m2,
                    request_pallets=request_pallets,
                    estimated_ready=f"~{avg_production_days} days",
                    avg_production_days=avg_production_days,
                    velocity_m2_day=velocity_m2_day,
                    # Buffer transparency (no target boat found)
                    buffer_days_applied=settings.production_buffer_days,
                    buffer_note="No target boat found - using fallback calculation",
                    # Low volume detection
                    days_to_consume_container=days_to_consume,
                    is_low_volume=is_low_volume,
                    low_volume_reason=low_volume_reason,
                    should_request=should_request,
                    skip_reason=skip_reason if not should_request else None,
                    urgency=p.urgency,
                    score=score,
                    is_selected=should_request,
                    minimum_applied=minimum_applied,
                    minimum_note=minimum_note,
                ))
                continue

            # Dynamic calculation with target boat
            target_boat = target_boat_global
            arrival_date = target_boat.arrival_date
            days_until_arrival = (arrival_date - today).days

            # Calculate consumption until arrival
            consumption_until_arrival = velocity_m2_day * Decimal(str(days_until_arrival))

            # Pipeline: in-transit + completed production (NOT factory_available — that's for warehouse orders)
            pipeline_m2 = in_transit_m2 + (p.production_completed_m2 or Decimal("0"))

            # Project stock at arrival
            projected_stock = warehouse_m2 + pipeline_m2 - consumption_until_arrival

            # If projected stock >= 0, pipeline covers demand
            if projected_stock >= 0:
                continue  # No request needed

            # Will stockout — calculate need
            future_gap = abs(projected_stock)

            # Add buffer until next boat after target
            if boats_after_target:
                next_boat_arrival = boats_after_target[0].arrival_date
                days_to_next = (next_boat_arrival - arrival_date).days
                buffer_m2 = velocity_m2_day * Decimal(str(days_to_next))
            else:
                buffer_m2 = velocity_m2_day * Decimal("30")  # 30-day buffer fallback

            calculated_need = future_gap + buffer_m2
            gap_pallets = max(1, int(calculated_need / M2_PER_PALLET))

            # Apply 1 container minimum with low-volume detection
            request_pallets, request_m2, minimum_applied, minimum_note, is_low_volume, low_volume_reason, should_request, skip_reason, days_to_consume = self._apply_container_minimum(
                gap_m2=calculated_need,
                gap_pallets=gap_pallets,
                velocity_m2_day=velocity_m2_day
            )

            # Format estimated ready display
            estimated_ready_display = f"{estimated_ready_date_global.strftime('%b %d')} → {target_boat.vessel_name}"

            # Calculate buffer transparency
            buffer_days = settings.production_buffer_days
            safe_ready = estimated_ready_date_global + timedelta(days=buffer_days)
            ready_str = estimated_ready_date_global.strftime("%b %d")
            safe_str = safe_ready.strftime("%b %d")
            deadline_str = target_boat.order_deadline.strftime("%b %d")
            buffer_note = (
                f"{buffer_days}-day buffer applied. "
                f"Ready {ready_str} + {buffer_days} = {safe_str}. "
                f"Deadline {deadline_str}"
            )

            factory_request_items.append(FactoryRequestItem(
                product_id=p.product_id,
                sku=p.sku,
                description=p.description,
                warehouse_m2=warehouse_m2,
                in_transit_m2=in_transit_m2,
                factory_available_m2=factory_available_m2,
                in_production_m2=in_production_m2,
                suggested_m2=calculated_need,
                gap_m2=future_gap,
                gap_pallets=gap_pallets,
                request_m2=request_m2,
                request_pallets=request_pallets,
                estimated_ready=estimated_ready_display,
                avg_production_days=avg_production_days,
                estimated_ready_date=estimated_ready_date_global,
                target_boat=target_boat.vessel_name,
                target_boat_departure=target_boat.departure_date,
                target_boat_order_deadline=target_boat.order_deadline,
                arrival_date=arrival_date,
                days_until_arrival=days_until_arrival,
                # Buffer transparency
                buffer_days_applied=buffer_days,
                safe_ready_date=safe_ready,
                buffer_note=buffer_note,
                # Velocity and consumption
                velocity_m2_day=velocity_m2_day,
                consumption_until_arrival_m2=consumption_until_arrival,
                pipeline_m2=pipeline_m2,
                projected_stock_at_arrival_m2=projected_stock,
                calculated_need_m2=calculated_need,
                days_to_consume_container=days_to_consume,
                is_low_volume=is_low_volume,
                low_volume_reason=low_volume_reason,
                should_request=should_request,
                skip_reason=skip_reason if not should_request else None,
                urgency=p.urgency,
                score=score,
                is_selected=should_request,
                minimum_applied=minimum_applied,
                minimum_note=minimum_note,
            ))

        # Sort: should_request=True first, then by urgency/score
        urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}
        factory_request_items.sort(
            key=lambda x: (
                0 if x.should_request else 1,  # Recommended first
                urgency_order.get(x.urgency, 4),
                -x.score
            )
        )

        # Calculate totals (only for items that should be requested)
        recommended_items = [item for item in factory_request_items if item.should_request]
        total_request_m2 = sum(item.request_m2 for item in recommended_items)
        total_request_pallets = sum(item.request_pallets for item in recommended_items)

        # Monthly limit tracking (60k m²)
        monthly_limit = Decimal("60000")

        try:
            capacity = self.production_schedule_service.get_production_capacity()
            already_requested = capacity.already_requested_m2
        except Exception:
            already_requested = Decimal("0")

        remaining_m2 = monthly_limit - already_requested
        utilization_pct = (already_requested / monthly_limit * 100) if monthly_limit > 0 else Decimal("0")

        # Estimated ready string (dynamic)
        if target_boat_global:
            estimated_ready_str = f"{estimated_ready_date_global.strftime('%b %d')} → {target_boat_global.vessel_name}"
        else:
            estimated_ready_str = f"~{avg_production_days} days"

        # Calculate submit deadline (next Monday for factory schedule)
        submit_deadline = next_monday
        submit_deadline_display = f"Submit by {submit_deadline.strftime('%a, %b %d')}"

        factory_request_summary = FactoryRequestSummary(
            product_count=len(factory_request_items),
            total_request_m2=total_request_m2,
            total_request_pallets=total_request_pallets,
            items=factory_request_items,
            limit_m2=monthly_limit,
            utilization_pct=utilization_pct,
            remaining_m2=remaining_m2,
            estimated_ready=estimated_ready_str,
            submit_deadline=submit_deadline,
            submit_deadline_display=submit_deadline_display,
        )

        logger.info(
            "section_summaries_calculated",
            warehouse_products=len(warehouse_products),
            add_to_production_items=len(add_to_production_items),
            factory_request_items=len(factory_request_items),
            add_to_production_total_m2=float(total_additional_m2),
            factory_request_total_m2=float(total_request_m2),
        )

        return warehouse_summary, add_to_production_summary, factory_request_summary

    def _apply_container_minimum(
        self,
        gap_m2: Decimal,
        gap_pallets: int,
        velocity_m2_day: Decimal
    ) -> tuple[int, Decimal, bool, Optional[str], bool, Optional[str], bool, Optional[str], Optional[int]]:
        """
        Apply 1 container minimum rule with low-volume detection.

        Factory requires minimum 1 container (14 pallets = 1,881.6 m²) PER PRODUCT.
        Products that would take > 1 year to consume 1 container are flagged as low-volume.

        Args:
            gap_m2: Calculated gap in m²
            gap_pallets: Gap converted to pallets
            velocity_m2_day: Daily velocity for this product

        Returns:
            Tuple of:
            - request_pallets: Pallets to request
            - request_m2: m² to request
            - minimum_applied: True if rounded up to minimum
            - minimum_note: Explanation if minimum applied
            - is_low_volume: True if product is low-volume
            - low_volume_reason: Explanation for low-volume flag
            - should_request: True if should include in request
            - skip_reason: Why skipped (if should_request=False)
            - days_to_consume_container: Days to consume 1 container
        """
        # Calculate days to consume 1 container
        days_to_consume: Optional[int] = None
        if velocity_m2_day > 0:
            days_to_consume = int(MIN_CONTAINER_M2 / velocity_m2_day)
        else:
            days_to_consume = None  # No velocity = infinite

        # Case 1: Need >= 1 container
        if gap_pallets >= PALLETS_PER_CONTAINER:
            # Round UP to whole containers
            containers_needed = math.ceil(gap_pallets / PALLETS_PER_CONTAINER)
            request_pallets = containers_needed * PALLETS_PER_CONTAINER
            request_m2 = Decimal(str(request_pallets)) * M2_PER_PALLET
            return (
                request_pallets,
                request_m2,
                False,  # minimum_applied
                None,  # minimum_note
                False,  # is_low_volume
                None,  # low_volume_reason
                True,  # should_request
                None,  # skip_reason
                days_to_consume
            )

        # Case 2: Need < 1 container — check if low-volume
        if velocity_m2_day <= 0:
            # No velocity = definitely low-volume (or no sales data)
            return (
                0,
                Decimal("0"),
                False,
                None,
                True,  # is_low_volume
                "No sales velocity data. 1 container would sit indefinitely.",
                False,  # should_request
                "Low volume — no velocity data",
                None
            )

        if days_to_consume and days_to_consume > LOW_VOLUME_THRESHOLD_DAYS:
            # Low-volume: would take > 1 year to consume 1 container
            years = days_to_consume / 365
            return (
                0,
                Decimal("0"),
                False,
                None,
                True,  # is_low_volume
                f"At {float(velocity_m2_day):.1f} m²/day, 1 container would last {days_to_consume} days (~{years:.1f} years). Special order only.",
                False,  # should_request
                f"Low volume — 1 container lasts {years:.1f} years",
                days_to_consume
            )

        # Case 3: Will consume 1 container within 1 year — request it
        request_pallets = PALLETS_PER_CONTAINER
        request_m2 = MIN_CONTAINER_M2
        minimum_note = (
            f"Calculated need: {int(gap_m2)} m² → "
            f"Rounded to 1 container minimum ({int(MIN_CONTAINER_M2)} m²). "
            f"Will consume in ~{days_to_consume} days."
        )
        return (
            request_pallets,
            request_m2,
            True,  # minimum_applied
            minimum_note,
            False,  # is_low_volume
            None,
            True,  # should_request
            None,
            days_to_consume
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
            days_until_arrival=0,
            days_until_warehouse=0,
            order_deadline=today - timedelta(days=30),
            days_until_order_deadline=-30,
            past_order_deadline=True,
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
            num_bls=num_bls,
            recommended_bls=1,
            recommended_bls_reason="No boats scheduled",
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
