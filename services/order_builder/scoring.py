from decimal import Decimal

from models.order_builder import OrderBuilderProduct, ProductScore, ProductReasoningDisplay, DominantFactor
from config.shipping import M2_PER_PALLET


class ScoringMixin:
    """Priority scoring and reasoning display."""

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
            parts.append(f"High-velocity product ({velocity:.0f} m\u00b2/day)")

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

        return " \u00b7 ".join(parts) if parts else "Standard replenishment"

    def _generate_why_quantity_sentence(self, product: OrderBuilderProduct) -> str:
        """Generate one-sentence explanation of the quantity recommendation."""
        velocity = float(product.daily_velocity_m2 or 0)
        breakdown = product.calculation_breakdown

        if breakdown and velocity > 0:
            coverage_days = breakdown.lead_time_days + breakdown.ordering_cycle_days
            return f"{coverage_days}d coverage \u00d7 {velocity:.1f} m\u00b2/day"
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
