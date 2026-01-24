"""
Trend models for the Intelligence system.

These models define the output shapes for product, country, and customer trends
with statistical confidence metrics and sparkline data.
"""

from datetime import date
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from pydantic import Field

from models.base import BaseSchema


class TrendDirection(str, Enum):
    """Direction of a trend."""

    UP = "up"
    DOWN = "down"
    STABLE = "stable"


class TrendStrength(str, Enum):
    """Strength/magnitude of a trend."""

    STRONG = "strong"
    MODERATE = "moderate"
    WEAK = "weak"


class ConfidenceLevel(str, Enum):
    """Statistical confidence level."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class CustomerTier(str, Enum):
    """Customer tier based on revenue contribution."""

    A = "A"  # Top 20% revenue
    B = "B"  # Next 30% revenue
    C = "C"  # Bottom 50% revenue


class CustomerStatus(str, Enum):
    """Customer activity status."""

    ACTIVE = "active"  # Purchased in last 30 days
    COOLING = "cooling"  # Last purchase 31-90 days ago
    DORMANT = "dormant"  # No purchase in 90+ days


class Predictability(str, Enum):
    """Customer ordering predictability based on coefficient of variation."""

    CLOCKWORK = "CLOCKWORK"      # CV < 0.3 - Very consistent ordering
    PREDICTABLE = "PREDICTABLE"  # CV < 0.5 - Reasonably predictable
    MODERATE = "MODERATE"        # CV < 1.0 - Some variability
    ERRATIC = "ERRATIC"          # CV >= 1.0 - Unpredictable


class SparklinePoint(BaseSchema):
    """Single point in a sparkline."""

    period: str = Field(..., description="Period label (e.g., 'W1', 'W2', 'Jan')")
    value: Decimal = Field(..., description="Value for this period")


class ProductTrend(BaseSchema):
    """Trend data for a single product."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    category: Optional[str] = Field(None, description="Product category")

    # Velocity metrics
    current_velocity_m2_day: Decimal = Field(
        ..., description="Current daily velocity in m²"
    )
    previous_velocity_m2_day: Decimal = Field(
        ..., description="Previous period velocity in m²/day"
    )
    velocity_change_pct: Decimal = Field(
        ..., description="Percent change in velocity"
    )

    # Volume metrics
    total_volume_m2: Decimal = Field(
        ..., description="Total volume sold in period (m²)"
    )
    total_revenue_usd: Decimal = Field(
        ..., description="Total revenue in period (USD)"
    )

    # Trend classification
    direction: TrendDirection = Field(..., description="Trend direction")
    strength: TrendStrength = Field(..., description="Trend strength")

    # Statistical confidence
    coefficient_of_variation: Decimal = Field(
        ..., description="CV = std_dev / mean (lower = more consistent)"
    )
    confidence: ConfidenceLevel = Field(
        ..., description="Confidence in trend assessment"
    )
    sample_count: int = Field(..., description="Number of data points")

    # Inventory context
    days_of_stock: Optional[int] = Field(
        None, description="Days of stock at current velocity"
    )
    current_stock_m2: Optional[Decimal] = Field(
        None, description="Current inventory in m²"
    )

    # Sparkline data
    sparkline: List[SparklinePoint] = Field(
        default_factory=list, description="Time series for sparkline chart"
    )


class CountryBreakdown(BaseSchema):
    """Revenue breakdown for a single country."""

    country_code: str = Field(..., description="ISO country code (e.g., 'CO', 'EC')")
    country_name: str = Field(..., description="Country name")
    total_revenue_usd: Decimal = Field(..., description="Total revenue from country")
    total_volume_m2: Decimal = Field(..., description="Total volume sold (m²)")
    customer_count: int = Field(..., description="Number of unique customers")
    order_count: int = Field(..., description="Number of orders")
    revenue_share_pct: Decimal = Field(..., description="Percent of total revenue")

    # Per-country trend data
    velocity_change_pct: Decimal = Field(
        default=Decimal("0"), description="Volume change vs previous period (%)"
    )
    direction: TrendDirection = Field(
        default=TrendDirection.STABLE, description="Per-country trend direction"
    )
    strength: TrendStrength = Field(
        default=TrendStrength.WEAK, description="Per-country trend strength"
    )
    confidence: ConfidenceLevel = Field(
        default=ConfidenceLevel.LOW, description="Data confidence level"
    )
    top_customers: List[str] = Field(
        default_factory=list, description="Top customer names (by revenue)"
    )
    sparkline: List[SparklinePoint] = Field(
        default_factory=list, description="Weekly volume time series"
    )


class CountryTrend(BaseSchema):
    """Trend data aggregated by country."""

    period_start: date = Field(..., description="Start of analysis period")
    period_end: date = Field(..., description="End of analysis period")
    total_revenue_usd: Decimal = Field(..., description="Total revenue across all countries")
    countries: List[CountryBreakdown] = Field(
        default_factory=list, description="Per-country breakdown"
    )

    # Trend vs previous period
    revenue_change_pct: Optional[Decimal] = Field(
        None, description="Revenue change vs previous period"
    )
    direction: TrendDirection = Field(
        default=TrendDirection.STABLE, description="Overall trend direction"
    )


class ProductPurchase(BaseSchema):
    """Record of a product purchase by a customer."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    total_m2: Decimal = Field(..., description="Total m² purchased")
    total_usd: Decimal = Field(..., description="Total USD spent")
    purchase_count: int = Field(..., description="Number of purchases")
    last_purchase: date = Field(..., description="Most recent purchase date")


class ProductMixChange(BaseSchema):
    """Change in product mix for a customer."""

    sku: str = Field(..., description="Product SKU")
    previous_share_pct: Decimal = Field(
        ..., description="Share of purchases in previous period"
    )
    current_share_pct: Decimal = Field(
        ..., description="Share of purchases in current period"
    )
    change_pct: Decimal = Field(..., description="Change in share percentage")


class CustomerTrend(BaseSchema):
    """Trend data for a single customer."""

    customer_normalized: str = Field(..., description="Normalized customer name")
    customer_original: Optional[str] = Field(None, description="Original customer name")

    # Classification
    tier: CustomerTier = Field(..., description="Customer tier (A/B/C)")
    status: CustomerStatus = Field(..., description="Activity status")
    country_code: Optional[str] = Field(None, description="Inferred country code")

    # Revenue metrics
    total_revenue_usd: Decimal = Field(..., description="Total lifetime revenue")
    period_revenue_usd: Decimal = Field(..., description="Revenue in current period")
    revenue_change_pct: Optional[Decimal] = Field(
        None, description="Revenue change vs previous period"
    )

    # Volume metrics
    total_volume_m2: Decimal = Field(..., description="Total lifetime volume (m²)")
    period_volume_m2: Decimal = Field(..., description="Volume in current period")

    # Purchase patterns
    order_count: int = Field(..., description="Total number of orders")
    avg_order_value_usd: Decimal = Field(..., description="Average order value")
    first_purchase: date = Field(..., description="First purchase date")
    last_purchase: date = Field(..., description="Most recent purchase date")
    days_since_last_purchase: int = Field(..., description="Days since last order")

    # Purchase frequency
    avg_days_between_orders: Optional[Decimal] = Field(
        None, description="Average days between orders"
    )
    gap_std_days: Optional[Decimal] = Field(
        None, description="Standard deviation of days between orders"
    )
    coefficient_of_variation: Optional[Decimal] = Field(
        None, description="CV = std/avg (lower = more predictable)"
    )

    # Pattern-based predictions
    expected_next_date: Optional[date] = Field(
        None, description="Expected next order date based on pattern"
    )
    days_overdue: int = Field(
        default=0, description="Days past expected order date (0 if not overdue)"
    )
    predictability: Optional[str] = Field(
        None, description="CLOCKWORK, PREDICTABLE, MODERATE, or ERRATIC"
    )

    # Product preferences
    top_products: List[ProductPurchase] = Field(
        default_factory=list, description="Top products by revenue"
    )
    product_mix_changes: List[ProductMixChange] = Field(
        default_factory=list, description="Changes in product preferences"
    )

    # Trend assessment
    direction: TrendDirection = Field(..., description="Overall customer trend")
    confidence: ConfidenceLevel = Field(..., description="Confidence in assessment")

    # Sparkline
    sparkline: List[SparklinePoint] = Field(
        default_factory=list, description="Revenue time series"
    )


class IntelligenceDashboard(BaseSchema):
    """Summary dashboard for intelligence overview."""

    period_start: date = Field(..., description="Analysis period start")
    period_end: date = Field(..., description="Analysis period end")

    # Top-level metrics
    total_revenue_usd: Decimal = Field(..., description="Total revenue in period")
    total_volume_m2: Decimal = Field(..., description="Total volume in period")
    active_customers: int = Field(..., description="Customers with purchases in period")
    active_products: int = Field(..., description="Products with sales in period")

    # Trend summaries
    products_trending_up: int = Field(..., description="Products with upward trend")
    products_trending_down: int = Field(..., description="Products with downward trend")
    products_stable: int = Field(..., description="Products with stable trend")

    customers_active: int = Field(..., description="Active customers (30 days)")
    customers_cooling: int = Field(..., description="Cooling customers (31-90 days)")
    customers_dormant: int = Field(..., description="Dormant customers (90+ days)")

    # Pattern-based overdue metrics
    customers_overdue: int = Field(
        default=0, description="Customers past their expected order date"
    )
    tier_a_overdue: int = Field(
        default=0, description="Tier A customers overdue"
    )
    value_at_risk_usd: Decimal = Field(
        default=Decimal("0"), description="Sum of avg order value for overdue customers"
    )

    # Top movers
    top_growing_products: List[ProductTrend] = Field(
        default_factory=list, description="Top 5 products by velocity growth"
    )
    top_declining_products: List[ProductTrend] = Field(
        default_factory=list, description="Top 5 products by velocity decline"
    )
    top_customers: List[CustomerTrend] = Field(
        default_factory=list, description="Top 5 customers by revenue"
    )

    # Geographic summary
    country_breakdown: List[CountryBreakdown] = Field(
        default_factory=list, description="Revenue by country"
    )
