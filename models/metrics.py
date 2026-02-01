"""
Metrics models — Single source of truth for all business metrics.

These models are returned by MetricsService and used by all pages.
"""

from decimal import Decimal
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class StockCoverage(BaseModel):
    """Complete stock coverage analysis for a single product.

    All pages call MetricsService to get this data.
    Each page decides which fields to display.
    """

    product_id: str
    sku: str

    # Raw inventory
    warehouse_m2: Decimal = Field(..., description="Current warehouse stock")
    in_transit_m2: Decimal = Field(default=Decimal("0"), description="Stock on ships")
    in_transit_arrival_date: Optional[date] = Field(None, description="When transit arrives")
    in_transit_arrival_days: Optional[Decimal] = Field(None, description="Days until transit arrives")

    # Velocity (90-day standard, 2 decimal precision)
    velocity_m2_day: Decimal = Field(..., description="Average daily sales over 90 days")

    # Calculated coverage (ALWAYS calculate both, 2 decimal precision)
    warehouse_days: Optional[Decimal] = Field(None, description="warehouse_m2 / velocity")
    with_transit_days: Optional[Decimal] = Field(None, description="(warehouse + transit) / velocity")

    # Gap analysis (2 decimal precision)
    gap_days: Optional[Decimal] = Field(None, description="Days with no product before transit arrives")
    has_gap: bool = Field(default=False, description="True if stockout before transit arrives")
    stockout_date: Optional[date] = Field(None, description="When warehouse runs out")

    # Boat context (for Order Builder, 2 decimal precision)
    next_boat_arrival_days: Optional[Decimal] = Field(None, description="Days until next boat arrives")
    days_until_boat_stockout: Optional[Decimal] = Field(None, description="Negative = stockout before boat")


class ProductMetrics(BaseModel):
    """All metrics for a single product.

    Combines stock coverage with trend analysis.
    """

    product_id: str
    sku: str
    category: Optional[str] = None
    active: bool = Field(default=True, description="Whether product is active in catalog")

    # Stock coverage (the main data)
    coverage: StockCoverage

    # Trend data (2 decimal precision)
    velocity_change_pct: Decimal = Field(default=Decimal("0"), description="% change vs previous period")
    trend_direction: str = Field(default="STABLE", description="UP, DOWN, or STABLE")
    trend_strength: str = Field(default="WEAK", description="STRONG, MODERATE, or WEAK")

    # Confidence
    confidence: str = Field(default="LOW", description="HIGH, MEDIUM, or LOW")
    sample_count: int = Field(default=0, description="Number of sales records")


class CategoryMetrics(BaseModel):
    """Aggregated metrics for a product category.

    Used by Intelligence page to show warehouse composition and trends by category.
    """

    category: str = Field(..., description="Category name (MADERAS, EXTERIORES, etc.)")

    # Warehouse composition
    warehouse_m2: Decimal = Field(..., description="Total m² in warehouse for this category")
    warehouse_pct: Decimal = Field(..., description="% of total warehouse occupied by this category")
    product_count: int = Field(..., description="Number of products in this category")

    # Sales velocity
    total_velocity_m2_day: Decimal = Field(..., description="Combined daily velocity for all products")
    avg_velocity_m2_day: Decimal = Field(..., description="Average daily velocity per product")

    # Trend (category-level aggregation)
    velocity_change_pct: Decimal = Field(default=Decimal("0"), description="% change vs previous period")
    trend_direction: str = Field(default="STABLE", description="UP, DOWN, or STABLE")
    trend_strength: str = Field(default="WEAK", description="STRONG, MODERATE, or WEAK")

    # Coverage
    avg_warehouse_days: Optional[Decimal] = Field(None, description="Average days of stock across products")
    products_at_risk: int = Field(default=0, description="Products with < 30 days of stock")


class CategoryInsight(BaseModel):
    """Generated insight about a category.

    Examples:
    - "MARMOLIZADOS declining 12% but occupies 28% of warehouse"
    - "MADERAS growing 15% with only 18% warehouse share - consider restocking"
    """

    category: str
    insight_type: str = Field(..., description="Type: IMBALANCE, GROWTH_OPPORTUNITY, RISK, etc.")
    message: str = Field(..., description="Human-readable insight message")
    severity: str = Field(default="INFO", description="INFO, WARNING, or CRITICAL")
