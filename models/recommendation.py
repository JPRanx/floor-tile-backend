"""
Recommendation schemas for order recommendations.

Provides "what to order" based on warehouse allocation
and current stock levels.
"""

from pydantic import Field
from typing import Optional
from decimal import Decimal
from datetime import date
from enum import Enum

from models.base import BaseSchema


class RecommendationPriority(str, Enum):
    """Priority levels for order recommendations."""
    CRITICAL = "CRITICAL"  # Stockout imminent, order arrives too late
    HIGH = "HIGH"          # High rotation (ALTA) products
    MEDIUM = "MEDIUM"      # Medium rotation (MEDIA-ALTA) products
    LOW = "LOW"            # Low rotation products


class WarningType(str, Enum):
    """Types of warnings in recommendations."""
    OVER_STOCKED = "OVER_STOCKED"
    NO_SALES_DATA = "NO_SALES_DATA"
    LOW_VELOCITY = "LOW_VELOCITY"


class ProductAllocation(BaseSchema):
    """Warehouse allocation for a single product."""

    product_id: str
    sku: str
    category: Optional[str] = None
    rotation: Optional[str] = None

    # Velocity metrics
    daily_velocity: Decimal = Field(..., description="Average daily sales (m²)")
    weekly_velocity: Decimal = Field(..., description="Average weekly sales (m²)")
    velocity_std_dev: Decimal = Field(..., description="Standard deviation of weekly sales")
    weeks_of_data: int = Field(..., description="Weeks of sales data used")

    # Allocation calculation
    base_stock_m2: Decimal = Field(..., description="velocity × lead_time")
    safety_stock_m2: Decimal = Field(..., description="std_dev × Z × √lead_time")
    target_m2: Decimal = Field(..., description="base + safety stock")
    target_pallets: Decimal = Field(..., description="target_m2 / 135")

    # After scaling (if total exceeds capacity)
    scaled_target_m2: Optional[Decimal] = None
    scaled_target_pallets: Optional[Decimal] = None
    scale_factor: Optional[Decimal] = None


class ProductRecommendation(BaseSchema):
    """Order recommendation for a single product."""

    product_id: str
    sku: str
    category: Optional[str] = None
    rotation: Optional[str] = None

    # Allocation
    target_pallets: Decimal
    target_m2: Decimal

    # Current stock
    warehouse_pallets: Decimal
    warehouse_m2: Decimal
    in_transit_pallets: Decimal
    in_transit_m2: Decimal
    current_pallets: Decimal = Field(..., description="warehouse + in_transit")
    current_m2: Decimal

    # Gap
    gap_pallets: Decimal = Field(..., description="target - current")
    gap_m2: Decimal

    # Timing
    daily_velocity: Decimal
    days_until_empty: Optional[Decimal] = None
    stockout_date: Optional[date] = None
    order_arrives_date: date = Field(..., description="today + lead_time")
    arrives_before_stockout: bool

    # Priority and action
    priority: RecommendationPriority
    action: str = Field(..., description="Human-readable action")
    reason: str = Field(..., description="Why this recommendation")


class RecommendationWarning(BaseSchema):
    """Warning for products that shouldn't be ordered."""

    product_id: str
    sku: str
    type: WarningType
    message: str
    details: Optional[dict] = None


class WarehouseStatus(BaseSchema):
    """Current warehouse utilization status."""

    total_capacity_pallets: int = Field(default=740)
    total_capacity_m2: Decimal

    # Allocation totals
    total_allocated_pallets: Decimal = Field(..., description="Sum of all target allocations")
    total_allocated_m2: Decimal

    # Current totals
    total_current_pallets: Decimal = Field(..., description="Sum of actual stock")
    total_current_m2: Decimal

    # Utilization
    utilization_percent: Decimal = Field(..., description="current / capacity × 100")
    allocation_scaled: bool = Field(..., description="True if allocations were scaled down")
    scale_factor: Optional[Decimal] = None


class OrderRecommendations(BaseSchema):
    """Complete order recommendations response."""

    # Summary
    warehouse_status: WarehouseStatus
    lead_time_days: int
    calculation_date: date

    # Recommendations (products that need ordering)
    recommendations: list[ProductRecommendation]
    total_recommended_pallets: Decimal
    total_recommended_m2: Decimal

    # Warnings (products to skip)
    warnings: list[RecommendationWarning]

    # Counts by priority
    critical_count: int
    high_count: int
    medium_count: int
    low_count: int
