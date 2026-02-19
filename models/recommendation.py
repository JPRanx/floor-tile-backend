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
from models.product_pairing import PairedProductInfo


class RecommendationPriority(str, Enum):
    """Priority levels based on boat arrival dates."""
    HIGH_PRIORITY = "HIGH_PRIORITY"  # Will stock out before next boat arrives
    CONSIDER = "CONSIDER"            # Will stock out before second boat arrives
    WELL_COVERED = "WELL_COVERED"    # Won't stock out for 2+ boat cycles
    YOUR_CALL = "YOUR_CALL"          # No data / needs manual review


class ActionType(str, Enum):
    """Action types for recommendations (what to do with each product)."""
    ORDER_NOW = "ORDER_NOW"        # CRITICAL health → order immediately
    ORDER_SOON = "ORDER_SOON"      # WARNING health → plan for next order
    WELL_STOCKED = "WELL_STOCKED"  # OK/OVERSTOCK at target → no action needed
    SKIP_ORDER = "SKIP_ORDER"      # OK/OVERSTOCK with excess → skip this cycle
    REVIEW = "REVIEW"              # LOW_VOLUME/NO_RECENT_SALES/NO_HISTORY → needs decision


class WarningType(str, Enum):
    """Types of warnings in recommendations."""
    WELL_STOCKED = "WELL_STOCKED"
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

    # Product pairing info (mueble <-> lavamano)
    paired_products: list[PairedProductInfo] = Field(
        default_factory=list,
        description="Paired products (e.g., lavamano for mueble)",
    )
    has_pairing_mismatch: bool = Field(
        default=False,
        description="True if paired inventory is mismatched",
    )


class ConfidenceLevel(str, Enum):
    """Confidence level for velocity estimates."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ProductRecommendation(BaseSchema):
    """Order recommendation for a single product."""

    product_id: str
    sku: str
    category: Optional[str] = None
    rotation: Optional[str] = None

    # Allocation (for inventory health monitoring)
    target_pallets: Decimal
    target_m2: Decimal

    # Current stock
    warehouse_pallets: Decimal
    warehouse_m2: Decimal
    in_transit_pallets: Decimal
    in_transit_m2: Decimal
    current_pallets: Decimal = Field(..., description="warehouse + in_transit")
    current_m2: Decimal

    # Allocation gap (target - current, for inventory health)
    gap_pallets: Decimal = Field(..., description="target - current (allocation gap)")
    gap_m2: Decimal

    # Coverage gap (demand until next boat - available, for Order Builder)
    days_to_cover: Optional[int] = Field(None, description="Days until next boat arrives")
    total_demand_m2: Optional[Decimal] = Field(None, description="Demand during coverage period")
    coverage_gap_m2: Optional[Decimal] = Field(None, description="Demand - available (positive = need to order)")
    coverage_gap_pallets: Optional[int] = Field(None, description="Pallets needed to cover until next boat")

    # Timing
    daily_velocity: Decimal
    days_until_empty: Optional[Decimal] = None
    stockout_date: Optional[date] = None
    order_arrives_date: date = Field(..., description="today + lead_time")
    arrives_before_stockout: bool

    # Confidence score
    confidence: ConfidenceLevel = Field(default=ConfidenceLevel.MEDIUM, description="Confidence in velocity estimate")
    confidence_reason: str = Field(default="", description="Why this confidence level")
    weeks_of_data: int = Field(default=0, description="Weeks of sales history used")
    velocity_cv: Optional[Decimal] = Field(None, description="Coefficient of variation (std_dev / avg)")

    # Customer analysis (for confidence)
    unique_customers: int = Field(default=0, description="Number of distinct customers")
    top_customer_name: Optional[str] = Field(None, description="Name of largest customer")
    top_customer_share: Optional[Decimal] = Field(None, description="Top customer's share of sales (0-1)")
    recurring_customers: int = Field(default=0, description="Customers with 2+ orders")
    recurring_share: Optional[Decimal] = Field(None, description="Share of sales from recurring customers (0-1)")

    # Production schedule integration
    upcoming_production_m2: Optional[Decimal] = Field(None, description="Total m² scheduled in production (next 60 days)")
    next_production_date: Optional[date] = Field(None, description="Next scheduled production date")
    production_before_stockout: Optional[bool] = Field(None, description="Whether production is scheduled before stockout")
    production_covers_gap: Optional[bool] = Field(None, description="Whether production will cover the coverage gap")

    # Product pairing info (mueble <-> lavamano)
    paired_products: list[PairedProductInfo] = Field(
        default_factory=list,
        description="Paired products (e.g., lavamano for mueble)",
    )
    has_pairing_mismatch: bool = Field(
        default=False,
        description="True if paired inventory is mismatched",
    )

    # Priority and action
    priority: RecommendationPriority
    action_type: ActionType = Field(..., description="Action to take (ORDER_NOW, ORDER_SOON, etc.)")
    action: str = Field(..., description="Human-readable action")
    reason: str = Field(..., description="Why this recommendation")


class RecommendationWarning(BaseSchema):
    """Warning for products that shouldn't be ordered."""

    product_id: str
    sku: str
    type: WarningType
    action_type: ActionType = Field(..., description="Action recommendation for this warning")
    message: str
    details: Optional[dict] = None
    # In-transit info (for WELL_STOCKED/OVER_STOCKED warnings)
    in_transit_m2: Optional[Decimal] = Field(None, description="Stock currently in transit")
    in_transit_pallets: Optional[Decimal] = None


class WarehouseStatus(BaseSchema):
    """Current warehouse utilization status."""

    total_capacity_pallets: int = Field(default=740)
    total_capacity_m2: Decimal

    # Allocation totals
    total_allocated_pallets: Decimal = Field(..., description="Sum of all target allocations")
    total_allocated_m2: Decimal

    # Current warehouse stock (for utilization calculation)
    total_current_pallets: Decimal = Field(..., description="Warehouse stock only")
    total_current_m2: Decimal

    # In-transit stock (shown separately, not in utilization)
    total_in_transit_pallets: Decimal = Field(default=Decimal("0"), description="Stock in transit")
    total_in_transit_m2: Decimal = Field(default=Decimal("0"))

    # Utilization (warehouse only / capacity)
    utilization_percent: Decimal = Field(..., description="warehouse / capacity × 100")
    allocation_scaled: bool = Field(..., description="True if allocations were scaled down")
    scale_factor: Optional[Decimal] = None


class OrderRecommendations(BaseSchema):
    """Complete order recommendations response."""

    # Summary
    warehouse_status: WarehouseStatus
    lead_time_days: int
    calculation_date: date

    # Boat arrival info (for coverage gap context)
    next_boat_arrival: Optional[date] = None
    days_to_next_boat: Optional[int] = None

    # Recommendations (products that need ordering)
    recommendations: list[ProductRecommendation]
    total_recommended_pallets: Decimal
    total_recommended_m2: Decimal

    # Coverage gap totals
    total_coverage_gap_pallets: int = 0
    total_coverage_gap_m2: Decimal = Decimal("0")

    # Warnings (products to skip)
    warnings: list[RecommendationWarning]

    # Counts by priority (boat-based)
    high_priority_count: int = 0
    consider_count: int = 0
    well_covered_count: int = 0
    your_call_count: int = 0

    # Counts by action
    order_now_count: int = 0
    order_soon_count: int = 0
    well_stocked_count: int = 0
    skip_order_count: int = 0
    review_count: int = 0
