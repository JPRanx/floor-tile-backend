"""
Order Builder schemas for the hero feature.

Order Builder answers: "What should I order for the next boat?"
Combines coverage gap, confidence, and multi-level optimization.
"""

from pydantic import Field
from typing import Optional
from decimal import Decimal
from datetime import date
from enum import Enum

from models.base import BaseSchema


class Urgency(str, Enum):
    """Urgency classification based on days of stock."""
    CRITICAL = "critical"  # <7 days
    URGENT = "urgent"      # 7-14 days
    SOON = "soon"          # 14-30 days
    OK = "ok"              # 30+ days


class TrendDirection(str, Enum):
    """Direction of product demand trend."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"


class TrendStrength(str, Enum):
    """Strength of trend movement."""
    STRONG = "strong"      # >20% change
    MODERATE = "moderate"  # 5-20% change
    WEAK = "weak"          # <5% change


class CalculationBreakdown(BaseSchema):
    """Breakdown of how suggested quantity was calculated."""

    # Time parameters
    lead_time_days: int = Field(..., description="Days until boat arrives")
    safety_stock_days: int = Field(default=14, description="Safety buffer days")

    # Velocity
    daily_velocity_m2: Decimal = Field(..., description="Average daily demand in m²")

    # Calculation steps
    base_quantity_m2: Decimal = Field(..., description="(lead_time + safety) × velocity")
    trend_adjustment_m2: Decimal = Field(default=Decimal("0"), description="Adjustment for trend")
    trend_adjustment_pct: Decimal = Field(default=Decimal("0"), description="Trend adjustment percentage")
    minus_current_stock_m2: Decimal = Field(..., description="Subtract warehouse stock")
    minus_incoming_m2: Decimal = Field(default=Decimal("0"), description="Subtract in-transit stock")
    final_suggestion_m2: Decimal = Field(..., description="Final recommended quantity")
    final_suggestion_pallets: int = Field(..., description="Final recommendation in pallets")


class OrderBuilderMode(str, Enum):
    """Order builder optimization modes."""
    MINIMAL = "minimal"    # 3 containers - only HIGH_PRIORITY
    STANDARD = "standard"  # 4 containers - HIGH_PRIORITY + CONSIDER
    OPTIMAL = "optimal"    # 5 containers - fill boat with WELL_COVERED


class OrderBuilderAlertType(str, Enum):
    """Alert severity types."""
    WARNING = "warning"
    BLOCKED = "blocked"
    SUGGESTION = "suggestion"


class OrderBuilderProduct(BaseSchema):
    """Product in Order Builder with selection state."""

    # Product info
    product_id: str
    sku: str
    description: Optional[str] = None

    # Priority (from stockout service)
    priority: str = Field(..., description="HIGH_PRIORITY, CONSIDER, WELL_COVERED, YOUR_CALL")
    action_type: str = Field(..., description="ORDER_NOW, ORDER_SOON, WELL_STOCKED, etc.")

    # Coverage gap
    current_stock_m2: Decimal = Field(..., description="Warehouse stock in m2")
    in_transit_m2: Decimal = Field(default=Decimal("0"), description="In-transit stock in m2")
    days_to_cover: int = Field(..., description="Days until next boat arrival")
    total_demand_m2: Decimal = Field(..., description="Demand during coverage period")
    coverage_gap_m2: Decimal = Field(..., description="Demand - available (positive = need)")
    coverage_gap_pallets: int = Field(..., description="Gap converted to pallets")
    suggested_pallets: int = Field(..., description="System suggestion based on gap")

    # Confidence
    confidence: str = Field(..., description="HIGH, MEDIUM, LOW")
    confidence_reason: str = Field(default="", description="Why this confidence level")
    unique_customers: int = Field(default=0, description="Number of distinct customers")
    top_customer_name: Optional[str] = None
    top_customer_share: Optional[Decimal] = None

    # Factory (MVP: placeholder)
    factory_available: Optional[int] = Field(None, description="Pallets available at factory")
    factory_status: str = Field(default="unknown", description="available, partial, blocked, unknown")

    # Trend data (from Intelligence system)
    urgency: str = Field(default="ok", description="critical, urgent, soon, ok")
    days_of_stock: Optional[int] = Field(None, description="Days of stock at current velocity")
    trend_direction: str = Field(default="stable", description="up, down, stable")
    trend_strength: str = Field(default="weak", description="strong, moderate, weak")
    velocity_change_pct: Decimal = Field(default=Decimal("0"), description="Percent change in velocity")
    daily_velocity_m2: Decimal = Field(default=Decimal("0"), description="Current daily velocity in m²")

    # Calculation breakdown (transparency)
    calculation_breakdown: Optional[CalculationBreakdown] = Field(
        None, description="How the suggestion was calculated"
    )

    # Selection state (editable by user)
    is_selected: bool = Field(default=False, description="Whether product is in order")
    selected_pallets: int = Field(default=0, description="Editable quantity")


class OrderBuilderBoat(BaseSchema):
    """Boat information for Order Builder."""

    boat_id: str
    name: str
    departure_date: date
    arrival_date: date
    days_until_departure: int
    booking_deadline: date
    days_until_deadline: int
    max_containers: int = Field(default=5, description="3-5, default 5")


class OrderBuilderAlert(BaseSchema):
    """Alert/warning in Order Builder."""

    type: OrderBuilderAlertType
    icon: str = Field(..., description="Emoji icon for display")
    product_sku: Optional[str] = None
    message: str


class OrderBuilderSummary(BaseSchema):
    """Summary of current order selection."""

    # Current selection totals
    total_pallets: int = Field(default=0)
    total_containers: int = Field(default=0)
    total_m2: Decimal = Field(default=Decimal("0"))

    # Boat capacity
    boat_max_containers: int = Field(default=5)
    boat_remaining_containers: int = Field(default=5)

    # Warehouse capacity
    warehouse_current_pallets: int = Field(default=0)
    warehouse_capacity: int = Field(default=740)
    warehouse_after_delivery: int = Field(default=0)
    warehouse_utilization_after: Decimal = Field(default=Decimal("0"), description="Percentage 0-100")

    # Alerts
    alerts: list[OrderBuilderAlert] = Field(default_factory=list)


class OrderBuilderResponse(BaseSchema):
    """Complete Order Builder API response."""

    # Boat info
    boat: OrderBuilderBoat
    next_boat: Optional[OrderBuilderBoat] = None

    # Mode
    mode: OrderBuilderMode

    # Products grouped by priority
    high_priority: list[OrderBuilderProduct] = Field(default_factory=list)
    consider: list[OrderBuilderProduct] = Field(default_factory=list)
    well_covered: list[OrderBuilderProduct] = Field(default_factory=list)
    your_call: list[OrderBuilderProduct] = Field(default_factory=list)

    # Summary
    summary: OrderBuilderSummary


# ===================
# CONFIRM ORDER (Create Factory Order)
# ===================

class ConfirmOrderProductItem(BaseSchema):
    """Product item for order confirmation."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    pallets: int = Field(..., gt=0, description="Number of pallets to order")


class ConfirmOrderRequest(BaseSchema):
    """Request to confirm order and create factory_order."""

    boat_id: str = Field(..., description="Boat schedule UUID")
    boat_name: str = Field(..., description="Boat name for notes")
    boat_departure: date = Field(..., description="Boat departure date")
    products: list[ConfirmOrderProductItem] = Field(
        ...,
        min_length=1,
        description="Selected products with pallets"
    )
    pv_number: Optional[str] = Field(
        None,
        description="Optional PV number. Auto-generated if not provided."
    )
    notes: Optional[str] = Field(None, description="Optional order notes")


class ConfirmOrderResponse(BaseSchema):
    """Response after confirming order."""

    factory_order_id: str = Field(..., description="Created factory order UUID")
    pv_number: str = Field(..., description="PV number (e.g., PV-20260108-001)")
    status: str = Field(..., description="Order status (PENDING)")
    order_date: date = Field(..., description="Order date")
    items_count: int = Field(..., description="Number of line items")
    total_m2: Decimal = Field(..., description="Total m² ordered")
    total_pallets: int = Field(..., description="Total pallets ordered")
    created_at: str = Field(..., description="Created timestamp")
