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


# ===================
# REASONING DATA STRUCTURES
# ===================

class PrimaryFactor(str, Enum):
    """Primary factor driving the recommendation."""
    LOW_STOCK = "LOW_STOCK"          # Days of stock < 14
    TRENDING_UP = "TRENDING_UP"      # Strong upward demand trend
    OVERSTOCKED = "OVERSTOCKED"      # >180 days stock + declining
    DECLINING = "DECLINING"          # Significant demand drop
    NO_SALES = "NO_SALES"            # Zero velocity
    NO_DATA = "NO_DATA"              # Insufficient data
    STABLE = "STABLE"                # Normal levels


class StockAnalysis(BaseSchema):
    """Stock position analysis for reasoning."""
    current_m2: Decimal = Field(..., description="Current warehouse stock in m²")
    days_of_stock: Optional[Decimal] = Field(None, description="Days of stock at current velocity")
    days_to_boat: int = Field(..., description="Days until boat arrival")
    gap_days: Optional[Decimal] = Field(None, description="Negative = stockout before boat")


class DemandAnalysis(BaseSchema):
    """Demand analysis for reasoning."""
    velocity_m2_day: Decimal = Field(..., description="Current daily velocity in m²")
    trend_pct: Decimal = Field(default=Decimal("0"), description="Percent change in velocity")
    trend_direction: str = Field(default="stable", description="up, down, stable")
    sales_rank: Optional[int] = Field(None, description="Rank by sales volume (1 = top seller)")


class QuantityReasoning(BaseSchema):
    """Quantity calculation reasoning."""
    target_coverage_days: int = Field(..., description="Target days of coverage")
    m2_needed: Decimal = Field(..., description="Total m² needed for coverage")
    m2_in_transit: Decimal = Field(default=Decimal("0"), description="m² currently in transit")
    m2_in_stock: Decimal = Field(..., description="m² currently in warehouse")
    m2_to_order: Decimal = Field(..., description="m² recommended to order")


class ProductReasoning(BaseSchema):
    """Complete reasoning for a product recommendation."""
    primary_factor: str = Field(..., description="Main driver: LOW_STOCK, TRENDING_UP, etc.")
    stock: StockAnalysis
    demand: DemandAnalysis
    quantity: QuantityReasoning
    exclusion_reason: Optional[str] = Field(None, description="Why not recommended (if applicable)")


class ExcludedProduct(BaseSchema):
    """Product excluded from recommendations with reason."""
    sku: str
    product_name: Optional[str] = None
    reason: str = Field(..., description="OVERSTOCKED, NO_SALES, DECLINING, NO_DATA")
    days_of_stock: Optional[Decimal] = None
    trend_pct: Optional[Decimal] = None
    last_sale_days_ago: Optional[int] = None


# ===================
# PRIORITY SCORING (Layer 2 & 4)
# ===================

class DominantFactor(str, Enum):
    """Which factor contributed most to the score."""
    STOCKOUT = "stockout"
    CUSTOMER = "customer"
    TREND = "trend"
    REVENUE = "revenue"


class ProductScore(BaseSchema):
    """Weighted priority score breakdown (0-100 total)."""
    total: int = Field(..., ge=0, le=100, description="Composite score 0-100")
    stockout_risk: int = Field(..., ge=0, le=40, description="0-40 points: Days until stockout")
    customer_demand: int = Field(..., ge=0, le=30, description="0-30 points: Customers waiting")
    growth_trend: int = Field(..., ge=0, le=20, description="0-20 points: Demand trend")
    revenue_impact: int = Field(..., ge=0, le=10, description="0-10 points: Sales velocity")


class ProductReasoningDisplay(BaseSchema):
    """
    Human-readable reasoning for product card display.

    This is separate from ProductReasoning which stores calculation details.
    This is for UI presentation.
    """
    why_product_sentence: str = Field(
        ...,
        description="One sentence explaining why: 'Out of stock · 2 customers waiting'"
    )
    why_quantity_sentence: str = Field(
        ...,
        description="One sentence explaining quantity: '63d coverage × 30 m²/day'"
    )
    dominant_factor: str = Field(
        ...,
        description="Which factor dominates: stockout, customer, trend, revenue"
    )
    would_include_if: Optional[str] = Field(
        None,
        description="For excluded products: 'Stock drops below 60 days' (Phase 3)"
    )


class OrderReasoning(BaseSchema):
    """Structured reasoning narrative for order strategy."""

    # Core narrative sentences (1 sentence each)
    strategy_sentence: str = Field(
        ...,
        description="Why this order: 'Prioritizing 22 products at stockout risk...'"
    )
    risk_sentence: str = Field(
        ...,
        description="Biggest risk: 'TOLU BEIGE is most critical (0 days)...'"
    )
    constraint_sentence: str = Field(
        ...,
        description="What limits: 'Warehouse space is the limit...'"
    )
    customer_sentence: Optional[str] = Field(
        None,
        description="Customer signal: '3 customers expected to order soon...'"
    )

    # Supporting facts for UI badges/pills
    limiting_factor: str = Field(
        default="none",
        description="warehouse, boat, mode, or none"
    )
    deferred_count: int = Field(
        default=0,
        description="Pallets deferred to next boat"
    )
    customers_expecting: int = Field(
        default=0,
        description="Customers expected to order soon"
    )
    critical_count: int = Field(
        default=0,
        description="Products at stockout risk"
    )
    highest_risk_sku: Optional[str] = Field(
        None,
        description="SKU with lowest days of stock"
    )
    highest_risk_days: Optional[int] = Field(
        None,
        description="Days of stock for highest risk product"
    )


class OrderSummaryReasoning(BaseSchema):
    """Order-level reasoning and strategy summary."""
    strategy: str = Field(..., description="STOCKOUT_PREVENTION, DEMAND_CAPTURE, BALANCED")
    days_to_boat: int = Field(..., description="Days until boat departure")
    boat_date: str = Field(..., description="Boat departure date")
    boat_name: str = Field(..., description="Boat/vessel name")
    critical_count: int = Field(default=0, description="Products with critical urgency")
    urgent_count: int = Field(default=0, description="Products with urgent priority")
    stable_count: int = Field(default=0, description="Products with stable stock")
    excluded_count: int = Field(default=0, description="Products excluded from recommendations")
    key_insights: list[str] = Field(default_factory=list, description="Top insights about the order (legacy)")
    excluded_products: list[ExcludedProduct] = Field(default_factory=list, description="Products not recommended")

    # NEW: Structured reasoning narrative
    reasoning: Optional[OrderReasoning] = Field(
        None,
        description="Structured narrative explaining order strategy"
    )


class CalculationBreakdown(BaseSchema):
    """Breakdown of how suggested quantity was calculated."""

    # Time parameters
    lead_time_days: int = Field(..., description="Days until product in warehouse")
    ordering_cycle_days: int = Field(default=30, description="Days until next boat arrives")

    # Velocity
    daily_velocity_m2: Decimal = Field(..., description="Average daily demand in m²")

    # Calculation steps
    base_quantity_m2: Decimal = Field(..., description="(lead_time + ordering_cycle) × velocity")
    trend_adjustment_m2: Decimal = Field(default=Decimal("0"), description="Adjustment for trend")
    trend_adjustment_pct: Decimal = Field(default=Decimal("0"), description="Trend adjustment percentage")
    minus_current_stock_m2: Decimal = Field(..., description="Subtract warehouse stock")
    minus_incoming_m2: Decimal = Field(default=Decimal("0"), description="Subtract in-transit stock")
    final_suggestion_m2: Decimal = Field(..., description="Final recommended quantity")
    final_suggestion_pallets: int = Field(..., description="Final recommendation in pallets")


# ===================
# FULL CALCULATION BREAKDOWN (Transparency Layer)
# ===================

class CoverageCalculation(BaseSchema):
    """
    Coverage Gap Calculation — Shows how we determine m² needed.

    Formula: coverage_gap = (velocity × target_days) - warehouse - in_transit - pending_orders
    """
    # Target days breakdown
    target_coverage_days: int = Field(..., description="Target days of coverage (days_to_warehouse + buffer)")
    days_to_warehouse: int = Field(..., description="Days until product in warehouse")
    buffer_days: int = Field(default=30, description="Safety buffer days")

    # Velocity inputs
    velocity_m2_per_day: Decimal = Field(..., description="Daily velocity in m²")
    velocity_source: str = Field(
        default="90d",
        description="Which velocity used: '90d', '180d', 'blended'"
    )

    # Need calculation
    need_for_target_m2: Decimal = Field(..., description="velocity × target_days = raw need")

    # Trend adjustment (optional)
    trend_direction: str = Field(default="stable", description="up, down, stable")
    velocity_change_pct: Decimal = Field(
        default=Decimal("0"),
        description="Raw velocity change: (90d - 180d) / 180d × 100 (e.g., -52% = demand down 52%)"
    )
    trend_adjustment_pct: Decimal = Field(
        default=Decimal("0"),
        description="Order quantity adjustment (capped at ±20%)"
    )
    trend_adjustment_m2: Decimal = Field(default=Decimal("0"), description="trend_pct × need")
    adjusted_need_m2: Decimal = Field(..., description="need + trend_adjustment")

    # Current position
    warehouse_m2: Decimal = Field(..., description="Current warehouse stock")
    in_transit_m2: Decimal = Field(default=Decimal("0"), description="Stock in transit")
    pending_order_m2: Decimal = Field(default=Decimal("0"), description="Pending warehouse orders (awaiting shipment)")

    # Gap result
    coverage_gap_m2: Decimal = Field(..., description="adjusted_need - warehouse - in_transit - pending_orders")
    coverage_gap_pallets: int = Field(..., description="Gap converted to pallets")

    # Suggestion from coverage alone
    suggested_pallets: int = Field(..., description="Pallets suggested by coverage gap")
    suggested_m2: Decimal = Field(..., description="m² suggested by coverage gap")


class CustomerDemandCalculation(BaseSchema):
    """
    Customer Demand Calculation — Shows expected orders from customers due soon.

    Looks at customers whose order cycle is due and calculates expected m².
    """
    # Customer inputs
    customers_expecting_count: int = Field(..., description="Number of customers due to order")
    customers_list: list[str] = Field(
        default_factory=list,
        description="Names of customers expecting this product"
    )

    # Expected volume
    expected_orders_m2: Decimal = Field(..., description="Sum of expected m² from all customers")
    expected_orders_pallets: int = Field(..., description="Expected orders in pallets")

    # Breakdown by customer (optional, for detail view)
    customer_breakdown: list[dict] = Field(
        default_factory=list,
        description="Per-customer: {name, avg_m2, tier, days_overdue}"
    )

    # Final suggestion from customer demand
    suggested_pallets: int = Field(..., description="Pallets suggested by customer demand")

    # Score
    customer_demand_score: int = Field(
        default=0,
        description="Priority score from customer demand (0-300)"
    )


class SelectionCalculation(BaseSchema):
    """
    Selection Calculation — Shows how final selected_pallets was determined.

    Combines coverage + customer demand, applies minimums and constraints.
    """
    # Input sources
    coverage_suggested_pallets: int = Field(..., description="From CoverageCalculation")
    customer_suggested_pallets: int = Field(..., description="From CustomerDemandCalculation")

    # Combined (higher of the two)
    combined_pallets: int = Field(..., description="max(coverage, customer) = base selection")
    combination_reason: str = Field(
        ...,
        description="'coverage_driven', 'customer_driven', or 'equal'"
    )

    # Minimum container rule
    minimum_container_applied: bool = Field(default=False, description="Was minimum applied?")
    minimum_container_pallets: int = Field(default=14, description="1 container = 14 pallets")
    after_minimum_pallets: int = Field(..., description="After applying minimum (if selected)")

    # SIESA constraint
    siesa_available_m2: Decimal = Field(..., description="Factory finished goods available")
    siesa_available_pallets: int = Field(..., description="SIESA in pallets")
    siesa_limited: bool = Field(default=False, description="Was selection capped by SIESA?")

    # Final selection
    final_selected_pallets: int = Field(..., description="Final selected_pallets value")
    final_selected_m2: Decimal = Field(..., description="Final in m²")

    # Human-readable reason
    selection_reason: str = Field(
        ...,
        description="E.g., 'Customer demand (18 expecting) + minimum container rule'"
    )

    # Constraint notes
    constraint_notes: list[str] = Field(
        default_factory=list,
        description="Any constraints applied: 'Capped at SIESA: 202 m²'"
    )


class FullCalculationBreakdown(BaseSchema):
    """
    Complete calculation transparency for a product.

    Shows all three calculation stages:
    1. Coverage: How much do we need based on velocity?
    2. Customer: How much do expected customers want?
    3. Selection: How did we pick the final number?
    """
    coverage: CoverageCalculation = Field(..., description="Coverage gap calculation")
    customer_demand: CustomerDemandCalculation = Field(..., description="Customer demand calculation")
    selection: SelectionCalculation = Field(..., description="Final selection logic")

    # Summary for quick display
    summary_sentence: str = Field(
        ...,
        description="One-line summary: 'Selected 14p: 0p coverage + 18 customers expecting → min container'"
    )


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


class AvailabilityBreakdown(BaseSchema):
    """Full breakdown of what's available for this boat."""

    # Components
    siesa_now_m2: Decimal = Field(
        default=Decimal("0"),
        description="Current SIESA finished goods stock"
    )
    production_completing_m2: Decimal = Field(
        default=Decimal("0"),
        description="Production ready before order deadline"
    )

    # Totals
    total_available_m2: Decimal = Field(
        default=Decimal("0"),
        description="siesa + production_completing"
    )

    # Order context
    suggested_order_m2: Decimal = Field(
        default=Decimal("0"),
        description="What system recommends"
    )

    # Gap analysis
    shortfall_m2: Decimal = Field(
        default=Decimal("0"),
        description="suggested - available (0 if available >= suggested)"
    )
    can_fulfill: bool = Field(
        default=False,
        description="True if available >= suggested"
    )

    # For display
    shortfall_note: Optional[str] = Field(
        None,
        description="e.g., '135 m² needs future production'"
    )


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

    # Pending orders (already ordered, awaiting shipment from factory)
    pending_order_m2: Decimal = Field(default=Decimal("0"), description="m² already ordered, awaiting shipment")
    pending_order_pallets: int = Field(default=0, description="Pallets already ordered")
    pending_order_boat: Optional[str] = Field(default=None, description="Which boat the pending order is on")

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

    # Factory production status (from production_schedule)
    factory_status: str = Field(
        default="not_scheduled",
        description="in_production, not_scheduled"
    )
    factory_production_date: Optional[date] = Field(
        None,
        description="When production completes"
    )
    factory_production_m2: Optional[Decimal] = Field(
        None,
        description="Total m² in production"
    )
    days_until_factory_ready: Optional[int] = Field(
        None,
        description="Days until production_date"
    )
    factory_ready_before_boat: Optional[bool] = Field(
        None,
        description="True if production completes before boat deadline"
    )
    factory_timing_message: Optional[str] = Field(
        None,
        description="Human-readable factory timing status"
    )

    # Factory availability (SIESA finished goods)
    factory_available_m2: Decimal = Field(
        default=Decimal("0"),
        description="Finished goods available at factory in m²"
    )
    factory_largest_lot_m2: Optional[Decimal] = Field(
        None,
        description="Size of largest available lot in m²"
    )
    factory_largest_lot_code: Optional[str] = Field(
        None,
        description="Lot code of largest available lot"
    )
    factory_lot_count: int = Field(
        default=0,
        description="Number of lots available at factory"
    )
    factory_fill_status: str = Field(
        default="unknown",
        description="single_lot, mixed_lots, needs_production, no_stock"
    )
    factory_fill_message: Optional[str] = Field(
        None,
        description="Human-readable factory fill status"
    )

    # === PRODUCTION SCHEDULE STATUS (from production_schedule table) ===
    # These fields track items from the "Programa de Produccion" Excel
    production_status: str = Field(
        default="not_scheduled",
        description="Production status: 'scheduled', 'in_progress', 'completed', 'not_scheduled'"
    )
    production_requested_m2: Decimal = Field(
        default=Decimal("0"),
        description="m² requested from factory (Programa column)"
    )
    production_completed_m2: Decimal = Field(
        default=Decimal("0"),
        description="m² already completed (Real column)"
    )
    production_can_add_more: bool = Field(
        default=False,
        description="True if status='scheduled' - production hasn't started, CAN ADD MORE!"
    )
    production_estimated_ready: Optional[date] = Field(
        default=None,
        description="When production expected to complete"
    )

    # === PRE-PRODUCTION ALERT ===
    # Alert when user should add more to a scheduled production request
    production_add_more_m2: Decimal = Field(
        default=Decimal("0"),
        description="Additional m² suggested to add before production starts"
    )
    production_add_more_alert: Optional[str] = Field(
        default=None,
        description="Alert message if user should add more: 'Add 1,000 m² before production starts!'"
    )

    # Trend data (from Intelligence system)
    urgency: str = Field(default="ok", description="critical, urgent, soon, ok")
    days_of_stock: Optional[int] = Field(None, description="Days of stock at current velocity")
    trend_direction: str = Field(default="stable", description="up, down, stable")
    trend_strength: str = Field(default="weak", description="strong, moderate, weak")
    velocity_change_pct: Decimal = Field(default=Decimal("0"), description="Percent change in velocity")
    daily_velocity_m2: Decimal = Field(default=Decimal("0"), description="Current daily velocity in m²")

    # Dual velocity system (90-day vs 6-month comparison)
    velocity_90d_m2: Decimal = Field(default=Decimal("0"), description="90-day daily velocity in m²")
    velocity_180d_m2: Decimal = Field(default=Decimal("0"), description="6-month daily velocity in m²")
    velocity_trend_signal: str = Field(
        default="stable",
        description="growing (90d > 180d by 20%+), stable (within 20%), declining (90d < 180d by 20%+)"
    )
    velocity_trend_ratio: Decimal = Field(
        default=Decimal("1.0"),
        description="Ratio of 90d/180d velocity (e.g., 1.5 = 90d is 50% higher)"
    )

    # Calculation breakdown (transparency)
    calculation_breakdown: Optional[CalculationBreakdown] = Field(
        None, description="How the suggestion was calculated"
    )

    # Reasoning (explains WHY this recommendation)
    reasoning: Optional[ProductReasoning] = Field(
        None, description="Detailed reasoning for this recommendation"
    )

    # Priority score (Layer 2 scoring system)
    score: Optional[ProductScore] = Field(
        None, description="Weighted priority score breakdown (0-100)"
    )

    # Display reasoning (Layer 4 per-product explanation)
    reasoning_display: Optional[ProductReasoningDisplay] = Field(
        None, description="Human-readable reasoning for UI display"
    )

    # Weight data (for container optimization)
    weight_per_m2_kg: Decimal = Field(default=Decimal("14.90"), description="Weight per m² in kg")
    total_weight_kg: Decimal = Field(default=Decimal("0"), description="Total weight for selected pallets")

    # Customer demand signal (for intelligent prioritization)
    customer_demand_score: int = Field(
        default=0,
        description="Priority score based on customers due soon who buy this product (0-300)"
    )
    customers_expecting_count: int = Field(
        default=0,
        description="Number of customers due soon who typically buy this product"
    )
    expected_customer_orders_m2: Decimal = Field(
        default=Decimal("0"),
        description="Expected m² from customers due to order soon (added to coverage gap)"
    )
    expected_orders_note: Optional[str] = Field(
        default=None,
        description="Explanation of expected customer orders, e.g. 'Includes 500 m² from 3 expected orders'"
    )

    # Selection state (editable by user)
    is_selected: bool = Field(default=False, description="Whether product is in order")
    selected_pallets: int = Field(default=0, description="Editable quantity")
    selection_constraint_note: Optional[str] = Field(
        default=None,
        description="Note if selection was limited (e.g., 'Capped at SIESA available: 202 m²')"
    )

    # NEW: Full availability breakdown for this boat
    availability_breakdown: Optional[AvailabilityBreakdown] = Field(
        None,
        description="Full breakdown of what's available for this boat"
    )

    # NEW: Full calculation breakdown (transparency layer)
    full_calculation_breakdown: Optional[FullCalculationBreakdown] = Field(
        None,
        description="Complete calculation showing coverage + customer + selection math"
    )


class OrderBuilderBoat(BaseSchema):
    """Boat information for Order Builder."""

    boat_id: str
    name: str
    departure_date: date
    arrival_date: date
    days_until_departure: int
    days_until_arrival: int = Field(..., description="Days until boat arrives at port")
    days_until_warehouse: int = Field(..., description="Days until product in warehouse (arrival + port + trucking)")
    order_deadline: date = Field(..., description="Recommended order deadline (30 days before departure)")
    days_until_order_deadline: int = Field(..., description="Days until order deadline (can be negative)")
    past_order_deadline: bool = Field(default=False, description="True if past recommended order deadline")
    booking_deadline: date
    days_until_deadline: int
    max_containers: int = Field(default=5, description="3-5, default 5")
    carrier: Optional[str] = Field(None, description="Freight carrier (e.g., TIBA, SEABOARD)")


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

    # Weight-based container calculation
    total_weight_kg: Decimal = Field(default=Decimal("0"), description="Total weight of selection in kg")
    containers_by_pallets: int = Field(default=0, description="Containers needed by pallet count")
    containers_by_weight: int = Field(default=0, description="Containers needed by weight limit")
    weight_is_limiting: bool = Field(default=False, description="True if weight > pallets for container calc")

    # Boat capacity
    boat_max_containers: int = Field(default=5)
    boat_remaining_containers: int = Field(default=5)

    # Warehouse capacity — service populates from settings.warehouse_max_pallets
    warehouse_current_pallets: int = Field(default=0)
    warehouse_capacity: int = Field(default=672, description="See settings.warehouse_max_pallets")
    warehouse_after_delivery: int = Field(default=0)
    warehouse_utilization_after: Decimal = Field(default=Decimal("0"), description="Percentage 0-100")

    # Alerts
    alerts: list[OrderBuilderAlert] = Field(default_factory=list)


class LiquidationClearanceProduct(BaseSchema):
    """Deactivated product with remaining SIESA factory stock to clear out."""
    product_id: str
    sku: str
    description: Optional[str] = None
    factory_available_m2: Decimal          # SIESA finished goods
    factory_lot_count: int = 0
    warehouse_m2: Decimal = Decimal("0")   # Guatemala warehouse (may also have stock)
    suggested_pallets: int                  # ceil(factory_m2 / M2_PER_PALLET) — bring it ALL
    suggested_m2: Decimal                   # = factory_available_m2 (all of it)
    days_since_last_sale: Optional[int] = None
    inactive_reason: Optional[str] = None
    inactive_date: Optional[date] = None


class LiquidationReason(str, Enum):
    """Reasons why a product is a liquidation candidate."""
    DECLINING_OVERSTOCKED = "declining_overstocked"  # Declining trend + high stock
    NO_SALES = "no_sales"                            # No recent sales
    EXTREME_OVERSTOCK = "extreme_overstock"          # Very high stock (any trend)


class LiquidationCandidate(BaseSchema):
    """Product identified as slow mover that could be cleared for fast movers."""

    product_id: str
    sku: str
    description: Optional[str] = None

    # Current stock
    current_m2: Decimal = Field(..., description="Current warehouse stock in m²")
    current_pallets: int = Field(..., description="Current stock in pallets")

    # Stock metrics
    days_of_stock: Optional[int] = Field(None, description="Days of stock at current velocity")
    trend_direction: str = Field(default="stable", description="up, down, stable")
    trend_pct: Decimal = Field(default=Decimal("0"), description="Velocity change percentage")
    daily_velocity_m2: Decimal = Field(default=Decimal("0"), description="Current daily velocity")

    # Liquidation reason
    reason: str = Field(..., description="declining_overstocked, no_sales, extreme_overstock")
    reason_display: str = Field(..., description="Human readable reason")

    # Space that could be freed
    potential_space_freed_m2: Decimal = Field(..., description="m² that would be freed")
    potential_space_freed_pallets: int = Field(..., description="Pallets that would be freed")


class ConstraintAnalysis(BaseSchema):
    """Analysis of order constraints and limiting factors."""

    # Total demand
    total_needed_pallets: int = Field(..., description="Total pallets suggested by system")
    total_needed_m2: Decimal = Field(..., description="Total m² suggested")

    # Available capacity
    warehouse_available_pallets: int = Field(..., description="Room in warehouse for new pallets")
    boat_capacity_pallets: int = Field(..., description="Max pallets for this boat")
    bl_capacity_pallets: int = Field(..., description="Max pallets based on BL count (num_bls × 5 × 14)")

    # Limiting factor
    limiting_factor: str = Field(
        ...,
        description="Which constraint is limiting: 'none', 'warehouse', 'boat', 'bl_capacity'"
    )
    effective_limit_pallets: int = Field(..., description="Actual max pallets (min of all constraints)")

    # What fits vs what doesn't
    can_order_pallets: int = Field(..., description="Pallets that fit within constraints")
    deferred_pallets: int = Field(..., description="Pallets that don't fit (need next boat or liquidation)")
    deferred_skus: list[str] = Field(default_factory=list, description="SKUs that couldn't fully fit")

    # Utilization
    constraint_utilization_pct: Decimal = Field(
        default=Decimal("0"),
        description="How much of the constraint is used (0-100)"
    )

    # Liquidation insight
    liquidation_candidates: list[LiquidationCandidate] = Field(
        default_factory=list,
        description="Slow movers that could be cleared to make room"
    )
    total_liquidation_potential_pallets: int = Field(
        default=0,
        description="Total pallets that could be freed by liquidating"
    )
    total_liquidation_potential_m2: Decimal = Field(
        default=Decimal("0"),
        description="Total m² that could be freed by liquidating"
    )

    # Helpful flags
    liquidation_needed: bool = Field(
        default=False,
        description="True if deferred_pallets > 0 and candidates exist"
    )
    liquidation_could_fit_deferred: bool = Field(
        default=False,
        description="True if liquidating would free enough space for deferred items"
    )


# ===================
# SECTION SUMMARIES (Three-Section Order Builder)
# ===================

class AddToProductionItem(BaseSchema):
    """Item that can have more quantity added to scheduled production."""

    product_id: str
    sku: str
    description: Optional[str] = None
    referencia: str = Field(..., description="Factory reference name")

    # Current production request
    current_requested_m2: Decimal = Field(..., description="What's already scheduled")

    # Order Builder suggestion
    suggested_total_m2: Decimal = Field(..., description="What Order Builder suggests total")
    suggested_additional_m2: Decimal = Field(..., description="Additional to add")
    suggested_additional_pallets: int = Field(default=0)

    # Timing
    estimated_ready_date: Optional[date] = Field(None, description="When production ready")
    target_boat: Optional[str] = Field(None, description="Which boat this could ship on")
    target_boat_departure: Optional[date] = None

    # Priority
    score: int = Field(default=0, description="Priority score from Order Builder")
    is_critical: bool = Field(default=False, description="Score >= 85")

    # Selection (pre-selected by default for recommended items)
    is_selected: bool = Field(default=True, description="Whether item is selected for export")

    # Piggyback history
    piggyback_history: list[dict] = Field(default_factory=list, description="Past piggyback records")
    total_piggybacked_m2: Decimal = Field(default=Decimal("0"), description="Total m2 already piggybacked")
    remaining_headroom_m2: Decimal = Field(default=Decimal("0"), description="How much more can be added")


class FactoryRequestItem(BaseSchema):
    """Item that needs a new factory production request."""

    product_id: str
    sku: str
    description: Optional[str] = None

    # Current coverage
    warehouse_m2: Decimal = Field(default=Decimal("0"))
    in_transit_m2: Decimal = Field(default=Decimal("0"))
    factory_available_m2: Decimal = Field(default=Decimal("0"))
    in_production_m2: Decimal = Field(default=Decimal("0"))

    # Need
    suggested_m2: Decimal = Field(..., description="What Order Builder suggests")
    gap_m2: Decimal = Field(..., description="Gap after all sources")
    gap_pallets: int = Field(default=0)

    # Request
    request_m2: Decimal = Field(default=Decimal("0"), description="User-adjustable request")
    request_pallets: int = Field(default=0)

    # Timing (legacy)
    estimated_ready: str = Field(default="", description="Estimated production time display")

    # Dynamic calculation fields
    avg_production_days: int = Field(default=7, description="Average production time in days")
    estimated_ready_date: Optional[date] = Field(None, description="When production will be ready")
    target_boat: Optional[str] = Field(None, description="Boat this would catch")
    target_boat_departure: Optional[date] = Field(None, description="Target boat departure date")
    target_boat_order_deadline: Optional[date] = Field(None, description="Target boat order deadline")
    arrival_date: Optional[date] = Field(None, description="When product arrives Guatemala")
    days_until_arrival: Optional[int] = Field(None, description="Days from today until arrival")

    # Buffer transparency (shows why this boat was selected)
    buffer_days_applied: int = Field(default=5, description="Safety buffer days applied to calculation")
    safe_ready_date: Optional[date] = Field(None, description="Ready date + buffer = safe ready date")
    buffer_note: Optional[str] = Field(None, description="Explanation e.g. '5-day buffer applied. Ready Mar 18 + 5 = Mar 23. Deadline Mar 25'")

    # Velocity and consumption
    velocity_m2_day: Decimal = Field(default=Decimal("0"), description="Daily velocity in m²")
    consumption_until_arrival_m2: Decimal = Field(default=Decimal("0"), description="Expected consumption until arrival")

    # Projection
    pipeline_m2: Decimal = Field(default=Decimal("0"), description="In-transit + scheduled production")
    projected_stock_at_arrival_m2: Decimal = Field(default=Decimal("0"), description="Stock at arrival")
    calculated_need_m2: Decimal = Field(default=Decimal("0"), description="Calculated need including buffer")

    # Low-volume detection (1 container / 365 days threshold)
    days_to_consume_container: Optional[int] = Field(None, description="Days to consume 1 container at current velocity")
    is_low_volume: bool = Field(default=False, description="True if would take >1 year to consume 1 container")
    low_volume_reason: Optional[str] = Field(None, description="Explanation for low-volume flag")
    should_request: bool = Field(default=True, description="False if low-volume or pipeline covers")
    skip_reason: Optional[str] = Field(None, description="Why request was skipped")

    # Priority
    urgency: str = Field(default="ok")
    score: int = Field(default=0)

    # Minimum enforcement (1 container = 14 pallets = 1,881.6 m²)
    minimum_applied: bool = Field(
        default=False,
        description="True if quantity was rounded up to 1 container minimum"
    )
    minimum_note: Optional[str] = Field(
        None,
        description="Note explaining minimum enforcement, e.g. 'Rounded up to 1 container minimum'"
    )

    # Selection (pre-selected by default for recommended items)
    is_selected: bool = Field(default=True, description="Whether item is selected for export")


class WarehouseOrderSummary(BaseSchema):
    """Summary for Section 1: Warehouse Order (ship now)."""

    product_count: int = Field(default=0)
    selected_count: int = Field(default=0)
    total_m2: Decimal = Field(default=Decimal("0"))
    total_pallets: int = Field(default=0)
    total_containers: int = Field(default=0)
    total_weight_kg: Decimal = Field(default=Decimal("0"))

    # BL allocation
    bl_count: int = Field(default=1)

    # Boat
    boat_name: Optional[str] = None
    boat_departure: Optional[date] = None


class AddToProductionSummary(BaseSchema):
    """Summary for Section 2: Add to Production (piggyback on scheduled items)."""

    product_count: int = Field(default=0, description="Items that can have more added")
    total_additional_m2: Decimal = Field(default=Decimal("0"))
    total_additional_pallets: int = Field(default=0)

    # Items eligible for adding more
    items: list[AddToProductionItem] = Field(default_factory=list)

    # Timing
    estimated_ready_range: str = Field(default="4-7 days")

    # Alert flag
    has_critical_items: bool = Field(default=False, description="Any item with score >= 85")

    # ACTION REQUIRED deadline (dynamic, e.g., "Monday, Feb 3")
    action_deadline: Optional[date] = Field(
        None, description="Deadline to submit additions (next Monday typically)"
    )
    action_deadline_display: str = Field(
        default="", description="Human-readable deadline, e.g. 'Monday, Feb 3'"
    )


class FactoryRequestSummary(BaseSchema):
    """Summary for Section 3: New Factory Request."""

    product_count: int = Field(default=0)
    total_request_m2: Decimal = Field(default=Decimal("0"))
    total_request_pallets: int = Field(default=0)

    # Items needing new requests
    items: list[FactoryRequestItem] = Field(default_factory=list)

    # Quota tracking (Guatemala: 60k m²/month)
    limit_m2: Decimal = Field(default=Decimal("60000"))
    utilization_pct: Decimal = Field(default=Decimal("0"))
    remaining_m2: Decimal = Field(default=Decimal("60000"))

    # Timing (removed the "30-60 days" guess)
    estimated_ready: str = Field(default="", description="Estimated ready month, e.g. 'March 2026'")

    # Submit deadline (dynamic, for new requests)
    submit_deadline: Optional[date] = Field(
        None, description="Deadline to submit new factory requests"
    )
    submit_deadline_display: str = Field(
        default="", description="Human-readable deadline, e.g. 'Submit by Feb 10'"
    )


class UnableToShipItem(BaseSchema):
    """Product that needs to be ordered but cannot ship due to logistical issues."""

    sku: str = Field(..., description="Product SKU")
    description: Optional[str] = Field(None, description="Product description")
    coverage_gap_m2: Decimal = Field(..., description="How much we need in m²")
    coverage_gap_pallets: int = Field(default=0, description="Gap in pallets")
    days_of_stock: Optional[int] = Field(None, description="Days of stock remaining")
    stockout_date: Optional[date] = Field(None, description="When we'll run out")

    # Why we can't ship
    reason: str = Field(..., description="Why we can't ship this product")
    production_status: Optional[str] = Field(None, description="Production status")
    production_estimated_ready: Optional[date] = Field(None, description="When production ready")

    # What to do
    suggested_action: str = Field(..., description="What user should do")

    # Priority
    priority: str = Field(default="HIGH_PRIORITY", description="Priority level")
    priority_score: int = Field(default=0, description="Urgency score for sorting")


class UnableToShipSummary(BaseSchema):
    """Summary of products that cannot ship."""

    count: int = Field(default=0, description="Number of products that can't ship")
    total_gap_m2: Decimal = Field(default=Decimal("0"), description="Total m² needed but can't ship")
    total_gap_pallets: int = Field(default=0, description="Total pallets needed but can't ship")
    message: str = Field(default="", description="Summary message")
    items: list[UnableToShipItem] = Field(default_factory=list)


class ShippingCostConfig(BaseSchema):
    """Shipping cost parameters for frontend cost estimation."""
    freight_per_container_usd: Decimal = Field(default=Decimal("460"))
    destination_per_container_usd: Decimal = Field(default=Decimal("630"))
    trucking_per_container_usd: Decimal = Field(default=Decimal("261.10"))
    other_per_container_usd: Decimal = Field(default=Decimal("46.44"))
    bl_fixed_costs_usd: Decimal = Field(default=Decimal("180.53"))
    m2_per_container: Decimal = Field(default=Decimal("1881.6"))
    per_container_total_usd: Decimal = Field(default=Decimal("1397.54"), description="Sum of all per-container costs")


class OrderBuilderResponse(BaseSchema):
    """Complete Order Builder API response."""

    # Boat info
    boat: OrderBuilderBoat
    next_boat: Optional[OrderBuilderBoat] = None

    # BL count (determines capacity)
    num_bls: int = Field(default=1, ge=1, le=5, description="Number of BLs (1-5). Capacity = num_bls × 5 × 14 pallets")

    # Recommended BL count (based on TRUE NEED: coverage gap - in transit - in production)
    recommended_bls: int = Field(
        default=1,
        ge=1,
        le=5,
        description="Recommended BLs based on TRUE NEED (what you need, regardless of factory stock)"
    )
    # Available BL count (what can ship now based on factory stock)
    # Can be 0 if no factory stock available
    available_bls: int = Field(
        default=0,
        ge=0,
        le=5,
        description="BLs that can ship now based on factory stock availability (0 = none available)"
    )
    recommended_bls_reason: str = Field(
        default="",
        description="Explanation showing need vs available, e.g. 'Need: 3 BLs (12,000 m²) • Available: 2 BLs (8,000 m²)'"
    )

    # Shippable BLs (what can actually be shipped to fill gaps)
    # shippable = min(coverage_gap, factory_available) for products with both > 0
    shippable_bls: int = Field(
        default=0,
        ge=0,
        le=5,
        description="BLs that can ship to fill coverage gaps (min of need and available per product)"
    )
    shippable_m2: Decimal = Field(
        default=Decimal("0"),
        description="Total m² that can ship to fill gaps (sum of min(gap, available) per product)"
    )

    # Products grouped by priority (existing)
    high_priority: list[OrderBuilderProduct] = Field(default_factory=list)
    consider: list[OrderBuilderProduct] = Field(default_factory=list)
    well_covered: list[OrderBuilderProduct] = Field(default_factory=list)
    your_call: list[OrderBuilderProduct] = Field(default_factory=list)

    # Summary
    summary: OrderBuilderSummary

    # === NEW: Three-Section Summaries ===
    warehouse_order_summary: Optional[WarehouseOrderSummary] = Field(
        None, description="Section 1: Ship from SIESA stock now"
    )
    add_to_production_summary: Optional[AddToProductionSummary] = Field(
        None, description="Section 2: Add to scheduled production (fast)"
    )
    factory_request_summary: Optional[FactoryRequestSummary] = Field(
        None, description="Section 3: New factory requests (future)"
    )

    # Constraint analysis (explains capacity limits)
    constraint_analysis: Optional[ConstraintAnalysis] = Field(
        None, description="Analysis of which constraint is limiting the order"
    )

    # Reasoning (explains WHY this order strategy)
    summary_reasoning: Optional[OrderSummaryReasoning] = Field(
        None, description="Order-level reasoning and strategy explanation"
    )

    # === NEW: Unable to Ship Alerts ===
    unable_to_ship: Optional[UnableToShipSummary] = Field(
        None, description="Products that need ordering but can't ship due to no SIESA stock"
    )

    # === NEW: Stability Forecast ===
    stability_forecast: Optional["StabilityForecast"] = Field(
        None, description="Cycle stability forecast showing when system will be stable"
    )

    # === Liquidation Clearance (deactivated products with factory stock) ===
    liquidation_clearance: list[LiquidationClearanceProduct] = Field(default_factory=list)

    # === Shipping Cost Config (for frontend cost estimation) ===
    shipping_cost_config: Optional[ShippingCostConfig] = None

    # === Factory-Aware Fields (OB V2) ===
    factory_id: Optional[str] = Field(None, description="Factory UUID if factory-scoped")
    factory_name: Optional[str] = Field(None, description="Factory name if factory-scoped")
    factory_timeline: Optional[dict] = Field(None, description="Factory-specific timeline milestones")


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


# ===================
# DEMAND FORECAST (Customer Pattern Overlay)
# ===================

class OverdueSeverity(str, Enum):
    """Severity levels for overdue customers."""
    CRITICAL = "critical"    # 180+ days - possibly lost
    WARNING = "warning"      # 60-180 days - at risk
    ATTENTION = "attention"  # 14-60 days - needs follow-up
    MINOR = "minor"          # 1-14 days - slightly overdue


class CustomerProduct(BaseSchema):
    """Product typically purchased by a customer."""
    sku: str
    avg_m2_per_order: Decimal = Field(..., description="Average m² per order for this product")
    purchase_count: int = Field(..., description="Number of times purchased")
    share_pct: Decimal = Field(..., description="Percentage of customer's total purchases")


class CustomerDue(BaseSchema):
    """Customer expected to order soon."""
    customer_normalized: str
    tier: str = Field(..., description="A, B, or C")
    days_overdue: int = Field(..., description="Days past expected order date (negative = due in future)")
    expected_date: Optional[str] = None
    predictability: Optional[str] = None
    avg_order_m2: Decimal = Field(..., description="Customer's average order size in m²")
    avg_order_usd: Decimal = Field(..., description="Customer's average order value in USD")
    last_order_date: Optional[str] = None
    trend_direction: str = Field(default="stable", description="up, down, stable")
    top_products: list[CustomerProduct] = Field(default_factory=list)


class OverdueAlert(BaseSchema):
    """Alert for severely overdue customer."""
    customer_normalized: str
    tier: str
    days_overdue: int
    severity: OverdueSeverity
    avg_order_usd: Decimal
    last_order_date: Optional[str] = None
    message: str = Field(..., description="Human-readable recommendation")


class ProductDemand(BaseSchema):
    """Aggregated demand for a product from customer patterns."""
    sku: str
    velocity_demand_m2: Decimal = Field(..., description="Demand based on current velocity")
    pattern_demand_m2: Decimal = Field(..., description="Demand based on customer patterns")
    recommended_m2: Decimal = Field(..., description="Recommended demand (higher of two)")
    customers_expecting: int = Field(..., description="Number of customers likely to order this")
    customer_names: list[str] = Field(default_factory=list, description="Top customer names")


class DemandForecastResponse(BaseSchema):
    """Demand forecast combining velocity and customer patterns."""

    # Demand estimates
    velocity_based_demand_m2: Decimal = Field(..., description="Traditional velocity × lead time demand")
    pattern_based_demand_m2: Decimal = Field(..., description="Sum of expected customer orders")
    recommended_demand_m2: Decimal = Field(..., description="Recommended demand (usually higher)")

    # Lead time info
    lead_time_days: int = Field(..., description="Days until boat arrives")

    # Customers due soon
    customers_due_soon: list[CustomerDue] = Field(default_factory=list)

    # Overdue alerts (for call-before-ordering)
    overdue_alerts: list[OverdueAlert] = Field(default_factory=list)

    # Demand by product
    demand_by_product: list[ProductDemand] = Field(default_factory=list)


# ===================
# STABILITY FORECAST
# ===================

class StabilityStatus(str, Enum):
    """Overall stability status of the cycle."""
    STABLE = "stable"           # All products have adequate coverage
    RECOVERING = "recovering"   # Products will recover with planned shipments
    UNSTABLE = "unstable"       # Products at risk, no recovery plan
    BLOCKED = "blocked"         # Some products have no supply scheduled


class SupplySource(str, Enum):
    """Source of supply for recovery."""
    SIESA = "siesa"             # Factory finished goods
    PRODUCTION = "production"   # Scheduled production
    NONE = "none"               # No supply available


class RecoveryStatus(str, Enum):
    """Recovery status for a product."""
    SHIPPING = "shipping"           # Will ship on upcoming boat
    IN_PRODUCTION = "in_production" # Waiting for production to complete
    BLOCKED = "blocked"             # No supply scheduled


class ProductRecovery(BaseSchema):
    """Recovery plan for a single unstable product."""
    sku: str
    product_name: Optional[str] = None
    current_coverage_days: int = Field(..., description="Days of stock at current velocity")
    stockout_date: Optional[date] = Field(None, description="When product will stock out")

    # Supply info
    supply_source: SupplySource
    supply_amount_m2: Decimal = Field(default=Decimal("0"), description="m² available/completing")
    supply_ready_date: Optional[date] = Field(None, description="When supply is available")

    # Shipping info
    ship_boat_name: Optional[str] = Field(None, description="Boat that will carry this")
    ship_boat_departure: Optional[date] = None
    arrival_date: Optional[date] = Field(None, description="When product arrives Guatemala")

    # Status
    status: RecoveryStatus
    status_note: str = Field(..., description="e.g., 'Ships on Boat 1, arrives Mar 5'")


class StabilityBlocker(BaseSchema):
    """Product blocking stability (no supply scheduled)."""
    sku: str
    product_name: Optional[str] = None
    current_coverage_days: int = Field(..., description="Days of stock remaining")
    stockout_date: Optional[date] = Field(None, description="When product will stock out")
    velocity_m2_per_day: Decimal = Field(default=Decimal("0"), description="Daily velocity")
    reason: str = Field(..., description="e.g., 'No production scheduled'")
    suggested_action: str = Field(..., description="e.g., 'Request production immediately'")


class StabilityTimeline(BaseSchema):
    """Snapshot of stability at a point in time."""
    date: date
    event: str = Field(..., description="e.g., 'Boat 1 arrives', 'Production completes'")
    resolved_count: int = Field(..., description="Products resolved by this event")
    remaining_unstable: int = Field(..., description="Products still unstable after event")
    resolved_skus: list[str] = Field(default_factory=list, description="SKUs resolved")


class StabilityForecast(BaseSchema):
    """Complete stability forecast for the cycle."""

    # Overall status
    status: StabilityStatus
    status_message: str = Field(..., description="e.g., '7 products at risk, stable by Apr 1'")

    # Counts
    total_products: int = Field(..., description="Total products tracked")
    stable_count: int = Field(..., description="Products with adequate coverage")
    unstable_count: int = Field(..., description="Products at risk")
    blocker_count: int = Field(..., description="Products with no supply scheduled")

    # Recovery info
    stable_date: Optional[date] = Field(None, description="When cycle will be stable (null if blocked)")
    stable_date_note: Optional[str] = Field(None, description="e.g., 'After Boat 2 arrives'")

    # Timeline
    timeline: list[StabilityTimeline] = Field(default_factory=list)

    # Product details
    recovering_products: list[ProductRecovery] = Field(default_factory=list)
    blockers: list[StabilityBlocker] = Field(default_factory=list)

    # Progress (for progress bar)
    recovery_progress_pct: int = Field(default=0, ge=0, le=100, description="0-100")
