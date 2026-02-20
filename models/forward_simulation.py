"""
Forward simulation schemas for the 3-month planning horizon.

Represents projected orders for future boats, including confidence
levels, urgency breakdowns, and draft status.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field
from typing import Optional
from enum import Enum

from models.base import BaseSchema


class ConfidenceLevel(str, Enum):
    """How confident the projection is, based on data freshness and horizon distance."""
    VERY_HIGH = "very_high"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    VERY_LOW = "very_low"


class UrgencyBreakdown(BaseSchema):
    """
    Count of products per urgency level for a projected boat.

    Used to visualize urgency distribution in the Planning View.
    """

    critical: int = Field(0, ge=0, description="Products that must ship on this boat")
    urgent: int = Field(0, ge=0, description="Products that should ship on this boat")
    soon: int = Field(0, ge=0, description="Products approaching reorder point")
    ok: int = Field(0, ge=0, description="Products with healthy stock levels")


class SupplySource(BaseSchema):
    """Breakdown of supply sources contributing to a product's available stock for a boat."""

    warehouse_m2: float = Field(0, description="Current warehouse stock in m2")
    factory_siesa_m2: float = Field(0, description="Factory SIESA finished goods available for this boat")
    production_pipeline_m2: float = Field(0, description="Production completing before this boat's departure")
    in_transit_m2: float = Field(0, description="In-transit from ordered/confirmed drafts on earlier boats")


class ProductProjection(BaseSchema):
    """Per-product projection for a boat: projected stock, urgency, and suggested order."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field("", description="Product SKU")
    daily_velocity_m2: float = Field(0, ge=0, description="Daily sales velocity in m2")
    current_stock_m2: float = Field(0, description="Current warehouse + in-transit stock in m2")
    projected_stock_m2: float = Field(0, description="Projected stock at arrival in m2")
    days_of_stock_at_arrival: float = Field(0, description="Days of stock remaining at arrival")
    urgency: str = Field("ok", description="Urgency level: critical, urgent, soon, ok")
    coverage_gap_m2: float = Field(0, ge=0, description="m2 needed to cover until next order cycle")
    suggested_pallets: int = Field(0, ge=0, description="Suggested pallets to order")
    supply_breakdown: Optional[SupplySource] = Field(
        None,
        description="Breakdown of supply sources contributing to available stock"
    )
    is_draft_committed: bool = Field(
        False,
        description="True if this quantity comes from a saved draft (not a simulation suggestion)"
    )


class DraftBLItem(BaseSchema):
    """A product assigned to a specific BL in a draft."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field("", description="Product SKU for display")
    selected_pallets: int = Field(0, ge=0, description="Pallets assigned")
    bl_number: int = Field(..., ge=1, description="BL number")


class StabilityImpact(BaseSchema):
    """Per-boat stability impact: how many products this boat stabilizes."""
    stabilizes_count: int = Field(0, ge=0, description="Products going from <30d to ≥30d coverage")
    stabilizes_products: list[str] = Field(default_factory=list, description="SKUs of stabilized products")
    recovering_count: int = Field(0, ge=0, description="Products still <30d but with supply on later boats")
    recovering_products: list[str] = Field(default_factory=list, description="SKUs of recovering products")
    blocked_count: int = Field(0, ge=0, description="Products <30d with no supply anywhere")
    blocked_products: list[str] = Field(default_factory=list, description="SKUs of blocked products")
    progress_before_pct: int = Field(0, ge=0, le=100, description="% of products stable before this boat")
    progress_after_pct: int = Field(0, ge=0, le=100, description="% of products stable after this boat")


class BoatProjection(BaseSchema):
    """
    Projection for a single future boat in the planning horizon.

    Combines real boat data (if scheduled) with forward simulation
    of what products will need to ship by that date.
    """

    boat_id: str = Field(..., description="Boat UUID or generated projection ID")
    boat_name: str = Field("", description="Vessel name if known")
    departure_date: str = Field(..., description="Projected departure date (ISO format)")
    arrival_date: str = Field(..., description="Projected arrival date (ISO format)")
    days_until_departure: int = Field(..., description="Days from today until departure")
    origin_port: str = Field(..., description="Port of origin for this shipment")
    confidence: ConfidenceLevel = Field(
        ...,
        description="Qualitative confidence level for this projection"
    )
    projected_pallets_min: int = Field(
        ...,
        ge=0,
        description="Low end of estimated pallet count"
    )
    projected_pallets_max: int = Field(
        ...,
        ge=0,
        description="High end of estimated pallet count"
    )
    urgency_breakdown: UrgencyBreakdown = Field(
        default_factory=UrgencyBreakdown,
        description="Product counts per urgency level"
    )
    draft_status: Optional[str] = Field(
        None,
        description="Draft lifecycle status: drafting, action_needed, ordered, confirmed"
    )
    draft_id: Optional[str] = Field(
        None,
        description="Draft UUID if one exists"
    )
    is_active: bool = Field(
        ...,
        description="True if an existing draft exists, False if purely projected"
    )
    order_by_date: Optional[str] = Field(
        None,
        description="Factory production order deadline (departure - production_lead - transport_to_port)"
    )
    days_until_order_deadline: Optional[int] = Field(
        None,
        description="Days from today until factory order deadline (negative = overdue)"
    )
    shipping_book_by_date: Optional[str] = Field(
        None,
        description="Shipping booking deadline (departure - transport_to_port)"
    )
    days_until_shipping_deadline: Optional[int] = Field(
        None,
        description="Days from today until shipping booking deadline (negative = overdue)"
    )
    # Dual deadline system for Planning View
    siesa_order_date: Optional[str] = Field(
        None,
        description="SIESA order deadline: last day to finalize picking from SIESA warehouse (departure - 20 days)"
    )
    days_until_siesa_deadline: Optional[int] = Field(
        None,
        description="Days from today until SIESA order deadline (negative = overdue)"
    )
    production_request_date: Optional[str] = Field(
        None,
        description="Production request deadline: last day to request new factory production (departure - production_lead - transport - buffer)"
    )
    days_until_production_deadline: Optional[int] = Field(
        None,
        description="Days from today until production request deadline (negative = overdue)"
    )
    product_details: list[ProductProjection] = Field(
        default_factory=list,
        description="Per-product projections sorted by urgency (critical first)"
    )
    draft_bl_items: list[DraftBLItem] = Field(
        default_factory=list,
        description="Draft items with BL assignments (empty if no BLs allocated)"
    )
    has_bl_allocation: bool = Field(
        False,
        description="True if the draft has BL assignments"
    )
    is_estimated: bool = Field(
        False,
        description="True if this is a phantom boat generated from a shipping route pattern"
    )
    carrier: Optional[str] = Field(
        None,
        description="Freight forwarder or carrier name (e.g., TIBA, SEABOARD)"
    )
    is_draft_locked: bool = Field(
        False,
        description="True if editing this draft is blocked by a later boat's draft"
    )
    blocking_boat_name: Optional[str] = Field(
        None,
        description="Name of the boat whose draft blocks editing this one"
    )
    has_earlier_drafts: bool = Field(
        False,
        description="True if this boat's calculation depends on earlier boat drafts"
    )
    needs_review: bool = Field(
        False,
        description="True if this draft needs review (status=action_needed or earlier draft changed)"
    )
    review_reason: Optional[str] = Field(
        None,
        description="Why this draft needs attention (e.g., 'Borrador anterior modificado')"
    )
    earlier_draft_context: Optional[str] = Field(
        None,
        description="Summary of earlier drafts this depends on (e.g., 'Basado en borrador de GEMINI (48 paletas)')"
    )
    has_factory_siesa_supply: bool = Field(
        False,
        description="True if factory SIESA stock contributes to this boat's supply"
    )
    has_production_supply: bool = Field(
        False,
        description="True if production pipeline contributes to this boat's supply"
    )
    factory_siesa_total_m2: float = Field(
        0,
        description="Total factory SIESA m2 consumed by this boat"
    )
    production_total_m2: float = Field(
        0,
        description="Total production pipeline m2 consumed by this boat"
    )
    has_in_transit_supply: bool = Field(
        False,
        description="True if in-transit inventory contributes to this boat's supply"
    )
    in_transit_total_m2: float = Field(
        0,
        description="Total in-transit m2 consumed by this boat"
    )
    stability_impact: Optional[StabilityImpact] = Field(
        None,
        description="Per-boat stability impact: stabilizes, recovering, blocked counts and progress"
    )


class FactoryOrderSignal(BaseSchema):
    """Factory-level signal for when to place the next production order."""
    next_order_date: Optional[str] = Field(
        None, description="Date by which next factory order should be placed (ISO)"
    )
    days_until_order: Optional[int] = Field(
        None, description="Days until next factory order (negative = overdue)"
    )
    is_overdue: bool = Field(False, description="True if the factory order is overdue")
    limiting_product_sku: Optional[str] = Field(
        None, description="SKU of the product that drives the earliest order date"
    )
    effective_coverage_days: Optional[int] = Field(
        None, description="Days of factory supply remaining (before lead time adjustment)"
    )
    target_boat_name: Optional[str] = Field(
        None, description="Name of the boat this production targets"
    )
    target_boat_departure: Optional[str] = Field(
        None, description="Departure date of the target boat (ISO)"
    )
    estimated_pallets: Optional[int] = Field(
        None, description="Estimated total pallets needing production"
    )
    product_count: Optional[int] = Field(
        None, description="Number of products needing production"
    )


class PlanningHorizonResponse(BaseSchema):
    """
    Full planning horizon response for a single factory.

    Contains all projected boats within the 3-month horizon,
    used to render the Planning View landing page.
    """

    factory_id: str = Field(..., description="Factory UUID")
    factory_name: str = Field(..., description="Factory display name")
    horizon_months: int = Field(..., ge=1, le=12, description="Months projected ahead")
    generated_at: str = Field(
        ...,
        description="When this projection was computed (ISO timestamp)"
    )
    projections: list[BoatProjection] = Field(
        default_factory=list,
        description="Projected boats in chronological order"
    )
    factory_order_signal: Optional[FactoryOrderSignal] = Field(
        None, description="Factory-level production order signal"
    )
