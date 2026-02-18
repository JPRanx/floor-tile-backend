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
        description="Deadline date to place the order (departure - production_lead - transport_to_port)"
    )
    days_until_order_deadline: Optional[int] = Field(
        None,
        description="Days from today until order_by_date (negative = overdue)"
    )
    product_details: list[ProductProjection] = Field(
        default_factory=list,
        description="Per-product projections sorted by urgency (critical first)"
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
