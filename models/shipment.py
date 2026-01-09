"""
Shipment schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from enum import Enum
from datetime import date, datetime
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


class ShipmentStatus(str, Enum):
    """Shipment status values."""
    AT_FACTORY = "AT_FACTORY"
    AT_ORIGIN_PORT = "AT_ORIGIN_PORT"
    IN_TRANSIT = "IN_TRANSIT"
    AT_DESTINATION_PORT = "AT_DESTINATION_PORT"
    IN_CUSTOMS = "IN_CUSTOMS"
    IN_TRUCK = "IN_TRUCK"
    DELIVERED = "DELIVERED"


# Status order for transition validation (lower index = earlier in flow)
STATUS_ORDER = {
    ShipmentStatus.AT_FACTORY: 0,
    ShipmentStatus.AT_ORIGIN_PORT: 1,
    ShipmentStatus.IN_TRANSIT: 2,
    ShipmentStatus.AT_DESTINATION_PORT: 3,
    ShipmentStatus.IN_CUSTOMS: 4,
    ShipmentStatus.IN_TRUCK: 5,
    ShipmentStatus.DELIVERED: 6,
}


def is_valid_shipment_status_transition(current: ShipmentStatus, new: ShipmentStatus) -> bool:
    """
    Check if shipment status transition is valid.

    Rules:
    - Can skip forward (AT_FACTORY → IN_TRANSIT is OK)
    - Cannot go backward (IN_TRANSIT → AT_ORIGIN_PORT is NOT OK)
    - DELIVERED is terminal (cannot transition from DELIVERED)
    """
    if current == ShipmentStatus.DELIVERED:
        return False  # Terminal state

    current_order = STATUS_ORDER[current]
    new_order = STATUS_ORDER[new]

    return new_order > current_order


# ===================
# SHIPMENT SCHEMAS
# ===================

class ShipmentCreate(BaseSchema):
    """
    Create a new shipment.

    All fields optional - can create minimal shipment from booking
    and add ports later from departure/arrival documents.
    """

    # FK references
    factory_order_id: Optional[str] = Field(
        None,
        description="Factory order UUID this shipment is for"
    )
    boat_schedule_id: Optional[str] = Field(
        None,
        description="Boat schedule UUID (planned vessel)"
    )
    shipping_company_id: Optional[str] = Field(
        None,
        description="Shipping company UUID"
    )
    origin_port_id: Optional[str] = Field(
        None,
        description="Origin port UUID (can be added later)"
    )
    destination_port_id: Optional[str] = Field(
        None,
        description="Destination port UUID (can be added later)"
    )

    # Reference numbers
    booking_number: Optional[str] = Field(
        None,
        max_length=50,
        description="CMA CGM booking reference (e.g., BGA0505879)"
    )
    shp_number: Optional[str] = Field(
        None,
        max_length=50,
        description="TIBA shipment reference (e.g., SHP0065011)"
    )
    bill_of_lading: Optional[str] = Field(
        None,
        max_length=50,
        description="Bill of lading number"
    )

    # Vessel info
    vessel_name: Optional[str] = Field(
        None,
        max_length=100,
        description="Vessel name"
    )
    voyage_number: Optional[str] = Field(
        None,
        max_length=50,
        description="Voyage number"
    )

    # Dates
    etd: Optional[date] = Field(
        None,
        description="Estimated time of departure"
    )
    eta: Optional[date] = Field(
        None,
        description="Estimated time of arrival"
    )

    # Free days
    free_days: Optional[int] = Field(
        None,
        ge=0,
        description="Number of free days at destination"
    )

    # Costs
    freight_cost_usd: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Freight cost in USD"
    )

    notes: Optional[str] = Field(
        None,
        max_length=1000,
        description="Optional notes"
    )

    @field_validator("booking_number", "shp_number", "bill_of_lading")
    @classmethod
    def normalize_reference(cls, v: Optional[str]) -> Optional[str]:
        """Normalize reference numbers to uppercase."""
        if v is None:
            return v
        return v.upper().strip()

    @field_validator("freight_cost_usd")
    @classmethod
    def round_cost(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ShipmentUpdate(BaseSchema):
    """
    Update shipment.

    All fields optional - only provided fields are updated.
    Status is updated via separate endpoint.
    """

    # FK references
    factory_order_id: Optional[str] = Field(None, description="Factory order UUID")
    boat_schedule_id: Optional[str] = Field(None, description="Boat schedule UUID")
    shipping_company_id: Optional[str] = Field(None, description="Shipping company UUID")

    # Reference numbers
    booking_number: Optional[str] = Field(None, max_length=50, description="CMA CGM booking reference")
    shp_number: Optional[str] = Field(None, max_length=50, description="TIBA shipment reference")
    bill_of_lading: Optional[str] = Field(None, max_length=50, description="Bill of lading number")

    # Vessel info
    vessel_name: Optional[str] = Field(None, max_length=100, description="Vessel name")
    voyage_number: Optional[str] = Field(None, max_length=50, description="Voyage number")

    # Dates
    etd: Optional[date] = Field(None, description="Estimated time of departure")
    eta: Optional[date] = Field(None, description="Estimated time of arrival")
    actual_departure: Optional[date] = Field(None, description="Actual departure date")
    actual_arrival: Optional[date] = Field(None, description="Actual arrival date")

    # Free days
    free_days: Optional[int] = Field(None, ge=0, description="Number of free days")
    free_days_expiry: Optional[date] = Field(None, description="Free days expiry date")

    # Costs
    freight_cost_usd: Optional[Decimal] = Field(None, ge=0, description="Freight cost in USD")

    notes: Optional[str] = Field(None, max_length=1000, description="Notes")

    @field_validator("booking_number", "shp_number", "bill_of_lading")
    @classmethod
    def normalize_reference(cls, v: Optional[str]) -> Optional[str]:
        """Normalize reference numbers to uppercase."""
        if v is None:
            return v
        return v.upper().strip()

    @field_validator("freight_cost_usd")
    @classmethod
    def round_cost(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ShipmentStatusUpdate(BaseSchema):
    """Update only the status of a shipment."""

    status: ShipmentStatus = Field(
        ...,
        description="New status"
    )


class ShipmentResponse(BaseSchema, TimestampMixin):
    """
    Shipment response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Shipment UUID")

    # FK references
    factory_order_id: Optional[str] = Field(None, description="Factory order UUID")
    boat_schedule_id: Optional[str] = Field(None, description="Boat schedule UUID")
    shipping_company_id: Optional[str] = Field(None, description="Shipping company UUID")
    origin_port_id: Optional[str] = Field(None, description="Origin port UUID")
    destination_port_id: Optional[str] = Field(None, description="Destination port UUID")

    # Status
    status: ShipmentStatus = Field(..., description="Current status")
    active: bool = Field(default=True, description="Whether shipment is active")

    # Reference numbers
    booking_number: Optional[str] = Field(None, description="CMA CGM booking reference")
    shp_number: Optional[str] = Field(None, description="TIBA shipment reference")
    bill_of_lading: Optional[str] = Field(None, description="Bill of lading number")

    # Vessel info
    vessel_name: Optional[str] = Field(None, description="Vessel name")
    voyage_number: Optional[str] = Field(None, description="Voyage number")

    # Dates
    etd: Optional[date] = Field(None, description="Estimated time of departure")
    eta: Optional[date] = Field(None, description="Estimated time of arrival")
    actual_departure: Optional[date] = Field(None, description="Actual departure date")
    actual_arrival: Optional[date] = Field(None, description="Actual arrival date")

    # Free days
    free_days: Optional[int] = Field(None, description="Number of free days")
    free_days_expiry: Optional[date] = Field(None, description="Free days expiry date")

    # Costs
    freight_cost_usd: Optional[Decimal] = Field(None, description="Freight cost in USD")

    notes: Optional[str] = Field(None, description="Notes")


class ShipmentListResponse(BaseSchema):
    """List of shipments with pagination."""

    data: list[ShipmentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
