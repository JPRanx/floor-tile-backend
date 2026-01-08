"""
Shipment event schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

from models.base import BaseSchema, TimestampMixin
from models.shipment import ShipmentStatus


# ===================
# SHIPMENT EVENT SCHEMAS
# ===================

class ShipmentEventCreate(BaseSchema):
    """
    Create a new shipment event.

    Required: shipment_id, status, occurred_at
    Optional: notes
    """

    shipment_id: str = Field(
        ...,
        description="Shipment UUID this event belongs to"
    )
    status: ShipmentStatus = Field(
        ...,
        description="Shipment status at this event"
    )
    occurred_at: datetime = Field(
        ...,
        description="When this status change occurred"
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional notes about this event"
    )

    @field_validator("occurred_at")
    @classmethod
    def validate_occurred_at(cls, v: datetime) -> datetime:
        """Ensure occurred_at is not in the future."""
        if v > datetime.utcnow():
            raise ValueError("occurred_at cannot be in the future")
        return v


class ShipmentEventResponse(BaseSchema, TimestampMixin):
    """
    Shipment event response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Event UUID")
    shipment_id: str = Field(..., description="Shipment UUID")
    status: ShipmentStatus = Field(..., description="Shipment status at this event")
    occurred_at: datetime = Field(..., description="When this status change occurred")
    notes: Optional[str] = Field(None, description="Notes about this event")


class ShipmentEventListResponse(BaseSchema):
    """List of shipment events."""

    data: list[ShipmentEventResponse]
    total: int
