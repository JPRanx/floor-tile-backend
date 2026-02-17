"""
Factory schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field, field_validator
from typing import Optional
from enum import Enum
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


class CutoffDay(str, Enum):
    """Allowed cutoff days for factory order cycles."""
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"


class FactoryCreate(BaseSchema):
    """
    Create a new factory.

    Required: name, country, origin_port
    Optional: lead times, cutoff_day, monthly_quota_m2, active, sort_order
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Factory name (unique)",
        examples=["CI", "Muebles"]
    )
    country: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Country where factory is located",
        examples=["Colombia", "China", "Brazil"]
    )
    origin_port: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Port of origin for shipments",
        examples=["Cartagena", "Shanghai"]
    )
    production_lead_days: int = Field(
        35,
        ge=0,
        description="Days from order to production complete"
    )
    piggyback_lead_days: int = Field(
        7,
        ge=0,
        description="Days to consolidate with other shipments"
    )
    transport_to_port_days: int = Field(
        5,
        ge=0,
        description="Days to transport goods to origin port"
    )
    cutoff_day: CutoffDay = Field(
        CutoffDay.MONDAY,
        description="Weekly cutoff day for order cycles"
    )
    monthly_quota_m2: Decimal = Field(
        Decimal("60000"),
        ge=0,
        description="Monthly production quota in m²"
    )
    active: bool = Field(
        False,
        description="Whether factory is currently active"
    )
    sort_order: int = Field(
        0,
        ge=0,
        description="Display sort order"
    )

    @field_validator("monthly_quota_m2")
    @classmethod
    def round_quota(cls, v: Decimal) -> Decimal:
        """Round to 2 decimal places."""
        return round(v, 2)


class FactoryUpdate(BaseSchema):
    """
    Update existing factory.

    All fields optional - only provided fields are updated.
    """

    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Factory name (unique)"
    )
    country: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Country where factory is located"
    )
    origin_port: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Port of origin for shipments"
    )
    production_lead_days: Optional[int] = Field(
        None,
        ge=0,
        description="Days from order to production complete"
    )
    piggyback_lead_days: Optional[int] = Field(
        None,
        ge=0,
        description="Days to consolidate with other shipments"
    )
    transport_to_port_days: Optional[int] = Field(
        None,
        ge=0,
        description="Days to transport goods to origin port"
    )
    cutoff_day: Optional[CutoffDay] = Field(
        None,
        description="Weekly cutoff day for order cycles"
    )
    monthly_quota_m2: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Monthly production quota in m²"
    )
    active: Optional[bool] = Field(
        None,
        description="Whether factory is currently active"
    )
    sort_order: Optional[int] = Field(
        None,
        ge=0,
        description="Display sort order"
    )

    @field_validator("monthly_quota_m2")
    @classmethod
    def round_quota(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class FactoryResponse(BaseSchema, TimestampMixin):
    """
    Factory response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Factory UUID")
    name: str = Field(..., description="Factory name")
    country: str = Field(..., description="Country where factory is located")
    origin_port: str = Field(..., description="Port of origin for shipments")
    production_lead_days: int = Field(..., description="Days from order to production complete")
    piggyback_lead_days: int = Field(..., description="Days to consolidate with other shipments")
    transport_to_port_days: int = Field(..., description="Days to transport goods to origin port")
    cutoff_day: CutoffDay = Field(..., description="Weekly cutoff day for order cycles")
    monthly_quota_m2: Decimal = Field(..., description="Monthly production quota in m²")
    active: bool = Field(..., description="Whether factory is currently active")
    sort_order: int = Field(..., description="Display sort order")
