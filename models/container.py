"""
Container schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


# ===================
# CONTAINER SCHEMAS
# ===================

class ContainerCreate(BaseSchema):
    """
    Create a new container.

    Required: shipment_id
    Optional: All other fields
    """

    shipment_id: str = Field(
        ...,
        description="Shipment UUID this container belongs to"
    )
    container_number: Optional[str] = Field(
        None,
        max_length=50,
        description="Container number (e.g., TCNU1234567)"
    )
    seal_number: Optional[str] = Field(
        None,
        max_length=50,
        description="Seal number"
    )
    trucking_company_id: Optional[str] = Field(
        None,
        description="Trucking company UUID"
    )
    total_pallets: Optional[int] = Field(
        None,
        ge=0,
        description="Total number of pallets"
    )
    total_weight_kg: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Total weight in kg"
    )
    total_m2: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Total area in m²"
    )
    fill_percentage: Optional[Decimal] = Field(
        None,
        ge=0,
        le=100,
        description="Fill percentage (0-100)"
    )
    unload_start: Optional[datetime] = Field(
        None,
        description="Unload start time"
    )
    unload_end: Optional[datetime] = Field(
        None,
        description="Unload end time"
    )

    @field_validator("container_number", "seal_number")
    @classmethod
    def normalize_reference(cls, v: Optional[str]) -> Optional[str]:
        """Normalize reference numbers to uppercase."""
        if v is None:
            return v
        return v.upper().strip()

    @field_validator("total_weight_kg", "total_m2", "fill_percentage")
    @classmethod
    def round_decimals(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ContainerUpdate(BaseSchema):
    """
    Update container.

    All fields optional - only provided fields are updated.
    """

    container_number: Optional[str] = Field(None, max_length=50)
    seal_number: Optional[str] = Field(None, max_length=50)
    trucking_company_id: Optional[str] = None
    total_pallets: Optional[int] = Field(None, ge=0)
    total_weight_kg: Optional[Decimal] = Field(None, ge=0)
    total_m2: Optional[Decimal] = Field(None, ge=0)
    fill_percentage: Optional[Decimal] = Field(None, ge=0, le=100)
    unload_start: Optional[datetime] = None
    unload_end: Optional[datetime] = None

    @field_validator("container_number", "seal_number")
    @classmethod
    def normalize_reference(cls, v: Optional[str]) -> Optional[str]:
        """Normalize reference numbers to uppercase."""
        if v is None:
            return v
        return v.upper().strip()

    @field_validator("total_weight_kg", "total_m2", "fill_percentage")
    @classmethod
    def round_decimals(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ContainerResponse(BaseSchema, TimestampMixin):
    """
    Container response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Container UUID")
    shipment_id: str = Field(..., description="Shipment UUID")
    container_number: Optional[str] = Field(None, description="Container number")
    seal_number: Optional[str] = Field(None, description="Seal number")
    trucking_company_id: Optional[str] = Field(None, description="Trucking company UUID")
    total_pallets: Optional[int] = Field(None, description="Total pallets")
    total_weight_kg: Optional[Decimal] = Field(None, description="Total weight in kg")
    total_m2: Optional[Decimal] = Field(None, description="Total area in m²")
    fill_percentage: Optional[Decimal] = Field(None, description="Fill percentage")
    unload_start: Optional[datetime] = Field(None, description="Unload start")
    unload_end: Optional[datetime] = Field(None, description="Unload end")


# ===================
# CONTAINER ITEM SCHEMAS
# ===================

class ContainerItemCreate(BaseSchema):
    """
    Create a new container item.

    Required: product_id, quantity
    Optional: pallets, weight_kg
    """

    product_id: str = Field(
        ...,
        description="Product UUID"
    )
    quantity: Decimal = Field(
        ...,
        gt=0,
        description="Quantity in m²"
    )
    pallets: Optional[int] = Field(
        None,
        ge=0,
        description="Number of pallets"
    )
    weight_kg: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Weight in kg"
    )

    @field_validator("quantity", "weight_kg")
    @classmethod
    def round_decimals(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ContainerItemUpdate(BaseSchema):
    """
    Update container item.

    All fields optional - only provided fields are updated.
    """

    quantity: Optional[Decimal] = Field(None, gt=0)
    pallets: Optional[int] = Field(None, ge=0)
    weight_kg: Optional[Decimal] = Field(None, ge=0)

    @field_validator("quantity", "weight_kg")
    @classmethod
    def round_decimals(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ContainerItemResponse(BaseSchema, TimestampMixin):
    """
    Container item response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Container item UUID")
    container_id: str = Field(..., description="Container UUID")
    product_id: str = Field(..., description="Product UUID")
    quantity: Decimal = Field(..., description="Quantity in m²")
    pallets: Optional[int] = Field(None, description="Number of pallets")
    weight_kg: Optional[Decimal] = Field(None, description="Weight in kg")


class ContainerWithItemsResponse(BaseSchema, TimestampMixin):
    """
    Container with nested items.

    Used for detailed GET response.
    """

    id: str = Field(..., description="Container UUID")
    shipment_id: str = Field(..., description="Shipment UUID")
    container_number: Optional[str] = Field(None, description="Container number")
    seal_number: Optional[str] = Field(None, description="Seal number")
    trucking_company_id: Optional[str] = Field(None, description="Trucking company UUID")
    total_pallets: Optional[int] = Field(None, description="Total pallets")
    total_weight_kg: Optional[Decimal] = Field(None, description="Total weight in kg")
    total_m2: Optional[Decimal] = Field(None, description="Total area in m²")
    fill_percentage: Optional[Decimal] = Field(None, description="Fill percentage")
    unload_start: Optional[datetime] = Field(None, description="Unload start")
    unload_end: Optional[datetime] = Field(None, description="Unload end")
    items: List[ContainerItemResponse] = Field(default_factory=list, description="Container items")


class ContainerListResponse(BaseSchema):
    """List of containers."""

    data: List[ContainerResponse]
    total: int