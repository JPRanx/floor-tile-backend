"""
Factory order schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from enum import Enum
from datetime import date, datetime
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


class OrderStatus(str, Enum):
    """Factory order status values."""
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    IN_PRODUCTION = "IN_PRODUCTION"
    READY = "READY"
    SHIPPED = "SHIPPED"


# Status order for transition validation (lower index = earlier in flow)
STATUS_ORDER = {
    OrderStatus.PENDING: 0,
    OrderStatus.CONFIRMED: 1,
    OrderStatus.IN_PRODUCTION: 2,
    OrderStatus.READY: 3,
    OrderStatus.SHIPPED: 4,
}


def is_valid_status_transition(current: OrderStatus, new: OrderStatus) -> bool:
    """
    Check if status transition is valid.

    Rules:
    - Can skip forward (PENDING → READY is OK)
    - Cannot go backward (READY → CONFIRMED is NOT OK)
    - SHIPPED is terminal (cannot transition from SHIPPED)
    """
    if current == OrderStatus.SHIPPED:
        return False  # Terminal state

    current_order = STATUS_ORDER[current]
    new_order = STATUS_ORDER[new]

    return new_order > current_order


# ===================
# FACTORY ORDER ITEM SCHEMAS
# ===================

class FactoryOrderItemCreate(BaseSchema):
    """Create a factory order item."""

    product_id: str = Field(..., description="Product UUID")
    quantity_ordered: Decimal = Field(
        ...,
        gt=0,
        description="Quantity ordered in m²"
    )
    estimated_ready_date: Optional[date] = Field(
        None,
        description="Estimated date product will be ready"
    )

    @field_validator("quantity_ordered")
    @classmethod
    def round_quantity(cls, v: Decimal) -> Decimal:
        """Round to 2 decimal places."""
        return round(v, 2)


class FactoryOrderItemResponse(BaseSchema):
    """Factory order item response."""

    id: str = Field(..., description="Item UUID")
    factory_order_id: str = Field(..., description="Parent order UUID")
    product_id: str = Field(..., description="Product UUID")
    quantity_ordered: Decimal = Field(..., description="Quantity ordered in m²")
    quantity_produced: Decimal = Field(default=Decimal("0"), description="Quantity produced so far")
    estimated_ready_date: Optional[date] = Field(None, description="Estimated ready date")
    actual_ready_date: Optional[date] = Field(None, description="Actual ready date")
    created_at: datetime = Field(..., description="Created timestamp")

    # Joined product info (optional)
    product_sku: Optional[str] = Field(None, description="Product SKU")


# ===================
# FACTORY ORDER SCHEMAS
# ===================

class FactoryOrderCreate(BaseSchema):
    """
    Create a new factory order.

    Required: order_date, items
    Optional: pv_number, notes
    """

    pv_number: Optional[str] = Field(
        None,
        max_length=50,
        description="Pedido de Ventas number (e.g., PV-00017759)"
    )
    order_date: date = Field(
        ...,
        description="Date order was placed"
    )
    items: list[FactoryOrderItemCreate] = Field(
        ...,
        min_length=1,
        description="Order line items"
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional notes"
    )

    @field_validator("pv_number")
    @classmethod
    def normalize_pv_number(cls, v: Optional[str]) -> Optional[str]:
        """Normalize PV number to uppercase."""
        if v is None:
            return v
        return v.upper().strip()

    @field_validator("order_date")
    @classmethod
    def not_future_date(cls, v: date) -> date:
        """Order date cannot be in the future."""
        if v > date.today():
            raise ValueError("Order date cannot be in the future")
        return v


class FactoryOrderUpdate(BaseSchema):
    """
    Update factory order.

    All fields optional - only provided fields are updated.
    Status is updated via separate endpoint.
    """

    pv_number: Optional[str] = Field(
        None,
        max_length=50,
        description="Pedido de Ventas number"
    )
    order_date: Optional[date] = Field(
        None,
        description="Order date"
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Notes"
    )

    @field_validator("pv_number")
    @classmethod
    def normalize_pv_number(cls, v: Optional[str]) -> Optional[str]:
        """Normalize PV number to uppercase."""
        if v is None:
            return v
        return v.upper().strip()


class FactoryOrderStatusUpdate(BaseSchema):
    """Update only the status of a factory order."""

    status: OrderStatus = Field(
        ...,
        description="New status"
    )


class FactoryOrderResponse(BaseSchema, TimestampMixin):
    """
    Factory order response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Order UUID")
    pv_number: Optional[str] = Field(None, description="Pedido de Ventas number")
    order_date: date = Field(..., description="Order date")
    status: OrderStatus = Field(..., description="Current status")
    notes: Optional[str] = Field(None, description="Notes")
    active: bool = Field(default=True, description="Whether order is active")

    # Computed fields
    total_m2: Optional[Decimal] = Field(None, description="Total m² ordered")
    item_count: Optional[int] = Field(None, description="Number of line items")


class FactoryOrderWithItemsResponse(FactoryOrderResponse):
    """Factory order response with line items included."""

    items: list[FactoryOrderItemResponse] = Field(
        default_factory=list,
        description="Order line items"
    )


class FactoryOrderListResponse(BaseSchema):
    """List of factory orders with pagination."""

    data: list[FactoryOrderResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
