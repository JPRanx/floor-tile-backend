"""
Warehouse Order schemas for validation and serialization.

Warehouse orders track Order Builder exports - SIESA stock selected for
shipment on a specific boat. Used to prevent double-ordering and calculate
pending coverage.

See STANDARDS_VALIDATION.md for patterns.
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from pydantic import Field, field_validator

from models.base import BaseSchema, TimestampMixin


class WarehouseOrderStatus(str, Enum):
    """Warehouse order status values."""
    PENDING = "pending"      # Exported, awaiting shipment
    SHIPPED = "shipped"      # On boat, in transit
    RECEIVED = "received"    # In warehouse (terminal)
    CANCELLED = "cancelled"  # Voided (terminal)


class Priority(str, Enum):
    """Product priority levels from Order Builder."""
    HIGH_PRIORITY = "HIGH_PRIORITY"
    CONSIDER = "CONSIDER"
    WELL_COVERED = "WELL_COVERED"
    YOUR_CALL = "YOUR_CALL"


# ===================
# WAREHOUSE ORDER ITEM SCHEMAS
# ===================

class WarehouseOrderItemCreate(BaseSchema):
    """Create a warehouse order item."""

    product_id: Optional[str] = Field(None, description="Product UUID")
    sku: str = Field(..., max_length=100, description="Product SKU")
    description: Optional[str] = Field(None, max_length=255, description="Product description")
    pallets: int = Field(..., ge=0, description="Number of pallets")
    m2: Decimal = Field(..., ge=0, description="Total m2")
    weight_kg: Decimal = Field(default=Decimal("0"), ge=0, description="Total weight in kg")
    score: Optional[int] = Field(None, ge=0, le=100, description="Priority score (0-100)")
    priority: Optional[Priority] = Field(None, description="Priority bucket")
    is_critical: bool = Field(default=False, description="True if score >= 85")
    primary_customer: Optional[str] = Field(None, max_length=255, description="Primary customer")
    bl_number: Optional[int] = Field(None, ge=1, le=5, description="BL assignment (1-5)")

    @field_validator("m2")
    @classmethod
    def round_m2(cls, v: Decimal) -> Decimal:
        """Round to 2 decimal places."""
        return round(v, 2)

    @field_validator("weight_kg")
    @classmethod
    def round_weight(cls, v: Decimal) -> Decimal:
        """Round to 2 decimal places."""
        return round(v, 2)


class WarehouseOrderItemResponse(BaseSchema):
    """Warehouse order item response."""

    id: str = Field(..., description="Item UUID")
    warehouse_order_id: str = Field(..., description="Parent order UUID")
    product_id: Optional[str] = Field(None, description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    description: Optional[str] = Field(None, description="Product description")
    pallets: int = Field(..., description="Number of pallets")
    m2: Decimal = Field(..., description="Total m2")
    weight_kg: Decimal = Field(default=Decimal("0"), description="Total weight in kg")
    score: Optional[int] = Field(None, description="Priority score (0-100)")
    priority: Optional[Priority] = Field(None, description="Priority bucket")
    is_critical: bool = Field(default=False, description="True if score >= 85")
    primary_customer: Optional[str] = Field(None, description="Primary customer")
    bl_number: Optional[int] = Field(None, description="BL assignment (1-5)")
    created_at: datetime = Field(..., description="Created timestamp")


# ===================
# WAREHOUSE ORDER SCHEMAS
# ===================

class WarehouseOrderCreate(BaseSchema):
    """
    Create a new warehouse order.

    Required: boat_id, items
    Optional: notes, exported_by
    """

    boat_id: str = Field(..., description="Boat schedule UUID")
    items: List[WarehouseOrderItemCreate] = Field(
        ...,
        min_length=1,
        description="Order line items"
    )
    notes: Optional[str] = Field(None, max_length=1000, description="Optional notes")
    exported_by: Optional[str] = Field(None, max_length=100, description="User who exported")
    excel_filename: Optional[str] = Field(None, max_length=255, description="Excel filename")

    # Boat info (denormalized)
    boat_departure_date: Optional[date] = Field(None, description="Boat departure date")
    boat_arrival_date: Optional[date] = Field(None, description="Boat arrival date")
    estimated_warehouse_date: Optional[date] = Field(None, description="Estimated warehouse date")
    boat_name: Optional[str] = Field(None, max_length=255, description="Boat name/identifier")


class WarehouseOrderUpdate(BaseSchema):
    """
    Update warehouse order.

    All fields optional - only provided fields are updated.
    """

    status: Optional[WarehouseOrderStatus] = Field(None, description="Order status")
    notes: Optional[str] = Field(None, max_length=1000, description="Notes")
    shipped_at: Optional[datetime] = Field(None, description="When shipped")
    received_at: Optional[datetime] = Field(None, description="When received")


class WarehouseOrderStatusUpdate(BaseSchema):
    """Update only the status of a warehouse order."""

    status: WarehouseOrderStatus = Field(..., description="New status")


class WarehouseOrderResponse(BaseSchema, TimestampMixin):
    """
    Warehouse order response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Order UUID")
    boat_id: Optional[str] = Field(None, description="Boat schedule UUID")
    status: WarehouseOrderStatus = Field(..., description="Current status")

    # Timestamps
    shipped_at: Optional[datetime] = Field(None, description="When shipped")
    received_at: Optional[datetime] = Field(None, description="When received")
    cancelled_at: Optional[datetime] = Field(None, description="When cancelled")

    # Boat info
    boat_departure_date: Optional[date] = Field(None, description="Boat departure date")
    boat_arrival_date: Optional[date] = Field(None, description="Boat arrival date")
    estimated_warehouse_date: Optional[date] = Field(None, description="Estimated warehouse date")
    boat_name: Optional[str] = Field(None, description="Boat name")

    # Export metadata
    export_date: Optional[datetime] = Field(None, description="When exported")
    exported_by: Optional[str] = Field(None, description="User who exported")
    excel_filename: Optional[str] = Field(None, description="Excel filename")

    # Totals
    total_pallets: int = Field(default=0, description="Total pallets")
    total_m2: Decimal = Field(default=Decimal("0"), description="Total m2")
    total_containers: int = Field(default=0, description="Total containers")
    total_weight_kg: Decimal = Field(default=Decimal("0"), description="Total weight in kg")

    # Notes
    notes: Optional[str] = Field(None, description="Notes")

    # Computed
    item_count: Optional[int] = Field(None, description="Number of line items")


class WarehouseOrderWithItemsResponse(WarehouseOrderResponse):
    """Warehouse order response with line items included."""

    items: List[WarehouseOrderItemResponse] = Field(
        default_factory=list,
        description="Order line items"
    )


class WarehouseOrderListResponse(BaseSchema):
    """List of warehouse orders with pagination."""

    data: List[WarehouseOrderResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


# ===================
# QUERY HELPERS
# ===================

class PendingOrdersForBoat(BaseSchema):
    """Pending orders for a specific boat (used in coverage calculation)."""

    boat_id: str
    boat_departure_date: date
    total_pallets: int
    total_m2: Decimal
    order_count: int


class PendingOrdersBySku(BaseSchema):
    """Pending order quantities by SKU (used in coverage calculation)."""

    sku: str
    product_id: Optional[str]
    total_pallets: int
    total_m2: Decimal
    order_count: int
    boat_ids: List[str]
