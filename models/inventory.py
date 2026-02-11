"""
Inventory snapshot schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field, field_validator
from typing import Optional
from datetime import date, datetime

from models.base import BaseSchema, TimestampMixin


class InventorySnapshotCreate(BaseSchema):
    """
    Create a new inventory snapshot.

    Required: product_id, warehouse_qty, snapshot_date
    Optional: in_transit_qty, notes
    """

    product_id: str = Field(
        ...,
        description="Product UUID"
    )
    warehouse_qty: float = Field(
        ...,
        ge=0,
        description="Warehouse quantity in m²"
    )
    in_transit_qty: float = Field(
        default=0,
        ge=0,
        description="In-transit quantity in m² (on boat)"
    )
    snapshot_date: date = Field(
        ...,
        description="Date of inventory count"
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional notes"
    )

    @field_validator("warehouse_qty", "in_transit_qty")
    @classmethod
    def round_quantity(cls, v: float) -> float:
        """Round quantities to 2 decimal places."""
        return round(v, 2)

    @field_validator("snapshot_date")
    @classmethod
    def not_future_date(cls, v: date) -> date:
        """Snapshot date cannot be in the future."""
        if v > date.today():
            raise ValueError("Snapshot date cannot be in the future")
        return v


class InventorySnapshotUpdate(BaseSchema):
    """
    Update existing inventory snapshot.

    All fields optional - only provided fields are updated.
    """

    warehouse_qty: Optional[float] = Field(
        None,
        ge=0,
        description="Warehouse quantity in m²"
    )
    in_transit_qty: Optional[float] = Field(
        None,
        ge=0,
        description="In-transit quantity in m²"
    )
    snapshot_date: Optional[date] = Field(
        None,
        description="Date of inventory count"
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional notes"
    )
    # Factory availability fields
    factory_available_m2: Optional[float] = Field(
        None,
        ge=0,
        description="Factory finished goods available in m²"
    )
    factory_largest_lot_m2: Optional[float] = Field(
        None,
        ge=0,
        description="Largest single lot size in m²"
    )
    factory_largest_lot_code: Optional[str] = Field(
        None,
        max_length=100,
        description="Lot code of largest lot"
    )
    factory_lot_count: Optional[int] = Field(
        None,
        ge=0,
        description="Number of lots available"
    )

    @field_validator("warehouse_qty", "in_transit_qty")
    @classmethod
    def round_quantity(cls, v: Optional[float]) -> Optional[float]:
        """Round quantities to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)

    @field_validator("snapshot_date")
    @classmethod
    def not_future_date(cls, v: Optional[date]) -> Optional[date]:
        """Snapshot date cannot be in the future."""
        if v is None:
            return v
        if v > date.today():
            raise ValueError("Snapshot date cannot be in the future")
        return v


class InventorySnapshotResponse(BaseSchema):
    """
    Inventory snapshot response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Snapshot UUID")
    product_id: str = Field(..., description="Product UUID")
    warehouse_qty: float = Field(..., description="Warehouse quantity in m²")
    in_transit_qty: float = Field(default=0, description="In-transit quantity in m²")
    snapshot_date: date = Field(..., description="Date of inventory count")
    notes: Optional[str] = Field(None, description="Optional notes")
    created_at: datetime = Field(..., description="Record creation timestamp")
    # Factory availability fields
    factory_available_m2: Optional[float] = Field(default=0, description="Factory finished goods available in m²")
    factory_largest_lot_m2: Optional[float] = Field(default=None, description="Largest single lot size in m²")
    factory_largest_lot_code: Optional[str] = Field(default=None, description="Lot code of largest lot")
    factory_lot_count: Optional[int] = Field(default=0, description="Number of lots available")


class InventorySnapshotWithProduct(InventorySnapshotResponse):
    """
    Inventory snapshot with product details.

    Extended response for dashboard views.
    """

    sku: Optional[str] = Field(None, description="Product SKU")
    category: Optional[str] = Field(None, description="Product category")
    rotation: Optional[str] = Field(None, description="Product rotation")


class InventoryListResponse(BaseSchema):
    """List of inventory snapshots with pagination."""

    data: list[InventorySnapshotResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class InventoryCurrentResponse(BaseSchema):
    """Latest inventory per product for dashboard."""

    data: list[InventorySnapshotWithProduct]
    total: int
    as_of: date = Field(..., description="Most recent snapshot date in results")


class InventoryUploadResponse(BaseSchema):
    """Response from inventory upload."""

    success: bool
    records_created: int
    message: str


class InTransitProductDetail(BaseSchema):
    """Single product in-transit detail."""
    sku: str
    in_transit_m2: float


class InTransitUploadResponse(BaseSchema):
    """Response from in-transit dispatch upload."""
    success: bool
    snapshot_date: date
    products_updated: int
    products_reset: int
    total_in_transit_m2: float
    excluded_orders: list[str] = Field(default_factory=list)
    unmatched_skus: list[str] = Field(default_factory=list)
    details: list[InTransitProductDetail] = Field(default_factory=list)


class BulkInventoryCreate(BaseSchema):
    """
    Bulk create inventory snapshots from parsed Excel.

    Used internally by upload endpoint.
    """

    snapshots: list[InventorySnapshotCreate]
