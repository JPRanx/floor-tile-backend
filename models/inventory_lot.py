"""
Inventory lot schemas for SIESA factory inventory tracking.

Lot-level tracking is critical because:
- Tiles from different lots may have color variations
- Large orders need tiles from same lot for consistency
- Weight data enables container planning

See BUILDER_BLUEPRINT.md for specifications.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from pydantic import Field

from models.base import BaseSchema, TimestampMixin


class InventoryLotCreate(BaseSchema):
    """Schema for creating an inventory lot record."""

    product_id: str = Field(..., description="Product UUID")
    lot_number: str = Field(..., max_length=50, description="Factory lot identifier")
    quantity_m2: Decimal = Field(..., ge=0, description="Available m²")
    weight_kg: Optional[Decimal] = Field(None, ge=0, description="Weight in kg")
    quality: Optional[str] = Field(None, max_length=50, description="Quality grade")
    warehouse_code: Optional[str] = Field(None, max_length=20, description="Warehouse code")
    warehouse_name: Optional[str] = Field(None, max_length=100, description="Warehouse name")
    snapshot_date: date = Field(..., description="Date of inventory snapshot")
    siesa_item: Optional[int] = Field(None, description="SIESA item code")
    siesa_description: Optional[str] = Field(None, max_length=255, description="Original SIESA description")


class InventoryLotResponse(TimestampMixin, BaseSchema):
    """Schema for inventory lot response."""

    id: str
    product_id: str
    lot_number: str
    quantity_m2: Decimal
    weight_kg: Optional[Decimal] = None
    quality: Optional[str] = None
    warehouse_code: Optional[str] = None
    warehouse_name: Optional[str] = None
    snapshot_date: date
    siesa_item: Optional[int] = None
    siesa_description: Optional[str] = None


class WarehouseSummary(BaseSchema):
    """Summary of inventory by warehouse."""

    code: str
    name: str
    total_m2: float
    total_weight_kg: float
    lot_count: int


class RowError(BaseSchema):
    """Error for a specific row in the upload."""

    row: int
    field: str
    error: str
    value: Optional[str] = None


class SIESAUploadResponse(BaseSchema):
    """Response from SIESA inventory upload."""

    success: bool
    snapshot_date: date
    total_rows: int
    processed_rows: int
    skipped_errors: int
    errors: list[RowError] = []

    # Lot statistics
    lots_created: int
    unique_products: int
    total_m2_available: float
    total_weight_kg: float

    # Container calculations
    container_limit_kg: int = 1881
    containers_needed: int
    container_utilization_pct: float

    # Matching status
    matched_by_siesa_item: int
    matched_by_name: int
    unmatched_count: int
    match_rate_pct: float
    unmatched_products: list[str] = []

    # Warehouse breakdown
    warehouses: list[WarehouseSummary] = []


class ProductLotSummary(BaseSchema):
    """Summary of lots for a single product."""

    product_id: str
    sku: str
    total_m2: float
    total_weight_kg: float
    lot_count: int
    lots: list[InventoryLotResponse]


class InventoryLotsListResponse(BaseSchema):
    """Paginated list of inventory lots."""

    data: list[InventoryLotResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class ContainerEstimateResponse(BaseSchema):
    """Response for container estimate calculation."""

    weight_kg: float
    container_limit_kg: int
    containers_needed: int
    utilization_breakdown: list[dict]
