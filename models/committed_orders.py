"""
Committed orders (pedidos/cotizaciones comprometidos) schemas.

Tracks committed customer orders from SIESA ERP.
Used to understand demand that has been promised but not yet fulfilled.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field
from typing import Optional
from datetime import date

from models.base import BaseSchema


class CommittedOrderPreviewRow(BaseSchema):
    """Single row in the committed orders preview."""

    sku: str = Field(..., description="Product SKU from Excel (Referencia or Desc. item)")
    product_id: Optional[str] = Field(None, description="Matched product UUID")
    quantity_committed: float = Field(..., ge=0, description="Committed quantity from SIESA")
    current_stock: Optional[float] = Field(None, description="Current stock (Existencia) - informational")
    available_qty: Optional[float] = Field(None, description="Available quantity (Cant. disponible) - informational")
    warehouse_code: Optional[str] = Field(None, description="Warehouse code (Bodega)")
    order_reference: Optional[str] = Field(None, description="Order reference (pedido)")
    matched: bool = Field(default=True, description="Whether SKU matched a known product")


class CommittedOrderPreview(BaseSchema):
    """Preview response for committed orders upload."""

    preview_id: str = Field(..., description="UUID to reference this preview")
    row_count: int = Field(..., description="Total rows parsed")
    snapshot_date: date = Field(..., description="Snapshot date (today, since file is point-in-time)")
    warnings: list[str] = Field(default_factory=list)
    rows: list[CommittedOrderPreviewRow] = Field(
        default_factory=list,
        description="All parsed rows for review"
    )
    expires_in_minutes: int = Field(default=30, description="Minutes until preview expires")


class CommittedOrderModification(BaseSchema):
    """A single row modification during preview editing."""

    sku: str = Field(..., description="SKU to modify")
    quantity_committed: Optional[float] = Field(None, ge=0, description="New committed quantity")


class CommittedOrderConfirmRequest(BaseSchema):
    """Confirm committed orders upload with optional edits."""

    modifications: list[CommittedOrderModification] = Field(
        default_factory=list,
        description="Rows to modify"
    )
    deletions: list[str] = Field(
        default_factory=list,
        description="SKUs to exclude from import"
    )


class CommittedOrderResponse(BaseSchema):
    """Response from committed orders confirm."""

    success: bool
    records_upserted: int
    snapshot_date: str
    message: str
