"""
Unfulfilled demand (productos faltantes) schemas.

Tracks demand that could not be fulfilled due to stockouts.
Used to adjust velocity calculations upward.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field, field_validator
from typing import Optional
from datetime import date

from models.base import BaseSchema


class UnfulfilledDemandCreate(BaseSchema):
    """
    Create a new unfulfilled demand record.

    Required: product_id, quantity_m2, snapshot_date
    """

    product_id: str = Field(
        ...,
        description="Product UUID"
    )
    quantity_m2: float = Field(
        ...,
        ge=0,
        description="Unfulfilled demand in m²"
    )
    snapshot_date: date = Field(
        ...,
        description="Date of the unfulfilled demand snapshot"
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional notes"
    )

    @field_validator("quantity_m2")
    @classmethod
    def round_quantity(cls, v: float) -> float:
        """Round quantities to 2 decimal places."""
        return round(v, 2)


class UnfulfilledDemandPreviewRow(BaseSchema):
    """Single row in the unfulfilled demand preview."""

    sku: str = Field(..., description="Product SKU from Excel")
    product_id: Optional[str] = Field(None, description="Matched product UUID")
    quantity_m2: float = Field(..., ge=0, description="Unfulfilled quantity in m²")
    snapshot_date: date = Field(..., description="Date of unfulfilled demand")
    matched: bool = Field(default=True, description="Whether SKU matched a known product")


class UnfulfilledDemandPreview(BaseSchema):
    """Preview response for unfulfilled demand upload."""

    preview_id: str = Field(..., description="UUID to reference this preview")
    row_count: int = Field(..., description="Total rows parsed")
    snapshot_date: date = Field(..., description="Snapshot date from file")
    warnings: list[str] = Field(default_factory=list)
    rows: list[UnfulfilledDemandPreviewRow] = Field(
        default_factory=list,
        description="All parsed rows for review"
    )
    expires_in_minutes: int = Field(default=30, description="Minutes until preview expires")


class UnfulfilledDemandModification(BaseSchema):
    """A single row modification during preview editing."""

    sku: str = Field(..., description="SKU to modify")
    quantity_m2: Optional[float] = Field(None, ge=0, description="New quantity in m²")


class UnfulfilledDemandConfirmRequest(BaseSchema):
    """Confirm unfulfilled demand upload with optional edits."""

    preview_id: str = Field(..., description="Preview UUID from /preview endpoint")
    modifications: list[UnfulfilledDemandModification] = Field(
        default_factory=list,
        description="Rows to modify"
    )
    deletions: list[str] = Field(
        default_factory=list,
        description="SKUs to exclude from import"
    )


class UnfulfilledDemandResponse(BaseSchema):
    """Response from unfulfilled demand confirm."""

    success: bool
    records_upserted: int
    snapshot_date: str
    message: str
