"""
Sales record schemas.

Tracks weekly sales per product from owner Excel uploads.
"""

from datetime import date, datetime
from typing import Optional
from decimal import Decimal
from pydantic import Field, field_validator

from models.base import BaseSchema, TimestampMixin


class SalesRecordCreate(BaseSchema):
    """Schema for creating a sales record."""

    product_id: str = Field(..., description="Product UUID")
    week_start: date = Field(..., description="Start of the week (Monday)")
    quantity_m2: Decimal = Field(..., ge=0, description="Sales quantity in m²")
    customer: Optional[str] = Field(None, description="Original customer name (with accents)")
    customer_normalized: Optional[str] = Field(None, description="Normalized for grouping (uppercase ASCII)")
    unit_price_usd: Optional[Decimal] = Field(None, ge=0, description="Unit price per m² in USD")
    total_price_usd: Optional[Decimal] = Field(None, ge=0, description="Total sale price in USD")

    @field_validator("quantity_m2", mode="before")
    @classmethod
    def round_quantity(cls, v):
        """Round to 2 decimal places."""
        if v is not None:
            return round(Decimal(str(v)), 2)
        return v

    @field_validator("week_start", mode="before")
    @classmethod
    def parse_date(cls, v):
        """Parse date from string or datetime."""
        if isinstance(v, str):
            return date.fromisoformat(v)
        if isinstance(v, datetime):
            return v.date()
        return v


class SalesRecordUpdate(BaseSchema):
    """Schema for updating a sales record."""

    week_start: Optional[date] = None
    quantity_m2: Optional[Decimal] = Field(None, ge=0)
    unit_price_usd: Optional[Decimal] = Field(None, ge=0, description="Unit price per m² in USD")
    total_price_usd: Optional[Decimal] = Field(None, ge=0, description="Total sale price in USD")

    @field_validator("quantity_m2", "unit_price_usd", "total_price_usd", mode="before")
    @classmethod
    def round_quantity(cls, v):
        """Round to 2 decimal places."""
        if v is not None:
            return round(Decimal(str(v)), 2)
        return v


class SalesRecordResponse(TimestampMixin, BaseSchema):
    """Schema for sales record response."""

    id: str
    product_id: str
    week_start: date
    quantity_m2: Decimal
    customer: Optional[str] = None
    customer_normalized: Optional[str] = None
    unit_price_usd: Optional[Decimal] = None
    total_price_usd: Optional[Decimal] = None

    @field_validator("quantity_m2", "unit_price_usd", "total_price_usd", mode="before")
    @classmethod
    def ensure_decimal(cls, v):
        """Ensure quantity is Decimal."""
        if v is not None:
            return Decimal(str(v))
        return v


class SalesRecordWithProduct(SalesRecordResponse):
    """Sales record with product details."""

    sku: str
    category: str
    rotation: Optional[str] = None


class SalesListResponse(BaseSchema):
    """Paginated sales list response."""

    data: list[SalesRecordResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class SalesHistoryResponse(BaseSchema):
    """Sales history for a single product."""

    product_id: str
    sku: str
    records: list[SalesRecordResponse]
    total_m2: Decimal
    weeks_count: int


class VerificationCheck(BaseSchema):
    """Single verification check comparing Excel vs DB."""
    excel: float
    db: float
    match: bool


class SalesMismatch(BaseSchema):
    """Per-product m² mismatch between Excel and DB."""
    sku: str
    excel_m2: float
    db_m2: float
    diff: float


class SalesVerification(BaseSchema):
    """Post-import verification results."""
    status: str  # "VERIFIED" or "MISMATCH"
    row_count: VerificationCheck
    total_m2: VerificationCheck
    products: VerificationCheck
    mismatches: list[SalesMismatch] = []


class SalesUploadResponse(BaseSchema):
    """Response from sales upload."""

    success: bool = True
    inserted: int
    deleted: int = 0
    date_range: Optional[dict] = None
    verification: Optional[SalesVerification] = None
    warnings: list[str] = []


class BulkSalesCreate(BaseSchema):
    """Schema for bulk sales creation."""

    records: list[SalesRecordCreate]


class SalesPreviewRow(BaseSchema):
    """Sample row shown in preview."""
    row_index: int = 0
    sku: str
    week_start: date
    quantity_m2: float
    customer: Optional[str] = None


class SalesPreview(BaseSchema):
    """Preview response — stats + sample rows, nothing saved yet."""
    preview_id: str
    row_count: int
    product_count: int
    total_m2: float
    date_range_start: date
    date_range_end: date
    warnings: list[str] = Field(default_factory=list)
    rows: list[SalesPreviewRow] = Field(default_factory=list, description="All rows for editing")
    sample_rows: list[SalesPreviewRow] = Field(default_factory=list, description="Deprecated: use rows instead")
    expires_in_minutes: int = 30


class SalesModification(BaseSchema):
    """A single row modification during sales preview editing."""
    row_index: int = Field(..., description="Row index to modify")
    quantity_m2: Optional[Decimal] = Field(None, ge=0, description="New sales quantity (m²)")
    customer: Optional[str] = Field(None, description="New customer name")


class SalesConfirmRequest(BaseSchema):
    """Confirm sales upload with optional edits."""
    preview_id: str
    modifications: list[SalesModification] = Field(default_factory=list)
    deletions: list[int] = Field(default_factory=list, description="Row indices to exclude from import")


class SACUploadResponse(BaseSchema):
    """Response from SAC sales CSV upload."""

    created: int
    deleted: int = 0
    total_rows: int
    matched_by_sac_sku: int
    matched_by_name: int
    unmatched_count: int
    match_rate_pct: float
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None

    # Summary statistics
    total_m2_sold: float = 0.0
    unique_customers: int = 0
    unique_products: int = 0
    top_product: Optional[str] = None
    skipped_non_tile: int = 0
    skipped_products: list[str] = []

    unmatched_products: list[str] = []
    errors: list[dict] = []


class SACPreviewRow(BaseSchema):
    """Sample row shown in SAC sales preview."""
    sku: str
    sale_date: date
    quantity_m2: float
    customer: Optional[str] = None
    matched_by: str = "sac_sku"  # "sac_sku" or "name"


class SACPreview(BaseSchema):
    """Preview response for SAC sales upload."""
    preview_id: str
    row_count: int
    total_m2: float
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    matched_by_sac_sku: int
    matched_by_name: int
    unmatched_count: int
    match_rate_pct: float
    unmatched_products: list[str] = Field(default_factory=list)
    unique_customers: int = 0
    unique_products: int = 0
    top_product: Optional[str] = None
    skipped_non_tile: int = 0
    skipped_products: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    sample_rows: list[SACPreviewRow] = Field(default_factory=list)
    expires_in_minutes: int = 30
