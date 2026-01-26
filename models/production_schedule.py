"""
Production schedule schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field
from typing import Optional
from enum import Enum
from datetime import date, datetime
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


class FactoryStatus(str, Enum):
    """Production status for Order Builder display."""
    IN_PRODUCTION = "in_production"      # Scheduled in current PDF
    NOT_SCHEDULED = "not_scheduled"      # Not in current PDF


class ProductionScheduleLineItem(BaseSchema):
    """
    Single line item from a production schedule.

    Represents one product being produced on one date at one plant.
    """

    production_date: date = Field(
        ...,
        description="Date this product will be produced"
    )
    factory_code: str = Field(
        ...,
        max_length=20,
        description="Factory internal product code (e.g., '5495')"
    )
    product_name: Optional[str] = Field(
        None,
        max_length=100,
        description="Product reference name (e.g., 'CEIBA GRIS CLARO BTE')"
    )
    plant: int = Field(
        ...,
        ge=1,
        le=2,
        description="Production plant (1 or 2)"
    )
    format: Optional[str] = Field(
        None,
        max_length=20,
        description="Tile format (e.g., '51X51')"
    )
    design: Optional[str] = Field(
        None,
        max_length=50,
        description="Design type (MADERA, MARMOLIZADO, PIEDRA, etc.)"
    )
    finish: Optional[str] = Field(
        None,
        max_length=50,
        description="Finish type (BRILLANTE, SATINADO, GRANILLA, etc.)"
    )
    shifts: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Number of production shifts"
    )
    quality_target_pct: Optional[Decimal] = Field(
        None,
        ge=0,
        le=100,
        description="Target quality percentage"
    )
    quality_actual_pct: Optional[Decimal] = Field(
        None,
        ge=0,
        le=100,
        description="Actual quality percentage"
    )
    m2_total_net: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Total net m² produced"
    )
    m2_export_first: Optional[Decimal] = Field(
        None,
        ge=0,
        description="First quality m² available for export"
    )
    pct_showroom: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="Percentage for showrooms"
    )
    pct_distribution: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="Percentage for distribution"
    )


class ParsedProductionSchedule(BaseSchema):
    """
    Parsed production schedule from Claude Vision.

    Contains metadata and list of line items.
    """

    schedule_date: date = Field(
        ...,
        description="Date the schedule was generated"
    )
    schedule_version: Optional[str] = Field(
        None,
        max_length=50,
        description="Version identifier (e.g., 'ACTUALIZACION 1')"
    )
    schedule_month: Optional[str] = Field(
        None,
        description="Month name from title (e.g., 'DICIEMBRE')"
    )
    line_items: list[ProductionScheduleLineItem] = Field(
        default_factory=list,
        description="List of production line items"
    )
    total_m2_plant1: Optional[Decimal] = Field(
        None,
        description="Total m² for plant 1"
    )
    total_m2_plant2: Optional[Decimal] = Field(
        None,
        description="Total m² for plant 2"
    )
    parsing_confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Overall parsing confidence"
    )
    parsing_notes: Optional[str] = Field(
        None,
        description="Notes or warnings from parsing"
    )


class ProductionScheduleUploadResponse(BaseSchema):
    """Response from uploading and parsing a production schedule PDF."""

    success: bool
    message: str
    parsed_data: Optional[ParsedProductionSchedule] = None
    items_count: int = 0
    matched_products: int = 0
    unmatched_factory_codes: list[str] = Field(default_factory=list)


class ProductionScheduleResponse(BaseSchema, TimestampMixin):
    """
    Production schedule database record response.

    Used for GET responses.
    """

    id: str = Field(..., description="Record UUID")
    schedule_date: date
    schedule_version: Optional[str] = None
    source_filename: Optional[str] = None
    production_date: date
    factory_code: str
    product_name: Optional[str] = None
    product_id: Optional[str] = None
    plant: int
    format: Optional[str] = None
    design: Optional[str] = None
    finish: Optional[str] = None
    shifts: Optional[Decimal] = None
    quality_target_pct: Optional[Decimal] = None
    quality_actual_pct: Optional[Decimal] = None
    m2_total_net: Optional[Decimal] = None
    m2_export_first: Optional[Decimal] = None
    pct_showroom: Optional[int] = None
    pct_distribution: Optional[int] = None


class ProductionScheduleListResponse(BaseSchema):
    """List of production schedule records with pagination."""

    data: list[ProductionScheduleResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class UpcomingProductionItem(BaseSchema):
    """
    Simplified view of upcoming production for a product.

    Used for inventory planning views.
    """

    production_date: date
    factory_code: str
    product_name: Optional[str] = None
    product_id: Optional[str] = None
    sku: Optional[str] = None
    plant: int
    m2_export_first: Optional[Decimal] = None
    days_until_production: int = Field(
        ...,
        description="Days from today until production"
    )


class UpcomingProductionResponse(BaseSchema):
    """Response with upcoming production grouped by product."""

    data: list[UpcomingProductionItem]
    total_m2_upcoming: Decimal
    date_range_start: date
    date_range_end: date


# ===================
# ORDER BUILDER INTEGRATION
# ===================

class MatchSuggestion(BaseSchema):
    """Suggested SKU match for an unmapped factory code."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    score: int = Field(..., ge=0, le=100, description="Match confidence 0-100")
    match_reason: str = Field(default="fuzzy", description="Why this match was suggested")


class UnmappedProduct(BaseSchema):
    """
    Factory product code that couldn't be matched to a SKU.

    Includes fuzzy match suggestions for manual mapping.
    """

    factory_code: str = Field(..., description="Factory internal code")
    factory_name: str = Field(..., description="Product name from PDF")

    # Production info
    total_m2: Decimal = Field(..., description="Total m² in schedule")
    production_dates: list[str] = Field(default_factory=list, description="Scheduled dates")
    row_count: int = Field(default=1, description="Number of schedule rows")

    # Match suggestions
    suggested_matches: list[MatchSuggestion] = Field(
        default_factory=list,
        description="Fuzzy match suggestions from products table"
    )


class MapProductRequest(BaseSchema):
    """Request to map a factory code to a product."""

    factory_code: str = Field(..., description="Factory internal code")
    product_id: str = Field(..., description="Product UUID to link")


class MapProductResponse(BaseSchema):
    """Response after mapping a product."""

    factory_code: str
    product_id: str
    product_sku: str
    rows_updated: int = Field(..., description="Schedule rows updated with this mapping")


class UploadResult(BaseSchema):
    """Result of uploading a production schedule PDF."""

    # Counts
    total_rows: int = Field(..., description="Total rows parsed from PDF")
    matched_count: int = Field(..., description="Rows matched to products")
    unmatched_count: int = Field(..., description="Rows without product match")

    # Schedule info
    schedule_date: date = Field(..., description="Date from PDF")
    schedule_version: Optional[str] = Field(None, description="Version from filename")
    filename: str = Field(..., description="Original filename")

    # Products needing mapping
    unmatched_products: list[UnmappedProduct] = Field(
        default_factory=list,
        description="Factory codes needing manual mapping"
    )

    # Warnings
    warnings: list[str] = Field(default_factory=list, description="Parse warnings")


class ProductFactoryStatus(BaseSchema):
    """
    Factory production status for a single product.

    Used by Order Builder to show production info.
    """

    product_id: str
    sku: str

    # Status
    status: FactoryStatus = Field(
        default=FactoryStatus.NOT_SCHEDULED,
        description="in_production or not_scheduled"
    )

    # Production details (if in_production)
    production_date: Optional[date] = Field(None, description="When production completes")
    production_m2: Optional[Decimal] = Field(None, description="Total m² in production")
    days_until_ready: Optional[int] = Field(None, description="Days until production_date")

    # Timing assessment (relative to boat)
    ready_before_boat: Optional[bool] = Field(
        None,
        description="True if production_date is before boat departure (with buffer)"
    )
    timing_message: Optional[str] = Field(
        None,
        description="Human-readable timing status"
    )
