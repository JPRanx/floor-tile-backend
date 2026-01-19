"""
Production schedule schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


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
