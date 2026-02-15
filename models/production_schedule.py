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


# ===================
# EXCEL-BASED PRODUCTION SCHEDULE (Programa de Produccion)
# ===================
# These models support the Excel-based production schedule parsing
# with color-coded status detection (white=scheduled, blue=in_progress, green=completed)


class ProductionStatus(str, Enum):
    """
    Production status derived from Excel cell colors.

    Key insight: 'scheduled' items CAN HAVE MORE QUANTITY ADDED before production starts!
    """
    SCHEDULED = "scheduled"          # White/no fill - NOT STARTED, CAN ADD MORE!
    IN_PROGRESS = "in_progress"      # Light blue - currently manufacturing
    COMPLETED = "completed"          # Green - finished, ready to ship


class ProductionScheduleCreate(BaseSchema):
    """
    Create a production schedule record from Excel parsing.

    Used when importing data from Programa de Produccion Excel files.
    """

    factory_item_code: Optional[str] = Field(
        None,
        max_length=50,
        description="Factory internal item code (e.g., 5549)"
    )
    referencia: str = Field(
        ...,
        max_length=255,
        description="Product reference name from factory (e.g., SAMAN BEIGE BTE)"
    )
    sku: Optional[str] = Field(
        None,
        max_length=50,
        description="Matched SKU from our system"
    )
    product_id: Optional[str] = Field(
        None,
        description="Linked product UUID"
    )

    # Production data
    plant: str = Field(
        ...,
        description="Plant identifier: 'plant_1' or 'plant_2'"
    )
    requested_m2: Decimal = Field(
        default=Decimal("0"),
        ge=0,
        description="m2 Primera exportacion under Programa - what Guatemala requested"
    )
    completed_m2: Decimal = Field(
        default=Decimal("0"),
        ge=0,
        description="m2 Primera exportacion under Real - what factory completed"
    )

    # Status
    status: ProductionStatus = Field(
        default=ProductionStatus.SCHEDULED,
        description="Status from cell color: scheduled=white, in_progress=blue, completed=green"
    )

    # Dates
    scheduled_start_date: Optional[date] = Field(
        None, description="Fecha Inicio from Excel"
    )
    scheduled_end_date: Optional[date] = Field(
        None, description="Fecha Fin from Excel"
    )
    estimated_delivery_date: Optional[date] = Field(
        None, description="Fecha estimada entrega from Excel"
    )

    # Source tracking
    source_file: Optional[str] = Field(
        None, description="Original filename"
    )
    source_month: Optional[str] = Field(
        None, description="Month identifier (e.g., ENERO-26)"
    )
    source_row: Optional[int] = Field(
        None, description="Row number in Excel for debugging"
    )


class ProductionScheduleDBResponse(BaseSchema, TimestampMixin):
    """
    Production schedule record from database.

    Includes computed fields for Order Builder integration.
    """

    id: str = Field(..., description="Record UUID")
    factory_item_code: Optional[str] = None
    referencia: str
    sku: Optional[str] = None
    product_id: Optional[str] = None

    # Production data
    plant: str
    requested_m2: Decimal
    completed_m2: Decimal
    remaining_m2: Decimal = Field(
        default=Decimal("0"),
        description="requested_m2 - completed_m2"
    )

    # Status
    status: ProductionStatus
    can_add_more: bool = Field(
        default=False,
        description="True if status='scheduled' - production hasn't started yet"
    )

    # Dates
    scheduled_start_date: Optional[date] = None
    scheduled_end_date: Optional[date] = None
    estimated_delivery_date: Optional[date] = None
    actual_completion_date: Optional[date] = None

    # Source
    source_file: Optional[str] = None
    source_month: Optional[str] = None

    @classmethod
    def from_db(cls, row: dict) -> "ProductionScheduleDBResponse":
        """Create response from database row with computed fields."""
        requested = Decimal(str(row.get("requested_m2", 0)))
        completed = Decimal(str(row.get("completed_m2", 0)))
        status = row.get("status", "scheduled")

        return cls(
            id=str(row["id"]),
            factory_item_code=row.get("factory_item_code"),
            referencia=row["referencia"],
            sku=row.get("sku"),
            product_id=str(row["product_id"]) if row.get("product_id") else None,
            plant=row["plant"],
            requested_m2=requested,
            completed_m2=completed,
            remaining_m2=requested - completed,
            status=ProductionStatus(status),
            can_add_more=(status == "scheduled"),
            scheduled_start_date=row.get("scheduled_start_date"),
            scheduled_end_date=row.get("scheduled_end_date"),
            estimated_delivery_date=row.get("estimated_delivery_date"),
            actual_completion_date=row.get("actual_completion_date"),
            source_file=row.get("source_file"),
            source_month=row.get("source_month"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


class ProductionSummary(BaseSchema):
    """
    Summary of production by status.

    Used for dashboard and capacity planning.
    """

    status: ProductionStatus
    item_count: int
    total_requested_m2: Decimal
    total_completed_m2: Decimal
    total_remaining_m2: Decimal
    action_hint: str = Field(
        ...,
        description="Action suggestion: 'CAN ADD MORE', 'MANUFACTURING', 'READY TO SHIP'"
    )


class ProductionCapacity(BaseSchema):
    """
    Factory request capacity tracking.

    Guatemala has a 60,000 m² monthly quota.
    """

    monthly_limit_m2: Decimal = Field(
        default=Decimal("60000"),
        description="Monthly quota for Guatemala requests"
    )
    already_requested_m2: Decimal = Field(
        default=Decimal("0"),
        description="Sum of all requested_m2 for current month"
    )
    available_to_request_m2: Decimal = Field(
        default=Decimal("60000"),
        description="monthly_limit - already_requested"
    )
    utilization_pct: Decimal = Field(
        default=Decimal("0"),
        description="Percentage of monthly quota used"
    )

    # Breakdown by status
    completed_m2: Decimal = Field(default=Decimal("0"))
    in_progress_m2: Decimal = Field(default=Decimal("0"))
    scheduled_m2: Decimal = Field(default=Decimal("0"))

    # Items that can have more added
    can_add_more_items: list[str] = Field(
        default_factory=list,
        description="Referencias of items in 'scheduled' status"
    )


class ProductionImportResult(BaseSchema):
    """Result of importing production schedule from Excel."""

    filename: str
    source_month: str

    # Counts
    total_rows_parsed: int
    rows_with_guatemala_data: int  # Rows with Primera exportacion data

    # Import results
    inserted: int = 0
    updated: int = 0
    skipped: int = 0

    # Product mapping
    matched_to_products: int = 0
    unmatched_referencias: list[str] = Field(default_factory=list)

    # Status breakdown
    completed_count: int = 0
    in_progress_count: int = 0
    scheduled_count: int = 0

    # Totals
    total_requested_m2: Decimal = Decimal("0")
    total_completed_m2: Decimal = Decimal("0")

    # Warnings
    warnings: list[str] = Field(default_factory=list)


class CanAddMoreAlert(BaseSchema):
    """
    Alert for Order Builder when a product can have more added to factory request.

    Key business value: Identify products in 'scheduled' status where we could
    request additional quantity before production starts.
    """

    product_id: Optional[str] = None
    sku: Optional[str] = None
    referencia: str

    # Current factory request
    current_requested_m2: Decimal
    current_status: ProductionStatus

    # Order Builder suggestion
    order_builder_suggested_m2: Decimal = Field(
        default=Decimal("0"),
        description="What Order Builder thinks we need"
    )

    # Gap analysis
    gap_m2: Decimal = Field(
        default=Decimal("0"),
        description="order_builder_suggested - current_requested"
    )
    should_add_more: bool = Field(
        default=False,
        description="True if gap > 0 and status='scheduled'"
    )

    # Alert message
    alert_message: Optional[str] = Field(
        None,
        description="Human-readable alert: 'Add 1,000 m² before production starts!'"
    )


# ===================
# PRODUCTION PREVIEW MODELS
# ===================

class ProductionPreviewRow(BaseSchema):
    """Sample row shown in production schedule preview."""
    referencia: str
    sku: Optional[str] = None
    plant: str
    requested_m2: Decimal = Decimal("0")
    completed_m2: Decimal = Decimal("0")
    status: str = "scheduled"
    estimated_delivery_date: Optional[date] = None


class ProductionPreview(BaseSchema):
    """Preview response for production schedule upload."""
    preview_id: str
    filename: str
    source_month: str
    total_rows: int
    rows_with_data: int
    matched_to_products: int
    unmatched_count: int
    unmatched_referencias: list[str] = Field(default_factory=list)
    total_requested_m2: Decimal = Decimal("0")
    total_completed_m2: Decimal = Decimal("0")
    status_breakdown: dict = Field(default_factory=dict, description="{'scheduled': X, 'in_progress': Y, 'completed': Z}")
    existing_records_to_delete: int = 0
    warnings: list[str] = Field(default_factory=list)
    sample_rows: list[ProductionPreviewRow] = Field(default_factory=list)
    expires_in_minutes: int = 30
