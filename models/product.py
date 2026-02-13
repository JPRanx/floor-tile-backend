"""
Product schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from enum import Enum
from datetime import datetime, date
from decimal import Decimal

from models.base import BaseSchema, TimestampMixin


class Category(str, Enum):
    """Product categories.

    Tile categories (used in category analysis):
    - MADERAS: Wood-look tiles
    - EXTERIORES: Outdoor/exterior tiles
    - MARMOLIZADOS: Marble-look tiles
    - OTHER: Uncategorized tile products

    Non-tile categories (excluded from category analysis):
    - FURNITURE: Furniture items
    - SINK: Sink products
    - SURCHARGE: Surcharges/fees
    """
    # Tile categories
    MADERAS = "MADERAS"
    EXTERIORES = "EXTERIORES"
    MARMOLIZADOS = "MARMOLIZADOS"
    OTHER = "OTHER"
    # Non-tile categories (excluded from category analysis)
    FURNITURE = "FURNITURE"
    SINK = "SINK"
    SURCHARGE = "SURCHARGE"


# Categories to include in category analysis (tiles only)
TILE_CATEGORIES = {Category.MADERAS, Category.EXTERIORES, Category.MARMOLIZADOS, Category.OTHER}


class Rotation(str, Enum):
    """Product rotation/velocity classification."""
    ALTA = "ALTA"
    MEDIA_ALTA = "MEDIA-ALTA"
    MEDIA = "MEDIA"
    BAJA = "BAJA"


class InactiveReason(str, Enum):
    """Reason for product deactivation."""
    DISCONTINUED = "DISCONTINUED"    # Factory stopped making it
    NO_STOCK = "NO_STOCK"            # No inventory and no plans to reorder
    SEASONAL = "SEASONAL"            # Temporarily unavailable (seasonal)
    REPLACED = "REPLACED"            # Replaced by another product
    OTHER = "OTHER"                  # Other reason


class ProductCreate(BaseSchema):
    """
    Create a new product.
    
    Required: sku, category
    Optional: rotation
    """
    
    sku: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Product SKU (unique identifier)",
        examples=["NOGAL CAFÉ", "CEIBA GRIS OSC"]
    )
    category: Category = Field(
        ...,
        description="Product category"
    )
    rotation: Optional[Rotation] = Field(
        None,
        description="Sales velocity classification"
    )
    
    @field_validator("sku")
    @classmethod
    def sku_uppercase(cls, v: str) -> str:
        """SKU must be uppercase and trimmed."""
        return v.upper().strip()


class ProductUpdate(BaseSchema):
    """
    Update existing product.

    All fields optional - only provided fields are updated.
    """

    sku: Optional[str] = Field(
        None,
        min_length=1,
        max_length=50,
        description="Product SKU"
    )
    category: Optional[Category] = Field(
        None,
        description="Product category"
    )
    rotation: Optional[Rotation] = Field(
        None,
        description="Sales velocity classification"
    )
    active: Optional[bool] = Field(
        None,
        description="Whether product is active"
    )
    fob_cost_usd: Optional[Decimal] = Field(
        None,
        ge=0,
        description="FOB cost per m² in USD"
    )
    factory_code: Optional[str] = Field(
        None,
        max_length=20,
        description="Factory internal product code (e.g., 5495)"
    )
    sac_sku: Optional[int] = Field(
        None,
        ge=1,
        description="SAC (Guatemala) ERP integer SKU code"
    )
    siesa_item: Optional[int] = Field(
        None,
        ge=1,
        description="SIESA (Factory Colombia) ERP item code"
    )
    inactive_reason: Optional[InactiveReason] = Field(
        None,
        description="Reason for deactivation"
    )
    inactive_date: Optional[date] = Field(
        None,
        description="Date when product was marked inactive"
    )

    @field_validator("sku")
    @classmethod
    def sku_uppercase(cls, v: Optional[str]) -> Optional[str]:
        """SKU must be uppercase if provided."""
        if v is None:
            return v
        return v.upper().strip()

    @field_validator("fob_cost_usd")
    @classmethod
    def round_cost(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Round to 2 decimal places."""
        if v is None:
            return v
        return round(v, 2)


class ProductResponse(BaseSchema, TimestampMixin):
    """
    Product response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    owner_code: Optional[str] = Field(None, description="Owner's Excel SKU code (e.g., '0000102', '0000119')")
    factory_code: Optional[str] = Field(None, description="Factory internal product code (e.g., 5495)")
    sac_sku: Optional[int] = Field(None, description="SAC (Guatemala) ERP integer SKU code")
    siesa_item: Optional[int] = Field(None, description="SIESA (Factory Colombia) ERP item code")
    category: Category = Field(..., description="Product category")
    rotation: Optional[Rotation] = Field(None, description="Sales velocity")
    active: bool = Field(..., description="Whether product is active")
    fob_cost_usd: Optional[Decimal] = Field(None, description="FOB cost per m² in USD")
    inactive_reason: Optional[InactiveReason] = Field(None, description="Reason for deactivation")
    inactive_date: Optional[date] = Field(None, description="Date when product was marked inactive")


class ProductListResponse(BaseSchema):
    """List of products with pagination."""
    
    data: list[ProductResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class ProductWithStats(ProductResponse):
    """
    Product with calculated statistics.

    Extended response for dashboard views.
    """

    warehouse_qty: Optional[float] = Field(None, description="Current warehouse quantity (m²)")
    daily_velocity: Optional[float] = Field(None, description="Average daily sales (m²)")
    days_until_empty: Optional[float] = Field(None, description="Days until stockout")
    status: Optional[str] = Field(None, description="Stockout status: CRITICAL, WARNING, OK, NO_SALES")


class LiquidationProductResponse(BaseSchema):
    """Deactivated product with remaining warehouse stock."""
    id: str
    sku: str
    category: Category
    rotation: Optional[Rotation] = None
    inactive_reason: Optional[InactiveReason] = None
    inactive_date: Optional[date] = None
    warehouse_m2: float              # Latest warehouse_qty from inventory_snapshots
    days_since_last_sale: Optional[int] = None  # Today - max(sales.week_start), None if no sales
