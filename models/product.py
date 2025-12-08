"""
Product schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from enum import Enum
from datetime import datetime

from models.base import BaseSchema, TimestampMixin


class Category(str, Enum):
    """Product categories."""
    MADERAS = "MADERAS"
    EXTERIORES = "EXTERIORES"
    MARMOLIZADOS = "MARMOLIZADOS"


class Rotation(str, Enum):
    """Product rotation/velocity classification."""
    ALTA = "ALTA"
    MEDIA_ALTA = "MEDIA-ALTA"
    MEDIA = "MEDIA"
    BAJA = "BAJA"


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
    
    @field_validator("sku")
    @classmethod
    def sku_uppercase(cls, v: Optional[str]) -> Optional[str]:
        """SKU must be uppercase if provided."""
        if v is None:
            return v
        return v.upper().strip()


class ProductResponse(BaseSchema, TimestampMixin):
    """
    Product response with all fields.
    
    Used for GET responses.
    """
    
    id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    category: Category = Field(..., description="Product category")
    rotation: Optional[Rotation] = Field(None, description="Sales velocity")
    active: bool = Field(..., description="Whether product is active")


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
