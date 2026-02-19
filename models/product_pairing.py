"""
Product pairing schemas for mueble <-> lavamano tracking.

Pairs products that must be ordered together (e.g., every mueble
needs a matching lavamano). Used by RecommendationService to flag
inventory mismatches and adjust order quantities.
"""

from pydantic import Field
from typing import Optional
from decimal import Decimal

from models.base import BaseSchema


class PairedProductInfo(BaseSchema):
    """Info about a paired product shown alongside the primary product."""

    paired_product_id: str = Field(..., description="UUID of the paired product")
    paired_sku: str = Field(..., description="SKU of the paired product")
    ratio: Decimal = Field(..., description="Pairing ratio (e.g., 1.0 means 1:1)")
    paired_warehouse_qty: Optional[Decimal] = Field(
        None, description="Paired product's current warehouse quantity"
    )
    paired_in_transit_qty: Optional[Decimal] = Field(
        None, description="Paired product's in-transit quantity"
    )
    inventory_mismatch: bool = Field(
        default=False,
        description="True when stock doesn't match ratio",
    )
    mismatch_detail: Optional[str] = Field(
        None,
        description="Human-readable mismatch description (e.g., '15 muebles but only 10 lavamanos')",
    )
