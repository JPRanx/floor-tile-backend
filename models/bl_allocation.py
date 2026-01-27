"""
BL (Bill of Lading) Allocation Models.

These models define the structure for allocating products across multiple BLs
for customs safety. Critical products (score >= 85) are spread across BLs
so that a single BL delay doesn't block all high-priority stock.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from models.base import BaseSchema


# Constants
CRITICAL_THRESHOLD = 85  # Score >= 85 is critical
MAX_CONTAINERS_PER_BL = 5
PALLETS_PER_CONTAINER = 14
M2_PER_PALLET = Decimal("134.4")


class BLProductAllocation(BaseSchema):
    """Product assigned to a specific BL."""

    product_id: str = Field(..., description="Product UUID")
    sku: str = Field(..., description="Product SKU")
    description: Optional[str] = Field(None, description="Product description")
    pallets: int = Field(..., ge=0, description="Number of pallets")
    m2: Decimal = Field(..., ge=0, description="Total m2 for this product")
    weight_kg: Decimal = Field(default=Decimal("0"), ge=0, description="Total weight in kg")
    primary_customer: Optional[str] = Field(
        None, description="Customer this product is primarily for"
    )
    score: int = Field(default=0, ge=0, le=100, description="Priority score from Order Builder")
    is_critical: bool = Field(
        default=False, description="True if score >= 85 (critical product)"
    )


class BLAllocation(BaseSchema):
    """Single BL (Bill of Lading) assignment."""

    bl_number: int = Field(..., ge=1, le=5, description="BL number 1-5")
    primary_customers: List[str] = Field(
        default_factory=list, description="Customers served by this BL"
    )
    products: List[BLProductAllocation] = Field(
        default_factory=list, description="Products in this BL"
    )
    total_pallets: int = Field(default=0, ge=0, description="Total pallets in this BL")
    total_containers: int = Field(default=0, ge=0, description="Total containers (ceil(pallets/14))")
    total_m2: Decimal = Field(default=Decimal("0"), ge=0, description="Total m2 in this BL")
    total_weight_kg: Decimal = Field(
        default=Decimal("0"), ge=0, description="Total weight in kg"
    )
    critical_product_count: int = Field(
        default=0, ge=0, description="Number of critical products in this BL"
    )


class BLAllocationReport(BaseSchema):
    """Complete BL allocation report for an order."""

    generated_at: datetime = Field(..., description="When this allocation was generated")
    boat_departure: date = Field(..., description="Boat departure date")
    boat_name: str = Field(..., description="Boat name or identifier")
    num_bls: int = Field(..., ge=1, le=5, description="Number of BLs requested")

    # Totals across all BLs
    total_containers: int = Field(default=0, ge=0, description="Total containers across all BLs")
    total_pallets: int = Field(default=0, ge=0, description="Total pallets across all BLs")
    total_m2: Decimal = Field(default=Decimal("0"), ge=0, description="Total m2 across all BLs")
    total_weight_kg: Decimal = Field(
        default=Decimal("0"), ge=0, description="Total weight across all BLs"
    )
    total_critical_products: int = Field(
        default=0, ge=0, description="Total critical products across all BLs"
    )

    # The actual allocations
    allocations: List[BLAllocation] = Field(
        default_factory=list, description="List of BL allocations"
    )

    # Risk analysis
    warnings: List[str] = Field(
        default_factory=list, description="Warnings about the allocation"
    )
    risk_distribution_even: bool = Field(
        default=False,
        description="True if critical products are evenly spread (no BL has > 40%)",
    )
    max_critical_pct: Decimal = Field(
        default=Decimal("0"),
        ge=0,
        le=100,
        description="Maximum percentage of critical products in any single BL",
    )


class BLAllocationRequest(BaseSchema):
    """Request to generate BL allocation."""

    num_bls: int = Field(..., ge=1, le=5, description="Number of BLs to allocate across")
    boat_id: Optional[str] = Field(
        None, description="Boat UUID (uses current boat if not specified)"
    )
    products: Optional[List[dict]] = Field(
        None,
        description="Products to allocate [{sku, pallets}]. Uses current selection if not specified.",
    )


class BLAllocationResponse(BaseSchema):
    """Response from BL allocation endpoint."""

    allocation: BLAllocationReport = Field(..., description="The allocation report")
    download_url: Optional[str] = Field(
        None, description="URL to download Excel file (if generated)"
    )
