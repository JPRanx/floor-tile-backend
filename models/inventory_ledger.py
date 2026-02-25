"""
Inventory Ledger schemas for validation and serialization.

Models for the event-sourced inventory ledger system:
- LedgerEventResponse: Individual inventory events (deltas and reconciliations)
- ProjectedStateResponse: Current projected state per product
- ReconciliationReportResponse: Reconciliation audit reports

See STANDARDS_VALIDATION.md for patterns.
"""

from enum import Enum
from decimal import Decimal
from datetime import date, datetime
from typing import Optional

from pydantic import Field

from models.base import BaseSchema


# ===================
# ENUMS
# ===================

class LedgerEventType(str, Enum):
    """Types of inventory ledger events."""
    SALE_RECORDED = "SALE_RECORDED"
    WAREHOUSE_RECONCILED = "WAREHOUSE_RECONCILED"
    FACTORY_RECONCILED = "FACTORY_RECONCILED"
    TRANSIT_RECONCILED = "TRANSIT_RECONCILED"
    PRODUCTION_RECONCILED = "PRODUCTION_RECONCILED"
    WAREHOUSE_ORDER_EXPORTED = "WAREHOUSE_ORDER_EXPORTED"
    PIGGYBACK_CONFIRMED = "PIGGYBACK_CONFIRMED"
    FACTORY_ORDER_EXPORTED = "FACTORY_ORDER_EXPORTED"
    MANUAL_ADJUSTMENT = "MANUAL_ADJUSTMENT"


# ===================
# RESPONSE SCHEMAS
# ===================

class LedgerEventResponse(BaseSchema):
    """
    Single ledger event response.

    Each event records a delta or reconciliation for a product's inventory.
    """

    id: str = Field(..., description="Event UUID")
    event_type: LedgerEventType = Field(..., description="Type of inventory event")
    product_id: str = Field(..., description="Product UUID")
    delta_warehouse_m2: Decimal = Field(..., description="Change in warehouse m2")
    delta_factory_m2: Decimal = Field(..., description="Change in factory m2")
    delta_transit_m2: Decimal = Field(..., description="Change in transit m2")
    snapshot_value_m2: Optional[Decimal] = Field(None, description="Actual value for reconciliation events")
    projected_value_m2: Optional[Decimal] = Field(None, description="Projected value before reconciliation")
    discrepancy_m2: Optional[Decimal] = Field(None, description="Difference: snapshot - projected")
    source_type: str = Field(..., description="Source: upload, ui, system")
    source_id: Optional[str] = Field(None, description="Reference ID from source")
    source_filename: Optional[str] = Field(None, description="Upload filename if applicable")
    event_date: date = Field(..., description="Business date of the event")
    notes: Optional[str] = Field(None, description="Optional notes")
    created_at: datetime = Field(..., description="When the event was recorded")


class ProjectedStateResponse(BaseSchema):
    """
    Projected inventory state for a single product.

    Maintained by applying ledger events to reconciliation baselines.
    """

    product_id: str = Field(..., description="Product UUID")
    warehouse_m2: Decimal = Field(..., description="Projected warehouse inventory in m2")
    factory_m2: Decimal = Field(..., description="Projected factory inventory in m2")
    transit_m2: Decimal = Field(..., description="Projected in-transit inventory in m2")
    warehouse_reconciled_at: Optional[datetime] = Field(None, description="Last warehouse reconciliation timestamp")
    factory_reconciled_at: Optional[datetime] = Field(None, description="Last factory reconciliation timestamp")
    transit_reconciled_at: Optional[datetime] = Field(None, description="Last transit reconciliation timestamp")
    events_since_warehouse_recon: int = Field(0, description="Events since last warehouse reconciliation")
    events_since_factory_recon: int = Field(0, description="Events since last factory reconciliation")
    events_since_transit_recon: int = Field(0, description="Events since last transit reconciliation")
    last_event_at: Optional[datetime] = Field(None, description="Timestamp of last event")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")


class ReconciliationReportResponse(BaseSchema):
    """
    Reconciliation report summary.

    Generated when upload data is compared to projected state.
    """

    id: str = Field(..., description="Report UUID")
    reconciliation_type: str = Field(..., description="Type: warehouse, factory, transit, production")
    filename: Optional[str] = Field(None, description="Source filename")
    reconciliation_date: date = Field(..., description="Date of reconciliation")
    products_reconciled: int = Field(..., description="Total products reconciled")
    products_matched: int = Field(..., description="Products where projected matched actual")
    products_discrepant: int = Field(..., description="Products with discrepancies")
    total_projected_m2: Decimal = Field(..., description="Sum of projected values")
    total_actual_m2: Decimal = Field(..., description="Sum of actual values")
    total_discrepancy_m2: Decimal = Field(..., description="Sum of discrepancies")
    items: list = Field(..., description="Per-product reconciliation details (JSONB)")
    created_at: datetime = Field(..., description="When the report was created")


# ===================
# LIST RESPONSE SCHEMAS
# ===================

class LedgerEventListResponse(BaseSchema):
    """List of ledger events."""

    data: list[LedgerEventResponse]
    total: int


class ProjectedStateListResponse(BaseSchema):
    """List of projected inventory states."""

    data: list[ProjectedStateResponse]
    total: int


class ReconciliationReportListResponse(BaseSchema):
    """List of reconciliation reports."""

    data: list[ReconciliationReportResponse]
    total: int
