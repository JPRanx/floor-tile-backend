"""
Factory Request Submission tracking schemas.

Records when Ashley exports a factory production request (Excel).
Simple tracking: who requested what, when, for which factory.
No submission workflow — just a ledger entry.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from models.base import BaseSchema


class FactoryRequestSubmissionItem(BaseSchema):
    """One product line in a factory request submission."""
    product_id: str
    sku: str
    pallets: int = Field(..., ge=0)
    m2: Decimal = Field(..., ge=0)
    urgency: str


class FactoryRequestSubmissionCreate(BaseSchema):
    """Create a factory request submission record."""
    factory_id: str
    factory_name: str
    items: List[FactoryRequestSubmissionItem]
    total_pallets: int = Field(..., ge=0)
    total_m2: Decimal = Field(..., ge=0)
    total_containers: int = Field(..., ge=0)
    notes: Optional[str] = None


class FactoryRequestSubmissionResponse(BaseSchema):
    """Response after recording a factory request submission."""
    id: str
    factory_id: str
    factory_name: str
    total_pallets: int
    total_m2: Decimal
    total_containers: int
    product_count: int
    submitted_at: datetime
    notes: Optional[str] = None


class FactoryRequestLastSubmission(BaseSchema):
    """Summary of the most recent submission for a factory — shown as banner."""
    id: str
    submitted_at: datetime
    total_pallets: int
    total_m2: Decimal
    total_containers: int
    product_count: int
    days_ago: int
