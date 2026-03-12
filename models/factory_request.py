"""
Response models for Factory Request Horizon endpoint.
"""

from decimal import Decimal
from typing import Optional

from models.base import BaseSchema


class FactoryRequestCycleItem(BaseSchema):
    product_id: str
    sku: str
    description: Optional[str] = None
    gap_m2: Decimal
    gap_pallets: int
    request_m2: Decimal
    request_pallets: int
    velocity_m2_day: Decimal
    coverage_days: int
    estimated_ready_date: Optional[str] = None
    target_boat: Optional[str] = None
    target_boat_departure: Optional[str] = None
    urgency: str  # critical, urgent, soon, ok
    should_request: bool
    is_low_volume: bool
    low_volume_reason: Optional[str] = None


class FactoryRequestCycle(BaseSchema):
    month: str                          # "2026-03"
    month_display: str                  # "Marzo 2026"
    product_count: int
    total_m2: Decimal
    total_pallets: int
    capacity_limit_m2: Decimal
    capacity_used_m2: Decimal
    capacity_remaining_m2: Decimal
    utilization_pct: Decimal
    deadline: Optional[str] = None
    days_until_deadline: Optional[int] = None
    signal_type: str
    target_boats: list[str]
    items: list[FactoryRequestCycleItem]


class FactoryRequestHorizonResponse(BaseSchema):
    factory_id: str
    factory_name: str
    cycles: list[FactoryRequestCycle]
    generated_at: str
