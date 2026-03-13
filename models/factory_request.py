"""
Response models for Factory Request Builder.

Factory requests are production orders — not boat assignments.
Products are aggregated across the full FS horizon.
"""

from decimal import Decimal
from typing import Optional

from models.base import BaseSchema


class FactoryRequestProduct(BaseSchema):
    """A product that needs factory production, aggregated across all boats."""
    product_id: str
    sku: str
    total_factory_need_pallets: int
    total_factory_need_m2: Decimal
    first_gap_boat: str
    first_gap_boat_id: str
    first_gap_departure: str
    ships_on_boat: Optional[str] = None
    ships_on_boat_id: Optional[str] = None
    ships_on_departure: Optional[str] = None
    estimated_ready_date: str
    daily_velocity_m2: Decimal
    days_of_stock_at_first_gap: int
    urgency: str  # overdue, order_now, upcoming
    trend_direction: str
    trend_adjustment_pct: Decimal


class UpcomingBoat(BaseSchema):
    """Boat in the horizon with factory-production eligibility."""
    boat_name: str
    departure_date: str
    arrival_date: str
    days_until_departure: int
    is_estimated: bool
    can_receive_production: bool


class FactoryRequestSummary(BaseSchema):
    total_products: int
    total_pallets: int
    total_m2: Decimal
    total_containers: int
    overdue_count: int
    order_now_count: int


class FactoryRequestHorizonResponse(BaseSchema):
    factory_id: str
    factory_name: str
    production_lead_days: int
    transport_to_port_days: int
    monthly_quota_m2: Decimal
    estimated_ready_date: str
    products: list[FactoryRequestProduct]
    upcoming_boats: list[UpcomingBoat]
    factory_order_signal: Optional[dict] = None
    summary: FactoryRequestSummary
    generated_at: str