"""
Boat schedule schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from enum import Enum
from datetime import date, datetime, timedelta

from models.base import BaseSchema, TimestampMixin


class BoatStatus(str, Enum):
    """Boat schedule status."""
    AVAILABLE = "available"
    BOOKED = "booked"
    DEPARTED = "departed"
    ARRIVED = "arrived"


class RouteType(str, Enum):
    """Route type for boat schedules."""
    DIRECT = "direct"
    WITH_STOPS = "with_stops"


# Constants
# Order deadline: 20 days before departure to finalize order
# (allows time for factory confirmation, logistics, etc.)
# This is the SOFT deadline - displayed to users as "Order by: [date]"
ORDER_DEADLINE_DAYS = 20

# Hard deadline: 10 days before departure - boat visibility cutoff
# After this point, boat is hidden from Order Builder and next boat is shown
# Provides 10-day grace period after order deadline for late orders
HARD_DEADLINE_DAYS = 10

# Booking buffer: 3 days before departure for final booking
BOOKING_BUFFER_DAYS = 3  # Days before departure to book


class BoatScheduleCreate(BaseSchema):
    """
    Create a new boat schedule.

    Required: departure_date, arrival_date, transit_days
    Optional: vessel_name, shipping_line, origin_port, destination_port, route_type
    """

    vessel_name: Optional[str] = Field(
        None,
        max_length=100,
        description="Name of the vessel"
    )
    shipping_line: Optional[str] = Field(
        None,
        max_length=100,
        description="Shipping company (e.g., CMA CGM)"
    )
    departure_date: date = Field(
        ...,
        description="Departure date from origin port"
    )
    arrival_date: date = Field(
        ...,
        description="Arrival date at destination port"
    )
    transit_days: int = Field(
        ...,
        gt=0,
        description="Number of days in transit"
    )
    origin_port: str = Field(
        "Cartagena",
        max_length=100,
        description="Origin port name"
    )
    destination_port: str = Field(
        "Puerto Quetzal",
        max_length=100,
        description="Destination port name"
    )
    route_type: Optional[RouteType] = Field(
        None,
        description="Route type: direct or with_stops"
    )
    source_file: Optional[str] = Field(
        None,
        max_length=255,
        description="Original filename from upload"
    )

    @model_validator(mode='after')
    def validate_dates(self):
        """Ensure arrival is after departure and transit days match."""
        if self.arrival_date <= self.departure_date:
            raise ValueError("Arrival date must be after departure date")

        calculated_days = (self.arrival_date - self.departure_date).days
        if calculated_days != self.transit_days:
            # Allow small discrepancies, but warn if major
            if abs(calculated_days - self.transit_days) > 2:
                raise ValueError(
                    f"Transit days ({self.transit_days}) doesn't match "
                    f"date difference ({calculated_days} days)"
                )
        return self

    @property
    def order_deadline(self) -> date:
        """Calculate order deadline (departure - 20 days)."""
        return self.departure_date - timedelta(days=ORDER_DEADLINE_DAYS)

    @property
    def booking_deadline(self) -> date:
        """Calculate booking deadline (departure - 3 days)."""
        return self.departure_date - timedelta(days=BOOKING_BUFFER_DAYS)


class BoatScheduleUpdate(BaseSchema):
    """
    Update existing boat schedule.

    All fields optional - only provided fields are updated.
    """

    vessel_name: Optional[str] = Field(
        None,
        max_length=100,
        description="Name of the vessel"
    )
    shipping_line: Optional[str] = Field(
        None,
        max_length=100,
        description="Shipping company"
    )
    departure_date: Optional[date] = Field(
        None,
        description="Departure date"
    )
    arrival_date: Optional[date] = Field(
        None,
        description="Arrival date"
    )
    transit_days: Optional[int] = Field(
        None,
        gt=0,
        description="Days in transit"
    )
    origin_port: Optional[str] = Field(
        None,
        max_length=100,
        description="Origin port"
    )
    destination_port: Optional[str] = Field(
        None,
        max_length=100,
        description="Destination port"
    )
    route_type: Optional[RouteType] = Field(
        None,
        description="Route type"
    )
    status: Optional[BoatStatus] = Field(
        None,
        description="Schedule status"
    )


class BoatScheduleStatusUpdate(BaseSchema):
    """Update only the status of a boat schedule."""

    status: BoatStatus = Field(
        ...,
        description="New status for the boat schedule"
    )


class BoatScheduleResponse(BaseSchema, TimestampMixin):
    """
    Boat schedule response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Boat schedule UUID")
    vessel_name: Optional[str] = Field(None, description="Vessel name")
    shipping_line: Optional[str] = Field(None, description="Shipping company")
    departure_date: date = Field(..., description="Departure date")
    arrival_date: date = Field(..., description="Arrival date")
    transit_days: int = Field(..., description="Days in transit")
    origin_port: str = Field(..., description="Origin port")
    destination_port: str = Field(..., description="Destination port")
    route_type: Optional[RouteType] = Field(None, description="Route type")
    order_deadline: date = Field(..., description="Recommended order deadline (20 days before departure)")
    booking_deadline: date = Field(..., description="Last date to book cargo (3 days before departure)")
    status: BoatStatus = Field(..., description="Current status")
    source_file: Optional[str] = Field(None, description="Source filename")

    # Computed fields for UI
    days_until_departure: Optional[int] = Field(None, description="Days until departure")
    days_until_order_deadline: Optional[int] = Field(None, description="Days until order deadline (can be negative)")
    days_until_deadline: Optional[int] = Field(None, description="Days until booking deadline")
    past_order_deadline: bool = Field(default=False, description="True if past recommended order deadline")

    @classmethod
    def from_db(cls, row: dict, today: Optional[date] = None) -> "BoatScheduleResponse":
        """Create response from database row with computed fields."""
        if today is None:
            today = date.today()

        departure = cls._parse_date(row["departure_date"])
        deadline = cls._parse_date(row["booking_deadline"])
        arrival = cls._parse_date(row["arrival_date"])

        # Calculate order deadline (20 days before departure)
        order_deadline = departure - timedelta(days=ORDER_DEADLINE_DAYS)

        days_until_departure = (departure - today).days if departure >= today else None
        days_until_order_deadline = (order_deadline - today).days  # Can be negative
        days_until_deadline = (deadline - today).days if deadline >= today else None
        past_order_deadline = today > order_deadline

        return cls(
            id=str(row["id"]),
            vessel_name=row.get("vessel_name"),
            shipping_line=row.get("shipping_line"),
            departure_date=departure,
            arrival_date=arrival,
            transit_days=row["transit_days"],
            origin_port=row["origin_port"],
            destination_port=row["destination_port"],
            route_type=row.get("route_type"),
            order_deadline=order_deadline,
            booking_deadline=deadline,
            status=row["status"],
            source_file=row.get("source_file"),
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
            days_until_departure=days_until_departure,
            days_until_order_deadline=days_until_order_deadline,
            days_until_deadline=days_until_deadline,
            past_order_deadline=past_order_deadline
        )

    @staticmethod
    def _parse_date(value) -> date:
        """Parse date from various formats."""
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            # Try ISO format first
            try:
                return date.fromisoformat(value)
            except ValueError:
                pass
            # Try datetime parsing
            try:
                return datetime.fromisoformat(value).date()
            except ValueError:
                pass
        raise ValueError(f"Cannot parse date: {value}")


class BoatScheduleListResponse(BaseSchema):
    """List of boat schedules with pagination."""

    data: list[BoatScheduleResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class BoatUploadResult(BaseSchema):
    """Result of uploading a TIBA Excel file."""

    imported: int = Field(..., description="Number of new schedules imported")
    updated: int = Field(..., description="Number of existing schedules updated")
    skipped: int = Field(0, description="Number of rows skipped")
    errors: list[str] = Field(default_factory=list, description="Error messages")
