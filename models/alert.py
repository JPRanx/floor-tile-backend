"""
Alert models and schemas.

Alerts notify users about important events:
- Stockout warnings
- Booking deadlines
- Shipment updates
- Inventory issues
"""

from datetime import datetime
from typing import Optional
from enum import Enum
from pydantic import Field

from models.base import BaseSchema


class AlertType(str, Enum):
    """Alert type enumeration."""

    STOCKOUT_WARNING = "STOCKOUT_WARNING"
    LOW_STOCK = "LOW_STOCK"
    ORDER_OPPORTUNITY = "ORDER_OPPORTUNITY"
    SHIPMENT_DEPARTED = "SHIPMENT_DEPARTED"
    SHIPMENT_ARRIVED = "SHIPMENT_ARRIVED"
    FREE_DAYS_EXPIRING = "FREE_DAYS_EXPIRING"
    SHIPMENT_DELAYED = "SHIPMENT_DELAYED"
    CONTAINER_READY = "CONTAINER_READY"
    OVER_STOCKED = "OVER_STOCKED"


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    CRITICAL = "CRITICAL"  # Requires immediate action
    WARNING = "WARNING"    # Should be addressed soon
    INFO = "INFO"          # Informational only


class AlertCreate(BaseSchema):
    """Create a new alert."""

    type: AlertType = Field(..., description="Alert type")
    severity: AlertSeverity = Field(..., description="Alert severity")
    title: str = Field(..., min_length=1, max_length=200, description="Alert title")
    message: str = Field(..., min_length=1, max_length=1000, description="Alert message")
    product_id: Optional[str] = Field(None, description="Related product UUID")
    shipment_id: Optional[str] = Field(None, description="Related shipment UUID")


class AlertUpdate(BaseSchema):
    """Update an alert."""

    is_read: Optional[bool] = Field(None, description="Mark as read")


class AlertResponse(BaseSchema):
    """Alert response model."""

    id: str
    type: str
    severity: str
    title: str
    message: str
    product_id: Optional[str] = None
    shipment_id: Optional[str] = None
    is_read: bool
    is_sent: bool
    created_at: datetime

    # Optional enriched data (joined from related tables)
    product_sku: Optional[str] = None
    shipment_booking_number: Optional[str] = None


class AlertListResponse(BaseSchema):
    """Paginated list of alerts."""

    data: list[AlertResponse]
    total: int
    page: int
    page_size: int
    total_pages: int