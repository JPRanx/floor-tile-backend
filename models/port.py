"""
Port models.

Represents shipping ports (origin and destination).
"""

from typing import Optional, Literal
from pydantic import Field

from models.base import BaseSchema, TimestampMixin


class PortType(str):
    """Port type enum."""
    ORIGIN = "ORIGIN"
    DESTINATION = "DESTINATION"


class PortCreate(BaseSchema):
    """Create a new port."""

    name: str = Field(..., max_length=200, description="Port name")
    country: str = Field(..., max_length=100, description="Country")
    type: Literal["ORIGIN", "DESTINATION"] = Field(..., description="Port type")
    unlocode: Optional[str] = Field(None, max_length=10, description="UN/LOCODE")
    avg_processing_days: Optional[float] = Field(None, ge=0, description="Average processing days")


class PortResponse(BaseSchema, TimestampMixin):
    """Port response with all fields."""

    id: str = Field(..., description="Port UUID")
    name: str = Field(..., description="Port name")
    country: str = Field(..., description="Country")
    type: Literal["ORIGIN", "DESTINATION"] = Field(..., description="Port type")
    unlocode: Optional[str] = Field(None, description="UN/LOCODE")
    avg_processing_days: Optional[float] = Field(None, description="Average processing days")