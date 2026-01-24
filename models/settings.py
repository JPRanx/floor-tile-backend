"""
Settings schemas for validation and serialization.

Settings are key-value pairs stored in the database.
Pre-seeded with 22 business parameters.
"""

from pydantic import Field
from typing import Optional
from enum import Enum

from models.base import BaseSchema


class SettingCategory(str, Enum):
    """Setting categories for grouping."""
    LEAD_TIME = "lead_time"
    WAREHOUSE = "warehouse"
    CONTAINER = "container"
    THRESHOLDS = "thresholds"
    GENERAL = "general"


class SettingUpdate(BaseSchema):
    """
    Update existing setting.

    Only value can be updated - key is immutable.
    """

    value: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Setting value"
    )


class SettingResponse(BaseSchema):
    """
    Setting response with all fields.

    Used for GET responses.
    """

    id: str = Field(..., description="Setting UUID")
    key: str = Field(..., description="Setting key (unique)")
    value: str = Field(..., description="Setting value")
    description: Optional[str] = Field(None, description="Human-readable description")
    category: Optional[str] = Field(None, description="Setting category for grouping")


class SettingListResponse(BaseSchema):
    """List of settings."""

    data: list[SettingResponse]
    total: int
