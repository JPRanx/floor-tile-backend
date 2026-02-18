"""
Boat factory draft schemas for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import Field
from typing import Optional
from enum import Enum

from models.base import BaseSchema, TimestampMixin


class DraftStatus(str, Enum):
    """Status lifecycle for a boat-factory draft."""
    DRAFTING = "drafting"
    ACTION_NEEDED = "action_needed"
    ORDERED = "ordered"
    CONFIRMED = "confirmed"


class DraftItemCreate(BaseSchema):
    """
    Create or update a draft item.

    Used inside DraftSave to specify product selections.
    """

    product_id: str = Field(
        ...,
        min_length=1,
        description="Product UUID"
    )
    selected_pallets: int = Field(
        ...,
        ge=0,
        description="Number of pallets selected for this product"
    )
    bl_number: Optional[int] = Field(
        None,
        ge=1,
        description="BL number this product is assigned to (null = not yet allocated)"
    )
    notes: Optional[str] = Field(
        None,
        description="Optional notes for this line item"
    )


class DraftItemResponse(BaseSchema, TimestampMixin):
    """
    Full draft item response.

    Used nested inside DraftResponse.
    """

    id: str = Field(..., description="Draft item UUID")
    draft_id: str = Field(..., description="Parent draft UUID")
    product_id: str = Field(..., description="Product UUID")
    selected_pallets: int = Field(..., description="Number of pallets selected")
    bl_number: Optional[int] = Field(None, description="BL number assignment")
    notes: Optional[str] = Field(None, description="Optional notes for this line item")


class DraftSave(BaseSchema):
    """
    Create or upsert a boat-factory draft.

    The main input model for saving draft selections.
    Items list can be empty (draft with no product selections yet).
    """

    boat_id: str = Field(
        ...,
        min_length=1,
        description="Boat UUID"
    )
    factory_id: str = Field(
        ...,
        min_length=1,
        description="Factory UUID"
    )
    notes: Optional[str] = Field(
        None,
        description="Optional notes for the draft"
    )
    items: list[DraftItemCreate] = Field(
        ...,
        description="Product selections for this draft"
    )


class DraftStatusUpdate(BaseSchema):
    """
    Update the status of a draft.

    Used for status transitions (e.g. drafting -> ordered).
    """

    status: DraftStatus = Field(
        ...,
        description="New draft status"
    )


class DraftResponse(BaseSchema, TimestampMixin):
    """
    Full draft response with nested items.

    Used for GET responses.
    """

    id: str = Field(..., description="Draft UUID")
    boat_id: str = Field(..., description="Boat UUID")
    factory_id: str = Field(..., description="Factory UUID")
    status: DraftStatus = Field(..., description="Current draft status")
    notes: Optional[str] = Field(None, description="Optional notes for the draft")
    last_edited_at: Optional[str] = Field(None, description="Last time the draft was edited")
    ordered_at: Optional[str] = Field(None, description="When the draft was marked as ordered")
    items: list[DraftItemResponse] = Field(..., description="Product selections in this draft")
