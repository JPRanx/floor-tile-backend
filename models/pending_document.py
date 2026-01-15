"""
Pending document models for unmatched document queue.

Documents that couldn't be auto-matched to shipments are stored here
for manual resolution via the dashboard.
"""

from typing import Optional, Literal
from datetime import datetime
from enum import Enum
from pydantic import Field

from models.base import BaseSchema, TimestampMixin
from models.ingest import ParsedDocumentData


class PendingStatus(str, Enum):
    """Status of pending document."""
    PENDING = "pending"
    RESOLVED = "resolved"
    EXPIRED = "expired"


class ResolvedAction(str, Enum):
    """Action taken to resolve a pending document."""
    ASSIGNED = "assigned"   # Assigned to existing shipment
    CREATED = "created"     # Created new shipment
    DISCARDED = "discarded" # Marked as not needed


class PendingDocumentCreate(BaseSchema):
    """
    Create a new pending document record.

    Used when email ingestion can't auto-match a document.
    """

    # Document info
    document_type: Literal["booking", "departure", "arrival", "hbl", "mbl", "unknown"]
    parsed_data: dict = Field(description="Full ParsedDocumentData as dict")
    pdf_storage_path: str = Field(description="Path in Supabase Storage bucket")

    # Source info
    source: Literal["email", "manual"] = "email"
    email_subject: Optional[str] = None
    email_from: Optional[str] = None

    # Matching context (what we tried)
    attempted_booking: Optional[str] = None
    attempted_shp: Optional[str] = None
    attempted_containers: list[str] = Field(default_factory=list)


class PendingDocumentResponse(BaseSchema, TimestampMixin):
    """
    Pending document response for dashboard listing.

    Includes all info needed to display and resolve the pending doc.
    """

    id: str = Field(description="Pending document UUID")

    # Document info
    document_type: str
    parsed_data: dict = Field(description="Full ParsedDocumentData as dict")
    pdf_storage_path: str

    # Source info
    source: str
    email_subject: Optional[str] = None
    email_from: Optional[str] = None

    # Matching context
    attempted_booking: Optional[str] = None
    attempted_shp: Optional[str] = None
    attempted_containers: list[str] = Field(default_factory=list)

    # Resolution state
    status: PendingStatus = PendingStatus.PENDING
    resolved_at: Optional[datetime] = None
    resolved_shipment_id: Optional[str] = None
    resolved_action: Optional[ResolvedAction] = None

    # Expiration
    expires_at: datetime


class ResolvePendingRequest(BaseSchema):
    """
    Request to resolve a pending document.

    User chooses one of:
    - Assign to existing shipment (target_shipment_id)
    - Create new shipment (action="create")
    - Discard (action="discard")
    """

    action: Literal["assign", "create", "discard"]

    # For "assign" action
    target_shipment_id: Optional[str] = Field(
        None,
        description="UUID of shipment to assign to (required for action='assign')"
    )

    # Optional overrides when creating/assigning
    booking_number: Optional[str] = None
    shp_number: Optional[str] = None


class PendingDocumentListResponse(BaseSchema):
    """List of pending documents with pagination."""

    data: list[PendingDocumentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
