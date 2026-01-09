"""
Document ingestion models.

Handles data structures for parsing and ingesting shipment documents from various sources.
"""

from typing import Optional, Literal
from datetime import date
from pydantic import BaseModel, Field


class ParsedFieldConfidence(BaseModel):
    """Confidence score for a parsed field."""

    value: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score 0-1")
    source_text: Optional[str] = Field(None, description="Original text that was parsed")


class ParsedDocumentData(BaseModel):
    """
    Data extracted from a shipment document.

    All fields are optional since parsing may not find everything.
    Each field includes confidence score.
    """

    # Document metadata
    document_type: Literal["booking", "departure", "arrival", "hbl", "mbl", "unknown"] = "unknown"
    document_type_confidence: float = Field(ge=0.0, le=1.0)

    # Core identifiers
    shp_number: Optional[ParsedFieldConfidence] = None
    booking_number: Optional[ParsedFieldConfidence] = None
    pv_number: Optional[ParsedFieldConfidence] = None

    # Container information
    containers: list[str] = Field(default_factory=list)
    containers_confidence: float = Field(ge=0.0, le=1.0, default=0.0)

    # Dates
    etd: Optional[ParsedFieldConfidence] = None  # Estimated Time Departure
    eta: Optional[ParsedFieldConfidence] = None  # Estimated Time Arrival
    atd: Optional[ParsedFieldConfidence] = None  # Actual Time Departure
    ata: Optional[ParsedFieldConfidence] = None  # Actual Time Arrival

    # Ports
    pol: Optional[ParsedFieldConfidence] = None  # Port of Loading
    pod: Optional[ParsedFieldConfidence] = None  # Port of Discharge

    # Vessel
    vessel: Optional[ParsedFieldConfidence] = None

    # Raw data
    raw_text: str = Field(description="Complete extracted text from document")

    # Overall confidence
    overall_confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="Overall parsing confidence (average of found fields)"
    )


class PDFIngestRequest(BaseModel):
    """Request to ingest a PDF document (file upload handled separately)."""

    filename: str = Field(description="Original filename of uploaded PDF")
    source: Literal["pdf_upload", "email_forward"] = "pdf_upload"


class ConfirmIngestRequest(BaseModel):
    """
    User-confirmed data after reviewing parsed document.

    This is what creates or updates the actual shipment.
    """

    # Required: Must have at least one identifier
    shp_number: Optional[str] = None
    booking_number: Optional[str] = None

    # Document info
    document_type: Literal["booking", "departure", "arrival", "hbl", "mbl"]

    # Optional fields user can confirm/correct
    containers: list[str] = Field(default_factory=list)
    etd: Optional[date] = None
    eta: Optional[date] = None
    atd: Optional[date] = None
    ata: Optional[date] = None
    pol: Optional[str] = None
    pod: Optional[str] = None
    vessel: Optional[str] = None

    # Metadata
    source: Literal["pdf_upload", "email_forward", "manual"] = "pdf_upload"
    notes: Optional[str] = Field(None, description="User notes about this document")

    # Original parsed data (for audit trail)
    original_parsed_data: Optional[ParsedDocumentData] = None


class StructuredIngestRequest(BaseModel):
    """
    Future: Clean structured data from API/webhook.

    No parsing needed, direct ingestion.
    """

    # Required identifiers
    shp_number: Optional[str] = None
    booking_number: Optional[str] = None

    # Document info
    document_type: Literal["booking", "departure", "arrival", "hbl", "mbl"]

    # Shipment data
    containers: list[str] = Field(default_factory=list)
    etd: Optional[date] = None
    eta: Optional[date] = None
    atd: Optional[date] = None
    ata: Optional[date] = None
    pol: Optional[str] = None
    pod: Optional[str] = None
    vessel: Optional[str] = None

    # Source metadata
    source: Literal["api_webhook", "partner_api"] = "api_webhook"
    source_system: Optional[str] = Field(None, description="Name of source system (e.g., 'TIBA API')")
    external_id: Optional[str] = Field(None, description="ID in source system")


class IngestResponse(BaseModel):
    """Response after document ingestion."""

    success: bool
    message: str
    shipment_id: Optional[str] = None
    shp_number: Optional[str] = None
    action: Literal["created", "updated", "parsed_pending_confirmation"] = "parsed_pending_confirmation"
    parsed_data: Optional[ParsedDocumentData] = None