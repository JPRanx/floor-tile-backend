"""
Ingestion service for unified document processing logic.

Consolidates matching and action-determination logic used by both
email ingestion and manual upload flows.
"""

from enum import Enum
from dataclasses import dataclass
from datetime import date
from typing import Optional, Literal
import structlog

from models.shipment import ShipmentResponse
from models.ingest import ParsedDocumentData, ConfirmIngestRequest
from services.shipment_service import get_shipment_service, ShipmentService
from exceptions import ShipmentNotFoundError

logger = structlog.get_logger(__name__)

# Type alias for matching methods
MatchMethod = Literal["target_id", "booking", "shp", "containers"]


class IngestAction(str, Enum):
    """Possible actions after document ingestion."""
    UPDATE = "update"           # Match found, update existing shipment
    CREATE = "create"           # No match, but doc type allows creation
    NEEDS_REVIEW = "needs_review"  # No match, needs human decision


@dataclass
class ActionDecision:
    """Result of determine_action() with context for logging/response."""
    action: IngestAction
    reason: str
    shipment: Optional[ShipmentResponse] = None
    matched_by: Optional[str] = None


class IngestionService:
    """
    Unified ingestion logic for document processing.

    Handles:
    - Shipment matching by various identifiers
    - Action determination based on document type and match status
    """

    # Document types that can create new shipments (genesis documents)
    GENESIS_DOC_TYPES = ["booking"]

    def __init__(self, shipment_service: ShipmentService):
        self.shipment_service = shipment_service

    def find_matching_shipment(
        self,
        booking_number: Optional[str] = None,
        shp_number: Optional[str] = None,
        container_numbers: Optional[list[str]] = None,
        target_shipment_id: Optional[str] = None,
        match_order: list[MatchMethod] = None
    ) -> tuple[Optional[ShipmentResponse], Optional[str]]:
        """
        Find existing shipment by identifiers in priority order.

        Args:
            booking_number: CMA CGM booking reference
            shp_number: TIBA shipment reference
            container_numbers: List of container numbers
            target_shipment_id: Direct shipment ID (for manual assignment)
            match_order: Priority order for matching attempts.
                         Default: ["target_id", "booking", "shp", "containers"]

        Returns:
            (shipment, matched_by) — e.g., (ShipmentResponse, "booking")
            (None, None) — if no match found
        """
        if match_order is None:
            match_order = ["target_id", "booking", "shp", "containers"]

        logger.debug(
            "finding_matching_shipment",
            booking=booking_number,
            shp=shp_number,
            containers_count=len(container_numbers) if container_numbers else 0,
            target_id=target_shipment_id,
            match_order=match_order
        )

        for method in match_order:
            shipment = None

            if method == "target_id" and target_shipment_id:
                try:
                    shipment = self.shipment_service.get_by_id(target_shipment_id)
                except ShipmentNotFoundError:
                    shipment = None

            elif method == "booking" and booking_number:
                shipment = self.shipment_service.get_by_booking_number(booking_number)

            elif method == "shp" and shp_number:
                shipment = self.shipment_service.get_by_shp_number(shp_number)

            elif method == "containers" and container_numbers:
                shipment = self.shipment_service.get_by_container_numbers(container_numbers)

            if shipment:
                logger.info(
                    "shipment_matched",
                    shipment_id=shipment.id,
                    matched_by=method,
                    shp_number=shipment.shp_number
                )
                return (shipment, method)

        logger.debug("no_shipment_match_found")
        return (None, None)

    def determine_action(
        self,
        document_type: str,
        booking_number: Optional[str] = None,
        shp_number: Optional[str] = None,
        container_numbers: Optional[list[str]] = None,
        target_shipment_id: Optional[str] = None,
        match_order: list[MatchMethod] = None
    ) -> ActionDecision:
        """
        Determine what action to take based on document type and matching.

        Business rules:
        - Any doc + match found → UPDATE existing shipment
        - Booking + no match + has identifier → CREATE new shipment
        - Booking + no match + no identifier → NEEDS_REVIEW
        - HBL/MBL + no match → NEEDS_REVIEW (should have booking first)
        - Unknown doc + no match → NEEDS_REVIEW (be conservative)

        Args:
            document_type: Type of document (booking, hbl, mbl, etc.)
            booking_number: CMA CGM booking reference
            shp_number: TIBA shipment reference
            container_numbers: List of container numbers
            target_shipment_id: Direct shipment ID (for manual assignment)
            match_order: Priority order for matching attempts

        Returns:
            ActionDecision with action, reason, and optional shipment
        """
        if match_order is None:
            match_order = ["target_id", "booking", "shp", "containers"]

        doc_type_lower = document_type.lower() if document_type else "unknown"

        logger.info(
            "determining_action",
            document_type=doc_type_lower,
            booking=booking_number,
            shp=shp_number,
            containers_count=len(container_numbers) if container_numbers else 0
        )

        # First, try to find a match
        shipment, matched_by = self.find_matching_shipment(
            booking_number=booking_number,
            shp_number=shp_number,
            container_numbers=container_numbers,
            target_shipment_id=target_shipment_id,
            match_order=match_order
        )

        # Match found → always update
        if shipment:
            logger.info(
                "action_determined",
                action="update",
                matched_by=matched_by,
                shipment_id=shipment.id
            )
            return ActionDecision(
                action=IngestAction.UPDATE,
                reason=f"Matched existing shipment by {matched_by}",
                shipment=shipment,
                matched_by=matched_by
            )

        # No match + genesis doc type + has identifier → create
        if doc_type_lower in self.GENESIS_DOC_TYPES:
            # Must have at least booking number to create
            if booking_number:
                logger.info(
                    "action_determined",
                    action="create",
                    document_type=doc_type_lower
                )
                return ActionDecision(
                    action=IngestAction.CREATE,
                    reason=f"New {doc_type_lower} - creating shipment"
                )
            else:
                # Booking without booking number is suspicious
                logger.warning(
                    "booking_without_booking_number",
                    document_type=doc_type_lower
                )
                return ActionDecision(
                    action=IngestAction.NEEDS_REVIEW,
                    reason=f"Booking document received but no booking number found"
                )

        # No match + non-genesis doc → needs review
        logger.info(
            "action_determined",
            action="needs_review",
            document_type=doc_type_lower,
            reason="no_matching_shipment"
        )
        return ActionDecision(
            action=IngestAction.NEEDS_REVIEW,
            reason=f"{doc_type_lower.upper()} received but no matching shipment found"
        )


# =============================================================================
# Unified ConfirmIngestRequest Builder
# =============================================================================

def _parsed_field_to_value(field) -> Optional[str]:
    """Extract value from ParsedFieldConfidence."""
    if field and hasattr(field, 'value'):
        return field.value
    return None


def _parsed_date_to_date(field) -> Optional[date]:
    """Convert ParsedFieldConfidence date string to date object."""
    value = _parsed_field_to_value(field)
    if value:
        try:
            # Claude returns YYYY-MM-DD format
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def build_confirm_request(
    parsed_data: ParsedDocumentData,
    target_shipment_id: Optional[str] = None,
    source: Literal["pdf_upload", "email_forward", "manual", "pending_resolution"] = "email_forward",
    notes: Optional[str] = None,
    shp_override: Optional[str] = None,
    booking_override: Optional[str] = None,
) -> ConfirmIngestRequest:
    """
    Single source of truth for building ConfirmIngestRequest from ParsedDocumentData.

    Used by:
    - email_ingest.py (auto-confirm flow)
    - pending_document_service.py (resolve assign/create actions)

    This ensures all fields are consistently extracted, especially:
    - original_parsed_data (critical for container_details)
    - All date fields (etd, eta, atd, ata)
    - All location fields (pol, pod, vessel, voyage)

    Args:
        parsed_data: ParsedDocumentData from parser
        target_shipment_id: Optional shipment ID for manual assignment
        source: Source of ingestion (email_forward, pending_resolution, etc.)
        notes: Optional notes to attach to the request
        shp_override: Override SHP number from user input
        booking_override: Override booking number from user input

    Returns:
        ConfirmIngestRequest ready for confirm_ingest()
    """
    return ConfirmIngestRequest(
        document_type=parsed_data.document_type,
        shp_number=shp_override or _parsed_field_to_value(parsed_data.shp_number),
        booking_number=booking_override or _parsed_field_to_value(parsed_data.booking_number),
        containers=list(parsed_data.containers) if parsed_data.containers else [],
        etd=_parsed_date_to_date(parsed_data.etd),
        eta=_parsed_date_to_date(parsed_data.eta),
        atd=_parsed_date_to_date(parsed_data.atd),
        ata=_parsed_date_to_date(parsed_data.ata),
        pol=_parsed_field_to_value(parsed_data.pol),
        pod=_parsed_field_to_value(parsed_data.pod),
        vessel=_parsed_field_to_value(parsed_data.vessel),
        voyage=_parsed_field_to_value(parsed_data.voyage),
        source=source,
        notes=notes,
        target_shipment_id=target_shipment_id,
        original_parsed_data=parsed_data,  # Critical for container_details!
    )


# =============================================================================
# Singleton
# =============================================================================

_ingestion_service: Optional[IngestionService] = None


def get_ingestion_service() -> IngestionService:
    """Get or create IngestionService instance."""
    global _ingestion_service
    if _ingestion_service is None:
        _ingestion_service = IngestionService(get_shipment_service())
    return _ingestion_service