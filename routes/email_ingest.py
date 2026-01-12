"""
Email ingestion routes.

Handles processing emails forwarded from Power Automate.
"""

import base64
from datetime import date
from typing import Optional
import structlog

from fastapi import APIRouter, HTTPException

from models.ingest import (
    EmailIngestRequest,
    EmailIngestResponse,
    ConfirmIngestRequest,
    ParsedDocumentData,
)
from services.claude_parser_service import get_claude_parser_service, CLAUDE_AVAILABLE
from services.shipment_service import get_shipment_service
from integrations.telegram import send_message

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/shipments/ingest", tags=["Email Ingestion"])

# Confidence threshold for auto-confirmation
AUTO_CONFIRM_THRESHOLD = 0.90


def _extract_pdf_from_attachments(attachments: list) -> tuple[Optional[bytes], Optional[str]]:
    """
    Extract the first PDF attachment.

    Returns:
        tuple: (pdf_bytes, filename) or (None, None) if no PDF found
    """
    for attachment in attachments:
        if attachment.content_type == "application/pdf" or attachment.filename.lower().endswith(".pdf"):
            try:
                pdf_bytes = base64.b64decode(attachment.content_base64)
                return pdf_bytes, attachment.filename
            except Exception as e:
                logger.warning("attachment_decode_failed", filename=attachment.filename, error=str(e))
                continue

    return None, None


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


def _send_success_telegram(
    sender: str,
    document_type: str,
    booking_number: Optional[str],
    action: str,
    confidence: float
):
    """Send Telegram notification for successful email processing."""
    message = f"""✅ *Email processed*

From: `{sender}`
Document: {document_type.upper()}
Booking: `{booking_number or 'N/A'}`
Action: {action}
Confidence: {confidence:.0%}"""

    try:
        send_message(message)
    except Exception as e:
        logger.warning("telegram_notification_failed", error=str(e))


def _send_needs_review_telegram(
    sender: str,
    subject: str,
    reason: str,
    parsed_data: Optional[ParsedDocumentData] = None
):
    """Send Telegram notification when email needs manual review."""
    details = ""
    if parsed_data:
        details = f"""
Document type: {parsed_data.document_type}
Booking: `{_parsed_field_to_value(parsed_data.booking_number) or 'N/A'}`
Containers: {len(parsed_data.containers)}
Confidence: {parsed_data.overall_confidence:.0%}"""

    message = f"""⚠️ *Email needs review*

From: `{sender}`
Subject: {subject}

Reason: {reason}
{details}

Please review manually in the system."""

    try:
        send_message(message)
    except Exception as e:
        logger.warning("telegram_notification_failed", error=str(e))


def _send_error_telegram(sender: str, subject: str, error: str):
    """Send Telegram notification for processing errors."""
    message = f"""❌ *Email processing failed*

From: `{sender}`
Subject: {subject}

Error: {error}

Please check the email manually."""

    try:
        send_message(message)
    except Exception as e:
        logger.warning("telegram_notification_failed", error=str(e))


@router.post("/email", response_model=EmailIngestResponse)
async def ingest_email(data: EmailIngestRequest) -> EmailIngestResponse:
    """
    Process email forwarded from Power Automate.

    Extracts PDF from attachments, parses with Claude Vision using email
    context, and auto-confirms if confidence is high enough.

    Args:
        data: Email payload from Power Automate

    Returns:
        EmailIngestResponse with processing result
    """
    logger.info(
        "email_ingest_started",
        sender=data.sender,
        subject=data.subject,
        attachment_count=len(data.attachments)
    )

    # Check if Claude is available
    if not CLAUDE_AVAILABLE:
        _send_error_telegram(data.sender, data.subject, "Claude Vision not configured")
        raise HTTPException(
            status_code=503,
            detail="Claude Vision not configured. Set ANTHROPIC_API_KEY."
        )

    # Extract PDF attachment
    pdf_bytes, filename = _extract_pdf_from_attachments(data.attachments)

    if not pdf_bytes:
        logger.warning("no_pdf_attachment", sender=data.sender, subject=data.subject)
        _send_needs_review_telegram(
            data.sender,
            data.subject,
            "No PDF attachment found"
        )
        return EmailIngestResponse(
            success=False,
            action="needs_review",
            message="No PDF attachment found in email",
            confidence=0.0
        )

    # Build email context for Claude
    email_context = f"""From: {data.sender}
Subject: {data.subject}

{data.body[:2000]}"""  # Limit body to 2000 chars

    # Parse with Claude Vision
    try:
        claude_parser = get_claude_parser_service()
        parsed_data = await claude_parser.parse_pdf(pdf_bytes, email_context=email_context)

        logger.info(
            "email_pdf_parsed",
            document_type=parsed_data.document_type,
            overall_confidence=parsed_data.overall_confidence,
            booking=_parsed_field_to_value(parsed_data.booking_number),
            containers_count=len(parsed_data.containers)
        )

    except Exception as e:
        logger.error("email_parsing_failed", error=str(e))
        _send_error_telegram(data.sender, data.subject, f"PDF parsing failed: {str(e)}")
        return EmailIngestResponse(
            success=False,
            action="error",
            message=f"Failed to parse PDF: {str(e)}",
            confidence=0.0
        )

    # Try to auto-match to existing shipment
    shipment_service = get_shipment_service()
    existing_shipment = None
    matched_by = None

    # Try booking number first
    booking_number = _parsed_field_to_value(parsed_data.booking_number)
    if booking_number:
        existing_shipment = shipment_service.get_by_booking_number(booking_number)
        if existing_shipment:
            matched_by = "booking"

    # Try SHP number
    if not existing_shipment:
        shp_number = _parsed_field_to_value(parsed_data.shp_number)
        if shp_number:
            existing_shipment = shipment_service.get_by_shp_number(shp_number)
            if existing_shipment:
                matched_by = "shp"

    # Try containers
    if not existing_shipment and parsed_data.containers:
        existing_shipment = shipment_service.get_by_container_numbers(parsed_data.containers)
        if existing_shipment:
            matched_by = "container"

    # Check confidence and decide action
    confidence = parsed_data.overall_confidence
    can_auto_confirm = confidence >= AUTO_CONFIRM_THRESHOLD

    # If we can't match and it's HBL/MBL, need review
    if parsed_data.document_type in ["hbl", "mbl"] and not existing_shipment:
        logger.info(
            "email_needs_manual_match",
            document_type=parsed_data.document_type,
            booking=booking_number
        )
        _send_needs_review_telegram(
            data.sender,
            data.subject,
            f"No matching shipment found for {parsed_data.document_type.upper()}",
            parsed_data
        )
        return EmailIngestResponse(
            success=True,
            action="needs_review",
            message=f"Parsed {parsed_data.document_type.upper()} but no matching shipment found",
            document_type=parsed_data.document_type,
            booking_number=booking_number,
            confidence=confidence,
            parsed_fields={
                "booking": booking_number,
                "shp": _parsed_field_to_value(parsed_data.shp_number),
                "vessel": _parsed_field_to_value(parsed_data.vessel),
                "containers": parsed_data.containers[:5],  # First 5
            }
        )

    # Low confidence - needs review
    if not can_auto_confirm:
        logger.info(
            "email_low_confidence",
            confidence=confidence,
            threshold=AUTO_CONFIRM_THRESHOLD
        )
        _send_needs_review_telegram(
            data.sender,
            data.subject,
            f"Low confidence ({confidence:.0%} < {AUTO_CONFIRM_THRESHOLD:.0%})",
            parsed_data
        )
        return EmailIngestResponse(
            success=True,
            action="needs_review",
            message=f"Confidence too low for auto-confirm ({confidence:.0%})",
            document_type=parsed_data.document_type,
            booking_number=booking_number,
            confidence=confidence,
            parsed_fields={
                "booking": booking_number,
                "shp": _parsed_field_to_value(parsed_data.shp_number),
                "vessel": _parsed_field_to_value(parsed_data.vessel),
                "containers": parsed_data.containers[:5],
            }
        )

    # High confidence - auto-confirm
    # Import here to avoid circular import
    from routes.ingest import confirm_ingest

    confirm_request = ConfirmIngestRequest(
        shp_number=_parsed_field_to_value(parsed_data.shp_number),
        booking_number=booking_number,
        document_type=parsed_data.document_type,
        containers=parsed_data.containers,
        etd=_parsed_date_to_date(parsed_data.etd),
        eta=_parsed_date_to_date(parsed_data.eta),
        atd=_parsed_date_to_date(parsed_data.atd),
        ata=_parsed_date_to_date(parsed_data.ata),
        pol=_parsed_field_to_value(parsed_data.pol),
        pod=_parsed_field_to_value(parsed_data.pod),
        vessel=_parsed_field_to_value(parsed_data.vessel),
        voyage=_parsed_field_to_value(parsed_data.voyage),
        source="email_forward",
        notes=f"Auto-ingested from email: {data.subject}",
        original_parsed_data=parsed_data,
        target_shipment_id=existing_shipment.id if existing_shipment else None,
    )

    try:
        result = await confirm_ingest(confirm_request)

        if result.success:
            action = "updated_shipment" if existing_shipment else "created_shipment"
            action_text = "Updated existing shipment" if existing_shipment else "Created new shipment"

            logger.info(
                "email_auto_confirmed",
                action=action,
                shipment_id=result.shipment_id,
                booking=booking_number,
                matched_by=matched_by
            )

            _send_success_telegram(
                data.sender,
                parsed_data.document_type,
                booking_number,
                action_text,
                confidence
            )

            return EmailIngestResponse(
                success=True,
                action=action,
                message=f"{action_text} from email",
                shipment_id=result.shipment_id,
                booking_number=result.shp_number or booking_number,
                document_type=parsed_data.document_type,
                confidence=confidence,
                parsed_fields={
                    "booking": booking_number,
                    "shp": _parsed_field_to_value(parsed_data.shp_number),
                    "vessel": _parsed_field_to_value(parsed_data.vessel),
                    "containers_count": len(parsed_data.containers),
                }
            )
        else:
            # Confirm failed - needs review
            _send_needs_review_telegram(
                data.sender,
                data.subject,
                f"Auto-confirm failed: {result.message}",
                parsed_data
            )
            return EmailIngestResponse(
                success=False,
                action="needs_review",
                message=f"Auto-confirm failed: {result.message}",
                document_type=parsed_data.document_type,
                booking_number=booking_number,
                confidence=confidence
            )

    except Exception as e:
        logger.error("email_confirm_failed", error=str(e))
        _send_error_telegram(data.sender, data.subject, f"Confirm failed: {str(e)}")
        return EmailIngestResponse(
            success=False,
            action="error",
            message=f"Failed to confirm: {str(e)}",
            confidence=confidence
        )