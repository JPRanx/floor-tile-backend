"""
Email ingestion routes.

Handles processing emails forwarded from Power Automate or Make.com Mailhook.
"""

import base64
import json
from datetime import date
from typing import Optional
import structlog

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from models.ingest import (
    EmailIngestRequest,
    EmailIngestResponse,
    ConfirmIngestRequest,
    ParsedDocumentData,
)
from services.claude_parser_service import get_claude_parser_service, CLAUDE_AVAILABLE
from services.ingestion_service import get_ingestion_service, IngestAction
from integrations.telegram import send_message

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/shipments/ingest", tags=["Email Ingestion"])

# Low confidence warning threshold (for logging, not gating)
LOW_CONFIDENCE_WARNING = 0.75


def _normalize_attachment(att) -> dict | None:
    """
    Convert Make.com Mailhook attachment format to standard format.

    Handles:
    - JSON strings (Make.com sends attachments as stringified JSON)
    - Buffer format {"type": "Buffer", "data": [bytes...]} -> base64
    - Various field name formats (camelCase, snake_case, spaces)

    Returns:
        Normalized dict with {filename, content_type, content_base64} or None
    """
    # If string, parse JSON first (Make.com sends stringified attachments)
    if isinstance(att, str):
        try:
            att = json.loads(att)
        except json.JSONDecodeError:
            logger.warning("attachment_json_parse_failed", raw=att[:100] if len(att) > 100 else att)
            return None

    if not isinstance(att, dict):
        return None

    # Extract filename (multiple possible field names)
    filename = (
        att.get("fileName") or
        att.get("filename") or
        att.get("File name") or
        att.get("name")
    )

    # Extract content type
    content_type = (
        att.get("contentType") or
        att.get("content_type") or
        att.get("MIME type") or
        att.get("mimeType")
    )

    # Extract data
    data = (
        att.get("data") or
        att.get("Data") or
        att.get("content_base64") or
        att.get("content")
    )

    if not all([filename, content_type, data]):
        logger.warning(
            "attachment_missing_fields",
            has_filename=bool(filename),
            has_content_type=bool(content_type),
            has_data=bool(data)
        )
        return None

    # Convert Buffer format to base64
    if isinstance(data, dict) and data.get("type") == "Buffer":
        byte_array = data.get("data", [])
        try:
            content_base64 = base64.b64encode(bytes(byte_array)).decode("utf-8")
        except Exception as e:
            logger.warning("buffer_to_base64_failed", error=str(e))
            return None
    elif isinstance(data, str):
        content_base64 = data  # Already base64
    else:
        logger.warning("attachment_data_unknown_format", data_type=type(data).__name__)
        return None

    logger.debug(
        "attachment_normalized",
        filename=filename,
        content_type=content_type,
        data_length=len(content_base64)
    )

    return {
        "filename": filename,
        "content_type": content_type,
        "content_base64": content_base64
    }


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
async def ingest_email(request: Request) -> EmailIngestResponse:
    """
    Process email forwarded from Power Automate or Make.com Mailhook.

    Handles multiple attachment formats:
    - Power Automate: {filename, content_type, content_base64}
    - Make.com Mailhook: stringified JSON with Buffer format

    Decision logic (match-based, not confidence-based):
    - Matched existing shipment → Auto-update (any confidence)
    - No match found → Telegram alert for manual review

    Returns:
        EmailIngestResponse with processing result
    """
    # Get raw JSON body
    try:
        raw_body = await request.json()
    except Exception as e:
        logger.error("invalid_json_body", error=str(e))
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {str(e)}")

    logger.info(
        "email_ingest_raw_received",
        has_from=bool(raw_body.get("from")),
        has_subject=bool(raw_body.get("subject")),
        attachment_count=len(raw_body.get("attachments", [])),
        attachment_types=[type(a).__name__ for a in raw_body.get("attachments", [])[:3]]
    )

    # Normalize attachments before Pydantic validation
    if "attachments" in raw_body and raw_body["attachments"]:
        normalized = []
        for att in raw_body["attachments"]:
            norm = _normalize_attachment(att)
            if norm:
                normalized.append(norm)
                logger.info("attachment_processed", filename=norm["filename"])
            else:
                logger.warning("attachment_skipped", raw_type=type(att).__name__)
        raw_body["attachments"] = normalized

    # Validate with Pydantic
    try:
        data = EmailIngestRequest(**raw_body)
    except ValidationError as e:
        logger.error("validation_failed", errors=e.errors())
        raise HTTPException(status_code=422, detail=e.errors())

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

    # Extract parsed values for matching
    booking_number = _parsed_field_to_value(parsed_data.booking_number)
    shp_number = _parsed_field_to_value(parsed_data.shp_number)
    container_numbers = parsed_data.containers if parsed_data.containers else []

    # Get confidence for logging (NOT for gating decisions)
    confidence = parsed_data.overall_confidence

    # Determine action based on document type and matching
    # Uses shared ingestion service for consistent logic
    decision = get_ingestion_service().determine_action(
        document_type=parsed_data.document_type,
        booking_number=booking_number,
        shp_number=shp_number,
        container_numbers=container_numbers,
        match_order=["booking", "shp", "containers"]  # Email doesn't have target_id
    )

    # Handle NEEDS_REVIEW - alert and return
    if decision.action == IngestAction.NEEDS_REVIEW:
        logger.info(
            "email_needs_review",
            document_type=parsed_data.document_type,
            reason=decision.reason,
            booking=booking_number,
            shp=shp_number,
            containers_count=len(container_numbers),
            confidence=confidence
        )
        _send_needs_review_telegram(
            data.sender,
            data.subject,
            decision.reason,
            parsed_data
        )
        return EmailIngestResponse(
            success=True,
            action="needs_review",
            message=decision.reason,
            document_type=parsed_data.document_type,
            booking_number=booking_number,
            confidence=confidence,
            parsed_fields={
                "booking": booking_number,
                "shp": shp_number,
                "vessel": _parsed_field_to_value(parsed_data.vessel),
                "containers": container_numbers[:5],  # First 5
            }
        )

    # UPDATE or CREATE - proceed to confirm
    existing_shipment = decision.shipment
    matched_by = decision.matched_by

    # Log warning if confidence is low but we're proceeding (diagnostic only)
    if confidence < LOW_CONFIDENCE_WARNING:
        logger.warning(
            "email_low_confidence_proceeding",
            confidence=confidence,
            action=decision.action.value,
            matched_by=matched_by,
            shipment_id=existing_shipment.id if existing_shipment else None,
            document_type=parsed_data.document_type
        )

    # Matched shipment - proceed to auto-confirm
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