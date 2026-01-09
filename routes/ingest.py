"""
Document ingestion routes.

Handles uploading and parsing shipment documents from various sources.
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from typing import Optional
import structlog

from models.ingest import (
    ParsedDocumentData,
    ConfirmIngestRequest,
    StructuredIngestRequest,
    IngestResponse,
)
from models.shipment import (
    ShipmentCreate,
    ShipmentUpdate,
    ShipmentStatus,
)
from services.document_parser_service import get_parser_service
from services.shipment_service import get_shipment_service
from services.port_service import get_port_service
from exceptions import NotFoundError, DatabaseError

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/shipments/ingest", tags=["Shipment Ingestion"])


@router.post("/pdf", response_model=IngestResponse)
async def ingest_pdf(
    file: UploadFile = File(..., description="PDF document to parse"),
    source: str = Form("pdf_upload", description="Source of upload")
) -> IngestResponse:
    """
    Upload and parse a shipment PDF document.

    Returns parsed data with confidence scores for user review.
    User must then call /confirm endpoint to create/update shipment.

    Supported document types:
    - Booking confirmations
    - Departure notices
    - Bills of lading (HBL/MBL)
    - Arrival notices

    Args:
        file: PDF file upload
        source: Source of upload (pdf_upload or email_forward)

    Returns:
        IngestResponse with parsed data and confidence scores

    Raises:
        400: Invalid file or parsing failed
    """
    logger.info(
        "pdf_ingest_started",
        filename=file.filename,
        content_type=file.content_type,
        source=source
    )

    # Validate file type
    if not file.filename or not file.filename.lower().endswith('.pdf'):
        raise HTTPException(
            status_code=400,
            detail="File must be a PDF"
        )

    try:
        # Read file contents
        pdf_bytes = await file.read()

        if len(pdf_bytes) == 0:
            raise HTTPException(
                status_code=400,
                detail="Uploaded file is empty"
            )

        # Parse PDF
        parser = get_parser_service()
        parsed_data = parser.parse_pdf(pdf_bytes)

        logger.info(
            "pdf_parsed_successfully",
            filename=file.filename,
            document_type=parsed_data.document_type,
            overall_confidence=parsed_data.overall_confidence,
            has_shp=bool(parsed_data.shp_number),
            has_booking=bool(parsed_data.booking_number),
            containers_count=len(parsed_data.containers)
        )

        return IngestResponse(
            success=True,
            message=f"PDF parsed successfully. Document type: {parsed_data.document_type}. "
                    f"Confidence: {parsed_data.overall_confidence:.0%}. "
                    "Please review and confirm the data.",
            action="parsed_pending_confirmation",
            parsed_data=parsed_data
        )

    except ValueError as e:
        logger.error("pdf_parsing_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse PDF: {str(e)}"
        )
    except Exception as e:
        logger.error("pdf_ingest_error", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Error processing PDF: {str(e)}"
        )


@router.post("/confirm", response_model=IngestResponse)
async def confirm_ingest(data: ConfirmIngestRequest) -> IngestResponse:
    """
    Confirm parsed data and create or update shipment.

    After user reviews parsed PDF data, they submit confirmed/corrected
    data through this endpoint. System will:
    - Create new shipment if SHP/booking number doesn't exist
    - Update existing shipment if found
    - Create shipment event for audit trail

    Args:
        data: Confirmed shipment data from user

    Returns:
        IngestResponse with created/updated shipment details

    Raises:
        400: Invalid data or missing required fields
        500: Database error
    """
    logger.info(
        "confirm_ingest_started",
        shp_number=data.shp_number,
        booking_number=data.booking_number,
        document_type=data.document_type
    )

    # Validate: Must have at least one identifier
    if not data.shp_number and not data.booking_number:
        raise HTTPException(
            status_code=400,
            detail="Must provide either SHP number or booking number"
        )

    shipment_service = get_shipment_service()

    try:
        # Try to find existing shipment
        existing_shipment = None

        if data.shp_number:
            existing_shipment = shipment_service.get_by_shp_number(data.shp_number)
            if existing_shipment:
                logger.info("existing_shipment_found_by_shp", shipment_id=existing_shipment.id)

        if not existing_shipment and data.booking_number:
            existing_shipment = shipment_service.get_by_booking_number(data.booking_number)
            if existing_shipment:
                logger.info("existing_shipment_found_by_booking", shipment_id=existing_shipment.id)

        # Determine status based on document type
        # HBL/MBL don't change status - they just add reference info
        status_map = {
            "booking": ShipmentStatus.AT_FACTORY,  # Booking confirmed, still at factory
            "departure": ShipmentStatus.IN_TRANSIT,  # Departed from origin port
            "arrival": ShipmentStatus.AT_DESTINATION_PORT,  # Arrived at destination
        }

        # HBL/MBL should NOT change status
        should_update_status = data.document_type not in ["hbl", "mbl"]
        new_status = status_map.get(data.document_type, ShipmentStatus.AT_FACTORY)

        if existing_shipment:
            # UPDATE existing shipment
            update_data = ShipmentUpdate(
                booking_number=data.booking_number or existing_shipment.booking_number,
                vessel_name=data.vessel or existing_shipment.vessel_name,
                etd=data.etd or data.atd or existing_shipment.etd,
                eta=data.eta or data.ata or existing_shipment.eta,
                actual_departure=data.atd if data.atd else None,
                actual_arrival=data.ata if data.ata else None,
            )

            updated_shipment = shipment_service.update(
                existing_shipment.id,
                update_data
            )

            # Update status separately if it changed AND we should update status
            # HBL/MBL documents should NOT change status
            if should_update_status and new_status != existing_shipment.status:
                updated_shipment = shipment_service.update_status(
                    existing_shipment.id,
                    new_status
                )

            # Add note about the update
            event_notes = f"Updated via {data.source} - {data.document_type} document"
            if data.notes:
                event_notes += f"\nUser notes: {data.notes}"

            # Status will auto-create event via service
            logger.info(
                "shipment_updated_from_ingest",
                shipment_id=updated_shipment.id,
                shp_number=updated_shipment.shp_number
            )

            return IngestResponse(
                success=True,
                message=f"Shipment {updated_shipment.shp_number} updated successfully",
                shipment_id=updated_shipment.id,
                shp_number=updated_shipment.shp_number,
                action="updated"
            )

        else:
            # CREATE new shipment

            # HBL/MBL documents should only update existing shipments, not create new ones
            if data.document_type in ["hbl", "mbl"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot create new shipment from {data.document_type.upper()} document. "
                           f"No existing shipment found with SHP '{data.shp_number}' or booking '{data.booking_number}'. "
                           "Please ingest a booking confirmation first."
                )

            # Generate SHP number if not provided
            shp_number = data.shp_number
            if not shp_number:
                # Use booking number as base, or generate from timestamp
                import uuid
                shp_number = f"SHP{str(uuid.uuid4())[:7].upper()}"

            # Look up or create ports
            port_service = get_port_service()

            origin_port_id = None
            destination_port_id = None

            if data.pol:
                origin_port = port_service.find_or_create(
                    name=data.pol,
                    port_type="ORIGIN",
                    country="Colombia"  # Default assumption for origin
                )
                origin_port_id = origin_port.id
                logger.info("origin_port_resolved", name=data.pol, port_id=origin_port_id)

            if data.pod:
                destination_port = port_service.find_or_create(
                    name=data.pod,
                    port_type="DESTINATION",
                    country="Guatemala"  # Default assumption for destination
                )
                destination_port_id = destination_port.id
                logger.info("destination_port_resolved", name=data.pod, port_id=destination_port_id)

            # Ports are optional - can be added later from departure/arrival documents
            if not origin_port_id and not destination_port_id:
                logger.info("shipment_created_without_ports", shp_number=shp_number)

            create_data = ShipmentCreate(
                shp_number=shp_number,
                booking_number=data.booking_number,
                vessel_name=data.vessel,
                etd=data.etd or data.atd,
                eta=data.eta or data.ata,
                origin_port_id=origin_port_id,
                destination_port_id=destination_port_id,
            )

            new_shipment = shipment_service.create(create_data)

            logger.info(
                "shipment_created_from_ingest",
                shipment_id=new_shipment.id,
                shp_number=new_shipment.shp_number
            )

            return IngestResponse(
                success=True,
                message=f"Shipment {new_shipment.shp_number} created successfully",
                shipment_id=new_shipment.id,
                shp_number=new_shipment.shp_number,
                action="created"
            )

    except DatabaseError as e:
        logger.error("confirm_ingest_db_error", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        logger.error("confirm_ingest_error", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Error confirming ingestion: {str(e)}"
        )


@router.post("/structured", response_model=IngestResponse)
async def ingest_structured(data: StructuredIngestRequest) -> IngestResponse:
    """
    Ingest clean structured data from API/webhook.

    Future-ready endpoint for receiving clean shipment data from
    partner APIs or webhooks. No parsing needed.

    Args:
        data: Clean structured shipment data

    Returns:
        IngestResponse with created/updated shipment details

    Raises:
        400: Invalid data
        500: Database error
    """
    logger.info(
        "structured_ingest_started",
        shp_number=data.shp_number,
        booking_number=data.booking_number,
        source_system=data.source_system
    )

    # Validate
    if not data.shp_number and not data.booking_number:
        raise HTTPException(
            status_code=400,
            detail="Must provide either SHP number or booking number"
        )

    # Convert to ConfirmIngestRequest and reuse logic
    confirm_data = ConfirmIngestRequest(
        shp_number=data.shp_number,
        booking_number=data.booking_number,
        document_type=data.document_type,
        containers=data.containers,
        etd=data.etd,
        eta=data.eta,
        atd=data.atd,
        ata=data.ata,
        pol=data.pol,
        pod=data.pod,
        vessel=data.vessel,
        source=data.source,
        notes=f"Source: {data.source_system or 'API'}, External ID: {data.external_id or 'N/A'}"
    )

    return await confirm_ingest(confirm_data)