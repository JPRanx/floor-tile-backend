"""
Document ingestion routes.

Handles uploading and parsing shipment documents from various sources.
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from typing import Optional
import hashlib
import structlog

from models.ingest import (
    ParsedDocumentData,
    ParsedContainerDetails,
    ConfirmIngestRequest,
    StructuredIngestRequest,
    IngestResponse,
    IngestPreviewResponse,
    CandidateShipment,
    # Packing list models
    PackingListLineItem,
    PackingListTotals,
    ParsedPackingListResponse,
    PackingListIngestResponse,
    ConfirmPackingListRequest,
)
from models.shipment import (
    ShipmentCreate,
    ShipmentUpdate,
    ShipmentStatus,
    ShipmentStatusUpdate,
    ShipmentCostsUpdate,
    is_valid_shipment_status_transition,
)
from services.document_parser_service import get_parser_service
from services.claude_parser_service import get_claude_parser_service, CLAUDE_AVAILABLE
from services.packing_list_parser_service import get_packing_list_parser_service
from services import preview_cache_service
from services.upload_history_service import get_upload_history_service
from exceptions.errors import PDFParseError
from services.shipment_service import get_shipment_service
from services.port_service import get_port_service
from services.alert_service import get_alert_service
from services.container_service import get_container_service
from services.ingestion_service import get_ingestion_service
from services.factory_order_service import get_factory_order_service
from models.alert import AlertType, AlertSeverity, AlertCreate
from models.container import ContainerCreate
from exceptions import NotFoundError, DatabaseError
from integrations.telegram_messages import get_message

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/shipments/ingest", tags=["Shipment Ingestion"])


def _find_similar_container(
    new_number: str,
    existing_numbers: list[str],
    threshold: float = 0.9
) -> str | None:
    """
    Check if new_number is similar to any existing container (fuzzy match).

    Handles OCR misreads where a single digit is wrong:
    - DFSU9116028 matches DFSU1916028 (10/11 = 91% similar)
    - Helps prevent duplicate containers from MBL misreads

    Args:
        new_number: Container number to check
        existing_numbers: List of existing container numbers
        threshold: Minimum similarity (0.9 = 10 of 11 chars must match)

    Returns:
        The existing container number if match found, None otherwise
    """
    new_upper = new_number.upper().replace(" ", "")

    for existing in existing_numbers:
        existing_upper = existing.upper().replace(" ", "")

        # Must be same length for comparison
        if len(new_upper) != len(existing_upper):
            continue

        # Count matching characters
        matches = sum(a == b for a, b in zip(new_upper, existing_upper))
        similarity = matches / len(new_upper)

        if similarity >= threshold:
            logger.info(
                "fuzzy_container_match",
                new_container=new_number,
                matched_to=existing,
                similarity=f"{similarity:.1%}",
                mismatched_chars=len(new_upper) - matches
            )
            return existing

    return None


def _create_containers_for_shipment(shipment_id: str, container_numbers: list[str]) -> int:
    """
    Create containers for a shipment, avoiding duplicates.

    Uses fuzzy matching to detect near-duplicates from OCR misreads.

    Args:
        shipment_id: UUID of the shipment
        container_numbers: List of container numbers to create

    Returns:
        Number of containers created
    """
    if not container_numbers:
        return 0

    container_service = get_container_service()

    # Get existing containers for this shipment
    existing_containers = container_service.get_by_shipment(shipment_id)
    existing_numbers_set = {c.container_number.upper() for c in existing_containers if c.container_number}
    existing_numbers_list = list(existing_numbers_set)

    created_count = 0
    for container_num in container_numbers:
        container_num_upper = container_num.upper()

        # Skip if exact match exists
        if container_num_upper in existing_numbers_set:
            logger.debug("container_already_exists", container_number=container_num)
            continue

        # Check for fuzzy match (handles OCR misreads like 1→9)
        fuzzy_match = _find_similar_container(container_num_upper, existing_numbers_list)
        if fuzzy_match:
            # Skip - this is likely the same container with OCR error
            logger.info(
                "container_skipped_fuzzy_match",
                new_container=container_num,
                matched_existing=fuzzy_match
            )
            continue

        try:
            container_service.create(ContainerCreate(
                shipment_id=shipment_id,
                container_number=container_num_upper
            ))
            created_count += 1
            # Add to existing list to prevent duplicates within same batch
            existing_numbers_set.add(container_num_upper)
            existing_numbers_list.append(container_num_upper)
            logger.info("container_created", shipment_id=shipment_id, container_number=container_num)
        except Exception as e:
            logger.warning("container_creation_failed", container_number=container_num, error=str(e))

    return created_count


def _create_containers_with_details_for_shipment(
    shipment_id: str,
    container_numbers: list[str],
    container_details: list[ParsedContainerDetails]
) -> int:
    """
    Create containers for a shipment with detailed info (type, weight, volume, pallets).

    Uses fuzzy matching to detect near-duplicates from OCR misreads.

    Args:
        shipment_id: UUID of the shipment
        container_numbers: List of container numbers to create
        container_details: Parsed container details (type, weight, volume, pallets)

    Returns:
        Number of containers created
    """
    if not container_numbers:
        return 0

    container_service = get_container_service()

    # Build lookup dict for container details
    details_lookup = {
        detail.container_number.upper(): detail
        for detail in container_details
    }

    # Get existing containers for this shipment
    existing_containers = container_service.get_by_shipment(shipment_id)
    existing_numbers_set = {c.container_number.upper() for c in existing_containers if c.container_number}
    existing_numbers_list = list(existing_numbers_set)

    created_count = 0
    for container_num in container_numbers:
        container_num_upper = container_num.upper()

        # Skip if exact match exists
        if container_num_upper in existing_numbers_set:
            logger.debug("container_already_exists", container_number=container_num)
            continue

        # Check for fuzzy match (handles OCR misreads like 1→9)
        fuzzy_match = _find_similar_container(container_num_upper, existing_numbers_list)
        if fuzzy_match:
            # Skip - this is likely the same container with OCR error
            logger.info(
                "container_skipped_fuzzy_match",
                new_container=container_num,
                matched_existing=fuzzy_match
            )
            continue

        # Get details if available
        detail = details_lookup.get(container_num_upper)

        try:
            from decimal import Decimal
            container_data = ContainerCreate(
                shipment_id=shipment_id,
                container_number=container_num_upper,
                container_type=detail.container_type if detail else None,
                total_weight_kg=Decimal(str(detail.weight_kg)) if detail and detail.weight_kg else None,
                total_m2=Decimal(str(detail.volume_m3)) if detail and detail.volume_m3 else None,
                total_pallets=detail.pallets if detail else None,
            )
            container_service.create(container_data)
            created_count += 1
            # Add to existing list to prevent duplicates within same batch
            existing_numbers_set.add(container_num_upper)
            existing_numbers_list.append(container_num_upper)
            logger.info(
                "container_created_with_details",
                shipment_id=shipment_id,
                container_number=container_num,
                container_type=detail.container_type if detail else None,
                weight_kg=detail.weight_kg if detail else None,
                pallets=detail.pallets if detail else None
            )
        except Exception as e:
            logger.warning("container_creation_failed", container_number=container_num, error=str(e))

    return created_count


def _create_containers_from_packing_list(
    shipment_id: str,
    parsed_data: ParsedPackingListResponse
) -> int:
    """
    Create containers from packing list line items, aggregated by container_number.

    Args:
        shipment_id: UUID of the shipment
        parsed_data: Parsed packing list response with line items

    Returns:
        Number of containers created
    """
    from decimal import Decimal
    from collections import defaultdict

    if not parsed_data.line_items:
        return 0

    container_service = get_container_service()

    # Get existing containers
    existing = container_service.get_by_shipment(shipment_id)
    existing_numbers = {c.container_number.upper() for c in existing if c.container_number}
    existing_list = list(existing_numbers)

    # Aggregate line items by container
    container_data = defaultdict(lambda: {
        'seal_number': None,
        'total_pallets': 0,
        'total_m2': Decimal('0'),
        'total_weight_kg': Decimal('0'),
    })

    for item in parsed_data.line_items:
        if not item.container_number:
            continue
        num = item.container_number.upper()
        d = container_data[num]
        if item.seal_number and not d['seal_number']:
            d['seal_number'] = item.seal_number
        d['total_pallets'] += item.pallets or 0
        d['total_m2'] += Decimal(str(item.m2_total or 0))
        d['total_weight_kg'] += Decimal(str(item.gross_weight_kg or 0))

    created = 0
    for num, d in container_data.items():
        # Skip if already exists
        if num in existing_numbers:
            logger.debug("packing_list_container_exists", container_number=num)
            continue

        # Check fuzzy match
        if _find_similar_container(num, existing_list):
            logger.info("packing_list_container_skipped_fuzzy", container_number=num)
            continue

        try:
            container_service.create(ContainerCreate(
                shipment_id=shipment_id,
                container_number=num,
                seal_number=d['seal_number'],
                total_pallets=d['total_pallets'],
                total_weight_kg=d['total_weight_kg'],
                total_m2=d['total_m2'],
            ))
            created += 1
            existing_numbers.add(num)
            existing_list.append(num)
            logger.info(
                "packing_list_container_created",
                shipment_id=shipment_id,
                container_number=num,
                pallets=d['total_pallets'],
                m2=str(d['total_m2']),
                weight_kg=str(d['total_weight_kg'])
            )
        except Exception as e:
            logger.warning("packing_list_container_failed", container_number=num, error=str(e))

    return created


def _check_cross_reference_discrepancies(
    shipment_id: str,
    parsed_data: ParsedDocumentData
) -> list[str]:
    """
    Compare HBL/MBL totals against linked factory order.

    Foundation for cross-reference validation between documents and orders.

    Args:
        shipment_id: UUID of the shipment
        parsed_data: Parsed document data with container details

    Returns:
        List of discrepancy messages (empty if no discrepancies)
    """
    discrepancies = []

    # Skip if no container details
    if not parsed_data.container_details:
        return discrepancies

    try:
        shipment_service = get_shipment_service()
        shipment = shipment_service.get_by_id(shipment_id)

        if not shipment or not shipment.factory_order_id:
            logger.debug("no_factory_order_linked", shipment_id=shipment_id)
            return discrepancies

        # Calculate totals from HBL/MBL container details
        hbl_total_pallets = sum(
            d.pallets or 0 for d in parsed_data.container_details
        )
        hbl_total_weight = sum(
            d.weight_kg or 0 for d in parsed_data.container_details
        )
        hbl_total_volume = sum(
            d.volume_m3 or 0 for d in parsed_data.container_details
        )

        # Future: Get order details from factory_order_service
        # For now, just log the totals for comparison
        logger.info(
            "cross_reference_totals",
            shipment_id=shipment_id,
            factory_order_id=shipment.factory_order_id,
            hbl_pallets=hbl_total_pallets,
            hbl_weight_kg=hbl_total_weight,
            hbl_volume_m3=hbl_total_volume
        )

        # TODO: When factory order service is available:
        # order = get_factory_order_service().get_by_id(shipment.factory_order_id)
        # if order:
        #     if abs(order.total_pallets - hbl_total_pallets) > 0:
        #         discrepancies.append(
        #             f"Pallets mismatch: Order={order.total_pallets}, HBL={hbl_total_pallets}"
        #         )
        #     if abs(order.total_weight_kg - hbl_total_weight) > 100:  # 100kg tolerance
        #         discrepancies.append(
        #             f"Weight mismatch: Order={order.total_weight_kg}kg, HBL={hbl_total_weight}kg"
        #         )

        if discrepancies:
            # Create alert for discrepancies
            try:
                alert_service = get_alert_service()
                alert_service.create(
                    AlertCreate(
                        type=AlertType.CONTAINER_READY,  # TODO: Add DISCREPANCY type
                        severity=AlertSeverity.WARNING,
                        title=f"Discrepancy detected: {shipment.shp_number}",
                        message=f"⚠️ Cross-reference discrepancies found:\n\n" +
                                "\n".join(f"• {d}" for d in discrepancies),
                        shipment_id=shipment_id,
                    ),
                    send_telegram=True
                )
            except Exception as alert_error:
                logger.warning("discrepancy_alert_failed", error=str(alert_error))

    except Exception as e:
        logger.warning("cross_reference_check_failed", shipment_id=shipment_id, error=str(e))

    return discrepancies


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

        parsed_data = None
        parser_used = "pdfplumber"

        # Try pdfplumber first (fast, free for native text PDFs)
        try:
            parser = get_parser_service()
            parsed_data = parser.parse_pdf(pdf_bytes)
            logger.info("pdf_parsed_with_pdfplumber", filename=file.filename)

        except PDFParseError as parse_error:
            # Check if we should fall back to Claude Vision
            if parse_error.details and parse_error.details.get("use_claude_vision"):
                logger.info(
                    "falling_back_to_claude_vision",
                    filename=file.filename,
                    reason="insufficient_text"
                )

                if not CLAUDE_AVAILABLE:
                    raise HTTPException(
                        status_code=400,
                        detail="PDF appears to be scanned but Claude Vision is not configured. "
                               "Set ANTHROPIC_API_KEY environment variable to enable."
                    )

                # Use Claude Vision for scanned PDFs
                claude_parser = get_claude_parser_service()
                parsed_data = await claude_parser.parse_pdf(pdf_bytes)
                parser_used = "claude_vision"
                logger.info("pdf_parsed_with_claude_vision", filename=file.filename)
            else:
                # Re-raise other PDFParseErrors
                raise

        logger.info(
            "pdf_parsed_successfully",
            filename=file.filename,
            parser_used=parser_used,
            document_type=parsed_data.document_type,
            overall_confidence=parsed_data.overall_confidence,
            has_shp=bool(parsed_data.shp_number),
            has_booking=bool(parsed_data.booking_number),
            containers_count=len(parsed_data.containers)
        )

        return IngestResponse(
            success=True,
            message=f"PDF parsed successfully ({parser_used}). Document type: {parsed_data.document_type}. "
                    f"Confidence: {parsed_data.overall_confidence:.0%}. "
                    "Please review and confirm the data.",
            action="parsed_pending_confirmation",
            parsed_data=parsed_data
        )

    except PDFParseError as e:
        logger.error("pdf_parsing_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse PDF: {str(e)}"
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

    # DEBUG: Print all identifiers for matching
    print(f"=== CONFIRM INGEST DEBUG ===")
    print(f"SHP: {data.shp_number}")
    print(f"Booking: {data.booking_number}")
    print(f"Containers: {data.containers}")
    print(f"Document Type: {data.document_type}")
    print(f"Target Shipment ID: {data.target_shipment_id}")
    print(f"==============================")

    # Validate: Must have at least one identifier (unless manual assignment)
    # HBL/MBL can match by containers alone, so allow that for those document types
    if not data.target_shipment_id and not data.shp_number and not data.booking_number and not data.containers:
        raise HTTPException(
            status_code=400,
            detail="Must provide SHP number, booking number, or containers for matching"
        )

    shipment_service = get_shipment_service()

    try:
        # Validate target_shipment_id if provided (manual assignment)
        if data.target_shipment_id:
            try:
                target_check = shipment_service.get_by_id(data.target_shipment_id)
                if not target_check:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Target shipment not found: {data.target_shipment_id}"
                    )
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail=f"Target shipment not found: {data.target_shipment_id}"
                )

        # Use shared ingestion service for matching
        # Order: target_id → shp → booking → containers (manual path prioritizes SHP)
        existing_shipment, matched_by = get_ingestion_service().find_matching_shipment(
            booking_number=data.booking_number,
            shp_number=data.shp_number,
            container_numbers=data.containers,
            target_shipment_id=data.target_shipment_id,
            match_order=["target_id", "shp", "booking", "containers"]
        )

        # Normalize matched_by for logging consistency
        if matched_by == "target_id":
            matched_by = "manual_assignment"
        elif matched_by == "containers":
            matched_by = "container"

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
            # DEBUG: Log what we're updating with
            print(f"=== HBL UPDATE DEBUG ===")
            print(f"Document type: {data.document_type}")
            print(f"Matched by: {matched_by}")
            print(f"Data SHP: {data.shp_number}")
            print(f"Data Booking: {data.booking_number}")
            print(f"Data Vessel: {data.vessel}")
            print(f"Data Voyage: {data.voyage}")
            print(f"Data POL: {data.pol}")
            print(f"Data POD: {data.pod}")
            print(f"Data ETD: {data.etd}")
            print(f"Data ETA: {data.eta}")
            print(f"Data ATD: {data.atd}")
            print(f"Data ATA: {data.ata}")
            print(f"Existing SHP: {existing_shipment.shp_number}")
            print(f"Existing Booking: {existing_shipment.booking_number}")
            print(f"Existing Vessel: {existing_shipment.vessel_name}")
            print(f"Existing Voyage: {existing_shipment.voyage_number}")
            print(f"Existing ATD: {existing_shipment.actual_departure}")
            print(f"=======================")

            # For HBL/MBL: Special update logic to preserve booking and update B/L
            if data.document_type in ["hbl", "mbl"]:
                update_data = ShipmentUpdate(
                    # PRESERVE existing booking - only set if currently empty
                    booking_number=existing_shipment.booking_number or data.booking_number,
                    # Update vessel and voyage from HBL
                    vessel_name=data.vessel or existing_shipment.vessel_name,
                    voyage_number=data.voyage or existing_shipment.voyage_number,
                    # Dates
                    etd=data.etd or data.atd or existing_shipment.etd,
                    eta=data.eta or data.ata or existing_shipment.eta,
                    actual_departure=data.atd if data.atd else existing_shipment.actual_departure,
                    actual_arrival=data.ata if data.ata else existing_shipment.actual_arrival,
                )

                # Update SHP from HBL (regardless of how we matched - container or manual)
                if data.shp_number and data.shp_number != existing_shipment.shp_number:
                    update_data.shp_number = data.shp_number
                    logger.info(
                        "updating_shp_from_hbl",
                        old_shp=existing_shipment.shp_number,
                        new_shp=data.shp_number,
                        matched_by=matched_by
                    )

                # Store HBL number as bill_of_lading
                if data.shp_number:
                    update_data.bill_of_lading = data.shp_number
                    logger.info("setting_bill_of_lading", bill_of_lading=data.shp_number)

            else:
                # Non-HBL/MBL documents: Standard update logic
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
            # Note: existing_shipment.status is a string value, new_status is ShipmentStatus enum
            if should_update_status and new_status.value != existing_shipment.status:
                # Pre-check: Only allow forward status progression
                # Prevents regression when documents arrive out of order
                current_status = ShipmentStatus(existing_shipment.status)
                if is_valid_shipment_status_transition(current_status, new_status):
                    updated_shipment = shipment_service.update_status(
                        existing_shipment.id,
                        ShipmentStatusUpdate(status=new_status)
                    )
                else:
                    logger.info(
                        "status_update_skipped_regression",
                        shipment_id=existing_shipment.id,
                        current_status=current_status.value,
                        proposed_status=new_status.value,
                        document_type=data.document_type,
                        reason="Document arrived but shipment already past this status"
                    )

            # Auto-populate freight cost from MBL/HBL if available
            # DEBUG: Log all conditions for freight auto-populate
            logger.info(
                "freight_auto_populate_check",
                document_type=data.document_type,
                is_hbl_mbl=data.document_type in ["hbl", "mbl"],
                has_original_parsed_data=data.original_parsed_data is not None,
                has_freight_amount=data.original_parsed_data.freight_amount_usd is not None if data.original_parsed_data else False,
                freight_amount_value=data.original_parsed_data.freight_amount_usd.value if data.original_parsed_data and data.original_parsed_data.freight_amount_usd else None,
                existing_freight_cost=existing_shipment.freight_cost_usd
            )
            if (data.document_type in ["hbl", "mbl"] and
                data.original_parsed_data and
                data.original_parsed_data.freight_amount_usd and
                not existing_shipment.freight_cost_usd):
                try:
                    from decimal import Decimal
                    freight_amount = Decimal(data.original_parsed_data.freight_amount_usd.value)
                    shipment_service.update_costs(
                        existing_shipment.id,
                        ShipmentCostsUpdate(freight_cost_usd=freight_amount)
                    )
                    logger.info(
                        "freight_cost_auto_populated",
                        shipment_id=existing_shipment.id,
                        freight_amount_usd=str(freight_amount),
                        source=f"{data.document_type}_document"
                    )
                except Exception as freight_error:
                    logger.warning(
                        "freight_cost_auto_populate_failed",
                        shipment_id=existing_shipment.id,
                        error=str(freight_error)
                    )

            # Update origin port if provided
            # DEBUG: Log port processing
            print(f"=== PORT UPDATE DEBUG ===")
            print(f"Data POL: {data.pol}")
            print(f"Data POD: {data.pod}")
            print(f"Existing origin_port_id: {existing_shipment.origin_port_id}")
            print(f"Existing destination_port_id: {existing_shipment.destination_port_id}")

            if data.pol:
                port_service = get_port_service()
                origin_port = port_service.find_or_create(
                    name=data.pol,
                    port_type="ORIGIN",
                    country="Colombia"
                )
                origin_port_id_str = str(origin_port.id)
                print(f"Origin port resolved: {origin_port_id_str} - {origin_port.name}")
                # Update if not already set OR if HBL/MBL (which should fill in missing data)
                if not existing_shipment.origin_port_id or data.document_type in ["hbl", "mbl"]:
                    port_update = ShipmentUpdate(origin_port_id=origin_port_id_str)
                    print(f"Updating origin_port_id with: {port_update}")
                    result = shipment_service.update(existing_shipment.id, port_update)
                    print(f"Origin port update result: {result.origin_port_id if result else 'FAILED'}")
                    logger.info("origin_port_updated", name=data.pol, port_id=origin_port_id_str)

            # Update destination port if provided
            if data.pod:
                port_service = get_port_service()
                destination_port = port_service.find_or_create(
                    name=data.pod,
                    port_type="DESTINATION",
                    country="Guatemala"
                )
                dest_port_id_str = str(destination_port.id)
                print(f"Destination port resolved: {dest_port_id_str} - {destination_port.name}")
                # Update if not already set OR if HBL/MBL (which should fill in missing data)
                if not existing_shipment.destination_port_id or data.document_type in ["hbl", "mbl"]:
                    port_update = ShipmentUpdate(destination_port_id=dest_port_id_str)
                    print(f"Updating destination_port_id with: {port_update}")
                    result = shipment_service.update(existing_shipment.id, port_update)
                    print(f"Destination port update result: {result.destination_port_id if result else 'FAILED'}")
                    logger.info("destination_port_updated", name=data.pod, port_id=dest_port_id_str)

            print(f"=========================")

            # Create containers from document (avoiding duplicates)
            # Use detailed container info if available (from HBL/MBL parsing)
            if data.containers:
                container_details = []
                if data.original_parsed_data and data.original_parsed_data.container_details:
                    container_details = data.original_parsed_data.container_details
                    logger.info(
                        "using_container_details",
                        count=len(container_details)
                    )

                if container_details:
                    containers_created = _create_containers_with_details_for_shipment(
                        existing_shipment.id,
                        data.containers,
                        container_details
                    )
                else:
                    containers_created = _create_containers_for_shipment(
                        existing_shipment.id,
                        data.containers
                    )
                logger.info(
                    "containers_added_to_shipment",
                    shipment_id=existing_shipment.id,
                    containers_requested=len(data.containers),
                    containers_created=containers_created
                )

            # Cross-reference check for HBL/MBL documents
            if data.document_type in ["hbl", "mbl"] and data.original_parsed_data:
                discrepancies = _check_cross_reference_discrepancies(
                    existing_shipment.id,
                    data.original_parsed_data
                )
                if discrepancies:
                    logger.warning(
                        "cross_reference_discrepancies_found",
                        shipment_id=existing_shipment.id,
                        discrepancies=discrepancies
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

            # Send HBL_PROCESSED alert when HBL successfully updates shipment
            if data.document_type in ["hbl", "mbl"]:
                try:
                    alert_service = get_alert_service()
                    container_service = get_container_service()
                    containers = container_service.get_by_shipment(updated_shipment.id)
                    container_count = len(containers)

                    alert_service.create(
                        AlertCreate(
                            type=AlertType.HBL_PROCESSED,
                            severity=AlertSeverity.INFO,
                            title=get_message("title_hbl_processed", shp_number=updated_shipment.shp_number or "N/A"),
                            message=get_message(
                                "hbl_processed",
                                shp_number=updated_shipment.shp_number or "N/A",
                                booking=updated_shipment.booking_number or "N/A",
                                vessel=updated_shipment.vessel_name or "N/A",
                                container_count=container_count
                            ),
                            shipment_id=updated_shipment.id,
                        ),
                        send_telegram=True
                    )
                    logger.info(
                        "hbl_processed_alert_sent",
                        shipment_id=updated_shipment.id,
                        matched_by=matched_by
                    )
                except Exception as alert_error:
                    logger.warning("hbl_processed_alert_failed", error=str(alert_error))

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
            # Instead of error, return candidate shipments for manual assignment
            if data.document_type in ["hbl", "mbl"]:
                logger.info(
                    "hbl_mbl_needs_manual_assignment",
                    document_type=data.document_type,
                    shp_number=data.shp_number,
                    booking_number=data.booking_number,
                    containers=data.containers
                )

                # Get all active shipments as candidates
                shipments_list, _total = shipment_service.get_all(page=1, page_size=50)
                candidates = [
                    CandidateShipment(
                        id=s.id,
                        shp_number=s.shp_number,
                        booking_number=s.booking_number,
                        vessel_name=s.vessel_name,
                        status=s.status.value if hasattr(s.status, 'value') else str(s.status),
                        etd=s.etd,
                        eta=s.eta,
                        created_at=s.created_at.isoformat() if hasattr(s.created_at, 'isoformat') else str(s.created_at)
                    )
                    for s in shipments_list
                ]

                # Send HBL_PENDING alert for manual assignment
                try:
                    alert_service = get_alert_service()
                    alert_service.create(
                        AlertCreate(
                            type=AlertType.HBL_PENDING,
                            severity=AlertSeverity.WARNING,
                            title=get_message("title_hbl_pending", shp_number=data.shp_number or "N/A"),
                            message=get_message(
                                "hbl_pending",
                                shp_number=data.shp_number or "N/A",
                                booking=data.booking_number or "N/A",
                                container_count=len(data.containers) if data.containers else 0
                            ),
                        ),
                        send_telegram=True
                    )
                    logger.info(
                        "hbl_pending_alert_sent",
                        document_type=data.document_type,
                        shp_number=data.shp_number,
                        candidates_count=len(candidates)
                    )
                except Exception as alert_error:
                    logger.warning("hbl_pending_alert_failed", error=str(alert_error))

                return IngestResponse(
                    success=False,
                    message=f"No matching shipment found for {data.document_type.upper()} document. "
                            "Please select an existing shipment to update or create a booking first.",
                    action="needs_assignment",
                    candidate_shipments=candidates
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
                factory_order_id=data.factory_order_id,
            )

            new_shipment = shipment_service.create(create_data)

            logger.info(
                "shipment_created_from_ingest",
                shipment_id=new_shipment.id,
                shp_number=new_shipment.shp_number
            )

            # Create containers from document
            if data.containers:
                containers_created = _create_containers_for_shipment(
                    new_shipment.id,
                    data.containers
                )
                logger.info(
                    "containers_created_for_shipment",
                    shipment_id=new_shipment.id,
                    containers_count=containers_created
                )

            # Send Telegram alert for new shipment
            try:
                alert_service = get_alert_service()

                # If factory order was linked, send BOOKING_AUTO_LINKED alert
                if data.factory_order_id:
                    # Get factory order PV number
                    try:
                        factory_order = get_factory_order_service().get_by_id(data.factory_order_id)
                        pv_number = factory_order.pv_number if factory_order else "N/A"
                    except Exception:
                        pv_number = "N/A"

                    alert_service.create(
                        AlertCreate(
                            type=AlertType.BOOKING_AUTO_LINKED,
                            severity=AlertSeverity.INFO,
                            title=get_message("title_booking_auto_linked", shp_number=new_shipment.shp_number),
                            message=get_message(
                                "booking_auto_linked",
                                shp_number=new_shipment.shp_number,
                                booking=new_shipment.booking_number or "N/A",
                                pv_number=pv_number
                            ),
                            shipment_id=new_shipment.id,
                        ),
                        send_telegram=True
                    )
                    logger.info(
                        "booking_auto_linked_alert_sent",
                        shipment_id=new_shipment.id,
                        factory_order_id=data.factory_order_id
                    )
                else:
                    # Standard new shipment alert
                    alert_service.create(
                        AlertCreate(
                            type=AlertType.CONTAINER_READY,
                            severity=AlertSeverity.INFO,
                            title=get_message("title_new_shipment", shp_number=new_shipment.shp_number),
                            message=get_message(
                                "new_shipment_created",
                                shp_number=new_shipment.shp_number,
                                booking=new_shipment.booking_number or "N/A",
                                vessel=new_shipment.vessel_name or "N/A"
                            ),
                            shipment_id=new_shipment.id,
                        ),
                        send_telegram=True
                    )

                    # Suggestion: Find unlinked factory orders
                    try:
                        factory_order_service = get_factory_order_service()
                        unlinked_orders = factory_order_service.get_unlinked_orders(limit=5)

                        if unlinked_orders:
                            import os
                            frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")
                            order_list = "\n".join([
                                f"• {fo.pv_number} ({fo.total_m2 or 0} m²)"
                                for fo in unlinked_orders
                            ])

                            alert_service.create(
                                AlertCreate(
                                    type=AlertType.LINK_SHIPMENT_TO_ORDER,
                                    severity=AlertSeverity.INFO,
                                    title=get_message("title_link_shipment_to_order", shp_number=new_shipment.shp_number),
                                    message=get_message(
                                        "link_shipment_to_order",
                                        shp_number=new_shipment.shp_number,
                                        vessel=new_shipment.vessel_name or "N/A",
                                        etd=str(new_shipment.etd) if new_shipment.etd else "N/A",
                                        available_orders=order_list,
                                        link=f"{frontend_url}/shipments/{new_shipment.id}"
                                    ),
                                    shipment_id=new_shipment.id,
                                ),
                                send_telegram=True
                            )
                            logger.info(
                                "link_shipment_suggestion_sent",
                                shipment_id=new_shipment.id,
                                available_orders=len(unlinked_orders)
                            )
                    except Exception as suggestion_error:
                        logger.warning("link_shipment_suggestion_failed", error=str(suggestion_error))

            except Exception as alert_error:
                logger.warning("shipment_alert_failed", error=str(alert_error))

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


@router.post("/pdf/preview", response_model=IngestPreviewResponse)
async def preview_pdf_upload(
    file: UploadFile = File(..., description="PDF document to parse"),
    source: str = Form("pdf_upload", description="Source of upload")
) -> IngestPreviewResponse:
    """
    Upload and parse a shipment PDF document (preview only - nothing is saved).

    Parses the PDF and returns data with confidence scores for user review.
    The parsed result is cached with a preview_id. User must then call
    /pdf/confirm/{preview_id} endpoint to create/update shipment.

    Supported document types:
    - Booking confirmations
    - Departure notices
    - Bills of lading (HBL/MBL)
    - Arrival notices

    Args:
        file: PDF file upload
        source: Source of upload (pdf_upload or email_forward)

    Returns:
        IngestPreviewResponse with parsed data, confidence scores, and preview_id

    Raises:
        400: Invalid file or parsing failed
    """
    logger.info(
        "pdf_preview_started",
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
        pdf_file_hash = hashlib.sha256(pdf_bytes).hexdigest()

        # Check for duplicate upload
        pdf_duplicate = get_upload_history_service().check_duplicate("shipment_pdf", pdf_file_hash)

        if len(pdf_bytes) == 0:
            raise HTTPException(
                status_code=400,
                detail="Uploaded file is empty"
            )

        parsed_data = None
        parser_used = "pdfplumber"

        # Try pdfplumber first (fast, free for native text PDFs)
        try:
            parser = get_parser_service()
            parsed_data = parser.parse_pdf(pdf_bytes)
            logger.info("pdf_parsed_with_pdfplumber", filename=file.filename)

        except PDFParseError as parse_error:
            # Check if we should fall back to Claude Vision
            if parse_error.details and parse_error.details.get("use_claude_vision"):
                logger.info(
                    "falling_back_to_claude_vision",
                    filename=file.filename,
                    reason="insufficient_text"
                )

                if not CLAUDE_AVAILABLE:
                    raise HTTPException(
                        status_code=400,
                        detail="PDF appears to be scanned but Claude Vision is not configured. "
                               "Set ANTHROPIC_API_KEY environment variable to enable."
                    )

                # Use Claude Vision for scanned PDFs
                claude_parser = get_claude_parser_service()
                parsed_data = await claude_parser.parse_pdf(pdf_bytes)
                parser_used = "claude_vision"
                logger.info("pdf_parsed_with_claude_vision", filename=file.filename)
            else:
                # Re-raise other PDFParseErrors
                raise

        logger.info(
            "pdf_parsed_successfully",
            filename=file.filename,
            parser_used=parser_used,
            document_type=parsed_data.document_type,
            overall_confidence=parsed_data.overall_confidence,
            has_shp=bool(parsed_data.shp_number),
            has_booking=bool(parsed_data.booking_number),
            containers_count=len(parsed_data.containers)
        )

        # Store in cache
        cache_data = {
            "parsed_data": parsed_data.model_dump(),
            "parser_used": parser_used,
            "filename": file.filename,
            "source": source,
            "pdf_bytes": pdf_bytes.hex(),  # Store as hex string
            "file_hash": pdf_file_hash,
            "upload_type": "shipment_pdf",
        }
        preview_id = preview_cache_service.store_preview(cache_data)

        logger.info(
            "pdf_preview_cached",
            preview_id=preview_id,
            filename=file.filename
        )

        duplicate_msg = ""
        if pdf_duplicate:
            duplicate_msg = f" AVISO: Este archivo ya fue subido el {pdf_duplicate['uploaded_at'][:10]}."

        return IngestPreviewResponse(
            success=True,
            message=f"PDF parsed successfully ({parser_used}). Document type: {parsed_data.document_type}. "
                    f"Confidence: {parsed_data.overall_confidence:.0%}. "
                    f"Please review and confirm the data.{duplicate_msg}",
            action="parsed_pending_confirmation",
            parsed_data=parsed_data,
            preview_id=preview_id,
            expires_in_minutes=30
        )

    except PDFParseError as e:
        logger.error("pdf_parsing_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse PDF: {str(e)}"
        )
    except ValueError as e:
        logger.error("pdf_parsing_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse PDF: {str(e)}"
        )
    except Exception as e:
        logger.error("pdf_preview_error", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Error processing PDF: {str(e)}"
        )


@router.post("/pdf/confirm/{preview_id}", response_model=IngestResponse)
async def confirm_pdf_preview(preview_id: str, data: ConfirmIngestRequest) -> IngestResponse:
    """
    Confirm previously previewed PDF data and create or update shipment.

    Retrieves cached parse result using preview_id, then uses confirmed/corrected
    data to create or update shipment. System will:
    - Create new shipment if SHP/booking number doesn't exist
    - Update existing shipment if found
    - Create shipment event for audit trail

    Args:
        preview_id: UUID of cached preview data
        data: Confirmed shipment data from user

    Returns:
        IngestResponse with created/updated shipment details

    Raises:
        400: Invalid data, missing required fields, or preview not found/expired
        500: Database error
    """
    logger.info(
        "confirm_pdf_preview_started",
        preview_id=preview_id,
        shp_number=data.shp_number,
        booking_number=data.booking_number,
        document_type=data.document_type
    )

    # Retrieve from cache
    cached = preview_cache_service.retrieve_preview(preview_id)
    if not cached:
        raise HTTPException(
            status_code=400,
            detail="Preview not found or expired. Please re-upload the PDF."
        )

    logger.info(
        "preview_retrieved",
        preview_id=preview_id,
        filename=cached.get("filename")
    )

    # Call the existing confirm logic
    # The existing /confirm endpoint has all the business logic we need
    result = await confirm_ingest(data)

    # Record upload history
    get_upload_history_service().record_upload(
        upload_type=cached.get("upload_type", "shipment_pdf"),
        file_hash=cached.get("file_hash", ""),
        filename=cached.get("filename", "unknown"),
        row_count=1,
    )

    # Delete from cache after successful confirmation
    preview_cache_service.delete_preview(preview_id)
    logger.info("preview_cache_deleted", preview_id=preview_id)

    return result


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


# ===================
# PACKING LIST ENDPOINTS
# ===================

@router.post("/packing-list", response_model=PackingListIngestResponse)
async def ingest_packing_list(
    file: UploadFile = File(..., description="Excel packing list file (.xlsx)")
) -> PackingListIngestResponse:
    """
    Upload and parse a factory packing list (Excel).

    Extracts PV number, line items, container assignments, and totals.
    Auto-links to matching FactoryOrder if PV number is found.

    Args:
        file: Excel file upload (.xlsx)

    Returns:
        PackingListIngestResponse with parsed data and factory order match

    Raises:
        400: Invalid file or parsing failed
    """
    logger.info(
        "packing_list_ingest_started",
        filename=file.filename,
        content_type=file.content_type
    )

    # Validate file type
    if not file.filename or not file.filename.lower().endswith('.xlsx'):
        raise HTTPException(
            status_code=400,
            detail="File must be an Excel file (.xlsx)"
        )

    try:
        # Read file contents
        file_bytes = await file.read()

        if len(file_bytes) == 0:
            raise HTTPException(
                status_code=400,
                detail="Uploaded file is empty"
            )

        # Parse packing list
        parser = get_packing_list_parser_service()
        parsed = parser.parse(file_bytes, file.filename)

        # Convert to response model
        parsed_response = ParsedPackingListResponse(
            pv_number=parsed.pv_number,
            pv_number_confidence=parsed.pv_number_confidence,
            customer_name=parsed.customer_name,
            line_items=[
                PackingListLineItem(
                    product_code=item.product_code,
                    product_name=item.product_name,
                    pallets=item.pallets,
                    cartons=item.cartons,
                    m2_total=str(item.m2_total),
                    net_weight_kg=str(item.net_weight_kg),
                    gross_weight_kg=str(item.gross_weight_kg),
                    volume_m3=str(item.volume_m3),
                    container_number=item.container_number,
                    seal_number=item.seal_number,
                )
                for item in parsed.line_items
            ],
            totals=PackingListTotals(
                total_pallets=parsed.totals.total_pallets,
                total_cartons=parsed.totals.total_cartons,
                total_m2=str(parsed.totals.total_m2),
                total_net_weight_kg=str(parsed.totals.total_net_weight_kg),
                total_gross_weight_kg=str(parsed.totals.total_gross_weight_kg),
                total_volume_m3=str(parsed.totals.total_volume_m3),
            ),
            containers=parsed.containers,
            overall_confidence=parsed.overall_confidence,
            parsing_errors=parsed.parsing_errors,
        )

        # Try to find matching factory order
        factory_order_id = None
        factory_order_pv = None
        action = "parsed_pending_confirmation"

        if parsed.pv_number:
            try:
                factory_order_service = get_factory_order_service()
                factory_order = factory_order_service.get_by_pv_number(parsed.pv_number)

                if factory_order:
                    factory_order_id = factory_order.id
                    factory_order_pv = factory_order.pv_number
                    action = "linked_to_factory_order"
                    logger.info(
                        "packing_list_matched_factory_order",
                        pv_number=parsed.pv_number,
                        factory_order_id=factory_order_id
                    )
                else:
                    action = "needs_factory_order"
                    logger.info(
                        "packing_list_no_factory_order_match",
                        pv_number=parsed.pv_number
                    )
            except Exception as e:
                logger.warning(
                    "factory_order_lookup_failed",
                    pv_number=parsed.pv_number,
                    error=str(e)
                )
                action = "needs_factory_order"

        logger.info(
            "packing_list_parsed_successfully",
            filename=file.filename,
            pv_number=parsed.pv_number,
            line_items=len(parsed.line_items),
            containers=len(parsed.containers),
            total_m2=str(parsed.totals.total_m2),
            factory_order_matched=factory_order_id is not None,
            confidence=parsed.overall_confidence
        )

        message = f"Packing list parsed successfully. "
        if factory_order_id:
            message += f"Matched to factory order {factory_order_pv}."
        elif parsed.pv_number:
            message += f"PV number {parsed.pv_number} found but no matching factory order."
        else:
            message += "No PV number found in document."

        return PackingListIngestResponse(
            success=True,
            message=message,
            action=action,
            parsed_data=parsed_response,
            factory_order_id=factory_order_id,
            factory_order_pv=factory_order_pv,
            confidence=parsed.overall_confidence
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("packing_list_ingest_error", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Error processing packing list: {str(e)}"
        )


@router.post("/packing-list/confirm", response_model=PackingListIngestResponse)
async def confirm_packing_list(data: ConfirmPackingListRequest) -> PackingListIngestResponse:
    """
    Confirm packing list data and wire to shipment.

    Flow:
    1. Find FactoryOrder by PV number
    2. Find Shipment linked to FactoryOrder
    3. Create containers from packing list data
    4. Update shipment status to AT_ORIGIN_PORT
    5. Send appropriate alert

    If no shipment found, sends pending alert for manual linking.
    """
    logger.info(
        "packing_list_confirm_started",
        pv_number=data.pv_number,
        containers=len(data.containers)
    )

    if not data.pv_number:
        raise HTTPException(status_code=400, detail="PV number is required")

    try:
        factory_order_service = get_factory_order_service()
        shipment_service = get_shipment_service()
        alert_service = get_alert_service()

        # Step 1: Find factory order by PV number
        factory_order = factory_order_service.get_by_pv_number(data.pv_number)

        total_m2 = data.totals.total_m2 if data.totals else "0"
        container_count = len(data.containers)

        if not factory_order:
            # No factory order - send pending alert
            logger.info("packing_list_no_factory_order", pv_number=data.pv_number)

            try:
                alert_service.create(
                    AlertCreate(
                        type=AlertType.PACKING_LIST_PENDING,
                        severity=AlertSeverity.WARNING,
                        title=get_message("title_packing_list_pending", pv_number=data.pv_number),
                        message=get_message(
                            "packing_list_pending",
                            pv_number=data.pv_number,
                            container_count=container_count,
                            total_m2=total_m2,
                            reason="Factory order not found"
                        ),
                    ),
                    send_telegram=True
                )
            except Exception as alert_error:
                logger.warning("packing_list_pending_alert_failed", error=str(alert_error))

            return PackingListIngestResponse(
                success=False,
                message=f"Factory order not found for PV: {data.pv_number}",
                action="needs_factory_order",
                confidence=0.0
            )

        # Step 2: Find shipment(s) linked to factory order
        shipments = shipment_service.get_by_factory_order_id(factory_order.id)

        target_shipment = None
        reason = ""

        if len(shipments) == 0:
            reason = "No shipment linked to factory order"
        elif len(shipments) == 1:
            target_shipment = shipments[0]
        else:
            # Multiple shipments - find one at AT_FACTORY status
            at_factory = [
                s for s in shipments
                if s.status == ShipmentStatus.AT_FACTORY.value
            ]
            if len(at_factory) == 1:
                target_shipment = at_factory[0]
            elif len(at_factory) == 0:
                reason = "Multiple shipments but none at AT_FACTORY"
            else:
                reason = "Multiple shipments at AT_FACTORY - manual selection required"

        if not target_shipment:
            # No clear shipment target - send pending alert
            logger.info(
                "packing_list_no_shipment",
                pv_number=data.pv_number,
                factory_order_id=factory_order.id,
                shipment_count=len(shipments),
                reason=reason
            )

            try:
                alert_service.create(
                    AlertCreate(
                        type=AlertType.PACKING_LIST_PENDING,
                        severity=AlertSeverity.WARNING,
                        title=get_message("title_packing_list_pending", pv_number=data.pv_number),
                        message=get_message(
                            "packing_list_pending",
                            pv_number=data.pv_number,
                            container_count=container_count,
                            total_m2=total_m2,
                            reason=reason
                        ),
                    ),
                    send_telegram=True
                )
            except Exception as alert_error:
                logger.warning("packing_list_pending_alert_failed", error=str(alert_error))

            return PackingListIngestResponse(
                success=False,
                message=f"Packing list needs manual linking: {reason}",
                action="needs_factory_order",
                factory_order_id=factory_order.id,
                factory_order_pv=factory_order.pv_number,
                confidence=0.5
            )

        # Step 3: Create containers from packing list
        containers_created = 0
        if data.original_parsed_data:
            containers_created = _create_containers_from_packing_list(
                target_shipment.id,
                data.original_parsed_data
            )
        elif data.containers:
            # Fallback: create basic containers from list
            containers_created = _create_containers_for_shipment(
                target_shipment.id,
                data.containers
            )

        logger.info(
            "packing_list_containers_created",
            shipment_id=target_shipment.id,
            containers_created=containers_created
        )

        # Step 4: Update shipment status to AT_ORIGIN_PORT (with guard)
        current_status = ShipmentStatus(target_shipment.status)
        new_status = ShipmentStatus.AT_ORIGIN_PORT

        if is_valid_shipment_status_transition(current_status, new_status):
            shipment_service.update_status(
                target_shipment.id,
                ShipmentStatusUpdate(status=new_status)
            )
            logger.info(
                "packing_list_status_updated",
                shipment_id=target_shipment.id,
                from_status=current_status.value,
                to_status=new_status.value
            )
        else:
            logger.info(
                "packing_list_status_skipped",
                shipment_id=target_shipment.id,
                current_status=current_status.value,
                reason="Shipment already past AT_ORIGIN_PORT"
            )

        # Step 5: Send success alert
        shp_number = target_shipment.shp_number or target_shipment.booking_number or target_shipment.id[:8]

        try:
            alert_service.create(
                AlertCreate(
                    type=AlertType.PACKING_LIST_PROCESSED,
                    severity=AlertSeverity.INFO,
                    title=get_message("title_packing_list_processed", pv_number=data.pv_number),
                    message=get_message(
                        "packing_list_processed",
                        pv_number=data.pv_number,
                        shp_number=shp_number,
                        container_count=containers_created,
                        total_m2=total_m2
                    ),
                    shipment_id=target_shipment.id,
                ),
                send_telegram=True
            )
        except Exception as alert_error:
            logger.warning("packing_list_processed_alert_failed", error=str(alert_error))

        logger.info(
            "packing_list_confirmed_successfully",
            pv_number=data.pv_number,
            factory_order_id=factory_order.id,
            shipment_id=target_shipment.id,
            containers_created=containers_created
        )

        return PackingListIngestResponse(
            success=True,
            message=f"Linked to {shp_number}. {containers_created} containers created.",
            action="linked_to_factory_order",
            factory_order_id=factory_order.id,
            factory_order_pv=factory_order.pv_number,
            shipment_id=target_shipment.id,
            shipment_shp=shp_number,
            confidence=1.0
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("packing_list_confirm_error", pv_number=data.pv_number, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Error confirming packing list: {str(e)}"
        )