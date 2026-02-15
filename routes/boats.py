"""
Boat schedule API routes.

See PHASE_2_BLUEPRINT.md for endpoint specifications.
See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query, UploadFile, File
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from io import BytesIO
import hashlib
import structlog

from models.boat_schedule import (
    BoatScheduleCreate,
    BoatScheduleUpdate,
    BoatScheduleStatusUpdate,
    BoatScheduleResponse,
    BoatScheduleListResponse,
    BoatUploadResult,
    BoatPreview,
    BoatPreviewRow,
)
from services.boat_schedule_service import get_boat_schedule_service
from parsers.tiba_parser import parse_tiba_excel
from services import preview_cache_service
from services.upload_history_service import get_upload_history_service
from exceptions import (
    AppError,
    BoatScheduleNotFoundError,
    BoatScheduleUploadError,
)

logger = structlog.get_logger(__name__)

router = APIRouter()


# ===================
# EXCEPTION HANDLER
# ===================

def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
    # Unexpected error
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


# ===================
# ROUTES
# ===================

@router.get("", response_model=BoatScheduleListResponse)
async def get_boat_schedules(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: Optional[str] = Query(None, description="Filter by status"),
    from_date: Optional[date] = Query(None, description="Departures after this date"),
    to_date: Optional[date] = Query(None, description="Departures before this date"),
):
    """
    List boat schedules with optional filters.

    Query parameters:
    - status: Filter by status (available, booked, departed, arrived)
    - from_date: Only departures after this date
    - to_date: Only departures before this date
    """
    try:
        service = get_boat_schedule_service()
        schedules, total = service.get_all(
            page=page,
            page_size=page_size,
            status=status,
            from_date=from_date,
            to_date=to_date,
        )

        total_pages = (total + page_size - 1) // page_size

        return BoatScheduleListResponse(
            data=schedules,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    except Exception as e:
        return handle_error(e)


@router.get("/available", response_model=list[BoatScheduleResponse])
async def get_available_boats(
    from_date: Optional[date] = Query(None, description="Departures after this date"),
    limit: int = Query(10, ge=1, le=50, description="Maximum results"),
):
    """
    Get available boat schedules for booking.

    Returns schedules with status='available' ordered by departure date.
    """
    try:
        service = get_boat_schedule_service()
        schedules = service.get_available(from_date=from_date, limit=limit)
        return schedules

    except Exception as e:
        return handle_error(e)


@router.get("/next", response_model=Optional[BoatScheduleResponse])
async def get_next_boat():
    """
    Get the next available boat schedule.

    Used for dashboard widget "Next boat departing..."
    """
    try:
        service = get_boat_schedule_service()
        schedule = service.get_next_available()
        return schedule

    except Exception as e:
        return handle_error(e)


@router.get("/{schedule_id}", response_model=BoatScheduleResponse)
async def get_boat_schedule(schedule_id: str):
    """
    Get a single boat schedule by ID.
    """
    try:
        service = get_boat_schedule_service()
        schedule = service.get_by_id(schedule_id)
        return schedule

    except BoatScheduleNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.post("", response_model=BoatScheduleResponse, status_code=201)
async def create_boat_schedule(data: BoatScheduleCreate):
    """
    Create a new boat schedule manually.

    Prefer using /upload for bulk import from TIBA Excel.
    """
    try:
        service = get_boat_schedule_service()
        schedule = service.create(data)
        return schedule

    except Exception as e:
        return handle_error(e)


@router.post("/upload/preview", response_model=BoatPreview)
async def preview_boat_upload(file: UploadFile = File(...)):
    """Parse TIBA Excel and return preview. Nothing is saved."""
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "INVALID_FILE_TYPE",
                        "message": "File must be an Excel file (.xlsx or .xls)"
                    }
                }
            )

        # Read file content
        content = await file.read()
        file_hash = hashlib.sha256(content).hexdigest()
        file_bytes = BytesIO(content)

        # Check for duplicate upload
        boat_duplicate = get_upload_history_service().check_duplicate("boats", file_hash)

        # Parse Excel
        parse_result = parse_tiba_excel(file_bytes)

        if not parse_result.success:
            # Fatal parse errors
            errors = [
                f"Row {e.row}: {e.error}"
                for e in parse_result.errors
            ]
            raise BoatScheduleUploadError(errors)

        # Convert skipped rows
        skipped_rows = [
            {"row": s.row, "reason": s.reason}
            for s in parse_result.skipped_rows
        ]

        # Calculate preview stats
        service = get_boat_schedule_service()
        total_rows = len(parse_result.schedules)
        new_boats = 0
        updated_boats = 0
        skipped_boats = 0
        sample_rows = []
        warnings = []

        # Check current date to detect past departures
        from datetime import date as date_type
        today = date_type.today()

        for record in parse_result.schedules:
            # Check if exists
            existing = service._find_existing(
                record.departure_date,
                record.vessel_name
            )

            if existing:
                # Check if needs update
                if service._needs_update(existing, record):
                    action = "update"
                    updated_boats += 1
                else:
                    action = "skip"
                    skipped_boats += 1
            else:
                action = "new"
                new_boats += 1

            # Add to sample rows
            sample_rows.append(BoatPreviewRow(
                vessel_name=record.vessel_name,
                departure_date=record.departure_date,
                arrival_date=record.arrival_date,
                transit_days=record.transit_days,
                origin_port=record.origin_port,
                destination_port=record.destination_port,
                action=action,
            ))

            # Check for past departures
            if record.departure_date < today:
                warnings.append(
                    f"Boat departing {record.departure_date} has already passed"
                )

        # Calculate date range
        if parse_result.schedules:
            dates = [s.departure_date for s in parse_result.schedules]
            earliest_departure = min(dates)
            latest_departure = max(dates)
        else:
            earliest_departure = None
            latest_departure = None

        if boat_duplicate:
            warnings.insert(0, f"Este archivo ya fue subido el {boat_duplicate['uploaded_at'][:10]} ({boat_duplicate['filename']})")

        # Cache the file bytes for confirm
        cache_data = {
            "file_bytes": content,
            "filename": file.filename,
            "file_hash": file_hash,
            "upload_type": "boats",
        }
        preview_id = preview_cache_service.store_preview(cache_data)

        logger.info(
            "boat_preview_created",
            preview_id=preview_id,
            total_rows=total_rows,
            new_boats=new_boats,
            updated_boats=updated_boats,
            skipped_boats=skipped_boats,
        )

        return BoatPreview(
            preview_id=preview_id,
            total_rows=total_rows,
            new_boats=new_boats,
            updated_boats=updated_boats,
            skipped_boats=skipped_boats,
            earliest_departure=earliest_departure,
            latest_departure=latest_departure,
            skipped_rows=skipped_rows,
            warnings=warnings[:10],  # Limit warnings
            sample_rows=sample_rows,
            expires_in_minutes=30,
        )

    except BoatScheduleUploadError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.post("/upload/confirm/{preview_id}", response_model=BoatUploadResult)
async def confirm_boat_upload(preview_id: str):
    """Save previously previewed boat data."""
    try:
        # Retrieve cached data
        cached = preview_cache_service.retrieve_preview(preview_id)
        if cached is None:
            return JSONResponse(
                status_code=404,
                content={
                    "error": {
                        "code": "PREVIEW_EXPIRED",
                        "message": "Preview has expired. Please upload the file again."
                    }
                }
            )

        # Extract file data
        file_bytes = cached["file_bytes"]
        filename = cached["filename"]

        # Re-process the file (ensures fresh DB state for upsert logic)
        file_io = BytesIO(file_bytes)
        service = get_boat_schedule_service()
        result = service.import_from_excel(file_io, filename)

        logger.info(
            "boat_confirm_complete",
            preview_id=preview_id,
            imported=result.imported,
            updated=result.updated,
            skipped=result.skipped,
        )

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type=cached.get("upload_type", "boats"),
            file_hash=cached.get("file_hash", ""),
            filename=cached.get("filename", "unknown"),
            row_count=result.imported + result.updated,
        )

        # Delete preview from cache
        preview_cache_service.delete_preview(preview_id)

        return result

    except Exception as e:
        return handle_error(e)


@router.post("/upload", response_model=BoatUploadResult)
async def upload_boat_schedules(file: UploadFile = File(...)):
    """
    Import boat schedules from TIBA Excel file.

    Parses the TABLA DE BOOKING sheet.
    Performs upsert: updates existing schedules, inserts new ones.

    Returns:
        imported: Number of new schedules added
        updated: Number of existing schedules updated
        skipped: Number of unchanged schedules
        errors: List of error messages
    """
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "INVALID_FILE_TYPE",
                        "message": "File must be an Excel file (.xlsx or .xls)"
                    }
                }
            )

        # Read file content
        content = await file.read()
        file_bytes = BytesIO(content)

        # Import schedules
        service = get_boat_schedule_service()
        result = service.import_from_excel(file_bytes, file.filename)

        return result

    except BoatScheduleUploadError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{schedule_id}", response_model=BoatScheduleResponse)
async def update_boat_schedule(schedule_id: str, data: BoatScheduleUpdate):
    """
    Update a boat schedule.

    Only provided fields are updated.
    """
    try:
        service = get_boat_schedule_service()
        schedule = service.update(schedule_id, data)
        return schedule

    except BoatScheduleNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{schedule_id}/status", response_model=BoatScheduleResponse)
async def update_boat_status(schedule_id: str, data: BoatScheduleStatusUpdate):
    """
    Update only the status of a boat schedule.

    Valid status transitions:
    - available -> booked (when order confirmed)
    - booked -> departed (when ship leaves)
    - departed -> arrived (when ship arrives at port)
    """
    try:
        service = get_boat_schedule_service()
        schedule = service.update_status(schedule_id, data)
        return schedule

    except BoatScheduleNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.delete("/{schedule_id}", status_code=204)
async def delete_boat_schedule(schedule_id: str):
    """
    Delete a boat schedule.

    Use with caution - prefer updating status instead.
    """
    try:
        service = get_boat_schedule_service()
        service.delete(schedule_id)
        return None

    except BoatScheduleNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)
