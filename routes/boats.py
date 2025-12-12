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
import structlog

from models.boat_schedule import (
    BoatScheduleCreate,
    BoatScheduleUpdate,
    BoatScheduleStatusUpdate,
    BoatScheduleResponse,
    BoatScheduleListResponse,
    BoatUploadResult,
)
from services.boat_schedule_service import get_boat_schedule_service
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
