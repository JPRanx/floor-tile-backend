"""
Production Schedule API routes.

See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from decimal import Decimal
import structlog

from models.production_schedule import (
    ProductionScheduleUploadResponse,
    ProductionScheduleResponse,
    ProductionScheduleListResponse,
    UpcomingProductionItem,
    UpcomingProductionResponse,
)
from services.production_schedule_parser_service import get_production_schedule_parser_service
from services.production_schedule_service import get_production_schedule_service
from exceptions import AppError, DatabaseError

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
# UPLOAD & PARSE
# ===================

@router.post("/upload", response_model=ProductionScheduleUploadResponse)
async def upload_production_schedule(
    file: UploadFile = File(..., description="Production schedule PDF file")
):
    """
    Upload and parse a production schedule PDF.

    Uses Claude Vision to extract production data from factory PDFs.
    Automatically matches factory codes to products and saves to database.

    Returns:
        Parsed schedule data with match statistics
    """
    # Validate file type
    if not file.filename or not file.filename.lower().endswith('.pdf'):
        raise HTTPException(
            status_code=400,
            detail="File must be a PDF"
        )

    try:
        # Read file content
        pdf_bytes = await file.read()

        if len(pdf_bytes) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")

        if len(pdf_bytes) > 10 * 1024 * 1024:  # 10MB limit
            raise HTTPException(status_code=400, detail="File too large (max 10MB)")

        logger.info("production_schedule_upload_started", filename=file.filename, size=len(pdf_bytes))

        # Parse PDF with Claude Vision
        parser = get_production_schedule_parser_service()
        parsed_data = await parser.parse_pdf(pdf_bytes, filename=file.filename)

        if not parsed_data.line_items:
            return ProductionScheduleUploadResponse(
                success=False,
                message="No production items could be extracted from the PDF",
                parsed_data=parsed_data,
                items_count=0,
                matched_products=0,
                unmatched_factory_codes=[]
            )

        # Save to database
        service = get_production_schedule_service()
        items_saved, matched, unmatched = service.save_parsed_schedule(
            parsed_data,
            filename=file.filename
        )

        return ProductionScheduleUploadResponse(
            success=True,
            message=f"Successfully parsed and saved {items_saved} production items. {matched} matched to products.",
            parsed_data=parsed_data,
            items_count=items_saved,
            matched_products=matched,
            unmatched_factory_codes=unmatched
        )

    except ValueError as e:
        logger.error("production_schedule_parse_error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return handle_error(e)


# ===================
# QUERY ROUTES
# ===================

@router.get("/upcoming", response_model=UpcomingProductionResponse)
async def get_upcoming_production(
    days_ahead: int = Query(30, ge=1, le=90, description="Days to look ahead"),
    product_id: Optional[str] = Query(None, description="Filter by product UUID"),
    factory_code: Optional[str] = Query(None, description="Filter by factory code")
):
    """
    Get upcoming production within a date range.

    Useful for inventory planning - shows what products will be available soon.

    Returns:
        List of upcoming production items with days until production
    """
    try:
        service = get_production_schedule_service()
        items = service.get_upcoming_production(
            days_ahead=days_ahead,
            product_id=product_id,
            factory_code=factory_code
        )

        # Calculate totals
        total_m2 = sum(
            item.m2_export_first or Decimal("0")
            for item in items
        )

        today = date.today()
        from datetime import timedelta
        end_date = today + timedelta(days=days_ahead)

        return UpcomingProductionResponse(
            data=items,
            total_m2_upcoming=total_m2,
            date_range_start=today,
            date_range_end=end_date
        )

    except Exception as e:
        return handle_error(e)


@router.get("/by-date/{schedule_date}")
async def get_schedule_by_date(
    schedule_date: date,
    plant: Optional[int] = Query(None, ge=1, le=2, description="Filter by plant")
):
    """
    Get all production items for a specific schedule date.

    Args:
        schedule_date: The schedule generation date (YYYY-MM-DD)
        plant: Optional filter by plant (1 or 2)
    """
    try:
        service = get_production_schedule_service()
        items = service.get_by_schedule_date(schedule_date, plant=plant)

        return {
            "schedule_date": schedule_date.isoformat(),
            "plant": plant,
            "data": items,
            "total_items": len(items)
        }

    except Exception as e:
        return handle_error(e)


@router.get("/for-product/{product_id}")
async def get_production_for_product(
    product_id: str,
    include_past: bool = Query(False, description="Include past production dates")
):
    """
    Get production schedule entries for a specific product.

    Useful for seeing when a product will next be produced.
    """
    try:
        service = get_production_schedule_service()
        items = service.get_production_for_product(
            product_id=product_id,
            include_past=include_past
        )

        return {
            "product_id": product_id,
            "data": items,
            "total_items": len(items)
        }

    except Exception as e:
        return handle_error(e)


@router.get("/unmatched")
async def get_unmatched_factory_codes():
    """
    Get factory codes that haven't been matched to products.

    Useful for identifying products that need factory_code set.

    Returns:
        List of unmatched factory codes with their product names and counts
    """
    try:
        service = get_production_schedule_service()
        codes = service.get_unmatched_factory_codes()

        return {
            "unmatched_codes": codes,
            "total_unmatched": len(codes)
        }

    except Exception as e:
        return handle_error(e)


@router.get("/dates")
async def get_schedule_dates(
    limit: int = Query(10, ge=1, le=50, description="Number of dates to return")
):
    """
    Get list of available schedule dates.

    Returns most recent schedules first.
    """
    try:
        service = get_production_schedule_service()
        dates = service.get_schedule_dates(limit=limit)

        return {
            "schedules": dates,
            "total": len(dates)
        }

    except Exception as e:
        return handle_error(e)


@router.post("/rematch")
async def rematch_products():
    """
    Re-match all unmatched schedule items to products.

    Useful after setting factory_code on products.
    Updates product_id for items where factory_code now matches.

    Returns:
        Statistics on how many items were processed and matched
    """
    try:
        service = get_production_schedule_service()
        processed, matched = service.rematch_products()

        return {
            "success": True,
            "message": f"Processed {processed} unmatched items, {matched} newly matched",
            "total_processed": processed,
            "newly_matched": matched
        }

    except Exception as e:
        return handle_error(e)
