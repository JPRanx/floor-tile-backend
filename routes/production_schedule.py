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
    # Order Builder integration
    UploadResult,
    MapProductRequest,
    MapProductResponse,
    UnmappedProduct,
    ProductFactoryStatus,
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


# ===================
# ORDER BUILDER INTEGRATION
# ===================

@router.post("/upload-replace", response_model=UploadResult)
async def upload_and_replace_schedule(
    file: UploadFile = File(..., description="Production schedule PDF file")
):
    """
    Upload a production schedule PDF, wiping and replacing all existing data.

    This is the preferred method for daily uploads where the new PDF
    represents the complete current state.

    Returns:
        Upload result with matched/unmatched counts and fuzzy suggestions
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

        logger.info(
            "production_schedule_upload_replace_started",
            filename=file.filename,
            size=len(pdf_bytes)
        )

        # Parse PDF with Claude Vision
        parser = get_production_schedule_parser_service()
        parsed_data = await parser.parse_pdf(pdf_bytes, filename=file.filename)

        if not parsed_data.line_items:
            raise HTTPException(
                status_code=400,
                detail="No production items could be extracted from the PDF"
            )

        # Wipe and replace
        service = get_production_schedule_service()
        result = service.wipe_and_replace(parsed_data, filename=file.filename)

        logger.info(
            "production_schedule_upload_replace_completed",
            total_rows=result.total_rows,
            matched=result.matched_count,
            unmatched=result.unmatched_count
        )

        return result

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("production_schedule_parse_error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return handle_error(e)


@router.post("/map-product", response_model=MapProductResponse)
async def map_factory_code_to_product(request: MapProductRequest):
    """
    Map a factory code to a product.

    Updates the product's factory_code and links all schedule rows
    with that factory code to the product.

    Use this after upload to resolve unmatched products.
    """
    try:
        service = get_production_schedule_service()

        # Get product SKU first
        from services.product_service import get_product_service
        product_service = get_product_service()
        product = product_service.get_by_id(request.product_id)

        if not product:
            raise HTTPException(
                status_code=404,
                detail=f"Product not found: {request.product_id}"
            )

        rows_updated = service.map_factory_code_to_product(
            factory_code=request.factory_code,
            product_id=request.product_id
        )

        return MapProductResponse(
            factory_code=request.factory_code,
            product_id=request.product_id,
            product_sku=product.sku,
            rows_updated=rows_updated
        )

    except HTTPException:
        raise
    except Exception as e:
        return handle_error(e)


@router.get("/unmapped-with-suggestions", response_model=list[UnmappedProduct])
async def get_unmapped_with_suggestions():
    """
    Get unmatched factory codes with fuzzy match suggestions.

    Returns products from the current schedule that couldn't be matched,
    along with suggested product matches based on name similarity.
    """
    try:
        service = get_production_schedule_service()

        # Get unmatched codes
        unmatched_codes_raw = service.get_unmatched_factory_codes()
        if not unmatched_codes_raw:
            return []

        # Get all products for fuzzy matching
        from services.product_service import get_product_service
        product_service = get_product_service()
        all_products = product_service.get_all_active()

        # Build UnmappedProduct with suggestions
        result = []
        for item in unmatched_codes_raw:
            factory_code = item["factory_code"]
            factory_name = item.get("product_name", factory_code)
            count = item.get("count", 1)

            suggestions = service._get_fuzzy_suggestions(
                factory_name,
                all_products,
                limit=3
            )

            result.append(UnmappedProduct(
                factory_code=factory_code,
                factory_name=factory_name,
                total_m2=Decimal("0"),  # Not available from this query
                production_dates=[],
                row_count=count,
                suggested_matches=suggestions
            ))

        return result

    except Exception as e:
        return handle_error(e)


@router.post("/factory-status", response_model=dict[str, ProductFactoryStatus])
async def get_factory_status(
    product_ids: list[str],
    boat_departure: date = Query(..., description="Boat departure date for timing assessment"),
    buffer_days: int = Query(3, ge=0, le=14, description="Days buffer before boat")
):
    """
    Get factory production status for Order Builder.

    Returns status for each product indicating whether it's in production
    and whether it will be ready before the boat departs.

    Used by Order Builder to display factory availability.
    """
    try:
        if not product_ids:
            return {}

        service = get_production_schedule_service()
        status_map = service.get_factory_status(
            product_ids=product_ids,
            boat_departure=boat_departure,
            buffer_days=buffer_days
        )

        return status_map

    except Exception as e:
        return handle_error(e)
