"""
Sales API routes.

Handles weekly sales records from owner Excel uploads.
"""

from datetime import date
from fastapi import APIRouter, Query, UploadFile, File
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.sales import (
    SalesRecordCreate,
    SalesRecordUpdate,
    SalesRecordResponse,
    SalesListResponse,
    SalesUploadResponse,
)
from services.sales_service import get_sales_service
from services.product_service import get_product_service
from parsers.excel_parser import parse_owner_excel
from utils.text_utils import normalize_customer_name, clean_customer_name
from exceptions import (
    AppError,
    SalesNotFoundError,
    ExcelParseError,
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
# UPLOAD ROUTES
# ===================

@router.post("/upload", response_model=SalesUploadResponse)
async def upload_sales(file: UploadFile = File(...)):
    """
    Upload sales data from Excel file.

    Parses VENTAS sheet and creates sales records.
    Rejects entire upload if any SKU is invalid.

    Returns:
        SalesUploadResponse with created records
    """
    try:
        # Get products with owner_code mappings
        product_service = get_product_service()
        products, _ = product_service.get_all(page=1, page_size=1000, active_only=False)

        # Build lookup: owner_code -> product_id
        # Excel SKU column contains owner codes (102, 119, etc.)
        known_owner_codes = {
            p.owner_code: p.id
            for p in products
            if p.owner_code is not None
        }

        # Build lookup: normalized SKU name -> product_id
        # For Excel files that use product names instead of codes
        from parsers.excel_parser import _normalize_sku_name
        known_sku_names = {
            _normalize_sku_name(p.sku): p.id
            for p in products
            if p.sku
        }

        # Parse Excel file
        contents = await file.read()
        parse_result = parse_owner_excel(contents, known_owner_codes, known_sku_names)

        # Log errors but continue with valid records (partial success)
        if parse_result.errors:
            # Group errors by type for logging
            unknown_products = [e for e in parse_result.errors if "Unknown product" in e.error]
            other_errors = [e for e in parse_result.errors if "Unknown product" not in e.error]

            if unknown_products:
                unique_unknown = set(e.error.split(": ")[1] for e in unknown_products if ": " in e.error)
                logger.warning(
                    "sales_upload_unknown_products",
                    count=len(unknown_products),
                    unique_products=len(unique_unknown),
                    sample=list(unique_unknown)[:5]
                )

            # Only reject if there are non-product errors (validation errors)
            if other_errors:
                raise ExcelParseError(
                    message=f"Upload failed with {len(other_errors)} validation errors",
                    details={"errors": [e.__dict__ for e in other_errors[:20]]}
                )

        # Convert parsed sales records to SalesRecordCreate
        # product_id already resolved by parser
        # customer name is cleaned and normalized for grouping
        sales_records = [
            SalesRecordCreate(
                product_id=record.product_id,
                week_start=record.sale_date,
                quantity_m2=record.quantity,
                customer=clean_customer_name(record.customer),
                customer_normalized=normalize_customer_name(record.customer),
            )
            for record in parse_result.sales
        ]

        sales_service = get_sales_service()

        if sales_records:
            # Make upload idempotent: delete existing records in date range
            dates = [r.week_start for r in sales_records]
            min_date = min(dates)
            max_date = max(dates)
            deleted = sales_service.delete_by_date_range(min_date, max_date)
            if deleted > 0:
                logger.info("sales_deleted_before_upload", count=deleted)

        # Bulk create
        created = sales_service.bulk_create(sales_records)

        logger.info(
            "sales_upload_complete",
            records_created=len(created)
        )

        return SalesUploadResponse(
            created=len(created),
            records=created
        )

    except (ExcelParseError, AppError) as e:
        return handle_error(e)
    except Exception as e:
        logger.error("sales_upload_failed", error=str(e))
        return handle_error(e)


# ===================
# READ ROUTES
# ===================

@router.get("/history/{product_id}")
async def get_sales_history(
    product_id: str,
    limit: int = Query(52, ge=1, le=260, description="Weeks to return")
):
    """
    Get sales history for a product.

    Default returns last 52 weeks (1 year).
    """
    try:
        service = get_sales_service()
        records = service.get_history(product_id, limit)

        total_m2 = sum(r.quantity_m2 for r in records)

        return {
            "product_id": product_id,
            "records": records,
            "total_m2": float(total_m2),
            "weeks_count": len(records)
        }

    except Exception as e:
        return handle_error(e)


@router.get("/weekly/{week_start}")
async def get_weekly_sales(week_start: date):
    """Get all sales for a specific week."""
    try:
        service = get_sales_service()
        records = service.get_weekly_totals(week_start)

        total_m2 = sum(r.quantity_m2 for r in records)

        return {
            "week_start": week_start.isoformat(),
            "records": records,
            "total_m2": float(total_m2),
            "products_count": len(records)
        }

    except Exception as e:
        return handle_error(e)


@router.get("", response_model=SalesListResponse)
async def list_sales(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    product_id: Optional[str] = Query(None, description="Filter by product"),
    week_start: Optional[date] = Query(None, description="Filter by week")
):
    """
    List all sales records with optional filters.

    Returns paginated list of sales records.
    """
    try:
        service = get_sales_service()

        records, total = service.get_all(
            page=page,
            page_size=page_size,
            product_id=product_id,
            week_start=week_start
        )

        total_pages = (total + page_size - 1) // page_size

        return SalesListResponse(
            data=records,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        return handle_error(e)


@router.get("/{record_id}", response_model=SalesRecordResponse)
async def get_sales_record(record_id: str):
    """
    Get a single sales record by ID.

    Raises:
        404: Record not found
    """
    try:
        service = get_sales_service()
        return service.get_by_id(record_id)

    except SalesNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# WRITE ROUTES
# ===================

@router.post("", response_model=SalesRecordResponse, status_code=201)
async def create_sales_record(data: SalesRecordCreate):
    """
    Create a new sales record.

    Raises:
        422: Validation error
    """
    try:
        service = get_sales_service()
        return service.create(data)

    except Exception as e:
        return handle_error(e)


@router.patch("/{record_id}", response_model=SalesRecordResponse)
async def update_sales_record(record_id: str, data: SalesRecordUpdate):
    """
    Update an existing sales record.

    Raises:
        404: Record not found
        422: Validation error
    """
    try:
        service = get_sales_service()
        return service.update(record_id, data)

    except SalesNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.delete("/{record_id}", status_code=204)
async def delete_sales_record(record_id: str):
    """
    Delete a sales record.

    Raises:
        404: Record not found
    """
    try:
        service = get_sales_service()
        service.delete(record_id)
        return None

    except SalesNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# UTILITY ROUTES
# ===================

@router.get("/count/total")
async def count_sales(
    product_id: Optional[str] = Query(None, description="Filter by product")
):
    """Get total sales record count."""
    try:
        service = get_sales_service()
        count = service.count(product_id=product_id)
        return {"count": count}

    except Exception as e:
        return handle_error(e)
