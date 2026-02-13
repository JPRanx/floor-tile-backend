"""
Sales API routes.

Handles weekly sales records from owner Excel uploads.
"""

from datetime import date
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from decimal import Decimal

from collections import defaultdict

from models.sales import (
    SalesRecordCreate,
    SalesRecordUpdate,
    SalesRecordResponse,
    SalesListResponse,
    SalesUploadResponse,
    SalesVerification,
    VerificationCheck,
    SalesMismatch,
    SalesPreview,
    SalesPreviewRow,
    SACUploadResponse,
)
from services.sales_service import get_sales_service
from services.product_service import get_product_service
from services import preview_cache_service
from parsers.excel_parser import parse_owner_excel
from parsers.sac_parser import parse_sac_csv
from utils.text_utils import normalize_customer_name, clean_customer_name, normalize_product_name
from exceptions import (
    AppError,
    SalesNotFoundError,
    ExcelParseError,
    SACParseError,
    SACMissingColumnsError,
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


def _parse_sales_file(file_contents: bytes, filename: str | None = None):
    """Parse sales Excel and return (sales_records, warnings, parse_result)."""
    product_service = get_product_service()
    products, _ = product_service.get_all(page=1, page_size=1000, active_only=False)

    known_owner_codes = {
        p.owner_code: p.id for p in products if p.owner_code is not None
    }
    from parsers.excel_parser import _normalize_sku_name
    known_sku_names = {
        _normalize_sku_name(p.sku): p.id for p in products if p.sku
    }

    parse_result = parse_owner_excel(file_contents, known_owner_codes, known_sku_names, filename=filename)

    warnings = []
    if parse_result.errors:
        unknown_products = [e for e in parse_result.errors if "Unknown product" in e.error]
        other_errors = [e for e in parse_result.errors if "Unknown product" not in e.error]

        if unknown_products:
            unique_unknown = set(e.error.split(": ")[1] for e in unknown_products if ": " in e.error)
            warnings = [f"Unknown product: {sku}" for sku in unique_unknown]

        if other_errors:
            raise ExcelParseError(
                message=f"Upload failed with {len(other_errors)} validation errors",
                details={"errors": [e.__dict__ for e in other_errors[:20]]}
            )

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

    return sales_records, warnings, parse_result


@router.post("/upload/preview", response_model=SalesPreview)
async def preview_sales_upload(file: UploadFile = File(...)):
    """Parse sales Excel and return preview. Nothing is saved."""
    try:
        contents = await file.read()
        sales_records, warnings, parse_result = _parse_sales_file(contents, filename=file.filename)

        if not sales_records:
            raise ExcelParseError(
                message="No valid sales records found in file",
                details={}
            )

        row_count = len(sales_records)
        product_count = len(set(r.product_id for r in sales_records))
        total_m2 = sum(float(r.quantity_m2) for r in sales_records)
        dates = [r.week_start for r in sales_records]
        date_range_start = min(dates)
        date_range_end = max(dates)

        # Build reverse lookup for SKU names from parse_result
        pid_to_sku: dict[str, str] = {}
        for r in parse_result.sales:
            if r.product_id and r.product_id not in pid_to_sku:
                pid_to_sku[r.product_id] = r.sku

        sample_rows = [
            SalesPreviewRow(
                sku=pid_to_sku.get(r.product_id, r.product_id[:8]),
                week_start=r.week_start,
                quantity_m2=float(r.quantity_m2),
                customer=r.customer,
            )
            for r in sales_records[:10]
        ]

        preview_id = preview_cache_service.store_preview(sales_records)

        logger.info(
            "sales_preview_created",
            preview_id=preview_id,
            row_count=row_count,
            product_count=product_count,
            total_m2=round(total_m2, 2),
        )

        return SalesPreview(
            preview_id=preview_id,
            row_count=row_count,
            product_count=product_count,
            total_m2=round(total_m2, 2),
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            warnings=warnings,
            sample_rows=sample_rows,
        )

    except (ExcelParseError, AppError) as e:
        return handle_error(e)
    except Exception as e:
        logger.error("sales_preview_failed", error=str(e))
        return handle_error(e)


@router.post("/upload/confirm/{preview_id}", response_model=SalesUploadResponse)
async def confirm_sales_upload(preview_id: str):
    """Save previously previewed sales data."""
    try:
        sales_records = preview_cache_service.retrieve_preview(preview_id)
        if sales_records is None:
            raise HTTPException(status_code=404, detail="Preview expired")

        sales_service = get_sales_service()
        deleted = 0
        min_date = max_date = None

        if sales_records:
            dates = [r.week_start for r in sales_records]
            min_date = min(dates)
            max_date = max(dates)
            deleted = sales_service.delete_by_date_range(min_date, max_date)
            if deleted > 0:
                logger.info("sales_deleted_before_upload", count=deleted)

        created = sales_service.bulk_create(sales_records)

        logger.info(
            "sales_confirm_complete",
            preview_id=preview_id,
            records_created=len(created),
        )

        # --- Inline verification (bridge code — remove with Excel imports) ---
        verification = None
        warnings = []
        if sales_records and min_date and max_date:
            excel_m2_by_product: dict[str, float] = defaultdict(float)
            for r in sales_records:
                excel_m2_by_product[r.product_id] += float(r.quantity_m2)
            excel_total_m2 = sum(excel_m2_by_product.values())
            excel_product_count = len(excel_m2_by_product)

            db = sales_service.db
            db_rows = (
                db.table("sales")
                .select("product_id, quantity_m2")
                .gte("week_start", min_date.isoformat())
                .lte("week_start", max_date.isoformat())
                .execute()
            )
            db_m2_by_product: dict[str, float] = defaultdict(float)
            for row in db_rows.data:
                db_m2_by_product[row["product_id"]] += float(row["quantity_m2"])
            db_total_m2 = sum(db_m2_by_product.values())
            db_row_count = len(db_rows.data)
            db_product_count = len(db_m2_by_product)

            mismatches = []
            all_pids = set(excel_m2_by_product) | set(db_m2_by_product)
            for pid in all_pids:
                e_m2 = excel_m2_by_product.get(pid, 0.0)
                d_m2 = db_m2_by_product.get(pid, 0.0)
                if abs(e_m2 - d_m2) > 0.01:
                    mismatches.append(SalesMismatch(
                        sku=pid[:8],
                        excel_m2=round(e_m2, 2),
                        db_m2=round(d_m2, 2),
                        diff=round(d_m2 - e_m2, 2),
                    ))

            row_match = len(sales_records) == db_row_count
            m2_match = abs(excel_total_m2 - db_total_m2) < 0.01
            prod_match = excel_product_count == db_product_count
            status = "VERIFIED" if (row_match and m2_match and not mismatches) else "MISMATCH"

            verification = SalesVerification(
                status=status,
                row_count=VerificationCheck(excel=len(sales_records), db=db_row_count, match=row_match),
                total_m2=VerificationCheck(excel=round(excel_total_m2, 2), db=round(db_total_m2, 2), match=m2_match),
                products=VerificationCheck(excel=excel_product_count, db=db_product_count, match=prod_match),
                mismatches=mismatches,
            )

            if deleted > 0 and abs(deleted - len(sales_records)) > len(sales_records) * 0.5:
                warnings.append(f"Deleted {deleted} rows but only inserted {len(sales_records)} — check if correct file was uploaded")

        preview_cache_service.delete_preview(preview_id)

        return SalesUploadResponse(
            success=True,
            inserted=len(created),
            deleted=deleted,
            date_range={"start": min_date.isoformat(), "end": max_date.isoformat()} if min_date else None,
            verification=verification,
            warnings=warnings,
        )

    except HTTPException:
        raise
    except (AppError,) as e:
        return handle_error(e)
    except Exception as e:
        logger.error("sales_confirm_failed", error=str(e), preview_id=preview_id)
        return handle_error(e)


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
        contents = await file.read()
        sales_records, warnings, parse_result = _parse_sales_file(contents, filename=file.filename)

        sales_service = get_sales_service()
        deleted = 0
        min_date = max_date = None

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

        # --- Inline verification (bridge code — remove with Excel imports) ---
        verification = None
        warnings = []
        if sales_records and min_date and max_date:
            # Excel-side totals
            excel_m2_by_product: dict[str, float] = defaultdict(float)
            for r in sales_records:
                excel_m2_by_product[r.product_id] += float(r.quantity_m2)
            excel_total_m2 = sum(excel_m2_by_product.values())
            excel_product_count = len(excel_m2_by_product)

            # DB-side totals
            db = sales_service.db
            db_rows = (
                db.table("sales")
                .select("product_id, quantity_m2")
                .gte("week_start", min_date.isoformat())
                .lte("week_start", max_date.isoformat())
                .execute()
            )
            db_m2_by_product: dict[str, float] = defaultdict(float)
            for row in db_rows.data:
                db_m2_by_product[row["product_id"]] += float(row["quantity_m2"])
            db_total_m2 = sum(db_m2_by_product.values())
            db_row_count = len(db_rows.data)
            db_product_count = len(db_m2_by_product)

            # Per-product mismatches
            mismatches = []
            all_pids = set(excel_m2_by_product) | set(db_m2_by_product)
            # Build reverse lookup: product_id -> sku
            pid_to_sku = {}
            for r in parse_result.sales:
                if r.product_id and r.product_id not in pid_to_sku:
                    pid_to_sku[r.product_id] = r.sku
            for pid in all_pids:
                e_m2 = excel_m2_by_product.get(pid, 0.0)
                d_m2 = db_m2_by_product.get(pid, 0.0)
                if abs(e_m2 - d_m2) > 0.01:
                    mismatches.append(SalesMismatch(
                        sku=pid_to_sku.get(pid, pid[:8]),
                        excel_m2=round(e_m2, 2),
                        db_m2=round(d_m2, 2),
                        diff=round(d_m2 - e_m2, 2),
                    ))

            row_match = len(sales_records) == db_row_count
            m2_match = abs(excel_total_m2 - db_total_m2) < 0.01
            prod_match = excel_product_count == db_product_count
            status = "VERIFIED" if (row_match and m2_match and not mismatches) else "MISMATCH"

            verification = SalesVerification(
                status=status,
                row_count=VerificationCheck(excel=len(sales_records), db=db_row_count, match=row_match),
                total_m2=VerificationCheck(excel=round(excel_total_m2, 2), db=round(db_total_m2, 2), match=m2_match),
                products=VerificationCheck(excel=excel_product_count, db=db_product_count, match=prod_match),
                mismatches=mismatches,
            )

            if deleted > 0 and abs(deleted - len(sales_records)) > len(sales_records) * 0.5:
                warnings.append(f"Deleted {deleted} rows but only inserted {len(sales_records)} — check if correct file was uploaded")

        return SalesUploadResponse(
            success=True,
            inserted=len(created),
            deleted=deleted,
            date_range={"start": min_date.isoformat(), "end": max_date.isoformat()} if min_date else None,
            verification=verification,
            warnings=warnings,
        )

    except (ExcelParseError, AppError) as e:
        return handle_error(e)
    except Exception as e:
        logger.error("sales_upload_failed", error=str(e))
        return handle_error(e)


@router.post("/upload-sac", response_model=SACUploadResponse)
async def upload_sac_sales(file: UploadFile = File(...)):
    """
    Upload daily sales data from SAC CSV export.

    Parses SAC (Guatemala ERP) CSV and creates sales records.
    Products are matched by sac_sku first, then by normalized name.

    Features:
    - Idempotent: Deletes existing records in the uploaded date range
    - Partial success: Creates records for matched products, logs unmatched
    - Statistics: Returns match rates and unmatched product list

    Returns:
        SACUploadResponse with created count and statistics
    """
    try:
        # Get products with sac_sku and name mappings
        product_service = get_product_service()
        products, _ = product_service.get_all(page=1, page_size=1000, active_only=False)

        # Build lookup: sac_sku (int) -> product_id
        known_sac_skus = {
            p.sac_sku: p.id
            for p in products
            if p.sac_sku is not None
        }

        # Build lookup: normalized product name -> product_id
        known_product_names = {
            normalize_product_name(p.sku): p.id
            for p in products
            if p.sku
        }

        # Parse CSV file
        contents = await file.read()
        parse_result = parse_sac_csv(contents, known_sac_skus, known_product_names)

        # Log unmatched products
        if parse_result.unmatched_products:
            logger.warning(
                "sac_upload_unmatched_products",
                count=len(parse_result.unmatched_products),
                sample=list(parse_result.unmatched_products)[:10]
            )

        # Convert parsed records to SalesRecordCreate
        sales_records = [
            SalesRecordCreate(
                product_id=record.product_id,
                week_start=record.sale_date,
                quantity_m2=record.quantity_m2,
                customer=record.customer,
                customer_normalized=record.customer_normalized,
                unit_price_usd=record.unit_price_usd,
                total_price_usd=record.total_price_usd,
            )
            for record in parse_result.sales
        ]

        sales_service = get_sales_service()
        deleted = 0

        if sales_records:
            # Make upload idempotent: delete existing records in date range
            min_date, max_date = parse_result.date_range
            if min_date and max_date:
                deleted = sales_service.delete_by_date_range(min_date, max_date)
                if deleted > 0:
                    logger.info("sac_sales_deleted_before_upload", count=deleted)

        # Bulk create
        created = sales_service.bulk_create(sales_records)

        logger.info(
            "sac_upload_complete",
            records_created=len(created),
            records_deleted=deleted,
            match_rate=f"{parse_result.match_rate:.1f}%"
        )

        return SACUploadResponse(
            created=len(created),
            deleted=deleted,
            total_rows=parse_result.total_rows,
            matched_by_sac_sku=parse_result.matched_by_sac_sku,
            matched_by_name=parse_result.matched_by_name,
            unmatched_count=len(parse_result.unmatched_products),
            match_rate_pct=round(parse_result.match_rate, 1),
            date_range_start=parse_result.date_range[0],
            date_range_end=parse_result.date_range[1],
            # Summary statistics
            total_m2_sold=float(parse_result.total_m2_sold),
            unique_customers=len(parse_result.unique_customers),
            unique_products=len(parse_result.unique_products),
            top_product=parse_result.top_product,
            skipped_non_tile=parse_result.skipped_non_tile,
            skipped_products=list(parse_result.skipped_products)[:10],
            unmatched_products=list(parse_result.unmatched_products)[:20],
            errors=[e.__dict__ for e in parse_result.errors[:20]],
        )

    except (SACParseError, SACMissingColumnsError, AppError) as e:
        return handle_error(e)
    except Exception as e:
        logger.error("sac_upload_failed", error=str(e))
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
