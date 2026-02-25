"""
Production Schedule API routes.

See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from decimal import Decimal
import hashlib
import structlog

from typing import Union
from collections import defaultdict
from models.production_schedule import (
    ProductionScheduleUploadResponse,
    ProductionScheduleResponse,
    ProductionScheduleListResponse,
    UpcomingProductionItem,
    UpcomingProductionResponse,
    # Order Builder integration
    UploadResult,
    ProductionImportResult,
    MapProductRequest,
    MapProductResponse,
    UnmappedProduct,
    ProductFactoryStatus,
    # Preview models
    ProductionPreview,
    ProductionPreviewRow,
    ProductionConfirmRequest,
    ProductionScheduleCreate,
)
from services.production_schedule_parser_service import get_production_schedule_parser_service
from services.production_schedule_service import get_production_schedule_service
from services import preview_cache_service
from services.upload_history_service import get_upload_history_service
from services.product_service import get_product_service
from services.inventory_ledger_service import get_ledger_service
from pydantic import BaseModel
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

def _validate_file_type(filename: str) -> str:
    """Validate file type and return 'pdf' or 'excel'."""
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    lower = filename.lower()
    if lower.endswith('.pdf'):
        return 'pdf'
    if lower.endswith('.xlsx') or lower.endswith('.xls'):
        return 'excel'
    raise HTTPException(
        status_code=400,
        detail="File must be PDF (.pdf) or Excel (.xlsx, .xls)"
    )


@router.post("/upload", response_model=ProductionScheduleUploadResponse)
async def upload_production_schedule(
    file: UploadFile = File(..., description="Production schedule PDF or Excel file")
):
    """
    Upload and parse a production schedule PDF or Excel file.

    Uses Claude Vision for PDFs or pandas for Excel files.
    Automatically matches factory codes to products and saves to database.

    Returns:
        Parsed schedule data with match statistics
    """
    # Validate file type
    file_type = _validate_file_type(file.filename)

    try:
        # Read file content
        file_bytes = await file.read()

        if len(file_bytes) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")

        if len(file_bytes) > 10 * 1024 * 1024:  # 10MB limit
            raise HTTPException(status_code=400, detail="File too large (max 10MB)")

        logger.info(
            "production_schedule_upload_started",
            filename=file.filename,
            size=len(file_bytes),
            file_type=file_type
        )

        # Parse based on file type
        parser = get_production_schedule_parser_service()
        if file_type == 'excel':
            parsed_data, _ = await parser.parse_excel(file_bytes, filename=file.filename)
        else:
            parsed_data = await parser.parse_pdf(file_bytes, filename=file.filename)

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

@router.post("/upload-replace/preview", response_model=ProductionPreview)
async def preview_production_upload(
    file: UploadFile = File(..., description="Production schedule Excel file")
):
    """Parse production schedule Excel and return preview. Nothing is saved."""
    # Validate file type
    file_type = _validate_file_type(file.filename)

    # Excel only for preview
    if file_type != 'excel':
        raise HTTPException(
            status_code=400,
            detail="Preview only supports Excel files (.xlsx, .xls). PDF uploads not supported."
        )

    try:
        # Read file content
        file_bytes = await file.read()
        prod_file_hash = hashlib.sha256(file_bytes).hexdigest()

        # Check for duplicate upload
        prod_duplicate = get_upload_history_service().check_duplicate("production_schedule", prod_file_hash)

        if len(file_bytes) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")

        if len(file_bytes) > 10 * 1024 * 1024:  # 10MB limit
            raise HTTPException(status_code=400, detail="File too large (max 10MB)")

        logger.info(
            "production_schedule_preview_started",
            filename=file.filename,
            size=len(file_bytes)
        )

        # Parse Excel
        parser = get_production_schedule_parser_service()
        parsed_data, production_records = await parser.parse_excel(
            file_bytes, filename=file.filename
        )

        if not production_records:
            raise HTTPException(
                status_code=400,
                detail="No production items could be extracted from the Excel"
            )

        # Get current record count (will be deleted on confirm)
        from config import get_supabase_client
        db = get_supabase_client()
        existing = db.table("production_schedule").select("id", count="exact").execute()
        existing_count = existing.count or 0

        # Count matched vs unmatched
        matched_count = sum(1 for r in production_records if r.product_id)
        unmatched_count = len(production_records) - matched_count
        unmatched_referencias = [
            r.referencia for r in production_records
            if not r.product_id
        ]

        # Build status breakdown
        status_breakdown = {
            "scheduled": 0,
            "in_progress": 0,
            "completed": 0
        }
        for r in production_records:
            status_breakdown[r.status] = status_breakdown.get(r.status, 0) + 1

        # Calculate totals
        total_requested_m2 = sum(r.requested_m2 for r in production_records)
        total_completed_m2 = sum(r.completed_m2 for r in production_records)

        # Build sample rows (first 15) for backward compat
        sample_rows = [
            ProductionPreviewRow(
                referencia=r.referencia,
                sku=r.sku,
                plant=r.plant,
                requested_m2=r.requested_m2,
                completed_m2=r.completed_m2,
                status=r.status,
                estimated_delivery_date=r.estimated_delivery_date
            )
            for r in production_records[:15]
        ]

        # Build ALL rows for inline editing
        all_rows = [
            ProductionPreviewRow(
                referencia=r.referencia,
                sku=r.sku,
                plant=r.plant,
                requested_m2=r.requested_m2,
                completed_m2=r.completed_m2,
                status=r.status,
                estimated_delivery_date=r.estimated_delivery_date
            )
            for r in production_records
        ]

        # Determine source month
        source_month = production_records[0].source_month if production_records else "Unknown"

        # Build warnings
        warnings = []
        if prod_duplicate:
            warnings.append(f"Este archivo ya fue subido el {prod_duplicate['uploaded_at'][:10]} ({prod_duplicate['filename']})")
        if unmatched_count > 0:
            warnings.append(f"{unmatched_count} items could not be matched to products")
        if existing_count > 0:
            warnings.append(f"Upload will replace {existing_count} existing schedule items")

        # Store in cache (include unmatched refs for manual resolution)
        preview_data = {
            "file_bytes": file_bytes,
            "filename": file.filename,
            "file_type": file_type,
            "file_hash": prod_file_hash,
            "upload_type": "production_schedule",
            "unmatched_referencias": unmatched_referencias,
        }
        preview_id = preview_cache_service.store_preview(preview_data)

        logger.info(
            "production_preview_created",
            preview_id=preview_id,
            total_rows=len(production_records),
            matched=matched_count,
            unmatched=unmatched_count
        )

        return ProductionPreview(
            preview_id=preview_id,
            filename=file.filename,
            source_month=source_month,
            total_rows=len(production_records),
            rows_with_data=len(production_records),
            matched_to_products=matched_count,
            unmatched_count=unmatched_count,
            unmatched_referencias=unmatched_referencias,
            total_requested_m2=total_requested_m2,
            total_completed_m2=total_completed_m2,
            status_breakdown=status_breakdown,
            existing_records_to_delete=existing_count,
            warnings=warnings,
            sample_rows=sample_rows,
            rows=all_rows,
            expires_in_minutes=30
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("production_schedule_preview_error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return handle_error(e)


# ===================
# PIGGYBACK PRESERVATION HELPERS
# ===================


def _snapshot_piggybacks_before_wipe(db) -> dict:
    """
    Before wiping production_schedule, capture data that would be lost:
    1. piggyback_history rows (CASCADE-deleted when production_schedule is wiped)
    2. ORDER_BUILDER source rows (not in factory Excel)
    3. Total piggyback delta per product_id (to add back to incoming records)

    Returns a dict with snapshot data for use after the wipe.
    """
    snapshot = {
        "piggyback_rows": [],
        "piggyback_totals": defaultdict(float),
        "ob_rows_to_preserve": [],
    }

    # 1. Snapshot piggyback_history rows
    try:
        ph_result = db.table("piggyback_history") \
            .select("production_schedule_id, product_id, additional_m2, source, created_at, notes") \
            .execute()
        if ph_result.data:
            snapshot["piggyback_rows"] = ph_result.data
            for h in ph_result.data:
                snapshot["piggyback_totals"][h["product_id"]] += float(h["additional_m2"])
            logger.info(
                "piggyback_snapshot_taken",
                history_rows=len(ph_result.data),
                products_with_piggyback=len(snapshot["piggyback_totals"]),
            )
    except Exception as e:
        logger.warning("piggyback_snapshot_failed", error=str(e))

    # 2. Snapshot ORDER_BUILDER rows
    try:
        ob_result = db.table("production_schedule") \
            .select("*") \
            .eq("source_file", "ORDER_BUILDER") \
            .execute()
        if ob_result.data:
            snapshot["ob_rows_to_preserve"] = ob_result.data
            logger.info("ob_rows_snapshot_taken", count=len(ob_result.data))
    except Exception as e:
        logger.warning("ob_rows_snapshot_failed", error=str(e))

    return snapshot


def _apply_piggyback_deltas(
    production_records: list[ProductionScheduleCreate],
    piggyback_totals: dict[str, float],
) -> int:
    """
    Add piggybacked m2 deltas to matching incoming records.

    For each incoming record whose product_id has piggyback history,
    increase its requested_m2 by the total piggybacked amount.

    Returns number of records adjusted.
    """
    adjusted = 0
    for record in production_records:
        if record.product_id and record.product_id in piggyback_totals:
            delta = Decimal(str(piggyback_totals[record.product_id]))
            record.requested_m2 = (record.requested_m2 or Decimal("0")) + delta
            adjusted += 1
            logger.info(
                "piggyback_preserved",
                product_id=record.product_id,
                delta_m2=float(delta),
                new_requested_m2=float(record.requested_m2),
            )
    return adjusted


def _restore_after_insert(db, snapshot: dict, incoming_product_ids: set[str]) -> None:
    """
    After new production_schedule rows are inserted, restore:
    1. ORDER_BUILDER rows whose products are NOT in the incoming data
    2. piggyback_history rows re-linked to new production_schedule FKs
    """
    # 1. Re-insert ORDER_BUILDER rows not covered by incoming upload
    ob_rows = snapshot.get("ob_rows_to_preserve", [])
    if ob_rows:
        ob_inserted = 0
        for ob_row in ob_rows:
            if ob_row.get("product_id") not in incoming_product_ids:
                insert_data = {
                    k: v for k, v in ob_row.items()
                    if k not in ("id", "created_at", "updated_at")
                }
                try:
                    db.table("production_schedule").insert(insert_data).execute()
                    ob_inserted += 1
                except Exception as e:
                    logger.warning(
                        "ob_row_restore_failed",
                        product_id=ob_row.get("product_id"),
                        error=str(e),
                    )
        if ob_inserted:
            logger.info("ob_rows_restored", count=ob_inserted)

    # 2. Re-insert piggyback_history with new FK references
    piggyback_rows = snapshot.get("piggyback_rows", [])
    if piggyback_rows:
        # Build map: product_id -> new production_schedule row id
        try:
            new_schedule = db.table("production_schedule") \
                .select("id, product_id") \
                .not_.is_("product_id", "null") \
                .execute()
        except Exception as e:
            logger.warning("piggyback_restore_schedule_lookup_failed", error=str(e))
            return

        new_id_by_product: dict[str, str] = {}
        for row in (new_schedule.data or []):
            if row["product_id"]:
                # Take the first (or latest) match per product
                if row["product_id"] not in new_id_by_product:
                    new_id_by_product[row["product_id"]] = row["id"]

        ph_restored = 0
        for h in piggyback_rows:
            new_schedule_id = new_id_by_product.get(h["product_id"])
            if new_schedule_id:
                try:
                    db.table("piggyback_history").insert({
                        "production_schedule_id": new_schedule_id,
                        "product_id": h["product_id"],
                        "additional_m2": h["additional_m2"],
                        "source": h.get("source", "ORDER_BUILDER"),
                        "notes": h.get("notes"),
                        "created_at": h.get("created_at"),
                    }).execute()
                    ph_restored += 1
                except Exception as e:
                    logger.warning(
                        "piggyback_history_restore_failed",
                        product_id=h["product_id"],
                        error=str(e),
                    )
        if ph_restored:
            logger.info("piggyback_history_restored", count=ph_restored)


@router.post("/upload-replace/confirm/{preview_id}", response_model=ProductionImportResult)
async def confirm_production_upload(
    preview_id: str,
    request: Optional[ProductionConfirmRequest] = None,
):
    """Save previously previewed production data (wipe and replace). Optionally resolve unmatched items and apply inline edits."""
    try:
        # Retrieve from cache
        cached = preview_cache_service.retrieve_preview(preview_id)
        if cached is None:
            raise HTTPException(status_code=404, detail="Preview expired or not found")

        file_bytes = cached["file_bytes"]
        filename = cached["filename"]
        file_type = cached["file_type"]

        # Extract request fields (backward compatible)
        manual_mappings = request.manual_mappings if request else []
        modifications = request.modifications if request else []
        deletions = request.deletions if request else []

        logger.info(
            "production_schedule_confirm_started",
            preview_id=preview_id,
            filename=filename
        )

        # Re-parse the file
        parser = get_production_schedule_parser_service()
        service = get_production_schedule_service()

        parsed_data, production_records = await parser.parse_excel(
            file_bytes, filename=filename
        )

        # Apply manual mappings to unmatched records
        if manual_mappings:
            mapping_dict = {m.original_key: m.mapped_product_id for m in manual_mappings}
            product_service = get_product_service()
            for record in production_records:
                if not record.product_id and record.referencia in mapping_dict:
                    product = product_service.get_by_id(mapping_dict[record.referencia])
                    record.product_id = product.id
                    record.sku = product.sku

        # Apply inline modifications (update field values by row_index)
        if modifications:
            mod_map = {m.row_index: m for m in modifications}
            for idx, record in enumerate(production_records):
                if idx in mod_map:
                    mod = mod_map[idx]
                    if mod.requested_m2 is not None:
                        record.requested_m2 = mod.requested_m2
                    if mod.status is not None:
                        record.status = mod.status
            logger.info("production_modifications_applied", count=len(modifications))

        # Apply inline deletions (exclude rows by index)
        if deletions:
            deletion_set = set(deletions)
            production_records = [
                r for idx, r in enumerate(production_records)
                if idx not in deletion_set
            ]
            logger.info("production_deletions_applied", count=len(deletions))

        if not production_records:
            raise HTTPException(
                status_code=400,
                detail="No production items could be extracted from the Excel"
            )

        # --- Piggyback preservation: BEFORE wipe ---
        from config import get_supabase_client
        db = get_supabase_client()
        piggyback_snapshot = _snapshot_piggybacks_before_wipe(db)

        # Apply piggybacked deltas to incoming records
        piggyback_totals = piggyback_snapshot.get("piggyback_totals", {})
        if piggyback_totals:
            adjusted = _apply_piggyback_deltas(production_records, piggyback_totals)
            logger.info("piggyback_deltas_applied", adjusted_records=adjusted)

        # Collect incoming product_ids for ORDER_BUILDER row filtering
        incoming_product_ids = {
            r.product_id for r in production_records if r.product_id
        }

        # Wipe existing data
        try:
            db.table("production_schedule").delete().neq(
                "id", "00000000-0000-0000-0000-000000000000"
            ).execute()
            logger.info("existing_schedule_deleted")
        except Exception as e:
            logger.warning("delete_existing_failed", error=str(e))

        # Import using correct schema
        result = service.import_from_excel(production_records, match_products=True)

        # --- Piggyback preservation: AFTER insert ---
        _restore_after_insert(db, piggyback_snapshot, incoming_product_ids)

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type=cached.get("upload_type", "production_schedule"),
            file_hash=cached.get("file_hash", ""),
            filename=cached.get("filename", "unknown"),
            row_count=result.total_rows_parsed,
        )

        # --- Ledger: reconcile production ---
        try:
            ledger = get_ledger_service()
            recon_items = []
            for record in production_records:
                if record.product_id and record.requested_m2:
                    recon_result = ledger.reconcile_production(
                        product_id=record.product_id,
                        actual_m2=Decimal(str(record.requested_m2)),
                        source_filename=cached.get("filename"),
                    )
                    if recon_result:
                        recon_items.append({
                            "product_id": record.product_id,
                            "actual_m2": float(record.requested_m2),
                            "projected_m2": float(recon_result.get("projected_value_m2", 0) or 0),
                            "discrepancy_m2": float(recon_result.get("discrepancy_m2", 0) or 0),
                        })
            if recon_items:
                ledger.generate_reconciliation_report("production", recon_items, cached.get("filename"))
        except Exception as ledger_err:
            logger.warning("ledger_production_hook_failed", error=str(ledger_err))

        # Delete preview from cache
        preview_cache_service.delete_preview(preview_id)

        logger.info(
            "production_schedule_confirm_complete",
            preview_id=preview_id,
            total_rows=result.total_rows_parsed,
            matched=result.matched_to_products,
            unmatched=len(result.unmatched_referencias)
        )

        return result

    except HTTPException:
        raise
    except ValueError as e:
        logger.error("production_schedule_confirm_error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return handle_error(e)


@router.post("/upload-replace", response_model=ProductionImportResult)
async def upload_and_replace_schedule(
    file: UploadFile = File(..., description="Production schedule PDF or Excel file")
):
    """
    Upload a production schedule Excel, wiping and replacing all existing data.

    This is the preferred method for daily uploads where the new file
    represents the complete current state.

    Returns:
        Import result with matched/unmatched counts and status breakdown
    """
    # Validate file type
    file_type = _validate_file_type(file.filename)

    try:
        # Read file content
        file_bytes = await file.read()

        if len(file_bytes) == 0:
            raise HTTPException(status_code=400, detail="Empty file uploaded")

        if len(file_bytes) > 10 * 1024 * 1024:  # 10MB limit
            raise HTTPException(status_code=400, detail="File too large (max 10MB)")

        logger.info(
            "production_schedule_upload_replace_started",
            filename=file.filename,
            size=len(file_bytes),
            file_type=file_type
        )

        # Parse based on file type
        parser = get_production_schedule_parser_service()
        service = get_production_schedule_service()

        if file_type == 'excel':
            # Excel uses the NEW schema with production_records
            parsed_data, production_records = await parser.parse_excel(
                file_bytes, filename=file.filename
            )

            if not production_records:
                raise HTTPException(
                    status_code=400,
                    detail="No production items could be extracted from the Excel"
                )

            # Wipe existing data
            try:
                from config import get_supabase_client
                db = get_supabase_client()
                db.table("production_schedule").delete().neq(
                    "id", "00000000-0000-0000-0000-000000000000"
                ).execute()
                logger.info("existing_schedule_deleted")
            except Exception as e:
                logger.warning("delete_existing_failed", error=str(e))

            # Import using correct schema
            result = service.import_from_excel(production_records, match_products=True)

            logger.info(
                "production_schedule_upload_replace_completed",
                total_rows=result.total_rows_parsed,
                matched=result.matched_to_products,
                unmatched=len(result.unmatched_referencias)
            )

            return result

        else:
            # PDF uses old schema (requires migration 015)
            parsed_data = await parser.parse_pdf(file_bytes, filename=file.filename)

            if not parsed_data.line_items:
                raise HTTPException(
                    status_code=400,
                    detail="No production items could be extracted from the PDF"
                )

            # For PDF, attempt old method but this may fail without migration
            raise HTTPException(
                status_code=400,
                detail="PDF upload not supported. Please use Excel format (.xlsx)"
            )

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
        all_products = product_service.get_all_active_tiles()

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


# ===================
# ORDER BUILDER FEEDBACK LOOP
# ===================


class OBProductionItem(BaseModel):
    product_id: str
    sku: Optional[str] = None
    referencia: Optional[str] = None
    requested_m2: float


class OBPiggybackItem(BaseModel):
    product_id: str
    additional_m2: float


class OBProductionRequest(BaseModel):
    items: list[OBProductionItem]
    boat_departure: Optional[str] = None


class OBPiggybackRequest(BaseModel):
    items: list[OBPiggybackItem]


@router.post("/from-order-builder")
def create_from_order_builder(request: OBProductionRequest):
    """
    Create production_schedule rows from Order Builder factory request export (Section 3).

    Closes the feedback loop: export → production_schedule → recommendation reads it back.
    """
    try:
        service = get_production_schedule_service()
        items = [item.model_dump() for item in request.items]
        created = service.create_from_order_builder(items, request.boat_departure)

        # --- Ledger: record factory order export (Section 3) ---
        try:
            ledger = get_ledger_service()
            for ob_item in request.items:
                if ob_item.requested_m2:
                    ledger.record_factory_order_exported(
                        product_id=ob_item.product_id,
                        requested_m2=Decimal(str(ob_item.requested_m2)),
                    )
        except Exception as ledger_err:
            logger.warning("ledger_factory_export_hook_failed", error=str(ledger_err))

        return {
            "created": len(created),
            "items": created,
        }
    except Exception as e:
        return handle_error(e)


@router.post("/piggyback-update")
def update_piggyback(request: OBPiggybackRequest):
    """
    Update production_schedule.requested_m2 for piggyback exports (Section 2).

    Closes the piggyback feedback loop: additional m2 → headroom shrinks.
    """
    try:
        service = get_production_schedule_service()
        items = [item.model_dump() for item in request.items]
        updated = service.update_piggyback(items)
        return {
            "updated": updated,
        }
    except Exception as e:
        return handle_error(e)


# ===================
# PIGGYBACK CONFIRMATION
# ===================


class PiggybackConfirmRequest(BaseModel):
    product_id: str
    additional_m2: float
    notes: Optional[str] = None


class PiggybackConfirmResponse(BaseModel):
    success: bool
    new_requested_m2: float
    history_id: str
    message: str


@router.post("/piggyback-confirm", response_model=PiggybackConfirmResponse)
def confirm_piggyback(request: PiggybackConfirmRequest):
    """
    Confirm a piggyback: update production requested_m2 and record in piggyback_history.

    Used by Section 2 UI for explicit per-product piggyback confirmation.
    """
    try:
        service = get_production_schedule_service()
        result = service.confirm_piggyback(
            product_id=request.product_id,
            additional_m2=request.additional_m2,
            notes=request.notes,
        )

        # --- Ledger: record piggyback confirmation (Section 2) ---
        try:
            ledger = get_ledger_service()
            ledger.record_piggyback_confirmed(
                product_id=request.product_id,
                added_m2=Decimal(str(request.additional_m2)),
                source_id=result["history_entry"]["id"],
            )
        except Exception as ledger_err:
            logger.warning("ledger_piggyback_hook_failed", error=str(ledger_err))

        return PiggybackConfirmResponse(
            success=True,
            new_requested_m2=result["production_schedule"]["requested_m2"],
            history_id=result["history_entry"]["id"],
            message=f"Piggyback confirmado: {request.additional_m2} m\u00b2",
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        return handle_error(e)
