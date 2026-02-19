"""
Inventory API routes.

See BUILDER_BLUEPRINT.md for endpoint specifications.
See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from decimal import Decimal
from io import BytesIO
from collections import defaultdict
import hashlib
import structlog

from models.inventory import (
    InventorySnapshotCreate,
    InventorySnapshotUpdate,
    InventorySnapshotResponse,
    InventoryListResponse,
    InventoryCurrentResponse,
    InventoryUploadResponse,
    InTransitUploadResponse,
    InventoryPreview,
    InventoryPreviewRow,
    InventoryConfirmRequest,
    ReconciliationItem,
    ReconciliationSummary,
)
from models.product import ProductCreate, Category, Rotation
from services.inventory_service import get_inventory_service
from services.product_service import get_product_service
from parsers.excel_parser import parse_owner_excel, extract_products_from_excel, _normalize_sku_name
from exceptions import (
    AppError,
    InventoryNotFoundError,
    InventoryUploadError,
    SIESAParseError,
    SIESAMissingColumnsError,
)
from models.inventory_lot import (
    SIESAUploadResponse,
    InventoryLotResponse,
    InventoryLotsListResponse,
    WarehouseSummary,
    RowError,
    ProductLotSummary,
    ContainerEstimateResponse,
    SIESAPreview,
    SIESAPreviewLot,
)
from parsers.siesa_parser import parse_siesa_bytes
from config.shipping import (
    calculate_containers_needed,
    calculate_utilization_breakdown,
    CONTAINER_WEIGHT_LIMIT_KG,
)
from utils.text_utils import normalize_product_name
from config import get_supabase_client
from services import preview_cache_service
from services.upload_history_service import get_upload_history_service
from models.manual_mapping import ManualMapping

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

@router.get("/latest", response_model=list[InventorySnapshotResponse])
async def get_latest_inventory():
    """
    Get the most recent inventory snapshot for each product.

    Returns list of snapshots (alias for /current).
    """
    try:
        service = get_inventory_service()
        return service.get_latest()
    except Exception as e:
        return handle_error(e)


@router.get("/current", response_model=InventoryCurrentResponse)
async def get_current_inventory():
    """
    Get the most recent inventory snapshot for each product.

    Used for dashboard display.
    """
    try:
        service = get_inventory_service()
        snapshots = service.get_latest()

        # Find the most recent date
        as_of = date.today()
        if snapshots:
            as_of = max(s.snapshot_date for s in snapshots)

        return InventoryCurrentResponse(
            data=snapshots,
            total=len(snapshots),
            as_of=as_of
        )

    except Exception as e:
        return handle_error(e)


@router.post("/upload/preview", response_model=InventoryPreview)
async def preview_inventory_upload(file: UploadFile = File(...)):
    """Parse inventory Excel and return preview. Nothing is saved."""
    try:
        # Read file content
        content = await file.read()
        file_hash = hashlib.sha256(content).hexdigest()
        file_obj = BytesIO(content)

        # Check for duplicate upload
        inv_duplicate = get_upload_history_service().check_duplicate("inventory", file_hash)

        product_service = get_product_service()

        # STEP 1: Extract products from Excel
        extracted_products = extract_products_from_excel(BytesIO(content))

        # Determine which products will be auto-created
        products, _ = product_service.get_all(page=1, page_size=1000, active_only=True)

        # Build lookup: owner_code -> product
        known_owner_codes = {
            p.owner_code: p
            for p in products
            if p.owner_code is not None
        }

        # Build lookup: normalized SKU name -> product
        known_sku_names = {
            _normalize_sku_name(p.sku): p
            for p in products
            if p.sku
        }

        # Determine which products would be auto-created
        auto_created_skus = []
        products_to_upsert = []
        for p in extracted_products:
            # Check if product already exists
            normalized_sku = _normalize_sku_name(p.sku)
            exists = (p.owner_code and p.owner_code in known_owner_codes) or (normalized_sku in known_sku_names)

            if not exists:
                auto_created_skus.append(p.sku)
                try:
                    products_to_upsert.append(ProductCreate(
                        sku=p.sku,
                        category=Category(p.category),
                        rotation=Rotation(p.rotation) if p.rotation else None,
                    ))
                except ValueError:
                    continue

        # STEP 2: Parse INVENTARIO sheet
        # Build lookup dicts for parsing (includes existing products)
        known_owner_codes_ids = {
            p.owner_code: p.id
            for p in products
            if p.owner_code is not None
        }
        known_sku_names_ids = {
            _normalize_sku_name(p.sku): p.id
            for p in products
            if p.sku
        }

        parse_result = parse_owner_excel(file_obj, known_owner_codes_ids, known_sku_names_ids)

        if parse_result.errors:
            inventory_errors = [
                e for e in parse_result.errors
                if e.sheet.upper().startswith("INVENTARIO")
            ]
            if inventory_errors:
                raise InventoryUploadError([
                    {
                        "sheet": e.sheet,
                        "row": e.row,
                        "field": e.field,
                        "error": e.error
                    }
                    for e in inventory_errors
                ])

        # Convert parsed records to create models
        snapshots_to_create = [
            InventorySnapshotCreate(
                product_id=record.product_id,
                warehouse_qty=record.warehouse_qty,
                in_transit_qty=record.in_transit_qty,
                snapshot_date=record.snapshot_date,
                notes=record.notes,
            )
            for record in parse_result.inventory
        ]

        # STEP 3: Determine which products would get zero-filled
        zero_filled_skus = []
        if snapshots_to_create:
            snapshot_date = snapshots_to_create[0].snapshot_date
            uploaded_product_ids = {s.product_id for s in snapshots_to_create}

            missing_products = [
                p for p in products
                if p.id not in uploaded_product_ids
            ]

            if missing_products:
                zero_filled_skus = [p.sku for p in missing_products]
                for p in missing_products:
                    snapshots_to_create.append(
                        InventorySnapshotCreate(
                            product_id=p.id,
                            warehouse_qty=0,
                            in_transit_qty=0,
                            snapshot_date=snapshot_date,
                        )
                    )

        # Build stats
        row_count = len(snapshots_to_create)
        product_count = len(set(s.product_id for s in snapshots_to_create))
        snapshot_date = snapshots_to_create[0].snapshot_date if snapshots_to_create else date.today()

        # Build all preview rows (with product_id for inline editing)
        sku_lookup = {p.id: p.sku for p in products}
        all_rows = [
            InventoryPreviewRow(
                product_id=s.product_id,
                sku=sku_lookup.get(s.product_id, "UNKNOWN"),
                warehouse_qty=float(s.warehouse_qty),
                in_transit_qty=float(s.in_transit_qty),
                snapshot_date=s.snapshot_date,
            )
            for s in snapshots_to_create
        ]

        # Store in cache
        cache_data = {
            "products_to_upsert": [p.model_dump() for p in products_to_upsert],
            "snapshots_to_create": [s.model_dump() for s in snapshots_to_create],
            "auto_created_skus": auto_created_skus,
            "zero_filled_skus": zero_filled_skus,
            "file_hash": file_hash,
            "filename": file.filename,
            "upload_type": "inventory",
        }
        preview_id = preview_cache_service.store_preview(cache_data)

        logger.info(
            "inventory_preview_created",
            preview_id=preview_id,
            row_count=row_count,
            product_count=product_count,
            auto_created_count=len(auto_created_skus),
            zero_filled_count=len(zero_filled_skus),
        )

        return InventoryPreview(
            preview_id=preview_id,
            row_count=row_count,
            product_count=product_count,
            snapshot_date=snapshot_date,
            auto_created_products=auto_created_skus,
            auto_created_count=len(auto_created_skus),
            zero_filled_count=len(zero_filled_skus),
            zero_filled_products=zero_filled_skus[:20],  # Limit to 20
            warnings=[f"Este archivo ya fue subido el {inv_duplicate['uploaded_at'][:10]} ({inv_duplicate['filename']})"] if inv_duplicate else [],
            rows=all_rows,
            sample_rows=all_rows[:10],  # Backward compat
        )

    except (InventoryUploadError, AppError) as e:
        return handle_error(e)
    except Exception as e:
        logger.error("inventory_preview_failed", error=str(e))
        return handle_error(e)


@router.post("/upload/confirm/{preview_id}", response_model=InventoryUploadResponse)
async def confirm_inventory_upload(preview_id: str, request: Optional[InventoryConfirmRequest] = None):
    """Save previously previewed inventory data with optional inline edits."""
    try:
        cache_data = preview_cache_service.retrieve_preview(preview_id)
        if cache_data is None:
            raise HTTPException(status_code=404, detail="Preview expired")

        product_service = get_product_service()

        # Retrieve cached data
        products_to_upsert = [ProductCreate(**p) for p in cache_data["products_to_upsert"]]
        snapshots_raw = cache_data["snapshots_to_create"]

        # Apply modifications from inline editing
        modifications = request.modifications if request else []
        deletions = request.deletions if request else []

        if modifications:
            mod_map = {m.product_id: m for m in modifications}
            for snap_dict in snapshots_raw:
                pid = snap_dict.get("product_id", "")
                if pid in mod_map:
                    mod = mod_map[pid]
                    if mod.warehouse_qty is not None:
                        snap_dict["warehouse_qty"] = float(mod.warehouse_qty)
                    if mod.in_transit_qty is not None:
                        snap_dict["in_transit_qty"] = float(mod.in_transit_qty)
            logger.info("inventory_modifications_applied", count=len(modifications))

        # Apply deletions (exclude rows by product_id)
        if deletions:
            deletion_set = set(deletions)
            snapshots_raw = [
                s for s in snapshots_raw
                if s.get("product_id", "") not in deletion_set
            ]
            logger.info("inventory_deletions_applied", count=len(deletions))

        snapshots_to_create = [InventorySnapshotCreate(**s) for s in snapshots_raw]

        # Bulk upsert auto-created products
        if products_to_upsert:
            created, updated = product_service.bulk_upsert(products_to_upsert)
            logger.info("products_auto_created", created=created, updated=updated)

        # Upsert to warehouse_snapshots (each upload only touches its own table)
        if snapshots_to_create:
            db = get_supabase_client()
            rows = [
                {
                    "product_id": s.product_id,
                    "snapshot_date": s.snapshot_date.isoformat(),
                    "warehouse_qty": s.warehouse_qty,
                }
                for s in snapshots_to_create
            ]
            # Batch upsert in chunks
            chunk_size = 100
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                db.table("warehouse_snapshots").upsert(
                    chunk, on_conflict="product_id,snapshot_date"
                ).execute()

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type=cache_data.get("upload_type", "inventory"),
            file_hash=cache_data.get("file_hash", ""),
            filename=cache_data.get("filename", "unknown"),
            row_count=len(snapshots_to_create),
        )

        # Delete preview from cache
        preview_cache_service.delete_preview(preview_id)

        logger.info(
            "inventory_confirm_complete",
            preview_id=preview_id,
            records_created=len(snapshots_to_create),
            modifications_count=len(modifications),
            deletions_count=len(deletions),
        )

        return InventoryUploadResponse(
            success=True,
            records_created=len(snapshots_to_create),
            message=f"Successfully uploaded {len(snapshots_to_create)} inventory records"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("inventory_confirm_failed", error=str(e), preview_id=preview_id)
        return handle_error(e)


@router.post("/upload", response_model=InventoryUploadResponse)
async def upload_inventory(file: UploadFile = File(...)):
    """
    Upload inventory data from Excel file.

    Parses the INVENTARIO sheet from the owner template.
    Auto-creates products from the sheet if they don't exist.
    Rejects entire upload if any row has errors.

    Raises:
        422: Validation error (missing columns, invalid data, etc.)
    """
    logger.info(
        "inventory_upload_started",
        filename=file.filename,
        content_type=file.content_type
    )

    try:
        # Read file content
        content = await file.read()
        file_obj = BytesIO(content)

        product_service = get_product_service()

        # STEP 1: Extract and seed products from Excel
        extracted_products = extract_products_from_excel(BytesIO(content))

        if extracted_products:
            # Convert to ProductCreate objects
            products_to_upsert = []
            for p in extracted_products:
                try:
                    products_to_upsert.append(ProductCreate(
                        sku=p.sku,
                        category=Category(p.category),
                        rotation=Rotation(p.rotation) if p.rotation else None,
                    ))
                except ValueError as e:
                    logger.warning("invalid_product_data", sku=p.sku, error=str(e))
                    continue

            # Upsert products
            created, updated = product_service.bulk_upsert(products_to_upsert)
            logger.info("products_seeded", created=created, updated=updated)

        # STEP 2: Re-fetch products to get updated mappings
        products, _ = product_service.get_all(page=1, page_size=1000, active_only=True)

        # Build lookup: owner_code -> product_id
        known_owner_codes = {
            p.owner_code: p.id
            for p in products
            if p.owner_code is not None
        }

        # Build lookup: normalized SKU name -> product_id
        known_sku_names = {
            _normalize_sku_name(p.sku): p.id
            for p in products
            if p.sku
        }

        # STEP 3: Parse Excel file (now products exist)
        parse_result = parse_owner_excel(file_obj, known_owner_codes, known_sku_names)

        # Check for inventory-specific errors only (ignore sales errors)
        if parse_result.errors:
            # Filter to only inventory sheet errors
            inventory_errors = [
                e for e in parse_result.errors
                if e.sheet.upper().startswith("INVENTARIO")
            ]
            other_errors = len(parse_result.errors) - len(inventory_errors)

            if other_errors > 0:
                logger.info(
                    "inventory_upload_ignoring_sales_errors",
                    sales_error_count=other_errors
                )

            # Only reject if there are actual inventory errors
            if inventory_errors:
                logger.warning(
                    "inventory_upload_validation_failed",
                    error_count=len(inventory_errors)
                )
                raise InventoryUploadError([
                    {
                        "sheet": e.sheet,
                        "row": e.row,
                        "field": e.field,
                        "error": e.error
                    }
                    for e in inventory_errors
                ])

        # Convert parsed records to create models
        inventory_service = get_inventory_service()
        snapshots_to_create = [
            InventorySnapshotCreate(
                product_id=record.product_id,
                warehouse_qty=record.warehouse_qty,
                in_transit_qty=record.in_transit_qty,
                snapshot_date=record.snapshot_date,
                notes=record.notes,
            )
            for record in parse_result.inventory
        ]

        # STEP 4: Fill missing active products with zero-rows
        # Ensures ALL active products get a snapshot row for this date,
        # preventing stale data from older dates in get_latest().
        if snapshots_to_create:
            snapshot_date = snapshots_to_create[0].snapshot_date
            uploaded_product_ids = {s.product_id for s in snapshots_to_create}

            # Re-use the products list already fetched in STEP 2
            missing_products = [
                p for p in products
                if p.id not in uploaded_product_ids
            ]

            if missing_products:
                for p in missing_products:
                    snapshots_to_create.append(
                        InventorySnapshotCreate(
                            product_id=p.id,
                            warehouse_qty=0,
                            in_transit_qty=0,
                            snapshot_date=snapshot_date,
                        )
                    )
                logger.info(
                    "inventory_filled_missing_products",
                    filled_count=len(missing_products),
                    filled_skus=[p.sku for p in missing_products],
                )

        if snapshots_to_create:
            # Upsert to warehouse_snapshots (idempotent, only touches warehouse_qty)
            db = get_supabase_client()
            rows = [
                {
                    "product_id": s.product_id,
                    "snapshot_date": s.snapshot_date.isoformat(),
                    "warehouse_qty": s.warehouse_qty,
                }
                for s in snapshots_to_create
            ]
            chunk_size = 100
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                db.table("warehouse_snapshots").upsert(
                    chunk, on_conflict="product_id,snapshot_date"
                ).execute()

        logger.info(
            "inventory_upload_completed",
            records_created=len(snapshots_to_create)
        )

        return InventoryUploadResponse(
            success=True,
            records_created=len(snapshots_to_create),
            message=f"Successfully uploaded {len(snapshots_to_create)} inventory records"
        )

    except InventoryUploadError:
        raise
    except Exception as e:
        logger.error("inventory_upload_failed", error=str(e))
        return handle_error(e)


# ===================
# SIESA INVENTORY (LOT-LEVEL)
# ===================

@router.post("/siesa/upload/preview", response_model=SIESAPreview)
async def preview_siesa_upload(
    file: UploadFile = File(...),
    snapshot_date: Optional[date] = Query(None, description="Snapshot date (defaults to today)")
):
    """Parse SIESA XLS and return preview. Nothing is saved."""
    actual_date = snapshot_date or date.today()

    try:
        # Read file content
        content = await file.read()
        siesa_file_hash = hashlib.sha256(content).hexdigest()

        # Check for duplicate upload
        siesa_duplicate = get_upload_history_service().check_duplicate("siesa", siesa_file_hash)

        # Build product lookup dictionaries
        product_service = get_product_service()
        products, _ = product_service.get_all(page=1, page_size=10000, active_only=False)

        # siesa_item -> (product_id, sku)
        products_by_siesa_item: dict[int, tuple[str, str]] = {}
        # normalized_name -> (product_id, sku)
        products_by_normalized_name: dict[str, tuple[str, str]] = {}

        for p in products:
            if p.siesa_item:
                products_by_siesa_item[p.siesa_item] = (p.id, p.sku)
            normalized = normalize_product_name(p.sku)
            if normalized:
                products_by_normalized_name[normalized] = (p.id, p.sku)

        # Parse the file
        parse_result = parse_siesa_bytes(
            file_content=content,
            filename=file.filename or "siesa.xls",
            snapshot_date=actual_date,
            products_by_siesa_item=products_by_siesa_item,
            products_by_normalized_name=products_by_normalized_name,
        )

        # Build lots_to_insert list
        lots_to_insert = []
        for match in parse_result.matched_lots:
            lot = match.lot
            lots_to_insert.append({
                "product_id": match.product_id,
                "lot_number": lot.lot_number,
                "quantity_m2": float(lot.quantity_m2),
                "weight_kg": float(lot.weight_kg) if lot.weight_kg else None,
                "quality": lot.quality,
                "warehouse_code": lot.warehouse_code,
                "warehouse_name": lot.warehouse_name,
                "snapshot_date": actual_date.isoformat(),
                "siesa_item": lot.siesa_item,
                "siesa_description": lot.siesa_description,
            })

        # Calculate container stats
        total_weight = float(parse_result.total_weight_kg)
        containers_needed = calculate_containers_needed(total_weight, CONTAINER_WEIGHT_LIMIT_KG)
        utilization = 0.0
        if containers_needed > 0:
            utilization = (total_weight / (containers_needed * CONTAINER_WEIGHT_LIMIT_KG)) * 100

        # Build warehouse summaries
        warehouse_summaries = [
            WarehouseSummary(
                code=wh.code,
                name=wh.name,
                total_m2=float(wh.total_m2),
                total_weight_kg=float(wh.total_weight_kg),
                lot_count=wh.lot_count,
            )
            for wh in parse_result.warehouses.values()
        ]

        # Calculate match stats
        total_matched = parse_result.matched_by_siesa_item + parse_result.matched_by_name
        total_processed = total_matched + parse_result.unmatched_count
        match_rate = (total_matched / total_processed * 100) if total_processed > 0 else 0.0

        # Get unmatched product descriptions
        unmatched_products = list(set(
            m.lot.siesa_description or f"Item {m.lot.siesa_item}"
            for m in parse_result.unmatched_lots
        ))[:20]

        # Build SKU lookup for sample lots
        sku_lookup = {p.id: p.sku for p in products}

        # Build sample lots (first 10 matched)
        sample_lots = [
            SIESAPreviewLot(
                sku=sku_lookup.get(match.product_id, "UNKNOWN"),
                warehouse_name=match.lot.warehouse_name,
                lot_number=match.lot.lot_number,
                quantity_m2=float(match.lot.quantity_m2),
                weight_kg=float(match.lot.weight_kg) if match.lot.weight_kg else None,
            )
            for match in parse_result.matched_lots[:10]
        ]

        # Build unmatched lots for manual resolution
        unmatched_lots_raw = [
            {
                "lot_number": m.lot.lot_number,
                "quantity_m2": float(m.lot.quantity_m2),
                "weight_kg": float(m.lot.weight_kg) if m.lot.weight_kg else None,
                "quality": m.lot.quality,
                "warehouse_code": m.lot.warehouse_code,
                "warehouse_name": m.lot.warehouse_name,
                "snapshot_date": actual_date.isoformat(),
                "siesa_item": m.lot.siesa_item,
                "siesa_description": m.lot.siesa_description,
            }
            for m in parse_result.unmatched_lots
        ]

        # Store in cache
        cache_data = {
            "lots_to_insert": lots_to_insert,
            "unmatched_lots": unmatched_lots_raw,
            "snapshot_date": actual_date.isoformat(),
            "parse_result": {
                "total_rows": parse_result.total_rows,
                "processed_rows": parse_result.processed_rows,
                "skipped_errors": parse_result.skipped_errors,
                "unique_siesa_items": parse_result.unique_siesa_items,
            },
            "warehouse_summaries": [w.model_dump() for w in warehouse_summaries],
            "unmatched_products": unmatched_products,
            "match_stats": {
                "matched_by_siesa_item": parse_result.matched_by_siesa_item,
                "matched_by_name": parse_result.matched_by_name,
                "unmatched_count": parse_result.unmatched_count,
                "match_rate_pct": round(match_rate, 1),
            },
            "container_stats": {
                "containers_needed": containers_needed,
                "container_utilization_pct": round(utilization, 1),
            },
            "file_hash": siesa_file_hash,
            "filename": file.filename,
            "upload_type": "siesa",
        }
        preview_id = preview_cache_service.store_preview(cache_data)

        logger.info(
            "siesa_preview_created",
            preview_id=preview_id,
            total_rows=parse_result.total_rows,
            lots_count=len(lots_to_insert),
            match_rate_pct=round(match_rate, 1),
        )

        return SIESAPreview(
            preview_id=preview_id,
            snapshot_date=actual_date,
            total_rows=parse_result.total_rows,
            lots_count=len(lots_to_insert),
            unique_products=parse_result.unique_siesa_items,
            total_m2_available=float(parse_result.total_m2),
            total_weight_kg=float(parse_result.total_weight_kg),
            containers_needed=containers_needed,
            container_utilization_pct=round(utilization, 1),
            matched_by_siesa_item=parse_result.matched_by_siesa_item,
            matched_by_name=parse_result.matched_by_name,
            unmatched_count=parse_result.unmatched_count,
            match_rate_pct=round(match_rate, 1),
            unmatched_products=unmatched_products,
            warehouses=warehouse_summaries,
            warnings=[f"Este archivo ya fue subido el {siesa_duplicate['uploaded_at'][:10]} ({siesa_duplicate['filename']})"] if siesa_duplicate else [],
            sample_lots=sample_lots,
        )

    except SIESAMissingColumnsError as e:
        logger.error("siesa_preview_missing_columns", missing=e.details.get("missing_columns"))
        return handle_error(e)
    except SIESAParseError as e:
        logger.error("siesa_preview_error", error=str(e))
        return handle_error(e)
    except Exception as e:
        logger.error("siesa_preview_failed", error=str(e), type=type(e).__name__)
        return handle_error(e)


@router.post("/siesa/upload/confirm/{preview_id}", response_model=SIESAUploadResponse)
async def confirm_siesa_upload(
    preview_id: str,
    manual_mappings: Optional[list[ManualMapping]] = None,
):
    """Save previously previewed SIESA data. Optionally resolve unmatched items."""
    try:
        cache_data = preview_cache_service.retrieve_preview(preview_id)
        if cache_data is None:
            raise HTTPException(status_code=404, detail="Preview expired")

        # Retrieve cached data
        lots_to_insert = cache_data["lots_to_insert"]

        # Apply manual mappings to unmatched lots
        if manual_mappings:
            mapping_dict = {m.original_key: m.mapped_product_id for m in manual_mappings}
            unmatched_lots = cache_data.get("unmatched_lots", [])
            for lot_data in unmatched_lots:
                key = lot_data.get("siesa_description") or f"Item {lot_data.get('siesa_item')}"
                if key in mapping_dict:
                    lot_data["product_id"] = mapping_dict[key]
                    lots_to_insert.append(lot_data)
        actual_date = date.fromisoformat(cache_data["snapshot_date"])
        parse_result_data = cache_data["parse_result"]
        warehouse_summaries = [WarehouseSummary(**w) for w in cache_data["warehouse_summaries"]]
        unmatched_products = cache_data["unmatched_products"]
        match_stats = cache_data["match_stats"]
        container_stats = cache_data["container_stats"]

        # Get database client
        db = get_supabase_client()

        # Delete existing lots for this snapshot_date (idempotent)
        delete_result = db.table("inventory_lots").delete().eq(
            "snapshot_date", actual_date.isoformat()
        ).execute()
        deleted_count = len(delete_result.data) if delete_result.data else 0
        if deleted_count > 0:
            logger.info("siesa_deleted_existing_lots", count=deleted_count, date=actual_date)

        # Batch insert lots
        lots_created = 0
        if lots_to_insert:
            chunk_size = 100
            for i in range(0, len(lots_to_insert), chunk_size):
                chunk = lots_to_insert[i:i + chunk_size]
                db.table("inventory_lots").insert(chunk).execute()
                lots_created += len(chunk)

        # Sync to factory_snapshots (independent table — no carry-forward needed)
        if lots_created > 0:
            product_stats: dict[str, dict] = {}
            for lot in lots_to_insert:
                pid = lot["product_id"]
                qty = lot["quantity_m2"]
                lot_num = lot.get("lot_number", "")

                if pid not in product_stats:
                    product_stats[pid] = {
                        "total_m2": 0.0,
                        "lot_count": 0,
                        "largest_lot_m2": 0.0,
                        "largest_lot_code": "",
                    }

                stats = product_stats[pid]
                stats["total_m2"] += qty
                stats["lot_count"] += 1

                if qty > stats["largest_lot_m2"]:
                    stats["largest_lot_m2"] = qty
                    stats["largest_lot_code"] = lot_num

            synced_count = 0
            for pid, stats in product_stats.items():
                db.table("factory_snapshots").upsert({
                    "product_id": pid,
                    "snapshot_date": actual_date.isoformat(),
                    "factory_available_m2": stats["total_m2"],
                    "factory_lot_count": stats["lot_count"],
                    "factory_largest_lot_m2": stats["largest_lot_m2"],
                    "factory_largest_lot_code": stats["largest_lot_code"],
                }, on_conflict="product_id,snapshot_date").execute()
                synced_count += 1

            logger.info(
                "siesa_synced_to_factory_snapshots",
                products_synced=synced_count,
                date=actual_date.isoformat(),
            )

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type=cache_data.get("upload_type", "siesa"),
            file_hash=cache_data.get("file_hash", ""),
            filename=cache_data.get("filename", "unknown"),
            row_count=lots_created,
        )

        # Delete preview from cache
        preview_cache_service.delete_preview(preview_id)

        logger.info(
            "siesa_confirm_complete",
            preview_id=preview_id,
            lots_created=lots_created,
        )

        # Build response
        return SIESAUploadResponse(
            success=True,
            snapshot_date=actual_date,
            total_rows=parse_result_data["total_rows"],
            processed_rows=parse_result_data["processed_rows"],
            skipped_errors=parse_result_data["skipped_errors"],
            errors=[],
            lots_created=lots_created,
            unique_products=parse_result_data["unique_siesa_items"],
            total_m2_available=sum(lot["quantity_m2"] for lot in lots_to_insert),
            total_weight_kg=sum(lot.get("weight_kg") or 0 for lot in lots_to_insert),
            container_limit_kg=CONTAINER_WEIGHT_LIMIT_KG,
            containers_needed=container_stats["containers_needed"],
            container_utilization_pct=container_stats["container_utilization_pct"],
            matched_by_siesa_item=match_stats["matched_by_siesa_item"],
            matched_by_name=match_stats["matched_by_name"],
            unmatched_count=match_stats["unmatched_count"],
            match_rate_pct=match_stats["match_rate_pct"],
            unmatched_products=unmatched_products,
            warehouses=warehouse_summaries,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("siesa_confirm_failed", error=str(e), preview_id=preview_id)
        return handle_error(e)


@router.post("/siesa/upload", response_model=SIESAUploadResponse)
async def upload_siesa_inventory(
    file: UploadFile = File(...),
    snapshot_date: Optional[date] = Query(None, description="Snapshot date (defaults to today)")
):
    """
    Upload lot-level inventory from SIESA factory XLS export.

    Parses the SIESA inventory report and stores each row as a separate lot.
    Upload is idempotent: existing lots for the snapshot_date are deleted before insert.

    Products are matched by:
    1. siesa_item code (exact match to products.siesa_item)
    2. Normalized name fallback (using normalize_product_name)

    Returns detailed statistics including match rates, warehouse breakdown,
    and container weight estimates.
    """
    actual_date = snapshot_date or date.today()

    logger.info(
        "siesa_upload_started",
        filename=file.filename,
        snapshot_date=actual_date,
    )

    try:
        # Read file content
        content = await file.read()

        # Build product lookup dictionaries
        product_service = get_product_service()
        products, _ = product_service.get_all(page=1, page_size=10000, active_only=False)

        # siesa_item -> (product_id, sku)
        products_by_siesa_item: dict[int, tuple[str, str]] = {}
        # normalized_name -> (product_id, sku)
        products_by_normalized_name: dict[str, tuple[str, str]] = {}

        for p in products:
            if p.siesa_item:
                products_by_siesa_item[p.siesa_item] = (p.id, p.sku)
            # Also build name lookup
            normalized = normalize_product_name(p.sku)
            if normalized:
                products_by_normalized_name[normalized] = (p.id, p.sku)

        # Parse the file
        parse_result = parse_siesa_bytes(
            file_content=content,
            filename=file.filename or "siesa.xls",
            snapshot_date=actual_date,
            products_by_siesa_item=products_by_siesa_item,
            products_by_normalized_name=products_by_normalized_name,
        )

        # Get database client
        db = get_supabase_client()

        # Delete existing lots for this snapshot_date (idempotent)
        delete_result = db.table("inventory_lots").delete().eq(
            "snapshot_date", actual_date.isoformat()
        ).execute()
        deleted_count = len(delete_result.data) if delete_result.data else 0
        if deleted_count > 0:
            logger.info("siesa_deleted_existing_lots", count=deleted_count, date=actual_date)

        # Insert matched lots (batch insert for performance)
        lots_to_insert = []
        for match in parse_result.matched_lots:
            lot = match.lot
            lots_to_insert.append({
                "product_id": match.product_id,
                "lot_number": lot.lot_number,
                "quantity_m2": float(lot.quantity_m2),
                "weight_kg": float(lot.weight_kg) if lot.weight_kg else None,
                "quality": lot.quality,
                "warehouse_code": lot.warehouse_code,
                "warehouse_name": lot.warehouse_name,
                "snapshot_date": actual_date.isoformat(),
                "siesa_item": lot.siesa_item,
                "siesa_description": lot.siesa_description,
            })

        lots_created = 0
        if lots_to_insert:
            # Batch insert in chunks of 100 for better performance
            chunk_size = 100
            for i in range(0, len(lots_to_insert), chunk_size):
                chunk = lots_to_insert[i:i + chunk_size]
                db.table("inventory_lots").insert(chunk).execute()
                lots_created += len(chunk)

        # ===================
        # SYNC TO FACTORY_SNAPSHOTS
        # ===================
        # Aggregate lots by product_id and upsert to factory_snapshots
        # SIESA data is FACTORY finished goods — independent table, no carry-forward needed
        if lots_created > 0:
            # Aggregate quantities and find largest lot per product
            product_stats: dict[str, dict] = {}
            for lot in lots_to_insert:
                pid = lot["product_id"]
                qty = lot["quantity_m2"]
                lot_num = lot.get("lot_number", "")

                if pid not in product_stats:
                    product_stats[pid] = {
                        "total_m2": 0.0,
                        "lot_count": 0,
                        "largest_lot_m2": 0.0,
                        "largest_lot_code": "",
                    }

                stats = product_stats[pid]
                stats["total_m2"] += qty
                stats["lot_count"] += 1

                if qty > stats["largest_lot_m2"]:
                    stats["largest_lot_m2"] = qty
                    stats["largest_lot_code"] = lot_num

            synced_count = 0
            for pid, stats in product_stats.items():
                db.table("factory_snapshots").upsert({
                    "product_id": pid,
                    "snapshot_date": actual_date.isoformat(),
                    "factory_available_m2": stats["total_m2"],
                    "factory_lot_count": stats["lot_count"],
                    "factory_largest_lot_m2": stats["largest_lot_m2"],
                    "factory_largest_lot_code": stats["largest_lot_code"],
                }, on_conflict="product_id,snapshot_date").execute()
                synced_count += 1

            logger.info(
                "siesa_synced_to_factory_snapshots",
                products_synced=synced_count,
                date=actual_date.isoformat(),
            )

        # Calculate container statistics
        total_weight = float(parse_result.total_weight_kg)
        containers_needed = calculate_containers_needed(total_weight, CONTAINER_WEIGHT_LIMIT_KG)
        utilization = 0.0
        if containers_needed > 0:
            utilization = (total_weight / (containers_needed * CONTAINER_WEIGHT_LIMIT_KG)) * 100

        # Build warehouse summaries
        warehouse_summaries = [
            WarehouseSummary(
                code=wh.code,
                name=wh.name,
                total_m2=float(wh.total_m2),
                total_weight_kg=float(wh.total_weight_kg),
                lot_count=wh.lot_count,
            )
            for wh in parse_result.warehouses.values()
        ]

        # Build error list
        errors = [
            RowError(
                row=e.row,
                field=e.field,
                error=e.error,
                value=e.value,
            )
            for e in parse_result.errors
        ]

        # Calculate match rate
        total_matched = parse_result.matched_by_siesa_item + parse_result.matched_by_name
        total_processed = total_matched + parse_result.unmatched_count
        match_rate = (total_matched / total_processed * 100) if total_processed > 0 else 0.0

        # Get unmatched product descriptions
        unmatched_products = list(set(
            m.lot.siesa_description or f"Item {m.lot.siesa_item}"
            for m in parse_result.unmatched_lots
        ))[:20]  # Limit to 20

        logger.info(
            "siesa_upload_complete",
            lots_created=lots_created,
            matched_by_siesa_item=parse_result.matched_by_siesa_item,
            matched_by_name=parse_result.matched_by_name,
            unmatched=parse_result.unmatched_count,
            match_rate_pct=round(match_rate, 1),
            total_m2=float(parse_result.total_m2),
            containers_needed=containers_needed,
        )

        return SIESAUploadResponse(
            success=parse_result.success,
            snapshot_date=actual_date,
            total_rows=parse_result.total_rows,
            processed_rows=parse_result.processed_rows,
            skipped_errors=parse_result.skipped_errors,
            errors=errors,
            lots_created=lots_created,
            unique_products=parse_result.unique_siesa_items,
            total_m2_available=float(parse_result.total_m2),
            total_weight_kg=float(parse_result.total_weight_kg),
            container_limit_kg=CONTAINER_WEIGHT_LIMIT_KG,
            containers_needed=containers_needed,
            container_utilization_pct=round(utilization, 1),
            matched_by_siesa_item=parse_result.matched_by_siesa_item,
            matched_by_name=parse_result.matched_by_name,
            unmatched_count=parse_result.unmatched_count,
            match_rate_pct=round(match_rate, 1),
            unmatched_products=unmatched_products,
            warehouses=warehouse_summaries,
        )

    except SIESAMissingColumnsError as e:
        logger.error("siesa_missing_columns", missing=e.details.get("missing_columns"))
        return handle_error(e)
    except SIESAParseError as e:
        logger.error("siesa_parse_error", error=str(e))
        return handle_error(e)
    except Exception as e:
        logger.error("siesa_upload_failed", error=str(e), type=type(e).__name__)
        return handle_error(e)


# ===================
# IN-TRANSIT / DISPATCH
# ===================

M2_PER_PALLET = Decimal("134.4")
RECONCILIATION_TOLERANCE_M2 = 10.0  # m² difference considered a "match"


def _reconcile_dispatch_vs_drafts(db, parse_result, products) -> Optional[ReconciliationSummary]:
    """Compare dispatch upload quantities against ordered/confirmed drafts."""
    try:
        # Get all ordered/confirmed drafts
        drafts_result = (
            db.table("boat_factory_drafts")
            .select("id, boat_id, status")
            .in_("status", ["ordered", "confirmed"])
            .execute()
        )
        if not drafts_result.data:
            return None

        # Get draft items
        draft_ids = [d["id"] for d in drafts_result.data]
        items_result = (
            db.table("draft_items")
            .select("draft_id, product_id, selected_pallets")
            .in_("draft_id", draft_ids)
            .execute()
        )

        # Get boat names for display
        boat_ids = list({d["boat_id"] for d in drafts_result.data})
        boats_result = (
            db.table("boat_schedules")
            .select("id, vessel_name")
            .in_("id", boat_ids)
            .execute()
        )
        boat_names = {b["id"]: b["vessel_name"] for b in (boats_result.data or [])}
        draft_boat = {d["id"]: d["boat_id"] for d in drafts_result.data}

        # Aggregate draft m² per product (across all ordered/confirmed drafts)
        draft_m2_by_pid: dict[str, float] = defaultdict(float)
        draft_boat_by_pid: dict[str, str] = {}
        for item in (items_result.data or []):
            pid = item["product_id"]
            pallets = item["selected_pallets"]
            m2 = float(Decimal(str(pallets)) * M2_PER_PALLET)
            draft_m2_by_pid[pid] += m2
            # Track boat name (use latest if multiple)
            boat_id = draft_boat.get(item["draft_id"])
            if boat_id:
                draft_boat_by_pid[pid] = boat_names.get(boat_id, "")

        # Build SKU lookup from products
        pid_to_sku = {p.id: p.sku for p in products}

        # Dispatch m² by product
        dispatch_m2_by_pid = {p.product_id: p.in_transit_m2 for p in parse_result.products}

        # All product IDs from both sources
        all_pids = set(dispatch_m2_by_pid.keys()) | set(draft_m2_by_pid.keys())

        items: list[ReconciliationItem] = []
        matched = mismatched = dispatch_only = draft_only = 0

        for pid in sorted(all_pids, key=lambda p: pid_to_sku.get(p, "")):
            d_m2 = dispatch_m2_by_pid.get(pid, 0.0)
            dr_m2 = draft_m2_by_pid.get(pid, 0.0)
            diff = round(d_m2 - dr_m2, 2)
            sku = pid_to_sku.get(pid, pid[:8])
            boat_name = draft_boat_by_pid.get(pid)

            if d_m2 > 0 and dr_m2 == 0:
                status = "dispatch_only"
                dispatch_only += 1
            elif d_m2 == 0 and dr_m2 > 0:
                status = "draft_only"
                draft_only += 1
            elif abs(diff) <= RECONCILIATION_TOLERANCE_M2:
                status = "match"
                matched += 1
            else:
                status = "mismatch"
                mismatched += 1

            # Only include non-matches and mismatches to keep response concise
            if status != "match":
                items.append(ReconciliationItem(
                    sku=sku,
                    dispatch_m2=round(d_m2, 2),
                    draft_m2=round(dr_m2, 2),
                    diff_m2=diff,
                    status=status,
                    boat_name=boat_name,
                ))

        return ReconciliationSummary(
            matched=matched,
            mismatched=mismatched,
            dispatch_only=dispatch_only,
            draft_only=draft_only,
            items=items,
        )
    except Exception as e:
        logger.error("reconciliation_failed", error=str(e))
        return None


@router.post("/in-transit/upload", response_model=InTransitUploadResponse)
async def upload_in_transit(
    file: UploadFile = File(...),
    snapshot_date: Optional[date] = Query(None, description="Target snapshot date. If omitted, uses the latest existing snapshot date."),
    received_orders: Optional[str] = Query(None, description="Comma-separated order numbers to exclude (e.g., 'OC002,OC003')"),
):
    """
    Upload dispatch schedule to update in-transit quantities.

    Parses PROGRAMACIÓN DE DESPACHO Excel and upserts to transit_snapshots.
    Each upload only touches in_transit_qty — warehouse and factory data
    are in separate tables and cannot be affected.

    Products in the dispatch file get their aggregated m².
    Products NOT in the dispatch file get in_transit_qty reset to 0.
    """
    # Parse received_orders from comma-separated string
    excluded = []
    if received_orders:
        excluded = [o.strip() for o in received_orders.split(",") if o.strip()]

    # Auto-resolve snapshot_date: default to today (no longer tied to inventory_snapshots dates)
    if snapshot_date is None:
        snapshot_date = date.today()

    logger.info(
        "in_transit_upload_started",
        filename=file.filename,
        snapshot_date=snapshot_date,
        excluded_orders=excluded,
    )

    try:
        content = await file.read()

        if len(content) == 0:
            return JSONResponse(
                status_code=400,
                content={"error": {"code": "EMPTY_FILE", "message": "Empty file uploaded"}}
            )

        # Get active products for SKU matching
        product_service = get_product_service()
        products, _ = product_service.get_all(page=1, page_size=1000, active_only=True)

        # Parse the dispatch file
        from parsers.dispatch_parser import parse_dispatch_excel
        parse_result = parse_dispatch_excel(content, products, excluded)

        # Get database client
        db = get_supabase_client()

        # Build set of product IDs with in-transit stock
        in_transit_pids = {p.product_id for p in parse_result.products}

        # Get all active product IDs for resetting non-dispatch products
        all_active_pids = {p.id for p in products}

        # Reset in_transit_qty=0 for active products NOT in dispatch file
        reset_pids = all_active_pids - in_transit_pids
        reset_count = 0
        for pid in reset_pids:
            db.table("transit_snapshots").upsert({
                "product_id": pid,
                "snapshot_date": snapshot_date.isoformat(),
                "in_transit_qty": 0,
            }, on_conflict="product_id,snapshot_date").execute()
            reset_count += 1

        # Upsert in_transit_qty for products in the dispatch file
        updated_count = 0
        details = []
        for product in parse_result.products:
            db.table("transit_snapshots").upsert({
                "product_id": product.product_id,
                "snapshot_date": snapshot_date.isoformat(),
                "in_transit_qty": product.in_transit_m2,
            }, on_conflict="product_id,snapshot_date").execute()
            updated_count += 1
            details.append({"sku": product.sku, "in_transit_m2": product.in_transit_m2})

        logger.info(
            "in_transit_upload_complete",
            products_updated=updated_count,
            products_reset=reset_count,
            total_m2=parse_result.total_m2,
            snapshot_date=snapshot_date.isoformat(),
        )

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type="in_transit",
            file_hash=hashlib.md5(content).hexdigest(),
            filename=file.filename or "in_transit.xlsx",
            row_count=updated_count,
        )

        # --- Reconciliation: compare dispatch vs ordered/confirmed drafts ---
        reconciliation = _reconcile_dispatch_vs_drafts(db, parse_result, products)

        return InTransitUploadResponse(
            success=True,
            snapshot_date=snapshot_date,
            products_updated=updated_count,
            products_reset=reset_count,
            total_in_transit_m2=parse_result.total_m2,
            excluded_orders=excluded,
            reconciliation=reconciliation,
            unmatched_skus=parse_result.unmatched_skus,
            details=details,
        )

    except ValueError as e:
        logger.error("in_transit_parse_error", error=str(e))
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "PARSE_ERROR", "message": str(e)}}
        )
    except Exception as e:
        logger.error("in_transit_upload_failed", error=str(e), type=type(e).__name__)
        return handle_error(e)


@router.get("/siesa/lots", response_model=InventoryLotsListResponse)
async def list_inventory_lots(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    product_id: Optional[str] = Query(None, description="Filter by product"),
    snapshot_date: Optional[date] = Query(None, description="Filter by snapshot date"),
    warehouse_code: Optional[str] = Query(None, description="Filter by warehouse"),
):
    """
    List inventory lots with optional filters.

    Returns paginated list ordered by snapshot_date descending.
    """
    try:
        db = get_supabase_client()

        # Build query
        query = db.table("inventory_lots").select("*", count="exact")

        if product_id:
            query = query.eq("product_id", product_id)
        if snapshot_date:
            query = query.eq("snapshot_date", snapshot_date.isoformat())
        if warehouse_code:
            query = query.eq("warehouse_code", warehouse_code)

        # Order and paginate
        query = query.order("snapshot_date", desc=True).order("created_at", desc=True)
        offset = (page - 1) * page_size
        query = query.range(offset, offset + page_size - 1)

        result = query.execute()
        total = result.count or 0
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0

        lots = [
            InventoryLotResponse(
                id=row["id"],
                product_id=row["product_id"],
                lot_number=row["lot_number"],
                quantity_m2=row["quantity_m2"],
                weight_kg=row.get("weight_kg"),
                quality=row.get("quality"),
                warehouse_code=row.get("warehouse_code"),
                warehouse_name=row.get("warehouse_name"),
                snapshot_date=row["snapshot_date"],
                siesa_item=row.get("siesa_item"),
                siesa_description=row.get("siesa_description"),
                created_at=row.get("created_at"),
                updated_at=row.get("updated_at"),
            )
            for row in result.data
        ]

        return InventoryLotsListResponse(
            data=lots,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    except Exception as e:
        logger.error("list_lots_failed", error=str(e))
        return handle_error(e)


@router.get("/siesa/containers", response_model=ContainerEstimateResponse)
async def estimate_containers(
    snapshot_date: Optional[date] = Query(None, description="Snapshot date (defaults to latest)")
):
    """
    Calculate container requirements based on current inventory weight.

    Returns breakdown of weight distribution across containers.
    """
    try:
        db = get_supabase_client()

        # Get snapshot date
        if snapshot_date is None:
            # Get most recent date
            result = db.table("inventory_lots").select("snapshot_date").order(
                "snapshot_date", desc=True
            ).limit(1).execute()
            if not result.data:
                return ContainerEstimateResponse(
                    weight_kg=0,
                    container_limit_kg=CONTAINER_WEIGHT_LIMIT_KG,
                    containers_needed=0,
                    utilization_breakdown=[],
                )
            snapshot_date = result.data[0]["snapshot_date"]

        # Sum weights for date
        result = db.table("inventory_lots").select("weight_kg").eq(
            "snapshot_date", snapshot_date if isinstance(snapshot_date, str) else snapshot_date.isoformat()
        ).execute()

        total_weight = sum(
            float(row["weight_kg"] or 0)
            for row in result.data
        )

        containers_needed = calculate_containers_needed(total_weight, CONTAINER_WEIGHT_LIMIT_KG)
        breakdown = calculate_utilization_breakdown(total_weight, CONTAINER_WEIGHT_LIMIT_KG)

        return ContainerEstimateResponse(
            weight_kg=round(total_weight, 2),
            container_limit_kg=CONTAINER_WEIGHT_LIMIT_KG,
            containers_needed=containers_needed,
            utilization_breakdown=breakdown,
        )

    except Exception as e:
        logger.error("container_estimate_failed", error=str(e))
        return handle_error(e)


@router.get("/history/{product_id}", response_model=list[InventorySnapshotResponse])
async def get_inventory_history(
    product_id: str,
    limit: int = Query(30, ge=1, le=365, description="Max records to return")
):
    """
    Get inventory history for a specific product.

    Returns snapshots ordered by date descending.
    """
    try:
        service = get_inventory_service()
        return service.get_history(product_id, limit=limit)

    except Exception as e:
        return handle_error(e)


@router.get("", response_model=InventoryListResponse)
async def list_inventory(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    product_id: Optional[str] = Query(None, description="Filter by product")
):
    """
    List all inventory snapshots with optional filters.

    Returns paginated list ordered by date descending.
    """
    try:
        service = get_inventory_service()

        snapshots, total = service.get_all(
            page=page,
            page_size=page_size,
            product_id=product_id
        )

        total_pages = (total + page_size - 1) // page_size

        return InventoryListResponse(
            data=snapshots,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        return handle_error(e)


@router.get("/{snapshot_id}", response_model=InventorySnapshotResponse)
async def get_inventory_snapshot(snapshot_id: str):
    """
    Get a single inventory snapshot by ID.

    Raises:
        404: Snapshot not found
    """
    try:
        service = get_inventory_service()
        return service.get_by_id(snapshot_id)

    except InventoryNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.post("", response_model=InventorySnapshotResponse, status_code=201)
async def create_inventory_snapshot(data: InventorySnapshotCreate):
    """
    Create a new inventory snapshot.

    Raises:
        422: Validation error
    """
    try:
        service = get_inventory_service()
        return service.create(data)

    except Exception as e:
        return handle_error(e)


@router.patch("/{snapshot_id}", response_model=InventorySnapshotResponse)
async def update_inventory_snapshot(snapshot_id: str, data: InventorySnapshotUpdate):
    """
    Update an existing inventory snapshot.

    Only provided fields are updated.

    Raises:
        404: Snapshot not found
        422: Validation error
    """
    try:
        service = get_inventory_service()
        return service.update(snapshot_id, data)

    except InventoryNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.delete("/{snapshot_id}", status_code=204)
async def delete_inventory_snapshot(snapshot_id: str):
    """
    Delete an inventory snapshot.

    Raises:
        404: Snapshot not found
    """
    try:
        service = get_inventory_service()
        service.delete(snapshot_id)
        return None  # 204 No Content

    except InventoryNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# UTILITY ROUTES
# ===================

@router.get("/count/total")
async def count_inventory_snapshots(
    product_id: Optional[str] = Query(None, description="Filter by product")
):
    """Get total inventory snapshot count."""
    try:
        service = get_inventory_service()
        count = service.count(product_id=product_id)
        return {"count": count}

    except Exception as e:
        return handle_error(e)
