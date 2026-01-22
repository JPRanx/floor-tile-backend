"""
Inventory API routes.

See BUILDER_BLUEPRINT.md for endpoint specifications.
See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query, UploadFile, File
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from io import BytesIO
import structlog

from models.inventory import (
    InventorySnapshotCreate,
    InventorySnapshotUpdate,
    InventorySnapshotResponse,
    InventoryListResponse,
    InventoryCurrentResponse,
    InventoryUploadResponse,
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
)
from parsers.siesa_parser import parse_siesa_bytes
from config.shipping import (
    calculate_containers_needed,
    calculate_utilization_breakdown,
    CONTAINER_WEIGHT_LIMIT_KG,
)
from utils.text_utils import normalize_product_name
from config import get_supabase_client

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

        if snapshots_to_create:
            # Make upload idempotent: delete existing records for these dates
            unique_dates = list(set(s.snapshot_date for s in snapshots_to_create))
            deleted = inventory_service.delete_by_dates(unique_dates)
            if deleted > 0:
                logger.info("inventory_deleted_before_upload", count=deleted)

            # Bulk insert
            inventory_service.bulk_create(snapshots_to_create)

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
