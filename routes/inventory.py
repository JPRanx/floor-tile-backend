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
from services.inventory_service import get_inventory_service
from services.product_service import get_product_service
from parsers.excel_parser import parse_owner_excel
from exceptions import (
    AppError,
    InventoryNotFoundError,
    InventoryUploadError,
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
    Validates all SKUs exist before inserting.
    Rejects entire upload if any row has errors.

    Raises:
        422: Validation error (invalid SKU, missing columns, etc.)
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

        # Get known SKUs from products table
        product_service = get_product_service()
        products, _ = product_service.get_all(page=1, page_size=1000, active_only=True)
        known_skus = {p.sku: p.id for p in products}

        # Parse Excel file
        parse_result = parse_owner_excel(file_obj, known_skus)

        # Check for errors
        if not parse_result.success:
            logger.warning(
                "inventory_upload_validation_failed",
                error_count=len(parse_result.errors)
            )
            raise InventoryUploadError([
                {
                    "sheet": e.sheet,
                    "row": e.row,
                    "field": e.field,
                    "error": e.error
                }
                for e in parse_result.errors
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

        # Bulk insert
        if snapshots_to_create:
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
