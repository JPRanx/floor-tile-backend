"""
Data freshness endpoint for the Data Hub.

Returns timestamps and status for all data sources.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Query
import structlog

from config import get_supabase_client

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/data-freshness", tags=["Data Freshness"])


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """
    Robustly parse timestamp strings from Supabase.

    Handles formats like:
    - '2025-12-11' (date only)
    - '2025-12-11T12:08:22' (datetime without tz)
    - '2025-12-11T12:08:22Z' (UTC)
    - '2025-12-11T12:08:22+00:00' (with tz offset)
    - '2025-12-11T12:08:22.90549+00:00' (with microseconds and tz)
    """
    if not ts:
        return None

    try:
        # Handle Z suffix
        ts = ts.replace("Z", "+00:00")

        # Try parsing as full datetime with timezone
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            pass

        # Try parsing date-only format
        if len(ts) == 10:  # YYYY-MM-DD
            return datetime.strptime(ts, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Try without timezone (naive datetime)
        if "+" not in ts and "-" not in ts[10:]:
            dt = datetime.fromisoformat(ts)
            return dt.replace(tzinfo=timezone.utc)

        # Fallback: strip timezone and parse
        if "+" in ts:
            ts_base = ts.rsplit("+", 1)[0]
        elif ts.count("-") > 2:  # Has negative offset like -05:00
            ts_base = ts.rsplit("-", 1)[0]
        else:
            ts_base = ts

        dt = datetime.fromisoformat(ts_base)
        return dt.replace(tzinfo=timezone.utc)

    except Exception:
        return None


def _get_freshness_status(last_updated: Optional[datetime]) -> str:
    """
    Determine freshness status based on age.

    - fresh: Updated within 24 hours
    - stale: 24-72 hours old
    - very_stale: More than 72 hours old
    """
    if last_updated is None:
        return "very_stale"

    now = datetime.now(timezone.utc)
    # Handle naive datetime
    if last_updated.tzinfo is None:
        last_updated = last_updated.replace(tzinfo=timezone.utc)

    age = now - last_updated

    if age <= timedelta(hours=24):
        return "fresh"
    elif age <= timedelta(hours=72):
        return "stale"
    else:
        return "very_stale"


@router.get("")
async def get_data_freshness():
    """
    Get freshness status for all data sources.

    Returns timestamps and status for:
    - Sales (from sales table, most recent week_start)
    - Inventory (from inventory_lots table, most recent snapshot_date)
    - Boats (from boat_schedules table, most recent created_at)
    """
    db = get_supabase_client()

    # Sales freshness - get most recent week_start
    sales_result = db.table("sales").select(
        "week_start", count="exact"
    ).order("week_start", desc=True).limit(1).execute()

    sales_last_updated = None
    sales_count = sales_result.count or 0
    if sales_result.data and len(sales_result.data) > 0:
        week_start = sales_result.data[0].get("week_start")
        sales_last_updated = _parse_timestamp(week_start)

    # Inventory freshness - from inventory_current (consolidated view)
    inventory_result = db.table("inventory_current").select(
        "warehouse_date, snapshot_date", count="exact"
    ).order("snapshot_date", desc=True).limit(1).execute()

    inventory_last_updated = None
    inventory_count = inventory_result.count or 0
    if inventory_result.data and len(inventory_result.data) > 0:
        # Use snapshot_date (most recent update across all sources)
        snapshot_date = inventory_result.data[0].get("snapshot_date")
        inventory_last_updated = _parse_timestamp(snapshot_date)

    # Boats freshness - get most recent updated_at (reflects last upload, not creation)
    boats_result = db.table("boat_schedules").select(
        "updated_at", count="exact"
    ).order("updated_at", desc=True).limit(1).execute()

    boats_last_updated = None
    boats_count = boats_result.count or 0
    if boats_result.data and len(boats_result.data) > 0:
        updated_at = boats_result.data[0].get("updated_at")
        boats_last_updated = _parse_timestamp(updated_at)

    logger.info(
        "data_freshness_checked",
        sales_count=sales_count,
        inventory_count=inventory_count,
        boats_count=boats_count,
    )

    return {
        "sales": {
            "last_updated": sales_last_updated.isoformat() if sales_last_updated else None,
            "record_count": sales_count,
            "status": _get_freshness_status(sales_last_updated),
        },
        "inventory": {
            "last_updated": inventory_last_updated.isoformat() if inventory_last_updated else None,
            "record_count": inventory_count,
            "status": _get_freshness_status(inventory_last_updated),
        },
        "boats": {
            "last_updated": boats_last_updated.isoformat() if boats_last_updated else None,
            "record_count": boats_count,
            "status": _get_freshness_status(boats_last_updated),
        },
    }


UPLOAD_TYPE_LABELS = {
    "sales": "Ventas (SAC)",
    "inventory": "Inventario",
    "siesa": "Inventario (SIESA)",
    "boats": "Barcos",
    "in_transit": "Despacho / En Transito",
    "production_schedule": "Programacion Produccion",
    "shipment_pdf": "BL / Embarque",
}


@router.get("/upload-history")
async def get_upload_history(
    limit: int = Query(20, ge=1, le=100),
):
    """Recent upload history for the Data Hub activity log."""
    db = get_supabase_client()
    result = (
        db.table("upload_history")
        .select("upload_type, filename, row_count, uploaded_at")
        .order("uploaded_at", desc=True)
        .limit(limit)
        .execute()
    )
    items = []
    for row in (result.data or []):
        items.append({
            "upload_type": row["upload_type"],
            "label": UPLOAD_TYPE_LABELS.get(row["upload_type"], row["upload_type"]),
            "filename": row["filename"],
            "row_count": row["row_count"],
            "uploaded_at": row["uploaded_at"],
        })
    return {"items": items}
