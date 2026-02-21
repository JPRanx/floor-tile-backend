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


def _latest_from_table(db, table: str, date_col: str, count: bool = False):
    """Query most recent timestamp and optional count from a table."""
    cols = date_col
    kw = {"count": "exact"} if count else {}
    result = db.table(table).select(cols, **kw).order(
        date_col, desc=True
    ).limit(1).execute()

    last_updated = None
    record_count = result.count if count else None
    if result.data and len(result.data) > 0:
        last_updated = _parse_timestamp(result.data[0].get(date_col))
    return last_updated, record_count


@router.get("")
async def get_data_freshness():
    """
    Get freshness status for all data sources.

    Returns timestamps and status for:
    - Sales (from sales table, most recent week_start)
    - Warehouse inventory (from warehouse_snapshots)
    - SIESA / factory inventory (from factory_snapshots)
    - In-transit inventory (from transit_snapshots)
    - Boats (from boat_schedules, most recent updated_at)
    - Production schedule (from production_schedule)
    """
    db = get_supabase_client()

    sales_ts, sales_count = _latest_from_table(db, "sales", "week_start", count=True)
    warehouse_ts, warehouse_count = _latest_from_table(db, "warehouse_snapshots", "snapshot_date", count=True)
    siesa_ts, siesa_count = _latest_from_table(db, "factory_snapshots", "snapshot_date", count=True)
    transit_ts, transit_count = _latest_from_table(db, "transit_snapshots", "snapshot_date", count=True)
    boats_ts, boats_count = _latest_from_table(db, "boat_schedules", "updated_at", count=True)
    production_ts, _ = _latest_from_table(db, "production_schedule", "updated_at")

    logger.info(
        "data_freshness_checked",
        sales_count=sales_count,
        warehouse_count=warehouse_count,
        siesa_count=siesa_count,
        transit_count=transit_count,
        boats_count=boats_count,
    )

    def _source(ts, count=None):
        out = {
            "last_updated": ts.isoformat() if ts else None,
            "status": _get_freshness_status(ts),
        }
        if count is not None:
            out["record_count"] = count
        return out

    return {
        "sales": _source(sales_ts, sales_count),
        "inventory": _source(warehouse_ts, warehouse_count),
        "siesa": _source(siesa_ts, siesa_count),
        "in_transit": _source(transit_ts, transit_count),
        "boats": _source(boats_ts, boats_count),
        "production": _source(production_ts),
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
