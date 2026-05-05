"""
Horizon endpoint — the one brain.

GET /api/v2/horizon/{factory_id}         → summary per boat (Planning View)
GET /api/v2/horizon/{factory_id}/{boat_id} → full detail for one boat (OB)

Route → DB queries → brain → respond. No services.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from collections import defaultdict


def _parse_ts(value):
    """Parse a Supabase ISO timestamp string into a timezone-aware datetime.
    Returns None for None/empty. Datetimes pass through unchanged."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    # Supabase returns timestamps like "2026-05-04T12:30:41.89762+00:00"
    return datetime.fromisoformat(str(value))

from fastapi import APIRouter, HTTPException
import structlog

from config import get_supabase_client
from lib.brain import compute_horizon

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/v2/horizon", tags=["horizon"])


def _merge_dispatched_dupes(projections: list[dict]) -> list[dict]:
    """
    Merge dispatched/confirmed boats with the same vessel+date into one card.
    Multiple BLs on one ship → one combined projection for the UI.
    Future/planning boats are left as-is.
    """
    merged = []
    groups: dict[tuple, list[dict]] = {}

    for p in projections:
        if p["state"] in ("DISPATCHED", "CONFIRMED"):
            key = (p["boat_name"], p["departure_date"])
            groups.setdefault(key, []).append(p)
        else:
            merged.append(p)

    for key, boats in groups.items():
        if len(boats) == 1:
            merged.append(boats[0])
            continue

        # Merge: combine products, sum totals, collect boat_ids
        base = dict(boats[0])
        product_totals: dict[str, dict] = {}

        for boat in boats:
            for prod in boat["products"]:
                pid = prod["product_id"]
                if pid in product_totals:
                    product_totals[pid]["allocated_pallets"] += prod["allocated_pallets"]
                else:
                    product_totals[pid] = dict(prod)

        base["products"] = list(product_totals.values())
        base["total_pallets"] = sum(p["allocated_pallets"] for p in product_totals.values())
        base["total_m2"] = base["total_pallets"] * 134.4
        base["total_containers"] = base["total_pallets"] // 13
        base["product_count"] = len([p for p in product_totals.values() if p["allocated_pallets"] > 0])
        base["merged_boat_ids"] = [b["boat_id"] for b in boats]
        base["bl_count"] = len(boats)
        base["skip_recommended"] = False

        merged.append(base)

    # Re-sort by departure date
    merged.sort(key=lambda p: p["departure_date"])
    return merged


def _query_inputs(factory_id: str, today: date) -> dict:
    """
    Fetch the 9 inputs the brain needs. Direct table queries, no services.
    Returns inputs dict + data_freshness dict for traceability.
    """
    db = get_supabase_client()
    freshness = {}

    # 1. Products (active tiles only — exclude furniture, sinks, surcharges)
    # Include `tier` so brain can use frozen A/B/C classification.
    tile_categories = ["MADERAS", "MARMOLIZADOS", "EXTERIORES"]
    products_res = db.table("products").select(
        "id, sku, category, active, tier"
    ).eq("active", True).in_("category", tile_categories).execute()
    products = [
        {
            "id": p["id"],
            "sku": p["sku"],
            "active": p.get("active", True),
            "tier": p.get("tier"),  # may be None — brain falls back to runtime classification
        }
        for p in products_res.data
    ]
    freshness["products"] = len(products)

    # 2. Boat schedules — only boats that haven't departed yet and aren't ignored
    boats_res = db.table("boat_schedules").select(
        "id, vessel_name, departure_date, arrival_date, carrier, status"
    ).gte("departure_date", today.isoformat()).neq(
        "status", "ignored"
    ).order("departure_date").execute()

    # Also fetch boats with shipment_items (anchor candidates, may have departed)
    shipment_boat_ids_res = db.table("shipment_items").select("boat_id").execute()
    anchor_boat_ids = {row["boat_id"] for row in shipment_boat_ids_res.data}

    if anchor_boat_ids:
        anchor_boats_res = db.table("boat_schedules").select(
            "id, vessel_name, departure_date, arrival_date, carrier"
        ).in_("id", list(anchor_boat_ids)).execute()
        anchor_boats_data = anchor_boats_res.data
    else:
        anchor_boats_data = []

    # Merge: anchor boats + upcoming boats, deduplicate by DB id
    seen_ids = set()
    boats = []
    for b in anchor_boats_data + boats_res.data:
        if b["id"] in seen_ids:
            continue
        seen_ids.add(b["id"])
        boats.append({
            "id": b["id"],
            "name": b["vessel_name"],
            "departure_date": b["departure_date"],
            "arrival_date": b["arrival_date"],
            "carrier": b.get("carrier", ""),
            "factory_id": factory_id,
        })
    freshness["boats"] = len(boats)

    # 3. Warehouse snapshots (latest per product)
    # Fetch only the most recent snapshot_date, then filter to that date
    latest_wh = db.table("warehouse_snapshots").select(
        "snapshot_date"
    ).order("snapshot_date", desc=True).limit(1).execute()

    inventory: dict[str, Decimal] = {}
    if latest_wh.data:
        wh_date = latest_wh.data[0]["snapshot_date"]
        warehouse_res = db.table("warehouse_snapshots").select(
            "product_id, warehouse_qty"
        ).eq("snapshot_date", wh_date).execute()
        for row in warehouse_res.data:
            pid = row["product_id"]
            inventory[pid] = Decimal(str(row.get("warehouse_qty") or 0))
        freshness["warehouse_snapshot_date"] = wh_date
    else:
        freshness["warehouse_snapshot_date"] = None

    # 4. Factory snapshots (latest per product)
    # Pull created_at as well: the brain needs to know WHEN the snapshot was
    # uploaded, so it can decide which drafts the factory already accounted for
    # (pre-snapshot) vs which ones it doesn't know about yet (post-snapshot).
    latest_fs = db.table("factory_snapshots").select(
        "snapshot_date, created_at"
    ).order("snapshot_date", desc=True).limit(1).execute()

    factory_stock: dict[str, Decimal] = {}
    snapshot_created_at = None
    if latest_fs.data:
        fs_date = latest_fs.data[0]["snapshot_date"]
        snapshot_created_at = _parse_ts(latest_fs.data[0].get("created_at"))
        factory_res = db.table("factory_snapshots").select(
            "product_id, factory_available_m2"
        ).eq("snapshot_date", fs_date).execute()
        for row in factory_res.data:
            pid = row["product_id"]
            factory_stock[pid] = Decimal(str(row.get("factory_available_m2") or 0))
        freshness["factory_snapshot_date"] = fs_date
        freshness["factory_snapshot_uploaded_at"] = snapshot_created_at
    else:
        freshness["factory_snapshot_date"] = None
        freshness["factory_snapshot_uploaded_at"] = None

    # 4b. Transit snapshots (latest per product — stock on the way)
    latest_tr = db.table("transit_snapshots").select(
        "snapshot_date"
    ).order("snapshot_date", desc=True).limit(1).execute()

    in_transit: dict[str, Decimal] = {}
    if latest_tr.data:
        tr_date = latest_tr.data[0]["snapshot_date"]
        transit_res = db.table("transit_snapshots").select(
            "product_id, in_transit_qty"
        ).eq("snapshot_date", tr_date).execute()
        for row in transit_res.data:
            pid = row["product_id"]
            qty = Decimal(str(row.get("in_transit_qty") or 0))
            if qty > 0:
                in_transit[pid] = qty
        freshness["transit_snapshot_date"] = tr_date
    else:
        freshness["transit_snapshot_date"] = None

    # 5. Sales → velocity (90-day simple average) + peak velocity (for tier A buffer)
    sales_start = (today - timedelta(days=90)).isoformat()
    sales_res = db.table("sales").select(
        "product_id, quantity_m2, week_start"
    ).gte("week_start", sales_start).execute()
    sales_totals: dict[str, Decimal] = defaultdict(Decimal)
    # Per-week aggregates for peak detection
    sales_by_week: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    for row in sales_res.data:
        pid = row.get("product_id")
        if not pid:
            continue
        qty = Decimal(str(row.get("quantity_m2") or 0))
        sales_totals[pid] += qty
        wk = str(row.get("week_start") or "")
        sales_by_week[pid][wk] += qty
    velocities: dict[str, Decimal] = {
        pid: (total / 90).quantize(Decimal("0.01"))
        for pid, total in sales_totals.items()
    }
    # Peak velocity (m²/day equivalent of the highest-volume week in the 90d window).
    # Used for tier A buffer to absorb demand spikes — average velocity isn't enough
    # when a single big customer can buy 10x in one week.
    peak_velocities: dict[str, Decimal] = {}
    for pid, weeks in sales_by_week.items():
        if weeks:
            peak_week_m2 = max(weeks.values())
            peak_velocities[pid] = (peak_week_m2 / 7).quantize(Decimal("0.01"))
    freshness["sales_records"] = len(sales_res.data)
    freshness["sales_window_start"] = sales_start

    # 6. Shipment items (per-boat dispatch reality)
    shipment_res = db.table("shipment_items").select(
        "boat_id, product_id, shipped_m2"
    ).execute()
    shipment_items = [
        {
            "boat_id": row["boat_id"],
            "product_id": row["product_id"],
            "shipped_m2": str(row.get("shipped_m2") or 0),
        }
        for row in shipment_res.data
    ]
    freshness["shipment_items"] = len(shipment_items)

    # 7. Production schedule
    production_res = db.table("production_schedule").select(
        "product_id, status, requested_m2, completed_m2, scheduled_start_date"
    ).in_("status", ["scheduled", "in_progress", "requested"]).execute()
    production_schedule = [
        {
            "product_id": row["product_id"],
            "status": row["status"],
            "requested_m2": str(row.get("requested_m2") or 0),
            "completed_m2": str(row.get("completed_m2") or 0),
            "scheduled_date": row.get("scheduled_start_date"),
        }
        for row in production_res.data
    ]
    freshness["production_records"] = len(production_schedule)

    # 8. Drafts — ALL drafts for this factory. No status filter.
    #    The brain decides what to do based on draft existence, not status.
    #    Status is a UI/notification concern, not a simulation concern.
    drafts_res = db.table("boat_factory_drafts").select(
        "id, boat_id, factory_id, status, ordered_at"
    ).eq("factory_id", factory_id).execute()

    # Draft headers: tells the brain which boats have drafts (even empty ones).
    # ordered_at lets the brain decide whether the snapshot already accounted
    # for this draft's commitments.
    draft_headers = [
        {
            "boat_id": d["boat_id"],
            "status": d["status"],
            "draft_id": d["id"],
            "ordered_at": _parse_ts(d.get("ordered_at")),
        }
        for d in drafts_res.data
    ]

    drafts: list[dict] = []
    draft_ids = [d["id"] for d in drafts_res.data]
    if draft_ids:
        items_res = db.table("draft_items").select(
            "draft_id, product_id, selected_pallets"
        ).in_("draft_id", draft_ids).execute()

        draft_lookup = {d["id"]: d for d in drafts_res.data}
        for item in items_res.data:
            draft = draft_lookup[item["draft_id"]]
            drafts.append({
                "boat_id": draft["boat_id"],
                "product_id": item["product_id"],
                "selected_pallets": item["selected_pallets"],
                "status": draft["status"],
                "draft_id": draft["id"],
            })
    freshness["drafts"] = len(drafts)

    return {
        "products": products,
        "boats": boats,
        "inventory": inventory,
        "in_transit": in_transit,
        "velocities": velocities,
        "peak_velocities": peak_velocities,
        "factory_stock": factory_stock,
        "drafts": drafts,
        "draft_headers": draft_headers,
        "shipment_items": shipment_items,
        "production_schedule": production_schedule,
        "snapshot_created_at": snapshot_created_at,
        "_freshness": freshness,
    }


@router.get("/{factory_id}")
async def get_horizon(factory_id: str):
    """
    The brain. Summary per boat for the Planning View.
    """
    today = date.today()

    try:
        inputs = _query_inputs(factory_id, today)
    except Exception as e:
        logger.error("horizon_query_failed", factory_id=factory_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to query inputs: {e}")

    freshness = inputs.pop("_freshness")
    result = compute_horizon(**inputs, today=today)

    # Merge freshness into data_as_of
    result["data_as_of"].update(freshness)

    # Merge duplicate boats (same vessel+date) for dispatched/confirmed.
    # Multiple BLs on one ship → one card in the UI.
    result["projections"] = _merge_dispatched_dupes(result["projections"])

    # Fetch factory info for the response envelope
    db = get_supabase_client()
    factory_res = db.table("factories").select(
        "id, name, production_lead_days, transport_to_port_days, monthly_quota_m2"
    ).eq("id", factory_id).execute()

    factory = factory_res.data[0] if factory_res.data else {}

    return {
        "factory_id": factory_id,
        "factory_name": factory.get("name", ""),
        "generated_at": today.isoformat(),
        "production_lead_days": factory.get("production_lead_days", 25),
        "transport_to_port_days": factory.get("transport_to_port_days", 5),
        "monthly_quota_m2": factory.get("monthly_quota_m2"),
        "projections": result["projections"],
        "production_requests": result["production_requests"],
        "production_pipeline": result["production_pipeline"],
        "skip_recommendations": result["skip_recommendations"],
        "factory_order_signal": result["factory_order_signal"],
        "data_as_of": result["data_as_of"],
    }


@router.get("/{factory_id}/{boat_id}")
async def get_horizon_detail(factory_id: str, boat_id: str):
    """
    Detail for one boat — the Order Builder reads this.
    Same brain, filters to one boat, includes _debug.
    """
    today = date.today()

    try:
        inputs = _query_inputs(factory_id, today)
    except Exception as e:
        logger.error("horizon_detail_query_failed", factory_id=factory_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to query inputs: {e}")

    freshness = inputs.pop("_freshness")
    result = compute_horizon(**inputs, today=today)
    result["data_as_of"].update(freshness)

    # For dispatched/confirmed boats, find all projections with same vessel+date
    # and merge them (multiple BLs → one detail view).
    boat_projection = None
    target = None
    for p in result["projections"]:
        if p["boat_id"] == boat_id:
            target = p
            break

    if target and target["state"] in ("DISPATCHED", "CONFIRMED"):
        # Find siblings (same vessel+date)
        siblings = [
            p for p in result["projections"]
            if p["boat_name"] == target["boat_name"]
            and p["departure_date"] == target["departure_date"]
            and p["state"] in ("DISPATCHED", "CONFIRMED")
        ]
        if len(siblings) > 1:
            merged = _merge_dispatched_dupes(siblings)
            boat_projection = merged[0] if merged else target
        else:
            boat_projection = target
    elif target:
        boat_projection = target

    if not boat_projection:
        raise HTTPException(status_code=404, detail=f"Boat {boat_id} not found in horizon")

    # Find next boat for context (use merged projections)
    merged_projs = _merge_dispatched_dupes(result["projections"])
    next_boat = None
    for i, p in enumerate(merged_projs):
        bid = p.get("boat_id") or (p.get("merged_boat_ids", [None])[0])
        if bid == boat_id and i + 1 < len(merged_projs):
            nb = merged_projs[i + 1]
            next_boat = {
                "boat_id": nb.get("boat_id") or nb.get("merged_boat_ids", [None])[0],
                "boat_name": nb["boat_name"],
                "departure_date": nb["departure_date"],
                "arrival_date": nb["arrival_date"],
            }
            break

    # Find debug trace for this boat (may span multiple boat_ids if merged)
    boat_debug = None
    search_ids = boat_projection.get("merged_boat_ids", [boat_id])
    for d in result.get("_debug", []):
        if d["boat_id"] in search_ids:
            boat_debug = d
            break

    db = get_supabase_client()
    factory_res = db.table("factories").select(
        "id, name, production_lead_days, transport_to_port_days, monthly_quota_m2"
    ).eq("id", factory_id).execute()
    factory = factory_res.data[0] if factory_res.data else {}

    return {
        "factory_id": factory_id,
        "factory_name": factory.get("name", ""),
        "generated_at": today.isoformat(),
        "boat": boat_projection,
        "next_boat": next_boat,
        "production_requests": result["production_requests"],
        "factory_order_signal": result["factory_order_signal"],
        "data_as_of": result["data_as_of"],
        "_debug": boat_debug,
    }


@router.patch("/boats/{boat_id}/ignore")
async def ignore_boat(boat_id: str):
    """Mark a boat as ignored. Brain will skip it and cascade products to next boats."""
    db = get_supabase_client()
    result = db.table("boat_schedules").update(
        {"status": "ignored"}
    ).eq("id", boat_id).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Boat not found")
    return {"success": True, "boat_id": boat_id, "status": "ignored"}


@router.patch("/boats/{boat_id}/restore")
async def restore_boat(boat_id: str):
    """Restore an ignored boat back to available."""
    db = get_supabase_client()
    result = db.table("boat_schedules").update(
        {"status": "available"}
    ).eq("id", boat_id).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Boat not found")
    return {"success": True, "boat_id": boat_id, "status": "available"}
