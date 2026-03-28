"""
Horizon endpoint — the one brain.

GET /api/v2/horizon/{factory_id}         → summary per boat (Planning View)
GET /api/v2/horizon/{factory_id}/{boat_id} → full detail for one boat (OB)

Route → DB queries → brain → respond. No services.
"""

from datetime import date, timedelta
from decimal import Decimal
from collections import defaultdict

from fastapi import APIRouter, HTTPException
import structlog

from config import get_supabase_client
from lib.brain import compute_horizon

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/v2/horizon", tags=["horizon"])


def _query_inputs(factory_id: str, today: date) -> dict:
    """
    Fetch the 9 inputs the brain needs. Direct table queries, no services.
    Returns inputs dict + data_freshness dict for traceability.
    """
    db = get_supabase_client()
    freshness = {}

    # 1. Products (active tiles only — exclude furniture, sinks, surcharges)
    tile_categories = ["MADERAS", "MARMOLIZADOS", "EXTERIORES"]
    products_res = db.table("products").select(
        "id, sku, category, active"
    ).eq("active", True).in_("category", tile_categories).execute()
    products = [
        {"id": p["id"], "sku": p["sku"], "active": p.get("active", True)}
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
    raw_boats = []
    for b in anchor_boats_data + boats_res.data:
        if b["id"] in seen_ids:
            continue
        seen_ids.add(b["id"])
        raw_boats.append({
            "id": b["id"],
            "name": b["vessel_name"],
            "departure_date": b["departure_date"],
            "arrival_date": b["arrival_date"],
            "carrier": b.get("carrier", ""),
            "factory_id": factory_id,
        })

    # Dedup same vessel on same date (e.g. AIAS with 3 BLs → one boat for the brain).
    # Keep the first ID as canonical, build alias map for remapping downstream data.
    boat_alias: dict[str, str] = {}  # duplicate_id → canonical_id
    boats = []
    _seen_vessel_date: dict[tuple, str] = {}  # (name, dep) → canonical_id
    for b in raw_boats:
        key = (b["name"], b["departure_date"])
        if key in _seen_vessel_date:
            boat_alias[b["id"]] = _seen_vessel_date[key]
        else:
            _seen_vessel_date[key] = b["id"]
            boats.append(b)
    freshness["boats"] = len(boats)
    freshness["boat_aliases"] = len(boat_alias)

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
    latest_fs = db.table("factory_snapshots").select(
        "snapshot_date"
    ).order("snapshot_date", desc=True).limit(1).execute()

    factory_stock: dict[str, Decimal] = {}
    if latest_fs.data:
        fs_date = latest_fs.data[0]["snapshot_date"]
        factory_res = db.table("factory_snapshots").select(
            "product_id, factory_available_m2"
        ).eq("snapshot_date", fs_date).execute()
        for row in factory_res.data:
            pid = row["product_id"]
            factory_stock[pid] = Decimal(str(row.get("factory_available_m2") or 0))
        freshness["factory_snapshot_date"] = fs_date
    else:
        freshness["factory_snapshot_date"] = None

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

    # 5. Sales → velocity (90-day simple average)
    sales_start = (today - timedelta(days=90)).isoformat()
    sales_res = db.table("sales").select(
        "product_id, quantity_m2"
    ).gte("week_start", sales_start).execute()
    sales_totals: dict[str, Decimal] = defaultdict(Decimal)
    for row in sales_res.data:
        pid = row.get("product_id")
        if pid:
            sales_totals[pid] += Decimal(str(row.get("quantity_m2") or 0))
    velocities: dict[str, Decimal] = {
        pid: (total / 90).quantize(Decimal("0.01"))
        for pid, total in sales_totals.items()
    }
    freshness["sales_records"] = len(sales_res.data)
    freshness["sales_window_start"] = sales_start

    # 6. Shipment items (per-boat dispatch reality)
    shipment_res = db.table("shipment_items").select(
        "boat_id, product_id, shipped_m2"
    ).execute()
    shipment_items = [
        {
            "boat_id": boat_alias.get(row["boat_id"], row["boat_id"]),
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
        "id, boat_id, factory_id, status"
    ).eq("factory_id", factory_id).execute()

    # Draft headers: tells the brain which boats have drafts (even empty ones)
    # Remap aliased boat IDs to canonical
    draft_headers = [
        {"boat_id": boat_alias.get(d["boat_id"], d["boat_id"]), "status": d["status"], "draft_id": d["id"]}
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
                "boat_id": boat_alias.get(draft["boat_id"], draft["boat_id"]),
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
        "factory_stock": factory_stock,
        "drafts": drafts,
        "draft_headers": draft_headers,
        "shipment_items": shipment_items,
        "production_schedule": production_schedule,
        "_freshness": freshness,
        "_boat_alias": boat_alias,
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
    inputs.pop("_boat_alias", None)
    result = compute_horizon(**inputs, today=today)

    # Merge freshness into data_as_of
    result["data_as_of"].update(freshness)

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
        "anchor_boat_id": result["anchor_boat_id"],
        "projections": result["projections"],
        "completed": result["completed"],
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
    boat_alias = inputs.pop("_boat_alias", {})
    # Resolve aliased boat_id → canonical so we find the right projection
    boat_id = boat_alias.get(boat_id, boat_id)
    result = compute_horizon(**inputs, today=today)
    result["data_as_of"].update(freshness)

    # Find the specific boat
    boat_projection = None
    for p in result["projections"]:
        if p["boat_id"] == boat_id:
            boat_projection = p
            break

    # Check completed boats too
    boat_completed = None
    if not boat_projection:
        for c in result["completed"]:
            if c["boat_id"] == boat_id:
                boat_completed = c
                break

    if not boat_projection and not boat_completed:
        raise HTTPException(status_code=404, detail=f"Boat {boat_id} not found in horizon")

    # Find next boat for context
    next_boat = None
    if boat_projection:
        projs = result["projections"]
        for i, p in enumerate(projs):
            if p["boat_id"] == boat_id and i + 1 < len(projs):
                next_boat = {
                    "boat_id": projs[i + 1]["boat_id"],
                    "boat_name": projs[i + 1]["boat_name"],
                    "departure_date": projs[i + 1]["departure_date"],
                    "arrival_date": projs[i + 1]["arrival_date"],
                }
                break

    # Find debug trace for this boat
    boat_debug = None
    for d in result.get("_debug", []):
        if d["boat_id"] == boat_id:
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
        "boat": boat_projection or boat_completed,
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
