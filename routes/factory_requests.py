"""
Factory Request Horizon routes.
"""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config import get_supabase_client
from lib.brain import _classify_tiers, _compute_buffer_m2
from lib.constants import M2_PER_PALLET
from models.factory_request import FactoryRequestHorizonResponse
from models.factory_request_submission import (
    FactoryRequestSubmissionCreate,
    FactoryRequestSubmissionResponse,
    FactoryRequestLastSubmission,
)
from services.factory_request_service import get_factory_request_service
from services.factory_request_submission_service import get_factory_request_submission_service

router = APIRouter(prefix="/api/factory-requests", tags=["Factory Requests"])


# Lead time used for production gap calc: how far ahead must the pipeline cover?
PRODUCTION_HORIZON_WEEKS = Decimal("6")


class AutoSuggestion(BaseModel):
    product_id: str
    sku: str
    tier: str
    velocity_m2_wk: float
    warehouse_m2: float
    transit_m2: float
    siesa_m2: float
    pipeline_total_m2: float
    buffer_m2: float
    projected_at_horizon_m2: float
    production_gap_m2: float
    production_gap_pallets: int


@router.get("/horizon/{factory_id}", response_model=FactoryRequestHorizonResponse)
async def get_factory_request_horizon(factory_id: str):
    try:
        service = get_factory_request_service()
        return service.get_horizon(factory_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/submissions", response_model=FactoryRequestSubmissionResponse)
async def record_factory_request_submission(body: FactoryRequestSubmissionCreate):
    """Record a factory request export (Excel) for tracking."""
    try:
        service = get_factory_request_submission_service()
        return service.record_submission(body.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/submissions/last/{factory_id}", response_model=FactoryRequestLastSubmission)
async def get_last_factory_request_submission(factory_id: str):
    """Get the most recent factory request submission for a factory."""
    service = get_factory_request_submission_service()
    result = service.get_last_submission(factory_id)
    if not result:
        raise HTTPException(status_code=404, detail="No submissions found")
    return result


@router.get("/auto-suggested", response_model=list[AutoSuggestion])
async def get_auto_suggested_production():
    """Identify products where pipeline can't sustain buffer levels.

    For each active tile product:
        projected_at_horizon = warehouse + transit + siesa - (velocity × 6 weeks)
        production_gap = buffer_m2 - projected_at_horizon

    If gap > 0, factory needs to produce that volume.
    """
    db = get_supabase_client()
    today = date.today()

    # Active tile products
    tile_categories = ["MADERAS", "MARMOLIZADOS", "EXTERIORES"]
    products = (
        db.table("products")
        .select("id, sku")
        .eq("active", True)
        .in_("category", tile_categories)
        .execute()
        .data or []
    )
    product_ids = [p["id"] for p in products]
    pid_to_sku = {p["id"]: p["sku"] for p in products}

    # 90-day velocity (m²/day)
    sales_start = (today - timedelta(days=90)).isoformat()
    sales = (
        db.table("sales")
        .select("product_id, quantity_m2")
        .gte("week_start", sales_start)
        .execute()
        .data or []
    )
    qty_totals: dict[str, Decimal] = defaultdict(Decimal)
    for r in sales:
        pid = r.get("product_id")
        if pid:
            qty_totals[pid] += Decimal(str(r.get("quantity_m2") or 0))
    velocities_daily = {
        pid: (tot / Decimal("90")).quantize(Decimal("0.01"))
        for pid, tot in qty_totals.items()
    }

    # Latest snapshots
    fs_d = db.table("factory_snapshots").select("snapshot_date").order(
        "snapshot_date", desc=True
    ).limit(1).execute().data
    wh_d = db.table("warehouse_snapshots").select("snapshot_date").order(
        "snapshot_date", desc=True
    ).limit(1).execute().data
    tr_d = db.table("transit_snapshots").select("snapshot_date").order(
        "snapshot_date", desc=True
    ).limit(1).execute().data

    siesa: dict[str, Decimal] = {}
    if fs_d:
        for r in db.table("factory_snapshots").select(
            "product_id, factory_available_m2"
        ).eq("snapshot_date", fs_d[0]["snapshot_date"]).execute().data or []:
            siesa[r["product_id"]] = Decimal(str(r.get("factory_available_m2") or 0))

    wh: dict[str, Decimal] = {}
    if wh_d:
        for r in db.table("warehouse_snapshots").select(
            "product_id, warehouse_qty"
        ).eq("snapshot_date", wh_d[0]["snapshot_date"]).execute().data or []:
            wh[r["product_id"]] = Decimal(str(r.get("warehouse_qty") or 0))

    transit: dict[str, Decimal] = {}
    if tr_d:
        for r in db.table("transit_snapshots").select(
            "product_id, in_transit_qty"
        ).eq("snapshot_date", tr_d[0]["snapshot_date"]).execute().data or []:
            transit[r["product_id"]] = Decimal(str(r.get("in_transit_qty") or 0))

    # Tier classification + buffer (reuses brain helpers for consistency)
    tier_map = _classify_tiers(product_ids, velocities_daily)

    out: list[AutoSuggestion] = []
    for pid in product_ids:
        v_daily = Decimal(str(velocities_daily.get(pid, 0)))
        if v_daily <= 0:
            continue  # zero-velocity products don't trigger production
        tier = tier_map[pid]
        buffer_m2 = _compute_buffer_m2(pid, v_daily, tier)

        w = wh.get(pid, Decimal(0))
        t = transit.get(pid, Decimal(0))
        s = siesa.get(pid, Decimal(0))
        pipeline = w + t + s
        consumption = v_daily * Decimal(7) * PRODUCTION_HORIZON_WEEKS
        projected = pipeline - consumption
        gap = buffer_m2 - projected

        if gap <= 0:
            continue  # pipeline sustains buffer, no production needed

        gap_pallets = int((gap / M2_PER_PALLET).to_integral_value(rounding="ROUND_UP"))
        out.append(AutoSuggestion(
            product_id=pid,
            sku=pid_to_sku.get(pid, "?"),
            tier=tier,
            velocity_m2_wk=float(v_daily * Decimal(7)),
            warehouse_m2=float(w),
            transit_m2=float(t),
            siesa_m2=float(s),
            pipeline_total_m2=float(pipeline),
            buffer_m2=float(buffer_m2),
            projected_at_horizon_m2=float(projected),
            production_gap_m2=float(gap),
            production_gap_pallets=gap_pallets,
        ))

    out.sort(key=lambda x: -x.production_gap_m2)
    return out
