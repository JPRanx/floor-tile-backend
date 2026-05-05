"""
Reconciliation — compare our committed drafts against factory's view of commits.

Three signals fall out:
  - "matched"                  → our drafts ≈ factory's Cant. comprometida
  - "zombies"                  → our drafts > factory's view (we say committed,
                                  factory has either closed/shipped/cancelled them)
  - "unknown_factory_commits"  → factory's view > our drafts (factory has commits
                                  we don't track — likely non-export orders,
                                  or our drafts haven't reached them yet)

Pure observation surface. Does not feed the cascade. Brain ignores it.
"""

from decimal import Decimal
from datetime import date

from fastapi import APIRouter, HTTPException
import structlog

from config import get_supabase_client
from lib.constants import M2_PER_PALLET

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/v2/reconciliation", tags=["reconciliation"])

# Anything within this many m² is considered "matched" (rounding tolerance).
MATCH_TOLERANCE_M2 = Decimal("100.0")


@router.get("/factory-commits/{factory_id}")
async def factory_commits_reconciliation(factory_id: str):
    """Per-product gap between our committed drafts and factory's Cant. comprometida.

    Only future boats count for "our_committed_m2" — past boats either shipped
    (their pallets are gone from the snapshot) or are zombies (separate problem).
    """
    db = get_supabase_client()
    today = date.today()

    # 1. Latest factory snapshot — committed view by product
    fs = (
        db.table("factory_snapshots")
        .select("snapshot_date, created_at")
        .order("snapshot_date", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not fs:
        return {"factory_id": factory_id, "products": [], "snapshot_date": None}

    snapshot_date = fs[0]["snapshot_date"]
    snapshot_uploaded_at = fs[0].get("created_at")

    snap_rows = (
        db.table("factory_snapshots")
        .select("product_id, factory_existencia_m2, factory_committed_m2, factory_available_m2")
        .eq("snapshot_date", snapshot_date)
        .execute()
        .data or []
    )
    factory_commit_by_pid: dict[str, Decimal] = {
        r["product_id"]: Decimal(str(r.get("factory_committed_m2") or 0))
        for r in snap_rows
    }
    factory_existencia_by_pid: dict[str, Decimal] = {
        r["product_id"]: Decimal(str(r.get("factory_existencia_m2") or 0))
        for r in snap_rows
    }

    # 2. Our committed drafts on FUTURE boats for this factory
    future_boats = (
        db.table("boat_schedules")
        .select("id")
        .gte("departure_date", today.isoformat())
        .neq("status", "ignored")
        .execute()
        .data or []
    )
    future_boat_ids = {b["id"] for b in future_boats}

    drafts = (
        db.table("boat_factory_drafts")
        .select("id, boat_id, status")
        .eq("factory_id", factory_id)
        .in_("status", ["ordered", "confirmed"])
        .execute()
        .data or []
    )
    active_draft_ids = [d["id"] for d in drafts if d["boat_id"] in future_boat_ids]

    our_commit_by_pid: dict[str, Decimal] = {}
    if active_draft_ids:
        items = (
            db.table("draft_items")
            .select("draft_id, product_id, selected_pallets")
            .in_("draft_id", active_draft_ids)
            .execute()
            .data or []
        )
        for it in items:
            pid = it["product_id"]
            pallets = Decimal(str(it.get("selected_pallets") or 0))
            our_commit_by_pid[pid] = our_commit_by_pid.get(pid, Decimal(0)) + pallets * M2_PER_PALLET

    # 3. Build per-product reconciliation rows
    products = (
        db.table("products")
        .select("id, sku, factory_id, active")
        .eq("factory_id", factory_id)
        .eq("active", True)
        .execute()
        .data or []
    )

    rows = []
    for p in products:
        pid = p["id"]
        ours = our_commit_by_pid.get(pid, Decimal(0))
        theirs = factory_commit_by_pid.get(pid, Decimal(0))
        existencia = factory_existencia_by_pid.get(pid, Decimal(0))
        gap = ours - theirs

        if abs(gap) <= MATCH_TOLERANCE_M2:
            reason = "matched"
        elif gap > 0:
            reason = "zombies"  # we say committed, factory doesn't see it
        else:
            reason = "unknown_factory_commits"  # factory committed more than we know

        rows.append({
            "product_id": pid,
            "sku": p["sku"],
            "factory_existencia_m2": float(existencia),
            "our_committed_m2": float(ours),
            "factory_committed_m2": float(theirs),
            "gap_m2": float(gap),
            "gap_reason": reason,
        })

    # Sort: largest absolute gaps first (most actionable)
    rows.sort(key=lambda r: -abs(r["gap_m2"]))

    return {
        "factory_id": factory_id,
        "snapshot_date": snapshot_date,
        "snapshot_uploaded_at": snapshot_uploaded_at,
        "products": rows,
        "summary": {
            "total_products": len(rows),
            "matched": sum(1 for r in rows if r["gap_reason"] == "matched"),
            "zombies": sum(1 for r in rows if r["gap_reason"] == "zombies"),
            "unknown_factory_commits": sum(
                1 for r in rows if r["gap_reason"] == "unknown_factory_commits"
            ),
        },
    }
