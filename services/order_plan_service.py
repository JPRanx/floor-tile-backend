"""Order plan service.

Generates a velocity-prioritized order plan across selected boats.
Deterministic math: velocity ranking + cascade allocation + capacity check.
Narrative prose is added separately by claude_narrative_service.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import structlog

from config import get_supabase_client


logger = structlog.get_logger(__name__)

M2_PER_PALLET = Decimal("134.4")
PALLETS_PER_CONTAINER = Decimal("13")
WAREHOUSE_MAX_PALLETS = Decimal("672")  # Guatemala warehouse capacity

# Buffer config — same as backend/lib/constants.py TIER_BUFFER_CONFIG.
# Mirrored here to keep this service standalone.
_TIER_CONFIG = {
    "A": {"weeks": 4, "floor_pallets": 5, "ceiling_pallets": 999},
    "B": {"weeks": 3, "floor_pallets": 3, "ceiling_pallets": 15},
    "C": {"weeks": 2, "floor_pallets": 1, "ceiling_pallets": 8},
}


def _classify_tier(velocity_wk: Decimal, all_velocities_wk: list[Decimal]) -> str:
    """Classify a product by velocity quartile.
    Top 25% = A, mid 50% = B, bottom 25% (or zero velocity) = C.
    """
    if velocity_wk <= 0:
        return "C"
    sorted_vels = sorted([v for v in all_velocities_wk if v > 0], reverse=True)
    n = len(sorted_vels)
    if n == 0:
        return "C"
    a_cut_idx = max(1, int(n * 0.25)) - 1
    c_cut_idx = max(a_cut_idx + 1, int(n * 0.75)) - 1
    a_threshold = sorted_vels[min(a_cut_idx, n - 1)]
    c_threshold = sorted_vels[min(c_cut_idx, n - 1)]
    if velocity_wk >= a_threshold:
        return "A"
    if velocity_wk >= c_threshold:
        return "B"
    return "C"


def _buffer_m2_for(velocity_wk: Decimal, tier: str) -> Decimal:
    cfg = _TIER_CONFIG[tier]
    raw = velocity_wk * Decimal(cfg["weeks"])
    floor_m2 = Decimal(cfg["floor_pallets"]) * M2_PER_PALLET
    ceil_m2 = Decimal(cfg["ceiling_pallets"]) * M2_PER_PALLET
    return max(floor_m2, min(raw, ceil_m2))


@dataclass
class PlanProductLine:
    product_id: str
    sku: str
    pallets: int
    m2: float
    # Reasoning metadata for the UI
    velocity_m2_wk: float
    siesa_m2: float
    coverage_weeks: float
    is_urgent: bool
    note_es: str


@dataclass
class PlanBoat:
    boat_id: str
    vessel_name: str
    departure_date: str
    arrival_date: str
    max_containers: int
    max_pallets: int
    lines: list[PlanProductLine] = field(default_factory=list)

    @property
    def total_pallets(self) -> int:
        return sum(l.pallets for l in self.lines)

    @property
    def total_m2(self) -> float:
        return float(self.total_pallets) * float(M2_PER_PALLET)

    @property
    def containers_used(self) -> float:
        return round(self.total_pallets / float(PALLETS_PER_CONTAINER), 1)


@dataclass
class VelocityRankingRow:
    sku: str
    velocity_m2_wk: float
    siesa_pallets: float
    siesa_m2: float
    coverage_weeks: float
    is_urgent: bool


@dataclass
class SkippedProduct:
    sku: str
    siesa_pallets: float
    siesa_m2: float
    reason_es: str


@dataclass
class WarehouseCapacity:
    current_pallets: int
    incoming_pallets: int
    plan_pallets: int
    outflow_pallets: int  # estimated sales in 3 weeks
    peak_pallets: int
    max_pallets: int
    utilization_pct: float
    is_safe: bool


@dataclass
class PlanResult:
    boats: list[PlanBoat]
    velocity_ranking: list[VelocityRankingRow]
    skipped: list[SkippedProduct]
    warehouse_capacity: WarehouseCapacity
    total_siesa_pallets: int
    plan_total_pallets: int


# ─────────────────────────────────────────────────────────────────────
# Core computation
# ─────────────────────────────────────────────────────────────────────


def compute_plan(
    boat_ids: list[str],
    max_containers: int,
    warehouse_buffer_pct: int,
    include_production: bool,
    factory_id: Optional[str] = None,
) -> PlanResult:
    """Build a velocity-prioritized order plan for the selected boats.

    Args:
        boat_ids: UUIDs of the boats to plan, in departure order.
        max_containers: Max containers per boat (13 pallets each).
        warehouse_buffer_pct: % of warehouse kept free as buffer (default 15).
        include_production: If True, include factory production scheduled to
            finish before each boat's departure in the available SIESA pool.
        factory_id: Optional factory filter. None = all tile factories.

    Returns:
        PlanResult with structured boats, lines, ranking, and capacity check.
    """
    db = get_supabase_client()

    # 1. Fetch selected boats, ordered by departure date
    boats_res = db.table("boat_schedules").select(
        "id, vessel_name, departure_date, arrival_date"
    ).in_("id", boat_ids).order("departure_date").execute()
    boats_data = boats_res.data or []

    # 2. Fetch active tile products
    tile_categories = ["MADERAS", "MARMOLIZADOS", "EXTERIORES"]
    prods_q = db.table("products").select("id, sku, factory_id").eq(
        "active", True
    ).in_("category", tile_categories)
    if factory_id:
        prods_q = prods_q.eq("factory_id", factory_id)
    products = prods_q.execute().data or []
    pid_to_sku: dict[str, str] = {p["id"]: p["sku"] for p in products}
    active_product_ids = set(pid_to_sku.keys())

    # 3. Latest SIESA (factory) inventory
    latest_fs = db.table("factory_snapshots").select(
        "snapshot_date"
    ).order("snapshot_date", desc=True).limit(1).execute()
    siesa: dict[str, Decimal] = {}
    if latest_fs.data:
        fs_date = latest_fs.data[0]["snapshot_date"]
        rows = db.table("factory_snapshots").select(
            "product_id, factory_available_m2"
        ).eq("snapshot_date", fs_date).execute().data or []
        for r in rows:
            if r["product_id"] in active_product_ids:
                siesa[r["product_id"]] = siesa.get(
                    r["product_id"], Decimal(0)
                ) + Decimal(str(r.get("factory_available_m2") or 0))

    # 3b. Subtract committed drafts on boats NOT being planned. Those pallets
    # are already spoken for — they'll leave SIESA when those boats sail, so
    # they must not be available to the selected plan. Only consider boats
    # that haven't departed yet (post-departure SIESA already reflects it).
    today_iso = date.today().isoformat()
    future_boats = (
        db.table("boat_schedules")
        .select("id")
        .gte("departure_date", today_iso)
        .execute()
        .data or []
    )
    future_boat_ids = {b["id"] for b in future_boats}

    selected_set = set(boat_ids)
    committed_drafts = (
        db.table("boat_factory_drafts")
        .select("id, boat_id, status")
        .in_("status", ["ordered", "confirmed"])
        .execute()
        .data or []
    )
    other_committed_draft_ids = [
        d["id"] for d in committed_drafts
        if d["boat_id"] not in selected_set and d["boat_id"] in future_boat_ids
    ]
    if other_committed_draft_ids:
        committed_items = (
            db.table("draft_items")
            .select("draft_id, product_id, selected_pallets")
            .in_("draft_id", other_committed_draft_ids)
            .execute()
            .data or []
        )
        for it in committed_items:
            pid = it["product_id"]
            pallets = Decimal(str(it.get("selected_pallets") or 0))
            consume = pallets * M2_PER_PALLET
            current = siesa.get(pid, Decimal(0))
            siesa[pid] = max(Decimal(0), current - consume)

    # 4. 90-day velocity from sales table (same method as brain.py — new middle)
    today_d = date.today()
    sales_start = (today_d - timedelta(days=90)).isoformat()
    sales_res = db.table("sales").select(
        "product_id, quantity_m2"
    ).gte("week_start", sales_start).execute().data or []
    sales_totals: dict[str, Decimal] = defaultdict(Decimal)
    for row in sales_res:
        pid = row.get("product_id")
        if pid:
            sales_totals[pid] += Decimal(str(row.get("quantity_m2") or 0))
    # Convert 90-day total to weekly velocity (m²/wk)
    velocity_wk: dict[str, Decimal] = {
        pid: (total / Decimal("90") * Decimal("7")).quantize(Decimal("0.01"))
        for pid, total in sales_totals.items()
    }

    # 5. Warehouse + transit (for coverage calc)
    latest_wh = db.table("warehouse_snapshots").select(
        "snapshot_date"
    ).order("snapshot_date", desc=True).limit(1).execute()
    wh: dict[str, Decimal] = {}
    if latest_wh.data:
        wh_date = latest_wh.data[0]["snapshot_date"]
        rows = db.table("warehouse_snapshots").select(
            "product_id, warehouse_qty"
        ).eq("snapshot_date", wh_date).execute().data or []
        for r in rows:
            wh[r["product_id"]] = Decimal(str(r.get("warehouse_qty") or 0))

    latest_tr = db.table("transit_snapshots").select(
        "snapshot_date"
    ).order("snapshot_date", desc=True).limit(1).execute()
    transit: dict[str, Decimal] = {}
    if latest_tr.data:
        tr_date = latest_tr.data[0]["snapshot_date"]
        rows = db.table("transit_snapshots").select(
            "product_id, in_transit_qty"
        ).eq("snapshot_date", tr_date).execute().data or []
        for r in rows:
            transit[r["product_id"]] = Decimal(str(r.get("in_transit_qty") or 0))

    # 6. Optional: include factory production scheduled to finish before each boat
    prod_ready_by_pid_and_date: dict[str, list[tuple[date, Decimal]]] = {}
    if include_production:
        pipeline = db.table("production_schedule").select(
            "product_id, estimated_delivery_date, completed_m2, requested_m2, status"
        ).in_("status", ["in_progress", "requested"]).execute().data or []
        for row in pipeline:
            pid = row["product_id"]
            est = row.get("estimated_delivery_date")
            if not est:
                continue
            try:
                est_date = date.fromisoformat(est)
            except (TypeError, ValueError):
                continue
            req = Decimal(str(row.get("requested_m2") or 0))
            comp = Decimal(str(row.get("completed_m2") or 0))
            contrib = max(Decimal(0), req - comp)
            if contrib > 0:
                prod_ready_by_pid_and_date.setdefault(pid, []).append((est_date, contrib))

    # 7. Build velocity ranking AND skipped list
    # Ranking is for products with meaningful demand + enough SIESA to ship (>=1 pallet)
    # Skipped covers: no-velocity items OR sub-pallet leftovers that can't ship
    ranking: list[VelocityRankingRow] = []
    skipped: list[SkippedProduct] = []
    for pid, siesa_m2 in siesa.items():
        if siesa_m2 <= 0:
            continue
        v = velocity_wk.get(pid, Decimal(0))
        siesa_pallets_exact = float(siesa_m2 / M2_PER_PALLET)

        # No Q1 sales → skipped (velocity unknown)
        if v <= 0:
            skipped.append(SkippedProduct(
                sku=pid_to_sku.get(pid, "?"),
                siesa_pallets=round(siesa_pallets_exact, 1),
                siesa_m2=float(siesa_m2),
                reason_es="Sin ventas en los ultimos 90 dias",
            ))
            continue

        # Has velocity but <1 full pallet available → skipped (can't ship partial)
        if siesa_pallets_exact < 1.0:
            skipped.append(SkippedProduct(
                sku=pid_to_sku.get(pid, "?"),
                siesa_pallets=round(siesa_pallets_exact, 1),
                siesa_m2=float(siesa_m2),
                reason_es="Cantidad insuficiente (menos de 1 pallet)",
            ))
            continue

        total_pipeline = (
            wh.get(pid, Decimal(0))
            + transit.get(pid, Decimal(0))
            + siesa_m2
        )
        cov = float(total_pipeline / v)
        ranking.append(VelocityRankingRow(
            sku=pid_to_sku.get(pid, "?"),
            velocity_m2_wk=float(v),
            siesa_pallets=round(siesa_pallets_exact, 1),
            siesa_m2=float(siesa_m2),
            coverage_weeks=round(cov, 1),
            is_urgent=cov < 4.0,
        ))
    # Display ranking: sorted by velocity (what "velocity ranking" means)
    ranking.sort(key=lambda r: r.velocity_m2_wk, reverse=True)

    # Allocation queue: urgent SKUs jump the queue so stockouts get first dibs,
    # then by velocity descending within each tier.
    allocation_queue = sorted(
        ranking, key=lambda r: (not r.is_urgent, -r.velocity_m2_wk)
    )

    # 9. Cascade allocation across boats in departure order — BUFFER-ANCHORED.
    # Per (boat, product), take only enough to keep stock above buffer through
    # the next boat's arrival. This naturally distributes products across boats
    # instead of greedy-filling the first boat.
    available: dict[str, Decimal] = {
        pid: Decimal(str(m2)) for pid, m2 in siesa.items()
    }
    # Pre-compute tier + buffer per product (same logic as brain.py)
    sku_to_pid = {sku: pid for pid, sku in pid_to_sku.items()}
    all_weekly_vels = [Decimal(str(r.velocity_m2_wk)) for r in ranking]
    product_tier: dict[str, str] = {}
    product_buffer_m2: dict[str, Decimal] = {}
    for r in ranking:
        pid = sku_to_pid.get(r.sku)
        if not pid:
            continue
        v = Decimal(str(r.velocity_m2_wk))
        tier = _classify_tier(v, all_weekly_vels)
        product_tier[pid] = tier
        product_buffer_m2[pid] = _buffer_m2_for(v, tier)

    # Track running warehouse projection per product across boat arrivals so
    # we don't double-allocate (each boat's take adds to the next-arrival pool).
    warehouse_projection: dict[str, Decimal] = {
        pid: Decimal(str(m2)) for pid, m2 in (
            list(map(lambda x: (x[0], x[1]), [(pid, 0) for pid in pid_to_sku.keys()]))
        )
    }
    # Note: we don't have warehouse data here; simplification — boat take is
    # capped by buffer + sales-between-boats, that's the constraint that matters.

    plan_boats: list[PlanBoat] = []
    today = date.today()

    for i, boat in enumerate(boats_data):
        dep = date.fromisoformat(boat["departure_date"]) \
            if isinstance(boat["departure_date"], str) else boat["departure_date"]
        arr = date.fromisoformat(boat["arrival_date"]) \
            if isinstance(boat["arrival_date"], str) else boat["arrival_date"]

        # Compute "weeks until next boat" — how long this boat's stock must last.
        if i + 1 < len(boats_data):
            nxt = boats_data[i + 1]
            next_arr = date.fromisoformat(nxt["arrival_date"]) \
                if isinstance(nxt["arrival_date"], str) else nxt["arrival_date"]
            weeks_to_next = max(Decimal("1"), Decimal(str((next_arr - arr).days)) / Decimal("7"))
        else:
            weeks_to_next = Decimal("6")  # default coverage horizon for last boat

        # Add production that will be ready before this boat's departure
        if include_production:
            for pid, batches in prod_ready_by_pid_and_date.items():
                remaining = []
                for est_date, contrib in batches:
                    if est_date <= dep:
                        available[pid] = available.get(pid, Decimal(0)) + contrib
                    else:
                        remaining.append((est_date, contrib))
                prod_ready_by_pid_and_date[pid] = remaining

        max_pallets = max_containers * int(PALLETS_PER_CONTAINER)
        remaining_pallets = Decimal(max_pallets)

        plan_boat = PlanBoat(
            boat_id=boat["id"],
            vessel_name=boat["vessel_name"],
            departure_date=str(dep),
            arrival_date=str(arr),
            max_containers=max_containers,
            max_pallets=max_pallets,
        )

        # Take from allocation queue (urgent first, then by velocity desc)
        for row in allocation_queue:
            if remaining_pallets <= 0:
                break
            pid = sku_to_pid.get(row.sku)
            if pid is None:
                continue
            avail_m2 = available.get(pid, Decimal(0))
            if avail_m2 <= 0:
                continue

            # Buffer-anchored take: enough to cover sales until next boat + buffer
            # for THIS product. Caps at SIESA available and remaining boat capacity.
            buffer_m2 = product_buffer_m2.get(pid, Decimal("403.2"))  # legacy fallback
            weekly_vel = Decimal(str(row.velocity_m2_wk))
            target_m2 = buffer_m2 + (weekly_vel * weeks_to_next)
            target_pallets = (target_m2 / M2_PER_PALLET).to_integral_value(rounding="ROUND_UP")

            avail_pallets = avail_m2 / M2_PER_PALLET
            take = min(target_pallets, avail_pallets, remaining_pallets)
            take_pallets = int(take)  # round down to whole pallets
            if take_pallets <= 0:
                continue
            take_m2 = Decimal(take_pallets) * M2_PER_PALLET
            available[pid] = avail_m2 - take_m2
            remaining_pallets -= take_pallets

            note = _build_line_note(row)
            plan_boat.lines.append(PlanProductLine(
                product_id=pid,
                sku=row.sku,
                pallets=take_pallets,
                m2=float(take_m2),
                velocity_m2_wk=row.velocity_m2_wk,
                siesa_m2=row.siesa_m2,
                coverage_weeks=row.coverage_weeks,
                is_urgent=row.is_urgent,
                note_es=note,
            ))

        plan_boats.append(plan_boat)

    # 10. Warehouse capacity check
    current_wh_pallets = int(sum(wh.values()) / M2_PER_PALLET) if wh else 0

    # Rough "incoming" = shipment_items not yet delivered (future boats only)
    incoming_res = db.table("shipment_items").select(
        "shipped_m2, boat_id"
    ).execute().data or []
    incoming_boat_ids = {row["boat_id"] for row in incoming_res}
    incoming_boats = db.table("boat_schedules").select(
        "id, arrival_date"
    ).in_("id", list(incoming_boat_ids)).execute().data or []
    future_incoming_boat_ids = {
        b["id"] for b in incoming_boats
        if date.fromisoformat(b["arrival_date"]) > today
    }
    incoming_m2 = sum(
        Decimal(str(row.get("shipped_m2") or 0))
        for row in incoming_res
        if row["boat_id"] in future_incoming_boat_ids
    )
    incoming_pallets = int(incoming_m2 / M2_PER_PALLET)

    plan_total_pallets = sum(b.total_pallets for b in plan_boats)

    # Estimate 3 weeks of outflow based on total weekly velocity
    total_weekly_vel = sum(velocity_wk.values())
    outflow_m2_3w = total_weekly_vel * Decimal("3")
    outflow_pallets = int(outflow_m2_3w / M2_PER_PALLET)

    peak = current_wh_pallets + incoming_pallets + plan_total_pallets - outflow_pallets
    max_pallets_allowed = int(
        WAREHOUSE_MAX_PALLETS * (Decimal("100") - Decimal(warehouse_buffer_pct)) / Decimal("100")
    )
    utilization_pct = round(peak / float(WAREHOUSE_MAX_PALLETS) * 100, 1) \
        if WAREHOUSE_MAX_PALLETS > 0 else 0.0
    is_safe = peak <= max_pallets_allowed

    capacity = WarehouseCapacity(
        current_pallets=current_wh_pallets,
        incoming_pallets=incoming_pallets,
        plan_pallets=plan_total_pallets,
        outflow_pallets=outflow_pallets,
        peak_pallets=peak,
        max_pallets=int(WAREHOUSE_MAX_PALLETS),
        utilization_pct=utilization_pct,
        is_safe=is_safe,
    )

    total_siesa_pallets = int(sum(siesa.values()) / M2_PER_PALLET) if siesa else 0

    logger.info(
        "order_plan_computed",
        boats=len(plan_boats),
        total_pallets=plan_total_pallets,
        siesa_pallets=total_siesa_pallets,
        wh_utilization_pct=utilization_pct,
    )

    return PlanResult(
        boats=plan_boats,
        velocity_ranking=ranking,
        skipped=skipped,
        warehouse_capacity=capacity,
        total_siesa_pallets=total_siesa_pallets,
        plan_total_pallets=plan_total_pallets,
    )


def _build_line_note(row: VelocityRankingRow) -> str:
    """Short Spanish reasoning for why this SKU/qty goes on this boat."""
    if row.is_urgent:
        return f"Urgente — {row.coverage_weeks:.1f} sem de cobertura, velocidad {row.velocity_m2_wk:.0f} m²/sem"
    return f"Velocidad {row.velocity_m2_wk:.0f} m²/sem, {row.coverage_weeks:.1f} sem de cobertura"
