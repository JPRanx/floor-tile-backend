"""
The Brain — one pure function that answers:
"What needs to happen at each stage of the supply chain
 to keep every product in stock?"

No DB access. No imports from services. Tables in → horizon out.

Production is unified into the cascade:
- Factory snapshot is the sole source of truth for available stock
- Scheduled/in-progress production does NOT inflate factory_avail
- After simulation: remaining gaps minus scheduled production = real requests
- Production requests only target non-skipped boats
"""

from datetime import date, timedelta
from decimal import Decimal, ROUND_UP
from typing import Any

from .constants import (
    M2_PER_PALLET,
    SAFETY_STOCK_M2,
    MIN_BOAT_PALLETS,
    MIN_BLS_PER_BOAT,
    PALLETS_PER_CONTAINER,
    LEAD_TIME_DAYS,
)


# ---------------------------------------------------------------------------
# Types (plain dicts — no Pydantic here, this is a pure computation layer)
# ---------------------------------------------------------------------------
#
# Product:    {id, sku, active}
# Boat:       {id, name, departure_date, arrival_date, factory_id, carrier}
# Draft:      {boat_id, product_id, selected_pallets, status}
# Shipment:   {boat_id, product_id, shipped_m2, shipped_pallets}
# Production: {product_id, status, requested_m2, completed_m2, scheduled_date}
#


def compute_horizon(
    *,
    products: list[dict],
    boats: list[dict],
    inventory: dict[str, Decimal],
    in_transit: dict[str, Decimal] | None = None,  # Reserved — not used yet
    velocities: dict[str, Decimal],
    factory_stock: dict[str, Decimal],
    drafts: list[dict],
    draft_headers: list[dict],
    shipment_items: list[dict],
    production_schedule: list[dict],
    today: date,
) -> dict[str, Any]:
    """
    The one brain. Pure function.

    Returns {
        projections:          boats that need action, with per-product suggestions,
        completed:            boats already dispatched/ordered (locked),
        production_requests:  what genuinely needs ordering (after accounting for scheduled),
        skip_recommendations: boats not worth shipping,
        factory_order_signal: next action date,
        data_as_of:           input freshness,
        _debug:               full math trace,
    }
    """

    # ── STEP 1: Index inputs ──────────────────────────────────────────────

    product_map = {p["id"]: p for p in products}
    product_ids = [p["id"] for p in products if p.get("active", True)]

    # Index shipment_items by boat_id
    shipments_by_boat: dict[str, dict[str, Decimal]] = {}
    for si in shipment_items:
        bid = si["boat_id"]
        pid = si["product_id"]
        shipments_by_boat.setdefault(bid, {})[pid] = Decimal(str(si["shipped_m2"]))

    # Index drafts: headers tell us WHICH boats have drafts (even empty ones),
    # items tell us the per-product pallets.
    # Three-tier allocation: shipment > draft > brain suggestion.
    drafts_by_boat: dict[str, dict[str, dict]] = {}
    for d in drafts:
        bid = d["boat_id"]
        pid = d["product_id"]
        drafts_by_boat.setdefault(bid, {})[pid] = d

    # From headers — boat-level draft existence, status, and ID.
    # A boat with a draft but no items = Ashley decided 0 (skip).
    draft_status_by_boat: dict[str, str] = {
        h["boat_id"]: h["status"] for h in draft_headers
    }
    draft_id_by_boat: dict[str, str] = {
        h["boat_id"]: h["draft_id"] for h in draft_headers
    }
    draft_boat_ids: set[str] = set(draft_status_by_boat.keys())

    # Index production by product_id
    production_by_product: dict[str, list[dict]] = {}
    for ps in production_schedule:
        pid = ps["product_id"]
        production_by_product.setdefault(pid, []).append(ps)

    # Mutable factory stock (consumed as we allocate)
    # Factory snapshot IS the source of truth — SIESA confirmed available stock.
    # Do NOT add in_progress completed_m2 on top — it may already be in the snapshot.
    # When production finishes, it shows up in the next factory snapshot upload.
    factory_avail: dict[str, Decimal] = {
        pid: Decimal(str(factory_stock.get(pid, 0))) for pid in product_ids
    }

    # Sum all scheduled/in-progress production per product (for post-loop gap analysis).
    # This represents what's in the pipeline — reduces urgency of production requests.
    scheduled_production: dict[str, Decimal] = {}
    for pid in product_ids:
        for entry in production_by_product.get(pid, []):
            if entry["status"] in ("scheduled", "requested"):
                requested = Decimal(str(entry.get("requested_m2") or 0))
                scheduled_production[pid] = scheduled_production.get(pid, Decimal(0)) + requested
            elif entry["status"] == "in_progress":
                # Full requested amount — completed portion may already be in factory snapshot,
                # but we count the full run to avoid requesting duplicate production.
                requested = Decimal(str(entry.get("requested_m2") or 0))
                scheduled_production[pid] = scheduled_production.get(pid, Decimal(0)) + requested

    # ── STEP 2: Sort and classify boats ────────────────────────────────────
    # No anchor. Every boat gets simulated. State is for display only.

    boats_with_shipments = {
        b["id"] for b in boats if b["id"] in shipments_by_boat
    }

    sorted_boats = sorted(boats, key=lambda b: b["departure_date"])

    # ── STEP 3: Compute arriving_soon ─────────────────────────────────────

    arriving_soon: dict[str, Decimal] = {}
    for b in sorted_boats:
        if b["id"] not in shipments_by_boat:
            continue
        arrival = b["arrival_date"]
        if isinstance(arrival, str):
            arrival = date.fromisoformat(arrival)
        if arrival >= today:
            for pid, m2 in shipments_by_boat[b["id"]].items():
                arriving_soon[pid] = arriving_soon.get(pid, Decimal(0)) + m2

    # ── STEP 4: Classify boats (display state only) ──────────────────────

    def _boat_state(b: dict) -> str:
        bid = b["id"]
        dep = b["departure_date"]
        if isinstance(dep, str):
            dep = date.fromisoformat(dep)
        if bid in boats_with_shipments:
            return "DISPATCHED" if dep <= today else "CONFIRMED"
        status = draft_status_by_boat.get(bid, "")
        if status in ("ordered", "confirmed"):
            return "ORDERED"
        if status in ("drafting", "action_needed", "skipped"):
            return "PLANNING"
        return "FUTURE"

    simulate_boats = []
    for b in sorted_boats:
        b["_state"] = _boat_state(b)
        simulate_boats.append(b)

    # ── STEP 5: Initialize running stock ──────────────────────────────────

    running_stock: dict[str, Decimal] = {}
    for pid in product_ids:
        wh = Decimal(str(inventory.get(pid, 0)))
        arr = arriving_soon.get(pid, Decimal(0))
        running_stock[pid] = wh + arr

    # ── STEP 6: Simulate forward ──────────────────────────────────────────

    projections = []
    skip_recommendations = []
    debug_trace: list[dict] = []
    # Track unmet gaps per product for post-loop production requests.
    # Gaps tracked on ALL boats (including skipped) — production needs are
    # independent of whether any single boat is worth shipping.
    _gap_viable: dict[str, dict] = {}  # pid → first gap seen
    _viable_boat_count = 0

    for i, boat in enumerate(simulate_boats):
        next_boat = simulate_boats[i + 1] if i + 1 < len(simulate_boats) else None
        has_draft = boat["id"] in draft_boat_ids

        dep = boat["departure_date"]
        arr = boat["arrival_date"]
        if isinstance(dep, str):
            dep = date.fromisoformat(dep)
        if isinstance(arr, str):
            arr = date.fromisoformat(arr)

        boat_products = []
        boat_debug = []
        boat_total_pallets = Decimal(0)

        for pid in product_ids:
            velocity = Decimal(str(velocities.get(pid, 0)))
            warehouse_m2 = Decimal(str(inventory.get(pid, 0)))
            stock = running_stock[pid]
            factory_m2 = factory_avail.get(pid, Decimal(0))

            # ── Core formula ──────────────────────────────────────────
            if next_boat:
                next_arr = next_boat["arrival_date"]
                if isinstance(next_arr, str):
                    next_arr = date.fromisoformat(next_arr)
                days_to_next_resupply = max(1, (next_arr - today).days)
            else:
                days_to_next_resupply = max(1, (arr - today).days + 30)

            # No velocity = no consumption = no gap. Show product but don't restock.
            factory_max_pallets = int(factory_m2 / M2_PER_PALLET)

            if velocity <= 0:
                stock_at_next_resupply = stock
                coverage_gap = Decimal(0)
                suggested_pallets = 0
                can_ship = 0
            else:
                stock_at_next_resupply = stock - (velocity * days_to_next_resupply)
                coverage_gap = max(Decimal(0), SAFETY_STOCK_M2 - stock_at_next_resupply)
                suggested_pallets = int(
                    (coverage_gap / M2_PER_PALLET).to_integral_value(rounding=ROUND_UP)
                ) if coverage_gap > 0 else 0
                can_ship = min(suggested_pallets, factory_max_pallets)

            # ── Urgency (days of stock from today) ────────────────────
            if velocity > 0:
                days_of_stock = float(warehouse_m2 / velocity)
            else:
                days_of_stock = 999.0

            if days_of_stock < 7:
                urgency = "critical"
            elif days_of_stock < 14:
                urgency = "urgent"
            elif days_of_stock < 30:
                urgency = "soon"
            else:
                urgency = "ok"

            # ── Track unmet gap (for any boat — used as fallback) ────
            # Only track on first few boats — gaps on distant boats are noise.
            # ── Cascade: allocation priority ──────────────────────────
            # 1. Shipment exists → reality (confirmed dispatch, locked)
            # 2. Draft exists for this boat → Ashley's pallets (0 if absent)
            # 3. No draft, departure > today + LEAD_TIME_DAYS → brain suggests
            # 4. No draft, departure ≤ today + LEAD_TIME_DAYS → too late, 0
            boat_shipments = shipments_by_boat.get(boat["id"], {})
            boat_drafts = drafts_by_boat.get(boat["id"], {})
            too_late = (dep - today).days <= LEAD_TIME_DAYS
            has_shipment = boat["id"] in shipments_by_boat

            # Is this allocation a real commitment or just a brain suggestion?
            # Only commitments (shipments / saved drafts) reserve SIESA for later boats.
            is_committed_alloc = False
            if has_shipment:
                shipped_m2 = boat_shipments.get(pid, Decimal(0))
                allocated_pallets = int(shipped_m2 / M2_PER_PALLET)
                is_committed_alloc = True
            elif has_draft:
                allocated_pallets = int(boat_drafts.get(pid, {}).get("selected_pallets", 0))
                is_committed_alloc = True
            elif too_late:
                allocated_pallets = 0
            else:
                allocated_pallets = can_ship  # brain suggestion only

            boat_total_pallets += allocated_pallets

            # ── Build product detail ──────────────────────────────────
            boat_products.append({
                "product_id": pid,
                "sku": product_map[pid].get("sku", ""),
                "daily_velocity_m2": float(velocity),
                "current_stock_m2": float(warehouse_m2),
                "running_stock_m2": float(stock),
                "days_of_stock": round(days_of_stock, 1),
                "urgency": urgency,
                "days_to_next_resupply": days_to_next_resupply,
                "stock_at_next_resupply": float(stock_at_next_resupply),
                "coverage_gap_m2": float(coverage_gap),
                "suggested_pallets": suggested_pallets,
                "can_ship_pallets": can_ship,
                "allocated_pallets": allocated_pallets,
                "factory_available_m2": float(factory_m2),
                "factory_max_pallets": factory_max_pallets,
                "is_shipment_locked": has_shipment,
                "is_draft_committed": has_draft,
                "is_past_lead_time": too_late and not has_draft and not has_shipment,
                "_is_committed_alloc": is_committed_alloc,  # internal: cascade gate
            })

            boat_debug.append({
                "product_id": pid,
                "sku": product_map[pid].get("sku", ""),
                "inputs": {
                    "warehouse_m2": float(warehouse_m2),
                    "arriving_soon_m2": float(arriving_soon.get(pid, 0)),
                    "running_stock_before": float(stock),
                    "velocity": float(velocity),
                    "factory_available_m2": float(factory_m2),
                },
                "math": {
                    "days_to_next_resupply": days_to_next_resupply,
                    "stock_at_next_resupply": float(stock_at_next_resupply),
                    "safety_buffer_m2": float(SAFETY_STOCK_M2),
                    "coverage_gap_m2": float(coverage_gap),
                    "suggested_pallets": suggested_pallets,
                    "can_ship_pallets": can_ship,
                    "allocated_pallets": allocated_pallets,
                    "too_late": too_late and not has_draft,
                },
                "cascade": {
                    "running_stock_after": float(running_stock[pid]),
                    "factory_remaining_m2": float(factory_avail.get(pid, Decimal(0))),
                },
            })

        # ── Urgency breakdown ─────────────────────────────────────────
        urgency_counts = {"critical": 0, "urgent": 0, "soon": 0, "ok": 0}
        for p in boat_products:
            if p["suggested_pallets"] > 0:
                urgency_counts[p["urgency"]] += 1

        # ── Skip-boat check ───────────────────────────────────────────
        # Two separate concepts:
        # 1. skip_recommended: math says this boat isn't worth shipping (advisory)
        # 2. skip (actual): only skip cascade if no draft AND math says skip
        # draft_boat_ids includes empty drafts (skipped boats) — Ashley's decision is locked.
        is_locked = boat["_state"] in ("ORDERED", "DISPATCHED", "CONFIRMED") or has_draft or boat["id"] in boats_with_shipments

        # Always compute the recommendation based on math
        skip_recommended = boat_total_pallets < MIN_BOAT_PALLETS
        skip_reason = None
        if skip_recommended:
            skip_reason = (
                f"Solo {int(boat_total_pallets)} pallets "
                f"({int(boat_total_pallets / PALLETS_PER_CONTAINER)} contenedores). "
                f"Minimo es {MIN_BOAT_PALLETS} pallets ({MIN_BLS_PER_BOAT} contenedores)."
            )

        # Only actually skip cascade if Ashley hasn't touched it
        skip = skip_recommended and not is_locked

        # ── Track production gaps (ALL boats, regardless of skip) ─────
        # Production needs are independent of whether a boat is worth shipping.
        for p in boat_products:
            pid = p["product_id"]
            gap = Decimal(str(p["coverage_gap_m2"]))
            fac = Decimal(str(p["factory_available_m2"]))
            if gap > 0 and fac < gap and pid not in _gap_viable:
                _gap_viable[pid] = {
                    "gap_m2": gap - fac,
                    "urgency": p["urgency"],
                    "boat_id": boat["id"],
                    "boat_name": boat.get("name", ""),
                    "departure": str(dep),
                }

        # ── Apply cascade (only if boat is NOT skipped) ───────────────
        # Only COMMITTED allocations (shipments / saved drafts) reserve SIESA
        # and add to warehouse stock for later boats. Brain suggestions are
        # display-only — they don't take SIESA away from later boats until
        # Ashley saves a draft.
        #
        # Also: confirmed/dispatched shipments already consumed SIESA in the
        # snapshot (that's what SIESA reports), so we don't double-deduct.
        has_shipment_data = boat["id"] in boats_with_shipments
        if not skip:
            for p in boat_products:
                pid = p["product_id"]
                if not p.get("_is_committed_alloc"):
                    continue  # brain suggestion — do not cascade
                alloc_m2 = Decimal(p["allocated_pallets"]) * M2_PER_PALLET
                running_stock[pid] = running_stock[pid] + alloc_m2
                if not has_shipment_data:
                    consumed = min(alloc_m2, factory_avail.get(pid, Decimal(0)))
                    factory_avail[pid] = factory_avail.get(pid, Decimal(0)) - consumed
            _viable_boat_count += 1
        else:
            # Skipped: zero out allocations in the product details
            boat_total_pallets = Decimal(0)
            for p in boat_products:
                p["allocated_pallets"] = 0

        # Strip internal cascade gate from the output
        for p in boat_products:
            p.pop("_is_committed_alloc", None)

        days_until_dep = (dep - today).days

        projection = {
            "boat_id": boat["id"],
            "boat_name": boat.get("name", ""),
            "departure_date": str(dep),
            "arrival_date": str(arr),
            "days_until_departure": days_until_dep,
            "past_lead_time": too_late and not has_draft,
            "carrier": boat.get("carrier", ""),
            "state": boat["_state"],
            "draft_status": draft_status_by_boat.get(boat["id"]),
            "draft_id": draft_id_by_boat.get(boat["id"]),
            "total_pallets": int(boat_total_pallets),
            "total_containers": int(boat_total_pallets / PALLETS_PER_CONTAINER),
            "total_m2": float(Decimal(int(boat_total_pallets)) * M2_PER_PALLET),
            "urgency_breakdown": urgency_counts,
            "skip_recommended": skip_recommended,
            "skip_reason": skip_reason,
            "product_count": len([p for p in boat_products if p["suggested_pallets"] > 0]),
            "products": boat_products,
        }

        projections.append(projection)

        if skip_recommended:
            skip_recommendations.append({
                "boat_id": boat["id"],
                "boat_name": boat.get("name", ""),
                "departure_date": str(dep),
                "total_pallets": int(boat_total_pallets),
                "reason": skip_reason,
                "consolidate_onto": next_boat["name"] if next_boat else None,
            })

        debug_trace.append({
            "boat_id": boat["id"],
            "boat_name": boat.get("name", ""),
            "products": boat_debug,
        })

    # ── STEP 7: Production requests (post-loop) ─────────────────────────────
    # After simulating all boats, we know every product's unmet gap.
    # Prefer viable (non-skipped) boat as target; fall back to any boat.
    # Subtract already-scheduled production → what genuinely needs ordering.

    production_requests = []
    for pid, gap_info in _gap_viable.items():
        unmet_m2 = gap_info["gap_m2"]
        already_scheduled = scheduled_production.get(pid, Decimal(0))

        if unmet_m2 - already_scheduled <= 0:
            continue

        # Stockout date: when total available stock (warehouse + arriving) hits 0
        # Uses running_stock so in-transit goods push the date out correctly.
        # Temporary gaps (warehouse empty but ship arriving) are accepted.
        total_available = running_stock.get(pid, Decimal(0))
        vel = velocities.get(pid, Decimal(0))
        if vel > 0:
            days_to_stockout = int(total_available / vel)
            stockout_date = str(today + timedelta(days=days_to_stockout))
        else:
            days_to_stockout = 999
            stockout_date = None

        # Urgency based on REAL stockout (including in-transit), not warehouse-only.
        # This prevents false alarms for products covered by arriving ships.
        if days_to_stockout <= 7:
            real_urgency = "critical"
        elif days_to_stockout <= 21:
            real_urgency = "urgent"
        elif days_to_stockout <= 45:
            real_urgency = "soon"
        else:
            real_urgency = "ok"

        production_requests.append({
            "product_id": pid,
            "sku": product_map[pid].get("sku", ""),
            "urgency": real_urgency,
            "is_piggyback": already_scheduled > 0,
            "scheduled_m2": float(already_scheduled),
            "additional_m2": float(unmet_m2 - already_scheduled),
            "stockout_date": stockout_date,
            "gap_boat_departure": gap_info["departure"],
        })

    # Sort: critical first, then urgent, then soon
    _urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}
    production_requests.sort(key=lambda r: _urgency_order.get(r["urgency"], 5))

    # ── STEP 8b: Production pipeline (grouped by product) ──────────────
    # One line per product showing total in pipeline and whether it covers the gap.

    production_pipeline = []
    for pid in product_ids:
        entries = production_by_product.get(pid, [])
        if not entries:
            continue

        total_requested = Decimal(0)
        total_completed = Decimal(0)
        has_in_progress = False
        earliest_date = None

        for entry in entries:
            req = Decimal(str(entry.get("requested_m2") or 0))
            comp = Decimal(str(entry.get("completed_m2") or 0))
            total_requested += req
            total_completed += comp
            if entry["status"] == "in_progress":
                has_in_progress = True
            sd = entry.get("scheduled_date")
            if sd and (earliest_date is None or sd < earliest_date):
                earliest_date = sd

        total_scheduled_m2 = scheduled_production.get(pid, Decimal(0))
        gap_info = _gap_viable.get(pid)
        unmet = gap_info["gap_m2"] if gap_info else Decimal(0)
        covers_gap = total_scheduled_m2 >= unmet
        progress = float(total_completed / total_requested * 100) if total_requested > 0 else 0

        production_pipeline.append({
            "product_id": pid,
            "sku": product_map[pid].get("sku", ""),
            "status": "in_progress" if has_in_progress else "scheduled",
            "total_m2": float(total_requested),
            "completed_m2": float(total_completed),
            "progress_pct": round(progress, 1),
            "earliest_date": earliest_date,
            "covers_gap": covers_gap,
            "gap_m2": float(unmet),
        })

    # Sort: in_progress first, then by earliest date
    production_pipeline.sort(key=lambda p: (0 if p["status"] == "in_progress" else 1, p["earliest_date"] or ""))

    # ── STEP 9: Factory order signal ──────────────────────────────────────

    factory_order_signal = _compute_factory_order_signal(
        production_requests=production_requests,
    )

    return {
        "projections": projections,
        "production_requests": production_requests,
        "production_pipeline": production_pipeline,
        "skip_recommendations": skip_recommendations,
        "factory_order_signal": factory_order_signal,
        "data_as_of": {
            "computed_at": str(today),
            "product_count": len(product_ids),
            "boat_count": len(sorted_boats),
        },
        "_debug": debug_trace,
    }


# ---------------------------------------------------------------------------
# Helpers (private, no DB)
# ---------------------------------------------------------------------------


def _compute_factory_order_signal(
    *,
    production_requests: list[dict],
) -> dict | None:
    """Are there products that need scheduling? How many?"""
    if not production_requests:
        return None

    return {
        "needs_scheduling": True,
        "product_count": len(production_requests),
        "piggyback_count": sum(1 for r in production_requests if r.get("is_piggyback")),
        "new_count": sum(1 for r in production_requests if not r.get("is_piggyback")),
        "limiting_product_sku": production_requests[0]["sku"],
    }
