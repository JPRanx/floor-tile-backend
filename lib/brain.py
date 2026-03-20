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

    # Index drafts by boat_id → product_id
    drafts_by_boat: dict[str, dict[str, dict]] = {}
    draft_status_by_boat: dict[str, str] = {}
    draft_id_by_boat: dict[str, str] = {}
    for d in drafts:
        bid = d["boat_id"]
        pid = d["product_id"]
        drafts_by_boat.setdefault(bid, {})[pid] = d
        if "status" in d:
            draft_status_by_boat[bid] = d["status"]
        if "draft_id" in d:
            draft_id_by_boat[bid] = d["draft_id"]

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

    # ── STEP 2: Find anchor boat ─────────────────────────────────────────

    boats_with_shipments = {
        b["id"] for b in boats if b["id"] in shipments_by_boat
    }

    sorted_boats = sorted(boats, key=lambda b: b["departure_date"])

    anchor_boat_id = None
    for b in reversed(sorted_boats):
        if b["id"] in boats_with_shipments:
            anchor_boat_id = b["id"]
            break

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

    # ── STEP 4: Classify boats ────────────────────────────────────────────

    def _boat_state(b: dict) -> str:
        bid = b["id"]
        dep = b["departure_date"]
        if isinstance(dep, str):
            dep = date.fromisoformat(dep)

        if bid in boats_with_shipments:
            return "DISPATCHED"
        status = draft_status_by_boat.get(bid, "")
        if status in ("ordered", "confirmed"):
            return "ORDERED"
        if status in ("drafting", "action_needed"):
            return "PLANNING"
        return "FUTURE"

    completed_boats = []
    simulate_boats = []

    anchor_seen = anchor_boat_id is None  # if no anchor, simulate all

    for b in sorted_boats:
        state = _boat_state(b)
        b["_state"] = state

        if not anchor_seen:
            if b["id"] == anchor_boat_id:
                anchor_seen = True
            # Anchor and everything before it → completed
            completed_boats.append(b)
            continue

        # After anchor → simulate
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
    # Prefer non-skipped boats as targets, fall back to skipped if all skip.
    # Only track gaps on the first 2 viable boats — beyond that is noise.
    _gap_viable: dict[str, dict] = {}  # pid → gap info from non-skipped boats
    _gap_any: dict[str, dict] = {}     # pid → gap info from any boat (fallback)
    _viable_boat_count = 0
    MAX_GAP_BOATS = 2  # Only request production for gaps on the next 2 shippable boats

    for i, boat in enumerate(simulate_boats):
        next_boat = simulate_boats[i + 1] if i + 1 < len(simulate_boats) else None

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
                can_ship = min(suggested_pallets, int(factory_m2 / M2_PER_PALLET))

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
            if i < MAX_GAP_BOATS and coverage_gap > 0 and factory_m2 < coverage_gap and pid not in _gap_any:
                _gap_any[pid] = {
                    "gap_m2": coverage_gap - factory_m2,
                    "urgency": urgency,
                    "boat_id": boat["id"],
                    "boat_name": boat.get("name", ""),
                    "departure": str(dep),
                }

            # ── Cascade: determine what this boat actually carries ─────
            boat_state = boat["_state"]
            boat_drafts = drafts_by_boat.get(boat["id"], {})

            if boat_state == "ORDERED" and pid in boat_drafts:
                allocated_pallets = int(boat_drafts[pid].get("selected_pallets", 0))
            elif boat_state == "PLANNING" and pid in boat_drafts:
                allocated_pallets = int(boat_drafts[pid].get("selected_pallets", 0))
            else:
                allocated_pallets = can_ship

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
                "is_draft_committed": pid in boat_drafts,
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
        # If boat doesn't justify shipping, zero it out.
        # Running stock stays unchanged — nothing ships on a skipped boat.
        skip = False
        skip_reason = None
        has_draft = boat["id"] in drafts_by_boat
        is_locked = boat["_state"] == "ORDERED" or has_draft  # Ashley touched it — never skip

        if not is_locked and boat_total_pallets < MIN_BOAT_PALLETS:
            skip = True
            skip_reason = (
                f"Only {int(boat_total_pallets)} pallets "
                f"({int(boat_total_pallets / PALLETS_PER_CONTAINER)} containers). "
                f"Minimum is {MIN_BOAT_PALLETS} pallets ({MIN_BLS_PER_BOAT} containers)."
            )

        # ── Apply cascade (only if boat is NOT skipped) ───────────────
        if not skip:
            for p in boat_products:
                pid = p["product_id"]
                alloc_m2 = Decimal(p["allocated_pallets"]) * M2_PER_PALLET
                running_stock[pid] = running_stock[pid] + alloc_m2
                consumed = min(alloc_m2, factory_avail.get(pid, Decimal(0)))
                factory_avail[pid] = factory_avail.get(pid, Decimal(0)) - consumed

                # Track unmet gap on viable (non-skipped) boats — preferred target.
                # Only track on first few viable boats.
                gap = Decimal(str(p["coverage_gap_m2"]))
                fac = Decimal(str(p["factory_available_m2"]))
                if _viable_boat_count < MAX_GAP_BOATS and gap > 0 and fac < gap and pid not in _gap_viable:
                    _gap_viable[pid] = {
                        "gap_m2": gap - fac,
                        "urgency": p["urgency"],
                        "boat_id": boat["id"],
                        "boat_name": boat.get("name", ""),
                        "departure": str(dep),
                    }
            _viable_boat_count += 1
        else:
            # Skipped: zero out allocations in the product details
            boat_total_pallets = Decimal(0)
            for p in boat_products:
                p["allocated_pallets"] = 0

        days_until_dep = (dep - today).days

        projection = {
            "boat_id": boat["id"],
            "boat_name": boat.get("name", ""),
            "departure_date": str(dep),
            "arrival_date": str(arr),
            "days_until_departure": days_until_dep,
            "carrier": boat.get("carrier", ""),
            "state": boat["_state"],
            "draft_status": draft_status_by_boat.get(boat["id"]),
            "draft_id": draft_id_by_boat.get(boat["id"]),
            "total_pallets": int(boat_total_pallets),
            "total_containers": int(boat_total_pallets / PALLETS_PER_CONTAINER),
            "total_m2": float(Decimal(int(boat_total_pallets)) * M2_PER_PALLET),
            "urgency_breakdown": urgency_counts,
            "skip_recommended": skip,
            "skip_reason": skip_reason,
            "product_count": len([p for p in boat_products if p["suggested_pallets"] > 0]),
            "products": boat_products,
        }

        projections.append(projection)

        if skip:
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

    # ── STEP 7: Build completed boats ─────────────────────────────────────

    completed = []
    for b in completed_boats:
        bid = b["id"]
        dep = b["departure_date"]
        arr = b["arrival_date"]
        if isinstance(dep, str):
            dep = date.fromisoformat(dep)
        if isinstance(arr, str):
            arr = date.fromisoformat(arr)

        items = []
        if bid in shipments_by_boat:
            for pid, m2 in shipments_by_boat[bid].items():
                items.append({
                    "product_id": pid,
                    "sku": product_map.get(pid, {}).get("sku", ""),
                    "shipped_m2": float(m2),
                    "shipped_pallets": int(m2 / M2_PER_PALLET),
                })
        elif bid in drafts_by_boat:
            for pid, d in drafts_by_boat[bid].items():
                items.append({
                    "product_id": pid,
                    "sku": product_map.get(pid, {}).get("sku", ""),
                    "selected_pallets": int(d.get("selected_pallets", 0)),
                })

        completed.append({
            "boat_id": bid,
            "boat_name": b.get("name", ""),
            "departure_date": str(dep),
            "arrival_date": str(arr),
            "days_until_departure": (dep - today).days,
            "carrier": b.get("carrier", ""),
            "state": b["_state"],
            "draft_status": draft_status_by_boat.get(bid),
            "draft_id": draft_id_by_boat.get(bid),
            "items": items,
        })

    # ── STEP 8: Production requests (post-loop) ────────────────────────────
    # After simulating all boats, we know every product's unmet gap.
    # Prefer viable (non-skipped) boat as target; fall back to any boat.
    # Subtract already-scheduled production → what genuinely needs ordering.

    worst_unmet_gap: dict[str, dict] = {}
    for pid in set(_gap_any) | set(_gap_viable):
        worst_unmet_gap[pid] = _gap_viable.get(pid) or _gap_any[pid]

    production_requests = []
    for pid, gap_info in worst_unmet_gap.items():
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
        gap_info = worst_unmet_gap.get(pid)
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
        "completed": completed,
        "production_requests": production_requests,
        "production_pipeline": production_pipeline,
        "skip_recommendations": skip_recommendations,
        "factory_order_signal": factory_order_signal,
        "anchor_boat_id": anchor_boat_id,
        "data_as_of": {
            "computed_at": str(today),
            "product_count": len(product_ids),
            "boat_count": len(sorted_boats),
            "anchor_boat_id": anchor_boat_id,
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
