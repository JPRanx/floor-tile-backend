"""
Reconciled V1 — pure sailing math (spec §3.2/§4.4/§6).

Timing is derived from the sailing departure date only [D3]; the constants
are the verified source values recovered from the old application
[VERIFIED-SOURCE: models/boat_schedule.py, services/boat_schedule_service.py,
config/shipping.py]. Ashley never re-enters a derived deadline.

Everything here is a pure function of its inputs: no clock access, no state.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, Sequence

from .planning_math import (  # preserved primitives [PRESERVED-3.1]
    HALF_PALLET_M2 as HALF_PALLET, M2_PER_PALLET, q2, round_up_to_half_pallet,
)

ZERO = Decimal("0")

# [VERIFIED-SOURCE: models/boat_schedule.py]
ORDER_DEADLINE_DAYS = 20
HARD_DEADLINE_DAYS = 10
BOOKING_BUFFER_DAYS = 3
# [VERIFIED-SOURCE: services/boat_schedule_service.py — settings default]
PRODUCTION_READINESS_BUFFER_DAYS = 5
# [VERIFIED-SOURCE: config/shipping.py]
WAREHOUSE_BUFFER_DAYS = 6
ORDERING_CYCLE_DAYS = 30            # "fallback when no next boat scheduled"

PALLETS_PER_CONTAINER = 13
CONTAINERS_PER_BL_DEFAULT = 3       # [D4] default, never an invariant


# ── D3 timing derivation ────────────────────────────────────────────────────

@dataclass(frozen=True)
class SailingTiming:
    departure: Optional[date]
    planning_anchor: date
    planning_basis: str
    order_by: date
    hard_cutoff: Optional[date]
    booking_date: Optional[date]
    production_readiness: date
    loading_terminal_eta: Optional[date]
    eta: date
    warehouse_arrival: date
    arrival_basis: str
    assumed_voyage_days: Optional[int]
    timing_state: str               # legacy: normal|late|exceptional|departed; roster: normal|closed|arrived
    days_to_order_by: int
    days_to_departure: Optional[int]


def timing_state(*, departure: date, today: date) -> str:
    """[D3, AB8] exact boundaries: normal through the order-by day; late
    (grace) STRICTLY BEFORE the hard cutoff; at `departure − 10 days` the
    sailing leaves the normal available set (exceptional, inclusive);
    departed on/after departure.

        normal      : today ≤ departure − 20
        late        : departure − 20 < today < departure − 10
        exceptional : departure − 10 ≤ today < departure
        departed    : today ≥ departure
    """
    order_by = departure - timedelta(days=ORDER_DEADLINE_DAYS)
    hard_cutoff = departure - timedelta(days=HARD_DEADLINE_DAYS)
    if today >= departure:
        return "departed"
    if today >= hard_cutoff:
        return "exceptional"
    if today > order_by:
        return "late"
    return "normal"


def schedule_timing_state(*, departure: Optional[date], bl_vgm_close=None,
                          loading_terminal_eta: Optional[date] = None,
                          today: date) -> str:
    if departure is not None:
        return timing_state(departure=departure, today=today)
    if bl_vgm_close is None or loading_terminal_eta is None:
        raise ValueError("sailing schedule has neither departure nor complete B/L-VGM basis")
    anchor = bl_vgm_close.date() if hasattr(bl_vgm_close, "date") else bl_vgm_close
    return ("arrived" if today >= loading_terminal_eta
            else "closed" if today > anchor else "normal")


def derive_timing(*, departure: date, voyage_days: int, today: date) -> SailingTiming:
    order_by = departure - timedelta(days=ORDER_DEADLINE_DAYS)
    eta = departure + timedelta(days=voyage_days)
    return SailingTiming(
        departure=departure,
        planning_anchor=departure,
        planning_basis="departure",
        order_by=order_by,
        hard_cutoff=departure - timedelta(days=HARD_DEADLINE_DAYS),
        booking_date=departure - timedelta(days=BOOKING_BUFFER_DAYS),
        production_readiness=order_by - timedelta(days=PRODUCTION_READINESS_BUFFER_DAYS),
        loading_terminal_eta=None,
        eta=eta,
        warehouse_arrival=eta + timedelta(days=WAREHOUSE_BUFFER_DAYS),
        arrival_basis="departure_plus_voyage_days",
        assumed_voyage_days=voyage_days,
        timing_state=timing_state(departure=departure, today=today),
        days_to_order_by=(order_by - today).days,
        days_to_departure=(departure - today).days,
    )


def derive_roster_timing(*, bl_vgm_close, loading_terminal_eta: date,
                         voyage_days: int, today: date) -> SailingTiming:
    """Timing for an explicit carrier roster that contains no departure.

    B/L-VGM closure is the operator-selected order/planning anchor. The source
    ETA is arrival at the loading terminal. Destination and warehouse arrival
    are explicitly estimated from configured voyage duration; no departure is
    copied or invented.
    """
    anchor = bl_vgm_close.date() if hasattr(bl_vgm_close, "date") else bl_vgm_close
    destination_eta = loading_terminal_eta + timedelta(days=voyage_days)
    state = schedule_timing_state(
        departure=None, bl_vgm_close=bl_vgm_close,
        loading_terminal_eta=loading_terminal_eta, today=today)
    return SailingTiming(
        departure=None,
        planning_anchor=anchor,
        planning_basis="bl_vgm_close",
        order_by=anchor,
        hard_cutoff=None,
        booking_date=None,
        production_readiness=anchor - timedelta(days=PRODUCTION_READINESS_BUFFER_DAYS),
        loading_terminal_eta=loading_terminal_eta,
        eta=destination_eta,
        warehouse_arrival=destination_eta + timedelta(days=WAREHOUSE_BUFFER_DAYS),
        arrival_basis="loading_terminal_eta_plus_assumed_voyage_days",
        assumed_voyage_days=voyage_days,
        timing_state=state,
        days_to_order_by=(anchor - today).days,
        days_to_departure=None,
    )


# ── window (§6.4) ───────────────────────────────────────────────────────────

def next_arrival_after(*, focus_arrival: date,
                       other_arrivals: Sequence[date]) -> tuple[date, bool]:
    """Earliest known arrival strictly after the focus sailing's arrival;
    otherwise the ORDERING_CYCLE_DAYS fallback, flagged for the trace."""
    later = sorted(a for a in other_arrivals if a > focus_arrival)
    if later:
        return later[0], False
    return focus_arrival + timedelta(days=ORDERING_CYCLE_DAYS), True


# ── half-pallet helpers [SETTLED-JORGE D1] ─────────────────────────────────

def floor_to_half_pallet(m2: Decimal) -> Decimal:
    """Availability caps round DOWN — never suggest more than SIESA shows."""
    if m2 <= 0:
        return q2(ZERO)
    halves = int(Decimal(m2) / HALF_PALLET)
    return q2(halves * HALF_PALLET)


# ── recommendation (§6.4) ──────────────────────────────────────────────────

@dataclass(frozen=True)
class RecommendResult:
    need_m2: Decimal                # uncapped true need (shown beside cap)
    suggested_m2: Decimal           # availability-capped, half-pallet multiple
    uncovered_m2: Decimal           # → monthly manufacturing context
    capped: bool                    # availability bound the suggestion
    watch_only: bool                # zero-velocity branch


def recommend(*, daily_velocity: Decimal, buffer_m2: Decimal,
              window_days: int, dated_supply: Decimal,
              siesa_available_effective: Decimal) -> RecommendResult:
    """The sailing suggestion uses CURRENT effective SIESA availability only
    [WWO6 #9]; the uncapped need is preserved so shortage vs available-now
    stays explicit; uncovered need feeds the monthly manufacturing context
    [WWO6 #6] — never boat-bound production."""
    if daily_velocity <= 0:
        # preserved zero-velocity branch: no formula runs [PRESERVED-3.1]
        return RecommendResult(q2(ZERO), q2(ZERO), q2(ZERO),
                               capped=False, watch_only=True)
    projected = Decimal(daily_velocity) * Decimal(window_days)
    need = max(ZERO, projected + Decimal(buffer_m2) - Decimal(dated_supply))
    need_rounded = round_up_to_half_pallet(need) if need > 0 else q2(ZERO)
    cap = floor_to_half_pallet(Decimal(siesa_available_effective))
    suggested = min(need_rounded, cap)
    uncovered = max(ZERO, need_rounded - cap)
    return RecommendResult(q2(need), q2(suggested), q2(uncovered),
                           capped=bool(need_rounded > cap),
                           watch_only=False)


# ── derivation chain: totals → pallets → containers → BLs (§6.6) [D4] ──────

def containers_for_pallets(total_pallets: Decimal) -> int:
    if total_pallets <= 0:
        return 0
    return math.ceil(Decimal(total_pallets) / PALLETS_PER_CONTAINER)


def default_bl_count(containers: int) -> int:
    if containers <= 0:
        return 0
    return math.ceil(containers / CONTAINERS_PER_BL_DEFAULT)


def _container_distribution(containers: int) -> list[int]:
    """3-3-…-remainder [D4 default]."""
    counts = []
    left = containers
    while left > 0:
        take = min(CONTAINERS_PER_BL_DEFAULT, left)
        counts.append(take)
        left -= take
    return counts


def default_bl_groups(lines: Sequence[tuple[str, Decimal]]) -> list[dict]:
    """Deterministic default split: stable line order, fill each BL group to
    its pallet capacity. Group capacities are whole-pallet multiples of 13,
    so every split boundary lands on a half-pallet multiple (physical)."""
    total_pallets = sum((Decimal(m2) / M2_PER_PALLET for _, m2 in lines),
                       ZERO)
    containers = containers_for_pallets(total_pallets)
    if containers == 0:
        return []
    groups = []
    dist = _container_distribution(containers)
    line_iter = [(pid, Decimal(m2)) for pid, m2 in lines]
    idx = 0
    remaining_in_line = line_iter[0][1] if line_iter else ZERO
    for bl_no, ccount in enumerate(dist, start=1):
        capacity_m2 = Decimal(ccount) * PALLETS_PER_CONTAINER * M2_PER_PALLET
        glines = []
        while capacity_m2 > 0 and idx < len(line_iter):
            pid, _ = line_iter[idx]
            take = min(remaining_in_line, capacity_m2)
            if take > 0:
                glines.append({"product_id": pid, "m2": q2(take)})
                capacity_m2 -= take
                remaining_in_line -= take
            if remaining_in_line == 0:
                idx += 1
                remaining_in_line = (line_iter[idx][1]
                                     if idx < len(line_iter) else ZERO)
        groups.append({"bl_no": bl_no, "container_count": ccount,
                       "lines": glines})
    return groups


def validate_bl_group_identity(groups: Sequence[dict]) -> None:
    """[D4, AB4] BL/order identity: every group carries a unique positive
    BL identifier (order references derive from it, so uniqueness here is
    order-reference uniqueness), a positive whole container count, and a
    non-empty line list. Raises ValueError with the exact failure."""
    seen: set = set()
    for g in groups:
        bl_no = g.get("bl_no")
        if not isinstance(bl_no, int) or isinstance(bl_no, bool) \
                or bl_no <= 0:
            raise ValueError(
                f"BL identifier {bl_no!r} is invalid — identifiers must be "
                "positive whole numbers [AB4]")
        if bl_no in seen:
            raise ValueError(
                f"duplicate BL number {bl_no} — BL numbers (and therefore "
                "order references) must be unique [AB4]")
        seen.add(bl_no)
        cc = g.get("container_count")
        if not isinstance(cc, int) or isinstance(cc, bool) or cc <= 0:
            raise ValueError(
                f"BL {bl_no}: container count {cc!r} is invalid — every "
                "group carries at least one whole container [AB4]")
        if not g.get("lines"):
            raise ValueError(
                f"BL {bl_no}: a BL/order group cannot be empty — every "
                "group carries at least one product line [AB4]")


def validate_bl_conservation(lines: Sequence[tuple[str, Decimal]],
                             groups: Sequence[dict]) -> None:
    """[D4] Reallocation must preserve sailing totals exactly: per-product
    m² totals, the container total, per-group pallet capacity, and physical
    half-pallet parts. Group identity (unique positive BL numbers, positive
    container counts, non-empty groups) is validated first [AB4]. Raises
    ValueError with the exact failure."""
    validate_bl_group_identity(groups)
    want = {}
    for pid, m2 in lines:
        want[pid] = want.get(pid, ZERO) + Decimal(m2)
    got = {}
    for g in groups:
        gp = ZERO
        for l in g["lines"]:
            m2 = Decimal(l["m2"])
            if m2 <= 0 or (m2 % HALF_PALLET) != 0:
                raise ValueError(
                    f"BL {g['bl_no']} line {l['product_id']}: {m2} m² is not "
                    f"a positive half-pallet multiple (67.2)")
            got[l["product_id"]] = got.get(l["product_id"], ZERO) + m2
            gp += m2 / M2_PER_PALLET
        if gp > Decimal(g["container_count"]) * PALLETS_PER_CONTAINER:
            raise ValueError(
                f"BL {g['bl_no']} exceeds capacity: {gp} pallets into "
                f"{g['container_count']} container(s)")
    if want != got:
        raise ValueError(
            "conservation violated: sailing totals changed by reallocation "
            f"(expected {want}, got {got}) — reallocation must never add, "
            "drop, or duplicate shipment quantity")
    total_pallets = sum((v / M2_PER_PALLET for v in want.values()), ZERO)
    expected_containers = containers_for_pallets(total_pallets)
    actual_containers = sum(g["container_count"] for g in groups)
    if actual_containers != expected_containers:
        raise ValueError(
            f"container total changed: plan derives {expected_containers}, "
            f"groups carry {actual_containers}")
