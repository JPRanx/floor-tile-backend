"""
Clean V1 pure calculation contracts (accepted spec §6). No I/O, no state.

Extracted primitives (tiers, buffer, coverage) preserve current-Brain behavior
exactly — proven against the I0 characterization fixtures. The deliberate,
Jorge-settled divergences (D1 half-pallet increments; §6.4b zero-velocity
branch as the single rule; §6.3 corrected warehouse-arrival chain) are V1
product law, each carried by its own tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_UP, ROUND_FLOOR
from typing import Optional, Sequence

from lib.constants import (  # verified snapshot constants (§6.1) — reused as-is
    M2_PER_PALLET,
    PRODUCTION_LEAD_DAYS,
    TRUCK_TO_PORT_DAYS,
    TIER_BUFFER_CONFIG,
)
from .config import PlanningConfig

# §6.1: WAREHOUSE_BUFFER_DAYS = PORT_BUFFER_DAYS (5) + TRUCKING_DAYS (1)
# [VERIFIED-SOURCE: config/shipping.py] — destination port + trucking → warehouse
WAREHOUSE_BUFFER_DAYS = 6

HALF_PALLET_M2 = M2_PER_PALLET / 2          # 67.2 — D1 settled increment
TWO_DP = Decimal("0.01")


def q2(x: Decimal) -> Decimal:
    """Quantities are m² Decimals with two decimals (§8.3 V1)."""
    return Decimal(x).quantize(TWO_DP)


# ── Extracted reused primitives (behavior-preserving; I0 fixtures) ──────────

def classify_tiers(product_ids: list[str], velocities: dict[str, Decimal]) -> dict[str, str]:
    """Tier by 90-day velocity: top 25% = A, mid 50% = B, bottom 25%/zero = C.
    Extraction of lib.brain._classify_tiers (unchanged behavior)."""
    tier_map: dict[str, str] = {pid: "C" for pid in product_ids}
    pairs = [(pid, Decimal(str(velocities.get(pid, 0)))) for pid in product_ids]
    with_vel = [(pid, v) for pid, v in pairs if v > 0]
    with_vel.sort(key=lambda x: x[1], reverse=True)
    n = len(with_vel)
    if n == 0:
        return tier_map
    a_cut = max(1, int(n * 0.25))
    c_start = max(a_cut, int(n * 0.75))
    for i, (pid, _) in enumerate(with_vel):
        if i < a_cut:
            tier_map[pid] = "A"
        elif i < c_start:
            tier_map[pid] = "B"
    return tier_map


def compute_buffer_m2(daily_velocity: Decimal, tier: str) -> Decimal:
    """Per-product safety buffer in m², bounded by tier floor/ceiling.
    Extraction of lib.brain._compute_buffer_m2 (unchanged behavior)."""
    cfg = TIER_BUFFER_CONFIG[tier]
    weekly_velocity = daily_velocity * Decimal(7)
    raw = weekly_velocity * Decimal(cfg["weeks"])
    floor_m2 = Decimal(cfg["floor_pallets"]) * M2_PER_PALLET
    ceil_m2 = Decimal(cfg["ceiling_pallets"]) * M2_PER_PALLET
    return max(floor_m2, min(raw, ceil_m2))


def buffer_for(tier: str, daily_velocity: Decimal,
               peak_velocity: Optional[Decimal] = None) -> Decimal:
    """§6.2: tier A uses PEAK velocity when available (>0); B/C use average.
    Mirrors the tier-A peak rule in compute_horizon."""
    if tier == "A" and peak_velocity is not None and peak_velocity > 0:
        return compute_buffer_m2(peak_velocity, tier)
    return compute_buffer_m2(daily_velocity, tier)


def days_of_stock(warehouse_m2, velocity_per_day) -> Optional[Decimal]:
    """Extraction of lib.coverage.days_of_stock (unchanged behavior)."""
    wh = Decimal(str(warehouse_m2))
    vel = Decimal(str(velocity_per_day))
    if vel <= 0:
        return None
    return wh / vel


# ── D1 half-pallet increments (settled V1 product rule; no whole-pallet fallback)

def round_up_to_half_pallet(m2: Decimal) -> Decimal:
    if m2 <= 0:
        return q2(Decimal(0))
    halves = (Decimal(m2) / HALF_PALLET_M2).to_integral_value(rounding=ROUND_UP)
    return q2(halves * HALF_PALLET_M2)


def is_valid_selection(m2: Decimal) -> bool:
    """Selections are positive multiples of 67.2 m² (§8.3 V1+V4)."""
    if m2 <= 0:
        return False
    return (Decimal(m2) % HALF_PALLET_M2) == 0


def nearest_valid_increments(m2: Decimal) -> tuple[Decimal, Decimal]:
    """The two nearest valid values for reject-with-options UX (§6.5)."""
    lo_halves = (Decimal(m2) / HALF_PALLET_M2).to_integral_value(rounding=ROUND_FLOOR)
    lo = lo_halves * HALF_PALLET_M2
    if lo <= 0:
        lo = HALF_PALLET_M2
    hi = round_up_to_half_pallet(Decimal(m2))
    if hi <= 0:
        hi = HALF_PALLET_M2
    return q2(lo), q2(hi)


def derived_pallets(m2: Decimal) -> Decimal:
    return (Decimal(m2) / M2_PER_PALLET).quantize(TWO_DP)


# ── §6.3 timing contract ────────────────────────────────────────────────────

def next_cycle_order_date(today: date, config: PlanningConfig,
                          next_order_submitted_on: Optional[date] = None) -> date:
    """Deterministic next-cycle order date: submitted date wins; else the
    configured day (default 25 — window end) of the following month."""
    if next_order_submitted_on is not None:
        return next_order_submitted_on
    year, month = today.year, today.month + 1
    if month > 12:
        year, month = year + 1, 1
    return date(year, month, config.next_cycle_order_day)


def warehouse_arrival_lead_days(voyage_days: int) -> int:
    """Order → WAREHOUSE arrival. Never bare TOTAL_LEAD_DAYS=30 (origin port only)."""
    return PRODUCTION_LEAD_DAYS + TRUCK_TO_PORT_DAYS + voyage_days + WAREHOUSE_BUFFER_DAYS


def next_replenishment_date(order_date: date, voyage_days: int) -> date:
    return order_date + timedelta(days=warehouse_arrival_lead_days(voyage_days))


def protection_horizon_days(today: date, replenishment: date) -> int:
    return (replenishment - today).days


def expected_arrival_for_proposed_order(today: date, voyage_days: int) -> date:
    return today + timedelta(days=warehouse_arrival_lead_days(voyage_days))


# §6.7 dating rules per supply kind (deterministic)

def arrival_date_in_transit(eta: Optional[date], as_of: date, voyage_days: int) -> date:
    if eta is not None:
        return eta + timedelta(days=WAREHOUSE_BUFFER_DAYS)
    # conservative no-ETA fallback (matched and unmatched transit alike)
    return as_of + timedelta(days=voyage_days + WAREHOUSE_BUFFER_DAYS)


def arrival_date_produced(produced_on: date, voyage_days: int) -> date:
    return produced_on + timedelta(
        days=TRUCK_TO_PORT_DAYS + voyage_days + WAREHOUSE_BUFFER_DAYS)


def arrival_date_confirmed(submitted_on: date, voyage_days: int) -> date:
    return submitted_on + timedelta(days=warehouse_arrival_lead_days(voyage_days))


def arrival_date_amendment(estimated_delivery: Optional[date],
                           scheduled_start: Optional[date], voyage_days: int) -> date:
    if estimated_delivery is not None:
        return estimated_delivery + timedelta(
            days=TRUCK_TO_PORT_DAYS + voyage_days + WAREHOUSE_BUFFER_DAYS)
    # conservative upper bound: full production lead from scheduled start
    return scheduled_start + timedelta(days=warehouse_arrival_lead_days(voyage_days))


# ── §6.4b true need and residual need ───────────────────────────────────────

@dataclass(frozen=True)
class NeedResult:
    projected_demand: Decimal
    true_need_m2: Decimal
    residual_need_m2: Decimal
    counted_supply_m2: Decimal          # dated supply admitted by the guard
    excluded_supply_m2: Decimal         # dated supply past next replenishment


def true_and_residual_need(*, daily_velocity: Decimal, buffer_m2: Decimal,
                           horizon_days: int, supply_now: Decimal,
                           dated_supply: Sequence[tuple[date, Decimal]],
                           pending_commitment_m2: Decimal,
                           next_replenishment_date: date) -> NeedResult:
    """§6.4b. `dated_supply` carries (expected_warehouse_arrival, m2) for
    supply_moving + supply_expected quantities (per §6.7 dating rules).
    Soft production is NEVER an input here. Pending commitment is NOT supply.
    """
    zero = q2(Decimal(0))
    if daily_velocity <= 0:
        # Zero-velocity branch — checked FIRST, before any formula (§6.4b)
        return NeedResult(zero, zero, zero, zero, zero)

    counted = sum((m2 for d, m2 in dated_supply if d <= next_replenishment_date),
                  Decimal(0))
    excluded = sum((m2 for d, m2 in dated_supply if d > next_replenishment_date),
                   Decimal(0))
    projected_demand = q2(daily_velocity * Decimal(horizon_days))
    true_need = max(Decimal(0),
                    projected_demand + buffer_m2 - supply_now - counted)
    residual = max(Decimal(0), true_need - pending_commitment_m2)
    return NeedResult(projected_demand, q2(true_need), q2(residual),
                      q2(counted), q2(excluded))


def suggest_m2(residual_need_m2: Decimal, *, daily_velocity: Decimal) -> Decimal:
    """§6.5 — zero-velocity branch FIRST; else round residual up to half-pallet."""
    if daily_velocity <= 0:
        return q2(Decimal(0))
    return round_up_to_half_pallet(residual_need_m2)


def zero_velocity_watch(*, daily_velocity: Decimal, warehouse_m2: Decimal,
                        buffer_m2: Decimal) -> bool:
    """Watch signal (never a suggestion) for zero-velocity stock below buffer."""
    return daily_velocity <= 0 and warehouse_m2 < buffer_m2


# ── §6.7 time-phased buffer-protection check ────────────────────────────────

@dataclass(frozen=True)
class ProjectionResult:
    minima: tuple                        # ((date, Decimal), …) at event⁻ / horizon⁻
    outcome: str                         # protected | dip_below_buffer | projected_stockout
    dip_start: Optional[date] = None
    dip_recovery: Optional[date] = None
    dip_reentry: Optional[date] = None
    stockout_date: Optional[date] = None
    trace: tuple = ()


def project_buffer_protection(*, supply_now: Decimal, daily_velocity: Decimal,
                              buffer_m2: Decimal,
                              events: Sequence[tuple[date, Decimal]],
                              today: date, horizon_end: date) -> ProjectionResult:
    """No calculation may treat future supply as available today (§6.7).

    projected_stock(t) = supply_now − velocity·t + Σ events arrived on/before t.
    Minima are evaluated immediately BEFORE each arrival (events strictly
    earlier included) and at horizon_end⁻.
    """
    v = Decimal(daily_velocity)
    ev = sorted(((d, Decimal(m)) for d, m in events), key=lambda e: e[0])

    def stock_before(d: date) -> Decimal:
        t = (d - today).days
        arrived = sum((m for ed, m in ev if ed < d), Decimal(0))
        return q2(supply_now - v * t + arrived)

    def stock_on(t: int) -> Decimal:
        d = today + timedelta(days=t)
        arrived = sum((m for ed, m in ev if ed <= d), Decimal(0))
        return q2(supply_now - v * t + arrived)

    check_dates = sorted({d for d, _ in ev if today < d <= horizon_end} | {horizon_end})
    minima = tuple((d, stock_before(d)) for d in check_dates)

    if all(m >= buffer_m2 for _, m in minima):
        return ProjectionResult(minima=minima, outcome="protected")

    horizon_days = (horizon_end - today).days
    dip_start = dip_recovery = dip_reentry = None
    stockout_date = None
    in_dip = False
    had_recovery = False
    for t in range(0, horizon_days + 1):
        s = stock_on(t)
        if s <= buffer_m2 and not in_dip:
            in_dip = True
            d = today + timedelta(days=t)
            if dip_start is None:
                dip_start = d
            elif had_recovery and dip_reentry is None:
                dip_reentry = d
        elif s > buffer_m2 and in_dip:
            in_dip = False
            had_recovery = True
            if dip_recovery is None:
                dip_recovery = today + timedelta(days=t)

    if any(m <= 0 for _, m in minima):
        # exact piecewise crossing time, floored to a date (matches the
        # current-app days_to_stockout = int(stock / velocity) semantics)
        if v > 0:
            running = Decimal(supply_now)
            seg_start = Decimal(0)
            points = [((d - today).days, m) for d, m in ev if d <= horizon_end]
            points.sort()
            idx = 0
            while True:
                seg_end = Decimal(points[idx][0]) if idx < len(points) else Decimal(horizon_days)
                cross = seg_start + running / v
                if cross <= seg_end or idx >= len(points):
                    stockout_date = today + timedelta(days=int(cross))
                    break
                running = running - v * (seg_end - seg_start) + points[idx][1]
                seg_start = seg_end
                idx += 1
        return ProjectionResult(minima=minima, outcome="projected_stockout",
                                dip_start=dip_start, dip_recovery=dip_recovery,
                                dip_reentry=dip_reentry, stockout_date=stockout_date)

    return ProjectionResult(minima=minima, outcome="dip_below_buffer",
                            dip_start=dip_start, dip_recovery=dip_recovery,
                            dip_reentry=dip_reentry)


# ── §6.8 explanation contract ───────────────────────────────────────────────

def build_explanation(*, suggested_m2: Decimal, velocity: Decimal,
                      days_covered: Decimal, buffer_m2: Decimal,
                      next_replenishment_date: date, residual_need_m2: Decimal,
                      pending_commitment_m2: Decimal,
                      inputs: list[dict], steps: list[dict]) -> dict:
    """One calm sentence + a structured, provenance-complete trace (§6.8)."""
    sentence = (
        f"Sugerimos {q2(suggested_m2)} m² porque a la rotación actual "
        f"({q2(velocity)} m²/día) el producto cubre {days_covered} días; "
        f"para llegar con {q2(buffer_m2)} m² de reserva hasta la próxima "
        f"reposición ({next_replenishment_date.isoformat()}) faltan "
        f"{q2(residual_need_m2)} m², redondeado al siguiente medio pallet."
    )
    if pending_commitment_m2 and pending_commitment_m2 > 0:
        sentence += (
            f" {q2(pending_commitment_m2)} m² ya están en el pedido enviado, "
            f"en espera de respuesta de fábrica; sugerimos solo el resto."
        )
    return {"sentence": sentence, "trace": {"inputs": list(inputs), "steps": list(steps)}}
