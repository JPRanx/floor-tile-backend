"""
Clean V1 §6.4a — deterministic, idempotent, bidirectional evidence matching,
mutually exclusive supply buckets, and warehouse arrival attribution.

The pass is a PURE FUNCTION of (current trusted evidence set, current open
expectations). Every trigger (new evidence, expectation created/corrected/
cancelled, snapshot replaced) recomputes the affected allocation by calling
`match` again with current inputs — allocations are never incrementally
patched. Conservation invariant: for every observation,
Σ allocations + unmatched remainder + held = observed quantity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, Sequence

ZERO = Decimal("0")
OPEN_STATES = ("confirmed", "produced", "in_transit")


@dataclass(frozen=True)
class Observation:
    obs_id: str
    feed: str                      # in_transit | produced | warehouse
    product_id: Optional[str]      # None when below match confidence
    raw_reference: str
    m2: Decimal
    observed_at: date              # file/observation as-of
    event_date: date               # the evidence event date (temporal key)
    reference: Optional[str]       # explicit order/line/production reference
    eta: Optional[date]            # boat ETA when known (transit)
    confident_match: bool
    snapshot_id: str               # cumulative-snapshot identity (replacement)


@dataclass(frozen=True)
class ExpectationView:
    """Matching-relevant projection of an ExpectedIncoming."""
    exp_id: str
    product_id: str
    ask_date: date                 # order submission / amendment creation date
    effective_m2: Decimal
    received_m2: Decimal
    state: str
    closed: bool
    source_refs: frozenset

    @property
    def remaining_m2(self) -> Decimal:
        return max(ZERO, self.effective_m2 - self.received_m2)

    @property
    def open(self) -> bool:
        return (not self.closed) and self.state in OPEN_STATES


@dataclass(frozen=True)
class Allocation:
    obs_id: str
    exp_id: str
    m2: Decimal


@dataclass(frozen=True)
class Held:
    obs_id: str
    reason: str                    # low_confidence | impossible_reference
    m2: Decimal


@dataclass(frozen=True)
class MatchResult:
    allocations: tuple
    unmatched: dict                # obs_id → remaining m2 (0-remainders omitted)
    held: tuple


def _obs_sort_key(o: Observation):
    return (o.observed_at, o.reference or "", o.obs_id)


def match(observations: Sequence[Observation],
          expectations: Sequence[ExpectationView]) -> MatchResult:
    """One §6.4a pass. Deterministic total ordering → unique allocation."""
    allocations: list[Allocation] = []
    unmatched: dict[str, Decimal] = {}
    held: list[Held] = []

    remaining = {e.exp_id: e.remaining_m2 for e in expectations if e.open}
    by_id = {e.exp_id: e for e in expectations}

    for o in sorted(observations, key=_obs_sort_key):
        if not o.confident_match or o.product_id is None:
            held.append(Held(o.obs_id, "low_confidence", o.m2))
            continue

        candidates: list[ExpectationView]
        if o.reference:
            ref_hits = [e for e in expectations if o.reference in e.source_refs]
            product_hits = [e for e in ref_hits
                            if e.product_id == o.product_id]
            if product_hits and all(e.open for e in product_hits):
                # A booking/order reference may legitimately cover several
                # products.  The explicit reference narrows to expectations
                # for this row's confidently matched product; sibling product
                # lines under the same reference are not contradictions.
                candidates = product_hits
            elif ref_hits:
                # The reference is known but has no expectation for this
                # product, or its same-product expectation is closed.
                held.append(Held(o.obs_id, "impossible_reference", o.m2))
                continue
            else:
                candidates = _temporal_candidates(o, expectations)
        else:
            candidates = _temporal_candidates(o, expectations)

        # deterministic candidate ordering: FIFO ask date, then smaller
        # remaining capacity, then id (total order → unique allocation)
        candidates = sorted(
            (e for e in candidates if remaining.get(e.exp_id, ZERO) > 0),
            key=lambda e: (e.ask_date, remaining[e.exp_id], e.exp_id),
        )

        left = o.m2
        for e in candidates:
            if left <= 0:
                break
            take = min(left, remaining[e.exp_id])
            if take > 0:
                allocations.append(Allocation(o.obs_id, e.exp_id, take))
                remaining[e.exp_id] -= take
                left -= take
        if left > 0:
            unmatched[o.obs_id] = left

    return MatchResult(tuple(allocations), unmatched, tuple(held))


def _temporal_candidates(o: Observation, expectations) -> list[ExpectationView]:
    """Key 2: same confidently-matched product AND event on/after the ask
    date (evidence dated before the ask cannot be that commitment's fruit)."""
    return [
        e for e in expectations
        if e.open and e.product_id == o.product_id and o.event_date >= e.ask_date
    ]


# ── mutually exclusive supply buckets (§6.4a) ───────────────────────────────

@dataclass(frozen=True)
class Buckets:
    supply_now: Decimal          # latest trusted warehouse snapshot
    supply_expected: Decimal     # Σ remaining_m2 of OPEN expectations
    supply_moving: Decimal       # unmatched verified transit ONLY
    held_m2: Decimal             # identity-ambiguous quantity — in NO bucket


def compute_buckets(*, warehouse_m2: Decimal, result: MatchResult,
                    observations: Sequence[Observation],
                    expectations: Sequence[ExpectationView]) -> Buckets:
    transit_ids = {o.obs_id for o in observations if o.feed == "in_transit"}
    moving = sum((m for oid, m in result.unmatched.items() if oid in transit_ids), ZERO)
    expected = sum((e.remaining_m2 for e in expectations if e.open), ZERO)
    held = sum((h.m2 for h in result.held), ZERO)
    return Buckets(Decimal(warehouse_m2), expected, moving, held)


# ── arrival attribution (§6.4a, incl. partial arrivals) ────────────────────

@dataclass(frozen=True)
class ArrivalAttribution:
    attributions: list           # [(exp_id, m2)] in candidate order
    remaining_after: dict        # exp_id → remaining_m2 after attribution
    closed: list                 # exp_ids whose remaining reached 0 → arrived
    unattributed: Decimal        # stock increase with no open expectation


def attribute_arrival(*, stock_increase_m2: Decimal,
                      open_expectations: Sequence[ExpectationView]) -> ArrivalAttribution:
    """Attribute a product-level warehouse increase to open expectations in
    §6.4a candidate order. Each expectation absorbs at most remaining_m2;
    a partial receipt leaves the remainder in supply_expected exactly once."""
    ordered = sorted((e for e in open_expectations if e.open),
                     key=lambda e: (e.ask_date, e.remaining_m2, e.exp_id))
    left = Decimal(stock_increase_m2)
    attributions: list[tuple[str, Decimal]] = []
    remaining_after: dict[str, Decimal] = {}
    closed: list[str] = []
    for e in ordered:
        take = min(left, e.remaining_m2)
        if take > 0:
            attributions.append((e.exp_id, take))
            left -= take
        after = e.remaining_m2 - take
        remaining_after[e.exp_id] = after
        if after == 0 and take >= 0 and e.remaining_m2 > 0 and take == e.remaining_m2:
            closed.append(e.exp_id)
    return ArrivalAttribution(attributions, remaining_after, closed, left)
