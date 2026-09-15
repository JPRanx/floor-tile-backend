"""
Reconciled V1 — sailing domain: append-only events, deterministic fold,
effective-state projection, and the §4 state machines.

Reuses the preserved event infrastructure from `clean_v1.domain`
[PRESERVED-3.1 §4.6b + AC1 hardening]: `Event`, `freeze`/`FrozenDict`,
`_ser`/`_de` serialization, and the emit-then-append discipline. Every
operation validates against CURRENT effective state before anything is
recorded, so every recorded stream folds to a legal state by induction.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from .domain import (
    DomainError, Event, freeze, _ser, _de,
)
from .planning_math import HALF_PALLET_M2, q2
from . import sailing_math as sm
from . import sailing_normalize as sn

ZERO = Decimal("0")

EXP_ORDER = ["booked", "departed", "in_transit", "arrived"]
IMPLICATION_FAMILIES = (
    "product_match", "decision_required", "reconciliation_exception",
    "reallocation_intent", "overdue_response", "missing_evidence",
)


# ── entities (effective-state projection) ──────────────────────────────────

@dataclass
class Sailing:
    sailing_id: str
    carrier: str
    name: str
    departure: Optional[date]
    voyage_days: Optional[int]
    entered_via: str
    voyage: Optional[str] = None
    loading_terminal_eta: Optional[date] = None
    bl_vgm_close: Optional[datetime] = None
    saes_reception: Optional[datetime] = None
    terminal: Optional[str] = None
    planning_basis: str = "departure"


@dataclass
class ShipmentPlan:
    plan_id: str
    sailing_id: str
    lifecycle: str = "draft"        # draft | finalized | closed
    opened_at: Optional[str] = None
    finalized_at: Optional[str] = None
    closed_at: Optional[str] = None
    close_reason: Optional[str] = None
    late_state: Optional[str] = None
    handoff_id: Optional[str] = None
    lines: dict = field(default_factory=dict)   # product_id → line dict


@dataclass
class ProductionOrder:
    production_order_id: str
    source_plan_id: str
    cycle_as_of: date
    factory_order_date: date
    required_by: Optional[date]
    calculation_head_seq: int
    candidate_fingerprint: str
    planning_provenance: dict
    lifecycle: str = "draft"
    lines: dict = field(default_factory=dict)
    handoff: Optional[dict] = None
    production_ref: Optional[str] = None
    opened_at: Optional[str] = None
    finalized_at: Optional[str] = None
    reference_recorded_at: Optional[str] = None


@dataclass
class OrderAmendment:
    amendment_id: str
    order_kind: str
    order_id: str
    original_refs: list
    lifecycle: str = "draft"
    lines: dict = field(default_factory=dict)
    handoff: Optional[dict] = None
    amendment_ref: Optional[str] = None
    opened_at: Optional[str] = None
    finalized_at: Optional[str] = None
    reference_recorded_at: Optional[str] = None


@dataclass
class SiesaCommitment:
    commitment_id: str
    siesa_ref: str
    handoff_order_ref: Optional[str]
    quantities: list                 # [{product_id, m2}]
    reference_recorded_at: str
    state: str = "pending_observation"   # → observed | released
    observed_at: Optional[str] = None
    observed_snapshot_id: Optional[str] = None
    release_note: Optional[str] = None
    # causal baseline [AB1]: the SIESA availability snapshot current at the
    # moment the reference was recorded — committed quantity already shown
    # by (or before) this snapshot is never new proof of this commitment
    baseline_snapshot_id: Optional[str] = None


@dataclass
class PendingHandoffExclusion:
    exclusion_id: str
    plan_ref: str
    product_id: str
    m2: Decimal                      # remaining protected quantity [AB2]
    resolution_ref: str
    # exact handoff order/commitment quantity this exclusion protects [AB2]
    order_ref: Optional[str] = None
    active: bool = True
    cleared_reason: Optional[str] = None


@dataclass
class Booking:
    booking_id: str
    booking_ref: str
    sailing_id: str
    commitment_refs: list


@dataclass
class Expectation:
    exp_id: str
    product_id: str
    commitment_ref: str
    booking_ref: str
    confirmed_m2: Decimal            # immutable historical commitment
    effective_m2: Decimal            # supersession updates this, with history
    received_m2: Decimal
    state: str                       # booked | departed | in_transit | arrived | cancelled
    ask_date: date
    sailing_id: str
    overdue: bool = False
    closed_at: Optional[str] = None
    cancel_note: Optional[str] = None
    history: list = field(default_factory=list)

    @property
    def remaining_m2(self) -> Decimal:
        return q2(max(ZERO, self.effective_m2 - self.received_m2))

    @property
    def open(self) -> bool:
        return self.state in ("booked", "departed", "in_transit")


@dataclass
class Implication:
    implication_id: str
    family: str
    severity: str                    # blocking | consequential | informational
    subject_key: str
    scope: dict
    evidence: list
    consequence: str
    recommendation: str
    shipment_effect: dict
    typed_actions: list
    state: str = "open"              # open | resolved | superseded
    resolution: Optional[dict] = None
    opened_at: Optional[str] = None
    resolved_at: Optional[str] = None


@dataclass
class SState:
    sailings: dict = field(default_factory=dict)
    sailing_as_of: dict = field(default_factory=dict)       # sid → latest trusted observation
    sailing_decisions: dict = field(default_factory=dict)   # sid → latest
    # [FCB2] sid → ACTIVE pursuit authority object:
    # {"generation", "recorded_on", "departure", "days_to_departure"}.
    # Membership (`sid in s.pursuits`) is unchanged; the value binds risk
    # acceptance to the pursuit generation that was actually judged.
    pursuits: dict = field(default_factory=dict)
    pursuit_seq: dict = field(default_factory=dict)  # sid → last generation
    plans: dict = field(default_factory=dict)
    production_orders: dict = field(default_factory=dict)
    order_amendments: dict = field(default_factory=dict)
    suggestions: dict = field(default_factory=dict)         # frozen snapshots
    bl_splits: dict = field(default_factory=dict)           # plan_id → current
    bl_history: dict = field(default_factory=dict)          # plan_id → [prior]
    handoffs: dict = field(default_factory=dict)
    commitments: dict = field(default_factory=dict)
    exclusions: dict = field(default_factory=dict)
    bookings: dict = field(default_factory=dict)
    expectations: dict = field(default_factory=dict)
    monthly_context: dict = field(default_factory=dict)     # mid → entry dict
    implications: dict = field(default_factory=dict)
    recomputes: list = field(default_factory=list)
    feeds: dict = field(default_factory=dict)               # feed → latest
    feed_history: dict = field(default_factory=dict)        # feed → [prior]
    mappings: dict = field(default_factory=dict)            # raw_ref → pid

    events: list = field(default_factory=list)
    _counter: int = 0

    @staticmethod
    def empty() -> "SState":
        return SState()

    def new_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    def bump_counter_to(self, entity_id: str):
        digits = "".join(ch for ch in str(entity_id) if ch.isdigit())
        if digits:
            self._counter = max(self._counter, int(digits))


def snapshot(s: SState) -> dict:
    """Comparable deep view of effective state + events (fold-equality)."""
    return _ser({
        "sailings": {k: asdict(v) for k, v in s.sailings.items()},
        "sailing_as_of": dict(s.sailing_as_of),
        "decisions": dict(s.sailing_decisions),
        "pursuits": {k: dict(v) for k, v in s.pursuits.items()},
        "pursuit_seq": dict(s.pursuit_seq),
        "plans": {k: asdict(v) for k, v in s.plans.items()},
        "production_orders": {k: asdict(v) for k, v in s.production_orders.items()},
        "order_amendments": {k: asdict(v) for k, v in s.order_amendments.items()},
        "suggestions": dict(s.suggestions),
        "bl_splits": dict(s.bl_splits),
        "bl_history": dict(s.bl_history),
        "handoffs": dict(s.handoffs),
        "commitments": {k: asdict(v) for k, v in s.commitments.items()},
        "exclusions": {k: asdict(v) for k, v in s.exclusions.items()},
        "bookings": {k: asdict(v) for k, v in s.bookings.items()},
        "expectations": {k: asdict(v) for k, v in s.expectations.items()},
        "monthly_context": dict(s.monthly_context),
        "implications": {k: asdict(v) for k, v in s.implications.items()},
        "recomputes": list(s.recomputes),
        "feeds": dict(s.feeds),
        "feed_history": dict(s.feed_history),
        "mappings": dict(s.mappings),

        "events": [(e.seq, e.type, e.at, e.actor, e.payload, e.note)
                   for e in s.events],
    })


def _emit(s: SState, etype: str, actor: str, on: date, payload: dict,
          note: Optional[str] = None):
    ev = Event(seq=len(s.events) + 1, type=etype, at=on.isoformat(),
               actor=actor, payload=freeze(_ser(payload)), note=note)
    _APPLY[etype](s, _de(ev.payload))
    s.events.append(ev)
    return ev


def fold(events: list) -> SState:
    s = SState.empty()
    for ev in events:
        payload = freeze(_ser(ev.payload))
        _APPLY[ev.type](s, _de(payload))
        s.events.append(Event(seq=ev.seq, type=ev.type, at=ev.at,
                              actor=ev.actor, payload=payload, note=ev.note))
    return s


def _half_pallet_valid(m2: Decimal) -> bool:
    return m2 > 0 and (Decimal(m2) % HALF_PALLET_M2) == 0


def _half_pallet_nonnegative(m2: Decimal) -> bool:
    return m2 >= 0 and (Decimal(m2) % HALF_PALLET_M2) == 0


# ── D3 advancement authority (single source for wrapper AND domain) ────────

def canonical_pursuit_case(s: "SState", sailing_id: str, imp) -> bool:
    """[FCB2A] The ONE canonical-pursuit-case law shared by every
    authority consumer (`pursuit_risk_accepted` here and the engine's
    `_ensure_pursuit_case`). A case counts as the ACTIVE pursuit's
    governing case only when ALL identity and governing-evidence fields
    match the active authority object: the canonical pursuit family
    (`decision_required`), the exact subject
    `pursuit:{sailing_id}`, an Ashley-owned scope on THIS sailing
    carrying the ACTIVE generation, governing evidence equal to the
    authority object's generation, departure, days-to-departure and
    (implied by those two) the recorded day, and the canonical pursuit
    typed-action contract carrying the real `accept_risk` path. A forged
    or unrelated case that copies only the subject and/or generation —
    whatever its family, scope, evidence or caller-supplied typed
    actions — is NOT the governing case: it can never impersonate
    pursuit-risk authority and never suppresses materialization of the
    canonical current-evidence case. The predicate verifies the case
    against the authority object itself — never naming convention,
    insertion order, or caller discipline."""
    active = s.pursuits.get(sailing_id)
    if active is None:
        return False
    if imp.family != "decision_required":
        return False
    if imp.subject_key != f"pursuit:{sailing_id}":
        return False
    scope = imp.scope or {}
    if scope.get("owner") != "ashley":
        return False
    if scope.get("sailing_id") != sailing_id:
        return False
    if scope.get("pursuit_generation") != active["generation"]:
        return False
    ev = imp.evidence or []
    e0 = ev[0] if ev else {}
    if e0.get("pursuit_generation") != active["generation"]:
        return False
    if e0.get("departure") != active["departure"]:
        return False
    if e0.get("days_to_departure") != active["days_to_departure"]:
        return False
    # the recorded day is governing evidence: the case's departure and
    # days-to-departure must imply the exact day the active pursuit was
    # recorded (malformed timing fields fail closed)
    try:
        implied = date.fromisoformat(str(e0.get("departure"))) \
            - timedelta(days=int(e0.get("days_to_departure")))
    except (TypeError, ValueError):
        return False
    if implied.isoformat() != active["recorded_on"]:
        return False
    acts = set(imp.typed_actions or ())
    if not {"accept_risk", "cancel_pursuit"} <= acts:
        return False
    return True


def pursuit_risk_accepted(s: "SState", sailing_id: str) -> bool:
    """[FC4/FCB2/FCB2A] The authoritative acceptance is the LATEST
    CANONICAL pursuit case for this sailing — one that matches the active
    authority object under the shared canonical-pursuit-case law —
    resolved with the explicit risk acceptance. An open, superseded,
    cancelled, older, superseded-generation, forged or unrelated case
    never authorizes."""
    if s.pursuits.get(sailing_id) is None:
        return False
    latest = None
    for imp in s.implications.values():
        if canonical_pursuit_case(s, sailing_id, imp):
            latest = imp
    return (latest is not None and latest.state == "resolved"
            and (latest.resolution or {}).get("action") == "accept_risk")


def advancement_denial(s: "SState", sailing_id: str, on: date):
    """[D3/RC4/FC4] Why a plan on this sailing may not open, materially
    advance, or finalize today — or None when authorized. The SAME law
    backs the engine command wrappers and the domain mutations, so the two
    boundaries can never contradict [FC5]."""
    sl = s.sailings.get(sailing_id)
    if sl is None:
        return f"unknown sailing {sailing_id}"
    state = sm.schedule_timing_state(
        departure=sl.departure, bl_vgm_close=sl.bl_vgm_close,
        loading_terminal_eta=sl.loading_terminal_eta, today=on)
    if state in ("departed", "closed", "arrived"):
        if state != "departed":
            return ("the B/L-VGM planning cutoff has closed — this roster "
                    "entry cannot open, advance, or finalize a plan")
        return ("the sailing has departed — a plan cannot open, advance, "
                "or finalize anymore [D3/RC4]")
    if state == "exceptional":
        if sailing_id not in s.pursuits:
            return ("the sailing is inside 10 days of departure "
                    "(exceptional): plan work requires the explicit "
                    "exceptional-pursuit path (PursueExceptionalSailing) "
                    "plus its accepted risk decision — a previously "
                    "recorded 'use' decision is not sufficient [D3/RC4]")
        if not pursuit_risk_accepted(s, sailing_id):
            return ("the exceptional pursuit is recorded but its risk "
                    "decision is NOT accepted — recording intent to pursue "
                    "is not acceptance of its consequence, and an "
                    "acceptance bound to a superseded pursuit generation "
                    "does not carry forward; resolve the ACTIVE pursuit "
                    "generation's risk case with the explicit risk "
                    "acceptance first [D3/FC4/FCB2]")
    return None


def _require_advancement_authority(s: "SState", plan, on: date):
    reason = advancement_denial(s, plan.sailing_id, on)
    if reason:
        raise DomainError(reason)


# ═══════════════════════ operations (validate → emit) ══════════════════════

# ── evidence feeds ─────────────────────────────────────────────────────────

def load_evidence(s: SState, *, feed: str, as_of: date, rows: list,
                  entered_via: str, actor: str, on: date,
                  raw_source_ref: Optional[str] = None) -> str:
    snap_id = s.new_id("SNAP")
    _emit(s, "evidence_loaded", actor, on, {
        "feed": feed, "as_of": as_of, "rows": rows,
        "entered_via": entered_via, "snapshot_id": snap_id,
        "raw_source_ref": raw_source_ref, "actor": actor})
    return snap_id


def record_mapping(s: SState, *, raw_ref: str, product_id: Optional[str],
                   actor: str, on: date):
    # [FCB1] collision law at the RECORDING boundary: the proposed mapping
    # is evaluated against the EFFECTIVE normalized current snapshot under
    # all existing mappings PLUS the proposed one. More than one confident
    # row for a product in a non-additive unique feed has no valid
    # normalization law — the mapping is denied atomically (zero mutation,
    # the implication stays open) whether the colliding row was originally
    # confident or became confident through an earlier mapping.
    if product_id is not None:
        for feed in sn.NON_ADDITIVE_UNIQUE_FEEDS:
            f = s.feeds.get(feed)
            if not f:
                continue
            if sn.mapping_would_collide(feed, f["rows"], s.mappings,
                                        raw_ref=str(raw_ref),
                                        product_id=product_id):
                raise DomainError(
                    f"mapping {raw_ref!r} to {product_id} would create "
                    f"duplicate confident {feed} rows for one product in "
                    "the effective normalized snapshot — the "
                    f"{feed} feed is not additive and no valid "
                    "normalization law exists; discard the row or reload "
                    "a corrected feed (atomic denial) [FC1/FCB1]")
    _emit(s, "mapping_recorded", actor, on,
          {"raw_ref": raw_ref, "product_id": product_id})


# ── sailings ───────────────────────────────────────────────────────────────

def _find_sailing(s: SState, carrier: str, departure: Optional[date], *,
                  name: str, voyage: Optional[str],
                  bl_vgm_close: Optional[datetime]) -> Optional[str]:
    for sid, sl in s.sailings.items():
        if (sl.carrier == carrier and sl.departure == departure
                and (departure is not None
                     or (sl.name == name and sl.voyage == voyage
                         and sl.bl_vgm_close == bl_vgm_close))):
            return sid
    return None


def record_sailing(s: SState, *, carrier: str, name: str,
                   departure: Optional[date],
                   voyage_days: Optional[int], entered_via: str,
                   actor: str, on: date, as_of: Optional[date] = None,
                   raw_source_ref: Optional[str] = None,
                   voyage: Optional[str] = None, loading_terminal_eta: Optional[date] = None,
                   bl_vgm_close: Optional[datetime] = None,
                   saes_reception: Optional[datetime] = None,
                   terminal: Optional[str] = None,
                   planning_basis: str = "departure") -> str:
    existing = _find_sailing(s, carrier, departure, name=name, voyage=voyage,
                             bl_vgm_close=bl_vgm_close)
    sid = existing or s.new_id("SAIL")
    _emit(s, "sailing_recorded", actor, on, {
        "sailing_id": sid, "carrier": carrier, "name": name,
        "departure": departure, "voyage_days": voyage_days,
        "voyage": voyage, "loading_terminal_eta": loading_terminal_eta, "bl_vgm_close": bl_vgm_close,
        "saes_reception": saes_reception, "terminal": terminal,
        "planning_basis": planning_basis,
        "entered_via": entered_via, "as_of": as_of or on,
        "raw_source_ref": raw_source_ref})
    return sid


def record_sailing_decision(s: SState, *, sailing_id: str, decision: str,
                            actor: str, on: date):
    if sailing_id not in s.sailings:
        raise DomainError(f"unknown sailing {sailing_id}")
    if decision not in ("use", "watch", "skip"):
        raise DomainError(f"invalid sailing decision {decision!r}")
    sl = s.sailings[sailing_id]
    state = sm.schedule_timing_state(
        departure=sl.departure, bl_vgm_close=sl.bl_vgm_close,
        loading_terminal_eta=sl.loading_terminal_eta, today=on)
    if decision == "use" and state in ("departed", "closed", "arrived"):
        raise DomainError("sailing planning window is already closed")
    if decision == "use" and state == "exceptional" \
            and sailing_id not in s.pursuits:
        raise DomainError(
            "sailing is inside 10 days of departure (exceptional): it is "
            "excluded from normal use — explicit exceptional pursuit is "
            "required [D3]")
    _emit(s, "sailing_decision_recorded", actor, on,
          {"sailing_id": sailing_id, "decision": decision})


def record_exceptional_pursuit(s: SState, *, sailing_id: str, actor: str,
                               on: date) -> dict:
    """[FCB2] Every pursuit event carries an authority GENERATION bound to
    its governing evidence (the recorded day and the departure it judged).
    An explicit same-event retry — identical sailing, identical recorded
    day, identical departure — is idempotent: the SAME generation, zero
    new state. Anything materially fresh (a later day, a changed
    departure, a re-pursuit after cancellation) creates a NEW generation,
    and prior risk acceptances die with the generation they judged: no
    caller, command or direct-domain, can manufacture a new pursuit while
    silently inheriting stale acceptance."""
    if sailing_id not in s.sailings:
        raise DomainError(f"unknown sailing {sailing_id}")
    sl = s.sailings[sailing_id]
    state = sm.schedule_timing_state(
        departure=sl.departure, bl_vgm_close=sl.bl_vgm_close,
        loading_terminal_eta=sl.loading_terminal_eta, today=on)
    if state != "exceptional":
        raise DomainError(
            f"exceptional pursuit applies only inside the 10-day window "
            f"(sailing is {state})")
    evidence = {"recorded_on": on.isoformat(),
                "departure": sl.departure.isoformat(),
                "days_to_departure": (sl.departure - on).days}
    active = s.pursuits.get(sailing_id)
    if active is not None and all(active.get(k) == v
                                  for k, v in evidence.items()):
        return {"generation": active["generation"], "created": False}
    gen = s.pursuit_seq.get(sailing_id, 0) + 1
    _emit(s, "exceptional_pursuit_recorded", actor, on,
          {"sailing_id": sailing_id, "generation": gen, **evidence})
    return {"generation": gen, "created": True}


def cancel_exceptional_pursuit(s: SState, *, sailing_id: str, actor: str,
                               on: date):
    """[AB6] `cancel_pursuit` actually cancels: the pursuit leaves the
    active set, so `use` is again denied without a fresh explicit act."""
    if sailing_id not in s.pursuits:
        raise DomainError(f"no active exceptional pursuit for {sailing_id}")
    _emit(s, "exceptional_pursuit_cancelled", actor, on,
          {"sailing_id": sailing_id})


# ── plans ──────────────────────────────────────────────────────────────────

def _open_plan_for(s: SState, sailing_id: str) -> Optional[str]:
    for pid, p in s.plans.items():
        if p.sailing_id == sailing_id and p.lifecycle in ("draft", "finalized"):
            return pid
    return None


def open_plan(s: SState, *, sailing_id: str, actor: str, on: date) -> str:
    if sailing_id not in s.sailings:
        raise DomainError(f"unknown sailing {sailing_id}")
    if _open_plan_for(s, sailing_id):
        raise DomainError(f"an open plan already exists for {sailing_id}")
    reason = advancement_denial(s, sailing_id, on)   # [FC5]
    if reason:
        raise DomainError(reason)
    plan_id = s.new_id("PLAN")
    _emit(s, "plan_opened", actor, on,
          {"plan_id": plan_id, "sailing_id": sailing_id,
           "opened_at": on.isoformat()})
    return plan_id


def record_suggestion_snapshot(s: SState, *, plan_id: str, product_id: str,
                               content: dict, actor: str, on: date) -> str:
    if plan_id not in s.plans:
        raise DomainError(f"unknown plan {plan_id}")
    ref = s.new_id("SSNAP")
    _emit(s, "suggestion_snapshotted", actor, on,
          {"snapshot_id": ref, "plan_id": plan_id, "product_id": product_id,
           "content": content})
    return ref


def record_plan_decision(s: SState, *, plan_id: str, product_id: str,
                         action: str, selected_m2: Optional[Decimal],
                         origin: Optional[str], snapshot_ref: Optional[str],
                         actor: str, on: date):
    plan = s.plans.get(plan_id)
    if plan is None:
        raise DomainError(f"unknown plan {plan_id}")
    if plan.lifecycle != "draft":
        raise DomainError(
            f"plan is {plan.lifecycle}: direct edits are rejected on a "
            "finalized plan — reallocation intent confirmation is the only "
            "path (§7.3)")
    if action not in ("accept", "edit", "manual", "postpone"):
        raise DomainError(f"invalid plan action {action!r}")
    # [FC5] the DOMAIN boundary enforces D3 timing/pursuit/risk authority
    # for material advancement — postpone stays the accepted pure
    # de-advancement; wrapper and domain share the same law and can never
    # contradict for the same business action
    if action != "postpone":
        _require_advancement_authority(s, plan, on)
    if action != "postpone":
        if selected_m2 is None or not _half_pallet_valid(Decimal(selected_m2)):
            raise DomainError(
                f"selected m² must be a positive multiple of 67.2 "
                f"[SETTLED-JORGE D1]; got {selected_m2}")
        if origin in ("suggestion", "edited") and (
                snapshot_ref not in s.suggestions):
            raise DomainError(
                "suggestion-based decisions require a server-owned frozen "
                "snapshot reference")
    _emit(s, "plan_decision_recorded", actor, on, {
        "plan_id": plan_id, "product_id": product_id, "action": action,
        "selected_m2": selected_m2, "origin": origin,
        "snapshot_ref": snapshot_ref})


def set_bl_split(s: SState, *, plan_id: str, groups: list, origin: str,
                 actor: str, on: date):
    plan = s.plans.get(plan_id)
    if plan is None:
        raise DomainError(f"unknown plan {plan_id}")
    # [RC2] the DOMAIN mutation itself enforces the finalized invariant —
    # no path (command or direct) may change a BL split off a draft plan;
    # after finalization the handoff is the order of record (§7.3 reopen)
    if plan.lifecycle != "draft":
        raise DomainError(
            f"plan is {plan.lifecycle}: BL splits mutate only on a draft "
            "plan — reallocation-intent reopen is the only path after "
            "finalization [AB4/RC2]")
    _require_advancement_authority(s, plan, on)      # [FC5]
    if origin not in ("default", "override"):
        raise DomainError(f"invalid split origin {origin!r}")
    _emit(s, "bl_split_set", actor, on,
          {"plan_id": plan_id, "groups": groups, "origin": origin})


def finalize_plan(s: SState, *, plan_id: str, handoff: dict, uncovered: list,
                  late_state: str, actor: str, on: date) -> str:
    plan = s.plans.get(plan_id)
    if plan is None:
        raise DomainError(f"unknown plan {plan_id}")
    if plan.lifecycle != "draft":
        raise DomainError(f"only draft plans finalize (plan is {plan.lifecycle})")
    if not plan.lines:
        raise DomainError("a plan needs at least one line to finalize (RV2)")
    _require_advancement_authority(s, plan, on)      # [FC5]
    handoff_id = s.new_id("HOF")
    prior = plan.handoff_id
    # [RC2] order identity carries the handoff GENERATION: a successor
    # handoff after a reallocation reopen can never reuse the superseded
    # generation's order references, so an old commitment cannot silently
    # attach or book against a materially changed successor. There is no
    # silent carry-forward: prior commitments stay bound to their own
    # generation (release + re-reference is the explicit path — U-R8).
    handoff = dict(handoff)
    handoff["orders"] = [
        dict(o, order_ref=f"{handoff_id}-O{o.get('order_no', i + 1)}")
        for i, o in enumerate(handoff.get("orders", []))]
    _emit(s, "plan_finalized", actor, on, {
        "plan_id": plan_id, "handoff_id": handoff_id, "handoff": handoff,
        "uncovered": uncovered, "late_state": late_state,
        "finalized_at": on.isoformat(), "supersedes": prior})
    return handoff_id


def confirm_reallocation(s: SState, *, plan_id: str, actor: str, on: date):
    plan = s.plans.get(plan_id)
    if plan is None:
        raise DomainError(f"unknown plan {plan_id}")
    if plan.lifecycle != "finalized":
        raise DomainError("reallocation intent applies to finalized plans only")
    _emit(s, "reallocation_intent_confirmed", actor, on,
          {"plan_id": plan_id, "handoff_id": plan.handoff_id})


def open_production_order(s: SState, *, source_plan_id: str, cycle_as_of: date,
                          factory_order_date: date, required_by: Optional[date],
                          calculation_head_seq: int, candidate_fingerprint: str,
                          planning_provenance: dict, lines: list,
                          actor: str, on: date) -> str:
    if source_plan_id not in s.plans:
        raise DomainError(f"unknown plan {source_plan_id}")
    if any(o.source_plan_id == source_plan_id
           and o.lifecycle in ("draft", "finalized", "reference_recorded")
           for o in s.production_orders.values()):
        raise DomainError("an active production order already exists for this plan")
    if not lines:
        raise DomainError("no positive production recommendation exists")
    order_id = s.new_id("PROD")
    _emit(s, "production_order_opened", actor, on, {
        "production_order_id": order_id, "source_plan_id": source_plan_id,
        "cycle_as_of": cycle_as_of, "factory_order_date": factory_order_date,
        "required_by": required_by, "calculation_head_seq": calculation_head_seq,
        "candidate_fingerprint": candidate_fingerprint,
        "planning_provenance": planning_provenance,
        "lines": lines, "opened_at": on.isoformat()})
    return order_id


def edit_production_order_batch(s: SState, *, production_order_id: str,
                                edits: list, actor: str, on: date):
    order = s.production_orders.get(production_order_id)
    if order is None:
        raise DomainError(f"unknown production order {production_order_id}")
    if order.lifecycle != "draft":
        raise DomainError("only a draft production order can be edited")
    if not edits:
        raise DomainError("production edits must be non-empty")
    seen, normalized = set(), []
    for edit in edits:
        if not isinstance(edit, dict) or set(edit) != {"product_id", "m2"}:
            raise DomainError("each production edit requires product_id and m2")
        pid = edit["product_id"]
        if pid in seen:
            raise DomainError("production edit products must be unique")
        try:
            m2 = Decimal(str(edit["m2"]))
        except Exception as exc:
            raise DomainError("production m² must be decimal") from exc
        if not _half_pallet_nonnegative(m2):
            raise DomainError("production m² must be a nonnegative multiple of 67.20")
        seen.add(pid)
        normalized.append({"product_id": pid, "m2": q2(m2)})
    _emit(s, "production_order_lines_edited", actor, on, {
        "production_order_id": production_order_id, "edits": normalized})


def finalize_production_order(s: SState, *, production_order_id: str,
                              actor: str, on: date):
    order = s.production_orders.get(production_order_id)
    if order is None:
        raise DomainError(f"unknown production order {production_order_id}")
    if order.lifecycle != "draft":
        raise DomainError("only a draft production order can be finalized")
    if not order.lines:
        raise DomainError("a production order needs at least one line")
    lines = [{"product_id": pid, "selected_m2": line["selected_m2"],
              "recommendation_m2": line["recommendation_m2"],
              "origin": line["origin"],
              "calculation": line.get("calculation")}
             for pid, line in order.lines.items()]
    handoff = {"production_order_id": production_order_id,
               "source_plan_id": order.source_plan_id,
               "cycle_as_of": order.cycle_as_of,
               "factory_order_date": order.factory_order_date,
               "required_by": order.required_by, "lines": lines,
               "calculation_head_seq": order.calculation_head_seq,
               "candidate_fingerprint": order.candidate_fingerprint,
               "planning_provenance": order.planning_provenance,
               "total_m2": q2(sum((Decimal(str(x["selected_m2"]))
                                    for x in lines), ZERO))}
    _emit(s, "production_order_finalized", actor, on, {
        "production_order_id": production_order_id, "handoff": handoff,
        "finalized_at": on.isoformat()})


def record_production_order_reference(s: SState, *, production_order_id: str,
                                      production_ref: str, actor: str,
                                      on: date) -> bool:
    order = s.production_orders.get(production_order_id)
    if order is None:
        raise DomainError(f"unknown production order {production_order_id}")
    if order.production_ref == production_ref and production_ref:
        return False
    if order.production_ref is not None:
        raise DomainError("conflicting production reference")
    if order.lifecycle != "finalized":
        raise DomainError("production reference requires a finalized order")
    if not isinstance(production_ref, str) or not production_ref.strip():
        raise DomainError("production reference must be non-empty")
    _emit(s, "production_order_reference_recorded", actor, on, {
        "production_order_id": production_order_id,
        "production_ref": production_ref,
        "reference_recorded_at": on.isoformat()})
    return True


def reopen_sailing_plan(s: SState, *, plan_id: str, actor: str, on: date):
    plan = s.plans.get(plan_id)
    if plan is None:
        raise DomainError(f"unknown plan {plan_id}")
    if plan.lifecycle != "finalized" or not plan.handoff_id:
        raise DomainError("only a finalized shipment plan can be reopened")
    order_refs = {
        order.get("order_ref")
        for handoff in s.handoffs.values()
        if handoff["plan_id"] == plan_id
        for order in handoff["handoff"].get("orders", [])
    }
    if any(c.handoff_order_ref in order_refs for c in s.commitments.values()):
        raise DomainError("referenced shipment is immutable; create an amendment")
    _emit(s, "shipment_plan_reopened", actor, on, {
        "plan_id": plan_id, "handoff_id": plan.handoff_id,
        "reopened_at": on.isoformat()})


def reopen_production_order(s: SState, *, production_order_id: str,
                            actor: str, on: date):
    order = s.production_orders.get(production_order_id)
    if order is None:
        raise DomainError(f"unknown production order {production_order_id}")
    if order.lifecycle == "reference_recorded" or order.production_ref is not None:
        raise DomainError("referenced production order is immutable; create an amendment")
    if order.lifecycle != "finalized":
        raise DomainError("only a finalized production order can be reopened")
    _emit(s, "production_order_reopened", actor, on, {
        "production_order_id": production_order_id,
        "reopened_at": on.isoformat()})


def open_order_amendment(s: SState, *, order_kind: str, order_id: str,
                         baseline_lines: list, original_refs: list,
                         actor: str, on: date) -> str:
    if order_kind not in ("shipment", "production"):
        raise DomainError("order_kind must be shipment or production")
    if not original_refs:
        raise DomainError("an amendment requires a referenced original order")
    if any(a.order_kind == order_kind and a.order_id == order_id
           and a.lifecycle in ("draft", "finalized")
           for a in s.order_amendments.values()):
        raise DomainError("an active amendment already exists for this original order")
    amendment_id = s.new_id("AMD")
    _emit(s, "order_amendment_opened", actor, on, {
        "amendment_id": amendment_id, "order_kind": order_kind,
        "order_id": order_id, "original_refs": list(original_refs),
        "lines": baseline_lines, "opened_at": on.isoformat()})
    return amendment_id


def edit_order_amendment_batch(s: SState, *, amendment_id: str, edits: list,
                               actor: str, on: date):
    amendment = s.order_amendments.get(amendment_id)
    if amendment is None:
        raise DomainError(f"unknown order amendment {amendment_id}")
    if amendment.lifecycle != "draft":
        raise DomainError("only a draft amendment can be edited")
    if not edits:
        raise DomainError("amendment edits must be non-empty")
    seen, normalized = set(), []
    for edit in edits:
        if not isinstance(edit, dict) or set(edit) != {"product_id", "m2"}:
            raise DomainError("each amendment edit requires product_id and m2")
        pid = edit["product_id"]
        if pid in seen:
            raise DomainError("amendment edit products must be unique")
        try:
            m2 = Decimal(str(edit["m2"]))
        except Exception as exc:
            raise DomainError("amendment m² must be decimal") from exc
        if not _half_pallet_nonnegative(m2):
            raise DomainError("amendment m² must be a nonnegative multiple of 67.20")
        seen.add(pid)
        normalized.append({"product_id": pid, "m2": q2(m2)})
    _emit(s, "order_amendment_lines_edited", actor, on, {
        "amendment_id": amendment_id, "edits": normalized})


def finalize_order_amendment(s: SState, *, amendment_id: str,
                             actor: str, on: date):
    amendment = s.order_amendments.get(amendment_id)
    if amendment is None:
        raise DomainError(f"unknown order amendment {amendment_id}")
    if amendment.lifecycle != "draft":
        raise DomainError("only a draft amendment can be finalized")
    lines = []
    for pid, line in amendment.lines.items():
        baseline = Decimal(str(line["baseline_m2"]))
        selected = Decimal(str(line["selected_m2"]))
        lines.append({"product_id": pid, "baseline_m2": q2(baseline),
                      "selected_m2": q2(selected),
                      "delta_m2": q2(selected - baseline)})
    if not any(line["delta_m2"] != ZERO for line in lines):
        raise DomainError("change at least one product before finalizing an amendment")
    handoff = {"amendment_id": amendment_id,
               "order_kind": amendment.order_kind,
               "order_id": amendment.order_id,
               "original_refs": list(amendment.original_refs),
               "lines": lines}
    _emit(s, "order_amendment_finalized", actor, on, {
        "amendment_id": amendment_id, "handoff": handoff,
        "finalized_at": on.isoformat()})


def record_order_amendment_reference(s: SState, *, amendment_id: str,
                                     amendment_ref: str, actor: str,
                                     on: date) -> bool:
    amendment = s.order_amendments.get(amendment_id)
    if amendment is None:
        raise DomainError(f"unknown order amendment {amendment_id}")
    if amendment.amendment_ref == amendment_ref and amendment_ref:
        return False
    if amendment.amendment_ref is not None:
        raise DomainError("conflicting amendment reference")
    if amendment.lifecycle != "finalized":
        raise DomainError("amendment reference requires a finalized amendment")
    if not isinstance(amendment_ref, str) or not amendment_ref.strip():
        raise DomainError("amendment reference must be non-empty")
    _emit(s, "order_amendment_reference_recorded", actor, on, {
        "amendment_id": amendment_id, "amendment_ref": amendment_ref,
        "reference_recorded_at": on.isoformat()})
    return True


def close_plan(s: SState, *, plan_id: str, reason: str, actor: str, on: date):
    plan = s.plans.get(plan_id)
    if plan is None:
        raise DomainError(f"unknown plan {plan_id}")
    if plan.lifecycle == "closed":
        raise DomainError("plan already closed")
    if reason not in ("closed", "abandoned"):
        raise DomainError(f"invalid close reason {reason!r}")
    # [AB4] lifecycle legality: only a DRAFT may be abandoned; a finalized
    # plan closes only as "closed" (the engine additionally requires the
    # sailing to have departed before a finalized plan may close).
    if reason == "abandoned" and plan.lifecycle != "draft":
        raise DomainError(
            "only a draft plan may be abandoned — a finalized plan's "
            "handoff is the order of record; reopen through reallocation "
            "intent or close it after departure [AB4]")
    if reason == "closed" and plan.lifecycle != "finalized":
        raise DomainError(
            "a draft plan does not close as 'closed' — abandon it "
            "explicitly, or finalize it first [AB4]")
    # Open commitments/bookings/expectations NEVER block closure (§4.1).
    _emit(s, "plan_closed", actor, on,
          {"plan_id": plan_id, "reason": reason,
           "closed_at": on.isoformat()})


def record_recompute(s: SState, *, plan_id: str, trigger: str, before: dict,
                     after: dict, actor: str, on: date):
    _emit(s, "plan_recomputed", actor, on, {
        "plan_id": plan_id, "trigger": trigger,
        "before": before, "after": after, "at": on.isoformat()})


# ── commitments (§4.2) ─────────────────────────────────────────────────────

def record_siesa_reference(s: SState, *, handoff_order_ref: Optional[str],
                           siesa_ref: str, quantities: list,
                           actor: str, on: date,
                           baseline_snapshot_id: Optional[str] = None) -> str:
    for c in s.commitments.values():
        if c.handoff_order_ref == handoff_order_ref \
                and handoff_order_ref is not None \
                and c.state != "released":
            raise DomainError(
                f"handoff order {handoff_order_ref} already carries "
                f"commitment {c.commitment_id} (RV3)")
    for qrow in quantities:
        if Decimal(qrow["m2"]) <= 0:
            raise DomainError("commitment quantities must be positive")
    cid = s.new_id("COM")
    _emit(s, "siesa_reference_recorded", actor, on, {
        "commitment_id": cid, "handoff_order_ref": handoff_order_ref,
        "siesa_ref": siesa_ref, "quantities": quantities,
        "reference_recorded_at": on.isoformat(),
        "baseline_snapshot_id": baseline_snapshot_id})
    return cid


def observe_commitment(s: SState, *, commitment_id: str, snapshot_id: str,
                       actor: str, on: date):
    c = s.commitments.get(commitment_id)
    if c is None:
        raise DomainError(f"unknown commitment {commitment_id}")
    if c.state != "pending_observation":
        raise DomainError(f"commitment is {c.state}, not pending_observation")
    _emit(s, "siesa_commitment_observed", actor, on, {
        "commitment_id": commitment_id, "snapshot_id": snapshot_id,
        "observed_at": on.isoformat()})


def release_commitment(s: SState, *, commitment_id: str, note: str,
                       actor: str, on: date):
    c = s.commitments.get(commitment_id)
    if c is None:
        raise DomainError(f"unknown commitment {commitment_id}")
    if c.state == "released":
        raise DomainError("commitment already released")
    _emit(s, "siesa_commitment_released", actor, on,
          {"commitment_id": commitment_id, "note": note})


# ── exclusions ─────────────────────────────────────────────────────────────

def create_exclusion(s: SState, *, plan_ref: str, product_id: str,
                     m2: Decimal, resolution_ref: str, actor: str,
                     on: date, order_ref: Optional[str] = None) -> str:
    if Decimal(m2) <= 0:
        raise DomainError("exclusion quantity must be positive")
    xid = s.new_id("EXCL")
    _emit(s, "exclusion_created", actor, on, {
        "exclusion_id": xid, "plan_ref": plan_ref, "product_id": product_id,
        "m2": m2, "resolution_ref": resolution_ref, "order_ref": order_ref})
    return xid


def reduce_exclusion(s: SState, *, exclusion_id: str, by_m2: Decimal,
                     reason: str, actor: str, on: date):
    """[AB2] Partial decrement: observation/release clears exactly the
    supported quantity; a fully consumed exclusion auto-clears in the same
    atomic step."""
    x = s.exclusions.get(exclusion_id)
    if x is None:
        raise DomainError(f"unknown exclusion {exclusion_id}")
    if not x.active:
        raise DomainError("exclusion already cleared")
    if Decimal(by_m2) <= 0:
        raise DomainError("exclusion decrement must be positive")
    _emit(s, "exclusion_reduced", actor, on,
          {"exclusion_id": exclusion_id, "by_m2": by_m2, "reason": reason})


def clear_exclusion(s: SState, *, exclusion_id: str, reason: str,
                    actor: str, on: date):
    x = s.exclusions.get(exclusion_id)
    if x is None:
        raise DomainError(f"unknown exclusion {exclusion_id}")
    if not x.active:
        raise DomainError("exclusion already cleared")
    _emit(s, "exclusion_cleared", actor, on,
          {"exclusion_id": exclusion_id, "reason": reason})


# ── bookings / expectations (§4.3) ─────────────────────────────────────────

def record_booking(s: SState, *, booking_ref: str, sailing_id: str,
                   commitment_refs: list, actor: str, on: date) -> str:
    """[AB5/FC2] Booking association is validated BEFORE any event append,
    AT THE DOMAIN MUTATION BOUNDARY: every linked commitment must exist,
    be observed (the authoritative accepted allocation), not be released,
    and its commitment → order → handoff → plan → sailing lineage must
    resolve to an ACTIVE (non-superseded) handoff generation whose plan is
    on the supplied sailing. Generation-versioned reference strings are
    necessary but not sufficient — the mutation validates actual object
    lineage; a mixed valid/invalid list fails atomically with zero
    mutation."""
    if not commitment_refs:
        raise DomainError("a booking must reference at least one commitment")
    for cref in commitment_refs:
        c = s.commitments.get(cref)
        if c is None:
            raise DomainError(f"unknown commitment {cref}")
        if c.state == "released":
            raise DomainError(
                f"commitment {cref} is released — a released commitment "
                "cannot be booked [AB5]")
        if c.state != "observed":
            raise DomainError(
                f"commitment {cref} is {c.state} — booking requires an "
                "OBSERVED SIESA commitment [D5/AB5]")
        active = None
        for h in s.handoffs.values():
            if h["superseded"]:
                continue
            if any(o.get("order_ref") == c.handoff_order_ref
                   for o in h["handoff"].get("orders", [])):
                active = h
                break
        if active is None:
            raise DomainError(
                f"commitment {cref}'s handoff order "
                f"{c.handoff_order_ref!r} is not part of any ACTIVE "
                "(non-superseded) handoff generation — a superseded "
                "generation's commitment cannot be booked [AB5/FC2]")
        plan = s.plans.get(active["plan_id"])
        if plan is None or plan.sailing_id != sailing_id:
            raise DomainError(
                f"commitment {cref} belongs to plan "
                f"{active['plan_id']} on sailing "
                f"{plan.sailing_id if plan else '?'}, not {sailing_id} — "
                "the booking must match every linked commitment's active "
                "handoff sailing [AB5/FC2]")
    bid = s.new_id("BOOK")
    _emit(s, "booking_recorded", actor, on, {
        "booking_id": bid, "booking_ref": booking_ref,
        "sailing_id": sailing_id, "commitment_refs": commitment_refs})
    return bid


def create_expectation(s: SState, *, commitment_ref: str, booking_ref: str,
                       product_id: str, confirmed_m2: Decimal,
                       sailing_id: str, ask_date: date, actor: str,
                       on: date) -> str:
    c = s.commitments.get(commitment_ref)
    if c is None:
        raise DomainError(f"unknown commitment {commitment_ref}")
    if c.state != "observed":
        raise DomainError(
            "expected incoming requires an OBSERVED SIESA commitment plus "
            "booking evidence [D5] — the commitment is "
            f"{c.state}")
    if Decimal(confirmed_m2) <= 0:
        raise DomainError("confirmed m² must be positive")
    eid = s.new_id("EXP")
    _emit(s, "expectation_created", actor, on, {
        "exp_id": eid, "product_id": product_id,
        "commitment_ref": commitment_ref, "booking_ref": booking_ref,
        "confirmed_m2": confirmed_m2, "sailing_id": sailing_id,
        "ask_date": ask_date})
    return eid


def _open_exp(s: SState, exp_id: str) -> Expectation:
    e = s.expectations.get(exp_id)
    if e is None:
        raise DomainError(f"unknown expectation {exp_id}")
    if e.state in ("arrived", "cancelled"):
        raise DomainError(
            f"expectation is {e.state} — an immutable terminal "
            "[PRESERVED-3.1 §4.3]")
    return e


def advance_expectation(s: SState, *, exp_id: str, new_state: str,
                        evidence_ref: str, actor: str, on: date):
    e = _open_exp(s, exp_id)
    if new_state not in EXP_ORDER:
        raise DomainError(f"invalid expectation state {new_state!r}")
    if new_state == "arrived":
        raise DomainError(
            "arrived is reached only through warehouse arrival attribution")
    if EXP_ORDER.index(new_state) <= EXP_ORDER.index(e.state):
        raise DomainError(
            f"backward move {e.state} → {new_state}: states are forward-only "
            "(skips forward are legal)")
    _emit(s, "expectation_advanced", actor, on,
          {"exp_id": exp_id, "state": new_state,
           "evidence_ref": evidence_ref})


def supersede_expectation(s: SState, *, exp_id: str,
                          new_effective_m2: Decimal, note: str,
                          evidence_ref: str, actor: str, on: date):
    e = _open_exp(s, exp_id)
    if Decimal(new_effective_m2) < e.received_m2:
        raise DomainError(
            "effective quantity cannot fall below what already arrived")
    _emit(s, "expectation_superseded", actor, on, {
        "exp_id": exp_id, "new_effective_m2": new_effective_m2,
        "note": note, "evidence_ref": evidence_ref})


def attribute_arrival_to(s: SState, *, exp_id: str, m2: Decimal,
                         actor: str, on: date):
    e = _open_exp(s, exp_id)
    take = min(Decimal(m2), e.remaining_m2)
    if take <= 0:
        raise DomainError("nothing to attribute")
    _emit(s, "arrival_attributed", actor, on,
          {"exp_id": exp_id, "m2": take, "at": on.isoformat()})


def cancel_expectation(s: SState, *, exp_id: str, note: str, actor: str,
                       on: date):
    _open_exp(s, exp_id)
    _emit(s, "expectation_cancelled", actor, on,
          {"exp_id": exp_id, "note": note})


def flag_expectation_overdue(s: SState, *, exp_id: str, actor: str, on: date):
    e = _open_exp(s, exp_id)
    if not e.overdue:
        _emit(s, "expectation_overdue_flagged", actor, on, {"exp_id": exp_id})


def clear_expectation_overdue(s: SState, *, exp_id: str, actor: str, on: date):
    e = s.expectations.get(exp_id)
    if e is None:
        raise DomainError(f"unknown expectation {exp_id}")
    if e.overdue:
        _emit(s, "expectation_overdue_cleared", actor, on, {"exp_id": exp_id})


# ── implications ───────────────────────────────────────────────────────────

def open_implication(s: SState, *, family: str, severity: str,
                     subject_key: str, scope: dict, evidence: list,
                     consequence: str, recommendation: str,
                     shipment_effect: dict, typed_actions: list,
                     actor: str, on: date) -> str:
    if family not in IMPLICATION_FAMILIES:
        raise DomainError(f"unknown implication family {family!r} — the six "
                          "families are a closed list [D6]")
    if severity not in ("blocking", "consequential", "informational"):
        raise DomainError(f"invalid severity {severity!r}")
    for i in s.implications.values():
        if i.state == "open" and i.family == family \
                and i.subject_key == subject_key:
            return i.implication_id      # dedup: one open per family+subject
    iid = s.new_id("IMP")
    _emit(s, "implication_opened", actor, on, {
        "implication_id": iid, "family": family, "severity": severity,
        "subject_key": subject_key, "scope": scope, "evidence": evidence,
        "consequence": consequence, "recommendation": recommendation,
        "shipment_effect": shipment_effect, "typed_actions": typed_actions,
        "opened_at": on.isoformat()})
    return iid


# [FC3] Actions whose name promises a business effect: they may resolve a
# case ONLY through the engine's ResolveImplication command, which performs
# and validates the named effect atomically in the same transaction. The
# domain resolution primitive below is a structural internal boundary — it
# refuses these actions, so no callable domain alias can record
# `implication_resolved` without the effect.
EFFECT_BEARING_ACTIONS = frozenset({
    "reduce_to_available", "confirm_reopen", "exclude_pending", "map",
    "discard", "accept_evidence", "reaccept_current", "pursue_exceptional",
    "abandon_plan", "cancel_pursuit",
})


def resolve_implication(s: SState, *, implication_id: str, action: str,
                        params: dict, actor: str, on: date):
    """Public domain boundary [FC3]: enforces case-owner authority and
    refuses effect-bearing actions — those resolve only through the engine
    command path. Non-effect keeps/acceptances (whose named effect IS the
    recorded resolution) pass through to the internal primitive."""
    i = s.implications.get(implication_id)
    if i is None:
        raise DomainError(f"unknown implication {implication_id}")
    owner = (i.scope or {}).get("owner", "ashley")
    allowed = ("ashley",) if owner == "ashley" else ("elicio",)
    if actor not in allowed:
        raise DomainError(
            f"implication {implication_id} is owned by {owner!r}: "
            f"resolution by {actor!r} is denied — the resolution actor "
            "must match the case owner/authority [AB6]")
    if action in EFFECT_BEARING_ACTIONS:
        raise DomainError(
            f"{action!r} promises a business effect: it resolves only "
            "through the engine's ResolveImplication command, which "
            "performs the named effect atomically — the domain primitive "
            "records no effect and refuses to stand in for it [FC3]")
    return _record_resolution(s, implication_id=implication_id,
                              action=action, params=params, actor=actor,
                              on=on)


def _record_resolution(s: SState, *, implication_id: str, action: str,
                       params: dict, actor: str, on: date):
    """INTERNAL event primitive — called by the engine after it has
    authorized the resolution and while it performs any named effect in
    the same transaction. Not a business-action path [FC3]."""
    i = s.implications.get(implication_id)
    if i is None:
        raise DomainError(f"unknown implication {implication_id}")
    if i.state != "open":
        raise DomainError(f"implication is {i.state}")
    if action not in i.typed_actions:
        raise DomainError(
            f"action {action!r} is not one of this case's typed actions "
            f"{i.typed_actions} — free-form resolution is not a path [D6]")
    _emit(s, "implication_resolved", actor, on, {
        "implication_id": implication_id, "action": action, "params": params,
        "resolved_at": on.isoformat()})


def supersede_implication(s: SState, *, implication_id: str, reason: str,
                          actor: str, on: date):
    i = s.implications.get(implication_id)
    if i is None:
        raise DomainError(f"unknown implication {implication_id}")
    if i.state != "open":
        raise DomainError(f"implication is {i.state}")
    _emit(s, "implication_superseded", actor, on,
          {"implication_id": implication_id, "reason": reason})


# ═══════════════════════ appliers (pre-validated payloads) ═════════════════

def _apply_evidence_loaded(s, p):
    feed = p["feed"]
    incoming = {"as_of": p["as_of"], "rows": p["rows"],
                "entered_via": p["entered_via"],
                "snapshot_id": p["snapshot_id"],
                "raw_source_ref": p.get("raw_source_ref"),
                "actor": p.get("actor")}
    current = s.feeds.get(feed)
    if current is None or incoming["as_of"] >= current["as_of"]:
        if current is not None:
            s.feed_history.setdefault(feed, []).append(current)
        s.feeds[feed] = incoming
    else:
        s.feed_history.setdefault(feed, []).append(incoming)
    s.bump_counter_to(p["snapshot_id"])


def _apply_mapping_recorded(s, p):
    if "created_product" in p:
        raise DomainError(
            "mapping_recorded does not accept product-creation payloads")
    s.mappings[p["raw_ref"]] = p["product_id"]


def _apply_sailing_recorded(s, p):
    incoming_as_of = p.get("as_of")
    current_as_of = s.sailing_as_of.get(p["sailing_id"])
    if (incoming_as_of is not None and current_as_of is not None
            and incoming_as_of < current_as_of):
        s.bump_counter_to(p["sailing_id"])
        return
    s.sailings[p["sailing_id"]] = Sailing(
        sailing_id=p["sailing_id"], carrier=p["carrier"], name=p["name"],
        departure=p["departure"], voyage_days=p["voyage_days"],
        entered_via=p["entered_via"], voyage=p.get("voyage"), loading_terminal_eta=p.get("loading_terminal_eta"),
        bl_vgm_close=p.get("bl_vgm_close"),
        saes_reception=p.get("saes_reception"), terminal=p.get("terminal"),
        planning_basis=p.get("planning_basis", "departure"))
    if incoming_as_of is not None:
        s.sailing_as_of[p["sailing_id"]] = incoming_as_of
    s.bump_counter_to(p["sailing_id"])


def _apply_sailing_decision_recorded(s, p):
    s.sailing_decisions[p["sailing_id"]] = p["decision"]


def _apply_exceptional_pursuit_recorded(s, p):
    sid = p["sailing_id"]
    gen = p.get("generation")
    if gen is None:                      # legacy payload — derive [FCB2]
        gen = s.pursuit_seq.get(sid, 0) + 1
    s.pursuits[sid] = {
        "generation": gen,
        "recorded_on": p.get("recorded_on"),
        "departure": p.get("departure"),
        "days_to_departure": p.get("days_to_departure"),
    }
    s.pursuit_seq[sid] = max(s.pursuit_seq.get(sid, 0), gen)


def _apply_exceptional_pursuit_cancelled(s, p):
    s.pursuits.pop(p["sailing_id"], None)


def _apply_plan_opened(s, p):
    s.plans[p["plan_id"]] = ShipmentPlan(
        plan_id=p["plan_id"], sailing_id=p["sailing_id"],
        opened_at=p["opened_at"])
    s.bump_counter_to(p["plan_id"])


def _apply_suggestion_snapshotted(s, p):
    s.suggestions[p["snapshot_id"]] = freeze(_ser(
        {"plan_id": p["plan_id"], "product_id": p["product_id"],
         "content": p["content"]}))
    s.bump_counter_to(p["snapshot_id"])


def _apply_plan_decision_recorded(s, p):
    plan = s.plans[p["plan_id"]]
    if p["action"] == "postpone":
        plan.lines.pop(p["product_id"], None)
    else:
        plan.lines[p["product_id"]] = {
            "selected_m2": p["selected_m2"], "origin": p["origin"],
            "snapshot_ref": p["snapshot_ref"], "action": p["action"]}


def _apply_bl_split_set(s, p):
    pid = p["plan_id"]
    if pid in s.bl_splits:
        s.bl_history.setdefault(pid, []).append(s.bl_splits[pid])
    s.bl_splits[pid] = {"groups": p["groups"], "origin": p["origin"]}


def _apply_plan_finalized(s, p):
    plan = s.plans[p["plan_id"]]
    plan.lifecycle = "finalized"
    plan.finalized_at = p["finalized_at"]
    plan.late_state = p["late_state"]
    plan.handoff_id = p["handoff_id"]
    s.handoffs[p["handoff_id"]] = {
        "handoff_id": p["handoff_id"], "plan_id": p["plan_id"],
        "handoff": p["handoff"], "produced_at": p["finalized_at"],
        "superseded": False, "supersedes": p.get("supersedes")}
    # supersede prior monthly-context entries for this plan, then record new
    for entry in s.monthly_context.values():
        if entry["plan_id"] == p["plan_id"]:
            entry["superseded"] = True
    for row in p["uncovered"]:
        mid = s.new_id("MCTX")
        s.monthly_context[mid] = {
            "entry_id": mid, "plan_id": p["plan_id"],
            "product_id": row["product_id"], "m2": row["m2"],
            "computed_at": p["finalized_at"], "superseded": False}
    s.bump_counter_to(p["handoff_id"])


def _apply_reallocation_intent_confirmed(s, p):
    plan = s.plans[p["plan_id"]]
    plan.lifecycle = "draft"
    plan.finalized_at = None
    if p["handoff_id"] and p["handoff_id"] in s.handoffs:
        s.handoffs[p["handoff_id"]]["superseded"] = True


def _apply_production_order_opened(s, p):
    s.production_orders[p["production_order_id"]] = ProductionOrder(
        production_order_id=p["production_order_id"],
        source_plan_id=p["source_plan_id"], cycle_as_of=p["cycle_as_of"],
        factory_order_date=p["factory_order_date"],
        required_by=p.get("required_by"),
        calculation_head_seq=p["calculation_head_seq"],
        candidate_fingerprint=p["candidate_fingerprint"],
        planning_provenance=p["planning_provenance"],
        lines={line["product_id"]: {
            "selected_m2": line["selected_m2"],
            "recommendation_m2": line["recommendation_m2"],
            "origin": line["origin"],
            "calculation": line.get("calculation")} for line in p["lines"]},
        opened_at=p["opened_at"])
    s.bump_counter_to(p["production_order_id"])


def _apply_production_order_lines_edited(s, p):
    order = s.production_orders[p["production_order_id"]]
    for edit in p["edits"]:
        pid, m2 = edit["product_id"], edit["m2"]
        if m2 == ZERO:
            order.lines.pop(pid, None)
        elif pid in order.lines:
            order.lines[pid]["selected_m2"] = m2
            order.lines[pid]["origin"] = "edited"
        else:
            order.lines[pid] = {"selected_m2": m2,
                                "recommendation_m2": q2(ZERO),
                                "origin": "manual_add"}


def _apply_production_order_finalized(s, p):
    order = s.production_orders[p["production_order_id"]]
    order.lifecycle = "finalized"
    order.handoff = p["handoff"]
    order.finalized_at = p["finalized_at"]


def _apply_production_order_reference_recorded(s, p):
    order = s.production_orders[p["production_order_id"]]
    order.lifecycle = "reference_recorded"
    order.production_ref = p["production_ref"]
    order.reference_recorded_at = p["reference_recorded_at"]


def _apply_shipment_plan_reopened(s, p):
    plan = s.plans[p["plan_id"]]
    plan.lifecycle = "draft"
    plan.finalized_at = None
    plan.handoff_id = None
    if p["handoff_id"] in s.handoffs:
        s.handoffs[p["handoff_id"]]["superseded"] = True


def _apply_production_order_reopened(s, p):
    order = s.production_orders[p["production_order_id"]]
    order.lifecycle = "draft"
    order.handoff = None
    order.finalized_at = None


def _apply_order_amendment_opened(s, p):
    s.order_amendments[p["amendment_id"]] = OrderAmendment(
        amendment_id=p["amendment_id"], order_kind=p["order_kind"],
        order_id=p["order_id"], original_refs=list(p["original_refs"]),
        lines={line["product_id"]: {
            "baseline_m2": line["baseline_m2"],
            "selected_m2": line["selected_m2"]} for line in p["lines"]},
        opened_at=p["opened_at"])
    s.bump_counter_to(p["amendment_id"])


def _apply_order_amendment_lines_edited(s, p):
    amendment = s.order_amendments[p["amendment_id"]]
    for edit in p["edits"]:
        amendment.lines[edit["product_id"]]["selected_m2"] = edit["m2"]


def _apply_order_amendment_finalized(s, p):
    amendment = s.order_amendments[p["amendment_id"]]
    amendment.lifecycle = "finalized"
    amendment.handoff = p["handoff"]
    amendment.finalized_at = p["finalized_at"]


def _apply_order_amendment_reference_recorded(s, p):
    amendment = s.order_amendments[p["amendment_id"]]
    amendment.lifecycle = "reference_recorded"
    amendment.amendment_ref = p["amendment_ref"]
    amendment.reference_recorded_at = p["reference_recorded_at"]


def _apply_plan_closed(s, p):
    plan = s.plans[p["plan_id"]]
    plan.lifecycle = "closed"
    plan.closed_at = p["closed_at"]
    plan.close_reason = p["reason"]


def _apply_plan_recomputed(s, p):
    s.recomputes.append({
        "plan_id": p["plan_id"], "trigger": p["trigger"],
        "before": p["before"], "after": p["after"], "at": p["at"]})


def _apply_siesa_reference_recorded(s, p):
    s.commitments[p["commitment_id"]] = SiesaCommitment(
        commitment_id=p["commitment_id"],
        siesa_ref=p["siesa_ref"],
        handoff_order_ref=p["handoff_order_ref"],
        quantities=p["quantities"],
        reference_recorded_at=p["reference_recorded_at"],
        baseline_snapshot_id=p.get("baseline_snapshot_id"))
    s.bump_counter_to(p["commitment_id"])


def _apply_siesa_commitment_observed(s, p):
    c = s.commitments[p["commitment_id"]]
    c.state = "observed"
    c.observed_at = p["observed_at"]
    c.observed_snapshot_id = p["snapshot_id"]


def _apply_siesa_commitment_released(s, p):
    c = s.commitments[p["commitment_id"]]
    c.state = "released"
    c.release_note = p["note"]


def _apply_exclusion_created(s, p):
    s.exclusions[p["exclusion_id"]] = PendingHandoffExclusion(
        exclusion_id=p["exclusion_id"], plan_ref=p["plan_ref"],
        product_id=p["product_id"], m2=p["m2"],
        resolution_ref=p["resolution_ref"],
        order_ref=p.get("order_ref"))
    s.bump_counter_to(p["exclusion_id"])


def _apply_exclusion_reduced(s, p):
    x = s.exclusions[p["exclusion_id"]]
    x.m2 = q2(Decimal(str(x.m2)) - Decimal(str(p["by_m2"])))
    if x.m2 <= 0:
        x.m2 = q2(ZERO)
        x.active = False
        x.cleared_reason = p["reason"]


def _apply_exclusion_cleared(s, p):
    x = s.exclusions[p["exclusion_id"]]
    x.active = False
    x.cleared_reason = p["reason"]


def _apply_booking_recorded(s, p):
    s.bookings[p["booking_id"]] = Booking(
        booking_id=p["booking_id"], booking_ref=p["booking_ref"],
        sailing_id=p["sailing_id"], commitment_refs=p["commitment_refs"])
    s.bump_counter_to(p["booking_id"])


def _apply_expectation_created(s, p):
    s.expectations[p["exp_id"]] = Expectation(
        exp_id=p["exp_id"], product_id=p["product_id"],
        commitment_ref=p["commitment_ref"], booking_ref=p["booking_ref"],
        confirmed_m2=p["confirmed_m2"], effective_m2=p["confirmed_m2"],
        received_m2=Decimal("0"), state="booked", ask_date=p["ask_date"],
        sailing_id=p["sailing_id"])
    s.bump_counter_to(p["exp_id"])


def _apply_expectation_advanced(s, p):
    e = s.expectations[p["exp_id"]]
    e.state = p["state"]
    e.history.append({"advanced_to": p["state"],
                      "evidence_ref": p["evidence_ref"]})


def _apply_expectation_superseded(s, p):
    e = s.expectations[p["exp_id"]]
    e.history.append({"superseded_from": e.effective_m2,
                      "superseded_to": p["new_effective_m2"],
                      "note": p["note"], "evidence_ref": p["evidence_ref"]})
    e.effective_m2 = p["new_effective_m2"]


def _apply_arrival_attributed(s, p):
    e = s.expectations[p["exp_id"]]
    e.received_m2 = q2(e.received_m2 + p["m2"])
    e.history.append({"arrival_attributed": p["m2"], "at": p["at"]})
    if e.remaining_m2 == 0:
        e.state = "arrived"
        e.closed_at = p["at"]


def _apply_expectation_cancelled(s, p):
    e = s.expectations[p["exp_id"]]
    e.state = "cancelled"
    e.cancel_note = p["note"]


def _apply_expectation_overdue_flagged(s, p):
    s.expectations[p["exp_id"]].overdue = True


def _apply_expectation_overdue_cleared(s, p):
    s.expectations[p["exp_id"]].overdue = False


def _apply_implication_opened(s, p):
    s.implications[p["implication_id"]] = Implication(
        implication_id=p["implication_id"], family=p["family"],
        severity=p["severity"], subject_key=p["subject_key"],
        scope=p["scope"], evidence=p["evidence"],
        consequence=p["consequence"], recommendation=p["recommendation"],
        shipment_effect=p["shipment_effect"],
        typed_actions=p["typed_actions"], opened_at=p["opened_at"])
    s.bump_counter_to(p["implication_id"])


def _apply_implication_resolved(s, p):
    i = s.implications[p["implication_id"]]
    i.state = "resolved"
    i.resolution = {"action": p["action"], "params": p["params"]}
    i.resolved_at = p["resolved_at"]


def _apply_implication_superseded(s, p):
    i = s.implications[p["implication_id"]]
    i.state = "superseded"
    i.resolution = {"superseded_reason": p["reason"]}


_APPLY = {
    "evidence_loaded": _apply_evidence_loaded,
    "mapping_recorded": _apply_mapping_recorded,
    "sailing_recorded": _apply_sailing_recorded,
    "sailing_decision_recorded": _apply_sailing_decision_recorded,
    "exceptional_pursuit_recorded": _apply_exceptional_pursuit_recorded,
    "exceptional_pursuit_cancelled": _apply_exceptional_pursuit_cancelled,
    "plan_opened": _apply_plan_opened,
    "suggestion_snapshotted": _apply_suggestion_snapshotted,
    "plan_decision_recorded": _apply_plan_decision_recorded,
    "bl_split_set": _apply_bl_split_set,
    "plan_finalized": _apply_plan_finalized,
    "reallocation_intent_confirmed": _apply_reallocation_intent_confirmed,
    "production_order_opened": _apply_production_order_opened,
    "production_order_lines_edited": _apply_production_order_lines_edited,
    "production_order_finalized": _apply_production_order_finalized,
    "production_order_reference_recorded": _apply_production_order_reference_recorded,
    "shipment_plan_reopened": _apply_shipment_plan_reopened,
    "production_order_reopened": _apply_production_order_reopened,
    "order_amendment_opened": _apply_order_amendment_opened,
    "order_amendment_lines_edited": _apply_order_amendment_lines_edited,
    "order_amendment_finalized": _apply_order_amendment_finalized,
    "order_amendment_reference_recorded": _apply_order_amendment_reference_recorded,
    "plan_closed": _apply_plan_closed,
    "plan_recomputed": _apply_plan_recomputed,
    "siesa_reference_recorded": _apply_siesa_reference_recorded,
    "siesa_commitment_observed": _apply_siesa_commitment_observed,
    "siesa_commitment_released": _apply_siesa_commitment_released,
    "exclusion_created": _apply_exclusion_created,
    "exclusion_reduced": _apply_exclusion_reduced,
    "exclusion_cleared": _apply_exclusion_cleared,
    "booking_recorded": _apply_booking_recorded,
    "expectation_created": _apply_expectation_created,
    "expectation_advanced": _apply_expectation_advanced,
    "expectation_superseded": _apply_expectation_superseded,
    "arrival_attributed": _apply_arrival_attributed,
    "expectation_cancelled": _apply_expectation_cancelled,
    "expectation_overdue_flagged": _apply_expectation_overdue_flagged,
    "expectation_overdue_cleared": _apply_expectation_overdue_cleared,
    "implication_opened": _apply_implication_opened,
    "implication_resolved": _apply_implication_resolved,
    "implication_superseded": _apply_implication_superseded,
}
