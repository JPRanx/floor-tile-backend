"""
Clean V1 domain layer (accepted spec §4): append-only events and ONE
deterministic effective-state projection.

Two layers, never merged (§4.6b):
- Events: immutable, appended in order, never edited or deleted.
- Effective state: a deterministic left-fold of the event stream. Every
  public operation validates the FULL request against current effective
  state first (step-1 rejection — nothing recorded on failure), then builds
  its event list, then applies each event through the single mutation
  dispatcher `_APPLY` and appends it. `fold(events)` replays the same
  dispatcher from an empty state, so incremental state ≡ replayed state.

No I/O, no persistence, no authorization here — commands (I3) wrap this
with the §4.6a transactional administrative boundary.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from .planning_math import is_valid_selection

D = Decimal
NO_PRODUCTION_DEFAULT_NOTE = "no producción"

LINE_TERMINAL = ("factory_confirmed", "no_production", "withdrawn")
EXP_ORDER = ("confirmed", "produced", "in_transit", "arrived")
EXP_OPEN = ("confirmed", "produced", "in_transit")


class DomainError(Exception):
    """Illegal command/event against current effective state (§4.6a step 1)."""


# ── entities (effective-state projection) ───────────────────────────────────

@dataclass(frozen=True)
class Event:
    """One appended, immutable fact (§4.6b). FV2: the event stream is the
    SOURCE replay reconstructs state from, so the record itself is frozen —
    attribute reassignment raises, and `payload` is deep-frozen at append
    time (`FrozenDict`/tuple trees), so no caller can mutate the
    authoritative history and fork `fold(events)` from live state."""
    seq: int
    type: str
    at: str                    # ISO date of the business action
    actor: str
    payload: "FrozenDict"      # deep-frozen at construction (_emit/fold)
    note: Optional[str] = None


@dataclass
class MonthlyOrder:
    order_id: str
    cycle_month: str
    state: str = "draft"       # draft | submitted | closed
    opened_at: Optional[str] = None
    submitted_at: Optional[str] = None
    closed_at: Optional[str] = None


@dataclass
class MonthlyOrderLine:
    line_id: str
    order_id: str
    product_id: str
    selected_m2: Decimal
    origin: str                # suggestion | edited | manual | carry_forward
    carry_forward_of: Optional[str] = None
    state: str = "draft"       # §4.2
    confirmed_m2: Optional[Decimal] = None
    factory_outcome_recorded_at: Optional[str] = None
    no_production_note: Optional[str] = None
    withdrawal_note: Optional[str] = None
    ask_date: Optional[date] = None


class FrozenDict(Mapping):
    """Deeply immutable mapping (R2/AC1).

    Backing storage is a tuple of frozen key/value pairs, so no mutable dict is
    retained behind a read-only facade.  The class is final: accepting a
    behavior-overriding subclass through ``freeze``/``fold`` would let mutable
    external state masquerade as authoritative event truth.
    """

    __slots__ = ("_FrozenDict__d",)

    def __init_subclass__(cls, **kwargs):
        raise TypeError("FrozenDict is final and cannot be subclassed")

    def __init__(self, d):
        # Calling __init__ directly is an ordinary public-method path. Once
        # initialized, refuse replacement of authoritative backing storage.
        try:
            object.__getattribute__(self, "_FrozenDict__d")
        except AttributeError:
            pass
        else:
            raise TypeError("FrozenDict is immutable")
        items = tuple((k, freeze(v)) for k, v in dict(d).items())
        object.__setattr__(self, "_FrozenDict__d", items)

    def __getitem__(self, k):
        for key, value in self.__d:
            if key == k:
                return value
        raise KeyError(k)

    def __iter__(self):
        return (key for key, _ in self.__d)

    def __len__(self):
        return len(self.__d)

    def __eq__(self, other):
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return NotImplemented

    __hash__ = None                       # mapping content is not hashable

    def __setattr__(self, *_):
        raise TypeError("FrozenDict is immutable")

    def __delattr__(self, *_):
        raise TypeError("FrozenDict is immutable")

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __repr__(self):
        return f"FrozenDict({dict(self.items())!r})"


def freeze(v):
    """Recursively normalize values into the authoritative immutable universe.

    Mappings become final ``FrozenDict`` objects, sequences become tuples,
    sets become frozensets, and mutable binary leaves become bytes. Unknown
    object types are rejected instead of being retained by reference.
    """
    if isinstance(v, FrozenDict):
        return v
    if isinstance(v, Mapping):
        return FrozenDict(v)
    if isinstance(v, (list, tuple)):
        return tuple(freeze(x) for x in v)
    if isinstance(v, (set, frozenset)):
        return frozenset(freeze(x) for x in v)
    if isinstance(v, (bytearray, memoryview)):
        return bytes(v)
    if isinstance(v, (str, bytes, int, float, bool, Decimal, date, type(None))):
        return v
    raise TypeError(
        f"unsupported mutable or non-serializable payload value: {type(v).__name__}")


def thaw(v):
    """Deep defensive copy for external/read-model consumers: every mapping
    becomes a fresh plain dict, every sequence a fresh list — mutating the
    result can never reach the event-derived domain state (R2)."""
    if isinstance(v, (FrozenDict, dict)):
        return {k: thaw(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [thaw(x) for x in v]
    return v


@dataclass(frozen=True)
class OrderSuggestionSnapshot:
    """§4.1 `OrderSuggestion`, frozen copy (V7/R2): created SERVER-side at
    the moment a decision references the suggestion; immutable BY
    CONSTRUCTION — frozen dataclass over deeply frozen nested structures
    (explanation trace and input provenance are FrozenDict/tuple trees)."""
    snapshot_id: str
    order_id: str
    product_id: str
    true_need_m2: Decimal
    suggested_m2: Decimal
    derived_pallets: Decimal
    explanation: FrozenDict         # §6.8 sentence + structured trace
    computed_at: str                # ISO date of computation
    input_snapshot_refs: FrozenDict # feed → {snapshot_id, as_of} provenance


@dataclass
class AshleyOrderDecision:
    decision_id: str
    order_id: str
    product_id: str
    action: str                # accept | edit | defer | manual_add
    selected_m2: Optional[Decimal]
    suggestion_snapshot_ref: Optional[str]
    note: Optional[str]
    decided_at: str


@dataclass
class ExpectedIncoming:
    exp_id: str
    source_ref: str            # line ref | amendment ref (ONLY §4.6a paths)
    product_id: str
    ask_date: date
    confirmed_m2: Decimal      # immutable historical commitment
    effective_m2: Decimal
    received_m2: Decimal = D("0")
    state: str = "confirmed"
    overdue: bool = False
    evidence_superseded: bool = False
    evidence_refs: list = field(default_factory=list)
    history: list = field(default_factory=list)
    closed_at: Optional[str] = None

    @property
    def remaining_m2(self) -> Decimal:
        return max(D("0"), self.effective_m2 - self.received_m2)

    @property
    def open(self) -> bool:
        return self.state in EXP_OPEN


@dataclass
class CarryForwardOffer:
    offer_id: str
    product_id: str
    original_line_ref: str
    original_cycle: str
    original_m2: Decimal
    note: str
    state: str = "open"        # open | accepted | declined | superseded
    descendant_line_ref: Optional[str] = None


@dataclass
class AmendmentRequest:
    amendment_id: str
    product_id: str
    requested_m2: Decimal
    production_ref: str
    originating_case_ref: Optional[str]
    state: str = "open"        # open | accepted | declined | closed_unfulfilled
    accepted_m2: Optional[Decimal] = None
    note: Optional[str] = None
    created_at: Optional[date] = None
    resolved_at: Optional[str] = None


@dataclass
class Implication:
    """Persisted implication record (§4.1). Used for the V6 dedicated
    decision-consequence implication: append-only, provenance-complete."""
    imp_id: str
    type: str                  # decision_consequence | …
    subtype: str
    product_id: str
    refs: dict
    severity: str              # info | watch | risk
    text: str
    evidence: dict
    recorded_at: str


@dataclass
class NeedsAshleyCase:
    case_id: str
    type: str                  # §10.5 closed taxonomy
    subtype: str
    refs: dict
    evidence: dict
    options: list
    state: str = "open"
    resolution: Optional[dict] = None
    opened_at: Optional[str] = None
    resolved_at: Optional[str] = None


@dataclass
class State:
    orders: dict = field(default_factory=dict)
    lines: dict = field(default_factory=dict)
    decisions: list = field(default_factory=list)
    suggestions: dict = field(default_factory=dict)   # frozen §4.1 snapshots (V7)
    expectations: dict = field(default_factory=dict)
    offers: dict = field(default_factory=dict)
    amendments: dict = field(default_factory=dict)
    cases: dict = field(default_factory=dict)
    implications: dict = field(default_factory=dict)  # persisted (V6) implications
    events: list = field(default_factory=list)
    _counter: int = 0

    @staticmethod
    def empty() -> "State":
        return State()

    def new_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    def bump_counter_to(self, entity_id: str):
        digits = "".join(ch for ch in entity_id if ch.isdigit())
        if digits:
            self._counter = max(self._counter, int(digits))


# ── serialization helpers (snapshot equality, payload round-trip) ──────────

def _ser(v):
    if isinstance(v, Decimal):
        return f"dec:{v}"
    if isinstance(v, datetime):
        return f"datetime:{v.isoformat()}"
    if isinstance(v, date):
        return f"date:{v.isoformat()}"
    if isinstance(v, (bytes, bytearray, memoryview)):
        # Normalize mutable binary leaves before they enter authoritative
        # history.  Hex is deterministic and JSON-safe; _de restores bytes.
        return f"bytes:{bytes(v).hex()}"
    if isinstance(v, (list, tuple)):
        return [_ser(x) for x in v]
    if isinstance(v, (dict, FrozenDict)):
        return {k: _ser(x) for k, x in v.items()}
    return v


def _de(v):
    """Decode a (possibly frozen) serialized payload into PLAIN mutable
    structures — every mapping a fresh dict, every sequence a fresh list —
    so appliers never hold references into the frozen event record (FV2)."""
    if isinstance(v, str) and v.startswith("dec:"):
        return D(v[4:])
    if isinstance(v, str) and v.startswith("datetime:"):
        return datetime.fromisoformat(v[9:])
    if isinstance(v, str) and v.startswith("date:"):
        return date.fromisoformat(v[5:])
    if isinstance(v, str) and v.startswith("bytes:"):
        return bytes.fromhex(v[6:])
    if isinstance(v, (list, tuple)):
        return [_de(x) for x in v]
    if isinstance(v, (dict, FrozenDict)):
        return {k: _de(x) for k, x in v.items()}
    return v


def snapshot(s: State) -> dict:
    """Comparable deep view of the effective state + event stream."""
    return _ser({
        "orders": {k: asdict(v) for k, v in s.orders.items()},
        "lines": {k: asdict(v) for k, v in s.lines.items()},
        "decisions": [asdict(d) for d in s.decisions],
        "suggestions": {k: asdict(v) for k, v in s.suggestions.items()},
        "expectations": {k: asdict(v) for k, v in s.expectations.items()},
        "offers": {k: asdict(v) for k, v in s.offers.items()},
        "amendments": {k: asdict(v) for k, v in s.amendments.items()},
        "cases": {k: asdict(v) for k, v in s.cases.items()},
        "implications": {k: asdict(v) for k, v in s.implications.items()},
        "events": [(e.seq, e.type, e.at, e.actor, e.payload, e.note) for e in s.events],
    })


# ── event emission ─────────────────────────────────────────────────────────

def _emit(s: State, etype: str, actor: str, on: date, payload: dict,
          note: Optional[str] = None):
    # FV2: the stored payload is deep-frozen — the appended record can never
    # be mutated into a replay fork
    ev = Event(seq=len(s.events) + 1, type=etype, at=on.isoformat(),
               actor=actor, payload=freeze(_ser(payload)), note=note)
    _APPLY[etype](s, _de(ev.payload))
    s.events.append(ev)
    return ev


def fold(events: list) -> State:
    """Deterministic left-fold of a recorded event stream (§4.6b).
    FV2: the retained stream re-freezes every payload, so the folded state
    shares no mutable payload reference with its input events."""
    s = State.empty()
    for ev in events:
        # Normalize even externally supplied/malformed event payloads through
        # the same deterministic serialization boundary as _emit.  This
        # prevents mutable leaf aliases from surviving into folded history.
        payload = freeze(_ser(ev.payload))
        _APPLY[ev.type](s, _de(payload))
        s.events.append(Event(seq=ev.seq, type=ev.type, at=ev.at,
                              actor=ev.actor, payload=payload,
                              note=ev.note))
    return s


# ── mutation dispatcher (assumes pre-validated payloads) ───────────────────

def _apply_order_opened(s, p):
    s.orders[p["order_id"]] = MonthlyOrder(
        order_id=p["order_id"], cycle_month=p["cycle_month"], state="draft",
        opened_at=p["opened_at"])
    s.bump_counter_to(p["order_id"])


def _apply_decision_recorded(s, p):
    s.decisions.append(AshleyOrderDecision(
        decision_id=p["decision_id"], order_id=p["order_id"],
        product_id=p["product_id"], action=p["action"],
        selected_m2=p.get("selected_m2"),
        suggestion_snapshot_ref=p.get("suggestion_snapshot_ref"),
        note=p.get("note"), decided_at=p["decided_at"]))
    s.bump_counter_to(p["decision_id"])
    effect = p["line_effect"]
    if effect == "removed":
        s.lines.pop(p["line_id"], None)
    elif effect == "created":
        s.lines[p["line_id"]] = MonthlyOrderLine(
            line_id=p["line_id"], order_id=p["order_id"],
            product_id=p["product_id"], selected_m2=p["selected_m2"],
            origin=p["origin"], carry_forward_of=p.get("carry_forward_of"))
        s.bump_counter_to(p["line_id"])
    elif effect == "updated":
        line = s.lines[p["line_id"]]
        line.selected_m2 = p["selected_m2"]
        line.origin = p["origin"]


def _apply_suggestion_snapshotted(s, p):
    s.suggestions[p["snapshot_id"]] = OrderSuggestionSnapshot(
        snapshot_id=p["snapshot_id"], order_id=p["order_id"],
        product_id=p["product_id"], true_need_m2=p["true_need_m2"],
        suggested_m2=p["suggested_m2"], derived_pallets=p["derived_pallets"],
        explanation=freeze(p["explanation"]),           # deep-frozen (R2)
        computed_at=p["computed_at"],
        input_snapshot_refs=freeze(p["input_snapshot_refs"]))
    s.bump_counter_to(p["snapshot_id"])


def _apply_implication_recorded(s, p):
    s.implications[p["imp_id"]] = Implication(
        imp_id=p["imp_id"], type=p["type"], subtype=p.get("subtype", ""),
        product_id=p["product_id"], refs=p.get("refs", {}),
        severity=p["severity"], text=p["text"],
        evidence=p.get("evidence", {}), recorded_at=p["at"])
    s.bump_counter_to(p["imp_id"])


def _apply_carry_forward_accepted(s, p):
    offer = s.offers[p["offer_id"]]
    offer.state = "accepted"
    offer.descendant_line_ref = p["line_id"]
    s.lines[p["line_id"]] = MonthlyOrderLine(
        line_id=p["line_id"], order_id=p["order_id"],
        product_id=p["product_id"], selected_m2=p["selected_m2"],
        origin="carry_forward", carry_forward_of=p["carry_forward_of"])
    s.bump_counter_to(p["line_id"])


def _apply_carry_forward_declined(s, p):
    s.offers[p["offer_id"]].state = "declined"


def _apply_carry_forward_acceptance_reversed(s, p):
    offer = s.offers[p["offer_id"]]
    offer.state = "open"
    offer.descendant_line_ref = None


def _apply_offer_spawned(s, p):
    s.offers[p["offer_id"]] = CarryForwardOffer(
        offer_id=p["offer_id"], product_id=p["product_id"],
        original_line_ref=p["original_line_ref"],
        original_cycle=p["original_cycle"], original_m2=p["original_m2"],
        note=p["note"])
    s.bump_counter_to(p["offer_id"])


def _apply_offer_superseded(s, p):
    s.offers[p["offer_id"]].state = "superseded"


def _apply_order_submitted(s, p):
    order = s.orders[p["order_id"]]
    order.state = "submitted"
    order.submitted_at = p["submitted_at"]
    for lid in p["line_ids"]:
        line = s.lines[lid]
        line.state = "awaiting_factory"
        line.ask_date = p["ask_date"]


def _apply_order_closed(s, p):
    order = s.orders[p["order_id"]]
    order.state = "closed"
    order.closed_at = p["closed_at"]


def _apply_order_reverted_to_submitted(s, p):
    order = s.orders[p["order_id"]]
    order.state = "submitted"
    order.closed_at = None


def _apply_outcome_recorded(s, p):
    line = s.lines[p["line_id"]]
    line.state = p["outcome"]
    line.factory_outcome_recorded_at = p["recorded_at"]
    if p["outcome"] == "factory_confirmed":
        line.confirmed_m2 = p["confirmed_m2"]
    else:
        line.no_production_note = p.get("note") or NO_PRODUCTION_DEFAULT_NOTE


def _apply_outcome_corrected(s, p):
    line = s.lines[p["line_id"]]
    line.state = p["corrected_outcome"]
    if p["corrected_outcome"] == "factory_confirmed":
        line.confirmed_m2 = p["corrected_m2"]
    else:
        line.no_production_note = p.get("note") or NO_PRODUCTION_DEFAULT_NOTE


def _apply_quantity_corrected(s, p):
    line = s.lines[p["line_id"]]
    line.confirmed_m2 = p["corrected_m2"]
    if p.get("expectation_id"):
        exp = s.expectations[p["expectation_id"]]
        exp.effective_m2 = p["corrected_m2"]
        exp.history.append({"event": "quantity_corrected",
                            "effective_m2": p["corrected_m2"], "at": p["at"]})


def _apply_withdrawn(s, p):
    line = s.lines[p["line_id"]]
    line.state = "withdrawn"
    line.withdrawal_note = p.get("note")


def _apply_withdrawal_reversed(s, p):
    line = s.lines[p["line_id"]]
    line.state = "awaiting_factory"
    line.withdrawal_note = None


def _apply_expectation_created(s, p):
    s.expectations[p["exp_id"]] = ExpectedIncoming(
        exp_id=p["exp_id"], source_ref=p["source_ref"],
        product_id=p["product_id"], ask_date=p["ask_date"],
        confirmed_m2=p["confirmed_m2"], effective_m2=p["confirmed_m2"])
    s.bump_counter_to(p["exp_id"])


def _apply_expectation_advanced(s, p):
    exp = s.expectations[p["exp_id"]]
    exp.state = p["new_state"]
    if p.get("evidence_ref"):
        exp.evidence_refs.append(p["evidence_ref"])
    exp.history.append({"event": "advanced", "state": p["new_state"], "at": p["at"]})
    if p["new_state"] == "arrived":
        exp.closed_at = p["at"]


def _apply_expectation_superseded(s, p):
    exp = s.expectations[p["exp_id"]]
    entry = {"event": "superseded", "from": exp.effective_m2,
             "to": p["new_effective_m2"], "note": p.get("note"),
             "at": p["at"]}
    if p.get("prominent"):
        # S8c: the prominent marker is EVENT truth, applied here so replay
        # reconstructs it identically (review finding F1 — never a
        # projection-side mutation outside the applier)
        entry["prominent"] = True
    exp.history.append(entry)
    exp.effective_m2 = p["new_effective_m2"]
    exp.evidence_superseded = True
    if p.get("evidence_ref"):
        exp.evidence_refs.append(p["evidence_ref"])


def _apply_arrival_attributed(s, p):
    exp = s.expectations[p["exp_id"]]
    exp.received_m2 = exp.received_m2 + p["m2"]
    exp.history.append({"event": "arrival_attributed", "m2": p["m2"], "at": p["at"]})
    if exp.remaining_m2 == 0:
        exp.state = "arrived"
        exp.closed_at = p["at"]


def _apply_expectation_cancelled(s, p):
    exp = s.expectations[p["exp_id"]]
    exp.state = "cancelled"
    exp.closed_at = p["at"]
    exp.history.append({"event": "cancelled", "reason": p.get("reason"), "at": p["at"]})


def _apply_expectation_overdue_flagged(s, p):
    exp = s.expectations[p["exp_id"]]
    exp.overdue = True
    # FV5: the transition is part of the expectation's replayable history
    exp.history.append({"event": "overdue_flagged", "at": p["at"]})


def _apply_expectation_overdue_cleared(s, p):
    exp = s.expectations[p["exp_id"]]
    exp.overdue = False
    # FV5: truthful recovery provenance travels with the transition
    exp.history.append({"event": "overdue_cleared",
                        "reason": p.get("reason"), "at": p["at"]})


def _apply_amendment_created(s, p):
    s.amendments[p["amendment_id"]] = AmendmentRequest(
        amendment_id=p["amendment_id"], product_id=p["product_id"],
        requested_m2=p["requested_m2"], production_ref=p["production_ref"],
        originating_case_ref=p.get("case_ref"), created_at=p["created_at"])
    s.bump_counter_to(p["amendment_id"])


def _apply_amendment_outcome_recorded(s, p):
    a = s.amendments[p["amendment_id"]]
    a.state = p["outcome"]
    a.resolved_at = p["at"]
    if p["outcome"] == "accepted":
        a.accepted_m2 = p["accepted_m2"]
    if p.get("note"):
        a.note = p["note"]


def _apply_amendment_outcome_corrected(s, p):
    a = s.amendments[p["amendment_id"]]
    a.state = p["corrected_outcome"]
    a.resolved_at = p["at"]
    if p["corrected_outcome"] == "accepted":
        a.accepted_m2 = p["accepted_m2"]
    if p.get("note"):
        a.note = p["note"]


def _apply_amendment_closed_unfulfilled(s, p):
    a = s.amendments[p["amendment_id"]]
    a.state = "closed_unfulfilled"
    a.resolved_at = p["at"]
    a.note = p.get("reason")


def _apply_amendment_note(s, p):
    a = s.amendments[p["amendment_id"]]
    a.note = p["note"]


def _apply_case_opened(s, p):
    s.cases[p["case_id"]] = NeedsAshleyCase(
        case_id=p["case_id"], type=p["type"], subtype=p.get("subtype", ""),
        refs=p.get("refs", {}), evidence=p.get("evidence", {}),
        options=p.get("options", []), opened_at=p["at"])
    s.bump_counter_to(p["case_id"])


def _apply_case_resolved(s, p):
    c = s.cases[p["case_id"]]
    c.state = "resolved"
    c.resolution = p.get("resolution")
    c.resolved_at = p["at"]


_APPLY = {
    "order_opened": _apply_order_opened,
    "decision_recorded": _apply_decision_recorded,
    "suggestion_snapshotted": _apply_suggestion_snapshotted,
    "implication_recorded": _apply_implication_recorded,
    "carry_forward_accepted": _apply_carry_forward_accepted,
    "carry_forward_declined": _apply_carry_forward_declined,
    "carry_forward_acceptance_reversed": _apply_carry_forward_acceptance_reversed,
    "offer_spawned": _apply_offer_spawned,
    "offer_superseded": _apply_offer_superseded,
    "order_submitted": _apply_order_submitted,
    "order_closed": _apply_order_closed,
    "order_reverted_to_submitted": _apply_order_reverted_to_submitted,
    "outcome_recorded": _apply_outcome_recorded,
    "outcome_corrected": _apply_outcome_corrected,
    "quantity_corrected": _apply_quantity_corrected,
    "withdrawn": _apply_withdrawn,
    "withdrawal_reversed": _apply_withdrawal_reversed,
    "expectation_created": _apply_expectation_created,
    "expectation_advanced": _apply_expectation_advanced,
    "expectation_superseded": _apply_expectation_superseded,
    "arrival_attributed": _apply_arrival_attributed,
    "expectation_cancelled": _apply_expectation_cancelled,
    "expectation_overdue_flagged": _apply_expectation_overdue_flagged,
    "expectation_overdue_cleared": _apply_expectation_overdue_cleared,
    "amendment_created": _apply_amendment_created,
    "amendment_outcome_recorded": _apply_amendment_outcome_recorded,
    "amendment_outcome_corrected": _apply_amendment_outcome_corrected,
    "amendment_closed_unfulfilled": _apply_amendment_closed_unfulfilled,
    "amendment_decline_confirmation_noted": _apply_amendment_note,
    "case_opened": _apply_case_opened,
    "case_resolved": _apply_case_resolved,
}


# ── guards ──────────────────────────────────────────────────────────────────

def _order(s, oid) -> MonthlyOrder:
    if oid not in s.orders:
        raise DomainError(f"unknown order {oid}")
    return s.orders[oid]


def _line(s, lid) -> MonthlyOrderLine:
    if lid not in s.lines:
        raise DomainError(f"unknown line {lid}")
    return s.lines[lid]


def _exp(s, eid) -> ExpectedIncoming:
    if eid not in s.expectations:
        raise DomainError(f"unknown expectation {eid}")
    return s.expectations[eid]


def _require_open_exp(exp: ExpectedIncoming):
    if not exp.open:
        raise DomainError(
            f"expectation {exp.exp_id} is {exp.state} — immutable terminal (§4.3)")


def _draft_line_for(s, order_id, product_id):
    for l in s.lines.values():
        if l.order_id == order_id and l.product_id == product_id:
            return l
    return None


# ── public operations ───────────────────────────────────────────────────────

def open_order(s: State, *, cycle_month: str, on: date, actor: str) -> str:
    if any(o.cycle_month == cycle_month for o in s.orders.values()):
        raise DomainError(f"one MonthlyOrder per cycle_month (V3): {cycle_month}")
    oid = s.new_id("ORD")
    _emit(s, "order_opened", actor, on,
          {"order_id": oid, "cycle_month": cycle_month, "opened_at": on.isoformat()})
    return oid


def record_suggestion_snapshot(s: State, *, order_id: str, product_id: str,
                               content: dict, actor: str, on: date) -> str:
    """Freeze the current suggestion as an immutable §4.1 snapshot (V7).
    Called ONLY by the command boundary, in the same transaction as the
    decision that references it — never from any client payload."""
    order = _order(s, order_id)
    if order.state != "draft":
        raise DomainError("suggestion snapshots freeze at draft decisions (§4.2)")
    required = ("true_need_m2", "suggested_m2", "derived_pallets",
                "explanation", "computed_at", "input_snapshot_refs")
    missing = [k for k in required if k not in content]
    if missing:
        raise DomainError(f"incomplete suggestion snapshot content: {missing}")
    sug_id = s.new_id("SUG")
    _emit(s, "suggestion_snapshotted", actor, on,
          {"snapshot_id": sug_id, "order_id": order_id,
           "product_id": product_id, **{k: content[k] for k in required}})
    return sug_id


def record_implication(s: State, *, type: str, subtype: str, product_id: str,
                       refs: dict, severity: str, text: str, evidence: dict,
                       actor: str, on: date) -> str:
    imp_id = s.new_id("IMP")
    _emit(s, "implication_recorded", actor, on,
          {"imp_id": imp_id, "type": type, "subtype": subtype,
           "product_id": product_id, "refs": refs, "severity": severity,
           "text": text, "evidence": evidence, "at": on.isoformat()})
    return imp_id


def draft_decision(s: State, order_id: str, product_id: str, *, action: str,
                   selected_m2: Optional[Decimal], actor: str, on: date,
                   suggestion_snapshot_ref: Optional[str] = None,
                   note: Optional[str] = None) -> Optional[str]:
    order = _order(s, order_id)
    if order.state != "draft":
        raise DomainError("decisions are revisable only while the order is draft (§4.2)")
    if action not in ("accept", "edit", "defer", "manual_add"):
        raise DomainError(f"unknown decision action {action}")
    if action != "defer":
        if selected_m2 is None or not is_valid_selection(selected_m2):
            raise DomainError(
                f"selected_m2 must be a positive multiple of 67.2 m² (V1/V4): {selected_m2}")

    existing = _draft_line_for(s, order_id, product_id)
    if existing and existing.state != "draft":
        raise DomainError("only draft lines are revisable (§4.2)")

    origin = {"accept": "suggestion", "edit": "edited", "manual_add": "manual"}.get(action)
    events = []
    did = s.new_id("DEC")
    if action == "defer":
        payload = {"decision_id": did, "order_id": order_id, "product_id": product_id,
                   "action": action, "selected_m2": None,
                   "suggestion_snapshot_ref": suggestion_snapshot_ref, "note": note,
                   "decided_at": on.isoformat(),
                   "line_effect": "removed" if existing else "none",
                   "line_id": existing.line_id if existing else None,
                   "origin": None}
        events.append(("decision_recorded", payload))
        if existing and existing.origin == "carry_forward":
            offer = next((o for o in s.offers.values()
                          if o.descendant_line_ref == existing.line_id), None)
            if offer:
                events.append(("carry_forward_acceptance_reversed",
                               {"offer_id": offer.offer_id, "line_id": existing.line_id}))
    else:
        if existing:
            effect, lid = "updated", existing.line_id
        else:
            effect, lid = "created", s.new_id("LIN")
        payload = {"decision_id": did, "order_id": order_id, "product_id": product_id,
                   "action": action, "selected_m2": selected_m2,
                   "suggestion_snapshot_ref": suggestion_snapshot_ref, "note": note,
                   "decided_at": on.isoformat(), "line_effect": effect,
                   "line_id": lid, "origin": origin, "carry_forward_of": None}
        events.append(("decision_recorded", payload))

    for etype, payload in events:
        _emit(s, etype, actor, on, payload, note=note)
    return did


def submit_order(s: State, order_id: str, *, on: date, actor: str):
    order = _order(s, order_id)
    if order.state != "draft":
        raise DomainError("only draft orders submit")
    line_ids = [l.line_id for l in s.lines.values()
                if l.order_id == order_id and l.state == "draft"]
    if not line_ids:
        raise DomainError("an order must contain at least one line to submit (V2)")
    _emit(s, "order_submitted", actor, on,
          {"order_id": order_id, "line_ids": sorted(line_ids),
           "submitted_at": on.isoformat(), "ask_date": on})


def record_outcome(s: State, line_id: str, outcome: str, *,
                   confirmed_m2: Optional[Decimal] = None,
                   note: Optional[str] = None, actor: str, on: date):
    line = _line(s, line_id)
    if line.state != "awaiting_factory":
        raise DomainError(
            f"factory outcome requires awaiting_factory, line is {line.state} (§4.2)")
    if outcome not in ("factory_confirmed", "no_production"):
        raise DomainError(f"unknown outcome {outcome}")

    events = []
    if outcome == "factory_confirmed":
        qty = confirmed_m2 if confirmed_m2 is not None else line.selected_m2
        if qty <= 0:
            raise DomainError("confirmed_m2 must be positive")
        eid = s.new_id("EXP")
        events.append(("outcome_recorded",
                       {"line_id": line_id, "outcome": outcome, "confirmed_m2": qty,
                        "recorded_at": on.isoformat(), "note": note}))
        events.append(("expectation_created",
                       {"exp_id": eid, "source_ref": line_id,
                        "product_id": line.product_id, "ask_date": line.ask_date,
                        "confirmed_m2": qty}))
    else:
        if any(o.original_line_ref == line_id and o.state == "open"
               for o in s.offers.values()):
            raise DomainError("at most one open CarryForwardOffer per original line (V9)")
        fid = s.new_id("OFF")
        cycle = s.orders[line.order_id].cycle_month
        events.append(("outcome_recorded",
                       {"line_id": line_id, "outcome": outcome, "confirmed_m2": None,
                        "recorded_at": on.isoformat(),
                        "note": note or NO_PRODUCTION_DEFAULT_NOTE}))
        events.append(("offer_spawned",
                       {"offer_id": fid, "product_id": line.product_id,
                        "original_line_ref": line_id, "original_cycle": cycle,
                        "original_m2": line.selected_m2,
                        "note": note or NO_PRODUCTION_DEFAULT_NOTE}))
    for etype, payload in events:
        _emit(s, etype, actor, on, payload, note=note)


def spawn_expectation_for_line(s: State, line_id: str, *, on: date, actor: str) -> str:
    """Guarded creation path proof: only factory_confirmed lines without an
    existing expectation may spawn one (§4.3 commitment-only creation)."""
    line = _line(s, line_id)
    if line.state != "factory_confirmed":
        raise DomainError(
            "ExpectedIncoming is created ONLY from a recorded factory commitment "
            f"(line is {line.state})")
    if any(e.source_ref == line_id for e in s.expectations.values()):
        raise DomainError("expectation already exists for this line")
    eid = s.new_id("EXP")
    _emit(s, "expectation_created", actor, on,
          {"exp_id": eid, "source_ref": line_id, "product_id": line.product_id,
           "ask_date": line.ask_date, "confirmed_m2": line.confirmed_m2})
    return eid


def withdraw_line(s: State, line_id: str, *, note: str, actor: str, on: date):
    line = _line(s, line_id)
    if line.state == "draft":
        # not a lifecycle withdrawal — a defer-equivalent draft revision
        # (§4.6c). FV1/AC4: provenance follows the ACTUAL withdrawn line's
        # origin, never a loose same-order/product reverse search:
        # - suggestion/edit lines preserve the server-owned snapshot of the
        #   accept/edit decision that created or last governed THIS line;
        # - manual lines stay truthfully snapshot-less;
        # - carry-forward lines stay snapshot-less even when unrelated
        #   suggestion decisions exist for the same order/product — the
        #   accepted carry-forward event IS the decision record (accepted
        #   interpretation 4), and borrowing an unrelated snapshot would
        #   claim Ashley acted from a suggestion she never saw for this
        #   line. A dangling reference is corrupt state — fail atomically.
        ref = None
        if line.origin in ("suggestion", "edited"):
            governing = next((d for d in reversed(s.decisions)
                              if d.order_id == line.order_id
                              and d.product_id == line.product_id
                              and d.action in ("accept", "edit")), None)
            if governing is not None and \
                    governing.suggestion_snapshot_ref is not None:
                if governing.suggestion_snapshot_ref not in s.suggestions:
                    raise DomainError(
                        f"decision {governing.decision_id} references "
                        f"unknown suggestion snapshot "
                        f"{governing.suggestion_snapshot_ref!r} — draft "
                        "withdrawal refused with nothing recorded (FV1/AC4); "
                        "the server-owned snapshot contract (C1/V7) admits "
                        "no silent snapshot loss")
                ref = governing.suggestion_snapshot_ref
        draft_decision(s, line.order_id, line.product_id, action="defer",
                       selected_m2=None, actor=actor, on=on, note=note,
                       suggestion_snapshot_ref=ref)
        return
    if line.state != "awaiting_factory":
        raise DomainError(
            f"post-outcome lines are not withdrawable (line is {line.state}; "
            "use CorrectFactoryOutcome — §4.6c)")
    _emit(s, "withdrawn", actor, on, {"line_id": line_id, "note": note}, note=note)


def reverse_withdrawal(s: State, line_id: str, *, note: str, actor: str, on: date):
    line = _line(s, line_id)
    if line.state != "withdrawn":
        raise DomainError("withdrawal reversal requires an effective withdrawn state")
    if not any(e.type == "withdrawn" and e.payload.get("line_id") == line_id
               for e in s.events):
        raise DomainError("no prior withdrawal event exists for this line")
    order = s.orders[line.order_id]
    _emit(s, "withdrawal_reversed", actor, on, {"line_id": line_id, "note": note},
          note=note)
    if order.state == "closed":
        _emit(s, "order_reverted_to_submitted", actor, on,
              {"order_id": order.order_id}, note="withdrawal reversal reopens order (§4.4)")


def close_order(s: State, order_id: str, *, actor: str, on: date):
    order = _order(s, order_id)
    if order.state != "submitted":
        raise DomainError("only submitted orders close")
    for l in s.lines.values():
        if l.order_id == order_id and l.state not in LINE_TERMINAL:
            raise DomainError(
                f"line {l.line_id} is {l.state} — every line must be terminal (§4.4)")
    _emit(s, "order_closed", actor, on,
          {"order_id": order_id, "closed_at": on.isoformat()})


# expectations ---------------------------------------------------------------

def advance_expectation(s: State, exp_id: str, new_state: str, *,
                        evidence_ref: Optional[str], actor: str, on: date):
    exp = _exp(s, exp_id)
    _require_open_exp(exp)
    if new_state not in EXP_ORDER:
        raise DomainError(f"unknown expectation state {new_state}")
    if EXP_ORDER.index(new_state) <= EXP_ORDER.index(exp.state):
        raise DomainError(
            f"backward/no-op move {exp.state} → {new_state} never occurs (§4.3)")
    _emit(s, "expectation_advanced", actor, on,
          {"exp_id": exp_id, "new_state": new_state,
           "evidence_ref": evidence_ref, "at": on.isoformat()})


def supersede_expectation(s: State, exp_id: str, *, new_effective_m2: Decimal,
                          evidence_ref: Optional[str], note: str, actor: str,
                          on: date, prominent: bool = False):
    exp = _exp(s, exp_id)
    _require_open_exp(exp)
    if new_effective_m2 < exp.received_m2:
        raise DomainError("effective_m2 cannot fall below already-received quantity")
    _emit(s, "expectation_superseded", actor, on,
          {"exp_id": exp_id, "new_effective_m2": new_effective_m2,
           "evidence_ref": evidence_ref, "note": note, "at": on.isoformat(),
           "prominent": prominent},
          note=note)


def attribute_arrival_to_expectation(s: State, exp_id: str, *, m2: Decimal,
                                     actor: str, on: date):
    exp = _exp(s, exp_id)
    _require_open_exp(exp)
    if m2 <= 0:
        raise DomainError("attributed quantity must be positive")
    if m2 > exp.remaining_m2:
        raise DomainError(
            f"attribution {m2} exceeds remaining_m2 {exp.remaining_m2} (§6.4a)")
    _emit(s, "arrival_attributed", actor, on,
          {"exp_id": exp_id, "m2": m2, "at": on.isoformat()})


def cancel_expectation(s: State, exp_id: str, *, reason: str, actor: str, on: date):
    exp = _exp(s, exp_id)
    _require_open_exp(exp)
    _emit(s, "expectation_cancelled", actor, on,
          {"exp_id": exp_id, "reason": reason, "at": on.isoformat()}, note=reason)


def flag_expectation_overdue(s: State, exp_id: str, *, actor: str, on: date):
    exp = _exp(s, exp_id)
    _require_open_exp(exp)
    if exp.overdue:
        return                # FV5: no transition happened — no event, no entry
    _emit(s, "expectation_overdue_flagged", actor, on,
          {"exp_id": exp_id, "at": on.isoformat()})


def clear_expectation_overdue(s: State, exp_id: str, *,
                              reason: Optional[str] = None,
                              actor: str, on: date):
    exp = _exp(s, exp_id)
    if not exp.overdue:
        return                # FV5: no transition happened — no event, no entry
    _emit(s, "expectation_overdue_cleared", actor, on,
          {"exp_id": exp_id, "reason": reason, "at": on.isoformat()})


# carry-forward ---------------------------------------------------------------

def accept_carry_forward(s: State, offer_id: str, order_id: str, *,
                         actor: str, on: date) -> str:
    if offer_id not in s.offers:
        raise DomainError(f"unknown offer {offer_id}")
    offer = s.offers[offer_id]
    if offer.state != "open":
        raise DomainError(f"offer is {offer.state}; only open offers accept (§4.1)")
    order = _order(s, order_id)
    if order.state != "draft":
        raise DomainError("carry-forward accepts into the current draft order only")
    if _draft_line_for(s, order_id, offer.product_id):
        raise DomainError(
            "one line per product per order (V9) — edit the existing line instead")
    lid = s.new_id("LIN")
    _emit(s, "carry_forward_accepted", actor, on,
          {"offer_id": offer_id, "order_id": order_id, "line_id": lid,
           "product_id": offer.product_id, "selected_m2": offer.original_m2,
           "carry_forward_of": offer.original_line_ref})
    return lid


def decline_carry_forward(s: State, offer_id: str, *, actor: str, on: date):
    offer = s.offers.get(offer_id)
    if offer is None:
        raise DomainError(f"unknown offer {offer_id}")
    if offer.state != "open":
        raise DomainError(f"offer is {offer.state}; only open offers decline")
    _emit(s, "carry_forward_declined", actor, on, {"offer_id": offer_id})


# corrections (§4.6d) ---------------------------------------------------------

def correct_outcome(s: State, line_id: str, corrected_outcome: str, *,
                    corrected_m2: Optional[Decimal] = None, note: str,
                    actor: str, on: date):
    line = _line(s, line_id)
    if line.state not in ("factory_confirmed", "no_production"):
        raise DomainError(
            f"corrections apply to recorded outcomes only (line is {line.state}; "
            "drafts are freely revisable — §4.6d)")
    if corrected_outcome not in ("factory_confirmed", "no_production"):
        raise DomainError(f"unknown corrected outcome {corrected_outcome}")
    if corrected_outcome == line.state:
        raise DomainError("correction must change the outcome (use quantity correction)")

    events = []
    if line.state == "factory_confirmed":     # → no_production
        exp = next((e for e in s.expectations.values() if e.source_ref == line_id), None)
        events.append(("outcome_corrected",
                       {"line_id": line_id, "corrected_outcome": "no_production",
                        "corrected_m2": None, "note": note}))
        spawn_offer = True
        if exp is not None:
            if exp.state == "arrived":
                # immutable terminal: no reopen, no offer (goods exist), exception
                spawn_offer = False
                cid = s.new_id("CAS")
                events.append(("case_opened", {
                    "case_id": cid, "type": "ReconciliationException",
                    "subtype": "arrived_contradiction",
                    "refs": {"line_id": line_id, "exp_id": exp.exp_id},
                    "evidence": {"recorded_outcome": "no_production",
                                 "arrived_m2": str(exp.received_m2)},
                    "options": ["re_correct_outcome", "keep_correction_annotate_arrival"],
                    "at": on.isoformat()}))
            elif exp.open:
                events.append(("expectation_cancelled",
                               {"exp_id": exp.exp_id,
                                "reason": f"outcome corrected: {note}",
                                "at": on.isoformat()}))
            # already cancelled → folds unchanged
        if spawn_offer:
            if any(o.original_line_ref == line_id and o.state == "open"
                   for o in s.offers.values()):
                raise DomainError("at most one open offer per original line (V9)")
            fid = s.new_id("OFF")
            events.append(("offer_spawned",
                           {"offer_id": fid, "product_id": line.product_id,
                            "original_line_ref": line_id,
                            "original_cycle": s.orders[line.order_id].cycle_month,
                            "original_m2": line.selected_m2,
                            "note": note or NO_PRODUCTION_DEFAULT_NOTE}))

    else:                                     # no_production → factory_confirmed
        qty = corrected_m2 if corrected_m2 is not None else line.selected_m2
        if qty is None or qty <= 0:
            raise DomainError("corrected confirmed_m2 must be positive")
        events.append(("outcome_corrected",
                       {"line_id": line_id, "corrected_outcome": "factory_confirmed",
                        "corrected_m2": qty, "note": note}))
        eid = s.new_id("EXP")
        events.append(("expectation_created",
                       {"exp_id": eid, "source_ref": line_id,
                        "product_id": line.product_id, "ask_date": line.ask_date,
                        "confirmed_m2": qty}))
        offer = next((o for o in s.offers.values()
                      if o.original_line_ref == line_id), None)
        if offer is not None:
            if offer.state == "open":
                events.append(("offer_superseded", {"offer_id": offer.offer_id}))
            elif offer.state == "declined":
                pass                           # already closed; correction event links it
            elif offer.state == "accepted":
                desc = s.lines.get(offer.descendant_line_ref)
                if desc is None:
                    raise DomainError(
                        "accepted offer without descendant line is unreachable (§4.1)")
                if desc.state in ("draft", "awaiting_factory", "factory_confirmed"):
                    subtype = ("double_supply" if desc.state == "factory_confirmed"
                               else "double_order_risk")
                    cid = s.new_id("CAS")
                    events.append(("case_opened", {
                        "case_id": cid, "type": "ReconciliationException",
                        "subtype": subtype,
                        "refs": {"line_id": line_id, "offer_id": offer.offer_id,
                                 "descendant_line_id": desc.line_id,
                                 "descendant_state": desc.state},
                        "evidence": {"corrected_confirmed_m2": str(qty),
                                     "descendant_m2": str(desc.selected_m2)},
                        "options": (["remove_draft_line"] if desc.state == "draft" else
                                    ["withdraw_descendant"] if desc.state == "awaiting_factory"
                                    else ["accept_over_supply", "correct_one_outcome"]),
                        "at": on.isoformat()}))
                # no_production / withdrawn descendants: no double supply — note only

    for etype, payload in events:
        _emit(s, etype, actor, on, payload, note=note)


def correct_quantity(s: State, line_id: str, *, corrected_m2: Decimal, note: str,
                     actor: str, on: date):
    line = _line(s, line_id)
    if line.state != "factory_confirmed":
        raise DomainError("quantity correction applies to factory_confirmed lines")
    if not corrected_m2 or corrected_m2 <= 0:
        raise DomainError("corrected_m2 must be positive")
    exp = next((e for e in s.expectations.values() if e.source_ref == line_id), None)
    exp_to_update = None
    if exp is not None and exp.open and not exp.evidence_superseded:
        exp_to_update = exp.exp_id            # else evidence wins / history-only
    _emit(s, "quantity_corrected", actor, on,
          {"line_id": line_id, "corrected_m2": corrected_m2,
           "expectation_id": exp_to_update, "note": note, "at": on.isoformat()},
          note=note)


# amendments (§4.1, §4.6e) ----------------------------------------------------

def create_amendment(s: State, *, product_id: str, requested_m2: Decimal,
                     production_ref: str, case_ref: Optional[str],
                     actor: str, on: date) -> str:
    if requested_m2 <= 0 or not is_valid_selection(requested_m2):
        raise DomainError("requested_m2 must be a positive multiple of 67.2 m²")
    if any(a.product_id == product_id and a.production_ref == production_ref
           and a.state == "open" for a in s.amendments.values()):
        raise DomainError(
            "at most one open AmendmentRequest per product + production_ref (V9)")
    aid = s.new_id("AMD")
    _emit(s, "amendment_created", actor, on,
          {"amendment_id": aid, "product_id": product_id,
           "requested_m2": requested_m2, "production_ref": production_ref,
           "case_ref": case_ref, "created_at": on})
    return aid


def record_amendment_outcome(s: State, amendment_id: str, outcome: str, *,
                             accepted_m2: Optional[Decimal] = None,
                             note: Optional[str] = None, actor: str, on: date):
    a = s.amendments.get(amendment_id)
    if a is None:
        raise DomainError(f"unknown amendment {amendment_id}")
    if a.state != "open":
        raise DomainError(f"amendment is {a.state}; outcomes record on open requests")
    if outcome not in ("accepted", "declined"):
        raise DomainError(f"unknown amendment outcome {outcome}")
    events = []
    if outcome == "accepted":
        qty = accepted_m2 if accepted_m2 is not None else a.requested_m2
        if qty <= 0:
            raise DomainError("accepted_m2 must be positive")
        eid = s.new_id("EXP")
        events.append(("amendment_outcome_recorded",
                       {"amendment_id": amendment_id, "outcome": "accepted",
                        "accepted_m2": qty, "note": note, "at": on.isoformat()}))
        events.append(("expectation_created",
                       {"exp_id": eid, "source_ref": amendment_id,
                        "product_id": a.product_id, "ask_date": a.created_at,
                        "confirmed_m2": qty}))
    else:
        events.append(("amendment_outcome_recorded",
                       {"amendment_id": amendment_id, "outcome": "declined",
                        "accepted_m2": None, "note": note, "at": on.isoformat()}))
    for etype, payload in events:
        _emit(s, etype, actor, on, payload, note=note)


def close_amendment_unfulfilled(s: State, amendment_id: str, *, reason: str,
                                actor: str, on: date):
    a = s.amendments.get(amendment_id)
    if a is None:
        raise DomainError(f"unknown amendment {amendment_id}")
    if a.state != "open":
        raise DomainError(f"amendment is {a.state}; only open requests auto-close")
    _emit(s, "amendment_closed_unfulfilled", actor, on,
          {"amendment_id": amendment_id, "reason": reason, "at": on.isoformat()},
          note=reason)


def correct_amendment_outcome(s: State, amendment_id: str, corrected_outcome: str, *,
                              accepted_m2: Optional[Decimal] = None, note: str,
                              actor: str, on: date):
    a = s.amendments.get(amendment_id)
    if a is None:
        raise DomainError(f"unknown amendment {amendment_id}")
    if a.state not in ("accepted", "declined", "closed_unfulfilled"):
        raise DomainError("correction requires a recorded effective outcome (§4.6e)")
    if corrected_outcome not in ("accepted", "declined", "open"):
        raise DomainError(f"unknown corrected outcome {corrected_outcome}")

    events = []
    if a.state == "accepted":
        if corrected_outcome == "accepted":
            raise DomainError("correction must change the outcome")
        exp = next((e for e in s.expectations.values()
                    if e.source_ref == amendment_id), None)
        if exp is not None:
            if exp.state == "arrived":
                cid = s.new_id("CAS")
                events.append(("case_opened", {
                    "case_id": cid, "type": "ReconciliationException",
                    "subtype": "arrived_contradiction",
                    "refs": {"amendment_id": amendment_id, "exp_id": exp.exp_id},
                    "evidence": {"corrected_outcome": corrected_outcome},
                    "options": ["re_correct_outcome", "keep_correction_annotate_arrival"],
                    "at": on.isoformat()}))
            elif exp.open:
                events.append(("expectation_cancelled",
                               {"exp_id": exp.exp_id,
                                "reason": f"amendment outcome corrected: {note}",
                                "at": on.isoformat()}))
        events.append(("amendment_outcome_corrected",
                       {"amendment_id": amendment_id,
                        "corrected_outcome": corrected_outcome,
                        "accepted_m2": None, "note": note, "at": on.isoformat()}))
    else:  # declined-in-error or late answer after closed_unfulfilled
        if corrected_outcome != "accepted":
            raise DomainError(
                "the only correction of declined/closed_unfulfilled is a late "
                "acceptance (§4.6e); a confirmed decline is a note event")
        qty = accepted_m2 if accepted_m2 is not None else a.requested_m2
        eid = s.new_id("EXP")
        events.append(("amendment_outcome_corrected",
                       {"amendment_id": amendment_id, "corrected_outcome": "accepted",
                        "accepted_m2": qty, "note": note, "at": on.isoformat()}))
        events.append(("expectation_created",
                       {"exp_id": eid, "source_ref": amendment_id,
                        "product_id": a.product_id, "ask_date": a.created_at,
                        "confirmed_m2": qty}))
    for etype, payload in events:
        _emit(s, etype, actor, on, payload, note=note)


def note_amendment_decline_confirmation(s: State, amendment_id: str, *, note: str,
                                        actor: str, on: date):
    a = s.amendments.get(amendment_id)
    if a is None:
        raise DomainError(f"unknown amendment {amendment_id}")
    if a.state != "closed_unfulfilled":
        raise DomainError("decline confirmation notes apply to closed_unfulfilled")
    _emit(s, "amendment_decline_confirmation_noted", actor, on,
          {"amendment_id": amendment_id, "note": note}, note=note)


# cases ------------------------------------------------------------------------

def open_case(s: State, *, type: str, subtype: str = "", refs: dict,
              evidence: dict, options: list, actor: str, on: date) -> str:
    cid = s.new_id("CAS")
    _emit(s, "case_opened", actor, on,
          {"case_id": cid, "type": type, "subtype": subtype, "refs": refs,
           "evidence": evidence, "options": options, "at": on.isoformat()})
    return cid


def resolve_case(s: State, case_id: str, *, resolution: dict, actor: str, on: date):
    c = s.cases.get(case_id)
    if c is None:
        raise DomainError(f"unknown case {case_id}")
    if c.state != "open":
        raise DomainError("case already resolved")
    _emit(s, "case_resolved", actor, on,
          {"case_id": case_id, "resolution": resolution, "at": on.isoformat()})
