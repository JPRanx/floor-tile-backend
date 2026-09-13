"""
Clean V1 §4.6a / §8 — the ONE authenticated transactional command boundary.

Every lifecycle mutation enters here. Each invocation, atomically and
all-or-nothing:
  1. resolves the actor SERVER-SIDE from the session token (never from the
     request payload — a payload carrying an actor claim is rejected);
  2. authorizes the command (Elicio-only administrative commands are denied
     to Ashley, other authenticated identities and unauthenticated callers
     BEFORE any event is appended);
  3. validates preconditions against current effective state (domain step-1
     rejection — nothing recorded on failure);
  4. appends the immutable event(s), updates the effective-state projection
     and creates/cancels spawned objects (domain layer);
  5. runs the §6.4a matching pass hook triggered by those changes.
Steps 3–5 run on a WORKING COPY of the state; the copy replaces the live
state only if every step succeeded — no partial mutation is observable.

In-memory repositories only (pre-persistence tranche). Exact transport is
an implementation-gate choice; this module is the contract.
"""

from __future__ import annotations

import copy
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

from . import domain as dm
from .config import PlanningConfig
from .domain import DomainError

D = Decimal


class AuthorizationError(Exception):
    """Denied before any event append (identity or role failure)."""


class CommandError(Exception):
    """Validation failure at the command boundary — nothing recorded."""


# §8.1/§8.2 — the administrative command set (D2: never reachable by Ashley)
ELICIO_ONLY_COMMANDS = frozenset({
    "RecordFactoryOutcome",
    "BulkRecordFactoryOutcomes",
    "CorrectFactoryOutcome",
    "ReverseWithdrawal",
    "RecordAmendmentOutcome",
    "CorrectAmendmentOutcome",
})

ASHLEY_COMMANDS = frozenset({
    "OpenMonthlyOrder", "AcceptSuggestion", "EditSelectedM2", "DeferProduct",
    "AddManualLine", "AcceptAllSuggestions", "AcceptCarryForward",
    "DeclineCarryForward", "SubmitMonthlyOrder", "WithdrawLine",
    "ResolveNeedsAshley", "ConfirmAmendmentIntent", "CloseMonthlyOrder",
})

# Case types whose resolution records a factory outcome/correction resolve
# via Elicio (§4.6a actor scope)
ADMIN_RESOLVED_CASE_TYPES = frozenset({"FactoryResponseOverdue"})

# Typed resolution options per case type (closed lists — never free-form)
TYPED_RESOLUTIONS = {
    "ProductMatchResolution": {"map", "discard"},
    "DecisionRequired": {"order_now", "amend_production", "accept_risk_with_note"},
    "ReconciliationException": {
        "reduce_effective", "keep_expectation", "cancel_expectation",
        "adjust_with_note", "remove_draft_line", "withdraw_descendant",
        "accept_over_supply", "correct_one_outcome", "manual_arrived",
        "re_correct_outcome", "keep_correction_annotate_arrival",
        "accept_evidence",
    },
    "AmendmentIntentConfirmation": {"confirm_intent", "dismiss"},
    "FactoryResponseOverdue": {"record_outcome", "extend_window", "withdraw"},
    "MissingEvidence": {"refresh_feed", "proceed_with_note"},
}


class InMemoryIdentityProvider:
    """Server-side session store: token → actor identity. The demo/local
    stand-in for real auth (real auth is behind a closed gate)."""

    def __init__(self, sessions: dict):
        self._sessions = dict(sessions)

    def resolve(self, token: Optional[str]) -> str:
        if not token or token not in self._sessions:
            raise AuthorizationError("unauthenticated: unknown or missing session")
        return self._sessions[token]


def _dec(value, field: str) -> Decimal:
    try:
        return D(str(value))
    except (InvalidOperation, TypeError):
        raise CommandError(f"{field} is not a valid decimal quantity")


class CommandBus:
    def __init__(self, *, state: dm.State, identity, config: PlanningConfig,
                 rematch_hook: Optional[Callable] = None,
                 suggestion_provider: Optional[Callable] = None):
        self.state = state
        self.identity = identity
        self.config = config
        # step-5 hook: called inside the transaction with (working_state, trigger)
        self.rematch_hook = rematch_hook or (lambda state, trigger: None)
        # C1/V7: server-side supplier of the CURRENT suggestion content for
        # (working_state, order_id, product_id) — the ONLY source of snapshot
        # content; request payloads can never supply or forge a snapshot
        self.suggestion_provider = suggestion_provider

    # -- C1: server-owned suggestion snapshots (§4.1, V7) --------------------

    def _server_suggestion_ref(self, s, order_id, product_id, *, actor, on,
                               fresh: bool) -> str:
        """Return the snapshot ref a decision MUST carry: a FRESH frozen copy
        of the current suggestion (accept), or the retained snapshot of the
        suggestion Ashley already decided from (edit/defer — §8.1 'suggestion
        snapshot retained'), else a newly frozen server-owned copy.
        Payload-supplied refs are ignored entirely. R1: this NEVER returns
        None — every suggestion-based decision either carries a valid
        server-owned snapshot or the whole command fails with zero mutation
        (`AddManualLine` is the one explicit no-suggestion path)."""
        prior = next((d.suggestion_snapshot_ref
                      for d in reversed(s.decisions)
                      if d.order_id == order_id and d.product_id == product_id
                      and d.suggestion_snapshot_ref in s.suggestions), None)
        if prior is not None and not fresh:
            return prior
        if self.suggestion_provider is None:
            raise CommandError(
                "no server-side suggestion provider is configured: "
                "suggestion-based decisions require a server-owned frozen "
                "snapshot (C1/V7) and never trust a client quantity")
        content = self.suggestion_provider(s, order_id, product_id)
        if content is None:
            raise CommandError(
                f"no current server suggestion exists for product "
                f"{product_id!r} in order {order_id!r} (unknown product or "
                "invalid order-product relation) — decision refused with "
                "nothing recorded (C1/V7)")
        return dm.record_suggestion_snapshot(
            s, order_id=order_id, product_id=product_id, content=content,
            actor=actor, on=on)

    # -- public surface -----------------------------------------------------

    def commands_available_to(self, actor: str) -> frozenset:
        if actor == "elicio":
            return ASHLEY_COMMANDS | ELICIO_ONLY_COMMANDS
        if actor == "ashley":
            return ASHLEY_COMMANDS
        return frozenset()

    def execute(self, command: str, params: dict, *, token: Optional[str],
                on: date) -> dict:
        # 1. server-derived identity — BEFORE anything else
        actor = self.identity.resolve(token)

        # 2. authorization — BEFORE any validation or event append
        self._authorize(command, actor, params)

        # payload can never carry/forge identity
        if any(k in params for k in ("actor", "actor_id", "identity", "role")):
            raise CommandError(
                "request payload must not carry actor identity (V8/§4.6a); "
                "identity is derived server-side from the session")

        handler = getattr(self, f"_cmd_{command}", None)
        if handler is None:
            raise CommandError(f"unknown command {command}")

        # 3–5. transactional: work on a deep copy, swap only on full success
        work = copy.deepcopy(self.state)
        decisions_before = len(self.state.decisions)
        try:
            result = handler(work, params, actor=actor, on=on)
        except DomainError as e:
            raise CommandError(str(e)) from e
        # FV1: boundary invariant — no public command path may record an
        # accept/edit/defer decision without a valid server-owned snapshot
        # (the explicit manual decision type, and defer-equivalents of
        # genuinely non-suggestion provenance, are the only exceptions)
        self._assert_snapshot_invariant(work, decisions_before)
        self.state = work
        return result or {}

    @staticmethod
    def _defer_provenance_is_non_suggestion(work: dm.State,
                                            d: "dm.AshleyOrderDecision") -> bool:
        """AC4: a snapshot-less defer is legitimate ONLY when its provenance
        is rooted in an ACCEPTED non-suggestion record in the authoritative
        event history — never merely because a preceding defer happened to
        be snapshot-less. The defer's own `decision_recorded` event names
        the line it removed (or `none`); the event that created or last
        governed that line determines the root:
        - `carry_forward_accepted` → carry-forward provenance (the
          acceptance event is the decision record) → legitimate;
        - `decision_recorded` with origin `manual` → manual provenance →
          legitimate;
        - any suggestion/edit-created line, or a defer that removed no
          line at all → NOT legitimate snapshot-less."""
        dec_ev = next((e for e in work.events
                       if e.type == "decision_recorded"
                       and e.payload.get("decision_id") == d.decision_id), None)
        if dec_ev is None:
            return False
        line_id = dec_ev.payload.get("line_id")
        if line_id is None:
            return False              # defer with no line: DeferProduct always
                                      # carries a server snapshot
        for e in reversed(work.events[:dec_ev.seq - 1]):
            if e.type == "carry_forward_accepted" \
                    and e.payload.get("line_id") == line_id:
                return True
            if e.type == "decision_recorded" \
                    and e.payload.get("line_id") == line_id \
                    and e.payload.get("line_effect") in ("created", "updated"):
                return e.payload.get("origin") == "manual"
        return False

    def _assert_snapshot_invariant(self, work: dm.State, start: int):
        """R1/FV1/AC4 backstop, checked before the working copy is swapped
        in: every suggestion-based decision this command recorded resolves
        to a server-owned frozen snapshot. `manual_add` never carries one;
        a `defer` may be snapshot-less ONLY when its provenance is rooted
        in an accepted non-suggestion record (manual / carry-forward) in
        the event history — see `_defer_provenance_is_non_suggestion`.
        Anything else fails the whole command atomically."""
        for idx in range(start, len(work.decisions)):
            d = work.decisions[idx]
            if d.action == "manual_add":
                if d.suggestion_snapshot_ref is None:
                    continue
                raise CommandError(
                    "manual_add is the explicit no-suggestion path and must "
                    "not carry a suggestion snapshot (C1/FV1)")
            ref = d.suggestion_snapshot_ref
            if ref is not None and ref in work.suggestions:
                continue
            if d.action == "defer" and ref is None \
                    and self._defer_provenance_is_non_suggestion(work, d):
                continue
            raise CommandError(
                f"decision {d.decision_id} ({d.action}) would be recorded "
                f"without a valid server-owned suggestion snapshot "
                f"(ref={ref!r}) — refused with zero mutation (C1/V7/FV1/AC4)")

    # -- authorization ------------------------------------------------------

    def _authorize(self, command: str, actor: str, params: dict):
        known = ASHLEY_COMMANDS | ELICIO_ONLY_COMMANDS
        if command not in known:
            # unknown commands fail closed for everyone but reach validation
            # for a precise error only for operator roles
            if actor not in ("ashley", "elicio"):
                raise AuthorizationError(f"actor {actor} has no command access")
            return
        if command in ELICIO_ONLY_COMMANDS:
            if actor != "elicio":
                raise AuthorizationError(
                    f"{command} is an Elicio-only transactional administrative "
                    f"command (§4.6a/D2); denied for {actor}")
            return
        # Ashley-surface commands: Ashley or Elicio (V1 single operator + admin)
        if actor not in ("ashley", "elicio"):
            raise AuthorizationError(
                f"actor {actor} is not an authorized operator identity")
        # dynamic rules
        if command == "WithdrawLine":
            line = self.state.lines.get(params.get("line_id"))
            if line is not None and line.state == "awaiting_factory" and actor != "elicio":
                raise AuthorizationError(
                    "submitted-line withdrawal is Elicio-only (§4.6c/§8.1)")
        if command == "ResolveNeedsAshley":
            case = self.state.cases.get(params.get("case_id"))
            if case is not None and case.type in ADMIN_RESOLVED_CASE_TYPES \
                    and actor != "elicio":
                raise AuthorizationError(
                    f"{case.type} resolutions are administrative (Elicio) — §10.5")

    # -- Ashley decision commands --------------------------------------------

    def _cmd_OpenMonthlyOrder(self, s, p, *, actor, on):
        existing = next((o for o in s.orders.values()
                         if o.cycle_month == p["cycle_month"]), None)
        if existing:
            raise CommandError(f"order for {p['cycle_month']} already exists (V3) — resume it")
        oid = dm.open_order(s, cycle_month=p["cycle_month"], on=on, actor=actor)
        return {"order_id": oid}

    def _cmd_AcceptSuggestion(self, s, p, *, actor, on):
        # server-side snapshot FIRST, same transaction (C1/V7). The accepted
        # quantity is ALWAYS the server-computed suggestion — a payload
        # `suggested_m2` is never a fallback (R1); no snapshot → no decision.
        ref = self._server_suggestion_ref(s, p["order_id"], p["product_id"],
                                          actor=actor, on=on, fresh=True)
        m2 = s.suggestions[ref].suggested_m2
        if m2 <= 0:
            raise CommandError(
                f"the current server suggestion for {p['product_id']!r} is "
                f"{m2} m² — there is nothing valid to accept (use "
                "AddManualLine for an explicit manual quantity)")
        dm.draft_decision(s, p["order_id"], p["product_id"], action="accept",
                          selected_m2=m2, actor=actor, on=on,
                          suggestion_snapshot_ref=ref)

    def _cmd_EditSelectedM2(self, s, p, *, actor, on):
        m2 = _dec(p["m2"], "m2")
        ref = self._server_suggestion_ref(s, p["order_id"], p["product_id"],
                                          actor=actor, on=on, fresh=False)
        dm.draft_decision(s, p["order_id"], p["product_id"], action="edit",
                          selected_m2=m2, actor=actor, on=on,
                          suggestion_snapshot_ref=ref)

    def _cmd_DeferProduct(self, s, p, *, actor, on):
        # defer preserves the suggestion snapshot Ashley saw (§4.1/rev 3.1)
        ref = self._server_suggestion_ref(s, p["order_id"], p["product_id"],
                                          actor=actor, on=on, fresh=False)
        dm.draft_decision(s, p["order_id"], p["product_id"], action="defer",
                          selected_m2=None, actor=actor, on=on,
                          suggestion_snapshot_ref=ref)

    def _cmd_AddManualLine(self, s, p, *, actor, on):
        m2 = _dec(p["m2"], "m2")
        dm.draft_decision(s, p["order_id"], p["product_id"], action="manual_add",
                          selected_m2=m2, actor=actor, on=on, note=p.get("note"))

    def _cmd_AcceptAllSuggestions(self, s, p, *, actor, on):
        # convenience is allowed, but one decision record per row (§8.2),
        # each with its own server-side suggestion snapshot (C1/V7). R1:
        # every item must be individually valid; one invalid item fails the
        # whole command with zero mutation (the §4.6a working-copy swap).
        for item in p["items"]:
            ref = self._server_suggestion_ref(s, p["order_id"], item["product_id"],
                                              actor=actor, on=on, fresh=True)
            m2 = s.suggestions[ref].suggested_m2
            if m2 <= 0:
                raise CommandError(
                    f"the current server suggestion for "
                    f"{item['product_id']!r} is {m2} m² — nothing valid to "
                    "accept; bulk accept refused with nothing recorded")
            dm.draft_decision(s, p["order_id"], item["product_id"], action="accept",
                              selected_m2=m2, actor=actor, on=on,
                              suggestion_snapshot_ref=ref)

    def _cmd_AcceptCarryForward(self, s, p, *, actor, on):
        lid = dm.accept_carry_forward(s, p["offer_id"], p["order_id"],
                                      actor=actor, on=on)
        return {"line_id": lid}

    def _cmd_DeclineCarryForward(self, s, p, *, actor, on):
        dm.decline_carry_forward(s, p["offer_id"], actor=actor, on=on)

    def _cmd_SubmitMonthlyOrder(self, s, p, *, actor, on):
        dm.submit_order(s, p["order_id"], on=on, actor=actor)

    def _cmd_CloseMonthlyOrder(self, s, p, *, actor, on):
        dm.close_order(s, p["order_id"], actor=actor, on=on)

    def _cmd_WithdrawLine(self, s, p, *, actor, on):
        dm.withdraw_line(s, p["line_id"], note=p.get("note", ""), actor=actor, on=on)
        self.rematch_hook(s, {"reason": "pending_released", "line_id": p["line_id"]})

    # -- Elicio administrative commands (§4.6a) --------------------------------

    def _cmd_RecordFactoryOutcome(self, s, p, *, actor, on):
        outcome = p.get("outcome")
        if outcome not in ("factory_confirmed", "no_production"):
            raise CommandError(f"unknown outcome {outcome}")
        qty = _dec(p["confirmed_m2"], "confirmed_m2") if p.get("confirmed_m2") else None
        dm.record_outcome(s, p["line_id"], outcome, confirmed_m2=qty,
                          note=p.get("note"), actor=actor, on=on)
        if outcome == "factory_confirmed":
            self.rematch_hook(s, {"reason": "expectation_created",
                                  "line_id": p["line_id"]})
        else:
            self.rematch_hook(s, {"reason": "pending_released",
                                  "line_id": p["line_id"]})

    def _cmd_BulkRecordFactoryOutcomes(self, s, p, *, actor, on):
        for item in p["outcomes"]:
            qty = _dec(item["confirmed_m2"], "confirmed_m2") \
                if item.get("confirmed_m2") else None
            dm.record_outcome(s, item["line_id"], item["outcome"], confirmed_m2=qty,
                              note=item.get("note"), actor=actor, on=on)
            # per-line hook: V6 evaluates each recorded outcome even in bulk
            reason = ("expectation_created" if item["outcome"] == "factory_confirmed"
                      else "pending_released")
            self.rematch_hook(s, {"reason": reason, "line_id": item["line_id"],
                                  "bulk": True})

    def _cmd_CorrectFactoryOutcome(self, s, p, *, actor, on):
        line = s.lines.get(p["line_id"])
        if line is None:
            raise CommandError(f"unknown line {p['line_id']}")
        corrected = p.get("corrected_outcome")
        qty = _dec(p["corrected_m2"], "corrected_m2") if p.get("corrected_m2") else None
        if corrected == line.state == "factory_confirmed" and qty is not None:
            # confirmed_m2 change, outcome unchanged (§4.6d quantity path)
            dm.correct_quantity(s, p["line_id"], corrected_m2=qty,
                                note=p.get("note", ""), actor=actor, on=on)
            self.rematch_hook(s, {"reason": "effective_changed",
                                  "line_id": p["line_id"]})
            return
        dm.correct_outcome(s, p["line_id"], corrected, corrected_m2=qty,
                           note=p.get("note", ""), actor=actor, on=on)
        self.rematch_hook(s, {"reason": "expectation_corrected",
                              "line_id": p["line_id"]})

    def _cmd_ReverseWithdrawal(self, s, p, *, actor, on):
        dm.reverse_withdrawal(s, p["line_id"], note=p.get("note", ""),
                              actor=actor, on=on)
        self.rematch_hook(s, {"reason": "pending_restored", "line_id": p["line_id"]})

    def _cmd_RecordAmendmentOutcome(self, s, p, *, actor, on):
        qty = _dec(p["accepted_m2"], "accepted_m2") if p.get("accepted_m2") else None
        dm.record_amendment_outcome(s, p["amendment_id"], p["outcome"],
                                    accepted_m2=qty, note=p.get("note"),
                                    actor=actor, on=on)
        if p["outcome"] == "accepted":
            self.rematch_hook(s, {"reason": "expectation_created",
                                  "amendment_id": p["amendment_id"]})

    def _cmd_CorrectAmendmentOutcome(self, s, p, *, actor, on):
        qty = _dec(p["accepted_m2"], "accepted_m2") if p.get("accepted_m2") else None
        dm.correct_amendment_outcome(s, p["amendment_id"], p["corrected_outcome"],
                                     accepted_m2=qty, note=p.get("note", ""),
                                     actor=actor, on=on)
        self.rematch_hook(s, {"reason": "expectation_corrected",
                              "amendment_id": p["amendment_id"]})

    # -- amendment intent (Ashley) ---------------------------------------------

    def _cmd_ConfirmAmendmentIntent(self, s, p, *, actor, on):
        m2 = _dec(p["additional_m2"], "additional_m2")
        draft = next((o for o in s.orders.values() if o.state == "draft"), None)
        if draft is not None:
            # a draft order is open: add/update the line — no amendment object
            existing = next((l for l in s.lines.values()
                             if l.order_id == draft.order_id
                             and l.product_id == p["product_id"]), None)
            if existing is not None:
                # an edit is a suggestion-based decision: retain/create the
                # server snapshot like any other edit (R1/C1)
                ref = self._server_suggestion_ref(
                    s, draft.order_id, p["product_id"],
                    actor=actor, on=on, fresh=False)
                dm.draft_decision(s, draft.order_id, p["product_id"], action="edit",
                                  selected_m2=existing.selected_m2 + m2,
                                  actor=actor, on=on, note="amendment intent",
                                  suggestion_snapshot_ref=ref)
            else:
                dm.draft_decision(s, draft.order_id, p["product_id"],
                                  action="manual_add", selected_m2=m2,
                                  actor=actor, on=on, note="amendment intent")
            return {"amendment_id": None, "added_to_order": draft.order_id}
        aid = dm.create_amendment(s, product_id=p["product_id"], requested_m2=m2,
                                  production_ref=p["production_ref"],
                                  case_ref=p.get("case_ref"), actor=actor, on=on)
        return {"amendment_id": aid}

    # -- typed exception resolution (§10.5/§10.6) -------------------------------

    def _cmd_ResolveNeedsAshley(self, s, p, *, actor, on):
        case = s.cases.get(p["case_id"])
        if case is None:
            raise CommandError(f"unknown case {p['case_id']}")
        if case.state != "open":
            raise CommandError("case already resolved")
        resolution = p.get("resolution") or {}
        action = resolution.get("action")
        allowed = TYPED_RESOLUTIONS.get(case.type, set())
        if action not in allowed:
            raise CommandError(
                f"resolution '{action}' is not a typed option for {case.type} "
                f"(closed list: {sorted(allowed)}) — free-form outcome entry "
                "does not exist (D2)")

        # typed resolutions execute through the same transactional machinery
        if action == "reduce_effective":
            exp_id = resolution.get("exp_id") or case.refs.get("exp_id")
            dm.supersede_expectation(s, exp_id,
                                     new_effective_m2=_dec(resolution["m2"], "m2"),
                                     evidence_ref=None,
                                     note=f"case {case.case_id} resolution",
                                     actor=actor, on=on)
            self.rematch_hook(s, {"reason": "effective_changed", "exp_id": exp_id})
        elif action == "cancel_expectation":
            exp_id = resolution.get("exp_id") or case.refs.get("exp_id")
            dm.cancel_expectation(s, exp_id,
                                  reason=f"case {case.case_id} resolution",
                                  actor=actor, on=on)
            self.rematch_hook(s, {"reason": "expectation_cancelled", "exp_id": exp_id})
        elif action == "manual_arrived":
            exp_id = resolution.get("exp_id") or case.refs.get("exp_id")
            exp = s.expectations.get(exp_id)
            if exp is None:
                raise CommandError(f"unknown expectation {exp_id}")
            dm.attribute_arrival_to_expectation(s, exp_id, m2=exp.remaining_m2,
                                                actor=actor, on=on)
        # keep_expectation / accept_* / proceed_with_note / dismiss / notes:
        # recorded on the case only — evidence and decision kept in history

        dm.resolve_case(s, p["case_id"],
                        resolution={"action": action, **{k: str(v) for k, v in
                                    resolution.items() if k != "action"}},
                        actor=actor, on=on)
