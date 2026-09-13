"""
Reconciled V1 — §7 command boundary for the sailing engine.

Preserves the AC-hardened discipline of the monthly-order bus
[PRESERVED-3.1 §4.6a]: server-derived actor identity (payload identity keys
rejected), authorization denied BEFORE any event append, and a transactional
deep-copy-and-swap so a failing command observably mutates nothing.

Handlers live on the engine (`_cmd_<Command>`) and operate ONLY on the
working state passed to them; the engine's reconcile pass runs inside the
same transaction, so evidence effects, implication generation, and plan
recomputation land atomically with the command that triggered them.
"""
from __future__ import annotations

import copy
from datetime import date
from typing import Optional

from .domain import DomainError


class AuthorizationError(Exception):
    """Denied before any event is appended."""


class CommandError(Exception):
    """Validation/domain failure — transaction rolled back."""


ASHLEY_COMMANDS = frozenset({
    "RecordSailing", "ImportSailingCalendarText", "SetSailingDecision",
    "PursueExceptionalSailing", "OpenShipmentPlan", "AcceptSuggestion",
    "EditSelectedM2", "EditSelectedM2Batch", "PostponeProduct", "AddManualLine",
    "AcceptAllSuggestions", "OverrideBLSplit", "FinalizeSailingPlan",
    "RequestReallocation", "ConfirmReallocationIntent", "CloseSailingPlan",
    "AcceptImplicationRisk",
    "RecordSiesaOrderReference", "RecordBookingConfirmation",
    "RecordBlDeparture",
    "LoadSiesaSnapshot", "LoadWarehouseSnapshot", "LoadSalesSnapshot",
    "LoadTransitSnapshot", "LoadCommittedOrders", "LoadProductionPlanning",
    "RunChecks",
    "OpenProductionOrder", "EditProductionOrderBatch",
    "FinalizeProductionOrder", "RecordProductionOrderReference",
    "ReopenSailingPlan", "ReopenProductionOrder",
    "OpenOrderAmendment", "EditOrderAmendmentBatch",
    "FinalizeOrderAmendment", "RecordOrderAmendmentReference",
})

ELICIO_ONLY_COMMANDS = frozenset({
    "CorrectObservedCommitment", "CorrectExpectation",
})

# [AB6] Ashley resolves her own cases; Elicio resolves ONLY other-owner
# (owner-routed / administrative) cases — the per-case owner check lives in
# the engine's ResolveImplication handler, before any event append.
SHARED_COMMANDS = frozenset({"ResolveImplication"})

_IDENTITY_KEYS = ("actor", "actor_id", "identity", "role")

# [AB7] Commands whose effects are EVIDENCE or RESOLUTION changes: when one
# of these changes an open draft plan's derived view (recommendations,
# totals, containers, BL proposal), a PlanRecompute history record is
# appended in the same transaction. Ashley's own planning decisions are
# decision history, not recomputation.
RECOMPUTE_TRIGGERS = frozenset({
    "LoadSiesaSnapshot", "LoadWarehouseSnapshot", "LoadSalesSnapshot",
    "LoadTransitSnapshot", "LoadCommittedOrders", "LoadProductionPlanning",
    # the sailing calendar is an evidence feed too (§3.1) — new/changed
    # sailings and use/watch/skip decisions move every open plan's
    # next-arrival window (B4 review finding — AB7)
    "RecordSailing", "ImportSailingCalendarText", "SetSailingDecision",
    "ResolveImplication", "AcceptImplicationRisk", "RunChecks",
    "CorrectObservedCommitment", "CorrectExpectation",
    "RecordBookingConfirmation", "RecordBlDeparture",
    "RecordSiesaOrderReference",
})


class InMemoryIdentityProvider:
    """Server-side token → actor resolution (synthetic local sessions)."""

    def __init__(self, sessions: dict):
        self._sessions = dict(sessions)

    def resolve(self, token: Optional[str]) -> str:
        if token is None or token not in self._sessions:
            raise AuthorizationError(
                "unknown or missing session token — identity is derived "
                "server-side and unauthenticated calls are denied before "
                "any event is appended")
        return self._sessions[token]

    def token_for(self, actor: str) -> Optional[str]:
        for tok, a in self._sessions.items():
            if a == actor:
                return tok
        return None


class SailingCommandBus:
    def __init__(self, *, state, identity, engine):
        self.state = state
        self.identity = identity
        self.engine = engine

    def commands_available_to(self, actor: str) -> frozenset:
        # [AB6] Elicio does NOT inherit the Ashley planning command set:
        # ordinary Ashley command authorization is unavailable to the
        # administrative identity; corrections and owner-routed case
        # resolution are its whole surface.
        if actor == "elicio":
            return ELICIO_ONLY_COMMANDS | SHARED_COMMANDS
        if actor == "ashley":
            return ASHLEY_COMMANDS | SHARED_COMMANDS
        return frozenset()

    def authorize_implication_resolution(self, actor: str,
                                         implication_id) -> None:
        """Fail closed without disclosing implication/reference validity."""
        if not isinstance(implication_id, str) or not implication_id:
            raise AuthorizationError(
                "implication resolution unavailable for authenticated actor")
        implication = self.state.implications.get(implication_id)
        if implication is None:
            raise AuthorizationError(
                "implication resolution unavailable for authenticated actor")
        try:
            # Reuse the engine's accepted AB6 owner authority rather than
            # introducing a second owner/resolver table at the HTTP boundary.
            self.engine._authorize_resolution(implication, actor)
        except DomainError as exc:
            raise AuthorizationError(
                "implication resolution unavailable for authenticated actor"
            ) from exc

    def _authorize(self, command: str, actor: str):
        if command in ELICIO_ONLY_COMMANDS:
            if actor != "elicio":
                raise AuthorizationError(
                    f"{command} is an Elicio-only administrative correction "
                    f"[D6 owner routing]; denied for {actor!r} before any "
                    "event append")
            return
        if command in SHARED_COMMANDS:
            if actor not in ("ashley", "elicio"):
                raise AuthorizationError(
                    f"{command} denied for {actor!r} before any event append")
            return
        if command in ASHLEY_COMMANDS:
            if actor != "ashley":
                raise AuthorizationError(
                    f"{command} is an Ashley planning/entry command; the "
                    f"administrative identity does not inherit it [AB6] — "
                    f"denied for {actor!r} before any event append")
            return
        raise CommandError(f"unknown command {command}")

    def execute(self, command: str, params: dict, *, token: Optional[str],
                on: date) -> dict:
        # 1. server-derived identity — BEFORE anything else
        actor = self.identity.resolve(token)
        # 2. authorization — BEFORE any validation or event append
        self._authorize(command, actor)
        if any(k in params for k in _IDENTITY_KEYS):
            raise CommandError(
                "request payload must not carry actor identity (§7.2); "
                "identity is derived server-side from the session")
        handler = getattr(self.engine, f"_cmd_{command}", None)
        if handler is None:
            raise CommandError(f"unknown command {command}")
        # 3–5. transactional: work on a deep copy, swap only on full success
        work = copy.deepcopy(self.state)
        try:
            before_views = {}
            if command in RECOMPUTE_TRIGGERS:
                before_views = {
                    pid: self.engine.plan_view(pid, self.state)
                    for pid, p in self.state.plans.items()
                    if p.lifecycle in ("draft", "finalized")}
            result = handler(work, dict(params), actor=actor, on=on)
            self.engine.reconcile(work, trigger=command, on=on)
            # [AB7/D7] evidence/resolution changes append before/after
            # recompute history for every open draft plan they changed —
            # inside the SAME transaction, without touching Ashley's
            # recorded selections.
            for pid, before in before_views.items():
                plan = work.plans.get(pid)
                if plan is None or plan.lifecycle != "draft":
                    continue
                after = self.engine.plan_view(pid, work)
                if after != before:
                    from . import sailing_domain as sd
                    sd.record_recompute(work, plan_id=pid, trigger=command,
                                        before=before, after=after,
                                        actor=actor, on=on)
        except DomainError as e:
            raise CommandError(str(e)) from e
        except ValueError as e:
            raise CommandError(str(e)) from e
        self.state = work
        return result or {}
