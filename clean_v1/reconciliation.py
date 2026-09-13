"""
Clean V1 §10 — reconciliation engine and proactive exceptions, plus the
in-memory Engine that wires facts (cumulative snapshot feeds), the §6.4a
matching pass, the domain event log and the §4.6a command boundary together.

Everything is in-memory and synthetic (pre-persistence tranche): feeds are
loaded from fixture rows, never from real parsers or a database.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from typing import Optional

from lib.constants import M2_PER_PALLET

from . import domain as dm
from . import planning_math as pm
from .commands import CommandBus, InMemoryIdentityProvider
from .config import PlanningConfig
from .matching import Observation, ExpectationView, MatchResult, match

D = Decimal
ZERO = D("0")
CRITICAL_FEEDS = ("warehouse", "sales", "in_transit")


# ── §10.2 predicates (pure) ────────────────────────────────────────────────

def variance_is_tolerated(diff: Decimal, expected: Decimal,
                          config: PlanningConfig) -> bool:
    """Tolerated ⇔ ≤ 1 pallet AND ≤ MATERIAL_VARIANCE_PCT of expected."""
    diff = abs(Decimal(diff))
    if diff == 0:
        return True
    within_pallet = diff <= M2_PER_PALLET
    within_pct = expected > 0 and diff <= config.material_variance_pct * expected
    return within_pallet and within_pct


def variance_is_material(diff: Decimal, expected: Decimal,
                         config: PlanningConfig) -> bool:
    """Exact complement of tolerated — no variance can fall in a gap."""
    return not variance_is_tolerated(diff, expected, config)


def production_shortfall_is_exception(*, supply_now: Decimal,
                                      daily_velocity: Decimal,
                                      buffer_m2: Decimal,
                                      expected_effective_m2: Decimal,
                                      produced_m2: Decimal,
                                      arrival_date: date, today: date,
                                      horizon_end: date,
                                      customer_commitment_uncovered: bool,
                                      config: PlanningConfig,
                                      other_supply=()) -> bool:
    """THE single production-shortfall predicate (§10.2) — both branches must
    hold: quantitative materiality AND a NEWLY created supply consequence.
    AC2: both §6.7 curves are COMPLETE — they carry the same other accepted
    dated supply (`other_supply`, the caller's candidate-excluded
    `_dated_supply_for` view), with the candidate inserted as its expected
    quantity in the before curve and its produced quantity in the after
    curve at the candidate arrival date. Supply that already protects (or
    already fails) both complete curves is never attributed to the
    candidate shortfall."""
    shortfall = expected_effective_m2 - produced_m2
    if shortfall <= 0:
        return False
    if variance_is_tolerated(shortfall, expected_effective_m2, config):
        return False                    # inside tolerance: never, regardless
    if customer_commitment_uncovered:
        return True
    base = sorted((d_, Decimal(m)) for d_, m in other_supply)
    proj_expected = pm.project_buffer_protection(
        supply_now=supply_now, daily_velocity=daily_velocity,
        buffer_m2=buffer_m2,
        events=sorted(base + [(arrival_date, expected_effective_m2)]),
        today=today, horizon_end=horizon_end)
    proj_produced = pm.project_buffer_protection(
        supply_now=supply_now, daily_velocity=daily_velocity,
        buffer_m2=buffer_m2,
        events=sorted(base + [(arrival_date, produced_m2)]),
        today=today, horizon_end=horizon_end)
    exp_min = dict(proj_expected.minima)
    for d_, produced_min in proj_produced.minima:
        if produced_min < buffer_m2 and exp_min.get(d_, buffer_m2) >= buffer_m2:
            return True                 # the shortfall CREATES the breach
    return False


# ── the in-memory engine ────────────────────────────────────────────────────

TOKENS = {"tok-ashley": "ashley", "tok-elicio": "elicio"}


@dataclass(frozen=True)
class ProductBuckets:
    supply_now: Decimal
    supply_expected: Decimal
    supply_moving: Decimal


class Engine:
    """Facts + matching + domain + commands, in memory, synthetic only."""

    def __init__(self, *, config: PlanningConfig, today: date, products: list,
                 velocities: dict, peak_velocities: dict):
        self.config = config
        self.today = today
        self.products = {p["id"]: p for p in products}
        self.velocities = dict(velocities)
        self.peaks = dict(peak_velocities or {})
        self.bus = CommandBus(state=dm.State.empty(),
                              identity=InMemoryIdentityProvider(TOKENS),
                              config=config,
                              rematch_hook=self._on_domain_trigger,
                              suggestion_provider=self._suggestion_snapshot_content)
        self.feeds: dict = {}                 # feed → {"as_of", "snapshot_id", …}
        self.production_planning: list = []   # soft context rows
        self.committed_orders: Optional[dict] = None    # U10 optional feed
        self._observations: list[Observation] = []
        self._snap = itertools.count(1)
        self.match_result: MatchResult = MatchResult((), {}, ())

        runtime_tiers = pm.classify_tiers(list(self.products), self.velocities)
        self.tiers = {pid: (p.get("tier") or runtime_tiers[pid])
                      for pid, p in self.products.items()}

    # -- basics ---------------------------------------------------------------

    @property
    def state(self) -> dm.State:
        return self.bus.state

    def velocity(self, pid) -> Decimal:
        return self.velocities.get(pid, ZERO)

    def buffer_m2(self, pid) -> Decimal:
        return pm.buffer_for(self.tiers.get(pid, "C"), self.velocity(pid),
                             peak_velocity=self.peaks.get(pid))

    def warehouse_value(self, pid) -> Decimal:
        wh = self.feeds.get("warehouse")
        return wh["values"].get(pid, ZERO) if wh else ZERO

    def pending_commitment(self, pid, state: Optional[dm.State] = None) -> Decimal:
        state = state if state is not None else self.state
        lines = sum((l.selected_m2 for l in state.lines.values()
                     if l.product_id == pid and l.state == "awaiting_factory"), ZERO)
        amendments = sum((a.requested_m2 for a in state.amendments.values()
                          if a.product_id == pid and a.state == "open"), ZERO)
        return lines + amendments

    def _input_snapshot_refs(self) -> dict:
        """Exact evidence-input provenance for a frozen suggestion snapshot."""
        refs = {}
        for feed, info in self.feeds.items():
            if not info:
                continue
            refs[feed] = {"snapshot_id": info.get("snapshot_id"),
                          "as_of": info["as_of"].isoformat()
                          if info.get("as_of") else None}
        return refs

    def _suggestion_snapshot_content(self, state: dm.State, order_id: str,
                                     product_id: str):
        """C1/V7 server-side snapshot content: EXACTLY the §6.4b/§6.5/§6.8
        computation the workspace row displays, evaluated against the
        transaction's working state. The only source of snapshot content —
        request payloads can never supply or forge it."""
        from .read_model import suggestion_view
        if product_id not in self.products:
            return None                     # unknown product: nothing to freeze
        if order_id not in state.orders:
            return None                     # invalid order-product relation
        sv = suggestion_view(self, product_id, state=state)
        return {
            "true_need_m2": sv["need"].true_need_m2,
            "suggested_m2": sv["suggested"],
            "derived_pallets": pm.derived_pallets(sv["suggested"]),
            "explanation": sv["explanation"],
            "computed_at": self.today.isoformat(),
            "input_snapshot_refs": self._input_snapshot_refs(),
        }

    def open_cases(self, type: Optional[str] = None,
                   state: Optional[dm.State] = None) -> list:
        state = state if state is not None else self.state
        cases = [c for c in state.cases.values() if c.state == "open"]
        return [c for c in cases if type is None or c.type == type]

    def _has_open_case(self, type: str, state: Optional[dm.State] = None,
                       **refs) -> bool:
        # review finding N1/N2: inside a §4.6a transaction the dedup MUST
        # consult the same (working) state the cases are opened on —
        # reading live state both breaks dedup and leaks across the
        # transaction boundary
        return any(all(c.refs.get(k) == v for k, v in refs.items())
                   for c in self.open_cases(type, state=state))

    # -- commands -------------------------------------------------------------

    def ashley(self, command: str, params: dict) -> dict:
        result = self.bus.execute(command, params, token="tok-ashley", on=self.today)
        self._post_command(command, params)
        return result

    def elicio(self, command: str, params: dict) -> dict:
        result = self.bus.execute(command, params, token="tok-elicio", on=self.today)
        self._post_command(command, params)
        return result

    def _post_command(self, command: str, params: dict):
        if command == "ResolveNeedsAshley":
            resolution = params.get("resolution") or {}
            action = resolution.get("action")
            case = self.state.cases.get(params["case_id"])
            raw = case.refs.get("raw_reference") if case else None
            if action == "map" and raw is not None:
                pid = resolution["product_id"]
                self._observations = [
                    replace(o, product_id=pid, confident_match=True)
                    if o.raw_reference == raw and not o.confident_match else o
                    for o in self._observations]
                self._reconcile(self.state)
            elif action == "discard" and raw is not None:
                self._observations = [
                    o for o in self._observations
                    if not (o.raw_reference == raw and not o.confident_match)]
                self._reconcile(self.state)
        self.match_result = self._compute_match(self.state)

    def open_cycle(self, cycle_month: str, *, on: date) -> str:
        self.today = on
        r = self.ashley("OpenMonthlyOrder", {"cycle_month": cycle_month})
        # §4.1/§8.1: any still-open amendment auto-closes at next cycle open
        for a in list(self.state.amendments.values()):
            if a.state == "open":
                dm.close_amendment_unfulfilled(
                    self.state, a.amendment_id,
                    reason="auto-closed: next cycle opened with no factory answer",
                    actor="system", on=on)
        # C6/S9: cycle open re-checks evidence freshness
        self._check_missing_evidence()
        return r["order_id"]

    # -- §4.6a step-5 hook (runs INSIDE the command transaction) ---------------

    def _on_domain_trigger(self, working_state: dm.State, trigger: dict):
        self._reconcile(working_state)
        if trigger.get("reason") == "expectation_created" and trigger.get("line_id"):
            # V6: a materially different confirmed_m2 raises the dedicated
            # decision-consequence implication in the SAME transaction
            self._check_v6_decision_consequence(working_state, trigger["line_id"])
        if trigger.get("reason") == "pending_released":
            # S6: a no_production/withdrawal removes the planned response —
            # the buffer-risk check runs immediately, inside the transaction
            self._check_decision_required(state=working_state)

    # -- V6 dedicated decision-consequence implication (accepted spec V6) ------

    def _check_v6_decision_consequence(self, state: dm.State, line_id: str):
        line = state.lines.get(line_id)
        if line is None or line.state != "factory_confirmed" \
                or line.confirmed_m2 is None:
            return
        selected, confirmed = line.selected_m2, line.confirmed_m2
        diff = abs(confirmed - selected)
        if not variance_is_material(diff, selected, self.config):
            return                              # V6 fires on MATERIAL only
        pid = line.product_id
        exp = next((e for e in state.expectations.values()
                    if e.source_ref == line_id), None)
        direction = "down" if confirmed < selected else "up"
        velocity = self.velocity(pid)
        buffer_m2 = self.buffer_m2(pid)
        horizon_end = self._next_replenishment()
        arrival = (self._arrival_date(exp, state) if exp is not None
                   else pm.expected_arrival_for_proposed_order(
                       self.today, self._voyage_days()))
        base = self._dated_supply_for(pid, state,
                                      exclude_exp=exp.exp_id if exp else None)

        proj_sel = proj_conf = None
        new_breach_dates: list = []
        stockout_new = False
        if velocity > 0:
            proj_sel = pm.project_buffer_protection(
                supply_now=self.warehouse_value(pid), daily_velocity=velocity,
                buffer_m2=buffer_m2, events=sorted(base + [(arrival, selected)]),
                today=self.today, horizon_end=horizon_end)
            proj_conf = pm.project_buffer_protection(
                supply_now=self.warehouse_value(pid), daily_velocity=velocity,
                buffer_m2=buffer_m2, events=sorted(base + [(arrival, confirmed)]),
                today=self.today, horizon_end=horizon_end)
            sel_min = dict(proj_sel.minima)
            new_breach_dates = [d for d, m in proj_conf.minima
                                if m < buffer_m2
                                and sel_min.get(d, buffer_m2) >= buffer_m2]
            stockout_new = (proj_conf.outcome == "projected_stockout"
                            and proj_sel.outcome != "projected_stockout")
        # R3: §6.7-dated, per-commitment comparison — supply counts only for
        # commitments due on/after its arrival; already-lost-under-both is
        # not a new consequence of the factory variance. FV3: all OTHER
        # accepted dated supply (open expectations + unmatched transit,
        # candidate excluded) counts toward each due date.
        commitment_new = self._commitment_newly_uncovered(
            pid, qty_before=selected, arrival_before=arrival,
            qty_after=confirmed, arrival_after=arrival,
            other_supply=base)

        qty = (f"La fábrica confirmó {pm.q2(confirmed)} m² frente a "
               f"{pm.q2(selected)} m² pedidos "
               f"({pm.q2(diff)} m² {'menos' if direction == 'down' else 'más'}).")
        if commitment_new:
            severity = "risk"
            text = (qty + " Con la cantidad confirmada, un compromiso con "
                    "cliente deja de estar cubierto.")
        elif stockout_new:
            severity = "risk"
            text = (qty + " Con la cantidad confirmada, se proyecta quiebre de "
                    f"stock hacia {proj_conf.stockout_date.isoformat()} antes de "
                    f"la próxima reposición ({horizon_end.isoformat()}).")
        elif new_breach_dates:
            severity = "watch"
            text = (qty + " Con la cantidad confirmada, el stock baja de la "
                    f"reserva alrededor de {new_breach_dates[0].isoformat()}, "
                    "donde con la cantidad pedida quedaba protegido.")
        else:
            severity = "info"
            text = (qty + " La diferencia no crea riesgo de reserva ni de "
                    "compromisos en el horizonte actual.")

        dm.record_implication(
            state, type="decision_consequence", subtype="v6_material_confirmation",
            product_id=pid,
            refs={"line_id": line_id, "order_id": line.order_id,
                  "product_id": pid,
                  **({"exp_id": exp.exp_id} if exp else {})},
            severity=severity, text=text,
            evidence={
                "selected_m2": str(pm.q2(selected)),
                "confirmed_m2": str(pm.q2(confirmed)),
                "difference_m2": str(pm.q2(diff)),
                "direction": direction,
                "arrival_date": arrival.isoformat(),
                "projection_selected": proj_sel.outcome if proj_sel else None,
                "projection_confirmed": proj_conf.outcome if proj_conf else None,
                "new_breach_dates": [d.isoformat() for d in new_breach_dates],
                "commitment_uncovered": commitment_new,
                "source": "RecordFactoryOutcome (§4.6a administrative boundary)",
                "as_of": self.today.isoformat(),
            },
            actor="system", on=self.today)

    # -- evidence feeds (cumulative snapshots — replacement semantics) ---------

    def load_warehouse_snapshot(self, *, as_of: date, values: dict,
                                sales_since_prev: Optional[dict] = None):
        prev = self.feeds.get("warehouse")
        sales = sales_since_prev or {}
        if prev is not None:
            # arrival attribution in the same reconciliation step (§6.4a)
            for pid, new_value in values.items():
                adjusted_prev = max(ZERO, prev["values"].get(pid, ZERO)
                                    - sales.get(pid, ZERO))
                increase = D(new_value) - adjusted_prev
                if increase <= 0:
                    continue
                open_exps = sorted(
                    (e for e in self.state.expectations.values()
                     if e.product_id == pid and e.open),
                    key=lambda e: (e.ask_date, e.remaining_m2, e.exp_id))
                left = increase
                for e in open_exps:
                    take = min(left, e.remaining_m2)
                    if take > 0:
                        dm.attribute_arrival_to_expectation(
                            self.state, e.exp_id, m2=take, actor="system", on=as_of)
                        left -= take
                # any remainder is plain warehouse truth
        self.feeds["warehouse"] = {"as_of": as_of, "values": {k: D(v) for k, v in values.items()},
                                   "snapshot_id": f"wh-{next(self._snap)}"}
        self._reconcile(self.state)
        self.match_result = self._compute_match(self.state)

    def load_sales_snapshot(self, *, as_of: date):
        self.feeds["sales"] = {"as_of": as_of, "snapshot_id": f"sa-{next(self._snap)}"}

    def load_siesa_snapshot(self, *, as_of: date, values: dict):
        """Factory availability context (§5.1 rung 4) — never caps true need."""
        self.feeds["siesa"] = {"as_of": as_of,
                               "values": {k: D(v) for k, v in values.items()},
                               "snapshot_id": f"si-{next(self._snap)}"}

    def load_transit_snapshot(self, *, as_of: date, rows: list):
        snap_id = f"tr-{next(self._snap)}"
        self._observations = [o for o in self._observations if o.feed != "in_transit"]
        for i, row in enumerate(rows):
            confident = row.get("confident", True)
            obs = Observation(
                obs_id=f"{snap_id}-{i}", feed="in_transit",
                product_id=row.get("product_id") if confident else None,
                raw_reference=row.get("raw_reference") or row.get("product_id") or f"row-{i}",
                m2=D(row["m2"]), observed_at=as_of,
                event_date=row.get("event_date", as_of),
                reference=row.get("reference"), eta=row.get("eta"),
                confident_match=confident, snapshot_id=snap_id)
            self._observations.append(obs)
            if not confident and not self._has_open_case(
                    "ProductMatchResolution", raw_reference=obs.raw_reference):
                dm.open_case(self.state, type="ProductMatchResolution",
                             refs={"raw_reference": obs.raw_reference},
                             evidence={"m2": str(obs.m2), "feed": "in_transit",
                                       "as_of": as_of.isoformat(),
                                       "candidates": row.get("candidates", [])},
                             options=["map", "discard"],
                             actor="system", on=as_of)
        self.feeds["in_transit"] = {"as_of": as_of, "snapshot_id": snap_id}
        self._reconcile(self.state)
        self.match_result = self._compute_match(self.state)

    def load_produced_evidence(self, *, as_of: date, rows: list):
        """Interim reduced-confidence produced signal (U11 gate CLOSED — this
        is synthetic evidence through the domain interface, no real feed)."""
        snap_id = f"pr-{next(self._snap)}"
        self._observations = [o for o in self._observations if o.feed != "produced"]
        for i, row in enumerate(rows):
            self._observations.append(Observation(
                obs_id=f"{snap_id}-{i}", feed="produced",
                product_id=row.get("product_id"),
                raw_reference=row.get("raw_reference") or row.get("product_id") or f"row-{i}",
                m2=D(row["m2"]), observed_at=as_of,
                event_date=row.get("produced_on", as_of),
                reference=row.get("reference"), eta=None,
                confident_match=row.get("confident", True), snapshot_id=snap_id))
        self.feeds["produced"] = {"as_of": as_of, "snapshot_id": snap_id,
                                  "confidence": "reduced (interim signal, U11)"}
        self._reconcile(self.state)
        self.match_result = self._compute_match(self.state)

    def load_production_planning(self, *, rows: list):
        self.production_planning = list(rows)          # SOFT context only

    def load_committed_orders(self, *, as_of: date, rows: list):
        self.committed_orders = {"as_of": as_of, "rows": list(rows)}
        self.feeds["committed_orders"] = {"as_of": as_of,
                                          "snapshot_id": f"co-{next(self._snap)}"}

    # -- §6.4a matching + evidence-driven expectation updates ------------------

    def _views(self, state: dm.State) -> list:
        return [ExpectationView(
            exp_id=e.exp_id, product_id=e.product_id, ask_date=e.ask_date,
            effective_m2=e.effective_m2, received_m2=e.received_m2,
            state=e.state, closed=e.closed_at is not None,
            source_refs=frozenset({e.source_ref, e.exp_id}))
            for e in state.expectations.values()]

    def _compute_match(self, state: dm.State) -> MatchResult:
        """One §6.4a pass per evidence stage: produced and transit evidence
        describe the SAME physical goods at different stages, so each stage
        allocates against the expectation's full remaining capacity — they
        never compete for one pool (a produced match must not push the same
        goods' transit evidence into supply_moving)."""
        views = self._views(state)
        transit = [o for o in self._observations if o.feed == "in_transit"]
        produced = [o for o in self._observations if o.feed == "produced"]
        other = [o for o in self._observations
                 if o.feed not in ("in_transit", "produced")]
        r_t = match(transit, views)
        r_p = match(produced, views)
        r_o = match(other, views)
        return MatchResult(
            tuple(r_t.allocations) + tuple(r_p.allocations) + tuple(r_o.allocations),
            {**r_t.unmatched, **r_p.unmatched, **r_o.unmatched},
            tuple(r_t.held) + tuple(r_p.held) + tuple(r_o.held))

    def _reconcile(self, state: dm.State):
        """Idempotent pass: recompute allocation, advance states forward,
        apply §10.2 supersession/exception rules. Pure function of (trusted
        evidence, open expectations) applied until stable."""
        for _ in range(3):                             # reaches fixpoint fast
            result = self._compute_match(state)
            changed = False
            by_exp: dict = {}
            for a in result.allocations:
                obs = next(o for o in self._observations if o.obs_id == a.obs_id)
                by_exp.setdefault(a.exp_id, {}).setdefault(obs.feed, []).append((obs, a.m2))

            for exp_id, feeds in by_exp.items():
                exp = state.expectations[exp_id]
                if not exp.open:
                    continue
                produced = feeds.get("produced", [])
                transit = feeds.get("in_transit", [])
                target = None
                if transit:
                    target = "in_transit"
                elif produced:
                    target = "produced"
                if target and dm.EXP_ORDER.index(target) > dm.EXP_ORDER.index(exp.state):
                    dm.advance_expectation(state, exp_id, target,
                                           evidence_ref=";".join(o.obs_id for o, _ in
                                                                 (transit or produced)),
                                           actor="system", on=self.today)
                    changed = True

                # quantity comparison per evidence stage (§10.2). The
                # already-received (warehouse-attributed) portion was also
                # produced/moved, so it counts as stage evidence.
                stage = transit or produced
                stage_name = "in_transit" if transit else ("produced" if produced else None)
                if not stage:
                    continue
                observed_total = sum((m for _, m in stage), ZERO) + exp.received_m2
                expected = exp.effective_m2
                diff = expected - observed_total
                if diff <= 0:
                    continue                            # full match: quiet verify
                if variance_is_tolerated(diff, expected, self.config):
                    dm.supersede_expectation(
                        state, exp_id, new_effective_m2=observed_total,
                        evidence_ref=None,
                        note=f"tolerated variance superseded quietly "
                             f"({expected} → {observed_total})",
                        actor="system", on=self.today)
                    changed = True
                    continue
                # material variance
                if stage_name == "produced":
                    pid = exp.product_id
                    # AC2/AC3: one candidate-excluded dated-supply view
                    # feeds BOTH the complete §6.7 curves and the
                    # per-commitment before/after comparison — the
                    # consequence must be NEWLY created by the shortfall
                    arrival = self._arrival_date(exp, state)
                    base = self._dated_supply_for(pid, state,
                                                  exclude_exp=exp_id)
                    commitment_new = self._commitment_newly_uncovered(
                        pid, qty_before=expected, arrival_before=arrival,
                        qty_after=observed_total, arrival_after=arrival,
                        other_supply=base)
                    is_exc = production_shortfall_is_exception(
                        supply_now=self.warehouse_value(pid),
                        daily_velocity=self.velocity(pid),
                        buffer_m2=self.buffer_m2(pid),
                        expected_effective_m2=expected,
                        produced_m2=observed_total,
                        arrival_date=arrival,
                        today=self.today,
                        horizon_end=self._next_replenishment(),
                        customer_commitment_uncovered=commitment_new,
                        config=self.config,
                        other_supply=base)
                    if is_exc:
                        if not self._has_open_case("ReconciliationException",
                                                   state=state, exp_id=exp_id):
                            dm.open_case(
                                state, type="ReconciliationException",
                                subtype="production_shortfall",
                                refs={"exp_id": exp_id, "product_id": pid},
                                evidence={"expected_m2": str(expected),
                                          "observed_m2": str(observed_total),
                                          "source": "produced (interim signal)",
                                          "as_of": self.feeds.get("produced", {}).get(
                                              "as_of", self.today).isoformat()},
                                options=["reduce_effective", "keep_expectation",
                                         "cancel_expectation"],
                                actor="system", on=self.today)
                            changed = True
                    else:
                        # material magnitude, no consequence → quiet supersession
                        # with a PROMINENT history note (Ashley's rule, S8c).
                        # The marker travels IN the event payload so replay
                        # reconstructs it (review finding F1).
                        dm.supersede_expectation(
                            state, exp_id, new_effective_m2=observed_total,
                            evidence_ref=None,
                            note=f"material shortfall without supply consequence "
                                 f"({expected} → {observed_total})",
                            actor="system", on=self.today, prominent=True)
                        changed = True
                else:
                    if not self._has_open_case("ReconciliationException",
                                               state=state, exp_id=exp_id):
                        dm.open_case(
                            state, type="ReconciliationException",
                            subtype="material_transit_variance",
                            refs={"exp_id": exp_id, "product_id": exp.product_id},
                            evidence={"expected_m2": str(expected),
                                      "observed_m2": str(observed_total),
                                      "source": "in_transit",
                                      "as_of": self.feeds.get("in_transit", {}).get(
                                          "as_of", self.today).isoformat()},
                            options=["accept_evidence", "keep_expectation",
                                     "adjust_with_note"],
                            actor="system", on=self.today)
                        changed = True
            if not changed:
                break

    # -- dating helpers ---------------------------------------------------------

    def _voyage_days(self) -> int:
        return self.config.default_voyage_days     # injected; boat feed absent

    def _next_replenishment(self) -> date:
        order = pm.next_cycle_order_date(self.today, self.config)
        return pm.next_replenishment_date(order, self._voyage_days())

    def _arrival_date(self, exp, state: dm.State) -> date:
        v = self._voyage_days()
        if exp.state == "in_transit":
            matched = [o for o in self._observations
                       if o.feed == "in_transit" and any(
                           a.obs_id == o.obs_id and a.exp_id == exp.exp_id
                           for a in self._compute_match(state).allocations)]
            eta = next((o.eta for o in matched if o.eta), None)
            as_of = max((o.observed_at for o in matched),
                        default=self.feeds.get("in_transit", {}).get("as_of", self.today))
            return pm.arrival_date_in_transit(eta=eta, as_of=as_of, voyage_days=v)
        if exp.state == "produced":
            produced_on = next(
                (o.event_date for o in reversed(self._observations)
                 if o.feed == "produced" and o.product_id == exp.product_id),
                self.feeds.get("produced", {}).get("as_of", self.today))
            return pm.arrival_date_produced(produced_on=produced_on, voyage_days=v)
        if exp.source_ref in state.amendments:
            row = next((r for r in self.production_planning
                        if r.get("orden_produccion") ==
                        state.amendments[exp.source_ref].production_ref), None)
            return pm.arrival_date_amendment(
                estimated_delivery=row.get("estimated_delivery_date") if row else None,
                scheduled_start=(row.get("scheduled_start_date") if row else exp.ask_date),
                voyage_days=v)
        return pm.arrival_date_confirmed(submitted_on=exp.ask_date, voyage_days=v)

    def dated_supply(self, pid) -> list:
        """(expected_warehouse_arrival, m2) for open expectations + unmatched
        transit of this product (per §6.7 dating rules)."""
        return self._dated_supply_for(pid, self.state)

    def _dated_supply_for(self, pid, state: dm.State,
                          exclude_exp: Optional[str] = None) -> list:
        events = []
        mr = self._compute_match(state)
        for e in state.expectations.values():
            if e.exp_id == exclude_exp:
                continue
            if e.product_id == pid and e.open and e.remaining_m2 > 0:
                events.append((self._arrival_date(e, state), e.remaining_m2))
        for o in self._observations:
            if o.feed == "in_transit" and o.product_id == pid:
                unmatched = mr.unmatched.get(o.obs_id, ZERO)
                if unmatched > 0:
                    events.append((pm.arrival_date_in_transit(
                        eta=o.eta, as_of=o.observed_at,
                        voyage_days=self._voyage_days()), unmatched))
        return sorted(events)

    # -- buckets -----------------------------------------------------------------

    def buckets(self, pid) -> ProductBuckets:
        expected = sum((e.remaining_m2 for e in self.state.expectations.values()
                        if e.product_id == pid and e.open), ZERO)
        moving = ZERO
        for o in self._observations:
            if o.feed == "in_transit" and o.product_id == pid and o.confident_match:
                moving += self.match_result.unmatched.get(o.obs_id, ZERO)
        return ProductBuckets(self.warehouse_value(pid), expected, moving)

    def buckets_held_total(self) -> Decimal:
        return sum((h.m2 for h in self.match_result.held), ZERO)

    # -- proactive checks (§10.3, §10.5) ------------------------------------------

    def run_checks(self):
        self._check_factory_response_overdue()
        self._check_amendment_windows()
        self._check_missing_evidence()
        self._check_decision_required()
        self._check_amendment_opportunities()
        self._check_expected_transit_missing()
        self._check_production_delay()
        self.match_result = self._compute_match(self.state)

    # -- §10.3 expected transit missing (C3.1) ---------------------------------

    # System closure reasons for expected_transit_missing cases. Anything
    # else means Ashley decided — such cases never reopen on unchanged facts.
    _TRANSIT_SYSTEM_CLEARS = frozenset({
        "transit_evidence_arrived", "expectation_cancelled",
        "expectation_closed", "timing_no_longer_applicable"})

    def _check_expected_transit_missing(self):
        """Confirmed + produced but no transit evidence after the
        deterministic window: the next in-transit file upload AFTER the
        estimated dispatch (per-expectation MATCHED produced-evidence date +
        TRUCK_TO_PORT_DAYS — an unmatched or other-expectation observation
        never shifts a window, R5). Closed-taxonomy surface:
        ReconciliationException. Opening the case flags the governing
        expectation `overdue` in the same event sequence; closure clears the
        flag and records a TRUTHFUL typed reason: actual evidence arrival vs
        cancellation vs terminal closure vs timing no longer applicable."""
        from datetime import timedelta
        from lib.constants import TRUCK_TO_PORT_DAYS
        transit_feed = self.feeds.get("in_transit")
        upload_as_of = transit_feed["as_of"] if transit_feed else None
        mr = self._compute_match(self.state)
        obs_by_id = {o.obs_id: o for o in self._observations}

        def matched_produced_on(exp):
            """Latest event date of produced evidence ALLOCATED to THIS
            expectation — the only deterministic dispatch basis (R5)."""
            dates = [obs_by_id[a.obs_id].event_date
                     for a in mr.allocations
                     if a.exp_id == exp.exp_id and a.obs_id in obs_by_id
                     and obs_by_id[a.obs_id].feed == "produced"]
            return max(dates) if dates else None

        def existing_cases(exp_id):
            return [c for c in self.state.cases.values()
                    if c.type == "ReconciliationException"
                    and c.subtype == "expected_transit_missing"
                    and c.refs.get("exp_id") == exp_id]

        for exp in list(self.state.expectations.values()):
            cases = existing_cases(exp.exp_id)
            open_case = next((c for c in cases if c.state == "open"), None)

            missing = False
            dispatch_est = produced_on = None
            if exp.open and exp.state == "produced" and upload_as_of is not None:
                produced_on = matched_produced_on(exp)
                if produced_on is not None:
                    dispatch_est = produced_on + timedelta(days=TRUCK_TO_PORT_DAYS)
                    # "next in-transit file upload after estimated dispatch"
                    missing = upload_as_of > dispatch_est

            if not missing:
                reason = None
                if open_case is not None or exp.overdue:
                    # truthful recovery provenance (R5): what actually ended
                    # the missing condition?
                    if exp.state == "cancelled":
                        reason = "expectation_cancelled"
                    elif exp.state == "in_transit":
                        reason = "transit_evidence_arrived"
                    elif exp.state == "arrived":
                        transit_seen = any(
                            h.get("event") == "advanced"
                            and h.get("state") == "in_transit"
                            for h in exp.history)
                        reason = ("transit_evidence_arrived" if transit_seen
                                  else "expectation_closed")
                    else:
                        # still confirmed/produced, but the window no longer
                        # precedes the upload (evidence corrected/shifted)
                        reason = "timing_no_longer_applicable"
                if open_case is not None:
                    dm.resolve_case(self.state, open_case.case_id,
                                    resolution={"action": reason},
                                    actor="system", on=self.today)
                # the overdue flag follows the CONDITION, not the case: it
                # clears even when Ashley (not the system) already resolved
                # the case and the goods later ship/close/cancel. FV5: the
                # clear carries the same truthful recovery reason into the
                # expectation's replayable transition history.
                if exp.overdue:
                    dm.clear_expectation_overdue(
                        self.state, exp.exp_id, reason=reason,
                        actor="system", on=self.today)
                continue
            if open_case is not None:
                continue                          # dedup across repeated runs
            if any(c.state == "resolved"
                   and (c.resolution or {}).get("action")
                   not in self._TRANSIT_SYSTEM_CLEARS for c in cases):
                continue                          # Ashley already decided
            dm.open_case(
                self.state, type="ReconciliationException",
                subtype="expected_transit_missing",
                refs={"exp_id": exp.exp_id, "product_id": exp.product_id},
                evidence={
                    "expected_m2": str(exp.remaining_m2),
                    "produced_on": produced_on.isoformat(),
                    "estimated_dispatch": dispatch_est.isoformat(),
                    "transit_upload_as_of": upload_as_of.isoformat(),
                    "observed": "sin evidencia de tránsito para este producto "
                                "en la última carga",
                    "source": "in_transit_snapshot",
                },
                options=["keep_expectation", "adjust_with_note",
                         "cancel_expectation"],
                actor="system", on=self.today)
            # the accepted overdue lifecycle accompanies the case (R5)
            if not exp.overdue:
                dm.flag_expectation_overdue(self.state, exp.exp_id,
                                            actor="system", on=self.today)

    # -- §10.3 production delay threatening the window (C3.2) ------------------

    # System closure reasons for production_delay cases. Anything else in a
    # resolution means Ashley decided — such cases never reopen on unchanged
    # facts (R4).
    _DELAY_SYSTEM_CLEARS = frozenset({
        "timing_risk_cleared", "planning_row_removed", "planning_row_inactive",
        "planning_row_superseded", "no_longer_applicable"})

    def _active_planning_rows(self):
        return [r for r in self.production_planning
                if r.get("status") in ("scheduled", "in_progress")]

    def _delay_governing_pairs(self):
        """Deterministic row↔expectation association (R4/FV4). A soft
        planning row governs an expectation ONLY through a UNIQUE accepted
        reference:
        - amendment-sourced expectation → the amendment's `production_ref`
          must resolve to EXACTLY ONE active same-product row. A duplicated
          exact reference identifies nothing — ambiguous, stays quiet
          (never a feed-order-dependent `next(...)` pick).
        - line-sourced expectation → NO pairing. Cardinality ("one
          expectation, one row") is not a reference: nothing proves an
          arbitrary same-product row produces that line's goods, and no
          accepted line/order↔planning reference exists in this tranche —
          soft context stays quiet rather than inventing a consequence.
        Produced/transit evidence (state beyond `confirmed`) removes the
        expectation from soft-planning governance entirely. The result is
        invariant to planning-feed row order."""
        active = self._active_planning_rows()
        governable = [e for e in self.state.expectations.values()
                      if e.open and e.state == "confirmed"
                      and e.remaining_m2 > 0]
        pairs = []
        for e in governable:
            a = self.state.amendments.get(e.source_ref)
            if a is None:
                continue                  # line-sourced: no accepted reference
            rows = [r for r in active
                    if r.get("orden_produccion") == a.production_ref
                    and r.get("product_id") == e.product_id]
            if len(rows) == 1:
                pairs.append((e, rows[0]))
            # 0 rows → nothing to govern; ≥2 rows → duplicated exact
            # reference is ambiguous → quiet (the open-case sweep closes any
            # previously valid case with a truthful reason)
        return pairs

    def _check_production_delay(self):
        """Planned production dates slipping past the point where the product
        still protects buffer/commitments (§10.3, Ashley #15). Compares the
        §6.7 projection at the expectation's baseline arrival against the
        same projection re-dated by the slipped GOVERNING planning row
        (deterministic association only); surfaces only a NEW horizon breach
        / stockout / uncovered commitment. Closed-taxonomy surface:
        DecisionRequired. Every open case is swept every run: cases close
        with a TRUTHFUL typed system reason when the governing fact
        disappears or stops applying, and with `timing_risk_cleared` only on
        genuine date recovery (R4)."""
        def cases_for(exp_id, ref):
            return [c for c in self.state.cases.values()
                    if c.type == "DecisionRequired"
                    and c.subtype == "production_delay"
                    and c.refs.get("exp_id") == exp_id
                    and c.refs.get("production_ref") == ref]

        pairs = self._delay_governing_pairs()
        evaluated: dict = {}              # (exp_id, ref) → threatened bool
        for exp, row in pairs:
            pid = exp.product_id
            ref = row.get("orden_produccion")
            velocity = self.velocity(pid)
            threatened = False
            status = "not_new"
            evidence: dict = {}
            if velocity > 0:
                # Baseline: the accepted conservative §6.7 chain from the
                # commitment's ask date. For amendment-sourced expectations
                # this is deliberately NOT derived from the (possibly
                # slipped) planning row itself — otherwise baseline and
                # delayed would always coincide and no slip could ever be
                # detected (independent-review finding).
                baseline = pm.arrival_date_confirmed(
                    submitted_on=exp.ask_date,
                    voyage_days=self._voyage_days())
                delayed = pm.arrival_date_amendment(
                    estimated_delivery=row.get("estimated_delivery_date"),
                    scheduled_start=row.get("scheduled_start_date"),
                    voyage_days=self._voyage_days())
                if delayed <= baseline:
                    status = "recovered"          # genuine date recovery
                if delayed > baseline:
                    buffer_m2 = self.buffer_m2(pid)
                    horizon_end = self._next_replenishment()
                    base = self._dated_supply_for(pid, self.state,
                                                  exclude_exp=exp.exp_id)
                    qty = exp.remaining_m2
                    proj_base = pm.project_buffer_protection(
                        supply_now=self.warehouse_value(pid),
                        daily_velocity=velocity, buffer_m2=buffer_m2,
                        events=sorted(base + [(baseline, qty)]),
                        today=self.today, horizon_end=horizon_end)
                    proj_del = pm.project_buffer_protection(
                        supply_now=self.warehouse_value(pid),
                        daily_velocity=velocity, buffer_m2=buffer_m2,
                        events=sorted(base + [(delayed, qty)]),
                        today=self.today, horizon_end=horizon_end)
                    base_h = dict(proj_base.minima).get(horizon_end)
                    del_h = dict(proj_del.minima).get(horizon_end)
                    stockout_new = (proj_del.outcome == "projected_stockout"
                                    and proj_base.outcome != "projected_stockout")
                    horizon_breach_new = (del_h is not None and base_h is not None
                                          and del_h < buffer_m2 <= base_h)
                    commitment_new = self._commitment_newly_uncovered(
                        pid, qty_before=qty, arrival_before=baseline,
                        qty_after=qty, arrival_after=delayed,
                        other_supply=base)
                    threatened = (stockout_new or horizon_breach_new
                                  or commitment_new)
                    evidence = {
                        "expected_m2": str(qty),
                        "baseline_arrival": baseline.isoformat(),
                        "delayed_arrival": delayed.isoformat(),
                        "planned_start": (row["scheduled_start_date"].isoformat()
                                          if row.get("scheduled_start_date") else None),
                        "planned_delivery": (row["estimated_delivery_date"].isoformat()
                                             if row.get("estimated_delivery_date") else None),
                        "projection_baseline": proj_base.outcome,
                        "projection_delayed": proj_del.outcome,
                        "stockout_date": (proj_del.stockout_date.isoformat()
                                          if proj_del.stockout_date else None),
                        "commitment_uncovered": commitment_new,
                        "horizon_end": horizon_end.isoformat(),
                        "source": "production_planning (contexto blando) + "
                                  "§6.7 re-proyección",
                    }
            if velocity > 0 and threatened:
                status = "threatened"
            evaluated[(exp.exp_id, ref)] = status
            if status != "threatened":
                continue
            cases = cases_for(exp.exp_id, ref)
            if any(c.state == "open" for c in cases):
                continue                  # dedup across repeated runs
            if any(c.state == "resolved"
                   and (c.resolution or {}).get("action")
                   not in self._DELAY_SYSTEM_CLEARS for c in cases):
                continue                  # Ashley already decided
            dm.open_case(
                self.state, type="DecisionRequired",
                subtype="production_delay",
                refs={"product_id": pid, "exp_id": exp.exp_id,
                      "production_ref": ref},
                evidence=evidence,
                options=["order_now", "amend_production",
                         "accept_risk_with_note"],
                actor="system", on=self.today)

        # final sweep (R4): EVERY open production_delay case not confirmed
        # threatened this run closes with a truthful typed reason
        active = self._active_planning_rows()
        for c in [c for c in self.state.cases.values()
                  if c.type == "DecisionRequired"
                  and c.subtype == "production_delay" and c.state == "open"]:
            key = (c.refs.get("exp_id"), c.refs.get("production_ref"))
            if evaluated.get(key) == "threatened":
                continue                  # confirmed threatened — stays open
            ref = c.refs.get("production_ref")
            pid = c.refs.get("product_id")
            exp = self.state.expectations.get(c.refs.get("exp_id"))
            row_any = next((r for r in self.production_planning
                            if r.get("orden_produccion") == ref), None)
            row_active = next((r for r in active
                               if r.get("orden_produccion") == ref), None)
            if row_any is None:
                reason = ("planning_row_superseded"
                          if any(r.get("product_id") == pid for r in active)
                          else "planning_row_removed")
            elif row_active is None:
                reason = "planning_row_inactive"
            elif exp is None or not exp.open or exp.state != "confirmed":
                # produced/transit evidence or terminal state now governs
                reason = "no_longer_applicable"
            elif key not in evaluated:
                # row and expectation both live, but the deterministic
                # association no longer holds (ambiguity, claimed ref, …)
                reason = "no_longer_applicable"
            elif evaluated.get(key) == "recovered":
                reason = "timing_risk_cleared"    # genuine date recovery ONLY
            else:
                # the delay stands but no longer creates the DISTINGUISHING
                # new consequence (e.g. the baseline projection now fails
                # too, or velocity dropped) — truthful label, never
                # "timing recovered" (independent-review finding)
                reason = "no_longer_applicable"
            dm.resolve_case(self.state, c.case_id,
                            resolution={"action": reason},
                            actor="system", on=self.today)

    def _commitment_row_uncovered(self, pid, row, supply_m2: Decimal,
                                  arrival: date, other_supply=()) -> bool:
        """§6.7-dated coverage of ONE commitment row: warehouse minus
        time-phased demand, plus ALL other accepted dated supply arriving on
        or before the due date (FV3), plus the candidate quantity when ITS
        arrival is on or before the due date (R3)."""
        due = row["due_date"]
        days = max(0, (due - self.today).days)
        available = self.warehouse_value(pid) - self.velocity(pid) * days
        available += sum((m for d_, m in other_supply if d_ <= due), ZERO)
        if arrival <= due:
            available += supply_m2
        return available < D(row["m2"])

    def _commitment_newly_uncovered(self, pid, *, qty_before: Decimal,
                                    arrival_before: date, qty_after: Decimal,
                                    arrival_after: date,
                                    other_supply=()) -> bool:
        """True iff SOME commitment is covered under the before-timing but
        uncovered under the after-timing — evaluated PER commitment row, so
        a commitment already lost under both timings never masks (or fakes)
        a genuinely new consequence (R3). `other_supply` is the §6.7 dated
        supply for the product EXCLUDING the candidate expectation being
        compared (FV3) — passed by the caller so the candidate is excluded
        exactly once and never double-counted."""
        if not self.committed_orders:
            return False
        for row in self.committed_orders["rows"]:
            if row["product_id"] != pid:
                continue
            if self._commitment_row_uncovered(pid, row, qty_after,
                                              arrival_after, other_supply) \
                    and not self._commitment_row_uncovered(pid, row, qty_before,
                                                           arrival_before,
                                                           other_supply):
                return True
        return False

    def _check_factory_response_overdue(self):
        window = self.config.factory_response_overdue_days
        for l in self.state.lines.values():
            if l.state != "awaiting_factory" or l.ask_date is None:
                continue
            if (self.today - l.ask_date).days > window and \
                    not self._has_open_case("FactoryResponseOverdue", line_id=l.line_id):
                dm.open_case(self.state, type="FactoryResponseOverdue",
                             refs={"line_id": l.line_id, "product_id": l.product_id},
                             evidence={"ask_date": l.ask_date.isoformat(),
                                       "days_waiting": (self.today - l.ask_date).days,
                                       "window_days": window},
                             options=["record_outcome", "extend_window", "withdraw"],
                             actor="system", on=self.today)

    def _check_amendment_windows(self):
        for a in list(self.state.amendments.values()):
            if a.state != "open":
                continue
            row = next((r for r in self.production_planning
                        if r.get("orden_produccion") == a.production_ref), None)
            start = row.get("scheduled_start_date") if row else None
            if start is not None and self.today >= start:
                dm.close_amendment_unfulfilled(
                    self.state, a.amendment_id,
                    reason="auto-closed: production start passed with no answer",
                    actor="system", on=self.today)

    def _check_missing_evidence(self):
        for feed in CRITICAL_FEEDS:
            info = self.feeds.get(feed)
            age = (self.today - info["as_of"]).days if info else None
            if info is not None and age <= self.config.freshness_stale_days:
                continue
            if self._has_open_case("MissingEvidence", feed=feed):
                continue
            dm.open_case(self.state, type="MissingEvidence",
                         refs={"feed": feed},
                         evidence={"last_as_of": info["as_of"].isoformat() if info else None,
                                   "age_days": age,
                                   "affected_rows": len(self.products),
                                   "caveat": "suggestions computed with stale/"
                                             "missing evidence — visible caveat"},
                         options=["refresh_feed", "proceed_with_note"],
                         actor="system", on=self.today)

    def _check_decision_required(self, state: Optional[dm.State] = None):
        state = state if state is not None else self.state
        for pid in self.products:
            velocity = self.velocity(pid)
            if velocity <= 0:
                continue                    # zero-velocity → watch signal only
            buffer_m2 = self.buffer_m2(pid)
            supply_now = self.warehouse_value(pid)
            events = self._dated_supply_for(pid, state)
            # a pending ask (awaiting line / open amendment) is a planned
            # response — §10.5.2 fires only when NO viable response remains
            pending = sum((l.selected_m2 for l in state.lines.values()
                           if l.product_id == pid
                           and l.state == "awaiting_factory"), ZERO) \
                + sum((a.requested_m2 for a in state.amendments.values()
                       if a.product_id == pid and a.state == "open"), ZERO)
            if pending > 0:
                continue
            horizon_end = self._next_replenishment()
            proj = pm.project_buffer_protection(
                supply_now=supply_now, daily_velocity=velocity,
                buffer_m2=buffer_m2, events=events, today=self.today,
                horizon_end=horizon_end)
            below_no_response = supply_now < buffer_m2 and not events
            # §10.4 surfacing discipline + S10: the stockout branch interrupts
            # only when the risk lands before an order placed TODAY could
            # arrive (today + 25+5+voyage+6) — inside that window the normal
            # cycle decision is itself the viable planned response, and the
            # §6.7 dip/stockout implication already surfaces at the row.
            own_order_arrival = pm.expected_arrival_for_proposed_order(
                self.today, self._voyage_days())
            urgent_stockout = (proj.outcome == "projected_stockout"
                               and proj.stockout_date is not None
                               and proj.stockout_date <= own_order_arrival)
            if urgent_stockout or below_no_response:
                if not any(all(c.refs.get(k) == v for k, v in
                               {"product_id": pid}.items())
                           for c in state.cases.values()
                           if c.state == "open" and c.type == "DecisionRequired"
                           and not c.subtype):   # subtyped cases never mask it
                    dm.open_case(
                        state, type="DecisionRequired",
                        refs={"product_id": pid},
                        evidence={"projection_outcome": proj.outcome,
                                  "stockout_date": proj.stockout_date.isoformat()
                                  if proj.stockout_date else None,
                                  "supply_now": str(supply_now),
                                  "buffer_m2": str(buffer_m2),
                                  "planned_events": [(d.isoformat(), str(m))
                                                     for d, m in events]},
                        options=["order_now", "amend_production",
                                 "accept_risk_with_note"],
                        actor="system", on=self.today)
        # customer commitments at risk (U10 — only when the feed is present).
        # review finding N1/N3: this branch reads and writes the SAME state
        # the check was invoked with — inside a transaction that is the
        # working copy, so a failed command leaks nothing to live state and
        # a successful one keeps the case through the swap.
        if self.committed_orders:
            for row in self.committed_orders["rows"]:
                pid = row["product_id"]
                due = row["due_date"]
                committed = D(row["m2"])
                days = max(0, (due - self.today).days)
                available = self.warehouse_value(pid) - self.velocity(pid) * days
                available += sum((m for d_, m in
                                  self._dated_supply_for(pid, state)
                                  if d_ <= due), ZERO)
                if available < committed and not self._has_open_case(
                        "DecisionRequired", state=state, product_id=pid,
                        commitment_due=due.isoformat()):
                    dm.open_case(
                        state, type="DecisionRequired",
                        subtype="customer_commitment_at_risk",
                        refs={"product_id": pid, "commitment_due": due.isoformat()},
                        evidence={"committed_m2": str(committed),
                                  "projected_available_m2": str(available),
                                  "source": "committed_orders",
                                  "as_of": self.committed_orders["as_of"].isoformat()},
                        options=["order_now", "amend_production",
                                 "accept_risk_with_note"],
                        actor="system", on=self.today)

    def _check_amendment_opportunities(self):
        for row in self.production_planning:
            if row.get("status") != "scheduled" or not row.get("can_add_more"):
                continue
            start = row.get("scheduled_start_date")
            if start is None or start <= self.today:
                continue
            pid = row.get("product_id")
            if pid not in self.products or self.velocity(pid) <= 0:
                continue
            horizon_end = self._next_replenishment()
            need = pm.true_and_residual_need(
                daily_velocity=self.velocity(pid), buffer_m2=self.buffer_m2(pid),
                horizon_days=pm.protection_horizon_days(self.today, horizon_end),
                supply_now=self.warehouse_value(pid),
                dated_supply=self.dated_supply(pid),
                pending_commitment_m2=self.pending_commitment(pid),
                next_replenishment_date=horizon_end)
            if need.residual_need_m2 <= 0:
                continue
            proposed = pm.round_up_to_half_pallet(need.residual_need_m2)
            ref = row.get("orden_produccion")
            if not self._has_open_case("AmendmentIntentConfirmation",
                                       production_ref=ref):
                dm.open_case(
                    self.state, type="AmendmentIntentConfirmation",
                    refs={"product_id": pid, "production_ref": ref},
                    evidence={"scheduled_start_date": start.isoformat(),
                              "gap_m2": str(need.residual_need_m2),
                              "proposed_additional_m2": str(proposed),
                              "production_row": {k: str(v) for k, v in row.items()}},
                    options=["confirm_intent", "dismiss"],
                    actor="system", on=self.today)

    # AC3: the divergent `_commitment_uncovered` sibling helper was REMOVED.
    # The production-shortfall path routes through the same
    # `_commitment_newly_uncovered` before/after comparison as V6 and the
    # production-delay check — one coherent coverage semantics. A commitment
    # already uncovered under both candidate quantities surfaces only
    # through the dedicated `customer_commitment_at_risk` path.
