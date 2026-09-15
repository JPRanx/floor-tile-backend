"""
Reconciled V1 — the sailing/SIESA-centered engine (spec §§5–10).

Orchestrates: evidence normalization [D2], D3 sailing timing, the D5 layered
allocation chain (decision → observed SIESA commitment → booking-promoted
expected incoming → transit reality), mutually exclusive supply buckets with
the C4 never-double-subtract gate, availability-capped recommendations
[WWO6 #9], the six implication families [D6], plan-status derivation and
resolution-driven recomputation with before/after history [D7], and the
monthly-manufacturing context connection [WWO6 #6].

All lifecycle mutation flows through the command bus (server-derived actor,
atomic transaction); the reconcile pass runs INSIDE each transaction.
"""
from __future__ import annotations

import hmac

from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from .config import PlanningConfig
from . import matching as mt
from . import planning_math as pm
from . import sailing_domain as sd
from . import sailing_math as sm
from . import sailing_normalize as sn
from .domain import DomainError, thaw, _ser
from .production_catalog import product_from_inventory_reference
from .sailing_commands import (
    SailingCommandBus, InMemoryIdentityProvider, CommandError,
)

ZERO = Decimal("0")
D = Decimal

CRITICAL_FEEDS = ("siesa_availability", "warehouse", "sales")
FEED_COMMANDS = {
    "LoadSiesaSnapshot": "siesa_availability",
    "LoadWarehouseSnapshot": "warehouse",
    "LoadSalesSnapshot": "sales",
    "LoadTransitSnapshot": "in_transit",
    "LoadCommittedOrders": "committed_orders",
    "LoadProductionPlanning": "production_planning",
}
# expectation-state mapping into the preserved matching engine's vocabulary
_MT_STATE = {"booked": "confirmed", "departed": "produced",
             "in_transit": "in_transit", "arrived": "arrived",
             "cancelled": "cancelled"}

DEFAULT_SESSIONS = {"tok-ashley": "ashley", "tok-elicio": "elicio"}


class SailingEngine:
    def __init__(self, *, config: PlanningConfig, today: date, products: list,
                 sessions: Optional[dict] = None):
        self.config = config
        self._today = today
        self.products = {p["product_id"]: dict(p) for p in products}
        # Only the durable PostgreSQL unit of work may enable this capability.
        # The pure engine can replay resolutions but cannot create catalog rows.
        self._durable_catalog_staging = False
        self.identity = InMemoryIdentityProvider(sessions or DEFAULT_SESSIONS)
        self.bus = SailingCommandBus(state=sd.SState.empty(),
                                     identity=self.identity, engine=self)

    # ── surface ------------------------------------------------------------

    @property
    def state(self) -> sd.SState:
        return self.bus.state

    @property
    def today(self) -> date:
        return self._today

    def set_today(self, d: date):
        self._today = d

    def catalog(self, s=None) -> dict:
        """Return only the configured or durably hydrated product catalog."""
        return dict(self.products)

    def execute(self, command: str, params: dict, *, token: Optional[str]):
        return self.bus.execute(command, params, token=token, on=self._today)

    def ashley(self, command: str, params: dict) -> dict:
        return self.execute(command, params,
                            token=self.identity.token_for("ashley"))

    def elicio(self, command: str, params: dict) -> dict:
        return self.execute(command, params,
                            token=self.identity.token_for("elicio"))

    def run_checks(self):
        """Re-evaluate time-window checks (a state-mutating pass through the
        same transactional boundary)."""
        return self.ashley("RunChecks", {})

    # ── feed access helpers (state-parametrized, mappings applied on read) --

    def _feed(self, s, name):
        return s.feeds.get(name)

    def _rows(self, s, name) -> list:
        """Normalized rows of the latest snapshot with product-match
        resolutions applied on read: a recorded mapping upgrades an
        unconfident row; a mapping to None discards it."""
        f = s.feeds.get(name)
        if not f:
            return []
        return self._normalize_feed_rows(s, name, f)

    def _normalize_feed_rows(self, s, name, f) -> list:
        """[FC1 review B2] ONE law for reading any snapshot — current or
        historical: mapping resolutions applied, then the additive
        duplicate merge. The observation baseline chain uses this same
        path, so a later mapping can never make baseline and current
        snapshots disagree about physically identical rows. The law itself
        lives in `sailing_normalize.effective_rows` so the FCB1 mapping
        collision guard evaluates the SAME law before recording [FCB1]."""
        return sn.effective_rows(name, f["rows"], s.mappings)

    def _held_rows(self, s) -> list:
        held = []
        for feed in ("in_transit", "siesa_availability", "warehouse",
                     "sales", "committed_orders", "production_planning"):
            for row in self._rows(s, feed):
                if not row.get("confident_match"):
                    material_values = (
                        row.get("m2"), row.get("available_m2"),
                        row.get("committed_m2"), row.get("daily_velocity"),
                        row.get("peak_weekly_m2"),
                    )
                    held.append({"feed": feed,
                                 "raw_ref": str(row.get("product_ref")),
                                 "m2": row.get("m2") or row.get("available_m2")
                                 or ZERO,
                                 "material": any(
                                     D(str(value)) != ZERO
                                     for value in material_values
                                     if value is not None),
                                 })
        return held

    def held_total(self, s=None) -> Decimal:
        s = s or self.state
        # unconfident rows across all feeds (once), plus confident rows held
        # by the matching pass for impossible references (identity-level
        # contradiction — not in _held_rows)
        total = sum((D(str(h["m2"])) for h in self._held_rows(s)), ZERO)
        total += sum((h.m2 for h in self._match(s).held
                      if h.reason == "impossible_reference"), ZERO)
        return pm.q2(total)

    def velocity(self, pid, s=None) -> Optional[Decimal]:
        s = s or self.state
        if not self._feed(s, "sales"):
            return None
        for row in self._rows(s, "sales"):
            if row.get("product_id") == pid:
                return D(str(row["daily_velocity"]))
        return D("0")

    def _peak(self, s, pid) -> Optional[Decimal]:
        for row in self._rows(s, "sales"):
            if row.get("product_id") == pid:
                v = row.get("peak_weekly_m2")
                return (pm.q2(D(str(v)) / D("7"))
                        if v is not None else None)
        return None

    def tier_map(self, s=None) -> dict[str, str]:
        """Classify the complete current catalog from the active 90-day velocity feed."""
        s = s or self.state
        product_ids = list(self.catalog(s))
        velocities = {pid: self.velocity(pid, s) or ZERO for pid in product_ids}
        return pm.classify_tiers(product_ids, velocities)

    def tier_for(self, pid, s=None) -> str:
        return self.tier_map(s).get(pid, "C")

    def buffer_m2(self, pid, s=None) -> Decimal:
        s = s or self.state
        v = self.velocity(pid, s) or ZERO
        tier = self.tier_for(pid, s)
        return pm.buffer_for(tier, v, self._peak(s, pid))

    def warehouse_value(self, pid, s=None) -> Optional[Decimal]:
        s = s or self.state
        if not self._feed(s, "warehouse"):
            return None
        for row in self._rows(s, "warehouse"):
            if row.get("product_id") == pid:
                return D(str(row["m2"]))
        return D("0")

    # ── sailing timing / windows (D3, §6.4) ---------------------------------

    def _voyage(self, sailing) -> int:
        return (sailing.voyage_days if sailing.voyage_days is not None
                else self.config.default_voyage_days)

    def sailing_timing(self, sid, s=None) -> sm.SailingTiming:
        s = s or self.state
        sl = s.sailings[sid]
        if sl.planning_basis == "bl_vgm_close":
            return sm.derive_roster_timing(
                bl_vgm_close=sl.bl_vgm_close,
                loading_terminal_eta=sl.loading_terminal_eta,
                voyage_days=self._voyage(sl), today=self._today)
        return sm.derive_timing(departure=sl.departure,
                                voyage_days=self._voyage(sl),
                                today=self._today)

    def planning_window_for(self, sid, s=None) -> dict:
        """Canonical §19 planning boundary consumed by math and presentation."""
        s = s or self.state
        if sid not in s.sailings:
            raise ValueError(f"unknown sailing {sid}")
        focus_timing = self.sailing_timing(sid, s)
        focus_arrival = focus_timing.warehouse_arrival
        candidates = []
        for other_sid, sailing in s.sailings.items():
            if other_sid == sid or s.sailing_decisions.get(other_sid) == "skip":
                continue
            timing = self.sailing_timing(other_sid, s)
            if timing.warehouse_arrival <= focus_arrival:
                continue
            decision = s.sailing_decisions.get(other_sid, "watch")
            candidates.append((timing.warehouse_arrival, other_sid, sailing,
                               decision))
        if candidates:
            arrival, boundary_sid, sailing, decision = sorted(candidates)[0]
            boundary = {
                "sailing_id": boundary_sid,
                "name": sailing.name,
                "decision": decision,
                "authority": ("confirmed_use" if decision == "use"
                              else "tentative_watch"),
                "warehouse_arrival": arrival,
                "basis": "earliest_known_non_skipped_after_focus",
            }
            projected_until = None
            fallback = {"active": False, "ordering_cycle_days": 30,
                        "projected_until": None, "reason": None}
            target = arrival
        else:
            projected_until = focus_arrival + timedelta(
                days=sm.ORDERING_CYCLE_DAYS)
            boundary = None
            fallback = {
                "active": True,
                "ordering_cycle_days": sm.ORDERING_CYCLE_DAYS,
                "projected_until": projected_until,
                "reason": "no credible later sailing scheduled",
            }
            target = projected_until
        return {
            "focus_sailing_id": sid,
            "focus_arrival": focus_arrival,
            "window_days": max(0, (target - self._today).days),
            "next_planning_boundary": boundary,
            "fallback": fallback,
        }

    def window_for(self, sid, s=None) -> dict:
        """Backward-compatible row/math projection of the canonical selector."""
        canonical = self.planning_window_for(sid, s)
        boundary = canonical["next_planning_boundary"]
        target = (boundary["warehouse_arrival"] if boundary is not None
                  else canonical["fallback"]["projected_until"])
        return {"focus_arrival": canonical["focus_arrival"],
                "next_arrival": target,
                "fallback": canonical["fallback"]["active"],
                "window_days": canonical["window_days"],
                "canonical": canonical}

    # ── matching / buckets (§6.2/§6.3, D5) ----------------------------------

    def _transit_observations(self, s) -> list:
        f = self._feed(s, "in_transit")
        if not f:
            return []
        obs = []
        for i, row in enumerate(self._rows(s, "in_transit")):
            obs.append(mt.Observation(
                obs_id=f"{f['snapshot_id']}:{i}", feed="in_transit",
                product_id=row.get("product_id"),
                raw_reference=str(row.get("product_ref")),
                m2=D(str(row["m2"])), observed_at=f["as_of"],
                event_date=f["as_of"], reference=row.get("reference"),
                eta=row.get("eta"),
                confident_match=bool(row.get("confident_match")),
                snapshot_id=f["snapshot_id"]))
        return obs

    def _exp_views(self, s) -> list:
        views = []
        for e in s.expectations.values():
            refs = {e.exp_id}
            booking = next((b for b in s.bookings.values()
                            if b.booking_id == e.booking_ref
                            or b.booking_ref == e.booking_ref), None)
            if booking:
                refs.add(booking.booking_ref)
            com = s.commitments.get(e.commitment_ref)
            if com:
                refs.add(com.siesa_ref)
            views.append(mt.ExpectationView(
                exp_id=e.exp_id, product_id=e.product_id,
                ask_date=e.ask_date, effective_m2=e.effective_m2,
                received_m2=e.received_m2,
                state=_MT_STATE.get(e.state, e.state),
                closed=e.state in ("arrived", "cancelled"),
                source_refs=frozenset(refs)))
        return views

    def _match(self, s) -> mt.MatchResult:
        return mt.match(self._transit_observations(s), self._exp_views(s))

    def _pending_handoff_quantities(self, s, *, other_than_plan=None) -> list:
        """Finalized, non-superseded handoff quantities whose SIESA
        commitment is not yet observed — the duplicate-allocation window."""
        out = []
        for h in s.handoffs.values():
            if h["superseded"]:
                continue
            plan = s.plans.get(h["plan_id"])
            if plan is None or plan.lifecycle != "finalized":
                continue
            if other_than_plan is not None and h["plan_id"] == other_than_plan:
                continue
            for order in h["handoff"].get("orders", []):
                com = next((c for c in s.commitments.values()
                            if c.handoff_order_ref == order.get("order_ref")
                            and c.state != "released"), None)
                if com is not None and com.state == "observed":
                    continue
                for line in order.get("lines", []):
                    out.append({"plan_id": h["plan_id"],
                                "order_ref": order.get("order_ref"),
                                "product_id": line["product_id"],
                                "m2": D(str(line["m2"]))})
        return out

    def buckets(self, pid, s=None, exclude_plan=None) -> dict:
        s = s or self.state
        wh = self.warehouse_value(pid, s) or ZERO
        avail = ZERO
        for row in self._rows(s, "siesa_availability"):
            if row.get("product_id") == pid:
                avail += D(str(row["available_m2"]))
        # C4: subtract ONLY active exclusions belonging to OTHER plans;
        # observed commitments are never subtracted (the reflecting snapshot
        # already excludes them — never-double-subtract)
        for x in s.exclusions.values():
            if x.active and x.product_id == pid \
                    and (exclude_plan is None or x.plan_ref != exclude_plan):
                avail -= D(str(x.m2))
        committed = ZERO
        for c in s.commitments.values():
            if c.state != "observed":
                continue
            for q in c.quantities:
                if q["product_id"] != pid:
                    continue
                promoted = sum((e.confirmed_m2 for e in
                                s.expectations.values()
                                if e.commitment_ref == c.commitment_id
                                and e.product_id == pid
                                and e.state != "cancelled"), ZERO)
                committed += max(ZERO, D(str(q["m2"])) - promoted)
        expected = sum((e.remaining_m2 for e in s.expectations.values()
                        if e.product_id == pid and e.open), ZERO)
        result = self._match(s)
        obs = {o.obs_id: o for o in self._transit_observations(s)}
        moving = sum((m for oid, m in result.unmatched.items()
                      if obs[oid].product_id == pid), ZERO)
        held = sum((h.m2 for h in result.held
                    if obs[h.obs_id].product_id == pid
                    or obs[h.obs_id].product_id is None), ZERO)
        return {"warehouse": pm.q2(wh),
                "siesa_available_effective": pm.q2(max(ZERO, avail)),
                "committed_origin": pm.q2(committed),
                "expected": pm.q2(expected),
                "moving": pm.q2(moving),
                "held": pm.q2(held)}

    # ── recommendation (§6.4) ----------------------------------------------

    def suggestion_for(self, plan_id, pid, s=None) -> dict:
        s = s or self.state
        plan = s.plans[plan_id]
        sid = plan.sailing_id
        win = self.window_for(sid, s)
        vel = self.velocity(pid, s)
        wh = self.warehouse_value(pid, s)
        siesa_feed = self._feed(s, "siesa_availability")
        b = self.buckets(pid, s, exclude_plan=plan_id)
        if vel is None or wh is None or siesa_feed is None:
            missing = [name for name, val in
                       (("sales", vel), ("warehouse", wh),
                        ("siesa_availability", siesa_feed)) if val is None]
            return {"product_id": pid, "no_basis": True,
                    "missing": missing, "need_m2": pm.q2(ZERO),
                    "suggested_m2": pm.q2(ZERO), "uncovered_m2": pm.q2(ZERO),
                    "capped": False, "watch_only": False,
                    "window_days": win["window_days"],
                    "next_arrival": win["next_arrival"],
                    "fallback": win["fallback"], "trace": {
                        "no_basis_missing_feeds": missing}}
        # [AB3/D5] dated destination supply = warehouse + booking-confirmed
        # expected incoming + verified moving. An observed SIESA commitment
        # is an ORIGIN-side truth: it reduces origin availability through
        # the reflecting snapshot but NEVER offsets destination demand or
        # buffer until booking promotes it to expected incoming.
        dated = wh
        for e in s.expectations.values():
            if e.product_id == pid and e.open:
                arr = self.sailing_timing(e.sailing_id, s).warehouse_arrival \
                    if e.sailing_id in s.sailings else win["next_arrival"]
                if arr <= win["next_arrival"]:
                    dated += e.remaining_m2
        result = self._match(s)
        obs = {o.obs_id: o for o in self._transit_observations(s)}
        for oid, m2 in result.unmatched.items():
            o = obs[oid]
            if o.product_id != pid:
                continue
            arr = (o.eta + timedelta(days=sm.WAREHOUSE_BUFFER_DAYS)
                   if o.eta else o.observed_at + timedelta(
                       days=self.config.default_voyage_days
                       + sm.WAREHOUSE_BUFFER_DAYS))
            if arr <= win["next_arrival"]:
                dated += m2
        buf = self.buffer_m2(pid, s)
        r = sm.recommend(daily_velocity=vel, buffer_m2=buf,
                         window_days=win["window_days"], dated_supply=dated,
                         siesa_available_effective=b[
                             "siesa_available_effective"])
        trace = {
            "inputs": {
                "daily_velocity": {"value": vel, "source": "sales",
                                   "as_of": self._feed(s, "sales")["as_of"]},
                "warehouse_m2": {"value": wh, "source": "warehouse",
                                 "as_of": self._feed(s, "warehouse")["as_of"]},
                "siesa_available_effective": {
                    "value": b["siesa_available_effective"],
                    "source": "siesa_availability",
                    "as_of": siesa_feed["as_of"],
                    "active_exclusions": [
                        {"plan": x.plan_ref, "m2": x.m2}
                        for x in s.exclusions.values()
                        if x.active and x.product_id == pid
                        and x.plan_ref != plan_id]},
                "buffer_m2": {"value": buf,
                              "tier": self.catalog(s)[pid].get("tier")},
                "dated_supply": {"value": pm.q2(dated)},
            },
            "steps": {
                "window_days": win["window_days"],
                "next_arrival": win["next_arrival"],
                "next_arrival_fallback": win["fallback"],
                "projected_demand": pm.q2(vel * D(win["window_days"])),
                "need_m2": r.need_m2,
                "availability_cap": sm.floor_to_half_pallet(
                    b["siesa_available_effective"]),
                "suggested_m2": r.suggested_m2,
                "uncovered_m2": r.uncovered_m2,
                "capped_by_availability": r.capped,
            },
        }
        return {"product_id": pid, "no_basis": False, "missing": [],
                "need_m2": r.need_m2, "suggested_m2": r.suggested_m2,
                "uncovered_m2": r.uncovered_m2, "capped": r.capped,
                "watch_only": r.watch_only,
                "window_days": win["window_days"],
                "next_arrival": win["next_arrival"],
                "fallback": win["fallback"], "trace": trace}

    # ── plan totals / status / attention (D7) --------------------------------

    def _plan_line_order(self, s, plan) -> list:
        return [(pid, D(str(line["selected_m2"])))
                for pid, line in plan.lines.items()]

    def plan_totals(self, plan_id, s=None) -> dict:
        s = s or self.state
        plan = s.plans[plan_id]
        lines = self._plan_line_order(s, plan)
        total = sum((m2 for _, m2 in lines), ZERO)
        pallets = total / pm.M2_PER_PALLET if total else ZERO
        containers = sm.containers_for_pallets(pallets)
        split = s.bl_splits.get(plan_id)
        bl_count = (len(split["groups"]) if split
                    else sm.default_bl_count(containers))
        return {"lines": {pid: str(m2) for pid, m2 in lines},
                "total_m2": str(pm.q2(total)),
                "total_pallets": str(pm.q2(pallets)),
                "containers": containers, "bl_count": bl_count}

    def plan_view(self, plan_id, s=None) -> dict:
        """[AB7] The recompute-comparable derived view of a plan: totals,
        pallets, containers, BL proposal, per-product current
        recommendations, and derived plan status. Backward-compatible with
        the prior recompute payload (totals keys at the top level)."""
        s = s or self.state
        view = dict(self.plan_totals(plan_id, s))
        suggestions = {}
        for pid in self.catalog(s):
            sug = self.suggestion_for(plan_id, pid, s)
            suggestions[pid] = ("no_basis" if sug["no_basis"]
                                else str(sug["suggested_m2"]))
        view["suggestions"] = suggestions
        view["plan_status"] = self.plan_status(plan_id, s)[0]
        return view

    def _scoped(self, s, imp, plan) -> bool:
        sc = imp.scope or {}
        if sc.get("owner") not in (None, "ashley"):
            return False                    # owner-routed away from Ashley
        if sc.get("plan_id") == plan.plan_id:
            return True
        if sc.get("sailing_id") == plan.sailing_id:
            return True
        if sc.get("product_id") in self.catalog(s):
            return True                     # every product is a plan row
        if sc.get("feed") in CRITICAL_FEEDS:
            return True
        if sc.get("raw_ref"):
            return True                     # identity held-outs are
                                            # conservative planning risks
        return False

    def plan_status(self, plan_id, s=None) -> tuple:
        s = s or self.state
        plan = s.plans[plan_id]
        for imp in s.implications.values():
            if imp.state == "open" and imp.severity == "blocking" \
                    and self._scoped(s, imp, plan):
                return "blocked", imp.consequence
        # conservative stop: held identity quantity ≥ the product's own scale
        for pid, line in plan.lines.items():
            held = self.buckets(pid, s, exclude_plan=plan_id)["held"]
            if held > 0 and held >= D(str(line["selected_m2"])):
                return "blocked", (
                    f"La cantidad con identidad ambigua ({held} m²) iguala o "
                    f"supera la cantidad planificada para {pid}; no existe una "
                    "recomendación segura hasta resolverla.")
        for imp in s.implications.values():
            if imp.state == "open" and imp.severity == "consequential" \
                    and self._scoped(s, imp, plan):
                return "provisional", imp.consequence
        return "ready", None

    def attention_for_plan(self, plan_id, s=None) -> list:
        s = s or self.state
        plan = s.plans[plan_id]
        out = []
        for imp in s.implications.values():
            if imp.state == "open" and imp.severity in (
                    "blocking", "consequential") \
                    and self._scoped(s, imp, plan):
                out.append({"implication_id": imp.implication_id,
                            "family": imp.family, "severity": imp.severity,
                            "consequence": imp.consequence,
                            "recommendation": imp.recommendation,
                            "evidence": thaw(imp.evidence),
                            "shipment_effect": thaw(imp.shipment_effect),
                            "typed_actions": list(imp.typed_actions)})
        out.sort(key=lambda c: (c["severity"] != "blocking", c["family"]))
        return out

    def monthly_context(self, s=None) -> list:
        s = s or self.state
        return [dict(e) for e in s.monthly_context.values()
                if not e["superseded"]]

    def _current_sailing_source_event(self, s=None):
        """Return the event/provenance for the latest trusted sailing truth,
        never merely the most recently appended history observation."""
        s = s or self.state
        if not s.sailing_as_of:
            return None, None
        latest_as_of = max(s.sailing_as_of.values())
        for event in reversed(s.events):
            if event.type != "sailing_recorded":
                continue
            payload = thaw(event.payload)
            raw_as_of = payload.get("as_of")
            if isinstance(raw_as_of, str) and raw_as_of.startswith("date:"):
                raw_as_of = date.fromisoformat(raw_as_of[5:])
            sailing_id = payload.get("sailing_id")
            if (raw_as_of == latest_as_of
                    and s.sailing_as_of.get(sailing_id) == latest_as_of):
                return event, latest_as_of
        return None, latest_as_of

    def evidence_readiness(self, s=None) -> list:
        s = s or self.state
        out = []
        for feed in ("sailing_calendar", "siesa_availability", "warehouse",
                     "sales", "in_transit", "committed_orders",
                     "production_planning"):
            if feed == "sailing_calendar":
                have = bool(s.sailings)
                _, as_of = self._current_sailing_source_event(s)
                age = ((self._today - as_of).days if as_of is not None else None)
                if not have:
                    status = "missing"
                elif age is not None and age > self.config.freshness_stale_days:
                    status = "stale"
                elif age is not None and age > self.config.freshness_aging_days:
                    status = "aging"
                else:
                    status = "fresh"
                out.append({"source": feed, "as_of": as_of,
                            "age_days": age, "status": status})
                continue
            f = s.feeds.get(feed)
            if not f:
                out.append({"source": feed, "as_of": None, "age_days": None,
                            "status": "missing"})
                continue
            age = (self._today - f["as_of"]).days
            if age > self.config.freshness_stale_days:
                status = "stale"
            elif age > self.config.freshness_aging_days:
                status = "aging"
            else:
                status = "fresh"
            out.append({"source": feed, "as_of": f["as_of"],
                        "age_days": age, "status": status})
        return out

    # ═══════════════ command handlers (working-state only) ═════════════════

    def _cmd_RunChecks(self, s, p, *, actor, on):
        return {}

    # -- evidence loads ------------------------------------------------------

    def _load_feed(self, s, feed, p, *, actor, on):
        as_of = p["as_of"]
        errors: list = []
        if "text" in p and p["text"] is not None:
            raw_rows, errors = sn.parse_quantity_table_text(feed, p["text"])
            entered_via = "parsed"
        else:
            raw_rows = p.get("rows", [])
            entered_via = "direct"
        rows = sn.normalize_quantity_rows(
            feed, raw_rows, products=list(self.catalog(s).values()),
            entered_via=entered_via, mappings=s.mappings)
        # [FC1] deterministic duplicate handling at the load boundary:
        # additive feeds merge under the formal law; non-additive reject
        sn.reject_duplicate_product_rows(feed, rows)
        rows = sn.merge_duplicate_product_rows(feed, rows)
        snap_id = sd.load_evidence(s, feed=feed, as_of=as_of, rows=rows,
                                  entered_via=entered_via, actor=actor, on=on,
                                  raw_source_ref=p.get("raw_source_ref"))
        return {"snapshot_id": snap_id, "rows": len(rows),
                "row_errors": errors}

    def _cmd_LoadSiesaSnapshot(self, s, p, *, actor, on):
        return self._load_feed(s, "siesa_availability", p, actor=actor, on=on)

    def _cmd_LoadWarehouseSnapshot(self, s, p, *, actor, on):
        prev = {row.get("product_id"): D(str(row["m2"]))
                for row in self._rows(s, "warehouse")
                if row.get("product_id")}
        result = self._load_feed(s, "warehouse", p, actor=actor, on=on)
        # arrival attribution (§6.3): product-level increases attribute to
        # open expectations in candidate order, atomically with the load
        for row in self._rows(s, "warehouse"):
            pid = row.get("product_id")
            if not pid:
                continue
            increase = D(str(row["m2"])) - prev.get(pid, ZERO)
            if increase <= 0:
                continue
            open_exps = sorted(
                (e for e in s.expectations.values()
                 if e.product_id == pid and e.open and e.remaining_m2 > 0),
                key=lambda e: (e.ask_date, e.remaining_m2, e.exp_id))
            left = increase
            for e in open_exps:
                take = min(left, e.remaining_m2)
                if take > 0:
                    sd.attribute_arrival_to(s, exp_id=e.exp_id, m2=take,
                                            actor=actor, on=on)
                    left -= take
                if left <= 0:
                    break
        return result

    def _cmd_LoadSalesSnapshot(self, s, p, *, actor, on):
        return self._load_feed(s, "sales", p, actor=actor, on=on)

    def _cmd_LoadTransitSnapshot(self, s, p, *, actor, on):
        return self._load_feed(s, "in_transit", p, actor=actor, on=on)

    def _cmd_LoadCommittedOrders(self, s, p, *, actor, on):
        return self._load_feed(s, "committed_orders", p, actor=actor, on=on)

    def _cmd_LoadProductionPlanning(self, s, p, *, actor, on):
        return self._load_feed(s, "production_planning", p, actor=actor, on=on)

    # -- sailings ------------------------------------------------------------

    def _cmd_RecordSailing(self, s, p, *, actor, on):
        row = sn.normalize_sailing_row(
            {"carrier": p["carrier"], "name": p["name"],
             "departure": p.get("departure"),
             "voyage_days": p.get("voyage_days"), "voyage": p.get("voyage"),
             "loading_terminal_eta": p.get("loading_terminal_eta"),
             "bl_vgm_close": p.get("bl_vgm_close"),
             "saes_reception": p.get("saes_reception"),
             "terminal": p.get("terminal"),
             "planning_basis": p.get("planning_basis", "departure")},
            entered_via="direct")
        sid = sd.record_sailing(s, carrier=row["carrier"], name=row["name"],
                                departure=row["departure"],
                                voyage_days=row["voyage_days"],
                                voyage=row["voyage"],
                                loading_terminal_eta=row["loading_terminal_eta"],
                                bl_vgm_close=row["bl_vgm_close"],
                                saes_reception=row["saes_reception"],
                                terminal=row["terminal"],
                                planning_basis=row["planning_basis"],
                                entered_via="direct", actor=actor, on=on,
                                as_of=p.get("as_of"),
                                raw_source_ref=p.get("raw_source_ref"))
        return {"sailing_id": sid}

    def _cmd_ImportSailingCalendarText(self, s, p, *, actor, on):
        rows, errors = sn.parse_sailing_calendar_text(p["text"])
        ids = []
        for r in rows:
            row = sn.normalize_sailing_row(r, entered_via="parsed")
            ids.append(sd.record_sailing(
                s, carrier=row["carrier"], name=row["name"],
                departure=row["departure"], voyage_days=row["voyage_days"],
                entered_via="parsed", actor=actor, on=on,
                as_of=p.get("as_of"),
                raw_source_ref=p.get("raw_source_ref")))
        return {"sailing_ids": ids, "row_errors": errors}

    def _cmd_SetSailingDecision(self, s, p, *, actor, on):
        sd.record_sailing_decision(s, sailing_id=p["sailing_id"],
                                   decision=p["decision"], actor=actor, on=on)
        return {}

    def _cmd_PursueExceptionalSailing(self, s, p, *, actor, on):
        sid = p["sailing_id"]
        sd.record_exceptional_pursuit(s, sailing_id=sid, actor=actor, on=on)
        self._ensure_pursuit_case(s, sid, actor=actor, on=on)
        return {}

    def _ensure_pursuit_case(self, s, sid, *, actor, on):
        """[FCB2/FCB2A/FCB2C] Materialize the governing risk case for
        the ACTIVE pursuit generation. Only a CANONICAL matching case —
        the shared `sailing_domain.canonical_pursuit_case` law, verified
        against the active authority object — counts at all: a malformed
        or unrelated same-subject case (even one stamped with the
        current generation) never suppresses creation of the canonical
        current-evidence case. Canonical identity alone is NOT enough —
        the short-circuit is lifecycle-STATE-aware [FCB2C]: it stands
        only on an OPEN canonical case (Ashley already has the
        actionable governing case) or on a legitimately RESOLVED
        canonical acceptance under the shared acceptance law
        (`sailing_domain.pursuit_risk_accepted`) — both arms keep the
        explicit same-event retry idempotent. A `superseded` canonical
        case is historical proof only: it never suppresses
        materialization of one new open canonical case for the
        still-active generation, and recovery preserves it as history
        (one superseded historical case plus exactly one open canonical
        case is the valid recovered shape). A fresh generation
        supersedes any stale open pursuit case and opens a case bound to
        the new generation's evidence."""
        if any(i.state == "open" and sd.canonical_pursuit_case(s, sid, i)
               for i in s.implications.values()):
            return
        if sd.pursuit_risk_accepted(s, sid):
            return
        self._supersede_if_open(
            s, f"pursuit:{sid}",
            "superseded by a fresh exceptional-pursuit generation — its "
            "risk decision must be taken on current evidence [FCB2]",
            actor=actor, on=on)
        self._open_pursuit_case(s, sid, actor=actor, on=on)

    def _open_pursuit_case(self, s, sid, *, actor, on):
        sl = s.sailings[sid]
        # [FCB2] the case is BOUND to the active pursuit authority object:
        # its scope carries the pursuit generation and its evidence IS the
        # generation's governing evidence, so an acceptance can never
        # outlive the pursuit event it judged
        auth = s.pursuits[sid]
        # [FCB2B] resolution standing at THIS boundary uses the shared
        # canonical-pursuit-case law: only a CANONICAL case matching the
        # active authority object may stand as the resolution for the
        # canonical case.  An unrelated or malformed implication that
        # copies the pursuit subject, resolution action, serialized
        # evidence, generation, or any combination of those fields never
        # suppresses materialization of the canonical current-evidence
        # case — the predicate verifies against the authority object,
        # never naming convention, insertion order, evidence alone, or
        # caller discipline.
        self._open(s, actor, on,
                   standing_law=lambda imp:
                   sd.canonical_pursuit_case(s, sid, imp),
                   family="decision_required",
                   severity="consequential", subject_key=f"pursuit:{sid}",
                   scope={"sailing_id": sid, "owner": "ashley",
                          "pursuit_generation": auth["generation"]},
                   evidence=[{"sailing": sl.name,
                              "departure": auth["departure"],
                              "days_to_departure":
                              auth["days_to_departure"],
                              "pursuit_generation": auth["generation"]}],
                   consequence=(
                       f"El zarpe {sl.name} está a menos de 10 días de salir; "
                       "considerarlo es una excepción con riesgo, fuera de la "
                       "recomendación normal."),
                   recommendation=("Confirma la excepción solo si una referencia "
                                   "urgente requiere este zarpe; de lo contrario, "
                                   "usa el siguiente zarpe normal."),
                   shipment_effect={"note": "El plan avanza solo después de "
                                    "aceptar explícitamente el riesgo."},
                   typed_actions=["accept_risk", "cancel_pursuit"])

    def _require_advanceable(self, s, sailing_id, *, verb):
        """[RC4/FC4/FC5] Delegates to the SINGLE shared authority law in
        the domain (`sailing_domain.advancement_denial`): at the hard
        cutoff and later, a plan opens/advances/finalizes only with the
        active explicit pursuit AND its accepted risk decision; nothing
        advances after departure. Wrapper and domain can never contradict
        because both consult the same law."""
        reason = sd.advancement_denial(s, sailing_id, self._today)
        if reason:
            raise DomainError(reason)

    def _require_plan_advanceable(self, s, plan_id, *, verb):
        plan = s.plans.get(plan_id)
        if plan is None:
            raise DomainError(f"unknown plan {plan_id}")
        self._require_advanceable(s, plan.sailing_id, verb=verb)

    # -- planning ------------------------------------------------------------

    def _cmd_OpenShipmentPlan(self, s, p, *, actor, on):
        sid = p["sailing_id"]
        decision = s.sailing_decisions.get(sid)
        if decision != "use":
            raise DomainError(
                f"open a plan only for a sailing decided 'use' "
                f"(current decision: {decision or 'watch'})")
        self._require_advanceable(s, sid, verb="open")   # [RC4]
        plan_id = sd.open_plan(s, sailing_id=sid, actor=actor, on=on)
        return {"plan_id": plan_id}

    def _snapshot_suggestion(self, s, plan_id, pid, *, actor, on) -> tuple:
        sug = self.suggestion_for(plan_id, pid, s)
        ref = sd.record_suggestion_snapshot(
            s, plan_id=plan_id, product_id=pid,
            content={"suggested_m2": sug["suggested_m2"],
                     "need_m2": sug["need_m2"],
                     "uncovered_m2": sug["uncovered_m2"],
                     "trace": sug["trace"]},
            actor=actor, on=on)
        return sug, ref

    def _cmd_AcceptSuggestion(self, s, p, *, actor, on):
        plan_id, pid = p["plan_id"], p["product_id"]
        if pid not in self.catalog(s):
            raise DomainError(f"unknown product {pid}")
        self._require_plan_advanceable(s, plan_id, verb="advance")  # [RC4]
        sug, ref = self._snapshot_suggestion(s, plan_id, pid,
                                             actor=actor, on=on)
        if sug["no_basis"] or sug["suggested_m2"] <= 0:
            raise DomainError(
                f"no positive suggestion exists for {pid} — nothing to "
                "accept (edit or add manually instead)")
        sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                action="accept",
                                selected_m2=sug["suggested_m2"],
                                origin="suggestion", snapshot_ref=ref,
                                actor=actor, on=on)
        return {"selected_m2": sug["suggested_m2"], "snapshot_ref": ref}

    def _cmd_AcceptAllSuggestions(self, s, p, *, actor, on):
        plan_id = p["plan_id"]
        self._require_plan_advanceable(s, plan_id, verb="advance")  # [RC4]
        accepted = []
        for pid in self.catalog(s):
            sug = self.suggestion_for(plan_id, pid, s)
            if not sug["no_basis"] and sug["suggested_m2"] > 0:
                self._cmd_AcceptSuggestion(
                    s, {"plan_id": plan_id, "product_id": pid},
                    actor=actor, on=on)
                accepted.append(pid)
        return {"accepted": accepted}

    def _cmd_EditSelectedM2(self, s, p, *, actor, on):
        plan_id, pid = p["plan_id"], p["product_id"]
        if pid not in self.catalog(s):
            raise DomainError(f"unknown product {pid}")
        self._require_plan_advanceable(s, plan_id, verb="advance")  # [RC4]
        _, ref = self._snapshot_suggestion(s, plan_id, pid, actor=actor,
                                           on=on)
        sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                action="edit", selected_m2=D(str(p["m2"])),
                                origin="edited", snapshot_ref=ref,
                                actor=actor, on=on)
        return {}

    def _cmd_EditSelectedM2Batch(self, s, p, *, actor, on):
        """Apply every edit to the bus working state; publish only on full success."""
        edited = []
        for edit in p["edits"]:
            if D(str(edit["m2"])) == ZERO:
                self._cmd_PostponeProduct(
                    s, {"plan_id": p["plan_id"],
                        "product_id": edit["product_id"]}, actor=actor, on=on)
            else:
                self._cmd_EditSelectedM2(
                    s, {"plan_id": p["plan_id"], "product_id": edit["product_id"],
                        "m2": edit["m2"]}, actor=actor, on=on)
            edited.append(edit["product_id"])
        return {"edited": edited}

    def _cmd_PostponeProduct(self, s, p, *, actor, on):
        sd.record_plan_decision(s, plan_id=p["plan_id"],
                                product_id=p["product_id"],
                                action="postpone", selected_m2=None,
                                origin=None, snapshot_ref=None,
                                actor=actor, on=on)
        return {}

    def _cmd_AddManualLine(self, s, p, *, actor, on):
        plan_id, pid = p["plan_id"], p["product_id"]
        if pid not in self.catalog(s):
            raise DomainError(f"unknown product {pid}")
        self._require_plan_advanceable(s, plan_id, verb="advance")  # [RC4]
        m2 = D(str(p["m2"]))
        sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                action="manual", selected_m2=m2,
                                origin="manual", snapshot_ref=None,
                                actor=actor, on=on)
        avail = self.buckets(pid, s, exclude_plan=plan_id)[
            "siesa_available_effective"]
        if m2 > avail:
            self._open_manual_over(s, plan_id, pid, m2, avail,
                                   actor=actor, on=on)
        return {}

    def _open_manual_over(self, s, plan_id, pid, m2, avail, *, actor, on):
        self._open(
            s, actor, on, family="decision_required",
            severity="consequential",
            subject_key=f"manual_over:{plan_id}:{pid}",
            scope={"plan_id": plan_id, "product_id": pid,
                   "owner": "ashley"},
            evidence=[{"selected_m2": str(m2),
                       "siesa_available_effective": str(avail),
                       "source": "siesa_availability"}],
            consequence=(
                f"{pid}: los {m2} m² seleccionados superan los {avail} m² "
                "visibles como disponibles en SIESA; registrar esta cantidad "
                "puede fallar o consumir inventario comprometido."),
            recommendation=(f"Reduce la selección a un máximo de {avail} m² o "
                            "actualiza primero la disponibilidad SIESA."),
            shipment_effect={"product_id": pid,
                             "over_availability_m2": str(m2 - avail)},
            typed_actions=["reduce_to_available", "accept_risk"])

    def _cmd_OverrideBLSplit(self, s, p, *, actor, on):
        plan_id = p["plan_id"]
        plan = s.plans.get(plan_id)
        if plan is None:
            raise DomainError(f"unknown plan {plan_id}")
        if plan.lifecycle != "draft":
            raise DomainError(
                f"plan is {plan.lifecycle}: the finalized handoff is the "
                "order of record — BL changes after finalization require "
                "explicit reallocation-intent confirmation and a draft "
                "reopen first (§7.3) [AB4]")
        self._require_plan_advanceable(s, plan_id, verb="advance")  # [RC4]
        lines = self._plan_line_order(s, plan)
        groups = thaw(p["groups"])
        for g in groups:
            for l in g["lines"]:
                l["m2"] = D(str(l["m2"]))
        sm.validate_bl_conservation(lines, groups)
        sd.set_bl_split(s, plan_id=plan_id, groups=groups, origin="override",
                        actor=actor, on=on)
        return {}

    def _cmd_FinalizeSailingPlan(self, s, p, *, actor, on):
        plan_id = p["plan_id"]
        plan = s.plans.get(plan_id)
        if plan is None:
            raise DomainError(f"unknown plan {plan_id}")
        self._require_plan_advanceable(s, plan_id, verb="finalize")  # [RC4]
        status, reason = self.plan_status(plan_id, s)
        if status == "blocked":
            raise DomainError(
                f"finalization denied: plan is blocked — {reason}")
        open_attention = self.attention_for_plan(plan_id, s)
        if open_attention:
            raise DomainError(
                "finalization denied: unresolved implication(s) require "
                "resolution or explicit risk acceptance first [D7]: "
                + "; ".join(c["consequence"][:80] for c in open_attention))
        lines = self._plan_line_order(s, plan)
        split = s.bl_splits.get(plan_id)
        if split is None or split["origin"] == "default":
            groups = sm.default_bl_groups(lines)
            sd.set_bl_split(s, plan_id=plan_id, groups=groups,
                            origin="default", actor=actor, on=on)
        else:
            groups = thaw(split["groups"])
            for g in groups:
                for l in g["lines"]:
                    l["m2"] = D(str(l["m2"]))
            sm.validate_bl_conservation(lines, groups)
        orders = []
        for g in groups:
            orders.append({
                "order_no": g["bl_no"],
                # order_ref is stamped by the domain at finalization with
                # the handoff-generation identity [RC2]
                "containers": g["container_count"],
                "lines": [{"product_id": l["product_id"],
                           "m2": D(str(l["m2"])),
                           "pallets": str(pm.q2(D(str(l["m2"]))
                                                / pm.M2_PER_PALLET))}
                          for l in g["lines"]]})
        totals = self.plan_totals(plan_id, s)
        uncovered = []
        for pid in self.catalog(s):
            sug = self.suggestion_for(plan_id, pid, s)
            if not sug["no_basis"] and sug["uncovered_m2"] > 0:
                uncovered.append({"product_id": pid,
                                  "m2": sug["uncovered_m2"]})
        timing = self.sailing_timing(plan.sailing_id, s)
        handoff = {"sailing_id": plan.sailing_id,
                   "orders": orders, "totals": totals}
        hid = sd.finalize_plan(s, plan_id=plan_id, handoff=handoff,
                               uncovered=uncovered,
                               late_state=timing.timing_state,
                               actor=actor, on=on)
        return {"handoff_id": hid, "orders": len(orders),
                "late_state": timing.timing_state}

    def _production_recommendations(self, plan_id, s):
        plan = s.plans.get(plan_id)
        if plan is None:
            raise DomainError(f"unknown plan {plan_id}")
        rows = []
        required_by = None
        for pid in self.catalog(s):
            suggestion = self.suggestion_for(plan_id, pid, s)
            if suggestion["no_basis"]:
                continue
            selected = D(str(plan.lines.get(pid, {}).get("selected_m2", ZERO)))
            recommendation = sm.floor_to_half_pallet(
                max(ZERO, suggestion["need_m2"] - selected))
            if recommendation <= 0:
                continue
            rows.append({"product_id": pid,
                         "selected_m2": recommendation,
                         "recommendation_m2": recommendation,
                         "origin": "production_recommendation"})
            boundary = suggestion.get("next_arrival")
            if boundary is not None and (required_by is None or boundary < required_by):
                required_by = boundary
        return rows, required_by

    def _cmd_OpenProductionOrder(self, s, p, *, actor, on):
        from .sailing_read_model import (compose_workspace,
                                         source_evidence_fingerprint)
        plan = s.plans.get(p["plan_id"])
        if plan is None:
            raise DomainError(f"unknown plan {p['plan_id']}")
        if any(order.source_plan_id == p["plan_id"]
               for order in s.production_orders.values()):
            raise DomainError("an active production order already exists for this plan")
        candidate = compose_workspace(
            self, plan_id=p["plan_id"],
            production_anchor_sailing_id=p["production_anchor_sailing_id"],
            factory_order_date=p["factory_order_date"], principal=object(),
            _candidate_only=True, _state=s)
        if candidate is None:
            raise DomainError("no positive production recommendation exists")
        if not hmac.compare_digest(candidate["candidate_fingerprint"],
                                   p["candidate_fingerprint"]):
            raise DomainError("candidate_changed")
        order_date = (p["factory_order_date"]
                      if isinstance(p["factory_order_date"], date)
                      else date.fromisoformat(str(p["factory_order_date"])))
        anchor_readiness = date.fromisoformat(candidate["anchor_production_readiness"])
        order_timing_state = (
            "past_due" if order_date < self.today else
            "late_for_anchor" if order_date > anchor_readiness else "on_time")
        review = getattr(self, "review_provenance", {})
        planning_provenance = {
            "focus_sailing_id": plan.sailing_id,
            "production_anchor_sailing_id": candidate[
                "production_anchor_sailing_id"],
            "coverage_boundary_sailing_id": candidate[
                "coverage_boundary_sailing_id"],
            "anchor_production_readiness": candidate[
                "anchor_production_readiness"],
            "coverage_through": candidate["coverage_through"],
            "order_timing_state": order_timing_state,
            "review_mode": review.get("mode"),
            "review_as_of": str(review.get("as_of", self.today)),
            "current_truth": review.get("current_truth", False),
            "source_evidence_fingerprint": source_evidence_fingerprint(self, s),
        }
        order_id = sd.open_production_order(
            s, source_plan_id=p["plan_id"],
            cycle_as_of=date.fromisoformat(candidate["cycle_as_of"]),
            factory_order_date=order_date, required_by=anchor_readiness,
            calculation_head_seq=candidate["calculation_head_seq"],
            candidate_fingerprint=candidate["candidate_fingerprint"],
            planning_provenance=planning_provenance,
            lines=candidate["recommendation_rows"], actor=actor, on=on)
        return {"production_order_id": order_id,
                "plan_id": p["plan_id"],
                "production_anchor_sailing_id": p["production_anchor_sailing_id"],
                "factory_order_date": order_date.isoformat(),
                "candidate_fingerprint": candidate["candidate_fingerprint"]}

    def _cmd_EditProductionOrderBatch(self, s, p, *, actor, on):
        if any(edit.get("product_id") not in self.catalog(s)
               for edit in p.get("edits", [])):
            raise DomainError("production edit product is not in the catalog")
        sd.edit_production_order_batch(
            s, production_order_id=p["production_order_id"],
            edits=p["edits"], actor=actor, on=on)
        return {"edited": [edit["product_id"] for edit in p["edits"]]}

    def _cmd_FinalizeProductionOrder(self, s, p, *, actor, on):
        sd.finalize_production_order(
            s, production_order_id=p["production_order_id"], actor=actor, on=on)
        return {"production_order_id": p["production_order_id"]}

    def _cmd_RecordProductionOrderReference(self, s, p, *, actor, on):
        sd.record_production_order_reference(
            s, production_order_id=p["production_order_id"],
            production_ref=p["production_ref"], actor=actor, on=on)
        return {"production_order_id": p["production_order_id"],
                "production_ref": p["production_ref"]}

    def _cmd_ReopenSailingPlan(self, s, p, *, actor, on):
        sd.reopen_sailing_plan(s, plan_id=p["plan_id"], actor=actor, on=on)
        return {"plan_id": p["plan_id"]}

    def _cmd_ReopenProductionOrder(self, s, p, *, actor, on):
        sd.reopen_production_order(
            s, production_order_id=p["production_order_id"], actor=actor, on=on)
        return {"production_order_id": p["production_order_id"]}

    def _amendment_baseline(self, s, order_kind, order_id):
        if order_kind == "shipment":
            plan = s.plans.get(order_id)
            if plan is None or not plan.handoff_id or plan.handoff_id not in s.handoffs:
                raise DomainError(f"unknown finalized shipment plan {order_id}")
            handoff = s.handoffs[plan.handoff_id]
            order_refs = {
                order.get("order_ref")
                for generation in s.handoffs.values()
                if generation["plan_id"] == order_id
                for order in generation["handoff"].get("orders", [])
            }
            refs = sorted({c.siesa_ref for c in s.commitments.values()
                           if c.handoff_order_ref in order_refs})
            totals = {}
            for order in handoff["handoff"].get("orders", []):
                for line in order.get("lines", []):
                    pid = line["product_id"]
                    totals[pid] = totals.get(pid, ZERO) + D(str(line["m2"]))
            lines = [{"product_id": pid, "baseline_m2": pm.q2(m2),
                      "selected_m2": pm.q2(m2)} for pid, m2 in totals.items()]
            return lines, refs
        if order_kind == "production":
            order = s.production_orders.get(order_id)
            if order is None:
                raise DomainError(f"unknown production order {order_id}")
            refs = [order.production_ref] if order.production_ref else []
            lines = [{"product_id": pid,
                      "baseline_m2": pm.q2(D(str(line["selected_m2"]))),
                      "selected_m2": pm.q2(D(str(line["selected_m2"])))}
                     for pid, line in order.lines.items()]
            return lines, refs
        raise DomainError("order_kind must be shipment or production")

    def _cmd_OpenOrderAmendment(self, s, p, *, actor, on):
        lines, refs = self._amendment_baseline(s, p["order_kind"], p["order_id"])
        amendment_id = sd.open_order_amendment(
            s, order_kind=p["order_kind"], order_id=p["order_id"],
            baseline_lines=lines, original_refs=refs, actor=actor, on=on)
        return {"amendment_id": amendment_id}

    def _cmd_EditOrderAmendmentBatch(self, s, p, *, actor, on):
        amendment = s.order_amendments.get(p["amendment_id"])
        if amendment is None:
            raise DomainError(f"unknown order amendment {p['amendment_id']}")
        if any(edit.get("product_id") not in amendment.lines
               for edit in p.get("edits", [])):
            raise DomainError("amendment edit product is not in the original order")
        sd.edit_order_amendment_batch(
            s, amendment_id=p["amendment_id"], edits=p["edits"],
            actor=actor, on=on)
        return {"edited": [edit["product_id"] for edit in p["edits"]]}

    def _cmd_FinalizeOrderAmendment(self, s, p, *, actor, on):
        sd.finalize_order_amendment(
            s, amendment_id=p["amendment_id"], actor=actor, on=on)
        return {"amendment_id": p["amendment_id"]}

    def _cmd_RecordOrderAmendmentReference(self, s, p, *, actor, on):
        sd.record_order_amendment_reference(
            s, amendment_id=p["amendment_id"],
            amendment_ref=p["amendment_ref"], actor=actor, on=on)
        return {"amendment_id": p["amendment_id"],
                "amendment_ref": p["amendment_ref"]}

    def _cmd_RequestReallocation(self, s, p, *, actor, on):
        plan_id = p["plan_id"]
        plan = s.plans.get(plan_id)
        if plan is None or plan.lifecycle != "finalized":
            raise DomainError(
                "reallocation applies to a finalized plan — draft plans are "
                "edited directly")
        totals = self.plan_totals(plan_id, s)
        iid = self._open(
            s, actor, on, family="reallocation_intent",
            severity="consequential",
            subject_key=f"realloc:{plan_id}",
            scope={"plan_id": plan_id, "owner": "ashley"},
            evidence=[{"handoff_id": plan.handoff_id,
                       "current_totals": totals}],
            consequence=(
                "El traspaso es el registro vigente; cambiar cantidades o el "
                "reparto por grupo BL después de finalizar requiere confirmar "
                "explícitamente la intención."),
            recommendation="Confirma la reapertura; al finalizar de nuevo se "
                           "creará un traspaso sucesor vinculado al anterior.",
            shipment_effect={"before_totals": totals},
            typed_actions=["confirm_reopen", "keep_plan"])
        return {"implication_id": iid}

    def _cmd_ConfirmReallocationIntent(self, s, p, *, actor, on):
        sd.confirm_reallocation(s, plan_id=p["plan_id"], actor=actor, on=on)
        return {}

    def _cmd_CloseSailingPlan(self, s, p, *, actor, on):
        plan_id = p["plan_id"]
        plan = s.plans.get(plan_id)
        if plan is None:
            raise DomainError(f"unknown plan {plan_id}")
        timing = self.sailing_timing(plan.sailing_id, s)
        departed = timing.timing_state == "departed"
        if plan.lifecycle == "finalized":
            # [AB4] finalized → closed is allowed only after departure;
            # pre-departure abandonment applies only to a draft
            if not departed:
                raise DomainError(
                    "a finalized plan closes only once the sailing has "
                    "departed — before departure, changing course goes "
                    "through reallocation-intent reopen, never a close "
                    "[AB4]")
            reason = "closed"
        else:
            reason = "abandoned"      # draft abandon (pre- or post-departure)
        sd.close_plan(s, plan_id=plan_id, reason=reason, actor=actor, on=on)
        return {"reason": reason}

    # -- layered allocation (D5) --------------------------------------------

    def _cmd_RecordSiesaOrderReference(self, s, p, *, actor, on):
        order_ref = p["handoff_order_ref"]
        handoff_order = None
        for h in s.handoffs.values():
            if h["superseded"]:
                continue
            for o in h["handoff"].get("orders", []):
                if o.get("order_ref") == order_ref:
                    handoff_order = o
        if handoff_order is None:
            raise DomainError(f"unknown handoff order {order_ref}")
        default_qty = [{"product_id": l["product_id"], "m2": D(str(l["m2"]))}
                       for l in handoff_order["lines"]]
        quantities = p.get("quantities") or default_qty
        quantities = [{"product_id": q["product_id"], "m2": D(str(q["m2"]))}
                      for q in quantities]
        siesa_feed = self._feed(s, "siesa_availability")
        cid = sd.record_siesa_reference(
            s, handoff_order_ref=order_ref, siesa_ref=p["siesa_ref"],
            quantities=quantities, actor=actor, on=on,
            baseline_snapshot_id=(siesa_feed["snapshot_id"]
                                  if siesa_feed else None))
        # entered quantities materially different from the handoff → the
        # difference looks intentional → reallocation-intent confirmation
        want = {q["product_id"]: q["m2"] for q in default_qty}
        got = {q["product_id"]: q["m2"] for q in quantities}
        for pid in set(want) | set(got):
            diff = abs(want.get(pid, ZERO) - got.get(pid, ZERO))
            if diff > 0 and not self._tolerated(diff, want.get(pid, ZERO)):
                self._open(
                    s, actor, on, family="reallocation_intent",
                    severity="consequential",
                    subject_key=f"ref_differs:{cid}:{pid}",
                    scope={"product_id": pid, "owner": "ashley",
                           "commitment_id": cid,
                           "plan_id": self._plan_of_order(s, order_ref)},
                    evidence=[{"handoff_m2": str(want.get(pid, ZERO)),
                               "entered_m2": str(got.get(pid, ZERO)),
                               "siesa_ref": p["siesa_ref"]}],
                    consequence=(
                        f"{pid}: el registro SIESA ({got.get(pid, ZERO)} m²) "
                        f"difiere materialmente del traspaso "
                        f"({want.get(pid, ZERO)} m²); esto parece un cambio "
                        "intencional al plan vigente."),
                    recommendation="Confirma la reasignación, reabre y vuelve a "
                                   "finalizar, o corrige el registro.",
                    shipment_effect={"product_id": pid,
                                     "delta_m2": str(got.get(pid, ZERO)
                                                     - want.get(pid, ZERO))},
                    typed_actions=["confirm_reopen", "keep_plan",
                                   "accept_risk"])
        return {"commitment_id": cid}

    def _cmd_RecordBookingConfirmation(self, s, p, *, actor, on):
        # [AB5] full association validation BEFORE any event append: the
        # booking's sailing must exist and match the active handoff
        # plan/sailing of EVERY linked commitment; commitments must exist,
        # be observed, and not be released (domain re-checks the state
        # rules). Mixed-sailing payloads therefore fail atomically.
        sailing_id = p["sailing_id"]
        if sailing_id not in s.sailings:
            raise DomainError(
                f"unknown sailing {sailing_id!r} — a booking must confirm "
                "space on a recorded sailing [AB5]")
        for cref in list(p["commitment_refs"]):
            c = s.commitments.get(cref)
            if c is None:
                raise DomainError(f"unknown commitment {cref} [AB5]")
            plan_id = self._active_plan_of_order(s, c.handoff_order_ref)
            plan = s.plans.get(plan_id) if plan_id else None
            if plan is None:
                raise DomainError(
                    f"commitment {cref} does not belong to any active "
                    "(non-superseded) handoff plan — a reopened plan's "
                    "commitments cannot be booked until re-finalization "
                    "[AB5]")
            if plan.sailing_id != sailing_id:
                raise DomainError(
                    f"commitment {cref} belongs to plan {plan.plan_id} on "
                    f"sailing {plan.sailing_id}, not {sailing_id} — the "
                    "booking must match every linked commitment's active "
                    "handoff sailing [AB5]")
        bid = sd.record_booking(s, booking_ref=p["booking_ref"],
                                sailing_id=sailing_id,
                                commitment_refs=list(p["commitment_refs"]),
                                actor=actor, on=on)
        return {"booking_id": bid}

    def _cmd_RecordBlDeparture(self, s, p, *, actor, on):
        booking = next((b for b in s.bookings.values()
                        if b.booking_ref == p.get("booking_ref")
                        or b.booking_id == p.get("booking_ref")), None)
        if booking is None:
            raise DomainError("unknown booking")
        advanced = False
        for e in s.expectations.values():
            if e.booking_ref == booking.booking_id and e.state == "booked":
                sd.advance_expectation(s, exp_id=e.exp_id,
                                       new_state="departed",
                                       evidence_ref=p.get("bl_ref", "bl"),
                                       actor=actor, on=on)
                advanced = True
        if not advanced:
            raise DomainError(
                f"booking {booking.booking_ref} has no booked expectations "
                "remaining to depart")
        return {}

    # -- corrections (Elicio-only) ------------------------------------------

    def _cmd_CorrectObservedCommitment(self, s, p, *, actor, on):
        cid = p["commitment_id"]
        if p.get("action") != "release":
            raise DomainError("supported correction action: release")
        com = s.commitments.get(cid)
        if com is None:
            raise DomainError(f"unknown commitment {cid}")
        sd.release_commitment(s, commitment_id=cid, note=p.get("note", ""),
                              actor=actor, on=on)
        for e in list(s.expectations.values()):
            if e.commitment_ref == cid and e.open:
                sd.cancel_expectation(s, exp_id=e.exp_id,
                                      note=f"commitment {cid} released",
                                      actor=actor, on=on)
        # [AB2] release lifts ONLY the protection bound to this
        # commitment's handoff order, by exactly the released quantities —
        # unrelated plans'/products' exclusions are untouched
        self._consume_exclusions_for(
            s, order_ref=com.handoff_order_ref,
            quantities=com.quantities,
            reason=f"commitment {cid} released", actor=actor, on=on)
        return {}

    def _consume_exclusions_for(self, s, *, order_ref, quantities, reason,
                                actor, on):
        """[AB2] Decrement active exclusions bound to `order_ref` by the
        supported per-product quantities, in the same atomic step; fully
        consumed exclusions auto-clear (never both excluded and
        reflected)."""
        if order_ref is None:
            return
        by_pid = {}
        for q in quantities:
            by_pid[q["product_id"]] = by_pid.get(q["product_id"], ZERO) \
                + D(str(q["m2"]))
        for x in list(s.exclusions.values()):
            if not x.active or x.order_ref != order_ref:
                continue
            supported = by_pid.get(x.product_id, ZERO)
            if supported <= 0:
                continue
            take = min(D(str(x.m2)), supported)
            sd.reduce_exclusion(s, exclusion_id=x.exclusion_id, by_m2=take,
                                reason=reason, actor=actor, on=on)
            by_pid[x.product_id] = supported - take

    def _cmd_CorrectExpectation(self, s, p, *, actor, on):
        if p.get("action") != "cancel":
            raise DomainError("supported correction action: cancel")
        sd.cancel_expectation(s, exp_id=p["exp_id"],
                              note=p.get("note", ""), actor=actor, on=on)
        return {}

    # -- implication resolution (D7) ----------------------------------------

    def _cmd_AcceptImplicationRisk(self, s, p, *, actor, on):
        return self._cmd_ResolveImplication(
            s, {"implication_id": p["implication_id"], "action": "accept_risk",
                "params": {"note": p.get("note")}}, actor=actor, on=on)

    # [AB6] Owner-authorized resolution: a case is resolved only by its
    # owner/authority. Ashley resolves her own cases; other-owner cases
    # (agent/carrier chase, factory follow-up, technical repair) are
    # resolvable only through Elicio's explicit administrative identity —
    # recorded with the real actor, never through ordinary Ashley
    # authorization, and never the other way around (an Elicio resolution
    # of an Ashley planning case would silently create an Ashley decision).
    _CASE_RESOLVERS = {"ashley": ("ashley",)}

    def _authorize_resolution(self, imp, actor):
        owner = (imp.scope or {}).get("owner", "ashley")
        allowed = self._CASE_RESOLVERS.get(owner, ("elicio",))
        if actor not in allowed:
            raise DomainError(
                f"implication {imp.implication_id} is owned by {owner!r}: "
                f"resolution by {actor!r} is denied — the resolution actor "
                "must match the case owner/authority [AB6]")

    def _cmd_ResolveImplication(self, s, p, *, actor, on):
        iid = p["implication_id"]
        imp = s.implications.get(iid)
        if imp is None:
            raise DomainError(f"unknown implication {iid}")
        self._authorize_resolution(imp, actor)
        action = p["action"]
        params = p.get("params") or {}
        plan_id = (imp.scope or {}).get("plan_id")
        created_product = None
        if action == "create":
            if "product_id" in params:
                raise DomainError("non-map actions forbid product_id")
            unknown = set(params) - {"note"}
            if unknown:
                raise DomainError(f"unknown nested field(s): {sorted(unknown)}")
            if (imp.scope or {}).get("feed") not in {
                    "warehouse", "siesa_availability"}:
                raise DomainError("create is only legal for an unknown inventory reference")
            if not self._durable_catalog_staging:
                raise DomainError("create requires durable catalog staging")
            created_product = product_from_inventory_reference(
                str(imp.scope["raw_ref"]))
            if self.products.get(created_product["product_id"]) != created_product:
                raise DomainError("the exact server-derived product must be staged before create")
        # [FC3] the engine is the authoritative effect path: it records the
        # resolution through the internal primitive and performs the named
        # effect below in the SAME transaction (a failing effect rolls the
        # resolution back — effect-or-fail, never a no-op resolve)
        sd._record_resolution(s, implication_id=iid, action=action,
                              params=params, actor=actor, on=on)
        # typed effects — every action performs its named state effect
        # atomically, or is an explicitly-named keep/acceptance [AB6]
        if action == "exclude_pending":
            # applied against CURRENT state, not the case's frozen scope:
            # an order whose commitment is already observed is reflected by
            # the snapshot and must never be excluded (B1 review finding —
            # AB2/C4: reflected quantity is never simultaneously excluded);
            # an order already covered by an active exclusion is skipped
            for q in imp.scope.get("pending", []):
                order_ref = q.get("order_ref")
                observed = any(c.handoff_order_ref == order_ref
                               and c.state == "observed"
                               for c in s.commitments.values())
                if observed:
                    continue
                covered = any(x.active and x.order_ref == order_ref
                              and x.product_id == q["product_id"]
                              for x in s.exclusions.values())
                if covered:
                    continue
                sd.create_exclusion(
                    s, plan_ref=q["plan_id"], product_id=q["product_id"],
                    m2=D(str(q["m2"])), resolution_ref=iid,
                    order_ref=order_ref, actor=actor, on=on)
        elif action == "create":
            sd.record_mapping(
                s, raw_ref=str(imp.scope["raw_ref"]),
                product_id=created_product["product_id"], actor=actor, on=on)
        elif action == "map":
            # [FC1 review B1 → FCB1] the collision law now lives at the
            # domain RECORDING boundary (`sailing_domain.record_mapping`):
            # it evaluates the proposed mapping against the EFFECTIVE
            # normalized current snapshot under all existing mappings plus
            # the proposed one, so a target made confident by an EARLIER
            # mapping collides exactly like an originally confident row.
            # A denial raises here and the transaction rolls back — the
            # implication stays open with zero mutation.
            sd.record_mapping(s, raw_ref=str(imp.scope["raw_ref"]),
                              product_id=params["product_id"],
                              actor=actor, on=on)
        elif action == "discard":
            if imp.scope.get("row_key"):
                sd.record_mapping(s, raw_ref=imp.scope["row_key"],
                                  product_id=None, actor=actor, on=on)
            else:
                sd.record_mapping(s, raw_ref=imp.scope["raw_ref"],
                                  product_id=None, actor=actor, on=on)
        elif action == "accept_evidence":
            exp_id = imp.scope.get("exp_id")
            e = s.expectations.get(exp_id)
            if e and e.open:
                observed = D(str(imp.scope.get("observed_m2", "0")))
                sd.supersede_expectation(
                    s, exp_id=exp_id,
                    new_effective_m2=max(observed, e.received_m2),
                    note="shortfall accepted from evidence",
                    evidence_ref=str(imp.evidence), actor=actor, on=on)
                if e.state == "booked":
                    sd.advance_expectation(s, exp_id=exp_id,
                                           new_state="in_transit",
                                           evidence_ref="accepted evidence",
                                           actor=actor, on=on)
        elif action == "confirm_reopen":
            # [RC3] atomic-or-fail: the action must reopen through the
            # explicit reallocation path or FAIL without resolving the
            # case — it never resolves as a silent no-op
            target = plan_id
            if target is None:
                cid = (imp.scope or {}).get("commitment_id")
                com = s.commitments.get(cid) if cid else None
                if com is not None:
                    target = self._plan_of_order(s, com.handoff_order_ref)
            plan = s.plans.get(target) if target else None
            if plan is None or plan.lifecycle != "finalized":
                raise DomainError(
                    "confirm_reopen could not reopen: this case's lineage "
                    "does not lead to a finalized plan "
                    f"(plan: {target!r}, lifecycle: "
                    f"{plan.lifecycle if plan else 'none'}) — the case "
                    "remains open [RC3]")
            sd.confirm_reallocation(s, plan_id=target, actor=actor, on=on)
        elif action == "reduce_to_available":
            # [AB6] actually reduces the affected line to the valid cap —
            # the case never closes with the quantity still above
            # availability
            self._apply_reduce_to_available(s, imp, actor=actor, on=on)
        elif action == "cancel_pursuit":
            # [AB6] actually cancels the exceptional pursuit and reconciles
            # the affected open draft under the explicit rule: the draft is
            # abandoned with history and the sailing decision reverts to
            # watch — `use` needs a fresh explicit pursuit
            self._apply_cancel_pursuit(s, imp, actor=actor, on=on)
        elif action == "reaccept_current":
            # [AB7] Ashley's typed re-accept: an authentic new decision
            # with a fresh immutable snapshot — never fabricated by the
            # system
            self._apply_reaccept_current(s, imp, actor=actor, on=on)
        elif action == "pursue_exceptional":
            # [RC4] the crossing case's explicit escalation: records the
            # real pursuit (with its own risk case) — the crossing case is
            # superseded by the reconcile pass once the pursuit is active
            sid = (imp.scope or {}).get("sailing_id")
            if sid is None or sid not in s.sailings:
                raise DomainError("no sailing to pursue in this case")
            sd.record_exceptional_pursuit(s, sailing_id=sid, actor=actor,
                                          on=on)
            self._ensure_pursuit_case(s, sid, actor=actor, on=on)
        elif action == "abandon_plan":
            # [RC4] the crossing case's safe exit: the draft is abandoned
            # with history
            target = (imp.scope or {}).get("plan_id")
            plan = s.plans.get(target) if target else None
            if plan is None or plan.lifecycle != "draft":
                raise DomainError("no open draft to abandon in this case")
            sd.close_plan(s, plan_id=target, reason="abandoned",
                          actor=actor, on=on)
        # keep_plan / keep_expectation / keep_selected / keep / accept_risk /
        # verify_entry / chase_* / load_feed / proceed_with_note /
        # record_reference / note_for_monthly / plan_on_sailing: their named
        # effect IS the recorded resolution (an explicit keep/acceptance/
        # outside-the-app act) — no state mutation is promised or performed.
        return {}

    def _apply_reduce_to_available(self, s, imp, *, actor, on):
        """[RC3] Monotonic with its name: the target is
        min(current_selected, current_valid_cap) — this action can NEVER
        increase the selected quantity; a line already at or below the cap
        is left untouched (the recorded resolution closes the case)."""
        plan_id = (imp.scope or {}).get("plan_id")
        pid = (imp.scope or {}).get("product_id")
        plan = s.plans.get(plan_id)
        if plan is None or plan.lifecycle != "draft" \
                or pid not in plan.lines:
            raise DomainError(
                "reduce_to_available needs an open draft line to reduce")
        current = D(str(plan.lines[pid]["selected_m2"]))
        cap = sm.floor_to_half_pallet(self.buckets(
            pid, s, exclude_plan=plan_id)["siesa_available_effective"])
        target = min(current, cap)
        if target <= 0:
            sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                    action="postpone", selected_m2=None,
                                    origin=None, snapshot_ref=None,
                                    actor=actor, on=on)
        elif target < current:
            _, ref = self._snapshot_suggestion(s, plan_id, pid,
                                               actor=actor, on=on)
            sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                    action="edit", selected_m2=target,
                                    origin="edited", snapshot_ref=ref,
                                    actor=actor, on=on)
        # target == current: already valid — truthful closure, no mutation

    def _apply_cancel_pursuit(self, s, imp, *, actor, on):
        sid = (imp.scope or {}).get("sailing_id")
        if sid is None or sid not in s.pursuits:
            raise DomainError("no active exceptional pursuit to cancel")
        sd.cancel_exceptional_pursuit(s, sailing_id=sid, actor=actor, on=on)
        for plan in list(s.plans.values()):
            if plan.sailing_id == sid and plan.lifecycle == "draft":
                sd.close_plan(s, plan_id=plan.plan_id, reason="abandoned",
                              actor=actor, on=on)
        if s.sailing_decisions.get(sid) == "use":
            sd.record_sailing_decision(s, sailing_id=sid, decision="watch",
                                       actor=actor, on=on)

    def _apply_reaccept_current(self, s, imp, *, actor, on):
        plan_id = (imp.scope or {}).get("plan_id")
        pid = (imp.scope or {}).get("product_id")
        plan = s.plans.get(plan_id)
        if plan is None or plan.lifecycle != "draft":
            raise DomainError("re-accept needs an open draft plan")
        # [RC4 review B1] re-accepting is material advancement: it is
        # gated at the hard cutoff exactly like Accept/Edit/AddManual
        self._require_plan_advanceable(s, plan_id, verb="advance")
        sug, ref = self._snapshot_suggestion(s, plan_id, pid,
                                             actor=actor, on=on)
        if sug["no_basis"] or sug["suggested_m2"] <= 0:
            sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                    action="postpone", selected_m2=None,
                                    origin=None, snapshot_ref=None,
                                    actor=actor, on=on)
        else:
            sd.record_plan_decision(s, plan_id=plan_id, product_id=pid,
                                    action="accept",
                                    selected_m2=sug["suggested_m2"],
                                    origin="suggestion", snapshot_ref=ref,
                                    actor=actor, on=on)

    # ═══════════════════════ reconcile pass (§9) ═══════════════════════════

    def _tolerated(self, diff: Decimal, expected: Decimal) -> bool:
        if expected <= 0:
            return diff <= pm.M2_PER_PALLET
        return (diff <= pm.M2_PER_PALLET
                and diff <= self.config.material_variance_pct * expected)

    # [AB6] a typed keep/acceptance resolution STANDS: the same case is not
    # reopened by the reconcile pass while its evidence is unchanged —
    # otherwise "accept the risk" would be a no-op lie. New evidence (a
    # changed fingerprint) legitimately reopens.
    _KEEP_ACTIONS = frozenset({
        "accept_risk", "keep", "keep_plan", "keep_expectation",
        "keep_selected", "proceed_with_note", "verify_entry",
        "chase_booking", "chase_evidence", "record_reference",
        "note_for_monthly", "plan_on_sailing",
    })

    def _resolution_stands(self, s, subject_key, evidence, *,
                           standing_law=None) -> bool:
        """[AB6/FCB2B] Generic law: a resolved case with the same
        subject, a recognized keep/acceptance action and an unchanged
        serialized evidence fingerprint stands.  A case-identity
        boundary that requires MORE than the fingerprint — the pursuit
        case's canonical law [FCB2B] — passes `standing_law`, and only a
        resolved case that ALSO satisfies that predicate may stand: a
        merely fingerprint-equal unrelated resolution never suppresses
        the canonical case it impersonates."""
        frozen_new = _ser(evidence)
        for imp in s.implications.values():
            if imp.subject_key != subject_key or imp.state != "resolved":
                continue
            action = (imp.resolution or {}).get("action")
            if action in self._KEEP_ACTIONS \
                    and _ser(thaw(imp.evidence)) == frozen_new \
                    and (standing_law is None or standing_law(imp)):
                return True
        return False

    def _open(self, s, actor, on, *, standing_law=None, **kw) \
            -> Optional[str]:
        acts = list(kw.pop("typed_actions"))
        if (kw.get("severity") == "consequential"
                and kw.get("family") != "product_match"
                and "accept_risk" not in acts):
            acts.append("accept_risk")
        if self._resolution_stands(s, kw["subject_key"], kw["evidence"],
                                   standing_law=standing_law):
            return None
        return sd.open_implication(s, typed_actions=acts, actor=actor,
                                   on=on, **kw)

    def _supersede_if_open(self, s, subject_prefix, reason, *, actor, on,
                           family=None):
        for imp in list(s.implications.values()):
            if imp.state == "open" \
                    and imp.subject_key.startswith(subject_prefix) \
                    and (family is None or imp.family == family):
                sd.supersede_implication(s, implication_id=imp.implication_id,
                                         reason=reason, actor=actor, on=on)

    def reconcile(self, s, *, trigger: str, on: date):
        actor = "system"
        self._reconcile_commitment_observation(s, actor, on)
        self._reconcile_promotions(s, actor, on)
        self._reconcile_transit(s, actor, on)
        self._reconcile_duplicate_allocation(s, actor, on)
        self._reconcile_stale_decisions(s, actor, on)
        self._reconcile_manual_over(s, actor, on)
        self._reconcile_timing_crossings(s, actor, on)
        self._reconcile_missing_evidence(s, actor, on)
        self._reconcile_windows(s, actor, on)
        self._reconcile_risks(s, actor, on)
        self._reconcile_product_match(s, actor, on)

    # ── snapshot chronology helpers [AB1] ------------------------------------

    def _siesa_snapshot_chain(self, s) -> list:
        """All SIESA availability snapshots in load order (history +
        current), each as {snapshot_id, as_of, rows_by_pid, ordinal}."""
        chain = []
        stored = list(s.feed_history.get("siesa_availability", []))
        cur = s.feeds.get("siesa_availability")
        if cur:
            stored = stored + [cur]
        for i, f in enumerate(stored):
            chain.append({
                "snapshot_id": f["snapshot_id"], "as_of": f["as_of"],
                "ordinal": i,
                # [FC1 review B2] historical snapshots read through the SAME
                # mapping+merge law as the current one — a later product-
                # match resolution applies retroactively to the baseline,
                # so surfaced pre-existing quantities are never new proof
                "rows": {r["product_id"]: r
                         for r in self._normalize_feed_rows(
                             s, "siesa_availability", f)
                         if r.get("product_id")}})
        return chain

    @staticmethod
    def _snap_by_id(chain, snapshot_id):
        for entry in chain:
            if entry["snapshot_id"] == snapshot_id:
                return entry
        return None

    # 1 — attributable, quantity-conserving observation [AB1] (§3.5/§4.2)
    #
    # A refreshed snapshot observes commitments only to the aggregate
    # quantity its evidence can support:
    #  • the proof pool per product is the CAUSALLY BOUNDED pre/post delta
    #    (committed column growth, or availability decrease when the column
    #    is absent) measured against each commitment's own baseline — the
    #    snapshot current when its reference was recorded, so pre-existing
    #    committed quantity is never new proof;
    #  • quantities of commitments already observed against later snapshots
    #    are subtracted from the pool (exact conservation);
    #  • the pool is allocated deterministically in reference-recording
    #    order (recorded date, then commitment id — the total recording
    #    order); a commitment the pool cannot support stays
    #    pending_observation, and everything recorded after it for the same
    #    product also stays pending — attribution is never guessed;
    #  • every still-pending qualifying commitment opens/keeps the typed
    #    §9.3 case.
    def _reconcile_commitment_observation(self, s, actor, on):
        feed = self._feed(s, "siesa_availability")
        if not feed:
            return
        chain = self._siesa_snapshot_chain(s)
        current = chain[-1]
        rows = {r["product_id"]: r for r in
                self._rows(s, "siesa_availability") if r.get("product_id")}

        def commitment_sort_key(c):
            digits = "".join(ch for ch in c.commitment_id if ch.isdigit())
            return (c.reference_recorded_at, int(digits or 0))

        def observed_consumption(pid, baseline_ordinal):
            """Σ observed (not released) quantities for pid whose observing
            snapshot came AFTER the baseline snapshot — evidence already
            spent from this pool."""
            total = ZERO
            for c in s.commitments.values():
                if c.state != "observed":
                    continue
                osnap = self._snap_by_id(chain, c.observed_snapshot_id)
                if osnap is None or osnap["ordinal"] <= baseline_ordinal:
                    continue
                for q in c.quantities:
                    if q["product_id"] == pid:
                        total += D(str(q["m2"]))
            return total

        def pool_for(c, pid):
            """Causally bounded evidence pool available to commitment c for
            product pid, or None when no bounded proof path exists.

            [RC1] A missing baseline `committed_m2` value means UNKNOWN,
            not zero: the committed-column path requires the column in
            BOTH the baseline and the current snapshot (a later
            introduction of the optional column is not attributable delta
            proof by itself). Otherwise fall back to the causally bounded
            availability delta, which requires availability in both
            snapshots; with neither comparison available there is no
            bounded proof and the commitment stays pending."""
            base = self._snap_by_id(chain, c.baseline_snapshot_id)
            if base is None:
                return None            # no pre-reference snapshot: unbounded
            row_now = rows.get(pid)
            if row_now is None:
                return None
            base_row = base["rows"].get(pid, {})
            committed_now = row_now.get("committed_m2")
            base_committed = base_row.get("committed_m2")
            if committed_now is not None and base_committed is not None:
                pool = D(str(committed_now)) - D(str(base_committed))
            else:
                base_avail = base_row.get("available_m2")
                avail_now = row_now.get("available_m2")
                if base_avail is None or avail_now is None:
                    return None
                pool = D(str(base_avail)) - D(str(avail_now))
            return pool - observed_consumption(pid, base["ordinal"])

        pending = sorted((c for c in s.commitments.values()
                          if c.state == "pending_observation"),
                         key=commitment_sort_key)
        stopped_products: set = set()
        for c in pending:
            recorded = date.fromisoformat(c.reference_recorded_at)
            qualified = (feed["as_of"] >= recorded
                         and current["snapshot_id"] != c.baseline_snapshot_id)
            supported = qualified
            if qualified:
                for q in c.quantities:
                    pid, qty = q["product_id"], D(str(q["m2"]))
                    if pid in stopped_products:
                        supported = False
                        break
                    # [RC1] EXACT conservation: the pool must cover the
                    # full commitment quantity. Tolerance classifies a
                    # discrepancy for review (the §9.3 typed case below);
                    # it never manufactures observed quantity — aggregate
                    # committed-origin ≤ consumed evidence, exactly.
                    pool = pool_for(c, pid)
                    if pool is None or pool < qty:
                        supported = False
                        break
            if qualified and supported:
                sd.observe_commitment(s, commitment_id=c.commitment_id,
                                      snapshot_id=current["snapshot_id"],
                                      actor=actor, on=on)
                # [AB2] the reflecting snapshot and an active exclusion for
                # the same order quantity never coexist: decrement exactly
                # the supported quantity in the same atomic step
                self._consume_exclusions_for(
                    s, order_ref=c.handoff_order_ref,
                    quantities=c.quantities,
                    reason=f"commitment {c.commitment_id} observed",
                    actor=actor, on=on)
                self._supersede_if_open(
                    s, f"unreflected:{c.commitment_id}",
                    "commitment observed", actor=actor, on=on)
            else:
                if qualified and not supported:
                    # attribution stops here for these products: anything
                    # recorded later must not claim the same evidence
                    stopped_products.update(q["product_id"]
                                            for q in c.quantities)
                if not qualified:
                    continue          # no refreshed snapshot yet — no case
                self._open(
                    s, actor, on, family="overdue_response",
                    severity="consequential",
                    subject_key=f"unreflected:{c.commitment_id}",
                    scope={"commitment_id": c.commitment_id,
                           "owner": "ashley"},
                    evidence=[{"siesa_ref": c.siesa_ref,
                               "snapshot_as_of": feed["as_of"].isoformat(),
                               "quantities": [
                                   {"product_id": q["product_id"],
                                    "m2": str(q["m2"])}
                                   for q in c.quantities]}],
                    consequence=(
                        f"La actualización de SIESA no muestra de forma atribuible "
                        f"el compromiso {c.siesa_ref}; el registro pudo fallar, "
                        "diferir o quedar reclamado por una referencia anterior. "
                        "El compromiso todavía no es autoritativo."),
                    recommendation="Verifica el registro SIESA y actualiza de "
                                   "nuevo la disponibilidad.",
                    shipment_effect={"note": "La disponibilidad de zarpes "
                                     "posteriores todavía incluye estas cantidades."},
                    typed_actions=["verify_entry"])
        # duplicate-allocation cases whose pending quantities all became
        # observed are superseded by the reflecting snapshot
        for imp in list(s.implications.values()):
            if imp.state != "open" or not imp.subject_key.startswith("dup:"):
                continue
            pend = imp.scope.get("pending", [])
            def _order_observed(entry):
                com = next((c for c in s.commitments.values()
                            if c.handoff_order_ref == entry.get("order_ref")
                            and c.state == "observed"), None)
                return com is not None
            if pend and all(_order_observed(e) for e in pend):
                sd.supersede_implication(
                    s, implication_id=imp.implication_id,
                    reason="commitments observed — availability now "
                    "reflects them", actor=actor, on=on)

    def _plan_of_order(self, s, order_ref):
        for h in s.handoffs.values():
            if any(o.get("order_ref") == order_ref
                   for o in h["handoff"].get("orders", [])):
                return h["plan_id"]
        return None

    def _active_plan_of_order(self, s, order_ref):
        """The plan whose ACTIVE (non-superseded) handoff carries this
        order — a reopened plan's superseded handoff never qualifies
        (B2 review finding — AB5)."""
        for h in s.handoffs.values():
            if h["superseded"]:
                continue
            if any(o.get("order_ref") == order_ref
                   for o in h["handoff"].get("orders", [])):
                return h["plan_id"]
        return None

    # 2 — booking promotions (held until BOTH commitment observed + booking)
    def _reconcile_promotions(self, s, actor, on):
        for b in s.bookings.values():
            for cref in b.commitment_refs:
                c = s.commitments.get(cref)
                if c is None or c.state != "observed":
                    continue
                for q in c.quantities:
                    exists = any(e.commitment_ref == cref
                                 and e.product_id == q["product_id"]
                                 for e in s.expectations.values())
                    if not exists:
                        ask = date.fromisoformat(c.observed_at) \
                            if c.observed_at else on
                        sd.create_expectation(
                            s, commitment_ref=cref,
                            booking_ref=b.booking_id,
                            product_id=q["product_id"],
                            confirmed_m2=D(str(q["m2"])),
                            sailing_id=b.sailing_id, ask_date=ask,
                            actor=actor, on=on)

    # 3 — transit progression + variance (§9.2)
    def _reconcile_transit(self, s, actor, on):
        result = self._match(s)
        obs = {o.obs_id: o for o in self._transit_observations(s)}
        alloc_by_exp: dict = {}
        ref_alloc_by_exp: dict = {}
        for a in result.allocations:
            alloc_by_exp[a.exp_id] = alloc_by_exp.get(a.exp_id, ZERO) + a.m2
            if obs[a.obs_id].reference:
                ref_alloc_by_exp[a.exp_id] = \
                    ref_alloc_by_exp.get(a.exp_id, ZERO) + a.m2
        for e in list(s.expectations.values()):
            got = alloc_by_exp.get(e.exp_id, ZERO)
            if got <= 0 or not e.open:
                continue
            if e.state in ("booked", "departed"):
                sd.advance_expectation(s, exp_id=e.exp_id,
                                       new_state="in_transit",
                                       evidence_ref="transit evidence",
                                       actor=actor, on=on)
            if e.overdue:
                sd.clear_expectation_overdue(s, exp_id=e.exp_id,
                                             actor=actor, on=on)
                self._supersede_if_open(s, f"transit_overdue:{e.exp_id}",
                                        "transit evidence arrived",
                                        actor=actor, on=on)
            # variance only on positively identified (referenced) consignments
            ref_got = ref_alloc_by_exp.get(e.exp_id, ZERO)
            if ref_got <= 0:
                continue
            gap = e.effective_m2 - e.received_m2 - ref_got
            if gap <= 0:
                continue
            if self._tolerated(gap, e.effective_m2):
                sd.supersede_expectation(
                    s, exp_id=e.exp_id,
                    new_effective_m2=e.received_m2 + ref_got,
                    note=f"tolerated variance −{gap} m² superseded from "
                         "transit evidence",
                    evidence_ref="transit", actor=actor, on=on)
            else:
                consequence = self._shortfall_consequence(
                    s, e, reduced=e.received_m2 + ref_got)
                if consequence:
                    self._open(
                        s, actor, on, family="reconciliation_exception",
                        severity="consequential",
                        subject_key=f"shortfall:{e.exp_id}",
                        scope={"exp_id": e.exp_id,
                               "product_id": e.product_id,
                               "observed_m2": str(e.received_m2 + ref_got),
                               "owner": "ashley"},
                        evidence=[{"expected_effective_m2":
                                   str(e.effective_m2),
                                   "transit_observed_m2": str(ref_got),
                                   "received_m2": str(e.received_m2),
                                   "source": "in_transit"}],
                        consequence=(
                            f"{e.product_id}: el tránsito muestra "
                            f"{e.received_m2 + ref_got} m² frente a "
                            f"{e.effective_m2} m² esperados; existe un faltante "
                            f"material de {gap} m² que rompe la protección de "
                            "inventario usada por el plan."),
                        recommendation=(
                            f"Acepta la evidencia y planifica los {gap} m² "
                            "faltantes en el siguiente zarpe."),
                        shipment_effect={
                            "product_id": e.product_id,
                            "shortfall_m2": str(gap),
                            "next_sailing_suggestion_delta_m2": str(gap)},
                        typed_actions=["accept_evidence",
                                       "keep_expectation"])
                else:
                    sd.supersede_expectation(
                        s, exp_id=e.exp_id,
                        new_effective_m2=e.received_m2 + ref_got,
                        note=f"material variance −{gap} m² superseded "
                             "(no supply consequence) — prominent note",
                        evidence_ref="transit", actor=actor, on=on)

    def _shortfall_consequence(self, s, e, *, reduced: Decimal) -> bool:
        """Both curves complete [PRESERVED-3.1 AC2]: exception only when the
        reduced quantity breaches protection where the full quantity did not,
        or a customer commitment becomes newly uncovered."""
        pid = e.product_id
        vel = self.velocity(pid, s) or ZERO
        wh = self.warehouse_value(pid, s) or ZERO
        buf = self.buffer_m2(pid, s)
        if e.sailing_id in s.sailings:
            win = self.window_for(e.sailing_id, s)
            days = win["window_days"]
        else:
            days = sm.ORDERING_CYCLE_DAYS
        base = wh - vel * D(days)
        with_full = base + (e.effective_m2 - e.received_m2)
        with_reduced = base + max(ZERO, reduced - e.received_m2)
        if with_reduced < buf <= with_full:
            return True
        # customer commitment newly uncovered (optional feed; silent absent)
        for row in self._rows(s, "committed_orders"):
            if row.get("product_id") != pid:
                continue
            due_days = max(0, (row["due_date"] - self._today).days)
            avail_full = wh - vel * D(due_days) + (e.effective_m2
                                                   - e.received_m2)
            avail_red = wh - vel * D(due_days) + max(
                ZERO, reduced - e.received_m2)
            need = D(str(row["m2"]))
            if avail_red < need <= avail_full:
                return True
        return False

    # 4 — duplicate SIESA allocation (§10.6.3, scenario A2)
    def _reconcile_duplicate_allocation(self, s, actor, on):
        for plan in s.plans.values():
            if plan.lifecycle != "draft":
                continue
            pending = self._pending_handoff_quantities(
                s, other_than_plan=plan.plan_id)
            by_pid: dict = {}
            for q in pending:
                # [AB2] coverage is order-addressed: only an active
                # exclusion bound to this exact handoff order covers it
                covered = any(x.active
                              and x.order_ref == q["order_ref"]
                              and x.product_id == q["product_id"]
                              for x in s.exclusions.values())
                if not covered:
                    by_pid.setdefault(q["product_id"], []).append(q)
            for pid, qs in by_pid.items():
                total = sum((q["m2"] for q in qs), ZERO)
                avail = self.buckets(pid, s, exclude_plan=plan.plan_id)[
                    "siesa_available_effective"]
                if total <= 0 or avail <= 0:
                    continue
                sug_now = self.suggestion_for(plan.plan_id, pid, s)
                if sug_now["no_basis"]:
                    continue
                cap_after = sm.floor_to_half_pallet(max(ZERO, avail - total))
                sug_after = min(sug_now["need_m2"] and
                                pm.round_up_to_half_pallet(
                                    sug_now["need_m2"]) or ZERO, cap_after)
                self._open(
                    s, actor, on, family="reconciliation_exception",
                    severity="consequential",
                    subject_key=f"dup:{plan.plan_id}:{pid}",
                    scope={"plan_id": plan.plan_id, "product_id": pid,
                           "owner": "ashley",
                           "pending": [{"plan_id": q["plan_id"],
                                        "order_ref": q["order_ref"],
                                        "product_id": q["product_id"],
                                        "m2": str(q["m2"])} for q in qs]},
                    evidence=[{"handed_off_m2": str(total),
                               "from_plans": sorted({q["plan_id"]
                                                     for q in qs}),
                               "siesa_available_m2": str(avail),
                               "snapshot_as_of": self._feed(
                                   s, "siesa_availability")[
                                       "as_of"].isoformat()}],
                    consequence=(
                        f"{pid}: {total} m² de la disponibilidad SIESA visible "
                        "ya fueron traspasados a otro zarpe y aún no aparecen "
                        "como comprometidos; planificarlos de nuevo asignaría "
                        "el mismo inventario a dos zarpes."),
                    recommendation=(
                        f"Excluye los {total} m² ya traspasados de la "
                        "disponibilidad de este plan hasta que SIESA refleje "
                        "el compromiso."),
                    shipment_effect={
                        "product_id": pid,
                        "suggestion_before_m2": str(sug_now["suggested_m2"]),
                        "suggestion_after_m2": str(sug_after),
                        "availability_delta_m2": str(-total)},
                    typed_actions=["exclude_pending", "keep"])

    # 4b — stale accepted decisions [AB7/D7]
    #
    # Evidence/resolution changes recompute the recommendation immediately,
    # but Ashley's selected quantity and her immutable decision snapshot are
    # PRESERVED until she performs a typed re-accept. When the current
    # recommendation differs materially (§9.2 predicate) from an
    # accepted-suggestion line, the line is marked stale/review-needed with
    # the exact delta and the next action — visibly actionable before
    # finalization. Edited/manual lines are operator-owned and never marked.
    def _reconcile_stale_decisions(self, s, actor, on):
        for plan in s.plans.values():
            if plan.lifecycle != "draft":
                # a plan leaving draft (finalize/close/abandon) takes its
                # open stale cases with it
                for imp in list(s.implications.values()):
                    if imp.state == "open" and imp.subject_key.startswith(
                            f"stale:{plan.plan_id}:"):
                        sd.supersede_implication(
                            s, implication_id=imp.implication_id,
                            reason="plan left draft", actor=actor, on=on)
                continue
            # B3 review finding [AB7]: a stale case never outlives its
            # line — postponing/removing the line, or its origin becoming
            # operator-owned, supersedes the case
            for imp in list(s.implications.values()):
                if imp.state != "open" or not imp.subject_key.startswith(
                        f"stale:{plan.plan_id}:"):
                    continue
                pid = imp.subject_key.split(":", 2)[2]
                line = plan.lines.get(pid)
                if line is None or line.get("origin") != "suggestion":
                    sd.supersede_implication(
                        s, implication_id=imp.implication_id,
                        reason="the accepted line was postponed or became "
                        "operator-owned — nothing left to review",
                        actor=actor, on=on)
            for pid, line in list(plan.lines.items()):
                subject = f"stale:{plan.plan_id}:{pid}"
                if line.get("origin") != "suggestion":
                    continue
                sug = self.suggestion_for(plan.plan_id, pid, s)
                if sug["no_basis"]:
                    continue
                selected = D(str(line["selected_m2"]))
                current = D(str(sug["suggested_m2"]))
                diff = abs(current - selected)
                if diff <= 0 or self._tolerated(diff, selected):
                    self._supersede_if_open(
                        s, subject, "recommendation matches the accepted "
                        "quantity again", actor=actor, on=on)
                    continue
                if self._stale_already_kept(s, subject, current):
                    continue          # Ashley explicitly kept this quantity
                self._open(
                    s, actor, on, family="decision_required",
                    severity="consequential",
                    subject_key=subject,
                    scope={"plan_id": plan.plan_id, "product_id": pid,
                           "owner": "ashley"},
                    evidence=[{"accepted_m2": str(selected),
                               "current_recommendation_m2": str(current),
                               "delta_m2": str(pm.q2(current - selected)),
                               "decision_snapshot": line.get("snapshot_ref")}],
                    consequence=(
                        f"{pid}: la recomendación cambió a {current} m² después "
                        f"de nueva evidencia, pero la selección aceptada sigue "
                        f"en {selected} m²; debe revisarse antes de finalizar."),
                    recommendation=(
                        f"Vuelve a aceptar la recomendación actual "
                        f"({current} m²) o conserva explícitamente {selected} m²."),
                    shipment_effect={
                        "product_id": pid,
                        "delta_m2": str(pm.q2(current - selected))},
                    typed_actions=["reaccept_current", "keep_selected"])

    def _stale_already_kept(self, s, subject, current) -> bool:
        """A stale case Ashley resolved with keep_selected/accept_risk is
        not reopened while the recommendation is unchanged — her explicit
        keep stands until the situation moves again."""
        kept = None
        for imp in s.implications.values():
            if imp.subject_key != subject or imp.state != "resolved":
                continue
            action = (imp.resolution or {}).get("action")
            if action in ("keep_selected", "accept_risk"):
                kept = imp
        if kept is None:
            return False
        for ev in kept.evidence:
            rec = dict(ev).get("current_recommendation_m2")
            if rec is not None:
                return D(str(rec)) == current
        return False

    # 4c — manual over-availability cases track their governed line [RC3]
    #
    # A `manual_over` case refreshes or supersedes when the governed line
    # changes: it never outlives the line's removal, never blocks a plan
    # whose line is back within availability, and never presents stale
    # numbers next to its typed actions. The pass is also LINE-driven
    # [RC3 review B2]: every operator-owned (manual/edited) draft line
    # above the current effective availability carries the open case —
    # a prior risk acceptance stands only while its evidence is unchanged
    # (the `_resolution_stands` fingerprint), so an edit or an availability
    # move re-governs the quantity.
    def _reconcile_manual_over(self, s, actor, on):
        # case-driven: supersede/refresh open cases against current truth
        for imp in list(s.implications.values()):
            if imp.state != "open" \
                    or not imp.subject_key.startswith("manual_over:"):
                continue
            _, plan_id, pid = imp.subject_key.split(":", 2)
            plan = s.plans.get(plan_id)
            line = (plan.lines.get(pid)
                    if plan is not None and plan.lifecycle == "draft"
                    else None)
            if line is None or line.get("origin") == "suggestion":
                sd.supersede_implication(
                    s, implication_id=imp.implication_id,
                    reason="the governed operator-owned line no longer "
                    "exists — nothing is above availability anymore",
                    actor=actor, on=on)
                continue
            selected = D(str(line["selected_m2"]))
            avail = self.buckets(pid, s, exclude_plan=plan_id)[
                "siesa_available_effective"]
            if selected <= avail:
                sd.supersede_implication(
                    s, implication_id=imp.implication_id,
                    reason="the selected quantity is no longer above the "
                    "available quantity", actor=actor, on=on)
                continue
            ev = dict(imp.evidence[0]) if imp.evidence else {}
            if str(ev.get("selected_m2")) != str(selected) \
                    or str(ev.get("siesa_available_effective")) != str(avail):
                sd.supersede_implication(
                    s, implication_id=imp.implication_id,
                    reason="refreshed with the current quantities",
                    actor=actor, on=on)
                self._open_manual_over(s, plan_id, pid, selected, avail,
                                       actor=actor, on=on)
        # line-driven: ensure governance exists for every operator-owned
        # line above availability (suggestion-origin lines are governed by
        # the stale-decision mechanism instead)
        for plan in s.plans.values():
            if plan.lifecycle != "draft":
                continue
            for pid, line in plan.lines.items():
                if line.get("origin") not in ("manual", "edited"):
                    continue
                subject = f"manual_over:{plan.plan_id}:{pid}"
                if any(i.state == "open" and i.subject_key == subject
                       for i in s.implications.values()):
                    continue
                selected = D(str(line["selected_m2"]))
                avail = self.buckets(pid, s, exclude_plan=plan.plan_id)[
                    "siesa_available_effective"]
                if selected > avail:
                    self._open_manual_over(s, plan.plan_id, pid, selected,
                                           avail, actor=actor, on=on)

    # 4d — hard-cutoff crossings gate existing drafts [RC4]
    #
    # A draft whose sailing has crossed into the exceptional window without
    # an active pursuit carries the consequential crossing case; the case
    # yields to the pursuit's own risk case once the explicit act exists.
    def _reconcile_timing_crossings(self, s, actor, on):
        for plan in s.plans.values():
            subject = f"cutoff:{plan.plan_id}"
            sid = plan.sailing_id
            crossing = (plan.lifecycle == "draft" and sid in s.sailings
                        and self.sailing_timing(sid, s).timing_state == "exceptional"
                        and sid not in s.pursuits)
            if not crossing:
                self._supersede_if_open(
                    s, subject, "the cutoff condition no longer applies "
                    "(pursuit active, timing changed, or plan left draft)",
                    actor=actor, on=on)
                continue
            sl = s.sailings[sid]
            self._open(
                s, actor, on, family="decision_required",
                severity="consequential",
                subject_key=subject,
                scope={"plan_id": plan.plan_id, "sailing_id": sid,
                       "owner": "ashley"},
                evidence=[{"sailing": sl.name,
                           "departure": sl.departure.isoformat(),
                           "hard_cutoff": (sl.departure - timedelta(
                               days=sm.HARD_DEADLINE_DAYS)).isoformat(),
                           "today": self._today.isoformat()}],
                consequence=(
                    f"El zarpe {sl.name} cruzó el límite de 10 días antes de "
                    "salir mientras este plan seguía en borrador; ya no está "
                    "en el conjunto normal y requiere una excepción explícita."),
                recommendation=("Considera este zarpe solo si una referencia "
                                "urgente lo exige; de lo contrario, abandona "
                                "este borrador y usa el siguiente zarpe normal."),
                shipment_effect={"note": "No se puede avanzar ni finalizar "
                                 "hasta registrar la excepción explícita."},
                typed_actions=["pursue_exceptional", "abandon_plan"])

    # 5 — missing/stale critical evidence (§10.6.6)
    def _reconcile_missing_evidence(self, s, actor, on):
        for feed in CRITICAL_FEEDS:
            f = s.feeds.get(feed)
            blocking = feed == "siesa_availability"
            if not f:
                status = "missing"
            else:
                age = (self._today - f["as_of"]).days
                status = ("stale" if age > self.config.freshness_stale_days
                          else "ok")
            if status == "ok":
                self._supersede_if_open(s, f"feed:{feed}",
                                        "feed refreshed", actor=actor, on=on)
                continue
            self._open(
                s, actor, on, family="missing_evidence",
                severity="blocking" if blocking else "consequential",
                subject_key=f"feed:{feed}",
                scope={"feed": feed, "owner": "ashley"},
                evidence=[{"feed": feed, "status": status,
                           "as_of": f["as_of"].isoformat() if f else None}],
                consequence=(
                    f"La evidencia de {feed} está "
                    f"{'ausente' if status == 'missing' else 'desactualizada'}"
                    + ("; no se puede verificar la restricción de disponibilidad "
                       "y no es seguro finalizar la recomendación." if blocking else
                       "; las sugerencias afectadas muestran una advertencia.")),
                recommendation=f"Actualiza la fuente {feed}.",
                shipment_effect={"note": "Las sugerencias están degradadas."
                                 if not blocking else
                                 "La finalización está bloqueada por evidencia ausente o desactualizada."},
                typed_actions=(["load_feed"] if blocking
                               else ["load_feed", "proceed_with_note"]))

    # 6 — overdue windows (§9.3, A15)
    def _reconcile_windows(self, s, actor, on):
        # (i) SIESA reference/observation overdue — owner Ashley
        for plan in s.plans.values():
            if plan.lifecycle != "finalized" or plan.handoff_id is None:
                continue
            h = s.handoffs.get(plan.handoff_id)
            if h is None or h["superseded"]:
                continue
            fin = date.fromisoformat(plan.finalized_at)
            if (on - fin).days <= self.config.commitment_observation_days:
                continue
            for o in h["handoff"].get("orders", []):
                has = any(c.handoff_order_ref == o.get("order_ref")
                          and c.state != "released"
                          for c in s.commitments.values())
                if not has:
                    self._open(
                        s, actor, on, family="overdue_response",
                        severity="consequential",
                        subject_key=f"ref_overdue:{o.get('order_ref')}",
                        scope={"plan_id": plan.plan_id, "owner": "ashley"},
                        evidence=[{"order_ref": o.get("order_ref"),
                                   "finalized_at": plan.finalized_at,
                                   "days_waiting": (on - fin).days}],
                        consequence=(
                            f"El bloque de traspaso {o.get('order_ref')} no tiene "
                            f"referencia SIESA registrada {(on - fin).days} días "
                            "después de finalizar; el compromiso aún no ha sido "
                            "observado."),
                        recommendation="Ingresa el bloque en SIESA y registra "
                                       "su referencia.",
                        shipment_effect={"note": "Las cantidades siguen sin "
                                         "compromiso observado en origen."},
                        typed_actions=["record_reference"])
        # (ii) booking overdue — owner Jorge/agent; calm context for Ashley
        for c in s.commitments.values():
            if c.state != "observed":
                continue
            booked = any(c.commitment_id in b.commitment_refs
                         for b in s.bookings.values())
            if booked:
                self._supersede_if_open(
                    s, f"booking_overdue:{c.commitment_id}",
                    "booking recorded", actor=actor, on=on)
                continue
            plan_id = self._plan_of_order(s, c.handoff_order_ref)
            plan = s.plans.get(plan_id) if plan_id else None
            if plan is None or plan.sailing_id not in s.sailings:
                continue
            booking_date = self.sailing_timing(plan.sailing_id,
                                               s).booking_date
            if on >= booking_date - timedelta(
                    days=self.config.booking_overdue_days):
                self._open(
                    s, actor, on, family="overdue_response",
                    severity="consequential",
                    subject_key=f"booking_overdue:{c.commitment_id}",
                    scope={"commitment_id": c.commitment_id,
                           "owner": "jorge_agent"},
                    evidence=[{"siesa_ref": c.siesa_ref,
                               "booking_deadline":
                               booking_date.isoformat()}],
                    consequence=(
                        f"El compromiso {c.siesa_ref} no tiene reserva confirmada "
                        f"y la fecha límite de reserva ({booking_date}) está a "
                        f"{self.config.booking_overdue_days} días o menos."),
                    recommendation="Gestiona la reserva con el transportista o "
                                   "agente fuera de la aplicación.",
                    shipment_effect={"note": "Las cantidades comprometidas "
                                     "podrían perder este zarpe."},
                    typed_actions=["chase_booking"])
        # (iii) transit evidence overdue — flag, state unchanged
        result = self._match(s)
        with_alloc = {a.exp_id for a in result.allocations}
        for e in s.expectations.values():
            if e.state not in ("booked", "departed"):
                continue
            if e.sailing_id not in s.sailings:
                continue
            dep = s.sailings[e.sailing_id].departure
            if on > dep + timedelta(days=self.config.transit_evidence_days) \
                    and e.exp_id not in with_alloc:
                sd.flag_expectation_overdue(s, exp_id=e.exp_id,
                                            actor=actor, on=on)
                self._open(
                    s, actor, on, family="overdue_response",
                    severity="consequential",
                    subject_key=f"transit_overdue:{e.exp_id}",
                    scope={"exp_id": e.exp_id, "product_id": e.product_id,
                           "owner": "jorge_agent"},
                    evidence=[{"departure": dep.isoformat(),
                               "expected_m2": str(e.remaining_m2)}],
                    consequence=(
                        f"{e.product_id}: no hay evidencia de BL o tránsito "
                        f"{(on - dep).days} días después de la salida; el "
                        "movimiento esperado no está verificado."),
                    recommendation="Solicita al agente el estado del BL o "
                                   "tránsito fuera de la aplicación.",
                    shipment_effect={"note": "La entrada esperada no está verificada."},
                    typed_actions=["chase_evidence"])

    # 7 — risk-bearing decisions (§10.6.2)
    def _reconcile_risks(self, s, actor, on):
        if not (self._feed(s, "sales") and self._feed(s, "warehouse")
                and self._feed(s, "siesa_availability")):
            return
        arrivals = [self.sailing_timing(sid, s).warehouse_arrival
                    for sid in s.sailings
                    if s.sailing_decisions.get(sid) != "skip"
                    and sm.schedule_timing_state(
                        departure=s.sailings[sid].departure,
                        bl_vgm_close=s.sailings[sid].bl_vgm_close,
                        loading_terminal_eta=s.sailings[sid].loading_terminal_eta,
                        today=on) not in ("departed", "arrived")]
        earliest = min(arrivals) if arrivals else on + timedelta(
            days=sm.ORDERING_CYCLE_DAYS)
        open_plans = [p for p in s.plans.values() if p.lifecycle == "draft"]
        for pid in self.catalog(s):
            vel = self.velocity(pid, s) or ZERO
            if vel <= 0:
                continue
            wh = self.warehouse_value(pid, s) or ZERO
            b = self.buckets(pid, s)
            # [AB3] destination-protecting incoming = booking-confirmed
            # expected + verified moving ONLY; an observed but unbooked
            # commitment is origin-side and cannot avert a destination
            # stockout
            incoming = b["expected"] + b["moving"]
            days_to_stockout = int(wh / vel) if vel else 9999
            stockout_date = on + timedelta(days=days_to_stockout)
            if stockout_date < earliest and incoming <= 0 \
                    and b["siesa_available_effective"] <= 0:
                scope = {"product_id": pid, "owner": "ashley"}
                if open_plans:
                    scope["plan_id"] = open_plans[0].plan_id
                product = self.catalog(s).get(pid, {})
                product_label = product.get("name") or product.get("sku") or pid
                self._open(
                    s, actor, on, family="decision_required",
                    severity="consequential",
                    subject_key=f"stockout:{pid}",
                    scope=scope,
                    evidence=[{"warehouse_m2": str(wh),
                               "daily_velocity": str(vel),
                               "projected_stockout":
                               stockout_date.isoformat(),
                               "earliest_arrival": earliest.isoformat()}],
                    consequence=(
                        f"{product_label}: se proyecta quiebre de inventario el "
                        f"{stockout_date}, antes de cualquier llegada planificada "
                        f"({earliest}), sin disponibilidad SIESA ni cantidades "
                        "comprometidas o en movimiento."),
                    recommendation="Decide entre anotarlo para fabricación "
                                   "mensual, aceptar el riesgo de quiebre o "
                                   "considerar una opción excepcional.",
                    shipment_effect={"note": "Ninguna cantidad de zarpe puede "
                                     "cubrirlo con el inventario actual."},
                    typed_actions=["note_for_monthly", "accept_risk"])
            # customer commitment at risk (optional feed — silent absent)
            for row in self._rows(s, "committed_orders"):
                if row.get("product_id") != pid:
                    continue
                due = row["due_date"]
                need = D(str(row["m2"]))
                days = max(0, (due - on).days)
                supply = wh - vel * D(days)
                for e in s.expectations.values():
                    if e.product_id == pid and e.open:
                        arr = self.sailing_timing(
                            e.sailing_id, s).warehouse_arrival \
                            if e.sailing_id in s.sailings else None
                        if arr and arr <= due:
                            supply += e.remaining_m2
                if supply < need:
                    self._open(
                        s, actor, on, family="decision_required",
                        severity="consequential",
                        subject_key=f"commit_risk:{pid}:{due.isoformat()}",
                        scope={"product_id": pid, "owner": "ashley"},
                        evidence=[{"committed_m2": str(need),
                                   "due_date": due.isoformat(),
                                   "projected_available_m2":
                                   str(pm.q2(supply)),
                                   "source": "committed_orders"}],
                        consequence=(
                            f"{pid}: el compromiso del cliente de {need} m² "
                            f"con vencimiento {due} supera la disponibilidad "
                            f"proyectada ({pm.q2(supply)} m²)"),
                        recommendation=(
                            "embarcarlo en el zarpe utilizable más cercano o "
                            "renegociar la fecha"),
                        shipment_effect={"product_id": pid,
                                         "shortfall_m2":
                                         str(pm.q2(need - supply))},
                        typed_actions=["plan_on_sailing", "accept_risk"])

    # 8 — product identity (§10.6.1) + impossible references
    def _reconcile_product_match(self, s, actor, on):
        held_by_raw = {}
        for held in self._held_rows(s):
            if (held["raw_ref"] in s.mappings or not held["material"]):
                continue
            held_by_raw.setdefault(held["raw_ref"], []).append(held)

        feed_order = {"siesa_availability": 0, "warehouse": 1}
        for raw_ref in sorted(held_by_raw):
            held_rows = sorted(
                held_by_raw[raw_ref],
                key=lambda item: (feed_order.get(item["feed"], 9), item["feed"]))
            feeds = []
            evidence = []
            for held in held_rows:
                if held["feed"] not in feeds:
                    feeds.append(held["feed"])
                item = {"raw_reference": raw_ref, "feed": held["feed"],
                        "m2": str(held["m2"])}
                if item not in evidence:
                    evidence.append(item)
            warehouse_authority = "warehouse" in feeds
            inventory_creation_authority = warehouse_authority or (
                "siesa_availability" in feeds)
            primary_feed = "warehouse" if warehouse_authority else feeds[0]
            actions = (["create", "map", "discard"] if inventory_creation_authority
                       else ["map", "discard"])
            scope = {"raw_ref": raw_ref, "feed": primary_feed,
                     "feeds": feeds, "owner": "ashley"}
            subject_key = f"match:{raw_ref}"

            for implication in list(s.implications.values()):
                if (implication.state == "open"
                        and implication.family == "product_match"
                        and implication.subject_key == subject_key
                        and (thaw(implication.scope) != scope
                             or thaw(implication.evidence) != evidence
                             or list(implication.typed_actions) != actions)):
                    sd.supersede_implication(
                        s, implication_id=implication.implication_id,
                        reason="product-match evidence or authority changed",
                        actor=actor, on=on)

            total_m2 = sum((held["m2"] for held in held_rows), Decimal("0.00"))
            self._open(
                s, actor, on, family="product_match",
                severity="consequential", subject_key=subject_key,
                scope=scope, evidence=evidence,
                consequence=(
                    f"La referencia {raw_ref!r} aparece en {', '.join(feeds)} sin "
                    f"una coincidencia de producto confiable; sus {total_m2} m² "
                    "quedan fuera del abastecimiento hasta resolverla."),
                recommendation=(
                    "Créala desde la referencia de inventario, asígnala al producto "
                    "correcto o descarta la fila."
                    if inventory_creation_authority else
                    "Asígnala al producto correcto o descarta la fila."),
                shipment_effect={"held_m2": str(total_m2)},
                typed_actions=actions)
        # resolved refs: supersede stale cases
        for imp in list(s.implications.values()):
            if imp.state == "open" and imp.family == "product_match" \
                    and imp.scope.get("raw_ref") in s.mappings:
                sd.supersede_implication(
                    s, implication_id=imp.implication_id,
                    reason="reference resolved", actor=actor, on=on)
        result = self._match(s)
        obs = {o.obs_id: o for o in self._transit_observations(s)}
        for h in result.held:
            if h.reason != "impossible_reference":
                continue
            o = obs[h.obs_id]
            self._open(
                s, actor, on, family="reconciliation_exception",
                severity="consequential",
                subject_key=f"impossible:{o.reference}:{o.product_id}",
                scope={"raw_ref": o.raw_reference, "owner": "ashley",
                       "product_id": o.product_id,
                       "row_key": (f"discard-row:{o.feed}:"
                                   f"{o.raw_reference}|{o.reference}")},
                evidence=[{"reference": o.reference,
                           "product": o.product_id, "m2": str(o.m2),
                           "feed": o.feed}],
                consequence=(
                    f"La referencia {o.reference!r} contradice la identidad de "
                    f"{o.product_id}; sus {o.m2} m² quedan fuera del "
                    "abastecimiento hasta resolverla."),
                recommendation="Corrige la evidencia o descarta la fila.",
                shipment_effect={"held_m2": str(o.m2)},
                typed_actions=["discard"])
