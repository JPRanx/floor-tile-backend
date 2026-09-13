"""
Reconciled V1 — §8 workspace read model [D7].

One composer feeds the UI; the UI owns no business math. The composition is
a pure function of (engine config/products, state, today) — composing over
`fold(events)` equals composing over live state (rebuildability test).

Above-the-fold hierarchy [D7]: sailing+timing → evidence readiness →
blocking/consequential implications (with recommendation and exact shipment
effect) → plan state → totals (m² first) → clear next action. Informational
material lives in `history`, one tap behind, never competing for attention.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections import deque
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from . import planning_math as pm
from . import sailing_math as sm
from .cycle_math import compose_replenishment_cycle, product_buffer_breach
from .domain import thaw

ZERO = Decimal("0")


def _canonical_sha256(value: dict) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def source_evidence_fingerprint(eng, s) -> str:
    return _canonical_sha256({
        "review_provenance": _jsonable(getattr(eng, "review_provenance", {})),
        "feeds": {name: {"snapshot_id": row.get("snapshot_id"),
                          "as_of": _s(row.get("as_of"))}
                  for name, row in sorted(s.feeds.items())},
    })


def _s(v) -> Optional[str]:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _jsonable(v):
    """Deep-normalize a trace/effect structure for the UI: Decimals and
    dates become strings; mappings/sequences become plain dicts/lists."""
    if isinstance(v, Decimal):
        return str(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def _product_ref(catalog: dict, product_id: str) -> dict:
    """One structured, source-faithful product carrier for every projection."""
    row = catalog.get(product_id, {})
    return {
        "id": product_id, "sku": row.get("sku"),
        "name": row.get("name") or row.get("sku"),
        "siesa_item": row.get("siesa_item"), "category": row.get("category"),
        "rotation": row.get("rotation"), "tier": row.get("tier"),
        "source_unit": row.get("source_unit", "m2"),
        "dimensions_cm": {"length": row.get("length_cm"),
                          "width": row.get("width_cm"),
                          "height": row.get("height_cm")},
        "weight_kg": row.get("weight_kg"),
        "units_per_pallet": row.get("units_per_pallet"),
        "m2_per_pallet": _s(row.get("m2_per_pallet", pm.M2_PER_PALLET)),
        "edit_increment_m2": _s(row.get("edit_increment_m2", Decimal("67.20"))),
    }


def _factory_planning_context(eng, s, *, focus_sailing_id: str | None,
                              production_anchor_sailing_id: str | None,
                              factory_order_date: date | str | None) -> dict | None:
    """Resolve the three operator anchors into one server-owned horizon."""
    supplied = (production_anchor_sailing_id is not None,
                factory_order_date is not None)
    if any(supplied) and not all(supplied):
        raise ValueError("production anchor sailing and factory order date are required together")
    if not all(supplied):
        return None
    if focus_sailing_id is None or focus_sailing_id not in s.sailings:
        raise ValueError("factory planning requires a valid focus sailing")
    if production_anchor_sailing_id not in s.sailings:
        raise ValueError("unknown production anchor sailing")
    order_date = (factory_order_date if isinstance(factory_order_date, date)
                  else date.fromisoformat(str(factory_order_date)))
    focus = s.sailings[focus_sailing_id]
    anchor = s.sailings[production_anchor_sailing_id]
    if anchor.departure <= focus.departure:
        raise ValueError("production anchor sailing must depart after the focus sailing")
    anchor_timing = eng.sailing_timing(production_anchor_sailing_id, s)
    if (anchor_timing.timing_state in ("departed", "exceptional")
            and production_anchor_sailing_id not in s.pursuits):
        raise ValueError("production anchor sailing is not viable")
    boundary_candidates = []
    for sailing_id, sailing in s.sailings.items():
        timing = eng.sailing_timing(sailing_id, s)
        if sailing.departure <= anchor.departure:
            continue
        if timing.timing_state in ("departed", "exceptional") and sailing_id not in s.pursuits:
            continue
        boundary_candidates.append((sailing.departure, sailing_id, timing))
    if not boundary_candidates:
        raise ValueError("no viable sailing exists after the production anchor")
    _, boundary_id, boundary_timing = min(boundary_candidates)
    order_state = ("past_due" if order_date < eng.today else
                   "late_for_anchor" if order_date > anchor_timing.production_readiness
                   else "on_time")
    return {
        "focus_sailing_id": focus_sailing_id,
        "production_anchor_sailing_id": production_anchor_sailing_id,
        "coverage_boundary_sailing_id": boundary_id,
        "factory_order_date": order_date,
        "anchor_production_readiness": anchor_timing.production_readiness,
        "coverage_through": boundary_timing.warehouse_arrival,
        "horizon_days": max(0, (boundary_timing.warehouse_arrival - eng.today).days),
        "order_timing_state": order_state,
    }


def _destination_supply_through(eng, s, *, product_id: str,
                                through: date) -> Decimal:
    """Combine expectations and unmatched confirmed transit without duplication."""
    qualified = ZERO
    for expectation in s.expectations.values():
        if expectation.product_id != product_id or not expectation.open:
            continue
        arrival = (eng.sailing_timing(expectation.sailing_id, s).warehouse_arrival
                   if expectation.sailing_id in s.sailings else through)
        if arrival <= through:
            qualified += expectation.remaining_m2
    match = eng._match(s)
    observations = {row.obs_id: row for row in eng._transit_observations(s)}
    for observation_id, amount in match.unmatched.items():
        observation = observations[observation_id]
        if observation.product_id != product_id:
            continue
        arrival = (observation.eta + timedelta(days=sm.WAREHOUSE_BUFFER_DAYS)
                   if observation.eta else observation.observed_at + timedelta(
                       days=eng.config.default_voyage_days + sm.WAREHOUSE_BUFFER_DAYS))
        if arrival <= through:
            qualified += amount
    return pm.q2(qualified)


def _replenishment_cycle(eng, s, *, focus: dict | None,
                         production_need: list[dict],
                         current_production: dict | None,
                         factory_history: list[dict]) -> dict:
    """Compose the cycle from existing destination and timing authority only."""
    scenario_inputs = getattr(eng, "cycle_scenario_inputs", None)
    if scenario_inputs is not None:
        inputs = {key: value for key, value in scenario_inputs.items()
                  if key != "synthetic_cycle_only"}
        return _jsonable(compose_replenishment_cycle(**inputs))
    timings = []
    candidates = []
    for sid, sailing in s.sailings.items():
        timing = eng.sailing_timing(sid, s)
        timings.append(timing.warehouse_arrival)
        candidates.append({
            "sailing_id": sid, "name": sailing.name,
            "departure": timing.departure,
            "warehouse_arrival": timing.warehouse_arrival,
            "timing_state": timing.timing_state,
            "selectable": (timing.timing_state not in ("departed", "exceptional")
                           or sid in s.pursuits),
        })
    fallback_end = None
    if focus is not None:
        planning = eng.planning_window_for(focus["sailing_id"], s)
        fallback_end = planning["fallback"].get("projected_until")
    horizon_end = max([d for d in timings + [fallback_end] if d is not None],
                      default=eng.today)

    match = eng._match(s)
    observations = {row.obs_id: row for row in eng._transit_observations(s)}
    relevant = {row["product_id"] for row in production_need
                if Decimal(str(row["uncovered_m2"])) > ZERO}
    protection_rows = []
    for product_id in sorted(relevant):
        dated = []
        presented = []
        for expectation in s.expectations.values():
            if expectation.product_id != product_id or not expectation.open:
                continue
            arrival = (eng.sailing_timing(expectation.sailing_id, s).warehouse_arrival
                       if expectation.sailing_id in s.sailings else horizon_end)
            dated.append((arrival, expectation.remaining_m2))
            presented.append({"arrival_date": arrival,
                              "m2": _s(pm.q2(expectation.remaining_m2)),
                              "authority": "expected"})
        for observation_id, amount in match.unmatched.items():
            observation = observations[observation_id]
            if observation.product_id != product_id:
                continue
            arrival = (observation.eta + timedelta(days=sm.WAREHOUSE_BUFFER_DAYS)
                       if observation.eta else observation.observed_at + timedelta(
                           days=eng.config.default_voyage_days + sm.WAREHOUSE_BUFFER_DAYS))
            dated.append((arrival, amount))
            presented.append({"arrival_date": arrival, "m2": _s(pm.q2(amount)),
                              "authority": "verified_transit"})
        row = product_buffer_breach(
            as_of=eng.today, warehouse_m2=eng.warehouse_value(product_id, s) or ZERO,
            dated_destination_events=dated, buffer_m2=eng.buffer_m2(product_id, s),
            daily_velocity_m2=eng.velocity(product_id, s), horizon_end=horizon_end)
        row["product_id"] = product_id
        row["dated_destination_events"] = presented
        protection_rows.append(row)

    current = None
    if focus is not None:
        current = {
            "sailing_id": focus["sailing_id"], "name": focus["name"],
            "order_by": focus["timing"]["order_by"],
            "departure": focus["timing"]["departure"],
            "warehouse_arrival": focus["timing"]["warehouse_arrival"],
        }
    production = None
    if current_production is not None:
        production_ref = current_production.get("production_ref")
        order_product_ids = {
            line.get("product_id") for line in current_production.get("lines", [])
            if line.get("product_id") is not None}
        reference_matches = [
            row for row in eng._rows(s, "production_planning")
            if production_ref is not None and row.get("production_ref") == production_ref]
        matches_by_product = {}
        for row in reference_matches:
            product_id = row.get("product_id")
            if product_id in order_product_ids:
                matches_by_product.setdefault(product_id, []).append(row)
        lineage_complete = bool(order_product_ids) and all(
            len(matches_by_product.get(product_id, [])) == 1
            for product_id in order_product_ids)
        planning_rows = ([matches_by_product[product_id][0]
                          for product_id in sorted(order_product_ids)]
                         if lineage_complete else [])
        accepted_ready = bool(planning_rows) and all(
            row.get("status") in ("ready", "completed")
            and (row.get("actual_ready_date") is not None
                 or row.get("completion_confirmed") is True)
            for row in planning_rows)
        accepted_in_progress = bool(planning_rows) and not accepted_ready and any(
            row.get("status") == "in_progress" for row in planning_rows)
        siesa_orderable = accepted_ready and all(
            eng.buckets(product_id, s)["siesa_available_effective"] > ZERO
            for product_id in order_product_ids)
        estimates = [row.get("estimated_ready_date") for row in planning_rows
                     if row.get("estimated_ready_date") is not None]
        evidence_dates = [row.get("evidence_as_of") for row in planning_rows
                          if row.get("evidence_as_of") is not None]
        statuses = {row.get("status") for row in planning_rows}
        planning_status = ("completed" if accepted_ready else
                           "in_progress" if accepted_in_progress else
                           next(iter(statuses)) if len(statuses) == 1 else
                           "mixed" if statuses else None)
        production = {
            "production_order_id": current_production["production_order_id"],
            "production_ref": production_ref,
            "order_date": current_production.get("reference_recorded_at"),
            "production_status": planning_status,
            "factory_estimate": max(estimates) if estimates else None,
            "evidence_as_of": max(evidence_dates) if evidence_dates else None,
            "can_add_more": any(row.get("can_add_more") is True
                                for row in planning_rows),
            "accepted_in_progress": accepted_in_progress,
            "accepted_ready": accepted_ready,
            "accepted_actual_ready": accepted_ready,
            "siesa_orderable": siesa_orderable,
        }
    cycle = compose_replenishment_cycle(
        as_of=eng.today, protection_rows=protection_rows,
        current_shipment=current, candidate_sailings=candidates,
        factory_history=factory_history, current_production=production)
    provenance = getattr(eng, "review_provenance", {})
    if (provenance.get("mode") == "historical_local_replay"
            and provenance.get("current_truth") is False):
        cycle["status"] = "evidence_insufficient"
        cycle["material_risk"] = False
        cycle["headline"] = "Evidencia insuficiente"
        cycle["explanation"] = (
            "La repetición histórica conserva el rastro de exposición, pero no representa la verdad operativa actual.")
        protection = cycle["warehouse_protection"]
        protection["buffer_breach_date"] = None
        protection["controlling_product_ids"] = []
        protection["protected_through"] = None
        next_production = cycle["next_production"]
        next_production["target_sailing"] = None
        next_production["factory_order_by"] = None
        window = next_production["readiness_window"]
        window.update({
            "earliest": None, "expected": None, "latest": None,
            "open_ended": True, "confidence": "insufficient",
            "min_lead_days": None, "median_lead_days": None,
            "max_lead_days": None,
        })
        next_production["evidence_warnings"] = list(dict.fromkeys([
            *next_production["evidence_warnings"],
            "historical_replay_not_current_truth",
        ]))
    return _jsonable(cycle)


_HISTORY_LABELS = {
    "suggestion_snapshotted": "Recomendación calculada",
    "plan_decision_recorded": "Decisión del plan",
    "evidence_loaded": "Evidencia actualizada",
    "commitment_recorded": "Compromiso SIESA registrado",
    "expectation_recorded": "Entrada esperada registrada",
}


def _event_decimal(value) -> Decimal:
    text = str(value)
    return Decimal(text[4:] if text.startswith("dec:") else text)


def _product_histories(s, product_ids, limit: int = 50) -> dict[str, list[dict]]:
    """Build bounded product histories in one pass over the event stream."""
    product_ids = set(product_ids)
    histories = {pid: deque(maxlen=limit) for pid in product_ids}
    for event in s.events:
        payload = thaw(event.payload)
        quantities_by_product = {}
        for quantity in payload.get("quantities", []):
            pid = quantity.get("product_id")
            if pid in product_ids:
                quantities_by_product.setdefault(pid, []).append(quantity)
        rows_by_product = {}
        for row in payload.get("rows", []):
            pid = row.get("product_id")
            if pid in product_ids:
                rows_by_product.setdefault(pid, []).append(row)
        relevant = set(quantities_by_product) | set(rows_by_product)
        direct_pid = payload.get("product_id")
        if direct_pid in product_ids:
            relevant.add(direct_pid)
        for product_id in relevant:
            direct = direct_pid == product_id
            quantities = quantities_by_product.get(product_id, [])
            rows = rows_by_product.get(product_id, [])
            value = None
            if direct and payload.get("selected_m2") is not None:
                value = f"{_s(_event_decimal(payload['selected_m2']))} m²"
            elif quantities:
                amount = sum((_event_decimal(q["m2"]) for q in quantities), ZERO)
                value = f"{_s(amount)} m²"
            elif rows:
                amount = rows[-1].get("m2", rows[-1].get("available_m2"))
                value = (f"{_s(_event_decimal(amount))} m²"
                         if amount is not None else None)
            elif payload.get("content", {}).get("suggested_m2") is not None:
                amount = _event_decimal(payload["content"]["suggested_m2"])
                value = f"{_s(amount)} m²"
            label = _HISTORY_LABELS.get(
                event.type, event.type.replace("_", " ").capitalize())
            if event.type == "plan_decision_recorded":
                label = {"edit": "Selección editada", "accept": "Sugerencia aceptada",
                         "manual": "Línea manual registrada",
                         "postpone": "Producto pospuesto"}.get(
                             payload.get("action"), label)
            histories[product_id].append({
                "label": label, "timestamp": event.at, "actor": event.actor,
                "value": value, "source": event.type})
    return {pid: list(entries) for pid, entries in histories.items()}


def _pick_focus(eng, s):
    """The focus sailing: the one with an open plan, else the nearest
    'use', else the nearest normal/late non-departed sailing. Exceptional
    sailings are NEVER a default focus [D3/RV9]."""
    open_plans = [p for p in s.plans.values()
                  if p.lifecycle in ("draft", "finalized")]
    if open_plans:
        p = sorted(open_plans, key=lambda p: p.opened_at or "")[-1]
        return p.sailing_id, p
    candidates = []
    for sid, sl in s.sailings.items():
        state = sm.timing_state(departure=sl.departure, today=eng.today)
        if state in ("departed", "exceptional"):
            continue
        if s.sailing_decisions.get(sid) == "skip":
            continue
        pref = 0 if s.sailing_decisions.get(sid) == "use" else 1
        candidates.append((pref, sl.departure, sid))
    if not candidates:
        return None, None
    return sorted(candidates)[0][2], None


def _present_case(eng, s, imp) -> dict:
    catalog = eng.catalog(s)

    def operator_value(value):
        if isinstance(value, dict):
            out = {}
            for key, item in value.items():
                if key in ("product", "product_id") and isinstance(item, str):
                    row = catalog.get(item, {})
                    out["product"] = {
                        "id": item, "sku": row.get("sku"),
                        "name": row.get("name"), "tier": row.get("tier")}
                else:
                    out[key] = operator_value(item)
            return out
        if isinstance(value, (list, tuple)):
            return [operator_value(item) for item in value]
        return _jsonable(value)

    return {
        "implication_id": imp.implication_id,
        "family": imp.family,
        "severity": imp.severity,
        "evidence": operator_value(thaw(imp.evidence)),
        "consequence": imp.consequence,
        "why_stopped": (
            "La automatización se detuvo porque existe un riesgo material de "
            "inventario, cliente, duplicidad, zarpe, fecha límite o compromiso; "
            "se requiere tu decisión o aceptación explícita del riesgo."
            if imp.severity != "informational" else
            "Se resolvió automáticamente y se conserva para explicar el resultado."),
        "recommendation": imp.recommendation,
        "shipment_effect": operator_value(thaw(imp.shipment_effect)),
        "typed_actions": list(imp.typed_actions),
        "owner": (imp.scope or {}).get("owner", "ashley"),
        "opened_at": imp.opened_at,
    }


def _row(eng, s, plan, pid, history=None) -> dict:
    p = eng.catalog(s)[pid]
    b = eng.buckets(pid, s, exclude_plan=plan.plan_id if plan else None)
    line = plan.lines.get(pid) if plan else None
    sug = (eng.suggestion_for(plan.plan_id, pid, s) if plan else None)
    vel = eng.velocity(pid, s)
    row_imps = [i.implication_id for i in s.implications.values()
                if i.state == "open"
                and (i.scope or {}).get("product_id") == pid]
    stale_imp = next(
        (i for i in s.implications.values()
         if i.state == "open" and plan is not None
         and i.subject_key == f"stale:{plan.plan_id}:{pid}"), None)
    signal = "healthy"
    if row_imps:
        signal = "exception"
    elif line:
        signal = "active_recommendation"
    elif sug and sug["suggested_m2"] > 0:
        signal = "planning_needed"
    elif sug and sug.get("watch_only"):
        signal = "watch"
    selected = (Decimal(str(line["selected_m2"])) if line else ZERO)
    effective = b["siesa_available_effective"]
    raw_available = sum((Decimal(str(r["available_m2"]))
                         for r in eng._rows(s, "siesa_availability")
                         if r.get("product_id") == pid), ZERO)
    committed_elsewhere = max(ZERO, raw_available - effective)
    remaining = max(ZERO, effective - selected)
    shortfall = max(ZERO, selected - effective)
    if sug and not sug["no_basis"]:
        explanation = (
            f"La demanda y el colchón producen una necesidad de {_s(sug['need_m2'])} m²; "
            f"bodega y suministro entrante fechado se descuentan antes de recomendar. "
            f"El límite SIESA efectivo es {_s(effective)} m². La recomendación se "
            f"redondea hacia abajo a medio palé (67.20 m²): {_s(sug['suggested_m2'])} m². "
            f"Seleccionado: {_s(selected)} m² frente a recomendado: "
            f"{_s(sug['suggested_m2'])} m²; límite de planificación: "
            f"{_s(sug['next_arrival'])}.")
    else:
        explanation = (
            "No hay base completa para explicar demanda/necesidad, bodega y suministro "
            "entrante. El límite SIESA efectivo se conserva; el redondeo a medio palé, "
            "la comparación seleccionado vs recomendado y el límite temporal quedan "
            "pendientes de evidencia autoritativa.")
    product = _product_ref(eng.catalog(s), pid)
    product["tier"] = eng.tier_for(pid, s)
    return {
        "product": product,
        "warehouse_m2": _s(b["warehouse"]),
        "supply_buckets": {
            "warehouse_m2": _s(b["warehouse"]),
            "siesa_available_m2": _s(b["siesa_available_effective"]),
            "expected_incoming_m2": _s(b["expected"]),
            "in_transit_m2": _s(b["moving"]),
        },
        "sales_velocity": {"daily_m2": _s(vel), "basis_days": 90},
        "buffer_m2": _s(eng.buffer_m2(pid, s)),
        "siesa_available_effective_m2": _s(b["siesa_available_effective"]),
        "committed_origin_m2": _s(b["committed_origin"]),
        "expected_incoming_m2": _s(b["expected"]),
        "in_transit_unmatched_m2": _s(b["moving"]),
        "held_m2": _s(b["held"]),
        "window": ({"next_arrival": _s(sug["next_arrival"]),
                    "window_days": sug["window_days"],
                    "fallback_flagged": sug["fallback"]} if sug else None),
        "need_m2": _s(sug["need_m2"]) if sug else None,
        "suggested_m2": _s(sug["suggested_m2"]) if sug else None,
        "uncovered_m2": _s(sug["uncovered_m2"]) if sug else None,
        "capped_by_availability": bool(sug and sug["capped"]),
        "no_basis": bool(sug and sug["no_basis"]),
        "selected_m2": (_s(Decimal(str(line["selected_m2"])))
                        if line else None),
        "origin": line.get("origin") if line else None,
        # [AB7] a formerly accepted suggestion whose recommendation moved
        # materially: stale/review-needed, with the exact delta
        "decision_stale": stale_imp is not None,
        "stale_delta_m2": (_jsonable(thaw(stale_imp.shipment_effect)).get(
            "delta_m2") if stale_imp is not None else None),
        "derived_pallets": (_s(pm.q2(Decimal(str(line["selected_m2"]))
                                     / pm.M2_PER_PALLET))
                            if line else None),
        "trace": _jsonable(thaw(sug["trace"])) if sug else None,
        "recommendation_explanation": explanation,
        "siesa_conservation": {
            "available_before_current_selection_m2": _s(pm.q2(raw_available)),
            "selected_for_current_plan_m2": _s(pm.q2(selected)),
            "committed_elsewhere_m2": _s(pm.q2(committed_elsewhere)),
            "remaining_after_selection_m2": _s(pm.q2(remaining)),
            "shortfall_m2": _s(pm.q2(shortfall)),
        },
        "history": history or [],
        "row_implications": row_imps,
        "row_signal": signal,
    }


def _next_action(eng, s, plan, status, attention) -> str:
    if plan is None:
        if s.sailings:
            return ("choose which upcoming sailing to use "
                    "(use / watch / skip)")
        return "record the sailing calendar (direct entry or paste)"
    if attention:
        top = attention[0]
        return (f"resolve the {top['family'].replace('_', ' ')} case — "
                f"{top['recommendation']}")
    if status == "blocked":
        return "load fresh SIESA availability to unblock this plan"
    if plan.lifecycle == "draft":
        if plan.lines:
            return "review totals and finalize the plan (produces the exact "\
                   "SIESA handoff)"
        return "accept or edit the suggested m² per product"
    if plan.lifecycle == "finalized":
        pending = [c for c in s.commitments.values()
                   if c.state == "pending_observation"]
        has_ref = any(c.handoff_order_ref is not None
                      for c in s.commitments.values())
        if not has_ref:
            return ("enter the orders in SIESA and record each order's "
                    "SIESA reference")
        if pending:
            return "refresh SIESA availability to observe the commitment"
        return "record the booking confirmation when the agent confirms"
    return "review the sailing rail"


def compose_workspace(eng, plan_id: Optional[str] = None, principal=None,
                      focus_sailing_id: Optional[str] = None,
                      production_anchor_sailing_id: Optional[str] = None,
                      factory_order_date: date | str | None = None,
                      _candidate_only: bool = False, _state=None) -> dict:
    s = eng.state if _state is None else _state
    catalog = eng.catalog(s)
    product_histories = _product_histories(s, catalog)
    if plan_id is not None and plan_id in s.plans:
        plan = s.plans[plan_id]
        focus_sid = plan.sailing_id
    elif focus_sailing_id is not None:
        if focus_sailing_id not in s.sailings:
            raise ValueError(f"unknown focus sailing {focus_sailing_id}")
        if eng.sailing_timing(focus_sailing_id, s).timing_state == "departed":
            raise ValueError(f"departed focus sailing {focus_sailing_id}")
        focus_sid = focus_sailing_id
        plan = next((p for p in s.plans.values()
                     if p.sailing_id == focus_sid
                     and p.lifecycle in ("draft", "finalized")), None)
    else:
        focus_sid, plan = _pick_focus(eng, s)

    rail = []
    for sid, sl in sorted(s.sailings.items(),
                          key=lambda kv: kv[1].departure):
        t = eng.sailing_timing(sid, s)
        rail.append({
            "sailing_id": sid, "carrier": sl.carrier, "name": sl.name,
            "departure": _s(sl.departure),
            "timing_state": t.timing_state,
            "decision": s.sailing_decisions.get(sid, "watch"),
            "pursued": sid in s.pursuits,
            "plan_id": next((p.plan_id for p in s.plans.values()
                             if p.sailing_id == sid
                             and p.lifecycle != "closed"), None),
            "excluded_from_normal": t.timing_state in ("exceptional",
                                                       "departed"),
        })

    focus = None
    if focus_sid is not None:
        t = eng.sailing_timing(focus_sid, s)
        sl = s.sailings[focus_sid]
        focus = {
            "sailing_id": focus_sid, "carrier": sl.carrier, "name": sl.name,
            "decision": s.sailing_decisions.get(focus_sid, "watch"),
            "timing": {
                "departure": _s(t.departure), "order_by": _s(t.order_by),
                "hard_cutoff": _s(t.hard_cutoff),
                "booking_date": _s(t.booking_date),
                "production_readiness": _s(t.production_readiness),
                "eta": _s(t.eta),
                "warehouse_arrival": _s(t.warehouse_arrival),
                "timing_state": t.timing_state,
                "days_to_order_by": t.days_to_order_by,
                "days_to_departure": t.days_to_departure,
            },
        }

    attention: list = []
    status, reason = ("ready", None)
    plan_block = None
    if plan is not None:
        status, reason = eng.plan_status(plan.plan_id, s)
        raw_attention = eng.attention_for_plan(plan.plan_id, s)
        attention = [_present_case(eng, s, s.implications[c["implication_id"]])
                     for c in raw_attention]
        totals = eng.plan_totals(plan.plan_id, s)
        split = s.bl_splits.get(plan.plan_id)
        handoff = None
        if plan.handoff_id and plan.handoff_id in s.handoffs \
                and not s.handoffs[plan.handoff_id]["superseded"]:
            h = thaw(s.handoffs[plan.handoff_id]["handoff"])
            for o in h.get("orders", []):
                for l in o.get("lines", []):
                    l["m2"] = _s(Decimal(str(l["m2"])))
            handoff = {"handoff_id": plan.handoff_id,
                       "produced_at": s.handoffs[plan.handoff_id][
                           "produced_at"],
                       "orders": h.get("orders", []),
                       "supersedes": s.handoffs[plan.handoff_id][
                           "supersedes"]}
        plan_block = {
            "plan_id": plan.plan_id,
            "lifecycle": plan.lifecycle,
            "plan_status": status,
            "status_reason": reason,
            "late_state": plan.late_state,
            "rows": [_row(eng, s, plan, pid, product_histories.get(pid))
                     for pid in catalog],
            "totals": totals,
            "bl_split": (thaw({
                "origin": split["origin"],
                "groups": [{"bl_no": g["bl_no"],
                            "container_count": g["container_count"],
                            "lines": [{"product_id": l["product_id"],
                                       "product": {
                                           "id": l["product_id"],
                                           "sku": eng.catalog(s)[l["product_id"]].get("sku"),
                                           "name": eng.catalog(s)[l["product_id"]].get("name"),
                                           "tier": eng.catalog(s)[l["product_id"]].get("tier")},
                                       "m2": _s(Decimal(str(l["m2"])))}
                                      for l in g["lines"]]}
                           for g in split["groups"]]}) if split else None),
            "handoff": handoff,
            "amendments": [],
            "recompute_history": [
                {"trigger": r["trigger"], "at": r["at"],
                 "before": _jsonable(thaw(r["before"])), "after": _jsonable(thaw(r["after"]))}
                for r in s.recomputes if r["plan_id"] == plan.plan_id],
        }

    handoff_orders = []
    def product_ref(product_id):
        row = catalog.get(product_id, {})
        return {"id": product_id, "sku": row.get("sku"),
                "name": row.get("name"), "tier": row.get("tier")}

    for handoff in s.handoffs.values():
        if handoff["superseded"]:
            continue
        for order in handoff["handoff"].get("orders", []):
            ref = order.get("order_ref")
            commitment = next((c for c in s.commitments.values()
                               if c.handoff_order_ref == ref
                               and c.state != "released"), None)
            quantities = [{"product_id": line["product_id"],
                           "product": product_ref(line["product_id"]),
                           "m2": _s(Decimal(str(line["m2"])))}
                          for line in order.get("lines", [])]
            plan_for_block = s.plans[handoff["plan_id"]]
            sailing_for_block = s.sailings[plan_for_block.sailing_id]
            handoff_orders.append({
                "handoff_order_ref": ref,
                "plan_id": handoff["plan_id"],
                "quantities": quantities,
                "entry_state": (
                    "observed" if commitment and commitment.state == "observed"
                    else "reference_recorded" if commitment
                    else "pending_entry"),
                "sailing": {"id": sailing_for_block.sailing_id,
                            "name": sailing_for_block.name,
                            "carrier": sailing_for_block.carrier,
                            "departure": sailing_for_block.departure.isoformat()},
                "commitment_id": (commitment.commitment_id
                                  if commitment else None),
                "legal_actions": [],
            })
    progression = {
        "commitments": [
            {"commitment_id": c.commitment_id, "siesa_ref": c.siesa_ref,
             "handoff_order_ref": c.handoff_order_ref, "state": c.state,
             "quantities": [{"product_id": q["product_id"],
                             "product": product_ref(q["product_id"]),
                             "m2": _s(Decimal(str(q["m2"])))}
                            for q in c.quantities]}
            for c in s.commitments.values()],
        "bookings": [
            {"booking_id": b.booking_id, "booking_ref": b.booking_ref,
             "sailing_id": b.sailing_id,
             "sailing": {"id": b.sailing_id,
                         "name": s.sailings[b.sailing_id].name,
                         "carrier": s.sailings[b.sailing_id].carrier,
                         "departure": s.sailings[b.sailing_id].departure.isoformat()},
             "commitment_refs": list(b.commitment_refs)}
            for b in s.bookings.values()],
        "expected_incoming": [
            {"exp_id": e.exp_id, "product_id": e.product_id,
             "product": product_ref(e.product_id),
             "state": e.state, "confirmed_m2": _s(e.confirmed_m2),
             "effective_m2": _s(e.effective_m2),
             "received_m2": _s(e.received_m2),
             "remaining_m2": _s(e.remaining_m2), "overdue": e.overdue,
             "sailing_id": e.sailing_id,
             "sailing": ({"id": e.sailing_id,
                          "name": s.sailings[e.sailing_id].name,
                          "carrier": s.sailings[e.sailing_id].carrier,
                          "departure": s.sailings[e.sailing_id].departure.isoformat()}
                         if e.sailing_id in s.sailings else None)}
            for e in s.expectations.values()],
        "handoff_orders": handoff_orders,
        "order_progress": {
            "entered_count": sum(1 for order in handoff_orders
                                 if order["entry_state"] != "pending_entry"),
            "reference_recorded_count": sum(
                1 for order in handoff_orders
                if order["entry_state"] in ("reference_recorded", "observed")),
            "observed_count": sum(1 for order in handoff_orders
                                  if order["entry_state"] == "observed"),
            "total": len(handoff_orders),
        },
    }

    open_owner_routed = [
        _present_case(eng, s, i) for i in s.implications.values()
        if i.state == "open"
        and (i.scope or {}).get("owner") not in (None, "ashley")]
    resolved = [
        _present_case(eng, s, i) | {"state": i.state,
                            "resolution": _jsonable(thaw(i.resolution))}
        for i in s.implications.values() if i.state != "open"]
    informational = [
        _present_case(eng, s, i) for i in s.implications.values()
        if i.state == "open" and i.severity == "informational"]

    tier_map = eng.tier_map(s)
    product_roster = []
    for product_id in sorted(
            catalog,
            key=lambda pid: (catalog[pid].get("name") or catalog[pid].get("sku") or pid).casefold()):
        product = _product_ref(catalog, product_id)
        product["tier"] = tier_map[product_id]
        product_roster.append({
            "product": product,
            "tier": tier_map[product_id],
            "daily_velocity_m2": _s(eng.velocity(product_id, s) or ZERO),
            "velocity_basis_days": 90,
            "buffer_m2": _s(eng.buffer_m2(product_id, s)),
        })

    workspace = {
        "demo_banner": getattr(
            eng, "review_banner",
            "Entorno local sintético — sin datos reales, sin escritura en SIESA, sin despliegue"),
        "first_ten_seconds": (
            "qué riesgo requiere resolución, qué recomienda hacer el sistema "
            "y cómo cambia eso lo que debe embarcarse en este zarpe"),
        "focus_sailing": focus,
        "sailing_rail": rail,
        "evidence_readiness": [
            {**r, "as_of": _s(r["as_of"])}
            for r in eng.evidence_readiness(s)],
        "attention": attention,
        "plan": plan_block,
        "product_roster": product_roster,
        "next_action": _next_action(eng, s, plan, status, attention),
        "progression": progression,
        "monthly_manufacturing_context": [
            {"product_id": e["product_id"],
             "product": {
                 "id": e["product_id"],
                 "sku": eng.catalog(s).get(e["product_id"], {}).get("sku"),
                 "name": eng.catalog(s).get(e["product_id"], {}).get("name"),
                 "tier": eng.catalog(s).get(e["product_id"], {}).get("tier"),
             },
             "m2": _s(Decimal(str(e["m2"]))),
             "from_plan": e["plan_id"], "computed_at": e["computed_at"]}
            for e in eng.monthly_context(s)],
        "history": {
            "owner_routed": open_owner_routed,
            "informational": informational,
            "resolved": resolved,
        },
    }
    if principal is None:
        # Preserve the accepted V1 characterization; §19 carriers are V2-only.
        workspace["progression"].pop("handoff_orders", None)
        workspace["progression"].pop("order_progress", None)
        return workspace

    factory_planning = _factory_planning_context(
        eng, s, focus_sailing_id=focus_sid,
        production_anchor_sailing_id=production_anchor_sailing_id,
        factory_order_date=factory_order_date)
    workspace["factory_planning"] = _jsonable(factory_planning)
    production_need = []
    for product_id in catalog:
        suggestion = (eng.suggestion_for(plan.plan_id, product_id, s)
                      if plan is not None else None)
        line = plan.lines.get(product_id) if plan is not None else None
        selected = Decimal(str(line["selected_m2"])) if line else ZERO
        proposed_focus_shipment = (selected if line is not None else
                                   suggestion["suggested_m2"] if suggestion else ZERO)
        total_need = suggestion["need_m2"] if suggestion else ZERO
        future_demand = ZERO
        buffer = eng.buffer_m2(product_id, s)
        projected_warehouse = eng.warehouse_value(product_id, s) or ZERO
        qualified_incoming = ZERO
        focus_shipment = proposed_focus_shipment
        projected_boundary_balance = projected_warehouse
        gross_need_formula = "shipment_cycle_uncovered_need"
        anchor_readiness = None
        coverage_through = None
        if factory_planning is not None:
            coverage_through = factory_planning["coverage_through"]
            anchor_readiness = factory_planning["anchor_production_readiness"]
            velocity = eng.velocity(product_id, s) or ZERO
            future_demand = pm.q2(
                velocity * Decimal(factory_planning["horizon_days"]))
            qualified_incoming = _destination_supply_through(
                eng, s, product_id=product_id, through=coverage_through)
            expected_on_focus = sum((
                expectation.remaining_m2
                for expectation in s.expectations.values()
                if expectation.product_id == product_id
                and expectation.open
                and expectation.sailing_id == focus_sid
                and eng.sailing_timing(expectation.sailing_id, s).warehouse_arrival
                <= coverage_through
            ), ZERO)
            focus_shipment = pm.q2(max(ZERO, proposed_focus_shipment - expected_on_focus))
            projected_boundary_balance = pm.q2(
                projected_warehouse + qualified_incoming + focus_shipment
                - future_demand)
            raw_gross = max(ZERO, buffer - projected_boundary_balance)
            gross_factory_need = pm.round_up_to_half_pallet(raw_gross)
            gross_need_formula = (
                "max(0, future_demand_m2 + buffer_m2 - projected_warehouse_m2 - "
                "qualified_incoming_m2 - focus_shipment_m2) rounded up to half-pallet")
        else:
            gross_factory_need = (suggestion["uncovered_m2"]
                                  if suggestion else ZERO)
        qualified_rows = []
        excluded_rows = []
        for planning_row in eng._rows(s, "production_planning"):
            if (planning_row.get("product_id") != product_id
                    or not planning_row.get("confident_match")):
                continue
            amount = Decimal(str(planning_row.get("m2", ZERO)))
            presented = {
                "production_ref": planning_row.get("production_ref"),
                "m2": _s(amount), "status": planning_row.get("status"),
                "estimated_ready_date": _s(
                    planning_row.get("estimated_ready_date")),
                "evidence_as_of": _s(planning_row.get("evidence_as_of")),
            }
            completed = (planning_row.get("completion_confirmed") is True
                         or planning_row.get("status") in ("ready", "completed")
                         or planning_row.get("actual_ready_date") is not None)
            if completed:
                presented["excluded_reason"] = (
                    "completed_waiting_for_siesa_visibility")
                excluded_rows.append(presented)
            elif planning_row.get("status") not in ("scheduled", "in_progress"):
                presented["excluded_reason"] = "not_active_production"
                excluded_rows.append(presented)
            elif amount <= ZERO:
                presented["excluded_reason"] = "non_positive_m2"
                excluded_rows.append(presented)
            elif not planning_row.get("production_ref"):
                presented["excluded_reason"] = "missing_production_reference"
                excluded_rows.append(presented)
            elif planning_row.get("estimated_ready_date") is None:
                presented["excluded_reason"] = "missing_estimated_readiness"
                excluded_rows.append(presented)
            elif (anchor_readiness is not None
                  and planning_row.get("estimated_ready_date") > anchor_readiness):
                presented["excluded_reason"] = "ready_after_anchor_cutoff"
                excluded_rows.append(presented)
            else:
                qualified_rows.append(presented)
        qualified_m2 = sum(
            (Decimal(str(row["m2"])) for row in qualified_rows), ZERO)
        net_factory_need = max(ZERO, gross_factory_need - qualified_m2)
        product = _product_ref(catalog, product_id)
        product["tier"] = eng.tier_for(product_id, s)
        production_need.append({
            "product_id": product_id, "product": product,
            "total_need_m2": _s(total_need),
            "shipment_covered_m2": _s(selected),
            "uncovered_m2": _s(suggestion["uncovered_m2"]
                               if suggestion else ZERO),
            "future_demand_m2": _s(future_demand),
            "buffer_m2": _s(buffer),
            "projected_warehouse_m2": _s(projected_warehouse),
            "qualified_incoming_m2": _s(qualified_incoming),
            "focus_shipment_m2": _s(focus_shipment),
            "projected_boundary_balance_m2": _s(projected_boundary_balance),
            "gross_future_factory_need_m2": _s(gross_factory_need),
            "gross_need_formula": gross_need_formula,
            "qualified_approved_production_m2": _s(qualified_m2),
            "net_new_production_required_m2": _s(net_factory_need),
            "qualified_production_rows": qualified_rows,
            "excluded_production_rows": excluded_rows,
            "netting_formula": (
                "max(0, gross_future_factory_need_m2 - "
                "qualified_approved_production_m2)"),
            "timing_context": {
                "planning_boundary": (_s(coverage_through)
                                      if coverage_through is not None else
                                      _s(suggestion["next_arrival"])
                                      if suggestion else None),
                "anchor_production_readiness": _s(anchor_readiness),
                "copy": ("Demanda y suministro proyectados hasta el siguiente barco viable."
                         if factory_planning is not None else
                         "Contexto orientativo para conversar el ciclo de producción; sin fecha firme."),
            },
            "authority_notice": (
                "Este contexto no es suministro del embarque, no aumenta inventario, "
                "no reserva SIESA y no crea una orden."),
        })
    workspace["progression"]["production_need"] = production_need
    review_provenance = getattr(eng, "review_provenance", {
        "mode": "synthetic_test_scenario", "as_of": _s(eng.today),
        "current_truth": False, "source": "test_fixture"})
    workspace["review_provenance"] = review_provenance

    candidate = None
    if factory_planning is not None and plan is not None:
        cycle_as_of = _s(review_provenance.get("as_of", eng.today))
        if cycle_as_of != _s(eng.today):
            raise ValueError("review provenance as_of differs from calculation cut")
        recommendation_rows = []
        calculation_fields = (
            "future_demand_m2", "buffer_m2", "projected_warehouse_m2",
            "qualified_incoming_m2", "focus_shipment_m2",
            "projected_boundary_balance_m2", "gross_future_factory_need_m2",
            "qualified_approved_production_m2", "net_new_production_required_m2",
            "qualified_production_rows", "excluded_production_rows",
            "gross_need_formula", "netting_formula")
        for row in production_need:
            recommendation = Decimal(str(row["net_new_production_required_m2"]))
            if recommendation <= ZERO:
                continue
            recommendation_rows.append({
                "product_id": row["product_id"],
                "selected_m2": f"{recommendation:.2f}",
                "recommendation_m2": f"{recommendation:.2f}",
                "origin": "production_recommendation",
                "calculation": {key: copy.deepcopy(row[key])
                                for key in calculation_fields},
            })
        if recommendation_rows:
            candidate = {
                "plan_id": plan.plan_id,
                "focus_sailing_id": focus_sid,
                "production_anchor_sailing_id": factory_planning[
                    "production_anchor_sailing_id"],
                "factory_order_date": _s(factory_planning["factory_order_date"]),
                "coverage_boundary_sailing_id": factory_planning[
                    "coverage_boundary_sailing_id"],
                "anchor_production_readiness": _s(factory_planning[
                    "anchor_production_readiness"]),
                "coverage_through": _s(factory_planning["coverage_through"]),
                "cycle_as_of": cycle_as_of,
                "calculation_head_seq": len(s.events),
                "recommendation_rows": recommendation_rows,
            }
            candidate["candidate_fingerprint"] = _canonical_sha256(candidate)
    if _candidate_only:
        return candidate
    workspace["scheduled_dispatch_context"] = _jsonable(copy.deepcopy(
        getattr(eng, "scheduled_dispatch_context", [])))
    workspace["historical_factory_orders"] = copy.deepcopy(
        getattr(eng, "historical_factory_orders", []))

    # T1 version-2 additions are server-composed from current authority; the
    # legacy pure carrier remains available only to accepted characterization.
    from .action_legality import compose_legal_actions
    legal = compose_legal_actions(eng, principal, plan_id=plan_id,
                                  production_candidate=candidate)
    amendment_rows = {}
    for amendment in s.order_amendments.values():
        amendment_rows[amendment.amendment_id] = {
            "amendment_id": amendment.amendment_id,
            "order_kind": amendment.order_kind,
            "order_id": amendment.order_id,
            "original_refs": list(amendment.original_refs),
            "lifecycle": amendment.lifecycle,
            "lines": [{"product_id": product_id,
                       "product": _product_ref(catalog, product_id),
                       "baseline_m2": _s(line["baseline_m2"]),
                       "selected_m2": _s(line["selected_m2"])}
                      for product_id, line in amendment.lines.items()],
            "handoff": _jsonable(thaw(amendment.handoff)) if amendment.handoff else None,
            "amendment_ref": amendment.amendment_ref,
            "legal_actions": legal.get(f"amendment:{amendment.amendment_id}", []),
        }
    if plan_block is not None:
        plan_block["amendments"] = [copy.deepcopy(amendment_rows[a.amendment_id])
                                    for a in s.order_amendments.values()
                                    if a.order_kind == "shipment"
                                    and a.order_id == plan_block["plan_id"]]
    production_need_by_product = {
        row["product_id"]: row for row in production_need}
    production_rows = []
    for order in s.production_orders.values():
        handoff = _jsonable(thaw(order.handoff)) if order.handoff else None
        if handoff is not None:
            handoff["lines"] = [
                {**line, "product": _product_ref(catalog, line["product_id"])}
                for line in handoff.get("lines", [])]
        production_rows.append({
            "production_order_id": order.production_order_id,
            "source_plan_id": order.source_plan_id,
            "cycle_as_of": _s(order.cycle_as_of),
            "factory_order_date": _s(order.factory_order_date),
            "required_by": _s(order.required_by),
            "calculation_head_seq": order.calculation_head_seq,
            "candidate_fingerprint": order.candidate_fingerprint,
            "planning_provenance": _jsonable(thaw(order.planning_provenance)),
            "lifecycle": order.lifecycle,
            "total_m2": _s(sum(
                (Decimal(str(line["selected_m2"])) for line in order.lines.values()),
                Decimal("0"))),
            "lines": [{"product_id": product_id,
                       "product": _product_ref(catalog, product_id),
                       "total_need_m2": production_need_by_product.get(
                           product_id, {}).get("total_need_m2", "0.00"),
                       "shipment_covered_m2": production_need_by_product.get(
                           product_id, {}).get("shipment_covered_m2", "0.00"),
                       "selected_m2": _s(line["selected_m2"]),
                       "recommendation_m2": _s(line["recommendation_m2"]),
                       "origin": line["origin"],
                       "calculation": (_jsonable(thaw(line["calculation"]))
                                       if line.get("calculation") is not None else None)}
                      for product_id, line in order.lines.items()],
            "handoff": handoff,
            "production_ref": order.production_ref,
            "reference_recorded_at": order.reference_recorded_at,
            "history": [],
            "amendments": [copy.deepcopy(amendment_rows[a.amendment_id])
                           for a in s.order_amendments.values()
                           if a.order_kind == "production"
                           and a.order_id == order.production_order_id],
            "legal_actions": legal.get(
                f"production-order:{order.production_order_id}", []),
        })
    open_actions = legal.get(
        f"production-plan:{plan.plan_id}", []) if plan is not None else []
    workspace["production_orders"] = {
        "candidate": copy.deepcopy(candidate),
        "current": production_rows[-1] if production_rows else None,
        "orders": production_rows,
        "open_actions": open_actions,
        "legal_actions": open_actions,
        "provenance": {
            "mode": workspace["review_provenance"]["mode"],
            "as_of": workspace["review_provenance"]["as_of"],
            "current_truth": workspace["review_provenance"]["current_truth"],
        },
        "catalog_products": [_product_ref(catalog, product_id)
                             for product_id in catalog],
        "recommendation_rows": [{
            "product_id": row["product_id"],
            "product": row["product"],
            "total_need_m2": row["total_need_m2"],
            "shipment_covered_m2": row["shipment_covered_m2"],
            "recommendation_m2": row["net_new_production_required_m2"],
        } for row in production_need],
        "historical_factory_context": copy.deepcopy(
            workspace["historical_factory_orders"]),
    }
    workspace["workspace_schema_version"] = 2
    workspace["head_seq"] = len(s.events)
    workspace["planning_window"] = (
        _jsonable(eng.planning_window_for(focus_sid, s))
        if focus_sid is not None else None)
    workspace["replenishment_cycle"] = _replenishment_cycle(
        eng, s, focus=focus, production_need=production_need,
        current_production=(production_rows[-1] if production_rows else None),
        factory_history=workspace["historical_factory_orders"])
    for item in workspace["sailing_rail"]:
        item["legal_actions"] = legal.get(f"rail:{item['sailing_id']}", [])
    for case in workspace["attention"]:
        case["legal_actions"] = legal.get(
            f"attention:{case['implication_id']}", [])
    for section in workspace["history"].values():
        for case in section:
            case["legal_actions"] = legal.get(
                f"attention:{case['implication_id']}", [])

    pb = workspace["plan"]
    if pb is not None:
        pid = pb["plan_id"]
        pb["legal_actions"] = legal.get(f"plan:{pid}", []) + legal.get(
            f"reallocation:{pid}", [])
        freshness = workspace["evidence_readiness"]
        for row in pb["rows"]:
            product_id = row["product"]["id"]
            row["source_freshness"] = [dict(x) for x in freshness]
            row["legal_actions"] = legal.get(f"row:{pid}:{product_id}", [])
        split = pb["bl_split"]
        if split is not None:
            split["legal_actions"] = legal.get(f"bl:{pid}", [])
            for group in split["groups"]:
                pallets = ZERO
                for line in group["lines"]:
                    line_pallets = pm.q2(Decimal(str(line["m2"]))
                                          / pm.M2_PER_PALLET)
                    line["pallets"] = _s(line_pallets)
                    pallets += line_pallets
                group["total_pallets"] = _s(pm.q2(pallets))
        handoff = pb.get("handoff")
        if handoff:
            for order in handoff.get("orders", []):
                order["legal_actions"] = legal.get(
                    f"handoff:{order.get('order_ref')}", [])

    booking_actions = [action for key, actions in legal.items()
                       if key.startswith("booking:") for action in actions]
    for commitment in workspace["progression"]["commitments"]:
        commitment["legal_actions"] = [
            copy.deepcopy(action) for action in booking_actions
            if commitment["commitment_id"] in action.get("input_schema", {})
            .get("properties", {}).get("commitment_refs", {})
            .get("items", {}).get("enum", [])]
    for booking in workspace["progression"]["bookings"]:
        booking["legal_actions"] = legal.get(
            f"departure:{booking['booking_id']}", [])
    for expectation in workspace["progression"]["expected_incoming"]:
        expectation["legal_actions"] = []
    for order in workspace["progression"]["handoff_orders"]:
        actions = legal.get(f"handoff:{order['handoff_order_ref']}", [])
        order["legal_actions"] = []
        for action in actions:
            carried = copy.deepcopy(action)
            if carried["command"] == "RecordSiesaOrderReference":
                carried["prefill"] = {"quantities": copy.deepcopy(
                    order["quantities"])}
            order["legal_actions"].append(carried)

    ordered = []
    for prefix in ("attention:", "plan:", "row:", "bl:", "handoff:",
                   "booking:", "departure:", "rail:", "reallocation:"):
        for key, actions in legal.items():
            if key.startswith(prefix):
                ordered.extend(actions)
    if ordered:
        action = ordered[0]
        workspace["next_action"] = {"kind": "command", "primary": True,
                                    **copy.deepcopy(action)}
    else:
        workspace["next_action"] = None
    return workspace
