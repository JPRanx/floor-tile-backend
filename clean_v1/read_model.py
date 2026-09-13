"""
Clean V1 §7 — OrderPlanningWorkspaceReadModel composer.

One read model feeds the UI; the UI owns no business math. Composition is a
PURE function of (domain effective state, current trusted facts, config,
today) — rebuildable and disposable. Every displayed number carries
provenance (§3 traceability invariant). Hard facts, soft context,
projections, decisions and lifecycle state stay structurally distinct.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from lib.constants import MIN_BOAT_PALLETS, PALLETS_PER_CONTAINER, VELOCITY_PERIOD_DAYS

from . import planning_math as pm

D = Decimal
ZERO = D("0")


def _s(v) -> str:
    return str(pm.q2(D(v)))


def _feed_status(as_of, today, cfg) -> str:
    if as_of is None:
        return "missing"
    age = (today - as_of).days
    if age <= cfg.freshness_aging_days:
        return "fresh"
    if age <= cfg.freshness_stale_days:
        return "aging"
    return "stale"


def suggestion_view(eng, pid, state=None) -> dict:
    """The single §6.4b/§6.5/§6.8 suggestion computation, shared by the row
    composer AND the C1 server-side snapshot provider so the frozen copy is
    exactly what the row displayed. Pure function of (engine truth, state)."""
    state = state if state is not None else eng.state
    cfg = eng.config
    today = eng.today
    velocity = eng.velocity(pid)
    buffer_m2 = eng.buffer_m2(pid)
    supply_now = eng.warehouse_value(pid)
    dated = eng._dated_supply_for(pid, state)
    pending = eng.pending_commitment(pid, state=state)
    supply_expected = sum((e.remaining_m2 for e in state.expectations.values()
                           if e.product_id == pid and e.open), ZERO)
    mr = eng._compute_match(state)
    supply_moving = ZERO
    for o in eng._observations:
        if o.feed == "in_transit" and o.product_id == pid and o.confident_match:
            supply_moving += mr.unmatched.get(o.obs_id, ZERO)

    order_date = pm.next_cycle_order_date(today, cfg)
    replen = pm.next_replenishment_date(order_date, eng._voyage_days())
    horizon = pm.protection_horizon_days(today, replen)

    need = pm.true_and_residual_need(
        daily_velocity=velocity, buffer_m2=buffer_m2, horizon_days=horizon,
        supply_now=supply_now, dated_supply=dated,
        pending_commitment_m2=pending, next_replenishment_date=replen)
    suggested = pm.suggest_m2(need.residual_need_m2, daily_velocity=velocity)

    wh_feed = eng.feeds.get("warehouse")
    sales_feed = eng.feeds.get("sales")
    transit_feed = eng.feeds.get("in_transit")
    wh_as_of = wh_feed["as_of"].isoformat() if wh_feed else None
    sales_as_of = sales_feed["as_of"].isoformat() if sales_feed else None
    transit_as_of = transit_feed["as_of"].isoformat() if transit_feed else None

    dos = pm.days_of_stock(supply_now, velocity)
    days_covered = str(dos.quantize(D("0.1"))) if dos is not None else "∞"

    explanation = pm.build_explanation(
        suggested_m2=suggested, velocity=velocity,
        days_covered=days_covered, buffer_m2=buffer_m2,
        next_replenishment_date=replen,
        residual_need_m2=need.residual_need_m2,
        pending_commitment_m2=pending,
        inputs=[
            {"label": "warehouse_m2", "value": _s(supply_now),
             "source": "warehouse_snapshot", "as_of": wh_as_of or "—"},
            {"label": "daily_velocity", "value": _s(velocity),
             "source": f"sac_sales_{VELOCITY_PERIOD_DAYS}d", "as_of": sales_as_of or "—"},
            {"label": "buffer_m2", "value": _s(buffer_m2),
             "source": f"derived: tier {eng.tiers.get(pid, 'C')} buffer rule (§6.2)",
             "as_of": today.isoformat()},
            {"label": "expected_incoming_m2", "value": _s(supply_expected),
             "source": "lifecycle: open ExpectedIncoming (remaining_m2)",
             "as_of": today.isoformat()},
            {"label": "in_transit_unmatched_m2", "value": _s(supply_moving),
             "source": "in_transit_snapshot (unmatched only)",
             "as_of": transit_as_of or "—"},
        ],
        steps=[
            {"label": "projected_demand", "value": _s(need.projected_demand),
             "formula": f"{_s(velocity)} × {horizon} días"},
            {"label": "true_need", "value": _s(need.true_need_m2),
             "formula": "demanda + reserva − bodega − tránsito − confirmado (§6.4b)"},
            {"label": "residual_need", "value": _s(need.residual_need_m2),
             "formula": f"true_need − {_s(pending)} pendiente de fábrica"},
            {"label": "suggested", "value": _s(suggested),
             "formula": "redondeo ↑ al medio pallet (67.2 m²) [D1]"},
        ])

    # C6/S9 — stale or missing critical evidence degrades confidence with a
    # VISIBLE caveat on the suggestion; it never hard-blocks the decision
    stale_feeds = [f for f in ("warehouse", "sales", "in_transit")
                   if _feed_status(
                       (eng.feeds.get(f) or {}).get("as_of"), today, cfg)
                   in ("stale", "missing")]
    if stale_feeds:
        explanation["caveat"] = (
            "Atención: evidencia desactualizada o ausente en "
            + ", ".join(stale_feeds)
            + " — la sugerencia se calculó igualmente, con confianza reducida.")

    return {
        "velocity": velocity, "buffer_m2": buffer_m2,
        "supply_now": supply_now, "supply_expected": supply_expected,
        "supply_moving": supply_moving, "pending": pending, "dated": dated,
        "order_date": order_date, "replen": replen, "horizon": horizon,
        "need": need, "suggested": suggested, "days_covered": days_covered,
        "explanation": explanation,
        "wh_as_of": wh_as_of, "sales_as_of": sales_as_of,
        "transit_as_of": transit_as_of,
    }


FEED_ES = {"warehouse": "bodega", "sales": "ventas", "in_transit": "tránsito",
           "produced": "producido", "siesa": "SIESA",
           "committed_orders": "compromisos de clientes"}

# Typed-resolution labels (closed lists — §10.5; never an outcome-entry verb)
_ACTION_LABEL = {
    "map": "Asignar a un producto existente",

    "discard": "Descartar la referencia",
    "order_now": "Pedir ahora en la fila del producto",
    "amend_production": "Ampliar producción programada",
    "accept_risk_with_note": "Aceptar el riesgo (con nota)",
    "reduce_effective": "Aceptar la cantidad observada",
    "keep_expectation": "Mantener lo confirmado (evidencia mal asignada)",
    "cancel_expectation": "Cancelar la expectativa",
    "adjust_with_note": "Ajustar con nota",
    "accept_evidence": "Aceptar la evidencia",
    "remove_draft_line": "Quitar la línea en borrador duplicada",
    "withdraw_descendant": "Pedir el retiro de la línea duplicada",
    "accept_over_supply": "Aceptar el doble suministro",
    "correct_one_outcome": "Pedir corrección de uno de los resultados",
    "re_correct_outcome": "Pedir re-corrección del resultado",
    "keep_correction_annotate_arrival": "Mantener la corrección y anotar la llegada",
    "manual_arrived": "Marcar como llegado (excepcional)",
    "confirm_intent": "Confirmar intención de ampliación",
    "dismiss": "Descartar",
    "refresh_feed": "Ya actualicé el archivo — cerrar el caso",
    "proceed_with_note": "Continuar con nota (confianza reducida)",
}


def _actions_from(options, params_by_action=None) -> list:
    params_by_action = params_by_action or {}
    return [{"action": o, "label": _ACTION_LABEL.get(o, o),
             "params": params_by_action.get(o, {})} for o in options]


def _present_case(eng, c) -> dict:
    """§10.6 exception content contract, per §10.5 type: distinct reason,
    expected vs observed (source + as-of), concrete consequence, 2–4 typed
    resolutions when Ashley may act — or the calm `Lo gestiona Elicio`
    boundary with NO mutation control. Product-language-first, defensive
    against missing evidence fields."""
    ev = c.evidence or {}
    refs = c.refs or {}
    pid = refs.get("product_id")
    product = None
    if pid and pid in eng.products:
        p = eng.products[pid]
        product = {"id": pid, "name": p.get("name", pid), "sku": p.get("sku", pid)}
    pname = product["name"] if product else (pid or "")

    title = None
    expected = observed = None
    consequence = ""
    authority = "ashley"
    authority_note = None
    actions = _actions_from(c.options)
    raw_reference = None
    candidates = None

    if c.type == "ProductMatchResolution":
        raw_reference = refs.get("raw_reference")
        m2 = ev.get("m2", "?")
        cands = ev.get("candidates") or []
        candidates = [{"product_id": cand.get("product_id"),
                       "name": eng.products.get(cand.get("product_id"), {})
                       .get("name", cand.get("product_id")),
                       "score": cand.get("score")} for cand in cands]
        title = f"Referencia sin identificar — «{raw_reference}»"
        expected = {"text": "Cada fila de evidencia debe corresponder a un "
                            "producto conocido del catálogo.",
                    "source": None, "as_of": None}
        observed = {"text": f"{m2} m² en {FEED_ES.get(ev.get('feed'), ev.get('feed'))} "
                            f"con la referencia «{raw_reference}», sin "
                            "correspondencia segura.",
                    "source": ev.get("feed"), "as_of": ev.get("as_of")}
        consequence = (f"Los {m2} m² quedan fuera de todos los totales "
                       "(ni inventario ni tránsito) hasta que se resuelva.")
        actions = _actions_from(c.options)

    elif c.type == "DecisionRequired" and c.subtype == "customer_commitment_at_risk":
        committed = ev.get("committed_m2", "?")
        available = ev.get("projected_available_m2", "?")
        due = refs.get("commitment_due", "?")
        title = f"Compromiso con cliente en riesgo — {pname}"
        expected = {"text": f"{committed} m² comprometidos con cliente para "
                            f"el {due}, cubiertos con inventario y llegadas.",
                    "source": ev.get("source"), "as_of": ev.get("as_of")}
        observed = {"text": f"Disponibilidad proyectada al {due}: "
                            f"{available} m² — por debajo del compromiso.",
                    "source": ev.get("source"), "as_of": ev.get("as_of")}
        consequence = (f"Sin acción, el compromiso del {due} no queda "
                       f"cubierto (faltan m² frente a los {committed} m² "
                       "prometidos).")

    elif c.type == "DecisionRequired" and c.subtype == "production_delay":
        title = f"Retraso de producción amenaza la ventana — {pname}"
        expected = {"text": f"Llegada a bodega estimada el "
                            f"{ev.get('baseline_arrival', '?')} "
                            f"({ev.get('expected_m2', '?')} m² confirmados), "
                            "protegiendo la reserva hasta la próxima reposición.",
                    "source": "compromiso de fábrica (§6.7)",
                    "as_of": ev.get("baseline_arrival")}
        observed = {"text": f"Plan de producción "
                            f"{refs.get('production_ref', '?')}: entrega "
                            f"{ev.get('planned_delivery', '?')} → llegada "
                            f"estimada {ev.get('delayed_arrival', '?')}.",
                    "source": ev.get("source", "production_planning"),
                    "as_of": ev.get("planned_delivery")}
        if ev.get("commitment_uncovered"):
            consequence = ("Con el retraso, un compromiso con cliente deja "
                           "de estar cubierto antes de la llegada.")
        elif ev.get("stockout_date"):
            consequence = (f"Con el retraso se proyecta quiebre de stock "
                           f"hacia {ev['stockout_date']}, antes de la "
                           f"reposición ({ev.get('horizon_end', '?')}).")
        else:
            consequence = ("Con el retraso, el stock deja de proteger la "
                           "reserva antes de la próxima reposición.")

    elif c.type == "DecisionRequired":
        stockout = ev.get("stockout_date")
        title = f"Quiebre de stock proyectado — {pname}"
        expected = {"text": f"Stock por encima de la reserva "
                            f"({ev.get('buffer_m2', '?')} m²) hasta la "
                            "próxima reposición.",
                    "source": "proyección §6.7", "as_of": eng.today.isoformat()}
        observed = {"text": f"Bodega {ev.get('supply_now', '?')} m² y sin "
                            "reposición planificada que llegue a tiempo"
                            + (f"; quiebre proyectado hacia {stockout}"
                               if stockout else "") + ".",
                    "source": "warehouse_snapshot + proyección §6.7",
                    "as_of": (eng.feeds.get("warehouse") or {}).get("as_of",
                             eng.today).isoformat()
                    if (eng.feeds.get("warehouse") or {}).get("as_of") else None}
        consequence = ((f"Sin acción, el producto se agota hacia {stockout}, "
                        "antes de que llegue un pedido hecho hoy.")
                       if stockout else
                       "Sin acción, el stock queda por debajo de la reserva "
                       "sin reposición a la vista.")

    elif c.type == "ReconciliationException" and c.subtype == "expected_transit_missing":
        title = f"Tránsito esperado sin evidencia — {pname}"
        expected = {"text": f"Despacho estimado el "
                            f"{ev.get('estimated_dispatch', '?')} "
                            f"({ev.get('expected_m2', '?')} m² producidos el "
                            f"{ev.get('produced_on', '?')}); el producto "
                            "debería aparecer en la siguiente carga de tránsito.",
                    "source": "producido (señal provisional) + §10.3",
                    "as_of": ev.get("produced_on")}
        observed = {"text": f"Última carga de tránsito del "
                            f"{ev.get('transit_upload_as_of', '?')}: sin "
                            "rastro del producto.",
                    "source": ev.get("source", "in_transit_snapshot"),
                    "as_of": ev.get("transit_upload_as_of")}
        consequence = ("Si no embarcó, la llegada se retrasa y la reserva "
                       "queda expuesta más tiempo del previsto.")

    elif c.type == "ReconciliationException" and c.subtype in (
            "production_shortfall", "material_transit_variance"):
        exp_m2 = ev.get("expected_m2", "?")
        obs_m2 = ev.get("observed_m2", "?")
        if c.subtype == "production_shortfall":
            title = f"Producción por debajo de lo confirmado — {pname}"
            consequence = ("La diferencia compromete la reserva antes de la "
                           "próxima reposición (consecuencia real de "
                           "suministro, §10.2).")
        else:
            title = f"Tránsito difiere de lo esperado — {pname}"
            consequence = ("La cantidad en movimiento difiere materialmente "
                           "de lo esperado; los totales quedan en duda hasta "
                           "resolver.")
        expected = {"text": f"{exp_m2} m² esperados según el compromiso de "
                            "fábrica.",
                    "source": "compromiso registrado (§4.6a)",
                    "as_of": None}
        observed = {"text": f"{obs_m2} m² observados.",
                    "source": ev.get("source"), "as_of": ev.get("as_of")}
        actions = _actions_from(
            c.options,
            {"reduce_effective": {"m2": obs_m2, "exp_id": refs.get("exp_id")},
             "accept_evidence": {"m2": obs_m2, "exp_id": refs.get("exp_id")},
             "cancel_expectation": {"exp_id": refs.get("exp_id")}})

    elif c.type == "ReconciliationException" and c.subtype == "arrived_contradiction":
        title = f"Corrección contradice una llegada registrada — {pname}" \
            if pname else "Corrección contradice una llegada registrada"
        expected = {"text": "Una expectativa llegada es terminal: la mercancía "
                            "ya está en bodega y no se reabre (§4.3).",
                    "source": "lifecycle", "as_of": None}
        observed = {"text": f"Se registró «{ev.get('recorded_outcome', ev.get('corrected_outcome', '?'))}» "
                            "sobre una línea cuya mercancía ya llegó"
                            + (f" ({ev.get('arrived_m2')} m² recibidos)"
                               if ev.get("arrived_m2") else "") + ".",
                    "source": "corrección administrativa (§4.6d)",
                    "as_of": c.opened_at}
        consequence = ("El historial y la bodega se contradicen; hay que "
                       "decidir cuál versión vale.")

    elif c.type == "ReconciliationException" and c.subtype in (
            "double_order_risk", "double_supply"):
        title = f"Riesgo de pedido duplicado — {pname}" if pname \
            else "Riesgo de pedido duplicado"
        expected = {"text": "Una sola línea efectiva por producto y ciclo (V9).",
                    "source": "lifecycle", "as_of": None}
        observed = {"text": f"Corrección a «confirmado» "
                            f"({ev.get('corrected_confirmed_m2', '?')} m²) con "
                            f"una línea descendiente de "
                            f"{ev.get('descendant_m2', '?')} m² en estado "
                            f"{refs.get('descendant_state', '?')}.",
                    "source": "corrección administrativa (§4.6d)",
                    "as_of": c.opened_at}
        consequence = "Sin acción, el producto queda pedido dos veces."

    elif c.type == "AmendmentIntentConfirmation":
        title = f"Oportunidad de ampliación — {pname}"
        expected = {"text": f"Producción {refs.get('production_ref', '?')} "
                            f"programada para el "
                            f"{ev.get('scheduled_start_date', '?')} admite "
                            "más m² antes de iniciar.",
                    "source": "production_planning (contexto blando)",
                    "as_of": ev.get("scheduled_start_date")}
        observed = {"text": f"Necesidad adicional calculada: "
                            f"{ev.get('gap_m2', '?')} m² "
                            f"(propuesta: {ev.get('proposed_additional_m2', '?')} m²).",
                    "source": "cálculo §6.7/§6.4b", "as_of": eng.today.isoformat()}
        consequence = (f"Añadir {ev.get('proposed_additional_m2', '?')} m² "
                       "antes del inicio protege el próximo ciclo sin "
                       "esperar al siguiente pedido.")

    elif c.type == "FactoryResponseOverdue":
        authority = "elicio"
        authority_note = ("Lo gestiona Elicio — seguimiento con la fábrica "
                          "en curso; no requiere acción tuya.")
        actions = []
        title = f"Respuesta de fábrica pendiente — {pname}" if pname \
            else "Respuesta de fábrica pendiente"
        expected = {"text": f"Respuesta de la fábrica dentro de "
                            f"{ev.get('window_days', '?')} días desde el envío "
                            f"({ev.get('ask_date', '?')}).",
                    "source": "ventana §10.3", "as_of": ev.get("ask_date")}
        observed = {"text": f"{ev.get('days_waiting', '?')} días sin respuesta.",
                    "source": "lifecycle", "as_of": eng.today.isoformat()}
        consequence = ("La línea sigue sin confirmar: la cantidad pedida no "
                       "cuenta como llegada prevista hasta que la fábrica "
                       "responda.")

    elif c.type == "MissingEvidence":
        feed = refs.get("feed")
        feed_es = FEED_ES.get(feed, feed)
        title = f"Evidencia crítica desactualizada — {feed_es}"
        expected = {"text": f"Datos de {feed_es} con menos de "
                            f"{eng.config.freshness_stale_days} días al abrir "
                            "el ciclo o calcular sugerencias.",
                    "source": feed, "as_of": None}
        observed = {"text": (f"Última carga: {ev.get('last_as_of')}"
                             + (f" ({ev.get('age_days')} días)"
                                if ev.get("age_days") is not None else "")
                             if ev.get("last_as_of") else "Sin carga registrada."),
                    "source": feed, "as_of": ev.get("last_as_of")}
        consequence = (f"{ev.get('affected_rows', '?')} filas calculan con "
                       "confianza reducida; cada sugerencia muestra el aviso. "
                       "Nunca bloquea la decisión.")

    if title is None:                       # defensive fallback, never a dump
        title = f"{c.type} — {c.subtype or 'caso'}" + (f" — {pname}" if pname else "")
        expected = expected or {"text": "Revisión requerida.",
                                "source": None, "as_of": None}
        observed = observed or {"text": "Ver historial del caso.",
                                "source": None, "as_of": c.opened_at}
        consequence = consequence or "Requiere una decisión para continuar."

    return {
        "title": title,
        "expected": expected,
        "observed": observed,
        "consequence": consequence,
        "authority": authority,
        "authority_note": authority_note,
        "actions": actions,
        "product": product,
        "raw_reference": raw_reference,
        "candidates": candidates,
    }


def compose_workspace(eng) -> dict:
    """Compose the full §7.1 read model from the engine's current truth."""
    cfg = eng.config
    today = eng.today
    # recompute (never trust caches) — keeps the model rebuildable from truth
    eng.match_result = eng._compute_match(eng.state)

    order = None
    if eng.state.orders:
        order = max(eng.state.orders.values(), key=lambda o: o.cycle_month)

    window = None
    if order:
        y, m = (int(x) for x in order.cycle_month.split("-"))
        start, end = date(y, m, 20), date(y, m, 25)
        window = {"window_start": start.isoformat(), "window_end": end.isoformat(),
                  "today": today.isoformat(),
                  "days_remaining": max(0, (end - today).days)}

    offers = [{
        "offer_id": o.offer_id, "product_id": o.product_id,
        "original_cycle": o.original_cycle, "original_m2": _s(o.original_m2),
        "note": o.note,
    } for o in eng.state.offers.values() if o.state == "open"]

    readiness = []
    for feed in ("warehouse", "sales", "in_transit", "produced", "siesa",
                 "committed_orders"):
        info = eng.feeds.get(feed)
        if info is None and feed in ("produced", "siesa", "committed_orders"):
            continue                       # optional feeds omitted when absent
        as_of = info["as_of"] if info else None
        readiness.append({
            "source": feed, "as_of": as_of.isoformat() if as_of else None,
            "age_days": (today - as_of).days if as_of else None,
            "status": _feed_status(as_of, today, cfg),
            **({"confidence": info["confidence"]}
               if info and "confidence" in info else {}),
        })

    current_lines = {l.product_id: l for l in eng.state.lines.values()
                     if order and l.order_id == order.order_id}
    total_selected = sum((l.selected_m2 for l in current_lines.values()), ZERO)
    total_pallets = total_selected / D("134.4")
    grouped = {
        "total_selected_m2": _s(total_selected),
        "derived_pallets": str(total_pallets.quantize(D("0.01"))),
        "containers": str((total_pallets / PALLETS_PER_CONTAINER).quantize(D("0.01"))),
        "shipment_viability": {
            "min_boat_pallets": MIN_BOAT_PALLETS,
            "viable": total_pallets >= MIN_BOAT_PALLETS,
            "informational_only": True,    # never a gate on the monthly order (§6.6)
        },
    }

    # R2: hand out defensive copies only — a caller mutating the response
    # can never reach the event-derived domain state
    from .domain import thaw
    needs_open = [{"case_id": c.case_id, "type": c.type, "subtype": c.subtype,
                   "refs": thaw(c.refs), "evidence": thaw(c.evidence),
                   "options": thaw(c.options),
                   "presentation": _present_case(eng, c)}
                  for c in eng.open_cases()]

    # row set: all active products + any product with open lifecycle objects
    row_pids = list(eng.products.keys())
    for e in eng.state.expectations.values():
        if e.product_id not in row_pids:
            row_pids.append(e.product_id)

    rows = [_compose_row(eng, pid, order, current_lines.get(pid), today)
            for pid in row_pids]

    return {
        "demo_mode": {
            "synthetic": True,
            "label": "DEMO — datos sintéticos, sin conexión a producción",
        },
        "monthly_order": ({
            "order_id": order.order_id,
            "cycle_month": order.cycle_month, "state": order.state,
            "opened_at": order.opened_at, "submitted_at": order.submitted_at,
        } if order else None),
        "order_window": window,
        "carry_forward_offers": offers,
        "evidence_readiness": readiness,
        "grouped_context": grouped,
        "needs_ashley_open": needs_open,
        "rows": rows,
    }


def _latest_decision(eng, order, pid):
    if not order:
        return None
    ds = [d for d in eng.state.decisions
          if d.order_id == order.order_id and d.product_id == pid]
    return ds[-1] if ds else None


def _compose_row(eng, pid, order, line, today) -> dict:
    cfg = eng.config
    product = eng.products.get(pid, {"id": pid, "sku": pid, "name": pid})

    sv = suggestion_view(eng, pid)                 # single shared computation
    velocity = sv["velocity"]
    buffer_m2 = sv["buffer_m2"]
    supply_now = sv["supply_now"]
    supply_expected = sv["supply_expected"]
    supply_moving = sv["supply_moving"]
    pending = sv["pending"]
    dated = sv["dated"]
    replen = sv["replen"]
    need = sv["need"]
    suggested = sv["suggested"]
    explanation = sv["explanation"]
    wh_as_of, sales_as_of, transit_as_of = (
        sv["wh_as_of"], sv["sales_as_of"], sv["transit_as_of"])
    wh_feed = eng.feeds.get("warehouse")
    sales_feed = eng.feeds.get("sales")
    transit_feed = eng.feeds.get("in_transit")

    # §6.7 projection for implications (selected/suggested order included)
    events = list(dated)
    planned_qty = None
    if line is not None and line.state in ("draft", "awaiting_factory"):
        planned_qty = line.selected_m2
    elif suggested > 0:
        planned_qty = suggested
    if planned_qty and planned_qty > 0:
        events.append((pm.expected_arrival_for_proposed_order(
            today, eng._voyage_days()), planned_qty))
    proj = pm.project_buffer_protection(
        supply_now=supply_now, daily_velocity=velocity,
        buffer_m2=buffer_m2, events=sorted(events), today=today,
        horizon_end=replen) if velocity > 0 else None

    implications = []
    if proj is not None and proj.outcome == "dip_below_buffer":
        recover = (f", recuperándose hacia {proj.dip_recovery.isoformat()}"
                   if proj.dip_recovery else "")
        # dip_start can be None when the only dip is the §6.7 minimum
        # immediately BEFORE an arrival (stock on every integer day stays
        # above the buffer): fall back to the first below-buffer minimum
        # date — truthful, and never a 500 on the workspace GET
        dip_date = proj.dip_start or next(
            (d for d, m in proj.minima if m < buffer_m2), today)
        implications.append({
            "type": "decision_consequence", "severity": "watch",
            "text": (f"Aun con este pedido, el stock baja de la reserva "
                     f"alrededor de {dip_date.isoformat()}{recover}.")})
    if proj is not None and proj.outcome == "projected_stockout":
        implications.append({
            "type": "hidden_risk", "severity": "risk",
            "text": (f"Sin reposición, quiebre de stock proyectado hacia "
                     f"{proj.stockout_date.isoformat()}.")})

    offer = next((o for o in eng.state.offers.values()
                  if o.product_id == pid and o.state == "open"), None)
    if offer is not None:
        implications.append({
            "type": "constraint_tradeoff", "severity": "info",
            "text": (f"Oferta pendiente de «no producción» de {offer.original_cycle}: "
                     f"{_s(offer.original_m2)} m²; la necesidad actual calcula "
                     f"{_s(need.true_need_m2)} m².")})

    # C2/V6: persisted decision-consequence implications (append-only record
    # of the selected-versus-confirmed consequence, with provenance)
    from .domain import thaw as _thaw
    for imp in eng.state.implications.values():
        if imp.product_id == pid:
            implications.append({
                "type": imp.type, "severity": imp.severity, "text": imp.text,
                "persisted": True, "subtype": imp.subtype,
                "evidence": _thaw(imp.evidence), "refs": _thaw(imp.refs),
                "recorded_at": imp.recorded_at})

    open_cases_for_pid = [c for c in eng.open_cases()
                          if c.refs.get("product_id") == pid]

    decision = _latest_decision(eng, order, pid)
    decision_state = {"accept": "accepted", "edit": "edited", "defer": "deferred",
                      "manual_add": "manual"}.get(decision.action, "none") \
        if decision else "none"

    # C1/V7: the FROZEN copy of what Ashley saw when she decided — survives
    # every later feed change and recomputation ("what Ashley saw and why")
    decision_snapshot = None
    if decision and decision.suggestion_snapshot_ref in eng.state.suggestions:
        from .domain import thaw
        fs = eng.state.suggestions[decision.suggestion_snapshot_ref]
        decision_snapshot = {
            "suggestion_snapshot_ref": fs.snapshot_id,
            "suggested_m2": _s(fs.suggested_m2),
            "true_need_m2": _s(fs.true_need_m2),
            "derived_pallets": str(fs.derived_pallets),
            "computed_at": fs.computed_at,
            "explanation": thaw(fs.explanation),          # defensive copy (R2)
            "input_snapshot_refs": thaw(fs.input_snapshot_refs),
            "decision_action": decision.action,
            "decided_at": decision.decided_at,
        }

    watch = pm.zero_velocity_watch(daily_velocity=velocity,
                                   warehouse_m2=supply_now,
                                   buffer_m2=buffer_m2)
    if open_cases_for_pid:
        signal = "exception"
    elif watch or (proj is not None and proj.outcome == "dip_below_buffer"):
        signal = "watch"
    elif suggested > 0:
        signal = "active_recommendation"
    elif need.true_need_m2 > 0:
        signal = "planning_needed"
    else:
        signal = "healthy"

    siesa = eng.feeds.get("siesa")
    siesa_value = (_s(siesa["values"].get(pid))
                   if siesa and pid in siesa["values"] else None)

    commitments = None
    if eng.committed_orders:
        mine = [r for r in eng.committed_orders["rows"] if r["product_id"] == pid]
        commitments = [{"m2": _s(r["m2"]), "due_date": r["due_date"].isoformat(),
                        "source": "committed_orders",
                        "as_of": eng.committed_orders["as_of"].isoformat()}
                       for r in mine]

    production_context = [
        {"orden_produccion": r.get("orden_produccion"),
         "status": r.get("status"),
         "requested_m2": _s(r.get("requested_m2") or 0),
         "completed_m2": _s(r.get("completed_m2") or 0),
         "scheduled_start_date": (r["scheduled_start_date"].isoformat()
                                  if r.get("scheduled_start_date") else None),
         "estimated_delivery_date": (r["estimated_delivery_date"].isoformat()
                                     if r.get("estimated_delivery_date") else None),
         "can_add_more": bool(r.get("can_add_more")),
         "soft_context": True}            # visually + structurally soft (§7.2)
        for r in eng.production_planning if r.get("product_id") == pid]

    freshness = {
        "warehouse": {"as_of": wh_as_of,
                      "status": _feed_status(wh_feed["as_of"] if wh_feed else None,
                                             today, cfg)},
        "sales": {"as_of": sales_as_of,
                  "status": _feed_status(sales_feed["as_of"] if sales_feed else None,
                                         today, cfg)},
        "in_transit": {"as_of": transit_as_of,
                       "status": _feed_status(transit_feed["as_of"]
                                              if transit_feed else None, today, cfg)},
    }

    provenance = {
        "warehouse_m2": {"source": "warehouse_snapshot", "as_of": wh_as_of},
        "sales_velocity": {"source": f"sac_sales_{VELOCITY_PERIOD_DAYS}d",
                           "as_of": sales_as_of},
        "in_transit_m2": {"source": "in_transit_snapshot (unmatched only, §6.4a)",
                          "as_of": transit_as_of},
        "expected_incoming_m2": {
            "source": "lifecycle: Σ remaining_m2 of open ExpectedIncoming",
            "as_of": today.isoformat(),
            "refs": [e.exp_id for e in eng.state.expectations.values()
                     if e.product_id == pid and e.open]},
        "pending_commitment_m2": {
            "source": "lifecycle: awaiting_factory lines + open amendments",
            "as_of": today.isoformat()},
        "buffer_m2": {"source": f"derived: tier {eng.tiers.get(pid, 'C')} "
                                "buffer rule (§6.2)",
                      "as_of": today.isoformat()},
        "true_need_m2": {"source": "derived: §6.4b true-need contract",
                         "as_of": today.isoformat(), "trace": "explanation.trace"},
        "suggested_m2": {"source": "derived: §6.5 half-pallet rounding [D1]",
                         "as_of": today.isoformat(), "trace": "explanation.trace"},
    }

    return {
        "product": {"id": pid, "sku": product.get("sku", pid),
                    "name": product.get("name", pid),
                    "tier": eng.tiers.get(pid, "C")},
        "warehouse_m2": _s(supply_now),
        "sales_velocity": {"daily_m2": _s(velocity),
                           "basis_days": VELOCITY_PERIOD_DAYS,
                           "peak_weekly_m2": (_s(eng.peaks[pid] * 7)
                                              if pid in eng.peaks else None)},
        "buffer_m2": _s(buffer_m2),
        "buffer_pallets": str((buffer_m2 / D("134.4")).quantize(D("0.01"))),
        "in_transit_m2": _s(supply_moving),
        "siesa_available_m2": siesa_value,
        "production_context": production_context,
        "expected_incoming_m2": _s(supply_expected),
        "pending_commitment_m2": _s(pending),
        "customer_commitments": commitments,
        "timing": {"manufacturing_days": 25,
                   "transit_days": 5 + eng._voyage_days() + 6,
                   "next_replenishment_date": replen.isoformat()},
        "true_need_m2": _s(need.true_need_m2),
        "residual_need_m2": _s(need.residual_need_m2),
        "suggested_m2": _s(suggested),
        "suggested_pallets": str((suggested / D("134.4")).quantize(D("0.01"))),
        "selected_m2": _s(line.selected_m2) if line else None,
        "decision_state": decision_state,
        "decision_snapshot": decision_snapshot,
        "line_state": line.state if line else None,
        "confirmed_m2": (_s(line.confirmed_m2)
                         if line and line.confirmed_m2 is not None else None),
        "carry_forward_offer": ({"offer_id": offer.offer_id,
                                 "original_cycle": offer.original_cycle,
                                 "original_m2": _s(offer.original_m2),
                                 "note": offer.note} if offer else None),
        "implications": implications,
        "explanation": explanation,
        "freshness_confidence": freshness,
        "row_signal": signal,
        "provenance": provenance,
    }
