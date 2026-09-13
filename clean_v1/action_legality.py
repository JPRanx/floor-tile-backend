"""Actor-aware legal-action composer.

Every candidate is executed on a disposable deep copy at the same event head,
day and effective actor.  Only successful probes are emitted.
"""
from __future__ import annotations

import copy
from decimal import Decimal

from .command_schema import COMMAND_SCHEMAS, command_witness
from . import sailing_math as sm


def _actor(principal) -> str:
    return getattr(principal, "effective_actor", getattr(principal, "actor", ""))


def _transport_for_probe(engine, action: dict) -> dict:
    return command_witness(action["command"], action["params"], engine)


def probe_action(engine, principal, action: dict) -> bool:
    """Prove an emitted candidate without mutating or publishing the head."""
    disposable = copy.deepcopy(engine)
    if len(disposable.state.events) != len(engine.state.events):
        return False
    actor = _actor(principal)
    token = disposable.identity.token_for(actor)
    if token is None:
        return False
    try:
        params = _transport_for_probe(disposable, action)
        # Finalization is a legal draft action exposed alongside editing.  Its
        # probe supplies the smallest valid prospective edit without touching
        # live state; direct zero-delta execution remains atomically illegal.
        if action["command"] == "FinalizeOrderAmendment":
            amendment = disposable.state.order_amendments[params["amendment_id"]]
            first = next(iter(amendment.lines.values()))
            first["selected_m2"] = Decimal(str(first["baseline_m2"])) + Decimal("67.20")
        disposable.execute(action["command"], params, token=token)
    except Exception:
        return False
    return True


def _entry(engine, principal, command: str, params: dict,
           label: str, explanation: str) -> dict | None:
    schema = COMMAND_SCHEMAS[command]
    action = {"command": command, "params": copy.deepcopy(params),
              "label": label, "explanation": explanation}
    has_input = bool(schema.operator_fields) or command == "ResolveImplication"
    if has_input:
        action["input_schema"] = schema.input_schema_for(params, engine)
    if command == "SetSailingDecision" and "input_schema" in action:
        decision_schema = action["input_schema"]["properties"]["decision"]
        legal = []
        actor = _actor(principal)
        for decision in decision_schema["enum"]:
            disposable = copy.deepcopy(engine)
            token = disposable.identity.token_for(actor)
            try:
                disposable.execute(command, {**params, "decision": decision},
                                   token=token)
            except Exception:
                continue
            legal.append(decision)
        decision_schema["enum"] = legal
        if not legal:
            return None
    if not probe_action(engine, principal, action):
        return None
    return action


def compose_legal_actions(engine, principal, plan_id=None,
                          production_candidate=None) -> dict[str, list[dict]]:
    """Return legal actions keyed by stable server surface identity."""
    out: dict[str, list[dict]] = {}
    actor = _actor(principal)
    if actor not in ("ashley", "elicio"):
        return out
    s = engine.state

    def add(key, command, params, label=None, explanation=None):
        item = _entry(engine, principal, command, params,
                      label or command,
                      explanation or "Disponible y validado en el estado actual")
        if item is not None:
            out.setdefault(key, []).append(item)

    # Workspace controls in this matrix are Ashley controls except owner-routed
    # implication resolution, which is proved for the effective owner actor.
    if actor == "ashley":
        for sid, sailing in s.sailings.items():
            key = f"rail:{sid}"
            add(key, "SetSailingDecision", {"sailing_id": sid},
                "Cambiar decisión", "Decisión para esta salida registrada")
            if engine.sailing_timing(sid, s).timing_state == "exceptional":
                add(key, "PursueExceptionalSailing", {"sailing_id": sid},
                    "Perseguir salida excepcional")
            add(key, "OpenShipmentPlan", {"sailing_id": sid},
                "Abrir plan de embarque")

    for iid, imp in s.implications.items():
        if imp.state != "open":
            continue
        owner = (imp.scope or {}).get("owner", "ashley")
        required_actor = "ashley" if owner == "ashley" else "elicio"
        if actor != required_actor:
            continue
        for typed_action in imp.typed_actions:
            if owner == "ashley" and typed_action == "accept_risk":
                add(f"attention:{iid}", "AcceptImplicationRisk",
                    {"implication_id": iid}, "Aceptar riesgo")
            else:
                action_label = {
                    "abandon_plan": "Abandonar plan",
                    "accept_evidence": "Aceptar evidencia y planificar faltante",
                    "accept_risk": "Aceptar el riesgo",
                    "cancel_pursuit": "Cancelar excepción",
                    "chase_booking": "Gestionar reserva confirmada",
                    "confirm_reopen": "Confirmar reapertura",
                    "discard": "Descartar fila",
                    "exclude_pending": "Excluir lo ya asignado",
                    "keep": "Mantener",
                    "keep_expectation": "Mantener lo esperado",
                    "keep_plan": "Mantener plan",
                    "load_feed": "Actualizar fuente",
                    "map": "Asignar a un producto existente",
                    "note_for_monthly": "Anotar para el ciclo mensual",
                    "plan_on_sailing": "Planificar en este zarpe",
                    "proceed_with_note": "Continuar con nota",
                    "pursue_exceptional": "Considerar zarpe excepcional",
                    "record_reference": "Registrar referencia SIESA",
                    "verify_entry": "Verificar registro SIESA",
                }.get(typed_action)
                if action_label is None:
                    continue
                add(f"attention:{iid}", "ResolveImplication",
                    {"implication_id": iid, "action": typed_action, "params": {}},
                    action_label)

    plan = s.plans.get(plan_id) if plan_id else None
    if plan is None and plan_id is None:
        plan = next((p for p in s.plans.values()
                     if p.lifecycle in ("draft", "finalized")), None)
    if actor != "ashley" or plan is None:
        return out
    pid = plan.plan_id
    pkey = f"plan:{pid}"

    if production_candidate is not None and not any(
            order.source_plan_id == pid and order.lifecycle in (
                "draft", "finalized", "reference_recorded")
            for order in s.production_orders.values()):
        add(f"production-plan:{pid}", "OpenProductionOrder", {
            "plan_id": pid,
            "production_anchor_sailing_id": production_candidate[
                "production_anchor_sailing_id"],
            "factory_order_date": production_candidate["factory_order_date"],
            "candidate_fingerprint": production_candidate["candidate_fingerprint"],
        }, "Abrir orden de producción",
            "Crea explícitamente un borrador desde recomendaciones positivas")
    for order_id, production_order in s.production_orders.items():
        key = f"production-order:{order_id}"
        if production_order.lifecycle == "draft":
            add(key, "EditProductionOrderBatch", {"production_order_id": order_id},
                "Guardar edición de producción")
            if production_order.lines:
                add(key, "FinalizeProductionOrder", {"production_order_id": order_id},
                    "Finalizar orden de producción")
        elif production_order.lifecycle == "finalized":
            add(key, "RecordProductionOrderReference",
                {"production_order_id": order_id},
                "Registrar referencia de producción")
            if production_order.production_ref is None:
                add(key, "ReopenProductionOrder", {"production_order_id": order_id},
                    "Reabrir orden de producción")
        elif production_order.lifecycle == "reference_recorded":
            add(key, "OpenOrderAmendment",
                {"order_kind": "production", "order_id": order_id},
                "Abrir modificación de producción")

    add(pkey, "ReopenSailingPlan", {"plan_id": pid}, "Reabrir plan de embarque")
    add(pkey, "OpenOrderAmendment",
        {"order_kind": "shipment", "order_id": pid},
        "Abrir modificación de embarque")
    for amendment_id, amendment in s.order_amendments.items():
        key = f"amendment:{amendment_id}"
        if amendment.lifecycle == "draft":
            add(key, "EditOrderAmendmentBatch", {"amendment_id": amendment_id},
                "Guardar modificación")
            changed = any(
                Decimal(str(line["selected_m2"])) != Decimal(str(line["baseline_m2"]))
                for line in amendment.lines.values())
            if changed:
                add(key, "FinalizeOrderAmendment", {"amendment_id": amendment_id},
                    "Finalizar modificación")
        elif amendment.lifecycle == "finalized":
            add(key, "RecordOrderAmendmentReference", {"amendment_id": amendment_id},
                "Registrar referencia de modificación")

    for product_id in engine.catalog(s):
        rkey = f"row:{pid}:{product_id}"
        add(rkey, "AcceptSuggestion", {"plan_id": pid, "product_id": product_id},
            "Aceptar sugerencia")
        add(rkey, "EditSelectedM2", {"plan_id": pid, "product_id": product_id},
            "Editar m²")
        add(rkey, "PostponeProduct", {"plan_id": pid, "product_id": product_id},
            "Posponer producto")
        add(rkey, "AddManualLine", {"plan_id": pid, "product_id": product_id},
            "Agregar línea manual")
    pending_suggestion = False
    for product_id in engine.catalog(s):
        suggestion = engine.suggestion_for(pid, product_id, s)
        line = plan.lines.get(product_id)
        if (not suggestion["no_basis"] and suggestion["suggested_m2"] > 0
                and (line is None or line.get("origin") != "suggestion"
                     or Decimal(str(line["selected_m2"]))
                     != suggestion["suggested_m2"])):
            pending_suggestion = True
            break
    if pending_suggestion:
        add(pkey, "AcceptAllSuggestions", {"plan_id": pid}, "Aceptar sugerencias")
    add(pkey, "FinalizeSailingPlan", {"plan_id": pid}, "Finalizar plan")
    add(pkey, "EditSelectedM2Batch", {"plan_id": pid},
        "Guardar edición por lote",
        "Valida todos los productos y publica el lote completo o ninguno")
    add(pkey, "RequestReallocation", {"plan_id": pid}, "Solicitar reasignación")
    add(pkey, "CloseSailingPlan", {"plan_id": pid}, "Cerrar plan")
    add(f"reallocation:{pid}", "ConfirmReallocationIntent", {"plan_id": pid},
        "Confirmar reapertura")

    if plan.lifecycle == "draft" and plan.lines and plan.plan_id in s.bl_splits:
        add(f"bl:{pid}", "OverrideBLSplit", {"plan_id": pid},
            "Ajustar distribución BL", "Conservación validada por servidor")

    if plan.handoff_id and plan.handoff_id in s.handoffs:
        handoff = s.handoffs[plan.handoff_id]
        if not handoff["superseded"]:
            for order in handoff["handoff"].get("orders", []):
                ref = order["order_ref"]
                add(f"handoff:{ref}", "RecordSiesaOrderReference",
                    {"handoff_order_ref": ref}, "Registrar referencia SIESA")

    by_sailing: dict[str, list[str]] = {}
    booked_refs = {ref for b in s.bookings.values() for ref in b.commitment_refs}
    for cid, commitment in s.commitments.items():
        if commitment.state != "observed" or cid in booked_refs:
            continue
        for handoff in s.handoffs.values():
            if handoff["superseded"]:
                continue
            if any(o.get("order_ref") == commitment.handoff_order_ref
                   for o in handoff["handoff"].get("orders", [])):
                sailing_id = s.plans[handoff["plan_id"]].sailing_id
                by_sailing.setdefault(sailing_id, []).append(cid)
    for sailing_id in by_sailing:
        add(f"booking:{sailing_id}", "RecordBookingConfirmation",
            {"sailing_id": sailing_id},
            "Registrar reserva confirmada")
    for booking in s.bookings.values():
        if any(e.booking_ref == booking.booking_id and e.state == "booked"
               for e in s.expectations.values()):
            add(f"departure:{booking.booking_id}", "RecordBlDeparture",
                {"booking_ref": booking.booking_ref}, "Registrar salida confirmada")
    return out
