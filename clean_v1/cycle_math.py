"""Pure, read-only projection math for the two-stage replenishment cycle."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING
from typing import Iterable

from .planning_math import project_buffer_protection, q2


def _date(value):
    if value is None or isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _two(value) -> str:
    return f"{q2(Decimal(str(value))):.2f}"


def product_buffer_breach(*, as_of: date, warehouse_m2: Decimal,
                          dated_destination_events: list[tuple[date, Decimal]],
                          buffer_m2: Decimal, daily_velocity_m2: Decimal | None,
                          horizon_end: date) -> dict:
    """Adapt the accepted dated projection without making future supply current."""
    velocity = None if daily_velocity_m2 is None else Decimal(daily_velocity_m2)
    basis = {
        "warehouse_m2": _two(warehouse_m2),
        "dated_destination_events": [
            {"arrival_date": event_date, "m2": _two(amount), "authority": "expected"}
            for event_date, amount in dated_destination_events],
        "buffer_m2": _two(buffer_m2),
        "daily_velocity_m2": _two(velocity or 0),
        "breach_date": None,
        "protected_through": None,
        "dip_recovery": None,
        "dip_reentry": None,
        "stockout_date": None,
        "reason": None,
    }
    if velocity is None or velocity <= 0:
        basis["reason"] = "velocity_zero_or_missing"
        return basis

    result = project_buffer_protection(
        supply_now=Decimal(warehouse_m2), daily_velocity=velocity,
        buffer_m2=Decimal(buffer_m2), events=dated_destination_events,
        today=as_of, horizon_end=horizon_end)
    minima_breaches = [d for d, amount in result.minima if amount <= Decimal(buffer_m2)]
    candidates = minima_breaches + ([result.dip_start] if result.dip_start else [])
    basis["breach_date"] = min(candidates) if candidates else None
    basis["protected_through"] = horizon_end if basis["breach_date"] is None else None
    basis["dip_recovery"] = result.dip_recovery
    basis["dip_reentry"] = result.dip_reentry
    basis["stockout_date"] = result.stockout_date

    # The accepted function evaluates minima at event-minus and daily stock after
    # all same-day arrivals. Surface a same-day recovery that those two views prove.
    if basis["breach_date"] in {d for d, _ in dated_destination_events}:
        d = basis["breach_date"]
        before = next((amount for day, amount in result.minima if day == d), None)
        arrivals = sum((amount for day, amount in dated_destination_events if day == d), Decimal("0"))
        if before is not None and before <= Decimal(buffer_m2) and before + arrivals > Decimal(buffer_m2):
            basis["dip_recovery"] = basis["dip_recovery"] or d
            if result.dip_start and result.dip_start > d:
                basis["dip_reentry"] = basis["dip_reentry"] or result.dip_start
    return basis


def controlling_buffer_breach(rows: list[dict]) -> dict:
    breaches = [(_date(row.get("breach_date")), row.get("product_id")) for row in rows]
    breaches = [(d, pid) for d, pid in breaches if d is not None]
    if breaches:
        earliest = min(d for d, _ in breaches)
        return {"buffer_breach_date": earliest,
                "controlling_product_ids": sorted(pid for d, pid in breaches if d == earliest),
                "protected_through": None, "boundary_state": "breach"}
    if any(row.get("reason") == "velocity_zero_or_missing" for row in rows):
        return {"buffer_breach_date": None, "controlling_product_ids": [],
                "protected_through": None, "boundary_state": "indeterminate"}
    determinate = [row for row in rows if row.get("reason") != "velocity_zero_or_missing"]
    if determinate:
        through = [_date(row.get("protected_through")) for row in determinate]
        through = [d for d in through if d is not None]
        return {"buffer_breach_date": None, "controlling_product_ids": [],
                "protected_through": min(through) if through else None,
                "boundary_state": "protected"}
    return {"buffer_breach_date": None, "controlling_product_ids": [],
            "protected_through": None, "boundary_state": "indeterminate"}


def _history_rows(rows: Iterable[dict]):
    for group in rows:
        if "orders" in group:
            for order in group.get("orders", []):
                yield {**order, "lifecycle_inert": group.get("lifecycle_inert", False)}
        else:
            yield group


def valid_lead_samples(rows: list[dict]) -> dict:
    """Accept only nonnegative order→actual-ready lifecycle observations."""
    lead_days: list[int] = []
    warnings: list[str] = []
    seen: set[str] = set()
    for row in _history_rows(rows):
        identity = next((row.get(key) for key in (
            "external_order_id", "pv_number", "production_ref", "order_id", "reference")
            if row.get(key)), None)
        if identity is None:
            warnings.append("missing_order_identity")
            continue
        identity = str(identity)
        if identity in seen:
            warnings.append("duplicate_order_identity")
            continue
        seen.add(identity)
        ordered = _date(row.get("order_date"))
        actual = _date(row.get("actual_ready_date"))
        estimate = _date(row.get("estimated_ready_date"))
        if estimate is not None and ordered is not None and estimate < ordered:
            warnings.append("invalid_factory_estimate_before_order")
        if actual is None:
            warnings.append("missing_actual_ready_date")
            continue
        if ordered is None:
            warnings.append("missing_or_invalid_order_date")
            continue
        if row.get("lifecycle_inert") is not True:
            warnings.append("not_lifecycle_evidence")
            continue
        lead = (actual - ordered).days
        if lead < 0:
            warnings.append("invalid_negative_lead")
            continue
        lead_days.append(lead)
    return {"lead_days": lead_days, "valid_sample_count": len(lead_days),
            "warnings": list(dict.fromkeys(warnings))}


def readiness_window(*, as_of: date, lead_days: list[int]) -> dict:
    """Use min/ceil-middle-median/max; fewer than three samples stay open."""
    ordered = sorted(int(day) for day in lead_days)
    count = len(ordered)
    result = {
        "earliest": None, "expected": None, "latest": None,
        "open_ended": count < 3,
        "confidence": "insufficient" if count < 3 else "low" if count < 8 else "medium",
        "valid_sample_count": count,
        "min_lead_days": None, "median_lead_days": None, "max_lead_days": None,
    }
    if count < 3:
        return result
    middle = count // 2
    median = ordered[middle] if count % 2 else int(
        (Decimal(ordered[middle - 1] + ordered[middle]) / Decimal(2)).to_integral_value(
            rounding=ROUND_CEILING))
    result.update({
        "earliest": as_of + timedelta(days=ordered[0]),
        "expected": as_of + timedelta(days=median),
        "latest": as_of + timedelta(days=ordered[-1]),
        "open_ended": False,
        "min_lead_days": ordered[0], "median_lead_days": median,
        "max_lead_days": ordered[-1],
    })
    return result


def proposed_order_status(*, as_of: date, production_ready_by: date | None,
                          max_lead_days: int | None,
                          prepare_horizon_days: int = 7) -> dict:
    if production_ready_by is None or max_lead_days is None:
        return {"status": "evidence_insufficient", "factory_order_by": None}
    due = production_ready_by - timedelta(days=max_lead_days)
    if as_of < due - timedelta(days=prepare_horizon_days):
        status = "monitor"
    elif as_of < due:
        status = "prepare"
    elif as_of == due:
        status = "order_now"
    else:
        status = "at_risk"
    return {"status": status, "factory_order_by": due}


def _shipment(value: dict | None) -> dict:
    value = value or {}
    return {
        "sailing_id": value.get("sailing_id"), "name": value.get("name"),
        "order_by": _date(value.get("order_by")),
        "departure": _date(value.get("departure")),
        "warehouse_arrival": _date(value.get("warehouse_arrival")),
        "authority": "selected_current_siesa_shipment",
    }


def _copy(status: str) -> tuple[str, str]:
    return {
        "evidence_insufficient": ("Evidencia insuficiente", "Faltan fechas reales de alistamiento para calcular una ventana confiable."),
        "monitor": ("Monitorear", "La protección de bodega permite observar antes de preparar el pedido."),
        "prepare": ("Preparar pedido", "Conviene preparar la decisión de fábrica dentro de los próximos siete días."),
        "order_now": ("Pedir ahora", "Hoy es la fecha conservadora para llegar al zarpe objetivo."),
        "at_risk": ("En riesgo", "La protección de bodega no cuenta con un zarpe posterior viable."),
        "in_production": ("En producción", "La producción registrada está en curso; todavía no cuenta como suministro."),
        "awaiting_siesa": ("Producción terminada", "La producción terminó y está pendiente de aparecer en SIESA; todavía no se puede pedir para transporte."),
        "ready": ("Disponible en SIESA", "La producción terminada ya aparece disponible en SIESA y puede entrar al pedido de transporte hasta la cantidad visible."),
    }[status]


def compose_replenishment_cycle(*, as_of: date, protection_rows: list[dict],
                                current_shipment: dict | None,
                                candidate_sailings: list[dict],
                                factory_history: list[dict],
                                current_production: dict | None,
                                prepare_horizon_days: int = 7) -> dict:
    boundary = controlling_buffer_breach(protection_rows)
    breach = boundary["buffer_breach_date"]
    current_departure = _date((current_shipment or {}).get("departure"))
    viable = []
    if breach is not None:
        for sailing in candidate_sailings:
            departure = _date(sailing.get("departure"))
            arrival = _date(sailing.get("warehouse_arrival"))
            ready_by = departure - timedelta(days=25) if departure else None
            if (sailing.get("timing_state") != "departed"
                    and sailing.get("selectable", True)
                    and departure is not None and arrival is not None
                    and (current_departure is None or departure > current_departure)
                    and arrival <= breach and ready_by >= as_of):
                viable.append((arrival, departure, sailing, ready_by))
    target = max(viable, key=lambda row: (row[0], row[1])) if viable else None

    lead = valid_lead_samples(factory_history)
    production = current_production or {}
    anchor = _date(production.get("order_date")) or as_of
    window = readiness_window(as_of=anchor, lead_days=lead["lead_days"])
    warnings = list(lead["warnings"])
    if window["open_ended"]:
        warnings.append("insufficient_lead_evidence")

    target_block = None
    status_result = {"status": "evidence_insufficient", "factory_order_by": None}
    if target is not None:
        arrival, departure, sailing, ready_by = target
        target_block = {
            "sailing_id": sailing["sailing_id"], "name": sailing["name"],
            "production_ready_by": ready_by, "departure": departure,
            "warehouse_arrival": arrival, "authority": "tentative",
        }
        status_result = proposed_order_status(
            as_of=as_of, production_ready_by=ready_by,
            max_lead_days=window["max_lead_days"],
            prepare_horizon_days=prepare_horizon_days)

    material_risk = False
    if breach is not None and target is None:
        status = "at_risk"
        material_risk = True
    elif window["open_ended"]:
        status = "evidence_insufficient"
    elif boundary["boundary_state"] == "protected":
        status = "monitor"
    elif boundary["boundary_state"] == "indeterminate":
        status = "evidence_insufficient"
    else:
        status = status_result["status"]
        material_risk = status == "at_risk"

    if production.get("accepted_ready") or (
            production.get("accepted_actual_ready") and production.get("production_status") in ("ready", "completed")):
        status = "ready" if production.get("siesa_orderable") else "awaiting_siesa"
    elif production.get("accepted_in_progress"):
        status = "in_production"

    estimate = _date(production.get("factory_estimate"))
    ordered = _date(production.get("order_date"))
    if estimate is not None and ordered is not None and estimate < ordered:
        warnings.append("invalid_factory_estimate_before_order")
        estimate = None
    headline, explanation = _copy(status)
    if status == "at_risk" and target_block is not None:
        explanation = "La fecha conservadora para pedir a fábrica ya pasó para el zarpe objetivo tentativo."
    if production.get("can_add_more"):
        explanation += " Hay una oportunidad de enmienda; no se abre otra orden automáticamente."

    return {
        "as_of": as_of, "status": status, "material_risk": material_risk,
        "headline": headline, "explanation": explanation,
        "warehouse_protection": {
            "buffer_breach_date": breach,
            "controlling_product_ids": boundary["controlling_product_ids"],
            "protected_through": boundary["protected_through"],
            "basis": protection_rows,
        },
        "current_shipment": _shipment(current_shipment),
        "next_production": {
            "production_order_id": production.get("production_order_id"),
            "production_ref": production.get("production_ref"),
            "target_sailing": target_block,
            "factory_order_by": status_result["factory_order_by"] if not window["open_ended"] else None,
            "readiness_window": window,
            "factory_estimate": estimate,
            "production_status": production.get("production_status"),
            "siesa_orderable": bool(production.get("siesa_orderable", False)),
            "can_add_more": bool(production.get("can_add_more", False)),
            "evidence_as_of": _date(production.get("evidence_as_of")),
            "evidence_warnings": list(dict.fromkeys(warnings)),
        },
    }
