"""Deterministic database projections derived only from the domain event fold."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date
from decimal import Decimal
from uuid import UUID, uuid5

from psycopg.types.json import Jsonb

from ..domain import thaw


# The mutable set is the exact CREATE TABLE set in migration 005.  The final
# two tables were created by migration 003 and are immutable evidence indexes.
MUTABLE_PROJECTION_TABLES = (
    "sailings", "sailing_decisions", "pursuit_authorities",
    "shipment_plans", "plan_lines", "bl_splits", "siesa_handoffs",
    "handoff_orders", "handoff_order_lines", "siesa_commitments",
    "commitment_lines", "pending_handoff_exclusions", "bookings",
    "booking_commitments", "expected_incoming", "implications",
    "monthly_manufacturing_entries", "production_orders",
    "production_order_lines", "order_amendments", "order_amendment_lines",
)
IMMUTABLE_PROJECTION_TABLES = ("suggestion_snapshots", "plan_recomputes")
PROJECTION_TABLES = MUTABLE_PROJECTION_TABLES + IMMUTABLE_PROJECTION_TABLES

PRIMARY_KEYS = {
    "sailings": ("company_id", "sailing_id"),
    "sailing_decisions": ("company_id", "sailing_id"),
    "pursuit_authorities": ("company_id", "pursuit_id"),
    "shipment_plans": ("company_id", "plan_id"),
    "plan_lines": ("company_id", "plan_id", "product_id"),
    "bl_splits": ("company_id", "plan_id"),
    "siesa_handoffs": ("company_id", "handoff_id"),
    "handoff_orders": ("company_id", "handoff_order_id"),
    "handoff_order_lines": ("company_id", "handoff_order_id", "product_id"),
    "siesa_commitments": ("company_id", "commitment_id"),
    "commitment_lines": ("company_id", "commitment_id", "product_id"),
    "pending_handoff_exclusions": ("company_id", "exclusion_id"),
    "bookings": ("company_id", "booking_id"),
    "booking_commitments": ("company_id", "booking_id", "commitment_id"),
    "expected_incoming": ("company_id", "expectation_id"),
    "implications": ("company_id", "implication_id"),
    "monthly_manufacturing_entries": ("company_id", "entry_id"),
    "production_orders": ("company_id", "production_order_id"),
    "production_order_lines": ("company_id", "production_order_id", "product_id"),
    "order_amendments": ("company_id", "amendment_id"),
    "order_amendment_lines": ("company_id", "amendment_id", "product_id"),
    "suggestion_snapshots": ("company_id", "suggestion_id"),
    "plan_recomputes": ("company_id", "recompute_id"),
}


def _json_value(value):
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _decimal(value):
    return Decimal(str(value))


def _stable_uuid(company_id: str, family: str, source_id: str) -> str:
    return str(uuid5(UUID(company_id), f"{family}:{source_id}"))


def _event_at(state, event_type, key, value):
    for event in reversed(state.events):
        payload = thaw(event.payload)
        if event.type == event_type and payload.get(key) == value:
            return event.at
    raise ValueError(f"projection has no source event: {event_type}/{value}")


def _event_seq(state, event_type, key, value):
    for event in reversed(state.events):
        payload = thaw(event.payload)
        if event.type == event_type and (value is None or payload.get(key) == value):
            return event.seq
    raise ValueError(f"projection has no source event: {event_type}/{value}")


def _plan_lineage(state):
    by_sailing, answer = {}, {}
    for event in state.events:
        if event.type != "plan_opened":
            continue
        payload = thaw(event.payload)
        prior = by_sailing.get(payload["sailing_id"])
        generation = (answer[prior]["generation"] + 1) if prior else 1
        answer[payload["plan_id"]] = {"predecessor": prior, "generation": generation}
        by_sailing[payload["sailing_id"]] = payload["plan_id"]
    return answer


def _handoff_generations(state):
    counts, answer = {}, {}
    for event in state.events:
        if event.type != "plan_finalized":
            continue
        payload = thaw(event.payload)
        plan_id = payload["plan_id"]
        counts[plan_id] = counts.get(plan_id, 0) + 1
        answer[payload["handoff_id"]] = counts[plan_id]
    return answer


def _pursuit_rows(state, company_id, head):
    generations, rows = {}, []
    for event in state.events:
        if event.type != "exceptional_pursuit_recorded":
            continue
        payload = thaw(event.payload)
        sailing_id = payload["sailing_id"]
        generation = payload.get("generation") or generations.get(sailing_id, 0) + 1
        generations[sailing_id] = max(generations.get(sailing_id, 0), generation)
        current = state.pursuits.get(sailing_id)
        rows.append({
            "company_id": company_id,
            "pursuit_id": f"pursuit:{sailing_id}:{generation}",
            "sailing_id": sailing_id,
            "generation": generation,
            "active": bool(current and current.get("generation") == generation),
            "rebuilt_through_seq": head,
        })
    return rows


def projection_rows(state, company_id: str) -> dict:
    """Return exact database-shaped rows for every owned projection table."""
    head = len(state.events)
    rows = {"head_seq": head, **{table: [] for table in PROJECTION_TABLES}}

    rows["sailings"] = [{
        "company_id": company_id, "sailing_id": item.sailing_id,
        "carrier": item.carrier, "name": item.name, "departure": item.departure,
        "payload": {"voyage_days": item.voyage_days, "entered_via": item.entered_via},
        "rebuilt_through_seq": head,
    } for item in state.sailings.values()]
    rows["sailing_decisions"] = [{
        "company_id": company_id, "sailing_id": sailing_id,
        "decision": decision,
        "decided_at": _event_at(state, "sailing_decision_recorded", "sailing_id", sailing_id),
        "rebuilt_through_seq": head,
    } for sailing_id, decision in state.sailing_decisions.items()]
    rows["pursuit_authorities"] = _pursuit_rows(state, company_id, head)

    lineage = _plan_lineage(state)
    rows["shipment_plans"] = [{
        "company_id": company_id, "plan_id": plan.plan_id,
        "sailing_id": plan.sailing_id,
        "predecessor_plan_id": lineage.get(plan.plan_id, {}).get("predecessor"),
        "plan_generation": lineage.get(plan.plan_id, {}).get("generation", 1),
        "lifecycle": plan.lifecycle, "current_handoff_id": plan.handoff_id,
        "opened_at": plan.opened_at, "finalized_at": plan.finalized_at,
        "closed_at": plan.closed_at, "rebuilt_through_seq": head,
    } for plan in state.plans.values()]
    rows["plan_lines"] = [{
        "company_id": company_id, "plan_id": plan.plan_id,
        "product_id": product_id, "selected_m2": _decimal(line["selected_m2"]),
        "origin": {"manual": "manual_add"}.get(line.get("origin"), line.get("origin") or "postponed"),
        "rebuilt_through_seq": head,
    } for plan in state.plans.values() for product_id, line in plan.lines.items()
       if line.get("selected_m2") is not None]
    rows["bl_splits"] = [{
        "company_id": company_id, "plan_id": plan_id,
        "split_payload": thaw(value), "rebuilt_through_seq": head,
    } for plan_id, value in state.bl_splits.items()]

    handoff_generation = _handoff_generations(state)
    for handoff_id, value in state.handoffs.items():
        payload = thaw(value["handoff"])
        rows["siesa_handoffs"].append({
            "company_id": company_id, "handoff_id": handoff_id,
            "plan_id": value["plan_id"],
            "generation": handoff_generation.get(handoff_id, 1),
            "payload": payload, "superseded": value["superseded"],
            "created_at": value["produced_at"], "rebuilt_through_seq": head,
        })
        for order in payload.get("orders", []):
            order_id = order["order_ref"]
            rows["handoff_orders"].append({
                "company_id": company_id, "handoff_order_id": order_id,
                "handoff_id": handoff_id, "external_ref": None,
                "payload": order, "rebuilt_through_seq": head,
            })
            for line in order.get("lines", []):
                rows["handoff_order_lines"].append({
                    "company_id": company_id, "handoff_order_id": order_id,
                    "product_id": line["product_id"], "m2": _decimal(line["m2"]),
                })

    commitment_by_order = {}
    for commitment in state.commitments.values():
        order_id = commitment.handoff_order_ref
        if order_id is None:
            raise ValueError(
                f"commitment {commitment.commitment_id} has no handoff_order_ref; "
                "migration 005 requires siesa_commitments.handoff_order_id NOT NULL")
        commitment_by_order[order_id] = commitment.commitment_id
        db_state = {"pending_observation": "reference_recorded"}.get(
            commitment.state, commitment.state)
        rows["siesa_commitments"].append({
            "company_id": company_id, "commitment_id": commitment.commitment_id,
            "handoff_order_id": order_id, "state": db_state,
            "external_ref": commitment.siesa_ref, "rebuilt_through_seq": head,
        })
        for line in commitment.quantities:
            rows["commitment_lines"].append({
                "company_id": company_id, "commitment_id": commitment.commitment_id,
                "product_id": line["product_id"], "m2": _decimal(line["m2"]),
            })

    for exclusion in state.exclusions.values():
        rows["pending_handoff_exclusions"].append({
            "company_id": company_id, "exclusion_id": exclusion.exclusion_id,
            "commitment_id": commitment_by_order.get(exclusion.order_ref),
            "product_id": exclusion.product_id, "m2": _decimal(exclusion.m2),
            "active": exclusion.active, "rebuilt_through_seq": head,
        })

    booking_by_ref = {}
    for booking in state.bookings.values():
        booking_by_ref[booking.booking_ref] = booking.booking_id
        rows["bookings"].append({
            "company_id": company_id, "booking_id": booking.booking_id,
            "sailing_id": booking.sailing_id, "booking_ref": booking.booking_ref,
            "confirmed_at": _event_at(state, "booking_recorded", "booking_id", booking.booking_id),
            "rebuilt_through_seq": head,
        })
        rows["booking_commitments"].extend({
            "company_id": company_id, "booking_id": booking.booking_id,
            "commitment_id": commitment_id,
        } for commitment_id in booking.commitment_refs)
    for expectation in state.expectations.values():
        booking_id = booking_by_ref.get(expectation.booking_ref)
        if booking_id is None:
            raise ValueError(
                f"expectation {expectation.exp_id} refers to booking_ref "
                f"{expectation.booking_ref!r} without a folded booking")
        rows["expected_incoming"].append({
            "company_id": company_id, "expectation_id": expectation.exp_id,
            "booking_id": booking_id, "product_id": expectation.product_id,
            "state": expectation.state, "expected_m2": _decimal(expectation.effective_m2),
            "received_m2": _decimal(expectation.received_m2),
            "overdue": expectation.overdue, "rebuilt_through_seq": head,
        })

    rows["implications"] = [{
        "company_id": company_id, "implication_id": implication.implication_id,
        "owner_actor": (implication.scope or {}).get("owner", "ashley"),
        "family": implication.family, "state": implication.state,
        "typed_actions": list(implication.typed_actions),
        "payload": asdict(implication), "rebuilt_through_seq": head,
    } for implication in state.implications.values()]
    rows["monthly_manufacturing_entries"] = [{
        "company_id": company_id, "entry_id": entry_id,
        "plan_id": entry["plan_id"], "product_id": entry["product_id"],
        "uncovered_m2": _decimal(entry["m2"]),
        "month": date.fromisoformat(str(entry["computed_at"])[:10]).replace(day=1),
        "rebuilt_through_seq": head,
    } for entry_id, entry in state.monthly_context.items()
       if not entry["superseded"]]

    for order in state.production_orders.values():
        anchor = order.planning_provenance.get("production_anchor_sailing_id")
        if not anchor:
            raise ValueError(
                f"production order {order.production_order_id} lacks "
                "planning_provenance.production_anchor_sailing_id")
        rows["production_orders"].append({
            "company_id": company_id, "production_order_id": order.production_order_id,
            "source_plan_id": order.source_plan_id,
            "factory_order_date": order.factory_order_date, "required_by": order.required_by,
            "production_anchor_sailing_id": anchor,
            "calculation_head_seq": order.calculation_head_seq,
            "candidate_fingerprint": order.candidate_fingerprint,
            "provenance": order.planning_provenance, "lifecycle": order.lifecycle,
            "frozen_handoff": order.handoff, "production_ref": order.production_ref,
            "opened_at": order.opened_at, "finalized_at": order.finalized_at,
            "reference_recorded_at": order.reference_recorded_at,
            "rebuilt_through_seq": head,
        })
        for product_id, line in order.lines.items():
            rows["production_order_lines"].append({
                "company_id": company_id,
                "production_order_id": order.production_order_id,
                "product_id": product_id, "selected_m2": _decimal(line["selected_m2"]),
                "recommendation_m2": _decimal(line["recommendation_m2"]),
                "origin": line["origin"], "rebuilt_through_seq": head,
            })

    for amendment in state.order_amendments.values():
        rows["order_amendments"].append({
            "company_id": company_id, "amendment_id": amendment.amendment_id,
            "order_kind": amendment.order_kind,
            "shipment_plan_id": amendment.order_id if amendment.order_kind == "shipment" else None,
            "production_order_id": amendment.order_id if amendment.order_kind == "production" else None,
            "original_refs": list(amendment.original_refs),
            "lifecycle": amendment.lifecycle, "frozen_handoff": amendment.handoff,
            "amendment_ref": amendment.amendment_ref, "opened_at": amendment.opened_at,
            "finalized_at": amendment.finalized_at,
            "reference_recorded_at": amendment.reference_recorded_at,
            "rebuilt_through_seq": head,
        })
        for product_id, line in amendment.lines.items():
            rows["order_amendment_lines"].append({
                "company_id": company_id, "amendment_id": amendment.amendment_id,
                "product_id": product_id, "baseline_m2": _decimal(line["baseline_m2"]),
                "selected_m2": _decimal(line["selected_m2"]),
                "rebuilt_through_seq": head,
            })

    rows["suggestion_snapshots"] = [{
        "company_id": company_id,
        "suggestion_id": _stable_uuid(company_id, "suggestion", suggestion_id),
        "plan_id": value["plan_id"], "product_id": value["product_id"],
        "payload": {"source_snapshot_id": suggestion_id, **thaw(value)},
        "event_seq": _event_seq(state, "suggestion_snapshotted", "snapshot_id", suggestion_id),
    } for suggestion_id, value in state.suggestions.items()]
    recompute_events = [event for event in state.events if event.type == "plan_recomputed"]
    rows["plan_recomputes"] = [{
        "company_id": company_id,
        "recompute_id": _stable_uuid(company_id, "recompute", str(event.seq)),
        "plan_id": value["plan_id"], "event_seq": event.seq,
        "before_payload": thaw(value.get("before", {})),
        "after_payload": thaw(value.get("after", {})),
    } for value, event in zip(state.recomputes, recompute_events)]
    return rows


def rebuild_projection_tables(cursor, company_id: str, state, *, after_delete=None) -> dict:
    """Replace mutable company rows and append immutable evidence rows."""
    projected = projection_rows(state, company_id)
    immutable = set(IMMUTABLE_PROJECTION_TABLES)
    for table in reversed(MUTABLE_PROJECTION_TABLES):
        cursor.execute(
            f"DELETE FROM floor_tile.{table} WHERE company_id=%s",
            (company_id,))
    if after_delete is not None:
        after_delete()
    for table in PROJECTION_TABLES:
        for source_row in projected[table]:
            row = dict(source_row)
            # Break the deferrable plan↔handoff FK cycle during insertion.  The
            # exact current pointer is restored after all handoffs are present.
            if table == "shipment_plans":
                row["current_handoff_id"] = None
            columns = tuple(row)
            placeholders = ",".join(["%s"] * len(columns))
            conflict = ",".join(PRIMARY_KEYS[table])
            if table in immutable:
                resolution = "DO NOTHING"
            else:
                updates = ",".join(
                    f"{column}=EXCLUDED.{column}" for column in columns
                    if column not in PRIMARY_KEYS[table])
                resolution = f"DO UPDATE SET {updates}"
            cursor.execute(
                f"INSERT INTO floor_tile.{table} ({','.join(columns)}) "
                f"VALUES ({placeholders}) ON CONFLICT ({conflict}) {resolution}",
                tuple(Jsonb(_json_value(row[column]))
                      if isinstance(row[column], dict)
                      else row[column] for column in columns))
    for row in projected["shipment_plans"]:
        if row["current_handoff_id"] is not None:
            cursor.execute(
                "UPDATE floor_tile.shipment_plans SET current_handoff_id=%s "
                "WHERE company_id=%s AND plan_id=%s",
                (row["current_handoff_id"], company_id, row["plan_id"]))
    return projected
