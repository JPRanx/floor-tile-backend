"""Sole strict transport and operator-input registry for all accepted commands."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Callable

IDENTITY_FIELDS = frozenset({"actor", "actor_id", "identity", "role", "company_id",
                             "auth_user_id", "effective_actor"})
MAX_TEXT = 500
MAX_M2 = Decimal("9999999999.99")
MAX_ITEMS = 100


def selected_plan_products(engine, plan_id: str) -> list[str]:
    plan = engine.state.plans.get(plan_id)
    return sorted(plan.lines) if plan is not None else []


def handoff_order_products(engine, order_ref: str) -> list[str]:
    return sorted({line["product_id"]
                   for handoff in engine.state.handoffs.values()
                   if not handoff["superseded"]
                   for order in handoff["handoff"].get("orders", [])
                   if order.get("order_ref") == order_ref
                   for line in order.get("lines", [])})


def eligible_commitment_refs(engine, sailing_id: str) -> list[str]:
    state = engine.state
    booked = {ref for booking in state.bookings.values()
              for ref in booking.commitment_refs}
    eligible = []
    for ref, commitment in state.commitments.items():
        if commitment.state != "observed" or ref in booked:
            continue
        linked = any(
            not handoff["superseded"]
            and state.plans[handoff["plan_id"]].sailing_id == sailing_id
            and any(order.get("order_ref") == commitment.handoff_order_ref
                    for order in handoff["handoff"].get("orders", []))
            for handoff in state.handoffs.values())
        if linked:
            eligible.append(ref)
    return sorted(eligible)


def _text(value: Any, name: str, *, empty: bool = False) -> str:
    if not isinstance(value, str) or (not empty and not value):
        raise ValueError(f"{name} must be a non-empty UTF-8 string")
    if len(value) > MAX_TEXT:
        raise ValueError(f"{name} exceeds {MAX_TEXT} Unicode characters")
    return value


def _decimal(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a decimal string")
    try:
        d = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a decimal") from None
    if not d.is_finite() or d <= 0 or d > MAX_M2:
        raise ValueError(f"{name} must be positive and at most {MAX_M2}")
    if d.as_tuple().exponent < -2:
        raise ValueError(f"{name} scale must be at most 2")
    return format(d, "f")


def _nonnegative_decimal(value: Any, name: str) -> str:
    """Production-only quantity parser: zero removes a draft line."""
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a decimal string")
    try:
        d = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a decimal") from None
    if not d.is_finite() or d < 0 or d > MAX_M2:
        raise ValueError(f"{name} must be nonnegative and at most {MAX_M2}")
    if d.as_tuple().exponent < -2:
        raise ValueError(f"{name} scale must be at most 2")
    return format(d, "f")


def _list(value: Any, name: str, *, nonempty: bool = False) -> list:
    if not isinstance(value, list) or len(value) > MAX_ITEMS or (nonempty and not value):
        raise ValueError(f"{name} must be a bounded{' non-empty' if nonempty else ''} array")
    return value


def _date(value: Any, name: str):
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise ValueError(f"{name} must be an ISO date")


def _id(value: Any, name: str) -> str:
    return _text(value, name)


@dataclass(frozen=True)
class CommandSchema:
    allowed: frozenset[str]
    required: frozenset[str] = frozenset()
    validator: Callable[[dict, set[str] | None], dict] | None = None
    operator_fields: tuple[str, ...] = ()

    def validate(self, params: dict, catalog_ids: set[str] | None = None) -> dict:
        if not isinstance(params, dict):
            raise ValueError("params must be an object")
        identity = IDENTITY_FIELDS & params.keys()
        if identity:
            raise ValueError("request params must not carry identity fields")
        unknown = set(params) - self.allowed
        if unknown:
            raise ValueError(f"unknown field(s): {sorted(unknown)}")
        missing = self.required - params.keys()
        if missing:
            raise ValueError(f"missing required field(s): {sorted(missing)}")
        out = dict(params)
        return self.validator(out, catalog_ids) if self.validator else out

    def input_schema_for(self, fixed: dict, engine=None) -> dict:
        if self is COMMAND_SCHEMAS.get("ResolveImplication"):
            action = fixed.get("action")
            props = {"note": {"type": "string", "maxLength": MAX_TEXT}}
            required = []
            if action == "map":
                props["product_id"] = {"type": "string",
                                       "enum": sorted(engine.catalog()) if engine else []}
                required.append("product_id")

            return {"type": "object", "additionalProperties": False,
                    "properties": props, "required": required}
        properties = {}
        required = []
        for field in self.operator_fields:
            if field == "decision":
                properties[field] = {"type": "string", "enum": ["use", "watch", "skip"]}
                required.append(field)
            elif field == "m2":
                properties[field] = {"type": "string", "format": "decimal",
                                     "exclusiveMinimum": "0", "maximum": str(MAX_M2),
                                     "maxScale": 2}
                required.append(field)
            elif field == "edits":
                product_ids = sorted(engine.catalog()) if engine is not None else []
                nonnegative = self is COMMAND_SCHEMAS.get("EditSelectedM2Batch")
                quantity_schema = {"type": "string", "format": "decimal",
                                   "minimum" if nonnegative else "exclusiveMinimum": "0",
                                   "maximum": str(MAX_M2), "maxScale": 2,
                                   "multipleOf": "67.20"}
                properties[field] = {
                    "type": "array", "minItems": 1, "maxItems": MAX_ITEMS,
                    "items": {
                        "type": "object", "additionalProperties": False,
                        "properties": {
                            "product_id": {"type": "string", "enum": product_ids},
                            "m2": quantity_schema,
                        },
                        "required": ["product_id", "m2"],
                    },
                }
                required.append(field)
            elif field in ("note", "bl_ref"):
                properties[field] = {"type": "string", "maxLength": MAX_TEXT}
            elif field == "groups":
                product_ids = (selected_plan_products(engine, fixed.get("plan_id"))
                               if engine is not None else [])
                properties[field] = {
                    "type": "array", "minItems": 1, "maxItems": MAX_ITEMS,
                    "items": {
                        "type": "object", "additionalProperties": False,
                        "properties": {
                            "bl_no": {"type": ["string", "integer"],
                                      "minLength": 1, "maxLength": MAX_TEXT,
                                      "minimum": 1, "maximum": MAX_ITEMS},
                            "container_count": {"type": "integer", "minimum": 1,
                                                "maximum": MAX_ITEMS},
                            "lines": {
                                "type": "array", "minItems": 1,
                                "maxItems": MAX_ITEMS,
                                "items": {
                                    "type": "object", "additionalProperties": False,
                                    "properties": {
                                        "product_id": {"type": "string",
                                                       "enum": product_ids},
                                        "m2": {"type": "string", "format": "decimal",
                                               "exclusiveMinimum": "0",
                                               "maximum": str(MAX_M2), "maxScale": 2},
                                    },
                                    "required": ["product_id", "m2"],
                                },
                            },
                        },
                        "required": ["bl_no", "container_count", "lines"],
                    },
                }
                required.append(field)
            elif field in ("siesa_ref", "production_ref", "amendment_ref"):
                properties[field] = {"type": "string", "minLength": 1,
                                     "maxLength": MAX_TEXT}
                required.append(field)
            elif field == "quantities":
                product_ids = (handoff_order_products(
                    engine, fixed.get("handoff_order_ref")) if engine is not None else [])
                properties[field] = {
                    "type": "array", "minItems": 1, "maxItems": MAX_ITEMS,
                    "items": {
                        "type": "object", "additionalProperties": False,
                        "properties": {
                            "product_id": {"type": "string", "enum": product_ids},
                            "m2": {"type": "string", "format": "decimal",
                                   "exclusiveMinimum": "0", "maximum": str(MAX_M2),
                                   "maxScale": 2},
                        },
                        "required": ["product_id", "m2"],
                    },
                }
            elif field == "commitment_refs":
                refs = (eligible_commitment_refs(engine, fixed.get("sailing_id"))
                        if engine is not None else [])
                properties[field] = {"type": "array", "minItems": 1,
                                     "maxItems": MAX_ITEMS,
                                     "items": {"type": "string", "enum": refs}}
                required.append(field)
            elif field == "booking_ref":
                properties[field] = {"type": "string", "minLength": 1,
                                     "maxLength": MAX_TEXT}
                required.append(field)
            elif field == "product_id":
                ids = sorted(engine.catalog()) if engine is not None else []
                properties[field] = {"type": "string", "enum": ids}
                required.append(field)
        return {"type": "object", "additionalProperties": False,
                "properties": properties, "required": required}


def _ids(fields):
    def validate(p, catalog):
        for f in fields:
            p[f] = _id(p[f], f)
        return p
    return validate


def _record_sailing(p, _):
    p["carrier"] = _text(p["carrier"], "carrier")
    p["name"] = _text(p["name"], "name")
    p["departure"] = _date(p["departure"], "departure")
    if "voyage_days" in p and (not isinstance(p["voyage_days"], int)
                                or isinstance(p["voyage_days"], bool)
                                or not 0 < p["voyage_days"] <= 365):
        raise ValueError("voyage_days must be an integer from 1 to 365")
    if "as_of" in p:
        p["as_of"] = _date(p["as_of"], "as_of")
    if p.get("raw_source_ref") is not None:
        p["raw_source_ref"] = _text(p["raw_source_ref"], "raw_source_ref")
    return p


def _feed(p, _):
    p["as_of"] = _date(p["as_of"], "as_of")
    if "rows" in p:
        p["rows"] = _list(p["rows"], "rows")
    if "text" in p and p["text"] is not None:
        p["text"] = _text(p["text"], "text", empty=True)
    if "rows" not in p and "text" not in p:
        p["rows"] = []
    if p.get("raw_source_ref") is not None:
        p["raw_source_ref"] = _text(p["raw_source_ref"], "raw_source_ref")
    return p


def _decision(p, _):
    p["sailing_id"] = _id(p["sailing_id"], "sailing_id")
    if p["decision"] not in ("use", "watch", "skip"):
        raise ValueError("decision must be use, watch, or skip")
    return p


def _m2(p, catalog):
    p["plan_id"] = _id(p["plan_id"], "plan_id")
    p["product_id"] = _id(p["product_id"], "product_id")
    if catalog is not None and p["product_id"] not in catalog:
        raise ValueError("product_id is not in the current catalog")
    p["m2"] = _decimal(p["m2"], "m2")
    return p


def _m2_batch(p, catalog):
    p["plan_id"] = _id(p["plan_id"], "plan_id")
    edits = _list(p["edits"], "edits", nonempty=True)
    seen = set()
    for edit in edits:
        if not isinstance(edit, dict) or set(edit) != {"product_id", "m2"}:
            raise ValueError("each edit has exactly product_id and m2")
        product_id = _id(edit["product_id"], "product_id")
        if product_id in seen:
            raise ValueError("batch product_id values must be unique")
        if catalog is not None and product_id not in catalog:
            raise ValueError("product_id is not in the current catalog")
        m2 = _nonnegative_decimal(edit["m2"], "m2")
        if Decimal(m2) % Decimal("67.20") != 0:
            raise ValueError("m2 must be divisible by 67.20")
        edit["product_id"], edit["m2"] = product_id, m2
        seen.add(product_id)
    return p


def _open_production(p, _):
    p["plan_id"] = _id(p["plan_id"], "plan_id")
    p["production_anchor_sailing_id"] = _id(
        p["production_anchor_sailing_id"], "production_anchor_sailing_id")
    p["factory_order_date"] = _date(p["factory_order_date"], "factory_order_date")
    fingerprint = p["candidate_fingerprint"]
    if (not isinstance(fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None):
        raise ValueError("candidate_fingerprint must be lowercase 64-hex SHA-256")
    return p


def _production_m2_batch(p, catalog):
    p["production_order_id"] = _id(p["production_order_id"], "production_order_id")
    edits = _list(p["edits"], "edits", nonempty=True)
    seen = set()
    for edit in edits:
        if not isinstance(edit, dict) or set(edit) != {"product_id", "m2"}:
            raise ValueError("each edit has exactly product_id and m2")
        pid = _id(edit["product_id"], "product_id")
        if pid in seen:
            raise ValueError("batch product_id values must be unique")
        if catalog is not None and pid not in catalog:
            raise ValueError("product_id is not in the current catalog")
        m2 = _nonnegative_decimal(edit["m2"], "m2")
        if Decimal(m2) % Decimal("67.20") != 0:
            raise ValueError("m2 must be divisible by 67.20")
        edit["product_id"], edit["m2"] = pid, m2
        seen.add(pid)
    return p


def _amendment_open(p, _):
    if p["order_kind"] not in ("shipment", "production"):
        raise ValueError("order_kind must be shipment or production")
    p["order_id"] = _id(p["order_id"], "order_id")
    return p


def _amendment_m2_batch(p, catalog):
    p["amendment_id"] = _id(p["amendment_id"], "amendment_id")
    edits = _list(p["edits"], "edits", nonempty=True)
    seen = set()
    for edit in edits:
        if not isinstance(edit, dict) or set(edit) != {"product_id", "m2"}:
            raise ValueError("each edit has exactly product_id and m2")
        pid = _id(edit["product_id"], "product_id")
        if pid in seen:
            raise ValueError("batch product_id values must be unique")
        if catalog is not None and pid not in catalog:
            raise ValueError("product_id is not in the current catalog")
        if not isinstance(edit["m2"], str):
            raise ValueError("m2 must be a decimal string")
        try:
            m2 = Decimal(edit["m2"])
        except InvalidOperation:
            raise ValueError("m2 must be a decimal") from None
        if not m2.is_finite() or m2 < 0 or m2 > MAX_M2 or m2.as_tuple().exponent < -2:
            raise ValueError("m2 must be nonnegative with scale at most 2")
        if m2 % Decimal("67.20") != 0:
            raise ValueError("m2 must be divisible by 67.20")
        edit["product_id"], edit["m2"] = pid, format(m2, "f")
        seen.add(pid)
    return p


def _resolve(p, catalog):
    p["implication_id"] = _id(p["implication_id"], "implication_id")
    p["action"] = _id(p["action"], "action")
    nested = p.get("params", {})
    if not isinstance(nested, dict):
        raise ValueError("params must be a nested object")
    if p["action"] == "create":
        raise ValueError("create is not a supported V1 product-match action")
    unknown = set(nested) - {"product_id", "note"}
    if unknown:
        raise ValueError(f"unknown nested field(s): {sorted(unknown)}")
    if "note" in nested:
        nested["note"] = _text(nested["note"], "params.note", empty=True)
    if p["action"] == "map":
        if "product_id" not in nested:
            raise ValueError("map requires params.product_id")
        nested["product_id"] = _id(nested["product_id"], "params.product_id")
        if catalog is not None and nested["product_id"] not in catalog:
            raise ValueError("params.product_id is not in the current catalog")
    elif "product_id" in nested:
        raise ValueError("non-map actions forbid product_id")
    p["params"] = nested
    return p


def _risk(p, _):
    p["implication_id"] = _id(p["implication_id"], "implication_id")
    if "note" in p:
        p["note"] = _text(p["note"], "note", empty=True)
    return p


def _groups(p, catalog):
    p["plan_id"] = _id(p["plan_id"], "plan_id")
    groups = _list(p["groups"], "groups", nonempty=True)
    for group in groups:
        if not isinstance(group, dict) or set(group) != {"bl_no", "container_count", "lines"}:
            raise ValueError("each BL group has exactly bl_no, container_count, lines")
        bl_no = group["bl_no"]
        if isinstance(bl_no, int):
            if isinstance(bl_no, bool) or not 0 < bl_no <= MAX_ITEMS:
                raise ValueError("integer bl_no is out of bounds")
        else:
            _text(bl_no, "bl_no")
        if (not isinstance(group["container_count"], int)
                or isinstance(group["container_count"], bool)
                or not 0 < group["container_count"] <= MAX_ITEMS):
            raise ValueError("container_count is out of bounds")
        for line in _list(group["lines"], "lines", nonempty=True):
            if not isinstance(line, dict) or set(line) != {"product_id", "m2"}:
                raise ValueError("each BL line has exactly product_id and m2")
            line["product_id"] = _id(line["product_id"], "product_id")
            if catalog is not None and line["product_id"] not in catalog:
                raise ValueError("BL product_id is not permitted")
            line["m2"] = _decimal(line["m2"], "m2")
    return p


def _siesa_ref(p, catalog):
    p["handoff_order_ref"] = _id(p["handoff_order_ref"], "handoff_order_ref")
    p["siesa_ref"] = _text(p["siesa_ref"], "siesa_ref")
    if "quantities" in p:
        for q in _list(p["quantities"], "quantities", nonempty=True):
            if not isinstance(q, dict) or set(q) != {"product_id", "m2"}:
                raise ValueError("quantity has exactly product_id and m2")
            q["product_id"] = _id(q["product_id"], "product_id")
            if catalog is not None and q["product_id"] not in catalog:
                raise ValueError("quantity product_id is not permitted")
            q["m2"] = _decimal(q["m2"], "m2")
    return p


def _booking(p, _):
    p["sailing_id"] = _id(p["sailing_id"], "sailing_id")
    p["booking_ref"] = _text(p["booking_ref"], "booking_ref")
    p["commitment_refs"] = [_id(x, "commitment_ref") for x in
                             _list(p["commitment_refs"], "commitment_refs", nonempty=True)]
    return p


def _departure(p, _):
    p["booking_ref"] = _id(p["booking_ref"], "booking_ref")
    if "bl_ref" in p:
        p["bl_ref"] = _text(p["bl_ref"], "bl_ref", empty=True)
    return p


def _correction(action, id_field):
    def validate(p, _):
        p[id_field] = _id(p[id_field], id_field)
        if p["action"] != action:
            raise ValueError(f"action must be {action}")
        if "note" in p:
            p["note"] = _text(p["note"], "note", empty=True)
        return p
    return validate


S = CommandSchema
COMMAND_SCHEMAS = {
    "RecordSailing": S(frozenset({"carrier", "name", "departure", "voyage_days", "as_of", "raw_source_ref"}), frozenset({"carrier", "name", "departure"}), _record_sailing),
    "ImportSailingCalendarText": S(frozenset({"text", "as_of", "raw_source_ref"}), frozenset({"text"}), lambda p, _: {**p, "text": _text(p["text"], "text", empty=True), **({"as_of": _date(p["as_of"], "as_of")} if "as_of" in p else {}), **({"raw_source_ref": _text(p["raw_source_ref"], "raw_source_ref")} if p.get("raw_source_ref") is not None else {})}),
    "SetSailingDecision": S(frozenset({"sailing_id", "decision"}), frozenset({"sailing_id", "decision"}), _decision, ("decision",)),
    "PursueExceptionalSailing": S(frozenset({"sailing_id"}), frozenset({"sailing_id"}), _ids(("sailing_id",))),
    "OpenShipmentPlan": S(frozenset({"sailing_id"}), frozenset({"sailing_id"}), _ids(("sailing_id",))),
    "AcceptSuggestion": S(frozenset({"plan_id", "product_id"}), frozenset({"plan_id", "product_id"}), _ids(("plan_id", "product_id"))),
    "EditSelectedM2": S(frozenset({"plan_id", "product_id", "m2"}), frozenset({"plan_id", "product_id", "m2"}), _m2, ("m2",)),
    "EditSelectedM2Batch": S(frozenset({"plan_id", "edits"}), frozenset({"plan_id", "edits"}), _m2_batch, ("edits",)),
    "PostponeProduct": S(frozenset({"plan_id", "product_id"}), frozenset({"plan_id", "product_id"}), _ids(("plan_id", "product_id"))),
    "AddManualLine": S(frozenset({"plan_id", "product_id", "m2"}), frozenset({"plan_id", "product_id", "m2"}), _m2, ("m2",)),
    "AcceptAllSuggestions": S(frozenset({"plan_id"}), frozenset({"plan_id"}), _ids(("plan_id",))),
    "OverrideBLSplit": S(frozenset({"plan_id", "groups"}), frozenset({"plan_id", "groups"}), _groups, ("groups",)),
    "FinalizeSailingPlan": S(frozenset({"plan_id"}), frozenset({"plan_id"}), _ids(("plan_id",))),
    "RequestReallocation": S(frozenset({"plan_id"}), frozenset({"plan_id"}), _ids(("plan_id",))),
    "ConfirmReallocationIntent": S(frozenset({"plan_id"}), frozenset({"plan_id"}), _ids(("plan_id",))),
    "CloseSailingPlan": S(frozenset({"plan_id"}), frozenset({"plan_id"}), _ids(("plan_id",))),
    "AcceptImplicationRisk": S(frozenset({"implication_id", "note"}), frozenset({"implication_id"}), _risk, ("note",)),
    "ResolveImplication": S(frozenset({"implication_id", "action", "params"}), frozenset({"implication_id", "action", "params"}), _resolve),
    "RecordSiesaOrderReference": S(frozenset({"handoff_order_ref", "siesa_ref", "quantities"}), frozenset({"handoff_order_ref", "siesa_ref"}), _siesa_ref, ("siesa_ref", "quantities")),
    "RecordBookingConfirmation": S(frozenset({"sailing_id", "commitment_refs", "booking_ref"}), frozenset({"sailing_id", "commitment_refs", "booking_ref"}), _booking, ("commitment_refs", "booking_ref")),
    "RecordBlDeparture": S(frozenset({"booking_ref", "bl_ref"}), frozenset({"booking_ref"}), _departure, ("bl_ref",)),
    **{name: S(frozenset({"as_of", "rows", "text", "raw_source_ref"}), frozenset({"as_of"}), _feed)
       for name in ("LoadSiesaSnapshot", "LoadWarehouseSnapshot", "LoadSalesSnapshot",
                    "LoadTransitSnapshot", "LoadCommittedOrders", "LoadProductionPlanning")},
    "RunChecks": S(frozenset()),
    "OpenProductionOrder": S(
        frozenset({"plan_id", "production_anchor_sailing_id", "factory_order_date",
                   "candidate_fingerprint"}),
        frozenset({"plan_id", "production_anchor_sailing_id", "factory_order_date",
                   "candidate_fingerprint"}), _open_production),
    "EditProductionOrderBatch": S(frozenset({"production_order_id", "edits"}), frozenset({"production_order_id", "edits"}), _production_m2_batch, ("edits",)),
    "FinalizeProductionOrder": S(frozenset({"production_order_id"}), frozenset({"production_order_id"}), _ids(("production_order_id",))),
    "RecordProductionOrderReference": S(frozenset({"production_order_id", "production_ref"}), frozenset({"production_order_id", "production_ref"}), lambda p, _: {**p, "production_order_id": _id(p["production_order_id"], "production_order_id"), "production_ref": _text(p["production_ref"], "production_ref")}, ("production_ref",)),
    "ReopenSailingPlan": S(frozenset({"plan_id"}), frozenset({"plan_id"}), _ids(("plan_id",))),
    "ReopenProductionOrder": S(frozenset({"production_order_id"}), frozenset({"production_order_id"}), _ids(("production_order_id",))),
    "OpenOrderAmendment": S(frozenset({"order_kind", "order_id"}), frozenset({"order_kind", "order_id"}), _amendment_open),
    "EditOrderAmendmentBatch": S(frozenset({"amendment_id", "edits"}), frozenset({"amendment_id", "edits"}), _amendment_m2_batch, ("edits",)),
    "FinalizeOrderAmendment": S(frozenset({"amendment_id"}), frozenset({"amendment_id"}), _ids(("amendment_id",))),
    "RecordOrderAmendmentReference": S(frozenset({"amendment_id", "amendment_ref"}), frozenset({"amendment_id", "amendment_ref"}), lambda p, _: {**p, "amendment_id": _id(p["amendment_id"], "amendment_id"), "amendment_ref": _text(p["amendment_ref"], "amendment_ref")}, ("amendment_ref",)),
    "CorrectObservedCommitment": S(frozenset({"commitment_id", "action", "note"}), frozenset({"commitment_id", "action"}), _correction("release", "commitment_id")),
    "CorrectExpectation": S(frozenset({"exp_id", "action", "note"}), frozenset({"exp_id", "action"}), _correction("cancel", "exp_id")),
}

MATRIX_COMMANDS = frozenset({
    "SetSailingDecision", "PursueExceptionalSailing", "OpenShipmentPlan",
    "AcceptImplicationRisk", "ResolveImplication", "AcceptSuggestion",
    "EditSelectedM2", "EditSelectedM2Batch", "PostponeProduct", "AddManualLine", "AcceptAllSuggestions",
    "FinalizeSailingPlan", "RequestReallocation", "CloseSailingPlan",
    "OverrideBLSplit", "ConfirmReallocationIntent", "RecordSiesaOrderReference",
    "RecordBookingConfirmation", "RecordBlDeparture",
    "OpenProductionOrder", "EditProductionOrderBatch",
    "FinalizeProductionOrder", "RecordProductionOrderReference",
    "ReopenSailingPlan", "ReopenProductionOrder", "OpenOrderAmendment",
    "EditOrderAmendmentBatch", "FinalizeOrderAmendment",
    "RecordOrderAmendmentReference",
})


def validate_command(command: str, params: dict,
                     *, catalog_ids: set[str] | None = None, engine=None) -> dict:
    schema = COMMAND_SCHEMAS.get(command)
    if schema is None:
        raise ValueError(f"unknown command {command}")
    if catalog_ids is None and engine is not None:
        catalog_ids = set(engine.catalog())
    out = schema.validate(params, catalog_ids)
    if engine is None:
        return out
    if command == "OverrideBLSplit":
        permitted = set(selected_plan_products(engine, out["plan_id"]))
        if any(line["product_id"] not in permitted for group in out["groups"]
               for line in group["lines"]):
            raise ValueError("BL product_id is not in the current selected plan")
    elif command == "RecordSiesaOrderReference" and "quantities" in out:
        permitted = set(handoff_order_products(engine, out["handoff_order_ref"]))
        if any(q["product_id"] not in permitted for q in out["quantities"]):
            raise ValueError("quantity product_id is not in the current handoff")
    elif command == "RecordBookingConfirmation":
        permitted = set(eligible_commitment_refs(engine, out["sailing_id"]))
        if any(ref not in permitted for ref in out["commitment_refs"]):
            raise ValueError("commitment_ref is not currently eligible for this sailing")
    elif command == "ResolveImplication":
        implication = engine.state.implications.get(out["implication_id"])
        if (implication is None or implication.state != "open"
                or out["action"] not in implication.typed_actions):
            raise ValueError("action is not a current typed action for this implication")
    return out


def command_witness(command: str, fixed: dict, engine) -> dict:
    """Deterministic bounded valid transport witness for an input candidate."""
    p = dict(fixed)
    if command == "SetSailingDecision":
        p["decision"] = "watch"
    elif command in ("EditSelectedM2", "AddManualLine"):
        p["m2"] = "134.40"
    elif command == "EditSelectedM2Batch":
        product_ids = selected_plan_products(engine, p["plan_id"])
        if not product_ids:
            product_ids = sorted(engine.catalog())[:1]
        p["edits"] = [{"product_id": product_id, "m2": "134.40"}
                      for product_id in product_ids]
    elif command == "EditProductionOrderBatch":
        order = engine.state.production_orders[p["production_order_id"]]
        p["edits"] = [{"product_id": product_id, "m2": "134.40"}
                      for product_id in order.lines]
    elif command == "EditOrderAmendmentBatch":
        amendment = engine.state.order_amendments[p["amendment_id"]]
        p["edits"] = [{"product_id": product_id, "m2": "134.40"}
                      for product_id in amendment.lines]
    elif command == "RecordProductionOrderReference":
        p["production_ref"] = "PROD-PARITY-001"
    elif command == "RecordOrderAmendmentReference":
        p["amendment_ref"] = "AMD-PARITY-001"
    elif command == "OverrideBLSplit":
        plan = engine.state.plans[p["plan_id"]]
        groups = engine.state.bl_splits.get(p["plan_id"], {}).get("groups")
        if groups is None:
            from . import sailing_math
            groups = sailing_math.default_bl_groups(
                engine._plan_line_order(engine.state, plan))
        p["groups"] = [
            {
                "bl_no": group["bl_no"],
                "container_count": group["container_count"],
                "lines": [
                    {"product_id": line["product_id"],
                     "m2": format(line["m2"], "f")}
                    for line in group["lines"]
                ],
            }
            for group in groups
        ]
    elif command == "ResolveImplication":
        if p["action"] == "map":
            p["params"] = {"product_id": sorted(engine.catalog())[0]}
        else:
            p["params"] = {}
    elif command == "RecordSiesaOrderReference":
        p["siesa_ref"] = "T1-PROBE"
    elif command == "RecordBookingConfirmation":
        p["commitment_refs"] = eligible_commitment_refs(engine, p["sailing_id"])
        p["booking_ref"] = "T1-PROBE-BOOKING"
    return validate_command(command, p, catalog_ids=set(engine.catalog()), engine=engine)
