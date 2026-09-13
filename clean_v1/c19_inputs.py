"""§19 backend-only read/input carriers over the accepted command authority.

Previews and apply tokens are process-local and ephemeral.  Applying invokes an
existing §7 command; this module defines no event, domain object, or command.
"""
from __future__ import annotations

import copy
import hashlib
import json
import secrets
from datetime import date
from decimal import Decimal
from typing import Any

from . import sailing_normalize as sn
from .command_schema import COMMAND_SCHEMAS, validate_command
from .domain import thaw


FEED_COMMANDS = {
    "sailing_calendar": "ImportSailingCalendarText",
    "siesa_availability": "LoadSiesaSnapshot",
    "warehouse": "LoadWarehouseSnapshot",
    "sales": "LoadSalesSnapshot",
    "in_transit": "LoadTransitSnapshot",
    "committed_orders": "LoadCommittedOrders",
    "production_planning": "LoadProductionPlanning",
}
FULL_SNAPSHOT_FEEDS = frozenset({
    "siesa_availability", "warehouse", "sales", "in_transit",
    "committed_orders", "production_planning",
})

SOURCE_CONFIG = {
    "sailing_calendar": ("Calendario de zarpes", "planning", ["planning"],
                         "replace_by_sailing", ["direct", "paste", "upload"]),
    "siesa_availability": ("Disponibilidad SIESA", "planning", ["planning", "finalization"],
                           "full_snapshot", ["direct", "paste", "upload"]),
    "warehouse": ("Inventario en bodega", "planning", ["planning"],
                  "full_snapshot", ["direct", "paste", "upload"]),
    "sales": ("Ventas / rotación", "planning", ["planning"], "full_snapshot",
              ["direct", "paste", "upload"]),
    "siesa_order_reference": ("Referencia SIESA registrada", "follow_up", [],
                              "append", ["direct"]),
    "booking_confirmation": ("Reserva confirmada", "follow_up", [],
                             "append", ["direct"]),
    "bl_departure": ("Salida confirmada", "follow_up", [], "append", ["direct"]),
    "in_transit": ("Tránsito observado", "follow_up", [], "full_snapshot",
                   ["direct", "paste", "upload"]),
    "committed_orders": ("Compromisos de cliente", "optional", [],
                         "full_snapshot", ["direct", "paste", "upload"]),
    "production_planning": ("Planificación de producción", "context", [],
                            "full_snapshot", ["direct", "paste", "upload"]),
}


def _iso(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _iso(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_iso(v) for v in value]
    return value


def compose_source_hub(engine, principal) -> dict:
    """Project the existing readiness/status/action authorities without twins."""
    readiness = {item["source"]: item for item in engine.evidence_readiness()}
    open_plans = [p for p in engine.state.plans.values()
                  if p.lifecycle == "draft"]
    sources = []
    for feed, (label, group, critical, semantics, modes) in SOURCE_CONFIG.items():
        ready = readiness.get(feed)
        if ready is None:
            status, as_of, age = "not_applicable", None, None
        else:
            status, as_of, age = ready["status"], ready["as_of"], ready["age_days"]
        current = engine.state.feeds.get(feed)
        last = None
        if feed == "sailing_calendar":
            event, _ = engine._current_sailing_source_event(engine.state)
            if event is not None:
                payload = thaw(event.payload)
                last = {"observation_ref": payload["sailing_id"],
                        "at": event.at, "actor": event.actor,
                        "entered_via": payload.get("entered_via"),
                        "raw_source_ref": payload.get("raw_source_ref")}
        if current is not None:
            event = next((candidate for candidate in reversed(engine.state.events)
                          if candidate.type == "evidence_loaded"
                          and thaw(candidate.payload).get("snapshot_id")
                          == current["snapshot_id"]), None)
            last = {"observation_ref": current["snapshot_id"],
                    "at": event.at if event is not None else None,
                    "actor": event.actor if event is not None else None,
                    "entered_via": current.get("entered_via"),
                    "raw_source_ref": current.get("raw_source_ref")}
        if feed == "sailing_calendar":
            current_rows = [{
                "sailing_id": sailing.sailing_id,
                "carrier": sailing.carrier,
                "name": sailing.name,
                "departure": _iso(sailing.departure),
                "voyage_days": sailing.voyage_days,
                "entered_via": sailing.entered_via,
            } for sailing in sorted(engine.state.sailings.values(),
                                    key=lambda item: (item.departure, item.name))]
        elif current is not None:
            catalog = engine.catalog(engine.state)
            current_rows = []
            for row in engine._rows(engine.state, feed):
                presented = _iso(dict(row))
                product = catalog.get(row.get("product_id"), {})
                presented["product_name"] = product.get("name") or product.get("sku")
                presented["product_sku"] = product.get("sku")
                current_rows.append(presented)
        else:
            current_rows = []
        effects = []
        for plan in open_plans:
            plan_status = engine.plan_status(plan.plan_id)[0]
            effects.append({
                "plan_id": plan.plan_id,
                "plan_status_effect": (plan_status if plan_status != "ready" else "none"),
                "finalization_denied": plan_status != "ready",
            })
        legal_actions = []
        command = FEED_COMMANDS.get(feed)
        if command and getattr(principal, "effective_actor", "") == "ashley":
            legal_actions.append({
                "command": command,
                "params": {},
                "label": f"Actualizar {label}",
                "input_schema": {"server_preview_required": True,
                                 "supported_modes": list(modes)},
            })
        sources.append({
            "feed": feed, "operator_label": label, "group": group,
            "critical_for": list(critical), "as_of": _iso(as_of),
            "age_days": age, "status": status, "last_applied": last,
            "current_rows": current_rows,
            "supported_modes": list(modes),
            "replacement_semantics": semantics,
            "impact": {"affected_open_drafts": effects},
            "legal_actions": legal_actions,
        })
    held = engine._held_rows(engine.state)
    ashley_held = [i for i in engine.state.implications.values()
                   if i.state == "open" and i.family == "product_match"
                   and (i.scope or {}).get("owner", "ashley") == "ashley"]
    recommended = next(({"feed": source["feed"],
                         "action": source["legal_actions"][0]}
                        for source in sources
                        if source["status"] in ("missing", "stale")
                        and source["legal_actions"]), None)
    source_by_feed = {source["feed"]: source for source in sources}
    task_feeds = [
        ("planning", ["sailing_calendar", "warehouse", "sales"]),
        ("finalization", ["siesa_availability"]),
        ("follow_up", ["siesa_order_reference", "booking_confirmation",
                       "bl_departure", "in_transit"]),
        ("optional_context", ["committed_orders", "production_planning"]),
    ]
    task_groups = []
    for task, feeds in task_feeds:
        entries = []
        for feed in feeds:
            source = source_by_feed[feed]
            authority = ({"preview": "/api/input/preview",
                          "revise": "/api/input/preview",
                          "apply": "/api/input/apply",
                          "apply_requires_server_token": True}
                         if feed in FEED_COMMANDS else None)
            entries.append({
                "feed": feed,
                "operator_label": source["operator_label"],
                "columns": list(sn.FEED_COLUMNS.get(feed, ())),
                "supported_modes": list(source["supported_modes"]),
                "replacement_semantics": source["replacement_semantics"],
                "status": source["status"],
                "input_authority": authority,
                "legal_actions": copy.deepcopy(source["legal_actions"]),
            })
        task_groups.append({"task": task, "sources": entries})
    return {
        "sources": sources,
        "source_ledger": {"schema": "SourceLedger", "task_groups": task_groups},
        "recommended_next_input": recommended,
        "held_rows_summary": {
            "count": len(held),
            "feeds": sorted({row["feed"] for row in held}),
            "owner_routed_count": max(0, len(held) - len(ashley_held)),
            "ashley_actionable_count": len(ashley_held),
        },
    }


class PreviewDenied(ValueError):
    pass


class InputPreviewService:
    """Process-local capability store; deliberately absent from domain state."""

    def __init__(self):
        self._candidates: dict[str, dict] = {}
        self._previews: dict[str, dict] = {}

    @staticmethod
    def _digest(value: dict) -> str:
        encoded = json.dumps(_iso(value), sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _parse_as_of(value) -> date:
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError as exc:
            raise PreviewDenied("as_of must be an ISO date") from exc

    def _quantity_rows(self, engine, feed, input_mode, rows, text, *,
                       server_parsed=False):
        entered_via = "direct" if input_mode == "direct" else "parsed"
        invalid = []
        if input_mode == "direct" or server_parsed:
            raw_rows = list(rows or [])
        else:
            raw_rows, errors = sn.parse_quantity_table_text(feed, text or "")
            invalid = [{"source_row_ref": error.split(":", 1)[0],
                        "normalized_fields": None,
                        "product_match_state": "not_applicable",
                        "status": "invalid", "message": error}
                       for error in errors]
        presented = []
        normalized = []
        for index, raw in enumerate(raw_rows, 1):
            try:
                row = sn.normalize_quantity_rows(
                    feed, [raw], products=list(engine.catalog(engine.state).values()),
                    entered_via=entered_via, mappings=engine.state.mappings)[0]
                normalized.append(row)
                held = not row["confident_match"]
                presented.append({
                    "source_row_ref": f"row-{index}",
                    "normalized_fields": _iso({k: v for k, v in row.items()
                                                if k != "entered_via"}),
                    "product_match_state": "held" if held else "matched",
                    "status": "held" if held else "valid",
                    "message": ("requires accepted product_match resolution"
                                if held else "valid"),
                })
            except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
                invalid.append({"source_row_ref": f"row-{index}",
                                "normalized_fields": None,
                                "product_match_state": "not_applicable",
                                "status": "invalid", "message": str(exc)})
        try:
            sn.reject_duplicate_product_rows(feed, normalized)
        except ValueError as exc:
            invalid.extend({**row, "status": "invalid", "message": str(exc)}
                           for row in presented if row["status"] == "valid")
            presented = [row for row in presented if row["status"] != "valid"]
        normalized = sn.merge_duplicate_product_rows(feed, normalized)
        return normalized, presented + invalid, entered_via

    def _sailing_rows(self, engine, input_mode, rows, text):
        entered_via = "direct" if input_mode == "direct" else "parsed"
        invalid = []
        if input_mode == "direct":
            raw_rows = list(rows or [])
        else:
            raw_rows, errors = sn.parse_sailing_calendar_text(text or "")
            invalid = [{"source_row_ref": error.split(":", 1)[0],
                        "normalized_fields": None,
                        "product_match_state": "not_applicable",
                        "status": "invalid", "message": error}
                       for error in errors]
        normalized, presented = [], []
        for index, raw in enumerate(raw_rows, 1):
            try:
                candidate = dict(raw)
                if not isinstance(candidate.get("departure"), date):
                    candidate["departure"] = date.fromisoformat(
                        str(candidate.get("departure")))
                row = sn.normalize_sailing_row(candidate, entered_via=entered_via)
                collisions = [s for s in engine.state.sailings.values()
                              if s.carrier == row["carrier"]
                              and s.departure == row["departure"]
                              and s.name != row["name"]]
                if collisions:
                    raise ValueError("ambiguous sailing identity; correct this preview row")
                normalized.append(row)
                presented.append({"source_row_ref": f"row-{index}",
                                  "normalized_fields": _iso(row),
                                  "product_match_state": "not_applicable",
                                  "status": "valid", "message": "valid"})
            except (KeyError, TypeError, ValueError) as exc:
                invalid.append({"source_row_ref": f"row-{index}",
                                "normalized_fields": None,
                                "product_match_state": "not_applicable",
                                "status": "invalid", "message": str(exc)})
        identities = {}
        for index, row in enumerate(normalized, 1):
            key = (row["carrier"], row["departure"])
            prior = identities.get(key)
            if prior is not None and prior != row:
                invalid.append({
                    "source_row_ref": f"row-{index}",
                    "normalized_fields": _iso(row),
                    "product_match_state": "not_applicable",
                    "status": "invalid",
                    "message": "ambiguous sailing identity inside this candidate",
                })
            else:
                identities[key] = row
        if input_mode == "direct" and len(raw_rows) != 1:
            invalid.append({"source_row_ref": "candidate",
                            "normalized_fields": None,
                            "product_match_state": "not_applicable",
                            "status": "invalid",
                            "message": "direct sailing entry accepts exactly one row"})
        return normalized, presented + invalid, entered_via

    @staticmethod
    def _row_values(feed, row):
        fields = [x.rstrip("?") for x in sn.FEED_COLUMNS[feed]
                  if x.rstrip("?") != "product_ref"]
        return {field: _iso(row.get(field)) for field in fields
                if row.get(field) is not None}

    def _replacement_effect(self, engine, feed, normalized, as_of):
        semantics = ("replace_by_sailing" if feed == "sailing_calendar"
                     else "full_snapshot" if feed in FULL_SNAPSHOT_FEEDS
                     else "replace_by_row")
        current = engine.state.feeds.get(feed)
        effect = {"semantics": semantics,
                  "prior_snapshot_as_of": _iso(current["as_of"]) if current else None,
                  "candidate_as_of": as_of.isoformat()}
        if feed == "sailing_calendar":
            before = {(s.carrier, s.departure): s
                      for s in engine.state.sailings.values()}
            diffs, added, changed = [], 0, 0
            for row in normalized:
                current_sailing = before.get((row["carrier"], row["departure"]))
                after = {"carrier": row["carrier"], "name": row["name"],
                         "departure": _iso(row["departure"]),
                         "voyage_days": row.get("voyage_days")}
                if current_sailing is None:
                    added += 1
                    diffs.append({"sailing_id": None, "before": None,
                                  "after": after, "change": "added"})
                    continue
                before_fields = {"carrier": current_sailing.carrier,
                                 "name": current_sailing.name,
                                 "departure": _iso(current_sailing.departure),
                                 "voyage_days": current_sailing.voyage_days}
                if before_fields != after:
                    changed += 1
                    diffs.append({"sailing_id": current_sailing.sailing_id,
                                  "before": before_fields, "after": after,
                                  "change": "changed"})
            effect["counts"] = {"added": added, "changed": changed,
                                "omitted": 0}
            effect["diffs"] = diffs
            return effect
        if feed not in FULL_SNAPSHOT_FEEDS:
            effect["counts"] = {"added": len(normalized), "changed": 0,
                                "omitted": 0}
            effect["diffs"] = []
            return effect
        before_rows = [row for row in engine._rows(engine.state, feed)
                       if row.get("product_id") and row.get("confident_match")]
        after_rows = [row for row in normalized
                      if row.get("product_id") and row.get("confident_match")]
        def identity(row):
            if feed == "in_transit":
                return (row["product_id"], str(row.get("reference") or ""))
            if feed == "committed_orders":
                return (row["product_id"], _iso(row.get("due_date")))
            if feed == "production_planning":
                return (row["product_id"], str(row.get("production_ref") or ""))
            return (row["product_id"],)
        before = {identity(row): row for row in before_rows}
        after = {identity(row): row for row in after_rows}
        effect["row_counts"] = {"prior": len(before_rows),
                                "candidate": len(after_rows)}
        multi_line = (len({row["product_id"] for row in before_rows}) < len(before_rows)
                      or len({row["product_id"] for row in after_rows}) < len(after_rows))
        diffs, added, changed, omitted = [], 0, 0, 0
        for row_key in sorted(set(before) | set(after)):
            b, a = before.get(row_key), after.get(row_key)
            bval = self._row_values(feed, b) if b else None
            aval = self._row_values(feed, a) if a else None
            if b is None:
                kind, added = "added", added + 1
            elif a is None:
                kind, omitted = "omitted", omitted + 1
            elif bval != aval:
                kind, changed = "changed", changed + 1
            else:
                continue
            diff = {"product_id": row_key[0], "before": bval,
                    "after": aval, "change": kind}
            if multi_line:
                diff["row_identity"] = "|".join(str(part) for part in row_key)
            diffs.append(diff)
        effect["counts"] = {"added": added, "changed": changed,
                            "omitted": omitted}
        effect["diffs"] = diffs
        return effect

    def preview(self, engine, *, actor: str, feed: str, input_mode: str,
                as_of, rows=None, text=None, raw_source_ref=None,
                _preview_id=None, _server_parsed=False) -> dict:
        if actor != "ashley":
            raise PreviewDenied("input preview unavailable for actor")
        if input_mode not in ("direct", "paste", "upload"):
            raise PreviewDenied("input_mode must be direct, paste, or upload")
        as_of_date = self._parse_as_of(as_of)
        if feed == "sailing_calendar":
            normalized, output_rows, entered_via = self._sailing_rows(
                engine, input_mode, rows, text)
            command = ("RecordSailing" if input_mode == "direct"
                       else "ImportSailingCalendarText")
            if input_mode == "direct" and normalized:
                row = normalized[0]
                params = {k: row[k] for k in ("carrier", "name", "departure")}
                if row.get("voyage_days") is not None:
                    params["voyage_days"] = row["voyage_days"]
                params.update({"as_of": as_of_date,
                               "raw_source_ref": raw_source_ref})
            else:
                params = {"text": text or "", "as_of": as_of_date,
                          "raw_source_ref": raw_source_ref}
        elif feed in FEED_COMMANDS:
            normalized, output_rows, entered_via = self._quantity_rows(
                engine, feed, input_mode, rows, text,
                server_parsed=_server_parsed)
            command = FEED_COMMANDS[feed]
            params = ({"as_of": as_of_date, "rows": copy.deepcopy(normalized)}
                      if input_mode == "direct" or _server_parsed
                      else {"as_of": as_of_date, "text": text or ""})
            params["raw_source_ref"] = raw_source_ref
        else:
            raise PreviewDenied(f"unsupported preview feed {feed}")
        counts = {name: sum(row["status"] == name for row in output_rows)
                  for name in ("valid", "held", "invalid")}
        summary = {"received": sum(counts.values()), **counts}
        has_authoritative_rows = (counts["valid"] > 0 if feed in FULL_SNAPSHOT_FEEDS
                                  else summary["received"] > 0)
        can_apply = counts["invalid"] == 0 and has_authoritative_rows
        head = len(engine.state.events)
        preview_id = _preview_id or "preview-" + secrets.token_urlsafe(12)
        prior = self._previews.get(preview_id)
        if prior and prior.get("apply_token"):
            self._candidates.pop(prior["apply_token"], None)
        replacement = self._replacement_effect(engine, feed, normalized,
                                                as_of_date)
        token = None
        action = None
        candidate = {"preview_id": preview_id, "feed": feed,
                     "normalized": normalized, "expected_head_seq": head,
                     "command": command, "params": params, "actor": actor,
                     "engine_ref": id(engine),
                     "input_mode": input_mode, "entered_via": entered_via,
                     "as_of": as_of_date, "raw_source_ref": raw_source_ref,
                     "summary": summary}
        if can_apply:
            # Validate the exact command/params before issuing capability.
            validate_command(command, copy.deepcopy(params), engine=engine)
            candidate["digest"] = self._digest(candidate)
            token = secrets.token_urlsafe(32)
            self._candidates[token] = candidate
            action = {"command": command, "fixed_params": _iso(params),
                      "expected_head_seq": head}
        self._previews[preview_id] = {
            "engine_ref": id(engine), "actor": actor, "feed": feed,
            "input_mode": input_mode, "as_of": as_of_date,
            "rows": copy.deepcopy(rows), "text": text,
            "raw_source_ref": raw_source_ref, "apply_token": token,
        }
        open_drafts = [p.plan_id for p in engine.state.plans.values()
                       if p.lifecycle == "draft"]
        return {
            "preview_id": preview_id, "feed": feed, "input_mode": input_mode,
            "entered_via": entered_via, "as_of": as_of_date.isoformat(),
            "raw_source_ref": raw_source_ref, "expected_head_seq": head,
            "rows": output_rows, "summary": summary,
            "replacement_effect": replacement,
            "downstream_impact": {"open_drafts_affected": open_drafts,
                                  "recommendations_recomputed": bool(open_drafts),
                                  "selected_m2_preserved": True},
            "can_apply": can_apply, "legal_apply_action": action,
            "apply_token": token,
        }

    def revise(self, engine, *, actor: str, preview_id: str,
               corrections: dict) -> dict:
        """Re-normalize corrections against a server-carried source draft.

        This is still preview work: no command runs and no domain state changes.
        The original paste/upload/direct rows never need to be supplied again.
        """
        draft = self._previews.get(preview_id)
        if draft is None:
            raise PreviewDenied("unknown or expired preview")
        if draft["actor"] != actor or draft["engine_ref"] != id(engine):
            raise PreviewDenied("preview binding mismatch")
        if not isinstance(corrections, dict) or not corrections:
            raise PreviewDenied("at least one row correction is required")

        rows = copy.deepcopy(draft["rows"])
        text = draft["text"]
        if draft["input_mode"] == "direct":
            rows = list(rows or [])
            for ref, replacement in corrections.items():
                try:
                    index = int(str(ref).replace("row-", "")) - 1
                except ValueError as exc:
                    raise PreviewDenied(f"invalid correction row {ref}") from exc
                if index < 0 or index >= len(rows) or not isinstance(replacement, dict):
                    raise PreviewDenied(f"invalid correction row {ref}")
                rows[index] = replacement
        else:
            lines = (text or "").splitlines()
            for ref, replacement in corrections.items():
                raw_ref = str(ref).replace("row-", "").replace("line ", "")
                try:
                    index = int(raw_ref) - 1
                except ValueError as exc:
                    raise PreviewDenied(f"invalid correction row {ref}") from exc
                if index < 0 or index >= len(lines) or not isinstance(replacement, str):
                    raise PreviewDenied(f"invalid correction row {ref}")
                lines[index] = replacement
            text = "\n".join(lines)
        return self.preview(
            engine, actor=actor, feed=draft["feed"],
            input_mode=draft["input_mode"], as_of=draft["as_of"],
            rows=rows, text=text, raw_source_ref=draft["raw_source_ref"],
            _preview_id=preview_id)

    def apply(self, engine, *, actor: str, apply_token: str) -> dict:
        candidate = self._candidates.pop(apply_token, None)
        if candidate is None:
            raise PreviewDenied("unknown or already-used apply token")
        if candidate["actor"] != actor:
            raise PreviewDenied("apply token actor mismatch")
        if candidate["engine_ref"] != id(engine):
            raise PreviewDenied("apply token engine mismatch")
        if candidate["digest"] != self._digest({k: v for k, v in candidate.items()
                                                if k != "digest"}):
            raise PreviewDenied("apply token candidate digest mismatch")
        if len(engine.state.events) != candidate["expected_head_seq"]:
            raise PreviewDenied("stale apply token head")
        command = candidate["command"]
        if command not in engine.bus.commands_available_to(actor):
            raise PreviewDenied("apply command is no longer legal")
        params = validate_command(command, copy.deepcopy(candidate["params"]),
                                  engine=engine)
        before_feed = engine.state.feeds.get(candidate["feed"])
        before_sailing_ids = set(engine.state.sailings)
        before_sailings = {
            sid: (sl.carrier, sl.name, sl.departure, sl.voyage_days, sl.entered_via)
            for sid, sl in engine.state.sailings.items()
        }
        before_recomputes = len(engine.state.recomputes)
        before_events = len(engine.state.events)
        before_implications = set(engine.state.implications)
        before_lines = {pid: copy.deepcopy(plan.lines)
                        for pid, plan in engine.state.plans.items()}
        token = engine.identity.token_for(actor)
        result = engine.execute(command, params, token=token)
        self._previews.pop(candidate["preview_id"], None)
        new_events = engine.state.events[before_events:]
        refs = []
        for key in ("snapshot_id", "sailing_id"):
            if result.get(key):
                refs.append(result[key])
        refs.extend(result.get("sailing_ids", []))
        current_effects = []
        if candidate["feed"] in engine.state.feeds:
            current = engine.state.feeds[candidate["feed"]]
            previous_ref = before_feed.get("snapshot_id") if before_feed else None
            if current["snapshot_id"] != previous_ref:
                current_effects.append({
                    "scope_ref": candidate["feed"],
                    "previous_current_ref": previous_ref,
                    "current_ref": current["snapshot_id"],
                })
        elif candidate["feed"] == "sailing_calendar":
            for ref in refs:
                current = engine.state.sailings[ref]
                current_sig = (current.carrier, current.name, current.departure,
                               current.voyage_days, current.entered_via)
                if before_sailings.get(ref) != current_sig:
                    current_effects.append({"scope_ref": ref,
                                            "previous_current_ref": (ref if ref in before_sailing_ids
                                                                     else None),
                                            "current_ref": ref})
        recompute_events = [event for event in new_events
                            if event.type == "plan_recomputed"]
        affected = []
        for event in recompute_events:
            payload = thaw(event.payload)
            affected.append({
                "plan_id": payload["plan_id"],
                "recompute_ref": f"event:{event.seq}",
                "recommendation_delta_summary": {
                    "before": _iso(payload.get("before", {}).get("suggestions", {})),
                    "after": _iso(payload.get("after", {}).get("suggestions", {})),
                },
            })
        # Defensive parity with the accepted recompute projection.
        if len(engine.state.recomputes) - before_recomputes != len(recompute_events):
            raise RuntimeError("recompute event/projection parity failure")
        selected_preserved = all(engine.state.plans[pid].lines == lines
                                 for pid, lines in before_lines.items()
                                 if pid in engine.state.plans)
        opened = sorted(set(engine.state.implications) - before_implications)
        return {
            "feed": candidate["feed"], "input_mode": candidate["input_mode"],
            "entered_via": candidate["entered_via"],
            "as_of": candidate["as_of"].isoformat(),
            "applied_at": engine.today.isoformat(),
            "accepted_rows": candidate["summary"]["valid"],
            "held_rows": candidate["summary"]["held"], "rejected_rows": 0,
            "applied_observation_refs": refs,
            "current_effects": current_effects,
            "affected_open_drafts": affected,
            "selected_m2_preserved": selected_preserved,
            "implications_opened": opened,
        }
