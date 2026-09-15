"""R3 — Reconciled spec §7: command surface, server-derived identity,
authorization denial before append, atomicity, validation rules.

RED-first for `clean_v1/sailing_commands.py` + `clean_v1/sailing_engine.py`.
"""
from datetime import date
from decimal import Decimal as D

import pytest

from clean_v1.config import PlanningConfig
from clean_v1.sailing_commands import AuthorizationError, CommandError
from clean_v1.sailing_engine import SailingEngine
from clean_v1 import sailing_domain as sd

TODAY = date(2026, 8, 3)

PRODUCTS = [
    {"product_id": "TILE-A", "sku": "TILE-A", "name": "Piso Alfa",
     "tier": "B", "aliases": []},
    {"product_id": "TILE-B", "sku": "TILE-B", "name": "Piso Beta",
     "tier": "B", "aliases": []},
]

TOKENS = {"tok-ashley": "ashley", "tok-elicio": "elicio",
          "tok-other": "warehouse_clerk"}


def make_engine(today=TODAY):
    cfg = PlanningConfig(default_voyage_days=15)
    return SailingEngine(config=cfg, today=today, products=PRODUCTS,
                         sessions=TOKENS)


def seed_basic(eng):
    eng.ashley("LoadSalesSnapshot", {
        "as_of": TODAY,
        "rows": [{"product_ref": "TILE-A", "daily_velocity": D("10"),
                  "peak_weekly_m2": None},
                 {"product_ref": "TILE-B", "daily_velocity": D("8"),
                  "peak_weekly_m2": None}]})
    eng.ashley("LoadWarehouseSnapshot", {
        "as_of": TODAY,
        "rows": [{"product_ref": "TILE-A", "m2": D("800")},
                 {"product_ref": "TILE-B", "m2": D("500")}]})
    eng.ashley("LoadSiesaSnapshot", {
        "as_of": TODAY,
        "rows": [{"product_ref": "TILE-A", "available_m2": D("2000"),
                  "committed_m2": D("0")},
                 {"product_ref": "TILE-B", "available_m2": D("1500"),
                  "committed_m2": D("0")}]})
    eng.ashley("RecordSailing", {"carrier": "CMA-SYN", "name": "S1",
                                 "departure": date(2026, 8, 30),
                                 "voyage_days": 15})
    eng.ashley("RecordSailing", {"carrier": "CMA-SYN", "name": "S2",
                                 "departure": date(2026, 9, 13),
                                 "voyage_days": 15})
    return eng


def open_s1_plan(eng):
    sid = next(s.sailing_id for s in eng.state.sailings.values()
               if s.name == "S1")
    eng.ashley("SetSailingDecision", {"sailing_id": sid, "decision": "use"})
    r = eng.ashley("OpenShipmentPlan", {"sailing_id": sid})
    return sid, r["plan_id"]


# ── identity / authorization ────────────────────────────────────────────────

def test_unknown_token_denied_before_any_append():
    eng = make_engine()
    before = sd.snapshot(eng.state)
    with pytest.raises(AuthorizationError):
        eng.execute("RecordSailing",
                    {"carrier": "C", "name": "S", "departure": TODAY},
                    token="tok-forged")
    assert sd.snapshot(eng.state) == before


def test_payload_identity_rejected():
    eng = make_engine()
    with pytest.raises(CommandError, match="identity"):
        eng.execute("RecordSailing",
                    {"carrier": "C", "name": "S",
                     "departure": date(2026, 9, 1), "actor": "elicio"},
                    token="tok-ashley")


def test_elicio_only_commands_denied_to_ashley_before_append():
    eng = seed_basic(make_engine())
    before = sd.snapshot(eng.state)
    for cmd in ("CorrectObservedCommitment", "CorrectExpectation"):
        with pytest.raises(AuthorizationError):
            eng.execute(cmd, {"commitment_id": "COM1", "exp_id": "EXP1",
                              "action": "release", "note": "x"},
                        token="tok-ashley")
        with pytest.raises(AuthorizationError):
            eng.execute(cmd, {"commitment_id": "COM1", "exp_id": "EXP1",
                              "action": "release", "note": "x"},
                        token="tok-other")
    assert sd.snapshot(eng.state) == before


def test_other_identity_denied_planning_commands():
    eng = seed_basic(make_engine())
    with pytest.raises(AuthorizationError):
        eng.execute("RecordSailing",
                    {"carrier": "C", "name": "S",
                     "departure": date(2026, 9, 1)},
                    token="tok-other")


# ── atomicity ───────────────────────────────────────────────────────────────

def test_failed_command_rolls_back_everything():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    before = sd.snapshot(eng.state)
    with pytest.raises(CommandError):
        eng.ashley("EditSelectedM2", {"plan_id": plan,
                                      "product_id": "TILE-A",
                                      "m2": D("100.00")})   # invalid increment
    assert sd.snapshot(eng.state) == before


# ── planning command surface ────────────────────────────────────────────────

def test_accept_suggestion_snapshots_server_side():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    r = eng.ashley("AcceptSuggestion", {"plan_id": plan,
                                        "product_id": "TILE-A"})
    line = eng.state.plans[plan].lines["TILE-A"]
    assert line["origin"] == "suggestion"
    assert line["snapshot_ref"] in eng.state.suggestions
    # Dynamic tier arithmetic: window 62d → need 492.00 → suggested 537.60
    assert line["selected_m2"] == D("537.60")
    assert r["selected_m2"] == D("537.60")


def test_manual_add_above_availability_raises_consequential_implication():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    eng.ashley("AddManualLine", {"plan_id": plan, "product_id": "TILE-B",
                                 "m2": D("2016.00")})   # > 1500 available
    cases = [i for i in eng.state.implications.values()
             if i.state == "open" and i.family == "decision_required"
             and i.scope.get("product_id") == "TILE-B"]
    assert len(cases) == 1 and cases[0].severity == "consequential"
    assert cases[0].shipment_effect        # exact effect present


def test_finalize_produces_handoff_and_default_bl_split():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    r = eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    assert eng.state.plans[plan].lifecycle == "finalized"
    hof = eng.state.handoffs[r["handoff_id"]]["handoff"]
    assert hof["orders"][0]["lines"][0]["m2"] == D("537.60")
    assert eng.state.bl_splits[plan]["origin"] in ("default", "override")


def test_finalize_denied_when_edit_rejected_after():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    with pytest.raises(CommandError, match="finalized"):
        eng.ashley("EditSelectedM2", {"plan_id": plan,
                                      "product_id": "TILE-A",
                                      "m2": D("134.40")})


def test_bl_override_conservation_enforced():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    eng.ashley("EditSelectedM2", {"plan_id": plan, "product_id": "TILE-A",
                                  "m2": D("2688.00")})   # 20 pallets → 2 ctr
    bad = [{"bl_no": 1, "container_count": 2,
            "lines": [{"product_id": "TILE-A", "m2": D("2620.80")}]}]
    with pytest.raises(CommandError, match="conservation"):
        eng.ashley("OverrideBLSplit", {"plan_id": plan, "groups": bad})


def test_exceptional_pursuit_flow():
    eng = seed_basic(make_engine())
    eng.ashley("RecordSailing", {"carrier": "CMA-SYN", "name": "SX",
                                 "departure": date(2026, 8, 10),
                                 "voyage_days": 15})
    sx = next(s.sailing_id for s in eng.state.sailings.values()
              if s.name == "SX")
    with pytest.raises(CommandError, match="exceptional"):
        eng.ashley("SetSailingDecision", {"sailing_id": sx,
                                          "decision": "use"})
    eng.ashley("PursueExceptionalSailing", {"sailing_id": sx})
    eng.ashley("SetSailingDecision", {"sailing_id": sx, "decision": "use"})
    # pursuit is itself a risk-bearing consequential implication
    cases = [i for i in eng.state.implications.values()
             if i.state == "open" and i.scope.get("sailing_id") == sx
             and i.family == "decision_required"]
    assert len(cases) == 1


def test_request_reallocation_opens_family4_case_then_reopen():
    eng = seed_basic(make_engine())
    _, plan = open_s1_plan(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    hid = eng.ashley("FinalizeSailingPlan", {"plan_id": plan})["handoff_id"]
    eng.ashley("RequestReallocation", {"plan_id": plan})
    case = next(i for i in eng.state.implications.values()
                if i.state == "open" and i.family == "reallocation_intent")
    eng.ashley("ResolveImplication", {"implication_id": case.implication_id,
                                      "action": "confirm_reopen",
                                      "params": {}})
    assert eng.state.plans[plan].lifecycle == "draft"
    assert eng.state.handoffs[hid]["superseded"] is True
