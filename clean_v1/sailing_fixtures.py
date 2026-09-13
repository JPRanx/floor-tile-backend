"""
Reconciled V1 — synthetic demo world and dev scenario driver (local proof
only; fixture values are NOT product decisions — default_voyage_days = 15
is the §13 synthetic injection, U3 stays closed).

Every scenario is built by replaying real commands through the same
boundary the UI uses — fixture mechanics never bypass the engine.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal as D

from .config import PlanningConfig
from .sailing_engine import SailingEngine

TODAY = date(2026, 8, 3)

PRODUCTS = [
    {"product_id": "TILE-A", "siesa_item": 5465,
     "sku": "ALMENDRO BEIGE BTE", "name": "ALMENDRO BEIGE BTE",
     "category": "MADERAS", "rotation": "BAJA", "tier": "B", "aliases": [],
     "source_unit": "m2", "weight_kg": None, "length_cm": None,
     "width_cm": None, "height_cm": None, "units_per_pallet": None,
     "m2_per_pallet": "134.40", "edit_increment_m2": "67.20"},
    {"product_id": "TILE-B", "siesa_item": 5468,
     "sku": "ALMENDRO GRIS BTE", "name": "ALMENDRO GRIS BTE",
     "category": "MADERAS", "rotation": "BAJA", "tier": "B", "aliases": [],
     "source_unit": "m2", "weight_kg": None, "length_cm": None,
     "width_cm": None, "height_cm": None, "units_per_pallet": None,
     "m2_per_pallet": "134.40", "edit_increment_m2": "67.20"},
    {"product_id": "TILE-C", "siesa_item": 5492,
     "sku": "CEIBA CAFE BTE", "name": "CEIBA CAFE BTE",
     "category": "MADERAS", "rotation": "ALTA", "tier": "A", "aliases": [],
     "source_unit": "m2", "weight_kg": None, "length_cm": None,
     "width_cm": None, "height_cm": None, "units_per_pallet": None,
     "m2_per_pallet": "134.40", "edit_increment_m2": "67.20"},
    {"product_id": "TILE-D", "siesa_item": 5540,
     "sku": "NOGAL CAFE BTE", "name": "NOGAL CAFE BTE",
     "category": "MADERAS", "rotation": "ALTA", "tier": "A", "aliases": [],
     "source_unit": "m2", "weight_kg": None, "length_cm": None,
     "width_cm": None, "height_cm": None, "units_per_pallet": None,
     "m2_per_pallet": "134.40", "edit_increment_m2": "67.20"},
    {"product_id": "TILE-E", "siesa_item": 5633,
     "sku": "TOLU GRIS", "name": "TOLU GRIS", "category": "MADERAS",
     "rotation": "ALTA", "tier": "A", "aliases": [], "source_unit": "m2",
     "weight_kg": None, "length_cm": None, "width_cm": None,
     "height_cm": None, "units_per_pallet": None,
     "m2_per_pallet": "134.40", "edit_increment_m2": "67.20"},
]

VESSEL_NAMES = {
    "S1": "SEABOARD GALAXI", "S2": "SEABOARD PIONEER 2",
    "SL": "SEABOARD PIONEER 3", "SX": "SEABOARD PRIDE",
}

SESSIONS = {"tok-ashley": "ashley", "tok-elicio": "elicio"}


def base_engine(today=TODAY) -> SailingEngine:
    return SailingEngine(config=PlanningConfig(default_voyage_days=15),
                         today=today, products=PRODUCTS, sessions=SESSIONS)


def seed_feeds(eng, *, siesa=None, warehouse=None, sales=None,
               siesa_as_of=None):
    sales = sales or {"TILE-A": "10", "TILE-B": "12", "TILE-C": "6",
                      "TILE-D": "4", "TILE-E": "0"}
    warehouse = warehouse or {"TILE-A": "800", "TILE-B": "100",
                              "TILE-C": "600", "TILE-D": "700",
                              "TILE-E": "50"}
    siesa = siesa or {"TILE-A": "2000", "TILE-B": "3000", "TILE-C": "2600",
                      "TILE-D": "1000", "TILE-E": "500"}
    eng.ashley("LoadSalesSnapshot", {"as_of": eng.today, "rows": [
        {"product_ref": k, "daily_velocity": D(v), "peak_weekly_m2": None}
        for k, v in sales.items()]})
    eng.ashley("LoadWarehouseSnapshot", {"as_of": eng.today, "rows": [
        {"product_ref": k, "m2": D(v)} for k, v in warehouse.items()]})
    eng.ashley("LoadSiesaSnapshot", {
        "as_of": siesa_as_of or eng.today,
        "rows": [{"product_ref": k, "available_m2": D(v),
                  "committed_m2": D("0")} for k, v in siesa.items()]})
    return eng


def seed_sailings(eng, include_late=True, include_exceptional=True):
    eng.ashley("ImportSailingCalendarText", {"text": (
        "SEABOARD\tSEABOARD GALAXI\t2026-08-30\t15\n"
        "SEABOARD\tSEABOARD PIONEER 2\t2026-09-13\t15\n")})
    if include_late:
        eng.ashley("RecordSailing", {"carrier": "SEABOARD", "name": VESSEL_NAMES["SL"],
                                     "departure": date(2026, 8, 18),
                                     "voyage_days": 15})
    if include_exceptional:
        eng.ashley("RecordSailing", {"carrier": "SEABOARD", "name": VESSEL_NAMES["SX"],
                                     "departure": date(2026, 8, 10),
                                     "voyage_days": 15})
    return eng


def sid_of(eng, name):
    name = VESSEL_NAMES.get(name, name)
    return next(s.sailing_id for s in eng.state.sailings.values()
                if s.name == name)


def use_and_open(eng, name="S1"):
    sid = sid_of(eng, name)
    eng.ashley("SetSailingDecision", {"sailing_id": sid, "decision": "use"})
    plan = eng.ashley("OpenShipmentPlan", {"sailing_id": sid})["plan_id"]
    return sid, plan


def _finalize_a1(eng):
    sid, plan = use_and_open(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    eng.ashley("EditSelectedM2", {"plan_id": plan, "product_id": "TILE-B",
                                  "m2": D("2688.00")})
    eng.ashley("AddManualLine", {"plan_id": plan, "product_id": "TILE-C",
                                 "m2": D("2486.40")})
    return sid, plan


def _commit_and_observe(eng, plan, siesa_ref="PV-9001",
                        rows_after=None):
    order_ref = eng.state.handoffs[eng.state.plans[plan].handoff_id][
        "handoff"]["orders"][0]["order_ref"]
    cid = eng.ashley("RecordSiesaOrderReference",
                     {"handoff_order_ref": order_ref,
                      "siesa_ref": siesa_ref})["commitment_id"]
    com = eng.state.commitments[cid]
    if rows_after is None:
        # build a reflecting snapshot from the current one
        rows_after = []
        committed = {q["product_id"]: D(str(q["m2"]))
                     for q in com.quantities}
        for row in eng.state.feeds["siesa_availability"]["rows"]:
            pid = row.get("product_id")
            avail = D(str(row["available_m2"]))
            c = committed.get(pid, D("0"))
            rows_after.append({"product_ref": pid,
                               "available_m2": avail - c,
                               "committed_m2": c})
    eng.set_today(date(2026, 8, 5))
    eng.ashley("LoadSiesaSnapshot", {"as_of": date(2026, 8, 5),
                                     "rows": rows_after})
    return cid


# ── dev scenario driver (unlinked /dev route only) ─────────────────────────

def scenario_default() -> SailingEngine:
    """Home demo: rolling calendar, S1 in use with an open draft plan."""
    eng = seed_sailings(seed_feeds(base_engine()))
    sid, plan = use_and_open(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    return eng


def scenario_a1() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine()))
    _finalize_a1(eng)
    return eng


def scenario_a2() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine(), siesa={
        "TILE-A": "2000", "TILE-B": "806.40", "TILE-C": "2600",
        "TILE-D": "1000", "TILE-E": "500"}), include_late=False,
        include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("EditSelectedM2", {"plan_id": plan, "product_id": "TILE-B",
                                  "m2": D("672.00")})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    use_and_open(eng, "S2")
    return eng


def scenario_a3() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine(),
                                   siesa_as_of=date(2026, 6, 19)),
                        include_late=False, include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("EditSelectedM2", {"plan_id": plan, "product_id": "TILE-A",
                                  "m2": D("268.80")})
    return eng


def scenario_a4() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine()))
    sid, plan = use_and_open(eng, "SL")
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    return eng


def scenario_a5() -> SailingEngine:
    """[FC4] a plan and an OPEN pursuit risk case coexist only through the
    crossing path: the draft opened while normal, the sailing crossed the
    hard cutoff, and Ashley recorded the pursuit — its risk decision is
    still hers to accept (visible, unaccepted, gating advancement)."""
    eng = seed_sailings(seed_feeds(base_engine()))
    sid, plan = use_and_open(eng)             # S1, normal at 08-03
    eng.ashley("EditSelectedM2", {"plan_id": plan, "product_id": "TILE-A",
                                  "m2": D("134.40")})
    eng.set_today(date(2026, 8, 20))          # S1 crosses the hard cutoff
    eng.run_checks()
    eng.ashley("PursueExceptionalSailing", {"sailing_id": sid})
    return eng


def scenario_a6() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine()))
    sid, plan = _finalize_a1(eng)
    eng.ashley("OverrideBLSplit", {"plan_id": plan, "groups": [
        {"bl_no": 1, "container_count": 2,
         "lines": [{"product_id": "TILE-B", "m2": D("2688.00")},
                   {"product_id": "TILE-A", "m2": D("268.80")},
                   {"product_id": "TILE-C", "m2": D("537.60")}]},
        {"bl_no": 2, "container_count": 2,
         "lines": [{"product_id": "TILE-C", "m2": D("1948.80")}]},
    ]})
    return eng


def scenario_a7() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine()), include_late=False,
                        include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    _commit_and_observe(eng, plan)
    return eng


def scenario_a8() -> SailingEngine:
    eng = scenario_a7()
    plan = next(p.plan_id for p in eng.state.plans.values())
    cid = next(iter(eng.state.commitments))
    sid = eng.state.plans[plan].sailing_id
    eng.ashley("RecordBookingConfirmation",
               {"booking_ref": "BK-77", "sailing_id": sid,
                "commitment_refs": [cid]})
    eng.set_today(date(2026, 9, 1))
    eng.ashley("LoadTransitSnapshot", {"as_of": date(2026, 9, 1), "rows": [
        {"product_ref": "TILE-A", "m2": D("268.80"), "reference": "BK-77",
         "eta": date(2026, 9, 14)}]})
    return eng


def scenario_a8_arrival() -> SailingEngine:
    eng = scenario_a8()
    eng.set_today(date(2026, 9, 21))
    eng.ashley("LoadWarehouseSnapshot", {"as_of": date(2026, 9, 21),
                                         "rows": [
        {"product_ref": "TILE-A", "m2": D("934.40")},
        {"product_ref": "TILE-B", "m2": D("100")},
        {"product_ref": "TILE-C", "m2": D("600")},
        {"product_ref": "TILE-D", "m2": D("700")},
        {"product_ref": "TILE-E", "m2": D("50")}]})
    return eng


def scenario_a9() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine(), warehouse={
        "TILE-A": "800", "TILE-B": "200", "TILE-C": "600",
        "TILE-D": "700", "TILE-E": "50"}), include_late=False,
        include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("EditSelectedM2", {"plan_id": plan, "product_id": "TILE-B",
                                  "m2": D("672.00")})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    cid = _commit_and_observe(eng, plan, "PV-9002")
    eng.ashley("RecordBookingConfirmation",
               {"booking_ref": "BK-88", "sailing_id": sid,
                "commitment_refs": [cid]})
    eng.set_today(date(2026, 9, 1))
    eng.ashley("LoadTransitSnapshot", {"as_of": date(2026, 9, 1), "rows": [
        {"product_ref": "TILE-B", "m2": D("268.80"), "reference": "BK-88",
         "eta": date(2026, 9, 14)}]})
    return eng


def scenario_a10() -> SailingEngine:
    eng = scenario_a7()
    plan = next(p.plan_id for p in eng.state.plans.values())
    cid = next(iter(eng.state.commitments))
    sid = eng.state.plans[plan].sailing_id
    eng.ashley("RecordBookingConfirmation",
               {"booking_ref": "BK-99", "sailing_id": sid,
                "commitment_refs": [cid]})
    eng.set_today(date(2026, 9, 1))
    eng.ashley("LoadTransitSnapshot", {"as_of": date(2026, 9, 1), "rows": [
        {"product_ref": "TILE-A", "m2": D("201.60"), "reference": "BK-99",
         "eta": date(2026, 9, 14)}]})
    return eng


def scenario_a11() -> SailingEngine:
    eng = seed_sailings(seed_feeds(
        base_engine(),
        siesa={"TILE-A": "2000", "TILE-B": "3000", "TILE-C": "2600",
               "TILE-D": "403.20", "TILE-E": "500"},
        warehouse={"TILE-A": "800", "TILE-B": "100", "TILE-C": "600",
                   "TILE-D": "100", "TILE-E": "50"},
        sales={"TILE-A": "10", "TILE-B": "12", "TILE-C": "6",
               "TILE-D": "12", "TILE-E": "0"}), include_late=False,
        include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-D"})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    return eng


def scenario_a13() -> SailingEngine:
    eng = scenario_a7()
    plan = next(p.plan_id for p in eng.state.plans.values())
    cid = next(iter(eng.state.commitments))
    sid = eng.state.plans[plan].sailing_id
    eng.ashley("RecordBookingConfirmation",
               {"booking_ref": "BK-77", "sailing_id": sid,
                "commitment_refs": [cid]})
    eng.set_today(date(2026, 9, 1))
    eng.ashley("LoadTransitSnapshot", {"as_of": date(2026, 9, 1), "rows": [
        {"product_ref": "PISO-X 51X51", "m2": D("134.40"),
         "reference": None, "eta": None},
        {"product_ref": "TILE-B", "m2": D("134.40"),
         "reference": "PV-9001", "eta": None}]})
    return eng


def scenario_a14() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine()), include_late=False,
                        include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    eng.ashley("RequestReallocation", {"plan_id": plan})
    return eng


def scenario_a15() -> SailingEngine:
    eng = seed_sailings(seed_feeds(base_engine()), include_late=False,
                        include_exceptional=False)
    sid, plan = use_and_open(eng)
    eng.ashley("AcceptSuggestion", {"plan_id": plan, "product_id": "TILE-A"})
    eng.ashley("FinalizeSailingPlan", {"plan_id": plan})
    cid = _commit_and_observe(eng, plan)
    eng.set_today(date(2026, 8, 26))
    eng.run_checks()
    return eng


def _cycle_scenario(*, departure_days: int | None,
                    production: dict | None = None) -> SailingEngine:
    """Build an unlinked QA carrier that exercises the real cycle composer."""
    eng = scenario_default()
    breach = TODAY + timedelta(days=60)
    candidates = [] if departure_days is None else [{
        "sailing_id": "CYCLE-NEXT", "name": "SEABOARD FUTURO",
        "departure": TODAY + timedelta(days=departure_days),
        "warehouse_arrival": TODAY + timedelta(days=departure_days + 3),
        "timing_state": "normal", "selectable": True,
    }]
    eng.cycle_scenario_inputs = {
        "synthetic_cycle_only": True,
        "as_of": TODAY,
        "protection_rows": [{
            "product_id": "TILE-A", "warehouse_m2": "800.00",
            "dated_destination_events": [], "buffer_m2": "403.20",
            "daily_velocity_m2": "10.00", "breach_date": breach,
            "protected_through": None, "dip_recovery": None,
            "dip_reentry": None, "stockout_date": None, "reason": None,
        }],
        "current_shipment": {
            "sailing_id": "CYCLE-CURRENT", "name": "SEABOARD ACTUAL",
            "order_by": TODAY, "departure": TODAY + timedelta(days=5),
            "warehouse_arrival": TODAY + timedelta(days=10),
        },
        "candidate_sailings": candidates,
        "factory_history": [{
            "external_order_id": f"CYCLE-PV-{i}",
            "order_date": TODAY.isoformat(),
            "actual_ready_date": (TODAY + timedelta(days=lead)).isoformat(),
            "lifecycle_inert": True,
        } for i, lead in enumerate((5, 10, 15))],
        "current_production": production,
    }
    return eng


def scenario_cycle_monitor() -> SailingEngine:
    return _cycle_scenario(departure_days=55)


def scenario_cycle_prepare() -> SailingEngine:
    return _cycle_scenario(departure_days=43)


def scenario_cycle_order_now() -> SailingEngine:
    return _cycle_scenario(departure_days=40)


def scenario_cycle_at_risk() -> SailingEngine:
    return _cycle_scenario(departure_days=None)


def scenario_cycle_in_production() -> SailingEngine:
    return _cycle_scenario(departure_days=55, production={
        "production_order_id": "CYCLE-PROD", "production_ref": "PV-CYCLE",
        "order_date": TODAY, "production_status": "in_progress",
        "accepted_in_progress": True, "can_add_more": False,
    })


def scenario_cycle_ready() -> SailingEngine:
    return _cycle_scenario(departure_days=55, production={
        "production_order_id": "CYCLE-PROD", "production_ref": "PV-CYCLE",
        "order_date": TODAY, "production_status": "ready",
        "accepted_ready": True, "siesa_orderable": True, "can_add_more": False,
    })


def scenario_cycle_awaiting_siesa() -> SailingEngine:
    return _cycle_scenario(departure_days=55, production={
        "production_order_id": "CYCLE-PROD", "production_ref": "PV-CYCLE",
        "order_date": TODAY, "production_status": "ready",
        "accepted_ready": True, "siesa_orderable": False, "can_add_more": False,
    })


def scenario_cycle_can_add_more() -> SailingEngine:
    return _cycle_scenario(departure_days=55, production={
        "production_order_id": "CYCLE-PROD", "production_ref": "PV-CYCLE",
        "order_date": TODAY, "production_status": "scheduled",
        "can_add_more": True,
    })


SCENARIOS = {
    "default": scenario_default,
    "a1": scenario_a1, "a2": scenario_a2, "a3": scenario_a3,
    "a4": scenario_a4, "a5": scenario_a5, "a6": scenario_a6,
    "a7": scenario_a7, "a8": scenario_a8, "a8_arrival": scenario_a8_arrival,
    "a9": scenario_a9, "a10": scenario_a10, "a11": scenario_a11,
    "a13": scenario_a13, "a14": scenario_a14, "a15": scenario_a15,
    "cycle_monitor": scenario_cycle_monitor,
    "cycle_prepare": scenario_cycle_prepare,
    "cycle_order_now": scenario_cycle_order_now,
    "cycle_at_risk": scenario_cycle_at_risk,
    "cycle_in_production": scenario_cycle_in_production,
    "cycle_awaiting_siesa": scenario_cycle_awaiting_siesa,
    "cycle_ready": scenario_cycle_ready,
    "cycle_can_add_more": scenario_cycle_can_add_more,
}
