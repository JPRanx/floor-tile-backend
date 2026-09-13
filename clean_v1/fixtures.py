"""
Synthetic demo fixtures for the I6A visible workspace.

EVERYTHING here is invented demo data (product names, quantities, dates).
DEFAULT_VOYAGE_DAYS = 15 is the §13 synthetic fixture value ONLY — the real
operating default is unresolved (U3, gate CLOSED). No real feeds, no real
records, no persistence.

The dataset is built by replaying real command/evidence flows through the
engine, so every number on the demo surface has genuine lifecycle provenance:
a July cycle with confirmed / no-production / awaiting lines, transit and
produced evidence, a partial arrival, a carry-forward offer, an amendment
opportunity, an unknown transit reference, and the open August cycle.
"""

from datetime import date
from decimal import Decimal

from .config import PlanningConfig
from .reconciliation import Engine

D = Decimal

TODAY = date(2026, 8, 21)
CFG = PlanningConfig(default_voyage_days=15)   # synthetic fixture value (U3)

PRODUCTS = [
    {"id": "natura-60", "sku": "NATURA BEIGE 60X60", "name": "Natura Beige 60×60", "tier": "B"},
    {"id": "roble-20", "sku": "ROBLE MIEL 20X120", "name": "Roble Miel 20×120", "tier": "B"},
    {"id": "terra-45", "sku": "TERRA COTTO 45X45", "name": "Terra Cotto 45×45", "tier": "B"},
    {"id": "marmol-30", "sku": "MÁRMOL CARRARA 30X60", "name": "Mármol Carrara 30×60", "tier": "A"},
    {"id": "piedra-45", "sku": "PIEDRA GRIS 45X45", "name": "Piedra Gris 45×45", "tier": "C"},
    {"id": "kreta-60", "sku": "KRETA ARENA 60X60", "name": "Kreta Arena 60×60", "tier": "A"},
    {"id": "azul-exp", "sku": "AZUL EXPORT 33X33", "name": "Azul Export 33×33", "tier": "C"},
]

VELOCITIES = {
    "natura-60": D("10.00"),
    "roble-20": D("8.00"),
    "terra-45": D("6.00"),
    "marmol-30": D("14.00"),
    "piedra-45": D("4.00"),
    "kreta-60": D("12.00"),
    "azul-exp": D("0"),
}
PEAKS = {"marmol-30": D("21.00"), "kreta-60": D("24.00")}

JULY_WAREHOUSE = {
    "natura-60": D("1100.00"),
    "roble-20": D("1400.00"),
    "terra-45": D("650.00"),
    "marmol-30": D("2200.00"),
    "piedra-45": D("520.00"),
    "kreta-60": D("640.00"),
    "azul-exp": D("80.00"),
}

AUGUST_WAREHOUSE = {
    "natura-60": D("800.00"),      # S1 arithmetic: suggestion 470.40
    "roble-20": D("1310.40"),      # July: 1400 − 289.60 sold + 200.00 arrived
    "terra-45": D("470.00"),
    "marmol-30": D("1780.00"),
    "piedra-45": D("400.00"),
    "kreta-60": D("210.00"),       # stockout risk, no planned response
    "azul-exp": D("80.00"),        # zero velocity below buffer floor → watch
}
AUGUST_SALES_SINCE_JULY = {
    "natura-60": D("300.00"),
    "roble-20": D("289.60"),
    "terra-45": D("180.00"),
    "marmol-30": D("420.00"),
    "piedra-45": D("120.00"),
    "kreta-60": D("430.00"),
    "azul-exp": D("0"),
}


def build_demo_engine() -> Engine:
    eng = Engine(config=CFG, today=date(2026, 7, 22), products=PRODUCTS,
                 velocities=VELOCITIES, peak_velocities=PEAKS)

    # ── July evidence baseline ────────────────────────────────────────────
    eng.load_warehouse_snapshot(as_of=date(2026, 7, 20), values=JULY_WAREHOUSE)
    eng.load_transit_snapshot(as_of=date(2026, 7, 20), rows=[])
    eng.load_sales_snapshot(as_of=date(2026, 7, 19))
    eng.load_siesa_snapshot(as_of=date(2026, 7, 18), values={
        "natura-60": D("2688.00"), "roble-20": D("940.80"),
        "terra-45": D("403.20"), "marmol-30": D("1344.00"),
        "kreta-60": D("0.00")})

    # ── July cycle: decisions → submission → factory outcomes ─────────────
    oid7 = eng.open_cycle("2026-07", on=date(2026, 7, 22))
    eng.ashley("EditSelectedM2", {"order_id": oid7, "product_id": "roble-20", "m2": "470.40"})
    eng.ashley("EditSelectedM2", {"order_id": oid7, "product_id": "terra-45", "m2": "134.40"})
    eng.ashley("EditSelectedM2", {"order_id": oid7, "product_id": "piedra-45", "m2": "201.60"})
    eng.ashley("SubmitMonthlyOrder", {"order_id": oid7})

    lines7 = {l.product_id: l for l in eng.state.lines.values()
              if l.order_id == oid7}
    eng.today = date(2026, 7, 24)
    # factory answered Ashley outside the app; Elicio records administratively
    eng.elicio("RecordFactoryOutcome",
               {"line_id": lines7["roble-20"].line_id,
                "outcome": "factory_confirmed", "confirmed_m2": "470.40"})
    eng.elicio("RecordFactoryOutcome",
               {"line_id": lines7["terra-45"].line_id, "outcome": "no_production",
                "note": "no producción — molde en mantenimiento"})
    # piedra-45 stays awaiting_factory (calm overdue context for Jorge/Elicio)

    # ── Production planning (soft context) + produced evidence ────────────
    eng.load_production_planning(rows=[
        {"orden_produccion": "OP-2026-118", "product_id": "roble-20",
         "status": "completed", "requested_m2": D("470.40"),
         "completed_m2": D("470.40"), "scheduled_start_date": date(2026, 7, 26),
         "estimated_delivery_date": date(2026, 8, 4), "can_add_more": False},
        {"orden_produccion": "OP-2026-131", "product_id": "marmol-30",
         "status": "scheduled", "requested_m2": D("806.40"),
         "completed_m2": D("0"), "scheduled_start_date": date(2026, 9, 8),
         "estimated_delivery_date": date(2026, 10, 2), "can_add_more": True},
        {"orden_produccion": "OP-2026-127", "product_id": "natura-60",
         "status": "in_progress", "requested_m2": D("268.80"),
         "completed_m2": D("134.40"), "scheduled_start_date": date(2026, 8, 12),
         "estimated_delivery_date": date(2026, 9, 6), "can_add_more": False},
    ])
    eng.today = date(2026, 8, 5)
    eng.load_produced_evidence(as_of=date(2026, 8, 5), rows=[
        {"product_id": "roble-20", "m2": D("470.40"),
         "produced_on": date(2026, 8, 4)}])       # quiet verification → produced

    # ── Transit evidence: roble moving; one unknown reference ─────────────
    eng.today = date(2026, 8, 18)
    eng.load_transit_snapshot(as_of=date(2026, 8, 18), rows=[
        {"product_id": "roble-20", "m2": D("470.40"),
         "event_date": date(2026, 8, 10), "eta": date(2026, 8, 30),
         "reference": None, "confident": True},
        {"raw_reference": "PISO NUEVO 51X51 CJ", "m2": D("134.40"),
         "event_date": date(2026, 8, 12), "confident": False,
         "candidates": [{"product_id": "terra-45", "score": 0.58},
                        {"product_id": "natura-60", "score": 0.41}]},
    ])

    # ── August warehouse snapshot: partial arrival of roble (200 of 470.40)
    eng.today = date(2026, 8, 20)
    eng.load_warehouse_snapshot(as_of=date(2026, 8, 20), values=AUGUST_WAREHOUSE,
                                sales_since_prev=AUGUST_SALES_SINCE_JULY)
    eng.load_sales_snapshot(as_of=date(2026, 8, 19))
    eng.load_siesa_snapshot(as_of=date(2026, 8, 19), values={
        "natura-60": D("2419.20"), "roble-20": D("134.40"),
        "terra-45": D("537.60"), "marmol-30": D("1075.20"),
        "kreta-60": D("0.00")})

    # optional committed-orders snapshot (U10 present-branch)
    eng.load_committed_orders(as_of=date(2026, 8, 19), rows=[
        {"product_id": "kreta-60", "m2": D("268.80"),
         "due_date": date(2026, 9, 12)}])

    # ── August cycle opens; proactive checks run ──────────────────────────
    oid8 = eng.open_cycle("2026-08", on=TODAY)
    eng.run_checks()
    return eng
