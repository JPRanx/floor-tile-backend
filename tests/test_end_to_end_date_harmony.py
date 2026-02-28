"""
End-to-end date harmony tests.

Validates that dates are consistently respected across:
- Forward Simulation (SIESA transport, production pipeline, in-transit arrival gates)
- Order Builder (availability breakdown, factory_available_m2, selection logic)
- Planning View (urgency thresholds, deadline displays)
- API response contract (all date fields present)

Uses "The March Fleet" scenario: 3 boats, 5 products, known dates.
TODAY = 2026-03-01 (frozen).
"""

import math
import pytest
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

# =====================
# SCENARIO CONSTANTS
# =====================

TODAY = date(2026, 3, 1)

# Factory
FACTORY = {
    "id": "factory-001",
    "name": "CI Ceramics",
    "origin_port": "Barranquilla",
    "transport_to_port_days": 5,
    "production_lead_days": 35,
    "active": True,
}
TRANSPORT_TO_PORT = 5

# Boats (sorted by departure)
BOAT_A = {
    "id": "boat-a",
    "vessel_name": "Vessel Alpha",
    "departure_date": "2026-03-10",
    "arrival_date": "2026-03-19",
    "origin_port": "Barranquilla",
    "carrier": "TIBA",
    "shipping_line": "TIBA",
    "status": "available",
    "max_containers": 5,
}
BOAT_B = {
    "id": "boat-b",
    "vessel_name": "Vessel Beta",
    "departure_date": "2026-03-20",
    "arrival_date": "2026-03-29",
    "origin_port": "Barranquilla",
    "carrier": "SEABOARD",
    "shipping_line": "SEABOARD",
    "status": "available",
    "max_containers": 5,
}
BOAT_C = {
    "id": "boat-c",
    "vessel_name": "Vessel Gamma",
    "departure_date": "2026-04-05",
    "arrival_date": "2026-04-14",
    "origin_port": "Barranquilla",
    "carrier": "TIBA",
    "shipping_line": "TIBA",
    "status": "available",
    "max_containers": 5,
}

# Product IDs
PID_SIESA_URGENT = "pid-siesa-urgent"
PID_SIESA_OK = "pid-siesa-ok"
PID_NO_SIESA = "pid-no-siesa"
PID_PRODUCTION = "pid-production"
PID_INTRANSIT = "pid-intransit"

# Products (active tiles for CI Ceramics)
PRODUCTS = [
    {"id": PID_SIESA_URGENT, "sku": "ALMENDRO URGENTE", "active": True},
    {"id": PID_SIESA_OK, "sku": "CEIBA BIEN", "active": True},
    {"id": PID_NO_SIESA, "sku": "NOGAL SIN SIESA", "active": True},
    {"id": PID_PRODUCTION, "sku": "TOLU PRODUCCION", "active": True},
    {"id": PID_INTRANSIT, "sku": "ROBLE TRANSITO", "active": True},
]

# Initial warehouse stock (m²)
WAREHOUSE_STOCK = {
    PID_SIESA_URGENT: Decimal("200"),
    PID_SIESA_OK: Decimal("2000"),
    PID_NO_SIESA: Decimal("100"),
    PID_PRODUCTION: Decimal("300"),
    PID_INTRANSIT: Decimal("150"),
}

# Factory SIESA finished goods (m²)
FACTORY_SIESA = {
    PID_SIESA_URGENT: Decimal("3000"),
    PID_SIESA_OK: Decimal("1500"),
    PID_NO_SIESA: Decimal("0"),
    PID_PRODUCTION: Decimal("0"),
    PID_INTRANSIT: Decimal("500"),
}

# Daily velocity (m²/day, 90-day average)
VELOCITIES = {
    PID_SIESA_URGENT: Decimal("30"),
    PID_SIESA_OK: Decimal("20"),
    PID_NO_SIESA: Decimal("25"),
    PID_PRODUCTION: Decimal("15"),
    PID_INTRANSIT: Decimal("20"),
}

# Production pipeline: TOLU PRODUCCION completing Mar 8
PRODUCTION_PIPELINE = {
    PID_PRODUCTION: [
        {
            "id": "prod-row-1",
            "product_id": PID_PRODUCTION,
            "status": "in_progress",
            "requested_m2": 2000,
            "completed_m2": 0,
            "estimated_delivery_date": "2026-03-08",
        }
    ],
}

# In-transit: ROBLE TRANSITO arriving Mar 15 from earlier ordered draft
IN_TRANSIT_DRAFTS = {
    PID_INTRANSIT: [
        {
            "arrival_date": "2026-03-15",
            "pallets_m2": Decimal("1000"),
        }
    ],
}

# Constants from config/shipping.py
WAREHOUSE_BUFFER_DAYS = 6
ORDERING_CYCLE_DAYS = 30
M2_PER_PALLET = Decimal("134.4")

# Constants from models/boat_schedule.py
ORDER_DEADLINE_DAYS = 20


# =====================
# HELPERS
# =====================

def _simulate_fleet(boats, drafts_map=None):
    """
    Simulate the full fleet through _simulate_boat with proper cascade.

    Returns list of per-boat projection dicts, with mutable state
    cascading across boats (SIESA consumed, production consumed, stock updates).
    """
    from services.forward_simulation_service import ForwardSimulationService

    with patch("services.forward_simulation_service.get_supabase_client"):
        svc = ForwardSimulationService()

    # Mutable cascade state
    current_stock = deepcopy(WAREHOUSE_STOCK)
    factory_siesa_consumed: set[str] = set()
    production_consumed: set[str] = set()
    in_transit = deepcopy(IN_TRANSIT_DRAFTS)

    results = []
    for boat in boats:
        proj = svc._simulate_boat(
            boat=boat,
            factory=FACTORY,
            products=PRODUCTS,
            current_stock=current_stock,
            velocity_map=VELOCITIES,
            drafts_map=drafts_map or {},
            today=TODAY,
            factory_siesa_map=FACTORY_SIESA,
            factory_siesa_consumed=factory_siesa_consumed,
            production_pipeline=deepcopy(PRODUCTION_PIPELINE),
            production_consumed=production_consumed,
            in_transit_drafts=in_transit,
        )
        results.append(proj)

    return results


def _find_product(projection, pid):
    """Find a product_details entry by product_id in a boat projection."""
    for pd in projection["product_details"]:
        if pd["product_id"] == pid:
            return pd
    return None


# =====================
# GROUP 1: FORWARD SIMULATION DATE GATES
# =====================


class TestForwardSimulationDateGates:
    """
    Tests that the forward simulation correctly gates supply events
    based on date arithmetic (SIESA transport, production pipeline,
    in-transit arrival).
    """

    def test_siesa_transport_gate(self):
        """
        SIESA available when departure >= today + transport_to_port.

        factory_ready_by = Mar 1 + 5 = Mar 6.
        Boat A departs Mar 10 >= Mar 6 → SIESA available on Boat A.
        Boat B should NOT get SIESA (consumed by Boat A).
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])
        boat_a, boat_b, boat_c = projections

        # Boat A gets SIESA for products that have it
        pa_urgent = _find_product(boat_a, PID_SIESA_URGENT)
        assert pa_urgent["supply_breakdown"]["factory_siesa_m2"] == 3000.0

        pa_ok = _find_product(boat_a, PID_SIESA_OK)
        assert pa_ok["supply_breakdown"]["factory_siesa_m2"] == 1500.0

        pa_intransit = _find_product(boat_a, PID_INTRANSIT)
        assert pa_intransit["supply_breakdown"]["factory_siesa_m2"] == 500.0

        # Boat A does NOT get SIESA for zero-SIESA products
        pa_no_siesa = _find_product(boat_a, PID_NO_SIESA)
        assert pa_no_siesa["supply_breakdown"]["factory_siesa_m2"] == 0.0

        pa_prod = _find_product(boat_a, PID_PRODUCTION)
        assert pa_prod["supply_breakdown"]["factory_siesa_m2"] == 0.0

        # Boat B gets NO SIESA (consumed by Boat A)
        pb_urgent = _find_product(boat_b, PID_SIESA_URGENT)
        assert pb_urgent["supply_breakdown"]["factory_siesa_m2"] == 0.0

        pb_ok = _find_product(boat_b, PID_SIESA_OK)
        assert pb_ok["supply_breakdown"]["factory_siesa_m2"] == 0.0

    def test_production_pipeline_gate(self):
        """
        Production available when est_delivery + transport <= departure.

        PRODUCT_PRODUCTION: delivery Mar 8, prod_ready_by = Mar 8 + 5 = Mar 13.
        Boat A departs Mar 10 < Mar 13 → production NOT available.
        Boat B departs Mar 20 >= Mar 13 → production available.
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])
        boat_a, boat_b = projections[0], projections[1]

        # Boat A: production NOT ready (Mar 13 > Mar 10)
        pa = _find_product(boat_a, PID_PRODUCTION)
        assert pa["supply_breakdown"]["production_pipeline_m2"] == 0.0

        # Boat B: production IS ready (Mar 13 <= Mar 20)
        pb = _find_product(boat_b, PID_PRODUCTION)
        assert pb["supply_breakdown"]["production_pipeline_m2"] == 2000.0

    def test_in_transit_arrival_gate(self):
        """
        In-transit available when entry_arrival + WAREHOUSE_BUFFER <= departure.

        PRODUCT_INTRANSIT: arrives Mar 15, warehouse_ready = Mar 15 + 6 = Mar 21.
        Boat A departs Mar 10 < Mar 21 → NOT available.
        Boat B departs Mar 20 < Mar 21 → NOT available.
        Boat C departs Apr 5 >= Mar 21 → available.
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])
        boat_a, boat_b, boat_c = projections

        pa = _find_product(boat_a, PID_INTRANSIT)
        assert pa["supply_breakdown"]["in_transit_m2"] == 0.0

        pb = _find_product(boat_b, PID_INTRANSIT)
        assert pb["supply_breakdown"]["in_transit_m2"] == 0.0

        pc = _find_product(boat_c, PID_INTRANSIT)
        assert pc["supply_breakdown"]["in_transit_m2"] == 1000.0

    def test_cascade_correct_depletion(self):
        """
        Cascade uses effective_stock (before depletion) so each boat
        independently depletes from today. No double-counting.

        PID_NO_SIESA: warehouse=100, velocity=25, no supply.
        Boat A: effective=100, projected=100-25×24=-500, fills 14 pallets.
        Cascade: current_stock = effective(100) + filled(1881.6) = 1981.6
        Boat B: effective=1981.6, projected=1981.6-25×34=1131.6

        Old buggy code would give: 1381.6-25×34=531.6 (double-depleted).
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B])
        boat_a, boat_b = projections

        # Boat A for NO_SIESA: warehouse=100, no supply
        pa_a = _find_product(boat_a, PID_NO_SIESA)
        days_a = (date(2026, 3, 19) - TODAY).days + WAREHOUSE_BUFFER_DAYS  # 24
        assert pa_a["projected_stock_m2"] == pytest.approx(100.0 - 25.0 * days_a, abs=1.0)  # -500

        # Boat A fills 14 pallets (ceil(1850/134.4))
        assert pa_a["suggested_pallets"] == 14
        filled_a = 14 * float(M2_PER_PALLET)  # 1881.6

        # Boat B should use effective(100 + 1881.6) = 1981.6 as starting stock
        pa_b = _find_product(boat_b, PID_NO_SIESA)
        days_b = (date(2026, 3, 29) - TODAY).days + WAREHOUSE_BUFFER_DAYS  # 34
        expected_projected_b = (100.0 + filled_a) - 25.0 * days_b  # 1981.6 - 850 = 1131.6
        assert pa_b["projected_stock_m2"] == pytest.approx(expected_projected_b, abs=1.0)

        # Verify the value is HIGHER than the old buggy value would have been
        # Old buggy: (100 - 25*24 + 1881.6) - 25*34 = 531.6
        buggy_value = (100.0 - 25.0 * days_a + filled_a) - 25.0 * days_b
        assert pa_b["projected_stock_m2"] > buggy_value + 100  # significantly higher

    def test_cascade_no_fill_preserves_effective(self):
        """
        When suggested_pallets=0, cascade preserves effective_stock.

        PID_SIESA_URGENT on Boat A: warehouse=200, siesa=3000.
        effective=3200, projected=2480, suggested=0 (no gap).
        Cascade: current_stock = effective(3200), not projected(2480).

        Boat B: stock=3200 (no SIESA, consumed), projected=3200-30×34=2180.
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B])
        boat_a, boat_b = projections

        pa_a = _find_product(boat_a, PID_SIESA_URGENT)
        assert pa_a["suggested_pallets"] == 0  # No gap, no fill needed

        # Boat B for SIESA_URGENT: starts with effective 3200 (not projected 2480)
        pa_b = _find_product(boat_b, PID_SIESA_URGENT)
        days_b = (date(2026, 3, 29) - TODAY).days + WAREHOUSE_BUFFER_DAYS  # 34
        # stock=3200 (cascaded effective), siesa=0 (consumed), effective=3200
        expected_b = 3200.0 - 30.0 * days_b  # 3200 - 1020 = 2180
        assert pa_b["projected_stock_m2"] == pytest.approx(expected_b, abs=1.0)

    def test_days_of_stock_at_arrival(self):
        """
        days_of_stock = projected_stock / daily_velocity.

        For SIESA_URGENT on Boat A:
        - days_until_arrival = (Mar 19 - Mar 1) + 6 = 24
        - effective_stock = 200 + 3000 = 3200
        - projected_stock = 3200 - (30 × 24) = 2480
        - days_of_stock = 2480 / 30 ≈ 82.7
        """
        projections = _simulate_fleet([BOAT_A])
        pa = _find_product(projections[0], PID_SIESA_URGENT)

        # Verify projected_stock matches expected calculation
        days_until = (date(2026, 3, 19) - TODAY).days + WAREHOUSE_BUFFER_DAYS  # 24
        effective = 200.0 + 3000.0  # warehouse + SIESA
        expected_projected = effective - (30.0 * days_until)
        assert pa["projected_stock_m2"] == pytest.approx(expected_projected, abs=1.0)

        # Verify days_of_stock
        expected_days = expected_projected / 30.0
        assert pa["days_of_stock_at_arrival"] == pytest.approx(expected_days, abs=0.5)

    def test_boat_level_flags(self):
        """
        Boat-level flags correctly reflect supply presence.

        Boat A has SIESA supply → has_factory_siesa_supply = True.
        Boat A has NO production supply → has_production_supply = False.
        Boat A has NO in-transit supply → has_in_transit_supply = False.
        Boat B has production supply → has_production_supply = True.
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])
        boat_a, boat_b, boat_c = projections

        # Boat A: has SIESA, no production, no in-transit
        assert boat_a["has_factory_siesa_supply"] is True
        assert boat_a["has_production_supply"] is False
        assert boat_a["has_in_transit_supply"] is False

        # Boat B: no SIESA (consumed), has production, no in-transit
        assert boat_b["has_factory_siesa_supply"] is False
        assert boat_b["has_production_supply"] is True
        assert boat_b["has_in_transit_supply"] is False

        # Boat C: no SIESA, no production (consumed), has in-transit
        assert boat_c["has_factory_siesa_supply"] is False
        assert boat_c["has_production_supply"] is False
        assert boat_c["has_in_transit_supply"] is True


# =====================
# GROUP 2: ORDER BUILDER DATE GATES
# =====================


class TestOrderBuilderDateGates:
    """
    Tests that the Order Builder correctly uses dates for
    availability breakdown and factory fill status.
    """

    @staticmethod
    def _make_ob_service():
        """Create OB service with all constructor dependencies mocked."""
        with patch("services.order_builder_service.get_boat_schedule_service"), \
             patch("services.order_builder_service.get_recommendation_service"), \
             patch("services.order_builder_service.get_inventory_service"), \
             patch("services.order_builder_service.get_trend_service"), \
             patch("services.order_builder_service.get_customer_pattern_service"), \
             patch("services.order_builder_service.get_production_schedule_service"), \
             patch("services.order_builder_service.get_warehouse_order_service"):
            from services.order_builder_service import OrderBuilderService
            return OrderBuilderService()

    def test_availability_production_gate_before_deadline(self):
        """
        Production NOT counted when estimated_ready > order_deadline.

        PRODUCT_PRODUCTION: ready Mar 8.
        If order_deadline = Feb 28 (< Mar 8) → production_completing = 0.
        """
        svc = self._make_ob_service()

        result = svc._calculate_availability_breakdown(
            factory_available_m2=Decimal("0"),
            production_status="in_progress",
            production_requested_m2=Decimal("2000"),
            production_completed_m2=Decimal("0"),
            production_estimated_ready=date(2026, 3, 8),
            order_deadline=date(2026, 2, 28),  # Before production ready
            suggested_pallets=10,
        )

        assert result.production_completing_m2 == Decimal("0")
        assert result.siesa_now_m2 == Decimal("0")
        assert result.total_available_m2 == Decimal("0")
        assert result.can_fulfill is False

    def test_availability_production_gate_after_deadline(self):
        """
        Production IS counted when estimated_ready <= order_deadline.

        PRODUCT_PRODUCTION: ready Mar 8.
        If order_deadline = Mar 16 (>= Mar 8) → production_completing = 2000.
        """
        svc = self._make_ob_service()

        result = svc._calculate_availability_breakdown(
            factory_available_m2=Decimal("0"),
            production_status="in_progress",
            production_requested_m2=Decimal("2000"),
            production_completed_m2=Decimal("0"),
            production_estimated_ready=date(2026, 3, 8),
            order_deadline=date(2026, 3, 16),  # After production ready
            suggested_pallets=10,
        )

        assert result.production_completing_m2 == Decimal("2000")
        assert result.total_available_m2 == Decimal("2000")
        assert result.can_fulfill is True  # 2000 >= 10 * 134.4 = 1344

    def test_availability_siesa_only_no_production(self):
        """
        SIESA stock correctly reflected without production.

        PRODUCT_SIESA_URGENT: SIESA=3000, no production.
        """
        svc = self._make_ob_service()

        result = svc._calculate_availability_breakdown(
            factory_available_m2=Decimal("3000"),
            production_status="not_scheduled",
            production_requested_m2=Decimal("0"),
            production_completed_m2=Decimal("0"),
            production_estimated_ready=None,
            order_deadline=date(2026, 3, 12),
            suggested_pallets=5,
        )

        assert result.siesa_now_m2 == Decimal("3000")
        assert result.production_completing_m2 == Decimal("0")
        assert result.total_available_m2 == Decimal("3000")
        assert result.can_fulfill is True

    def test_availability_no_stock_at_all(self):
        """
        Product with zero SIESA and no production → can_fulfill = False.

        PRODUCT_NO_SIESA: SIESA=0, no production.
        """
        svc = self._make_ob_service()

        result = svc._calculate_availability_breakdown(
            factory_available_m2=Decimal("0"),
            production_status="not_scheduled",
            production_requested_m2=Decimal("0"),
            production_completed_m2=Decimal("0"),
            production_estimated_ready=None,
            order_deadline=date(2026, 3, 12),
            suggested_pallets=5,
        )

        assert result.siesa_now_m2 == Decimal("0")
        assert result.total_available_m2 == Decimal("0")
        assert result.can_fulfill is False
        assert result.shortfall_m2 == Decimal("5") * M2_PER_PALLET

    def test_completed_production_not_double_counted(self):
        """
        Completed production is already in SIESA — should NOT be added again.

        If status=completed, production_completing_m2 should be 0.
        The completed m² is already in factory_available_m2.
        """
        svc = self._make_ob_service()

        result = svc._calculate_availability_breakdown(
            factory_available_m2=Decimal("2000"),  # SIESA includes completed production
            production_status="completed",
            production_requested_m2=Decimal("2000"),
            production_completed_m2=Decimal("2000"),
            production_estimated_ready=date(2026, 2, 20),  # Already done
            order_deadline=date(2026, 3, 12),
            suggested_pallets=5,
        )

        # Completed production should NOT be added on top of SIESA
        assert result.production_completing_m2 == Decimal("0")
        assert result.total_available_m2 == Decimal("2000")


# =====================
# GROUP 3: PLANNING VIEW DATE GATES
# =====================


class TestPlanningViewDateGates:
    """
    Tests that urgency labels and deadline displays are correct
    in the planning horizon projections.
    """

    def test_urgency_thresholds(self):
        """
        Urgency classification based on days_of_stock_at_arrival.

        < 7 days → critical
        7-14 days → urgent
        14-30 days → soon
        >= 30 days → ok
        """
        projections = _simulate_fleet([BOAT_A])
        boat_a = projections[0]

        # Check each product's urgency
        for pd in boat_a["product_details"]:
            days = pd["days_of_stock_at_arrival"]
            urgency = pd["urgency"]

            if days < 7:
                assert urgency == "critical", f"{pd['sku']}: {days}d → expected critical, got {urgency}"
            elif days < 14:
                assert urgency == "urgent", f"{pd['sku']}: {days}d → expected urgent, got {urgency}"
            elif days < 30:
                assert urgency == "soon", f"{pd['sku']}: {days}d → expected soon, got {urgency}"
            else:
                assert urgency == "ok", f"{pd['sku']}: {days}d → expected ok, got {urgency}"

    def test_deadline_display_values(self):
        """
        Deadline dates computed correctly from departure date.

        Boat A (departs Mar 10, CI factory: transport=5, production_lead=35):
        - siesa_order_date = Mar 10 - 20 = Feb 18
        - shipping_book_by = Mar 10 - 5 = Mar 5
        - order_by_date (factory) = Mar 10 - (35+5) = Jan 29
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])
        boat_a, boat_b, boat_c = projections

        # Boat A deadlines
        assert boat_a["siesa_order_date"] == "2026-02-18"
        assert boat_a["shipping_book_by_date"] == "2026-03-05"
        assert boat_a["order_by_date"] == "2026-01-29"

        # Boat A days_until (from TODAY = Mar 1)
        assert boat_a["days_until_siesa_deadline"] == (date(2026, 2, 18) - TODAY).days  # -11
        assert boat_a["days_until_shipping_deadline"] == (date(2026, 3, 5) - TODAY).days  # 4
        assert boat_a["days_until_order_deadline"] == (date(2026, 1, 29) - TODAY).days  # -31

        # Boat B deadlines
        assert boat_b["siesa_order_date"] == "2026-02-28"
        assert boat_b["shipping_book_by_date"] == "2026-03-15"

        # Boat B days_until_siesa_deadline is negative (overdue)
        assert boat_b["days_until_siesa_deadline"] < 0

        # Boat C: SIESA deadline is in the future
        assert boat_c["siesa_order_date"] == "2026-03-16"
        assert boat_c["days_until_siesa_deadline"] == (date(2026, 3, 16) - TODAY).days  # 15
        assert boat_c["days_until_siesa_deadline"] > 0

    def test_days_until_departure(self):
        """days_until_departure = (departure - today).days."""
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])

        assert projections[0]["days_until_departure"] == (date(2026, 3, 10) - TODAY).days  # 9
        assert projections[1]["days_until_departure"] == (date(2026, 3, 20) - TODAY).days  # 19
        assert projections[2]["days_until_departure"] == (date(2026, 4, 5) - TODAY).days  # 35


# =====================
# GROUP 4: API RESPONSE CONTRACT
# =====================


class TestAPIResponseContract:
    """
    Tests that the response structure contains all required date
    fields that the frontend expects.
    """

    def test_boat_projection_has_all_date_fields(self):
        """
        Each boat projection must include all deadline and date fields.
        """
        projections = _simulate_fleet([BOAT_A])
        boat = projections[0]

        required_fields = [
            "departure_date",
            "arrival_date",
            "days_until_departure",
            "order_by_date",
            "days_until_order_deadline",
            "shipping_book_by_date",
            "days_until_shipping_deadline",
            "siesa_order_date",
            "days_until_siesa_deadline",
            "production_request_date",
            "days_until_production_deadline",
        ]

        for field in required_fields:
            assert field in boat, f"Missing field: {field}"

    def test_product_detail_has_all_fields(self):
        """
        Each product_details entry must include projection and supply fields.
        """
        projections = _simulate_fleet([BOAT_A])
        pd = projections[0]["product_details"][0]

        required_fields = [
            "product_id",
            "sku",
            "daily_velocity_m2",
            "current_stock_m2",
            "projected_stock_m2",
            "days_of_stock_at_arrival",
            "days_of_stock_after_fill",
            "urgency",
            "coverage_gap_m2",
            "suggested_pallets",
            "supply_breakdown",
            "is_draft_committed",
        ]

        for field in required_fields:
            assert field in pd, f"Missing product field: {field}"

        # supply_breakdown sub-fields
        sb_fields = [
            "warehouse_m2",
            "factory_siesa_m2",
            "production_pipeline_m2",
            "in_transit_m2",
        ]

        for field in sb_fields:
            assert field in pd["supply_breakdown"], f"Missing supply_breakdown field: {field}"

    def test_boat_level_supply_flags(self):
        """
        Boat-level summary flags must exist.
        """
        projections = _simulate_fleet([BOAT_A])
        boat = projections[0]

        required_flags = [
            "has_factory_siesa_supply",
            "has_production_supply",
            "has_in_transit_supply",
            "factory_siesa_total_m2",
            "production_total_m2",
            "in_transit_total_m2",
            "urgency_breakdown",
            "confidence",
            "projected_pallets_min",
            "projected_pallets_max",
        ]

        for field in required_flags:
            assert field in boat, f"Missing boat flag: {field}"

    def test_projection_for_boat_contract(self):
        """
        get_projection_for_boat returns dict with expected per-product keys.
        """
        from services.forward_simulation_service import ForwardSimulationService

        with patch("services.forward_simulation_service.get_supabase_client"):
            svc = ForwardSimulationService()

        # Build a minimal horizon response
        projections = _simulate_fleet([BOAT_A, BOAT_B])

        horizon = {
            "factory_id": "factory-001",
            "factory_name": "CI Ceramics",
            "horizon_months": 3,
            "generated_at": TODAY.isoformat(),
            "projections": projections,
            "factory_order_signal": None,
        }

        with patch.object(svc, "get_planning_horizon", return_value=horizon):
            result = svc.get_projection_for_boat("factory-001", "boat-b")

        assert result is not None
        assert set(result.keys()) == {
            PID_SIESA_URGENT, PID_SIESA_OK, PID_NO_SIESA,
            PID_PRODUCTION, PID_INTRANSIT,
        }

        # Each product entry has the right keys
        for pid, data in result.items():
            assert "projected_stock_m2" in data
            assert "daily_velocity_m2" in data
            assert "supply_breakdown" in data
            assert "earlier_drafts_consumed_m2" in data
            assert "coverage_gap_m2" in data
            assert "days_of_stock_at_arrival" in data

            # Values are Decimal where expected
            assert isinstance(data["projected_stock_m2"], Decimal)
            assert isinstance(data["daily_velocity_m2"], Decimal)
            assert isinstance(data["earlier_drafts_consumed_m2"], Decimal)


# =====================
# GROUP 5: CROSS-SYSTEM CONSISTENCY
# =====================


class TestCrossSystemConsistency:
    """
    Tests that values computed in one system are consistent
    when consumed by another.
    """

    def test_cascade_earlier_consumed_matches_warehouse_delta(self):
        """
        get_projection_for_boat's earlier_drafts_consumed_m2 should equal
        the warehouse delta between first boat and target boat.
        """
        from services.forward_simulation_service import ForwardSimulationService

        with patch("services.forward_simulation_service.get_supabase_client"):
            svc = ForwardSimulationService()

        projections = _simulate_fleet([BOAT_A, BOAT_B])

        horizon = {
            "factory_id": "factory-001",
            "factory_name": "CI Ceramics",
            "horizon_months": 3,
            "generated_at": TODAY.isoformat(),
            "projections": projections,
            "factory_order_signal": None,
        }

        with patch.object(svc, "get_planning_horizon", return_value=horizon):
            result = svc.get_projection_for_boat("factory-001", "boat-b")

        assert result is not None

        # For each product, earlier_consumed = boat_a_warehouse - boat_b_warehouse
        for pid in [PID_SIESA_URGENT, PID_SIESA_OK, PID_NO_SIESA, PID_PRODUCTION, PID_INTRANSIT]:
            boat_a_warehouse = Decimal(str(
                _find_product(projections[0], pid)["supply_breakdown"]["warehouse_m2"]
            ))
            boat_b_warehouse = result[pid]["supply_breakdown"]["warehouse_m2"]
            expected_consumed = max(Decimal("0"), boat_a_warehouse - boat_b_warehouse)

            assert result[pid]["earlier_drafts_consumed_m2"] == expected_consumed, (
                f"{pid}: expected consumed={expected_consumed}, "
                f"got={result[pid]['earlier_drafts_consumed_m2']}"
            )

    def test_urgency_counts_match_product_urgencies(self):
        """
        Boat-level urgency_breakdown counts must match actual product urgencies.
        """
        projections = _simulate_fleet([BOAT_A])
        boat_a = projections[0]

        # Count urgencies from product_details
        actual_counts = {"critical": 0, "urgent": 0, "soon": 0, "ok": 0}
        for pd in boat_a["product_details"]:
            actual_counts[pd["urgency"]] += 1

        # Compare with boat-level breakdown
        for urgency in ["critical", "urgent", "soon", "ok"]:
            assert boat_a["urgency_breakdown"][urgency] == actual_counts[urgency], (
                f"Mismatch for {urgency}: "
                f"breakdown={boat_a['urgency_breakdown'][urgency]}, "
                f"actual={actual_counts[urgency]}"
            )

    def test_supply_totals_match_product_details(self):
        """
        Boat-level SIESA/production/in-transit totals must equal
        sum of product-level supply breakdowns.
        """
        projections = _simulate_fleet([BOAT_A, BOAT_B, BOAT_C])

        for proj in projections:
            siesa_sum = sum(
                pd["supply_breakdown"]["factory_siesa_m2"]
                for pd in proj["product_details"]
            )
            prod_sum = sum(
                pd["supply_breakdown"]["production_pipeline_m2"]
                for pd in proj["product_details"]
            )
            transit_sum = sum(
                pd["supply_breakdown"]["in_transit_m2"]
                for pd in proj["product_details"]
            )

            assert proj["factory_siesa_total_m2"] == pytest.approx(siesa_sum, abs=0.01), (
                f"Boat {proj['boat_name']}: SIESA total mismatch"
            )
            assert proj["production_total_m2"] == pytest.approx(prod_sum, abs=0.01), (
                f"Boat {proj['boat_name']}: production total mismatch"
            )
            assert proj["in_transit_total_m2"] == pytest.approx(transit_sum, abs=0.01), (
                f"Boat {proj['boat_name']}: in-transit total mismatch"
            )

    def test_no_siesa_product_urgency_is_critical(self):
        """
        PRODUCT_NO_SIESA has warehouse=100, velocity=25.
        Without any supply, it burns through stock in 4 days.
        On Boat A: projected stock should be deeply negative → critical.
        """
        projections = _simulate_fleet([BOAT_A])
        pd = _find_product(projections[0], PID_NO_SIESA)

        # 100m² warehouse, 25m²/day, 24 days → projected = 100 - 600 = -500
        assert pd["projected_stock_m2"] < 0
        assert pd["urgency"] == "critical"
        assert pd["days_of_stock_at_arrival"] < 7

    def test_siesa_ok_product_urgency_is_ok(self):
        """
        PRODUCT_SIESA_OK has warehouse=2000, siesa=1500, velocity=20.
        On Boat A: effective=3500, depletion=480, projected=3020 → ok.
        """
        projections = _simulate_fleet([BOAT_A])
        pd = _find_product(projections[0], PID_SIESA_OK)

        assert pd["projected_stock_m2"] > 0
        assert pd["urgency"] == "ok"
        assert pd["days_of_stock_at_arrival"] >= 30
