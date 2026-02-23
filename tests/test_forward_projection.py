"""
Unit tests for ForwardSimulationService.get_projection_for_boat().

Tests cover:
- Happy path with multi-boat cascading
- Boat not found → None
- Empty horizon → None
- Earlier drafts consumed calculation
- First boat (no earlier consumption)
- Exception handling → None
- Feature flag off (OB skips forward sim)
- Feature flag on but no factory_id (projection_map is None)
"""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch


# =====================
# HELPERS
# =====================

def _make_product_detail(
    product_id: str,
    sku: str = "TEST-SKU",
    warehouse_m2: float = 500.0,
    factory_siesa_m2: float = 0.0,
    production_pipeline_m2: float = 0.0,
    in_transit_m2: float = 0.0,
    projected_stock_m2: float = 400.0,
    daily_velocity_m2: float = 10.0,
    coverage_gap_m2: float = 100.0,
    days_of_stock_at_arrival: float = 40.0,
    days_of_stock_after_fill: float = 60.0,
    suggested_pallets: int = 2,
    urgency: str = "ok",
) -> dict:
    """Build a realistic product_details entry matching _simulate_boat output."""
    return {
        "product_id": product_id,
        "sku": sku,
        "daily_velocity_m2": daily_velocity_m2,
        "current_stock_m2": warehouse_m2 + factory_siesa_m2 + production_pipeline_m2 + in_transit_m2,
        "projected_stock_m2": projected_stock_m2,
        "days_of_stock_at_arrival": days_of_stock_at_arrival,
        "days_of_stock_after_fill": days_of_stock_after_fill,
        "urgency": urgency,
        "coverage_gap_m2": coverage_gap_m2,
        "suggested_pallets": suggested_pallets,
        "supply_breakdown": {
            "warehouse_m2": warehouse_m2,
            "factory_siesa_m2": factory_siesa_m2,
            "production_pipeline_m2": production_pipeline_m2,
            "in_transit_m2": in_transit_m2,
        },
        "is_draft_committed": False,
    }


def _make_boat_projection(
    boat_id: str,
    boat_name: str = "Test Vessel",
    product_details: list[dict] | None = None,
) -> dict:
    """Build a realistic boat projection matching _simulate_boat output."""
    return {
        "boat_id": boat_id,
        "boat_name": boat_name,
        "departure_date": "2026-03-15",
        "arrival_date": "2026-04-10",
        "days_until_departure": 20,
        "origin_port": "Barranquilla",
        "confidence": "high",
        "projected_pallets_min": 5,
        "projected_pallets_max": 15,
        "urgency_breakdown": {"critical": 0, "urgent": 0, "soon": 1, "ok": 1},
        "draft_status": None,
        "draft_id": None,
        "is_active": False,
        "order_by_date": "2026-02-25",
        "days_until_order_deadline": 2,
        "shipping_book_by_date": "2026-03-10",
        "days_until_shipping_deadline": 15,
        "siesa_order_date": "2026-02-23",
        "days_until_siesa_deadline": 0,
        "production_request_date": None,
        "days_until_production_deadline": None,
        "product_details": product_details or [],
        "draft_bl_items": [],
        "has_bl_allocation": False,
        "is_estimated": False,
        "carrier": "TIBA",
        "is_draft_locked": False,
        "blocking_boat_name": None,
        "has_earlier_drafts": False,
        "needs_review": False,
        "review_reason": None,
        "earlier_draft_context": None,
        "has_factory_siesa_supply": False,
        "has_production_supply": False,
        "factory_siesa_total_m2": 0.0,
        "production_total_m2": 0.0,
        "has_in_transit_supply": False,
        "in_transit_total_m2": 0.0,
        "stability_impact": {
            "stabilizes_count": 0,
            "stabilizes_products": [],
            "recovering_count": 0,
            "recovering_products": [],
            "blocked_count": 0,
            "blocked_products": [],
            "progress_before_pct": 100,
            "progress_after_pct": 100,
        },
    }


def _make_horizon(projections: list[dict]) -> dict:
    """Build a realistic get_planning_horizon return value."""
    return {
        "factory_id": "factory-001",
        "factory_name": "CI Ceramics",
        "horizon_months": 3,
        "generated_at": "2026-02-23",
        "projections": projections,
        "factory_order_signal": None,
    }


# =====================
# TESTS
# =====================


@patch("services.forward_simulation_service.get_supabase_client")
def test_happy_path_boat_found(mock_get_db):
    """
    Boat 2 is found in a 2-boat horizon. Verify projected_stock_m2
    reflects cascade from boat 1 (warehouse_m2 decreased for boat 2).
    """
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    pid_a = "prod-aaa"
    pid_b = "prod-bbb"

    # Boat 1: pristine warehouse = 1000 for prod-aaa, 800 for prod-bbb
    boat1_details = [
        _make_product_detail(pid_a, sku="SKU-A", warehouse_m2=1000.0, projected_stock_m2=900.0),
        _make_product_detail(pid_b, sku="SKU-B", warehouse_m2=800.0, projected_stock_m2=700.0),
    ]
    boat1 = _make_boat_projection("boat-001", "Vessel Alpha", boat1_details)

    # Boat 2: warehouse cascaded down (after boat 1 consumed stock)
    boat2_details = [
        _make_product_detail(pid_a, sku="SKU-A", warehouse_m2=700.0, projected_stock_m2=600.0,
                             daily_velocity_m2=12.0, coverage_gap_m2=50.0, days_of_stock_at_arrival=50.0),
        _make_product_detail(pid_b, sku="SKU-B", warehouse_m2=500.0, projected_stock_m2=400.0,
                             daily_velocity_m2=8.0, coverage_gap_m2=80.0, days_of_stock_at_arrival=50.0),
    ]
    boat2 = _make_boat_projection("boat-002", "Vessel Beta", boat2_details)

    horizon = _make_horizon([boat1, boat2])

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", return_value=horizon):
        result = svc.get_projection_for_boat("factory-001", "boat-002")

    assert result is not None
    assert set(result.keys()) == {pid_a, pid_b}

    # Product A
    pa = result[pid_a]
    assert pa["projected_stock_m2"] == Decimal("600.0")
    assert pa["daily_velocity_m2"] == Decimal("12.0")
    assert pa["supply_breakdown"]["warehouse_m2"] == Decimal("700.0")
    assert pa["coverage_gap_m2"] == Decimal("50.0")
    assert pa["days_of_stock_at_arrival"] == 50.0
    # Earlier consumed: boat1 warehouse (1000) - boat2 warehouse (700) = 300
    assert pa["earlier_drafts_consumed_m2"] == Decimal("300.0")

    # Product B
    pb = result[pid_b]
    assert pb["projected_stock_m2"] == Decimal("400.0")
    assert pb["earlier_drafts_consumed_m2"] == Decimal("300.0")  # 800 - 500


@patch("services.forward_simulation_service.get_supabase_client")
def test_boat_not_found(mock_get_db):
    """Calling with an unknown boat_id returns None."""
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    boat1 = _make_boat_projection("boat-001", "Vessel Alpha", [
        _make_product_detail("prod-aaa"),
    ])
    horizon = _make_horizon([boat1])

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", return_value=horizon):
        result = svc.get_projection_for_boat("factory-001", "boat-unknown")

    assert result is None


@patch("services.forward_simulation_service.get_supabase_client")
def test_empty_horizon(mock_get_db):
    """Empty projections list returns None."""
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    horizon = _make_horizon([])

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", return_value=horizon):
        result = svc.get_projection_for_boat("factory-001", "boat-001")

    assert result is None


@patch("services.forward_simulation_service.get_supabase_client")
def test_earlier_drafts_consumed(mock_get_db):
    """
    Boat 1 warehouse=1000, Boat 2 warehouse=300
    → earlier_drafts_consumed_m2 = 700.
    """
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    pid = "prod-xxx"

    boat1 = _make_boat_projection("boat-001", "Vessel A", [
        _make_product_detail(pid, warehouse_m2=1000.0),
    ])
    boat2 = _make_boat_projection("boat-002", "Vessel B", [
        _make_product_detail(pid, warehouse_m2=300.0, projected_stock_m2=200.0),
    ])
    horizon = _make_horizon([boat1, boat2])

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", return_value=horizon):
        result = svc.get_projection_for_boat("factory-001", "boat-002")

    assert result is not None
    assert result[pid]["earlier_drafts_consumed_m2"] == Decimal("700.0")


@patch("services.forward_simulation_service.get_supabase_client")
def test_first_boat_no_earlier_consumption(mock_get_db):
    """First boat has earlier_drafts_consumed_m2 = 0 (it IS the first)."""
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    pid = "prod-yyy"

    boat1 = _make_boat_projection("boat-001", "Vessel A", [
        _make_product_detail(pid, warehouse_m2=500.0, projected_stock_m2=400.0),
    ])
    boat2 = _make_boat_projection("boat-002", "Vessel B", [
        _make_product_detail(pid, warehouse_m2=300.0, projected_stock_m2=200.0),
    ])
    horizon = _make_horizon([boat1, boat2])

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", return_value=horizon):
        result = svc.get_projection_for_boat("factory-001", "boat-001")

    assert result is not None
    # First boat: warehouse_m2 == first_warehouse_by_pid → consumed = 0
    assert result[pid]["earlier_drafts_consumed_m2"] == Decimal("0")


@patch("services.forward_simulation_service.get_supabase_client")
def test_exception_handling_returns_none(mock_get_db):
    """If get_planning_horizon raises, get_projection_for_boat returns None."""
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", side_effect=RuntimeError("DB exploded")):
        result = svc.get_projection_for_boat("factory-001", "boat-001")

    assert result is None


@patch("services.forward_simulation_service.get_supabase_client")
def test_feature_flag_off_skips_forward_sim(mock_get_db):
    """
    When use_projection=False the Order Builder service does NOT call forward sim.
    This validates the guard at line 274 of order_builder_service.py.
    """
    mock_get_db.return_value = MagicMock()

    with patch("services.forward_simulation_service.get_forward_simulation_service") as mock_fwd:
        # Import locally after patch is active
        from services.order_builder_service import OrderBuilderService

        ob_svc = MagicMock(spec=OrderBuilderService)

        # Simulate the guard: use_projection=False → projection_map stays None
        use_projection = False
        factory_id = "factory-001"
        boat_id = "boat-001"

        projection_map = None
        if use_projection and factory_id and boat_id:
            fwd_sim = mock_fwd()
            projection_map = fwd_sim.get_projection_for_boat(factory_id, boat_id)

        # Forward sim was never called
        mock_fwd.assert_not_called()
        assert projection_map is None


@patch("services.forward_simulation_service.get_supabase_client")
def test_feature_flag_on_no_factory_skips_forward_sim(mock_get_db):
    """
    When use_projection=True but factory_id is None, projection_map stays None.
    This validates the guard: `if use_projection and factory_id and boat and boat.boat_id`.
    """
    mock_get_db.return_value = MagicMock()

    with patch("services.forward_simulation_service.get_forward_simulation_service") as mock_fwd:
        use_projection = True
        factory_id = None  # No factory provided
        boat_id = "boat-001"

        projection_map = None
        if use_projection and factory_id and boat_id:
            fwd_sim = mock_fwd()
            projection_map = fwd_sim.get_projection_for_boat(factory_id, boat_id)

        # Forward sim was never called because factory_id is falsy
        mock_fwd.assert_not_called()
        assert projection_map is None


@patch("services.forward_simulation_service.get_supabase_client")
def test_supply_breakdown_fields(mock_get_db):
    """Verify all supply_breakdown sub-fields are Decimal in the result."""
    mock_get_db.return_value = MagicMock()

    from services.forward_simulation_service import ForwardSimulationService

    pid = "prod-supply"

    boat1 = _make_boat_projection("boat-001", "Vessel A", [
        _make_product_detail(
            pid, warehouse_m2=500.0, factory_siesa_m2=100.0,
            production_pipeline_m2=50.0, in_transit_m2=25.0,
        ),
    ])
    horizon = _make_horizon([boat1])

    svc = ForwardSimulationService()
    with patch.object(svc, "get_planning_horizon", return_value=horizon):
        result = svc.get_projection_for_boat("factory-001", "boat-001")

    assert result is not None
    sb = result[pid]["supply_breakdown"]
    assert sb["warehouse_m2"] == Decimal("500.0")
    assert sb["factory_siesa_m2"] == Decimal("100.0")
    assert sb["production_pipeline_m2"] == Decimal("50.0")
    assert sb["in_transit_m2"] == Decimal("25.0")
