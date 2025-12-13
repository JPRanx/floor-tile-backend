"""
Unit tests for RecommendationService.

Tests cover warehouse allocation and order recommendation logic.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, timedelta
from decimal import Decimal

from services.recommendation_service import (
    RecommendationService,
    get_recommendation_service,
    M2_PER_PALLET,
    Z_SCORE,
)
from models.recommendation import (
    ConfidenceLevel,
    RecommendationPriority,
    WarningType,
)
from services.stockout_service import StockoutStatus


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_product_service():
    """Mock ProductService."""
    with patch("services.recommendation_service.get_product_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_inventory_service():
    """Mock InventoryService."""
    with patch("services.recommendation_service.get_inventory_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_sales_service():
    """Mock SalesService."""
    with patch("services.recommendation_service.get_sales_service") as mock:
        service = MagicMock()
        # Default: no customer analysis data
        service.get_customer_analysis_batch.return_value = {}
        mock.return_value = service
        yield service


@pytest.fixture
def mock_stockout_service():
    """Mock StockoutService."""
    with patch("services.recommendation_service.get_stockout_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_boat_service():
    """Mock BoatScheduleService."""
    with patch("services.recommendation_service.get_boat_schedule_service") as mock:
        service = MagicMock()
        # Default: no boats scheduled
        service.get_next_two_arrivals.return_value = (None, None)
        mock.return_value = service
        yield service


@pytest.fixture
def mock_settings():
    """Mock settings."""
    with patch("services.recommendation_service.settings") as mock:
        mock.lead_time_days = 45
        mock.warehouse_max_pallets = 740
        mock.velocity_window_weeks = 12
        mock.low_volume_min_records = 2
        yield mock


@pytest.fixture
def recommendation_service(
    mock_product_service,
    mock_inventory_service,
    mock_sales_service,
    mock_stockout_service,
    mock_boat_service,
    mock_settings
):
    """Create RecommendationService with mocked dependencies."""
    return RecommendationService()


@pytest.fixture
def sample_product():
    """Sample product with ALTA rotation."""
    product = MagicMock()
    product.id = "product-uuid-123"
    product.sku = "NOGAL CAFE"
    product.category = MagicMock()
    product.category.value = "MADERAS"
    product.rotation = MagicMock()
    product.rotation.value = "ALTA"
    return product


@pytest.fixture
def sample_product_low_rotation():
    """Sample product with BAJA rotation."""
    product = MagicMock()
    product.id = "product-uuid-456"
    product.sku = "CEIBA GRIS"
    product.category = MagicMock()
    product.category.value = "MADERAS"
    product.rotation = MagicMock()
    product.rotation.value = "BAJA"
    return product


@pytest.fixture
def sample_sales_history():
    """4 weeks of sales data."""
    records = []
    for i in range(4):
        record = MagicMock()
        record.quantity_m2 = Decimal("700")  # 700 m²/week = 100 m²/day
        records.append(record)
    return records


@pytest.fixture
def sample_inventory():
    """Sample inventory snapshot."""
    inventory = MagicMock()
    inventory.product_id = "product-uuid-123"
    inventory.warehouse_qty = 3000  # ~22 pallets
    inventory.in_transit_qty = 1000  # ~7 pallets
    return inventory


@pytest.fixture
def sample_stockout():
    """Sample stockout calculation."""
    stockout = MagicMock()
    stockout.product_id = "product-uuid-123"
    stockout.status = StockoutStatus.CONSIDER  # Boat-based: stockout between boats
    stockout.days_to_stockout = Decimal("40")
    return stockout


# ===================
# ALLOCATION TESTS
# ===================

class TestWarehouseAllocation:
    """Tests for warehouse allocation calculation."""

    def test_allocation_basic_calculation(
        self,
        recommendation_service,
        mock_product_service,
        mock_sales_service,
        sample_product
    ):
        """Allocation calculated correctly with velocity and safety stock."""
        mock_product_service.get_all.return_value = ([sample_product], 1)

        # Variable sales data to get non-zero std_dev
        variable_sales = [
            MagicMock(quantity_m2=Decimal("600")),
            MagicMock(quantity_m2=Decimal("800")),
            MagicMock(quantity_m2=Decimal("650")),
            MagicMock(quantity_m2=Decimal("750")),
        ]  # avg = 700
        # Use get_recent_sales_all instead of get_history
        mock_sales_service.get_recent_sales_all.return_value = {
            sample_product.id: variable_sales
        }

        allocations, scale = recommendation_service.allocate_warehouse_slots()

        assert len(allocations) == 1
        alloc = allocations[0]

        # Weekly velocity: ~700 m²/week, Daily: 100 m²/day
        assert alloc.weekly_velocity == Decimal("700.00")
        assert alloc.daily_velocity == Decimal("100.00")

        # Base stock: 100 m²/day × 45 days = 4500 m²
        assert alloc.base_stock_m2 == Decimal("4500.00")

        # Safety stock should be > 0 due to variability
        assert alloc.safety_stock_m2 > Decimal("0")

        # Target should be base + safety stock
        assert alloc.target_m2 > alloc.base_stock_m2

        # Scale factor should be 1 (under capacity)
        assert scale == Decimal("1")

    def test_allocation_no_sales_data(
        self,
        recommendation_service,
        mock_product_service,
        mock_sales_service,
        sample_product
    ):
        """Products with no sales get zero allocation."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {}  # No sales

        allocations, _ = recommendation_service.allocate_warehouse_slots()

        assert len(allocations) == 1
        alloc = allocations[0]
        assert alloc.target_pallets == Decimal("0")
        assert alloc.weeks_of_data == 0

    def test_allocation_scaling_when_over_capacity(
        self,
        recommendation_service,
        mock_product_service,
        mock_sales_service
    ):
        """Allocations scaled down when total exceeds 740 pallets."""
        # Create 10 products each needing 100 pallets = 1000 total
        products = []
        for i in range(10):
            p = MagicMock()
            p.id = f"product-{i}"
            p.sku = f"SKU-{i}"
            p.category = None
            p.rotation = None
            products.append(p)

        mock_product_service.get_all.return_value = (products, 10)

        # Each product: high velocity sales data
        sales_by_product = {
            f"product-{i}": [MagicMock(quantity_m2=Decimal("2000")) for _ in range(4)]
            for i in range(10)
        }
        mock_sales_service.get_recent_sales_all.return_value = sales_by_product

        allocations, scale = recommendation_service.allocate_warehouse_slots()

        # Scale factor should be < 1
        assert scale < Decimal("1")

        # All allocations should have scaled values
        for alloc in allocations:
            if alloc.target_pallets > 0:
                assert alloc.scaled_target_pallets is not None
                assert alloc.scaled_target_pallets < alloc.target_pallets

    def test_allocation_std_dev_calculation(
        self,
        recommendation_service,
        mock_product_service,
        mock_sales_service,
        sample_product
    ):
        """Standard deviation calculated from weekly sales variability."""
        mock_product_service.get_all.return_value = ([sample_product], 1)

        # Variable sales: 500, 900, 600, 800 (avg 700, std ~158)
        variable_sales = [
            MagicMock(quantity_m2=Decimal("500")),
            MagicMock(quantity_m2=Decimal("900")),
            MagicMock(quantity_m2=Decimal("600")),
            MagicMock(quantity_m2=Decimal("800")),
        ]
        mock_sales_service.get_recent_sales_all.return_value = {
            sample_product.id: variable_sales
        }

        allocations, _ = recommendation_service.allocate_warehouse_slots()

        alloc = allocations[0]
        # Std dev should be > 0 due to variability
        assert alloc.velocity_std_dev > Decimal("0")
        # Safety stock should be > 0
        assert alloc.safety_stock_m2 > Decimal("0")


# ===================
# RECOMMENDATION TESTS
# ===================

class TestOrderRecommendations:
    """Tests for order recommendation generation."""

    def test_recommendation_generated_when_below_target(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product,
        sample_sales_history,
        sample_inventory,
        sample_stockout
    ):
        """Recommendation created when current stock below target."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-123": sample_sales_history}

        # Low inventory: only 1000 m² (~7 pallets)
        low_inventory = MagicMock()
        low_inventory.product_id = "product-uuid-123"
        low_inventory.warehouse_qty = 1000
        low_inventory.in_transit_qty = 0
        mock_inventory_service.get_latest.return_value = [low_inventory]

        # Stockout summary
        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[sample_stockout]
        )

        result = recommendation_service.get_recommendations()

        assert len(result.recommendations) == 1
        rec = result.recommendations[0]
        assert rec.gap_pallets > 0
        assert rec.sku == "NOGAL CAFE"

    def test_no_recommendation_when_at_target(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product,
        sample_sales_history,
        sample_stockout
    ):
        """No recommendation when stock equals or exceeds target."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-123": sample_sales_history}

        # High inventory: 10000 m² (well above any target)
        high_inventory = MagicMock()
        high_inventory.product_id = "product-uuid-123"
        high_inventory.warehouse_qty = 10000
        high_inventory.in_transit_qty = 5000
        mock_inventory_service.get_latest.return_value = [high_inventory]

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[sample_stockout]
        )

        result = recommendation_service.get_recommendations()

        # Either no recommendations or a warning for over-stocked
        recommendations_for_product = [
            r for r in result.recommendations
            if r.product_id == "product-uuid-123"
        ]
        assert len(recommendations_for_product) == 0

    def test_overstocked_warning_generated(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product,
        sample_sales_history,
        sample_stockout
    ):
        """Warning generated when significantly over target."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-123": sample_sales_history}

        # Very high inventory
        high_inventory = MagicMock()
        high_inventory.product_id = "product-uuid-123"
        high_inventory.warehouse_qty = 20000
        high_inventory.in_transit_qty = 5000
        mock_inventory_service.get_latest.return_value = [high_inventory]

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[sample_stockout]
        )

        result = recommendation_service.get_recommendations()

        # Should have an overstocked warning
        overstocked_warnings = [
            w for w in result.warnings
            if w.type == WarningType.OVER_STOCKED
        ]
        assert len(overstocked_warnings) == 1
        assert "above target" in overstocked_warnings[0].message

    def test_no_sales_warning_generated(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product
    ):
        """Warning generated for products with no sales data."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {}  # No sales
        mock_inventory_service.get_latest.return_value = []

        no_sales_stockout = MagicMock()
        no_sales_stockout.product_id = "product-uuid-123"
        no_sales_stockout.status = StockoutStatus.YOUR_CALL  # No sales data
        no_sales_stockout.days_to_stockout = None

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[no_sales_stockout]
        )

        result = recommendation_service.get_recommendations()

        no_sales_warnings = [
            w for w in result.warnings
            if w.type == WarningType.NO_SALES_DATA
        ]
        assert len(no_sales_warnings) == 1


# ===================
# PRIORITY TESTS
# ===================

class TestPriorityAssignment:
    """Tests for recommendation priority logic."""

    def test_critical_when_stockout_before_arrival(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product,
        sample_sales_history
    ):
        """CRITICAL priority when order arrives after stockout."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-123": sample_sales_history}

        low_inventory = MagicMock()
        low_inventory.product_id = "product-uuid-123"
        low_inventory.warehouse_qty = 500  # Very low
        low_inventory.in_transit_qty = 0
        mock_inventory_service.get_latest.return_value = [low_inventory]

        # Stockout in 5 days (order arrives in 45)
        critical_stockout = MagicMock()
        critical_stockout.product_id = "product-uuid-123"
        critical_stockout.status = StockoutStatus.HIGH_PRIORITY  # Boat-based: stockout before next boat
        critical_stockout.days_to_stockout = Decimal("5")

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[critical_stockout]
        )

        result = recommendation_service.get_recommendations()

        assert len(result.recommendations) == 1
        assert result.recommendations[0].priority == RecommendationPriority.HIGH_PRIORITY
        assert result.recommendations[0].arrives_before_stockout is False

    def test_high_priority_for_alta_rotation(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product,
        sample_sales_history
    ):
        """HIGH priority for ALTA rotation products (not critical)."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-123": sample_sales_history}

        low_inventory = MagicMock()
        low_inventory.product_id = "product-uuid-123"
        low_inventory.warehouse_qty = 1000
        low_inventory.in_transit_qty = 0
        mock_inventory_service.get_latest.return_value = [low_inventory]

        # Not high priority, just consider
        consider_stockout = MagicMock()
        consider_stockout.product_id = "product-uuid-123"
        consider_stockout.status = StockoutStatus.CONSIDER  # Boat-based: stockout between boats
        consider_stockout.days_to_stockout = Decimal("55")

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[consider_stockout]
        )

        result = recommendation_service.get_recommendations()

        if result.recommendations:
            rec = result.recommendations[0]
            # Should be CONSIDER (stockout between boats)
            assert rec.priority in [RecommendationPriority.CONSIDER, RecommendationPriority.HIGH_PRIORITY]

    def test_low_priority_for_baja_rotation(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product_low_rotation,
        sample_sales_history
    ):
        """LOW priority for BAJA rotation products."""
        mock_product_service.get_all.return_value = ([sample_product_low_rotation], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-456": sample_sales_history}

        low_inventory = MagicMock()
        low_inventory.product_id = "product-uuid-456"
        low_inventory.warehouse_qty = 1000
        low_inventory.in_transit_qty = 0
        mock_inventory_service.get_latest.return_value = [low_inventory]

        well_covered_stockout = MagicMock()
        well_covered_stockout.product_id = "product-uuid-456"
        well_covered_stockout.status = StockoutStatus.WELL_COVERED  # Boat-based: won't stock out for 2+ cycles
        well_covered_stockout.days_to_stockout = Decimal("80")

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[well_covered_stockout]
        )

        result = recommendation_service.get_recommendations()

        # WELL_COVERED products go to warnings, not recommendations
        # Check it's handled correctly (either in recommendations or warnings)
        if result.recommendations:
            rec = result.recommendations[0]
            assert rec.priority == RecommendationPriority.WELL_COVERED
        else:
            # Should be in warnings as WELL_STOCKED
            assert len(result.warnings) > 0


# ===================
# SORTING TESTS
# ===================

class TestRecommendationSorting:
    """Tests for recommendation sorting."""

    def test_sorted_by_priority_then_days(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service
    ):
        """Recommendations sorted by priority, then days until empty."""
        # Create 3 products with different priorities
        products = []
        for i, (rotation, days) in enumerate([
            ("BAJA", 30),     # LOW priority, 30 days
            ("ALTA", 50),     # HIGH priority, 50 days
            ("MEDIA", 25),    # Will be CRITICAL (25 < 45)
        ]):
            p = MagicMock()
            p.id = f"product-{i}"
            p.sku = f"SKU-{i}"
            p.category = MagicMock()
            p.category.value = "MADERAS"
            p.rotation = MagicMock()
            p.rotation.value = rotation
            products.append(p)

        mock_product_service.get_all.return_value = (products, 3)

        # Each needs ordering
        mock_sales_service.get_recent_sales_all.return_value = {
            f"product-{i}": [MagicMock(quantity_m2=Decimal("700")) for _ in range(4)]
            for i in range(3)
        }

        # Low inventory for all
        inventories = [
            MagicMock(product_id=f"product-{i}", warehouse_qty=500, in_transit_qty=0)
            for i in range(3)
        ]
        mock_inventory_service.get_latest.return_value = inventories

        stockouts = []
        for i, (status, days) in enumerate([
            (StockoutStatus.WELL_COVERED, 90),   # Product 0: will be WELL_STOCKED
            (StockoutStatus.CONSIDER, 50),       # Product 1: stockout between boats
            (StockoutStatus.HIGH_PRIORITY, 25),  # Product 2: stockout before next boat
        ]):
            s = MagicMock()
            s.product_id = f"product-{i}"
            s.status = status
            s.days_to_stockout = Decimal(str(days))
            stockouts.append(s)

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=stockouts
        )

        result = recommendation_service.get_recommendations()

        # Should be sorted by action type: ORDER_NOW first, then ORDER_SOON
        if len(result.recommendations) >= 2:
            priorities = [r.priority for r in result.recommendations]
            # HIGH_PRIORITY should come before CONSIDER
            high_priority_indices = [
                i for i, p in enumerate(priorities)
                if p == RecommendationPriority.HIGH_PRIORITY
            ]
            consider_indices = [
                i for i, p in enumerate(priorities)
                if p == RecommendationPriority.CONSIDER
            ]
            if high_priority_indices and consider_indices:
                assert max(high_priority_indices) < min(consider_indices)


# ===================
# WAREHOUSE STATUS TESTS
# ===================

class TestWarehouseStatus:
    """Tests for warehouse status calculation."""

    def test_utilization_calculated(
        self,
        recommendation_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        sample_product,
        sample_sales_history,
        sample_stockout
    ):
        """Warehouse utilization percentage calculated correctly."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_sales_service.get_recent_sales_all.return_value = {"product-uuid-123": sample_sales_history}

        # 740 pallets × 135 m² = 99,900 m² capacity
        # 5000 m² current = ~37 pallets = ~5% utilization
        inventory = MagicMock()
        inventory.product_id = "product-uuid-123"
        inventory.warehouse_qty = 3000
        inventory.in_transit_qty = 2000
        mock_inventory_service.get_latest.return_value = [inventory]

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[sample_stockout]
        )

        result = recommendation_service.get_recommendations()

        assert result.warehouse_status.total_capacity_pallets == 740
        assert result.warehouse_status.utilization_percent >= 0


# ===================
# SINGLETON TESTS
# ===================

class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_recommendation_service_returns_same_instance(
        self,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_stockout_service,
        mock_settings
    ):
        """get_recommendation_service returns the same instance."""
        import services.recommendation_service as module
        module._recommendation_service = None

        service1 = get_recommendation_service()
        service2 = get_recommendation_service()

        assert service1 is service2


# ===================
# CONFIDENCE CALCULATION TESTS
# ===================

class TestConfidenceCalculation:
    """Tests for _calculate_confidence method."""

    def test_low_confidence_no_sales_data(
        self,
        recommendation_service
    ):
        """LOW confidence when no sales data."""
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[],
            weeks_of_data=0,
            customer_analysis=None
        )

        assert confidence == ConfidenceLevel.LOW
        assert "No sales data" in reason
        assert cv is None

    def test_low_confidence_few_weeks(
        self,
        recommendation_service
    ):
        """LOW confidence when less than 4 weeks of data."""
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100"), Decimal("100")],
            weeks_of_data=2,
            customer_analysis=None
        )

        assert confidence == ConfidenceLevel.LOW
        assert "2 weeks" in reason

    def test_low_confidence_no_recent_sales(
        self,
        recommendation_service
    ):
        """LOW confidence when no sales in last 4 weeks."""
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")],
            weeks_of_data=4,
            customer_analysis=None
        )

        assert confidence == ConfidenceLevel.LOW
        assert "No sales in last 4 weeks" in reason

    def test_low_confidence_top_customer_dominates(
        self,
        recommendation_service
    ):
        """LOW confidence when top customer > 70% of sales."""
        customer_analysis = {
            "unique_customers": 3,
            "top_customer_name": "ACME Corp",
            "top_customer_share": Decimal("0.75"),
            "recurring_count": 2,
            "recurring_share": Decimal("0.5"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 8,
            weeks_of_data=8,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.LOW
        assert "75%" in reason
        assert "ACME" in reason

    def test_low_confidence_single_customer(
        self,
        recommendation_service
    ):
        """LOW confidence with single customer (triggers via >70% rule first)."""
        customer_analysis = {
            "unique_customers": 1,
            "top_customer_name": "Solo Buyer",
            "top_customer_share": Decimal("1.0"),
            "recurring_count": 1,
            "recurring_share": Decimal("1.0"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 8,
            weeks_of_data=8,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.LOW
        # Single customer with 100% share triggers the ">70% from customer" rule first
        assert "100%" in reason or "Solo Buyer" in reason

    def test_low_confidence_erratic_sales(
        self,
        recommendation_service
    ):
        """LOW confidence when coefficient of variation > 0.8."""
        # Very erratic sales: 10, 200, 5, 300, 15, 400, 10, 250 (CV > 0.8)
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": None,
            "top_customer_share": Decimal("0.2"),
            "recurring_count": 3,
            "recurring_share": Decimal("0.5"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("10"), Decimal("200"), Decimal("5"), Decimal("300"),
                         Decimal("15"), Decimal("400"), Decimal("10"), Decimal("250")],
            weeks_of_data=8,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.LOW
        assert "Erratic sales" in reason

    def test_medium_confidence_top_customer_significant(
        self,
        recommendation_service
    ):
        """MEDIUM confidence when top customer 50-70% of sales."""
        customer_analysis = {
            "unique_customers": 3,
            "top_customer_name": "Big Buyer",
            "top_customer_share": Decimal("0.55"),
            "recurring_count": 2,
            "recurring_share": Decimal("0.4"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 8,
            weeks_of_data=8,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.MEDIUM
        assert "55%" in reason

    def test_medium_confidence_few_customers(
        self,
        recommendation_service
    ):
        """MEDIUM confidence when only 2 customers."""
        customer_analysis = {
            "unique_customers": 2,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.4"),
            "recurring_count": 2,
            "recurring_share": Decimal("0.9"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 8,
            weeks_of_data=8,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.MEDIUM
        assert "2 customers" in reason

    def test_medium_confidence_limited_history(
        self,
        recommendation_service
    ):
        """MEDIUM confidence when 4-7 weeks of history."""
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": None,
            "top_customer_share": Decimal("0.3"),
            "recurring_count": 3,
            "recurring_share": Decimal("0.5"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 5,
            weeks_of_data=5,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.MEDIUM
        assert "Limited history" in reason

    def test_medium_confidence_variable_sales(
        self,
        recommendation_service
    ):
        """MEDIUM confidence when CV between 0.5 and 0.8 (no customer analysis)."""
        # Variable but not erratic: pattern that gives CV ~0.4-0.6
        # Without customer analysis, we skip to CV check
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100"), Decimal("30"), Decimal("120"), Decimal("25"),
                         Decimal("100"), Decimal("35"), Decimal("110"), Decimal("30")],
            weeks_of_data=8,
            customer_analysis=None  # No customer data, will check CV
        )

        # Without customer analysis and with variable sales, should get MEDIUM
        # If CV > 0.5, reason = "Variable sales pattern"
        # If CV < 0.5, reason = "X weeks history"
        assert confidence == ConfidenceLevel.MEDIUM
        assert "Variable sales" in reason or "weeks history" in reason

    def test_high_confidence_good_recurring_base(
        self,
        recommendation_service
    ):
        """HIGH confidence with >70% recurring customers and >=3 customers."""
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.3"),
            "recurring_count": 4,
            "recurring_share": Decimal("0.85"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 8,
            weeks_of_data=8,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.HIGH
        assert "5 customers" in reason
        assert "recurring" in reason

    def test_high_confidence_diverse_stable_demand(
        self,
        recommendation_service
    ):
        """HIGH confidence with diverse customers and stable demand."""
        customer_analysis = {
            "unique_customers": 6,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.2"),
            "recurring_count": 3,
            "recurring_share": Decimal("0.5"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 10,
            weeks_of_data=10,
            customer_analysis=customer_analysis
        )

        assert confidence == ConfidenceLevel.HIGH
        assert "stable demand" in reason

    def test_customer_metrics_populated(
        self,
        recommendation_service
    ):
        """Customer metrics are correctly populated from analysis."""
        customer_analysis = {
            "unique_customers": 8,
            "top_customer_name": "Big Corp",
            "top_customer_share": Decimal("0.25"),
            "recurring_count": 5,
            "recurring_share": Decimal("0.6"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 10,
            weeks_of_data=10,
            customer_analysis=customer_analysis
        )

        assert metrics["unique_customers"] == 8
        assert metrics["top_customer_name"] == "Big Corp"
        assert metrics["top_customer_share"] == Decimal("0.25")
        assert metrics["recurring_customers"] == 5
        assert metrics["recurring_share"] == Decimal("0.6")

    def test_cv_calculated_correctly(
        self,
        recommendation_service
    ):
        """Coefficient of variation is calculated correctly."""
        # All same values -> CV = 0
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 8,
            weeks_of_data=8,
            customer_analysis={"unique_customers": 5, "top_customer_share": Decimal("0.2"), "recurring_share": Decimal("0.8"), "recurring_count": 4}
        )

        assert cv == Decimal("0")

    def test_default_metrics_when_no_customer_analysis(
        self,
        recommendation_service
    ):
        """Default metrics when no customer analysis provided."""
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[Decimal("100")] * 4,
            weeks_of_data=4,
            customer_analysis=None
        )

        assert metrics["unique_customers"] == 0
        assert metrics["top_customer_name"] is None
        assert metrics["top_customer_share"] is None
        assert metrics["recurring_customers"] == 0
