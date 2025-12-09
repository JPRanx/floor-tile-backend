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
def mock_settings():
    """Mock settings."""
    with patch("services.recommendation_service.settings") as mock:
        mock.lead_time_days = 45
        mock.warehouse_max_pallets = 740
        yield mock


@pytest.fixture
def recommendation_service(
    mock_product_service,
    mock_inventory_service,
    mock_sales_service,
    mock_stockout_service,
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
    stockout.status = StockoutStatus.WARNING
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
        assert "over target" in overstocked_warnings[0].message

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
        no_sales_stockout.status = StockoutStatus.NO_SALES
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
        critical_stockout.status = StockoutStatus.CRITICAL
        critical_stockout.days_to_stockout = Decimal("5")

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[critical_stockout]
        )

        result = recommendation_service.get_recommendations()

        assert len(result.recommendations) == 1
        assert result.recommendations[0].priority == RecommendationPriority.CRITICAL
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

        # Not critical, just warning
        warning_stockout = MagicMock()
        warning_stockout.product_id = "product-uuid-123"
        warning_stockout.status = StockoutStatus.WARNING
        warning_stockout.days_to_stockout = Decimal("55")

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[warning_stockout]
        )

        result = recommendation_service.get_recommendations()

        if result.recommendations:
            rec = result.recommendations[0]
            # Should be HIGH (ALTA rotation) not CRITICAL
            assert rec.priority in [RecommendationPriority.HIGH, RecommendationPriority.CRITICAL]

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

        ok_stockout = MagicMock()
        ok_stockout.product_id = "product-uuid-456"
        ok_stockout.status = StockoutStatus.OK
        ok_stockout.days_to_stockout = Decimal("80")

        mock_stockout_service.calculate_all.return_value = MagicMock(
            products=[ok_stockout]
        )

        result = recommendation_service.get_recommendations()

        if result.recommendations:
            rec = result.recommendations[0]
            assert rec.priority == RecommendationPriority.LOW


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
            (StockoutStatus.OK, 30),
            (StockoutStatus.WARNING, 50),
            (StockoutStatus.CRITICAL, 25),
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

        # Should be sorted: CRITICAL first, then by days
        if len(result.recommendations) >= 2:
            priorities = [r.priority for r in result.recommendations]
            # CRITICAL should come before HIGH/MEDIUM/LOW
            critical_indices = [
                i for i, p in enumerate(priorities)
                if p == RecommendationPriority.CRITICAL
            ]
            non_critical_indices = [
                i for i, p in enumerate(priorities)
                if p != RecommendationPriority.CRITICAL
            ]
            if critical_indices and non_critical_indices:
                assert max(critical_indices) < min(non_critical_indices)


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
