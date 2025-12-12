"""
Unit tests for StockoutService.

Tests cover stockout calculation logic with boat-based priority system.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, timedelta
from decimal import Decimal

from services.stockout_service import (
    StockoutService,
    get_stockout_service,
    StockoutStatus,
    ProductStockout,
    StockoutSummary,
)


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_inventory_service():
    """Mock InventoryService."""
    with patch("services.stockout_service.get_inventory_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_sales_service():
    """Mock SalesService."""
    with patch("services.stockout_service.get_sales_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_product_service():
    """Mock ProductService."""
    with patch("services.stockout_service.get_product_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_boat_service():
    """Mock BoatScheduleService."""
    with patch("services.stockout_service.get_boat_schedule_service") as mock:
        service = MagicMock()
        # Default: no boats scheduled (fallback to lead_time)
        service.get_next_two_arrivals.return_value = (None, None)
        service.get_next_two_departures.return_value = (None, None)
        mock.return_value = service
        yield service


@pytest.fixture
def mock_settings():
    """Mock settings."""
    with patch("services.stockout_service.settings") as mock:
        mock.lead_time_days = 45
        mock.velocity_window_weeks = 12
        yield mock


@pytest.fixture
def stockout_service(mock_inventory_service, mock_sales_service, mock_product_service, mock_boat_service, mock_settings):
    """Create StockoutService with mocked dependencies."""
    return StockoutService()


@pytest.fixture
def sample_product():
    """Sample product data."""
    product = MagicMock()
    product.id = "product-uuid-123"
    product.sku = "MAD-001"
    product.category = MagicMock()
    product.category.value = "MADERAS"
    product.rotation = MagicMock()
    product.rotation.value = "ALTA"
    return product


@pytest.fixture
def sample_inventory():
    """Sample inventory snapshot."""
    inventory = MagicMock()
    inventory.product_id = "product-uuid-123"
    inventory.warehouse_qty = 500
    inventory.in_transit_qty = 200
    return inventory


@pytest.fixture
def sample_sales():
    """Sample sales records (4 weeks)."""
    records = []
    for i in range(4):
        record = MagicMock()
        record.product_id = "product-uuid-123"
        record.quantity_m2 = Decimal("100")  # 100 m² per week
        records.append(record)
    return records


# ===================
# HIGH_PRIORITY STATUS TESTS
# ===================

class TestHighPriorityStatus:
    """Tests for HIGH_PRIORITY stockout status (stockout before next boat)."""

    def test_high_priority_when_stockout_before_next_boat(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Product is HIGH_PRIORITY when stockout before next boat arrives."""
        # Setup: 70 m² inventory, 10 m²/day = 7 days
        # Next boat in 30 days → stockout before boat → HIGH_PRIORITY
        inventory = MagicMock()
        inventory.warehouse_qty = 70
        inventory.in_transit_qty = 0

        today = date.today()
        mock_boat_service.get_next_two_arrivals.return_value = (
            today + timedelta(days=30),  # next boat
            today + timedelta(days=60),  # second boat
        )

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.HIGH_PRIORITY

    def test_high_priority_with_zero_inventory(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Product is HIGH_PRIORITY when inventory is zero with sales."""
        inventory = MagicMock()
        inventory.warehouse_qty = 0
        inventory.in_transit_qty = 0

        today = date.today()
        mock_boat_service.get_next_two_arrivals.return_value = (
            today + timedelta(days=30),
            today + timedelta(days=60),
        )

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        sales = [MagicMock(quantity_m2=Decimal("100")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.HIGH_PRIORITY
        assert result.days_to_stockout == Decimal("0")


# ===================
# CONSIDER STATUS TESTS
# ===================

class TestConsiderStatus:
    """Tests for CONSIDER stockout status (stockout before second boat)."""

    def test_consider_when_stockout_between_boats(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Product is CONSIDER when stockout after first but before second boat."""
        # Setup: 400 m² inventory, 10 m²/day = 40 days
        # Next boat in 30 days, second in 60 → stockout at day 40 → CONSIDER
        inventory = MagicMock()
        inventory.warehouse_qty = 400
        inventory.in_transit_qty = 0

        today = date.today()
        mock_boat_service.get_next_two_arrivals.return_value = (
            today + timedelta(days=30),  # stockout is AFTER this
            today + timedelta(days=60),  # stockout is BEFORE this
        )

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.CONSIDER


# ===================
# WELL_COVERED STATUS TESTS
# ===================

class TestWellCoveredStatus:
    """Tests for WELL_COVERED stockout status (won't stockout for 2+ boat cycles)."""

    def test_well_covered_when_stockout_after_second_boat(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Product is WELL_COVERED when stockout after second boat."""
        # Setup: 700 m² inventory, 10 m²/day = 70 days
        # Next boat in 30 days, second in 60 → stockout at day 70 → WELL_COVERED
        inventory = MagicMock()
        inventory.warehouse_qty = 700
        inventory.in_transit_qty = 0

        today = date.today()
        mock_boat_service.get_next_two_arrivals.return_value = (
            today + timedelta(days=30),
            today + timedelta(days=60),  # stockout is AFTER this
        )

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.WELL_COVERED

    def test_well_covered_with_in_transit_included(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """In-transit inventory is included in calculation."""
        # Setup: 300 warehouse + 400 in_transit = 700 total, 10/day = 70 days
        inventory = MagicMock()
        inventory.warehouse_qty = 300
        inventory.in_transit_qty = 400

        today = date.today()
        mock_boat_service.get_next_two_arrivals.return_value = (
            today + timedelta(days=30),
            today + timedelta(days=60),
        )

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.total_qty == Decimal("700")
        assert result.status == StockoutStatus.WELL_COVERED


# ===================
# YOUR_CALL STATUS TESTS
# ===================

class TestYourCallStatus:
    """Tests for YOUR_CALL stockout status (no data / needs review)."""

    def test_your_call_when_no_history(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product,
        sample_inventory
    ):
        """Product is YOUR_CALL when no sales history exists."""
        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [sample_inventory]
        mock_sales_service.get_history.return_value = []  # No sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.YOUR_CALL
        assert result.days_to_stockout is None

    def test_your_call_when_zero_sales(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product,
        sample_inventory
    ):
        """Product is YOUR_CALL when sales are all zero."""
        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [sample_inventory]

        # Sales records with zero quantity
        sales = [MagicMock(quantity_m2=Decimal("0")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.YOUR_CALL
        assert result.days_to_stockout is None


# ===================
# CALCULATE ALL TESTS
# ===================

class TestCalculateAll:
    """Tests for calculate_all method."""

    def test_calculate_all_returns_summary(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """calculate_all returns StockoutSummary with all products."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_recent_sales_all.return_value = {}

        result = stockout_service.calculate_all()

        assert isinstance(result, StockoutSummary)
        assert result.total_products == 1

    def test_calculate_all_counts_statuses(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service
    ):
        """calculate_all correctly counts status types."""
        # Create 3 products
        products = []
        for i in range(3):
            p = MagicMock()
            p.id = f"product-{i}"
            p.sku = f"SKU-{i}"
            p.category = None
            p.rotation = None
            products.append(p)

        mock_product_service.get_all.return_value = (products, 3)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_recent_sales_all.return_value = {}  # All will be YOUR_CALL

        result = stockout_service.calculate_all()

        assert result.total_products == 3
        assert result.your_call_count == 3


# ===================
# HELPER METHODS TESTS
# ===================

class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_critical_products(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service
    ):
        """get_critical_products returns only HIGH_PRIORITY products."""
        # One product with no sales (not high priority)
        product = MagicMock()
        product.id = "product-1"
        product.sku = "SKU-1"
        product.category = None
        product.rotation = None

        mock_product_service.get_all.return_value = ([product], 1)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_recent_sales_all.return_value = {}

        result = stockout_service.get_critical_products()

        # No sales = YOUR_CALL status, not HIGH_PRIORITY
        assert len(result) == 0

    def test_get_products_by_status(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service
    ):
        """get_products_by_status filters correctly."""
        product = MagicMock()
        product.id = "product-1"
        product.sku = "SKU-1"
        product.category = None
        product.rotation = None

        mock_product_service.get_all.return_value = ([product], 1)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_recent_sales_all.return_value = {}

        result = stockout_service.get_products_by_status(StockoutStatus.YOUR_CALL)

        assert len(result) == 1
        assert result[0].status == StockoutStatus.YOUR_CALL


# ===================
# CALCULATION TESTS
# ===================

class TestCalculations:
    """Tests for calculation accuracy."""

    def test_avg_daily_sales_calculation(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product,
        sample_inventory
    ):
        """Average daily sales is calculated correctly from weekly data."""
        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [sample_inventory]

        # 4 weeks at 140 m²/week = 560 total / 28 days = 20 m²/day
        sales = [MagicMock(quantity_m2=Decimal("140")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.avg_daily_sales == Decimal("20.00")
        assert result.weekly_sales == Decimal("140.00")
        assert result.weeks_of_data == 4

    def test_days_to_stockout_calculation(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Days to stockout is calculated correctly."""
        inventory = MagicMock()
        inventory.warehouse_qty = 200
        inventory.in_transit_qty = 0

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        # 200 / 10 = 20 days
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.days_to_stockout == Decimal("20.0")

    def test_no_inventory_snapshot(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Handles missing inventory snapshot (defaults to 0)."""
        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = []  # No inventory

        sales = [MagicMock(quantity_m2=Decimal("100")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.warehouse_qty == Decimal("0")
        assert result.in_transit_qty == Decimal("0")
        assert result.total_qty == Decimal("0")


# ===================
# BOAT FALLBACK TESTS
# ===================

class TestBoatFallback:
    """Tests for fallback behavior when no boats scheduled."""

    def test_fallback_to_lead_time_when_no_boats(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        mock_boat_service,
        sample_product
    ):
        """Uses lead_time as threshold when no boats scheduled."""
        # No boats scheduled
        mock_boat_service.get_next_two_arrivals.return_value = (None, None)

        inventory = MagicMock()
        inventory.warehouse_qty = 70
        inventory.in_transit_qty = 0

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        # 70 / 10 = 7 days < 45 (lead_time) → HIGH_PRIORITY
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        # With 7 days of stock and lead_time of 45 days, should be HIGH_PRIORITY
        assert result.status == StockoutStatus.HIGH_PRIORITY


# ===================
# SINGLETON TESTS
# ===================

class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_stockout_service_returns_same_instance(
        self,
        mock_inventory_service,
        mock_sales_service,
        mock_product_service,
        mock_boat_service,
        mock_settings
    ):
        """get_stockout_service returns the same instance."""
        # Reset singleton
        import services.stockout_service as module
        module._stockout_service = None

        service1 = get_stockout_service()
        service2 = get_stockout_service()

        assert service1 is service2
