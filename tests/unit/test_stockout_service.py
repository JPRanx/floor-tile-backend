"""
Unit tests for StockoutService.

Tests cover stockout calculation logic for all status types.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
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
def mock_settings():
    """Mock settings."""
    with patch("services.stockout_service.settings") as mock:
        mock.lead_time_days = 45
        yield mock


@pytest.fixture
def stockout_service(mock_inventory_service, mock_sales_service, mock_product_service, mock_settings):
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
# CRITICAL STATUS TESTS
# ===================

class TestCriticalStatus:
    """Tests for CRITICAL stockout status."""

    def test_critical_when_days_below_lead_time(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product,
        sample_inventory
    ):
        """Product is CRITICAL when days to stockout < lead time (45)."""
        # Setup: 100 m² inventory, 10 m²/day sales = 10 days (< 45)
        sample_inventory.warehouse_qty = 70
        sample_inventory.in_transit_qty = 0

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [sample_inventory]

        # 4 weeks of sales at 70 m²/week = 10 m²/day
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.CRITICAL
        assert result.days_to_stockout < 45

    def test_critical_with_zero_inventory(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product
    ):
        """Product is CRITICAL when inventory is zero with sales."""
        # Setup: 0 inventory, any sales velocity
        inventory = MagicMock()
        inventory.warehouse_qty = 0
        inventory.in_transit_qty = 0

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        sales = [MagicMock(quantity_m2=Decimal("100")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.CRITICAL
        assert result.days_to_stockout == Decimal("0")


# ===================
# WARNING STATUS TESTS
# ===================

class TestWarningStatus:
    """Tests for WARNING stockout status."""

    def test_warning_when_days_between_lead_and_threshold(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product
    ):
        """Product is WARNING when 45 <= days < 59."""
        # Setup: 500 m² inventory, 10 m²/day = 50 days (between 45 and 59)
        inventory = MagicMock()
        inventory.warehouse_qty = 500
        inventory.in_transit_qty = 0

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.WARNING
        assert 45 <= result.days_to_stockout < 59


# ===================
# OK STATUS TESTS
# ===================

class TestOkStatus:
    """Tests for OK stockout status."""

    def test_ok_when_days_above_threshold(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product
    ):
        """Product is OK when days >= 59."""
        # Setup: 700 m² inventory, 10 m²/day = 70 days (>= 59)
        inventory = MagicMock()
        inventory.warehouse_qty = 700
        inventory.in_transit_qty = 0

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        # 4 weeks at 70 m²/week = 10 m²/day
        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.OK
        assert result.days_to_stockout >= 59

    def test_ok_with_in_transit_included(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product
    ):
        """In-transit inventory is included in calculation."""
        # Setup: 300 warehouse + 400 in_transit = 700 total, 10/day = 70 days
        inventory = MagicMock()
        inventory.warehouse_qty = 300
        inventory.in_transit_qty = 400

        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [inventory]

        sales = [MagicMock(quantity_m2=Decimal("70")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.total_qty == Decimal("700")
        assert result.status == StockoutStatus.OK


# ===================
# NO_SALES STATUS TESTS
# ===================

class TestNoSalesStatus:
    """Tests for NO_SALES stockout status."""

    def test_no_sales_when_no_history(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product,
        sample_inventory
    ):
        """Product is NO_SALES when no sales history exists."""
        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [sample_inventory]
        mock_sales_service.get_history.return_value = []  # No sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.NO_SALES
        assert result.days_to_stockout is None
        assert "No sales history" in result.status_reason

    def test_no_sales_when_zero_sales(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service,
        sample_product,
        sample_inventory
    ):
        """Product is NO_SALES when sales are all zero."""
        mock_product_service.get_by_id.return_value = sample_product
        mock_inventory_service.get_history.return_value = [sample_inventory]

        # Sales records with zero quantity
        sales = [MagicMock(quantity_m2=Decimal("0")) for _ in range(4)]
        mock_sales_service.get_history.return_value = sales

        result = stockout_service.calculate_for_product("product-uuid-123")

        assert result.status == StockoutStatus.NO_SALES
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
        sample_product
    ):
        """calculate_all returns StockoutSummary with all products."""
        mock_product_service.get_all.return_value = ([sample_product], 1)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_history.return_value = []

        result = stockout_service.calculate_all()

        assert isinstance(result, StockoutSummary)
        assert result.total_products == 1
        assert result.lead_time_days == 45
        assert result.warning_threshold_days == 59

    def test_calculate_all_counts_statuses(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service
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
        mock_sales_service.get_history.return_value = []  # All will be NO_SALES

        result = stockout_service.calculate_all()

        assert result.total_products == 3
        assert result.no_sales_count == 3


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
        mock_sales_service
    ):
        """get_critical_products returns only CRITICAL products."""
        # One product with no sales (not critical)
        product = MagicMock()
        product.id = "product-1"
        product.sku = "SKU-1"
        product.category = None
        product.rotation = None

        mock_product_service.get_all.return_value = ([product], 1)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_history.return_value = []

        result = stockout_service.get_critical_products()

        # No sales = NO_SALES status, not CRITICAL
        assert len(result) == 0

    def test_get_products_by_status(
        self,
        stockout_service,
        mock_product_service,
        mock_inventory_service,
        mock_sales_service
    ):
        """get_products_by_status filters correctly."""
        product = MagicMock()
        product.id = "product-1"
        product.sku = "SKU-1"
        product.category = None
        product.rotation = None

        mock_product_service.get_all.return_value = ([product], 1)
        mock_inventory_service.get_latest.return_value = []
        mock_sales_service.get_history.return_value = []

        result = stockout_service.get_products_by_status(StockoutStatus.NO_SALES)

        assert len(result) == 1
        assert result[0].status == StockoutStatus.NO_SALES


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
# SINGLETON TESTS
# ===================

class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_stockout_service_returns_same_instance(
        self,
        mock_inventory_service,
        mock_sales_service,
        mock_product_service,
        mock_settings
    ):
        """get_stockout_service returns the same instance."""
        # Reset singleton
        import services.stockout_service as module
        module._stockout_service = None

        service1 = get_stockout_service()
        service2 = get_stockout_service()

        assert service1 is service2
