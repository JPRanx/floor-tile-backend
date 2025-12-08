"""
Unit tests for SalesService.

Tests cover CRUD operations and bulk create for sales records.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from decimal import Decimal

from services.sales_service import SalesService, get_sales_service
from models.sales import SalesRecordCreate, SalesRecordUpdate
from exceptions import SalesNotFoundError, DatabaseError


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_supabase():
    """Mock Supabase client."""
    with patch("services.sales_service.get_supabase_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def sales_service(mock_supabase):
    """Create SalesService with mocked database."""
    return SalesService()


@pytest.fixture
def sample_sales_data():
    """Sample sales record data from database."""
    return {
        "id": "sales-uuid-123",
        "product_id": "product-uuid-456",
        "week_start": "2025-01-06",
        "quantity_m2": 150.50,
        "created_at": "2025-01-07T10:00:00Z"
    }


@pytest.fixture
def sample_sales_create():
    """Sample sales record creation data."""
    return SalesRecordCreate(
        product_id="product-uuid-456",
        week_start=date(2025, 1, 6),
        quantity_m2=Decimal("150.50")
    )


# ===================
# GET ALL TESTS
# ===================

class TestGetAll:
    """Tests for get_all method."""

    def test_get_all_returns_records(self, sales_service, mock_supabase, sample_sales_data):
        """get_all returns list of sales records."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data],
            count=1
        )

        records, total = sales_service.get_all()

        assert len(records) == 1
        assert total == 1
        assert records[0].id == "sales-uuid-123"

    def test_get_all_with_product_filter(self, sales_service, mock_supabase, sample_sales_data):
        """get_all filters by product_id."""
        mock_result = MagicMock(data=[sample_sales_data], count=1)
        mock_supabase.table.return_value.select.return_value.eq.return_value.range.return_value.order.return_value.execute.return_value = mock_result

        records, total = sales_service.get_all(product_id="product-uuid-456")

        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("product_id", "product-uuid-456")
        assert len(records) == 1

    def test_get_all_with_week_filter(self, sales_service, mock_supabase, sample_sales_data):
        """get_all filters by week_start."""
        mock_result = MagicMock(data=[sample_sales_data], count=1)
        mock_supabase.table.return_value.select.return_value.eq.return_value.range.return_value.order.return_value.execute.return_value = mock_result

        records, total = sales_service.get_all(week_start=date(2025, 1, 6))

        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("week_start", "2025-01-06")

    def test_get_all_empty_returns_empty_list(self, sales_service, mock_supabase):
        """get_all returns empty list when no records."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[],
            count=0
        )

        records, total = sales_service.get_all()

        assert len(records) == 0
        assert total == 0

    def test_get_all_with_pagination(self, sales_service, mock_supabase, sample_sales_data):
        """get_all respects pagination parameters."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data],
            count=50
        )

        records, total = sales_service.get_all(page=2, page_size=10)

        mock_supabase.table.return_value.select.return_value.range.assert_called_with(10, 19)


# ===================
# GET BY ID TESTS
# ===================

class TestGetById:
    """Tests for get_by_id method."""

    def test_get_by_id_returns_record(self, sales_service, mock_supabase, sample_sales_data):
        """get_by_id returns the sales record."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_sales_data
        )

        record = sales_service.get_by_id("sales-uuid-123")

        assert record.id == "sales-uuid-123"
        assert record.product_id == "product-uuid-456"

    def test_get_by_id_not_found_raises_error(self, sales_service, mock_supabase):
        """get_by_id raises SalesNotFoundError when not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("0 rows returned")

        with pytest.raises(SalesNotFoundError):
            sales_service.get_by_id("nonexistent-id")


# ===================
# GET HISTORY TESTS
# ===================

class TestGetHistory:
    """Tests for get_history method."""

    def test_get_history_returns_records(self, sales_service, mock_supabase, sample_sales_data):
        """get_history returns list of records for product."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data]
        )

        records = sales_service.get_history("product-uuid-456")

        assert len(records) == 1
        assert records[0].product_id == "product-uuid-456"

    def test_get_history_empty_returns_empty_list(self, sales_service, mock_supabase):
        """get_history returns empty list when no history."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        records = sales_service.get_history("product-uuid-456")

        assert len(records) == 0

    def test_get_history_respects_limit(self, sales_service, mock_supabase):
        """get_history respects the limit parameter."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        sales_service.get_history("product-uuid-456", limit=10)

        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.assert_called_with(10)


# ===================
# GET WEEKLY TOTALS TESTS
# ===================

class TestGetWeeklyTotals:
    """Tests for get_weekly_totals method."""

    def test_get_weekly_totals_returns_records(self, sales_service, mock_supabase, sample_sales_data):
        """get_weekly_totals returns records for a specific week."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data]
        )

        records = sales_service.get_weekly_totals(date(2025, 1, 6))

        assert len(records) == 1
        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("week_start", "2025-01-06")


# ===================
# CREATE TESTS
# ===================

class TestCreate:
    """Tests for create method."""

    def test_create_returns_record(self, sales_service, mock_supabase, sample_sales_data, sample_sales_create):
        """create returns the created record."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data]
        )

        record = sales_service.create(sample_sales_create)

        assert record.id == "sales-uuid-123"
        assert record.product_id == "product-uuid-456"

    def test_create_calls_insert_with_correct_data(self, sales_service, mock_supabase, sample_sales_data, sample_sales_create):
        """create calls insert with properly formatted data."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data]
        )

        sales_service.create(sample_sales_create)

        mock_supabase.table.return_value.insert.assert_called_once()
        call_args = mock_supabase.table.return_value.insert.call_args[0][0]
        assert call_args["product_id"] == "product-uuid-456"
        assert call_args["week_start"] == "2025-01-06"
        assert call_args["quantity_m2"] == 150.50


# ===================
# BULK CREATE TESTS
# ===================

class TestBulkCreate:
    """Tests for bulk_create method."""

    def test_bulk_create_returns_created_records(self, sales_service, mock_supabase, sample_sales_data, sample_sales_create):
        """bulk_create returns list of created records."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data]
        )

        records = sales_service.bulk_create([sample_sales_create])

        assert len(records) == 1
        assert records[0].id == "sales-uuid-123"

    def test_bulk_create_empty_list_returns_empty(self, sales_service, mock_supabase):
        """bulk_create with empty list returns empty list."""
        records = sales_service.bulk_create([])

        assert len(records) == 0
        mock_supabase.table.return_value.insert.assert_not_called()

    def test_bulk_create_inserts_all_records(self, sales_service, mock_supabase, sample_sales_data, sample_sales_create):
        """bulk_create inserts all records in a single call."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_sales_data, sample_sales_data]
        )

        records = [sample_sales_create, sample_sales_create]
        sales_service.bulk_create(records)

        mock_supabase.table.return_value.insert.assert_called_once()
        call_args = mock_supabase.table.return_value.insert.call_args[0][0]
        assert len(call_args) == 2


# ===================
# UPDATE TESTS
# ===================

class TestUpdate:
    """Tests for update method."""

    def test_update_returns_updated_record(self, sales_service, mock_supabase, sample_sales_data):
        """update returns the updated record."""
        # Mock get_by_id
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_sales_data
        )
        # Mock update
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[{**sample_sales_data, "quantity_m2": 200.00}]
        )

        update_data = SalesRecordUpdate(quantity_m2=Decimal("200.00"))
        record = sales_service.update("sales-uuid-123", update_data)

        assert record.quantity_m2 == Decimal("200.00")

    def test_update_not_found_raises_error(self, sales_service, mock_supabase):
        """update raises SalesNotFoundError when record doesn't exist."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("0 rows returned")

        with pytest.raises(SalesNotFoundError):
            sales_service.update("nonexistent-id", SalesRecordUpdate(quantity_m2=Decimal("100.00")))

    def test_update_empty_data_returns_existing(self, sales_service, mock_supabase, sample_sales_data):
        """update with no changes returns existing record."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_sales_data
        )

        record = sales_service.update("sales-uuid-123", SalesRecordUpdate())

        assert record.id == "sales-uuid-123"
        mock_supabase.table.return_value.update.assert_not_called()


# ===================
# DELETE TESTS
# ===================

class TestDelete:
    """Tests for delete method."""

    def test_delete_returns_true(self, sales_service, mock_supabase, sample_sales_data):
        """delete returns True on success."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_sales_data
        )
        mock_supabase.table.return_value.delete.return_value.eq.return_value.execute.return_value = MagicMock()

        result = sales_service.delete("sales-uuid-123")

        assert result is True

    def test_delete_not_found_raises_error(self, sales_service, mock_supabase):
        """delete raises SalesNotFoundError when record doesn't exist."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("0 rows returned")

        with pytest.raises(SalesNotFoundError):
            sales_service.delete("nonexistent-id")


# ===================
# COUNT TESTS
# ===================

class TestCount:
    """Tests for count method."""

    def test_count_returns_total(self, sales_service, mock_supabase):
        """count returns total record count."""
        mock_supabase.table.return_value.select.return_value.execute.return_value = MagicMock(count=42)

        result = sales_service.count()

        assert result == 42

    def test_count_with_product_filter(self, sales_service, mock_supabase):
        """count filters by product_id."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(count=10)

        result = sales_service.count(product_id="product-uuid-456")

        assert result == 10
        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("product_id", "product-uuid-456")


# ===================
# SINGLETON TESTS
# ===================

class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_sales_service_returns_same_instance(self, mock_supabase):
        """get_sales_service returns the same instance on multiple calls."""
        # Reset singleton
        import services.sales_service as module
        module._sales_service = None

        service1 = get_sales_service()
        service2 = get_sales_service()

        assert service1 is service2


# ===================
# MODEL VALIDATION TESTS
# ===================

class TestModelValidation:
    """Tests for model validation."""

    def test_create_model_rounds_quantity(self):
        """SalesRecordCreate rounds quantity to 2 decimal places."""
        record = SalesRecordCreate(
            product_id="test-id",
            week_start=date(2025, 1, 6),
            quantity_m2=Decimal("150.555")
        )

        assert record.quantity_m2 == Decimal("150.56")

    def test_create_model_rejects_negative_quantity(self):
        """SalesRecordCreate rejects negative quantity."""
        with pytest.raises(ValueError):
            SalesRecordCreate(
                product_id="test-id",
                week_start=date(2025, 1, 6),
                quantity_m2=Decimal("-10.00")
            )

    def test_update_model_allows_partial_update(self):
        """SalesRecordUpdate allows partial updates."""
        update = SalesRecordUpdate(quantity_m2=Decimal("200.00"))

        assert update.quantity_m2 == Decimal("200.00")
        assert update.week_start is None
