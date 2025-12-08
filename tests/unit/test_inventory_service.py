"""
Unit tests for Inventory service.

Tests CRUD operations, bulk insert, and latest retrieval.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch
import pytest

from services.inventory_service import InventoryService, get_inventory_service
from models.inventory import (
    InventorySnapshotCreate,
    InventorySnapshotUpdate,
    InventorySnapshotResponse,
)
from exceptions import InventoryNotFoundError, DatabaseError


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_supabase():
    """Mock Supabase client."""
    with patch("services.inventory_service.get_supabase_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def inventory_service(mock_supabase):
    """Create InventoryService with mocked Supabase."""
    # Reset singleton
    import services.inventory_service as module
    module._inventory_service = None
    return InventoryService()


@pytest.fixture
def sample_snapshot_data():
    """Sample inventory snapshot data."""
    return {
        "id": "snapshot-uuid-123",
        "product_id": "product-uuid-456",
        "warehouse_qty": 1500.50,
        "in_transit_qty": 250.00,
        "snapshot_date": "2025-12-01",
        "notes": "Weekly count",
        "created_at": "2025-12-01T10:00:00Z"
    }


@pytest.fixture
def sample_snapshot_response(sample_snapshot_data):
    """Sample InventorySnapshotResponse."""
    return InventorySnapshotResponse(**sample_snapshot_data)


@pytest.fixture
def today():
    """Today's date."""
    return date.today()


@pytest.fixture
def yesterday():
    """Yesterday's date."""
    return date.today() - timedelta(days=1)


# ===================
# GET ALL TESTS
# ===================

class TestGetAll:
    """Tests for get_all method."""

    def test_get_all_returns_snapshots(self, inventory_service, mock_supabase, sample_snapshot_data):
        """get_all returns list of snapshots."""
        # Mock chain: select().range().order().execute()
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data],
            count=1
        )

        snapshots, total = inventory_service.get_all()

        assert len(snapshots) == 1
        assert total == 1
        assert snapshots[0].id == "snapshot-uuid-123"

    def test_get_all_with_product_filter(self, inventory_service, mock_supabase, sample_snapshot_data):
        """get_all filters by product_id."""
        # Mock chain: select().eq().range().order().execute()
        mock_result = MagicMock(data=[sample_snapshot_data], count=1)
        mock_supabase.table.return_value.select.return_value.eq.return_value.range.return_value.order.return_value.execute.return_value = mock_result

        snapshots, total = inventory_service.get_all(product_id="product-uuid-456")

        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("product_id", "product-uuid-456")
        assert len(snapshots) == 1

    def test_get_all_empty_returns_empty_list(self, inventory_service, mock_supabase):
        """get_all returns empty list when no snapshots."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[],
            count=0
        )

        snapshots, total = inventory_service.get_all()

        assert len(snapshots) == 0
        assert total == 0

    def test_get_all_with_pagination(self, inventory_service, mock_supabase, sample_snapshot_data):
        """get_all respects pagination parameters."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data],
            count=50
        )

        snapshots, total = inventory_service.get_all(page=2, page_size=10)

        # Should call range with offset 10-19 for page 2
        mock_supabase.table.return_value.select.return_value.range.assert_called_with(10, 19)


# ===================
# GET BY ID TESTS
# ===================

class TestGetById:
    """Tests for get_by_id method."""

    def test_get_by_id_returns_snapshot(self, inventory_service, mock_supabase, sample_snapshot_data):
        """get_by_id returns snapshot when found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_snapshot_data
        )

        snapshot = inventory_service.get_by_id("snapshot-uuid-123")

        assert snapshot.id == "snapshot-uuid-123"
        assert snapshot.warehouse_qty == 1500.50

    def test_get_by_id_not_found_raises_error(self, inventory_service, mock_supabase):
        """get_by_id raises InventoryNotFoundError when not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=None
        )

        with pytest.raises(InventoryNotFoundError) as exc_info:
            inventory_service.get_by_id("nonexistent-id")

        assert "nonexistent-id" in str(exc_info.value.details)


# ===================
# GET HISTORY TESTS
# ===================

class TestGetHistory:
    """Tests for get_history method."""

    def test_get_history_returns_snapshots(self, inventory_service, mock_supabase, sample_snapshot_data):
        """get_history returns list of snapshots for product."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data, sample_snapshot_data]
        )

        snapshots = inventory_service.get_history("product-uuid-456")

        assert len(snapshots) == 2

    def test_get_history_empty_returns_empty_list(self, inventory_service, mock_supabase):
        """get_history returns empty list when no snapshots."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[]
        )

        snapshots = inventory_service.get_history("product-uuid-456")

        assert len(snapshots) == 0

    def test_get_history_respects_limit(self, inventory_service, mock_supabase, sample_snapshot_data):
        """get_history respects limit parameter."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data]
        )

        inventory_service.get_history("product-uuid-456", limit=10)

        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.assert_called_with(10)


# ===================
# GET LATEST TESTS
# ===================

class TestGetLatest:
    """Tests for get_latest method."""

    def test_get_latest_returns_one_per_product(self, inventory_service, mock_supabase):
        """get_latest returns only the latest snapshot per product."""
        # Two snapshots for same product, different dates
        snapshots_data = [
            {
                "id": "snap-1",
                "product_id": "prod-1",
                "warehouse_qty": 100,
                "in_transit_qty": 0,
                "snapshot_date": "2025-12-05",
                "notes": None,
                "created_at": "2025-12-05T10:00:00Z",
                "products": {"sku": "NOGAL CAFÉ", "category": "MADERAS", "rotation": "ALTA"}
            },
            {
                "id": "snap-2",
                "product_id": "prod-1",
                "warehouse_qty": 90,
                "in_transit_qty": 0,
                "snapshot_date": "2025-12-01",
                "notes": None,
                "created_at": "2025-12-01T10:00:00Z",
                "products": {"sku": "NOGAL CAFÉ", "category": "MADERAS", "rotation": "ALTA"}
            },
        ]

        mock_supabase.table.return_value.select.return_value.order.return_value.order.return_value.execute.return_value = MagicMock(
            data=snapshots_data
        )

        result = inventory_service.get_latest()

        # Should only return 1 (the latest)
        assert len(result) == 1
        assert result[0].id == "snap-1"
        assert result[0].sku == "NOGAL CAFÉ"

    def test_get_latest_empty_returns_empty_list(self, inventory_service, mock_supabase):
        """get_latest returns empty list when no snapshots."""
        mock_supabase.table.return_value.select.return_value.order.return_value.order.return_value.execute.return_value = MagicMock(
            data=[]
        )

        result = inventory_service.get_latest()

        assert len(result) == 0


# ===================
# CREATE TESTS
# ===================

class TestCreate:
    """Tests for create method."""

    def test_create_returns_snapshot(self, inventory_service, mock_supabase, sample_snapshot_data, yesterday):
        """create returns created snapshot."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data]
        )

        data = InventorySnapshotCreate(
            product_id="product-uuid-456",
            warehouse_qty=1500.50,
            in_transit_qty=250.00,
            snapshot_date=yesterday,
            notes="Weekly count"
        )

        snapshot = inventory_service.create(data)

        assert snapshot.id == "snapshot-uuid-123"

    def test_create_calls_insert_with_correct_data(self, inventory_service, mock_supabase, sample_snapshot_data, yesterday):
        """create calls insert with correct data."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data]
        )

        data = InventorySnapshotCreate(
            product_id="product-uuid-456",
            warehouse_qty=1500.50,
            in_transit_qty=250.00,
            snapshot_date=yesterday,
        )

        inventory_service.create(data)

        # Verify insert was called
        mock_supabase.table.return_value.insert.assert_called_once()
        call_args = mock_supabase.table.return_value.insert.call_args[0][0]
        assert call_args["product_id"] == "product-uuid-456"
        assert call_args["warehouse_qty"] == 1500.50


# ===================
# BULK CREATE TESTS
# ===================

class TestBulkCreate:
    """Tests for bulk_create method."""

    def test_bulk_create_returns_created_snapshots(self, inventory_service, mock_supabase, sample_snapshot_data, yesterday):
        """bulk_create returns list of created snapshots."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data, sample_snapshot_data]
        )

        data = [
            InventorySnapshotCreate(
                product_id="product-uuid-1",
                warehouse_qty=100,
                in_transit_qty=0,
                snapshot_date=yesterday,
            ),
            InventorySnapshotCreate(
                product_id="product-uuid-2",
                warehouse_qty=200,
                in_transit_qty=50,
                snapshot_date=yesterday,
            ),
        ]

        snapshots = inventory_service.bulk_create(data)

        assert len(snapshots) == 2

    def test_bulk_create_empty_list_returns_empty(self, inventory_service, mock_supabase):
        """bulk_create with empty list returns empty list."""
        result = inventory_service.bulk_create([])

        assert len(result) == 0
        # Should not call insert
        mock_supabase.table.return_value.insert.assert_not_called()

    def test_bulk_create_inserts_all_records(self, inventory_service, mock_supabase, sample_snapshot_data, yesterday):
        """bulk_create inserts all records in one call."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_snapshot_data] * 3
        )

        data = [
            InventorySnapshotCreate(
                product_id=f"product-uuid-{i}",
                warehouse_qty=100 * i,
                in_transit_qty=0,
                snapshot_date=yesterday,
            )
            for i in range(3)
        ]

        inventory_service.bulk_create(data)

        # Should be called once with list of 3 records
        mock_supabase.table.return_value.insert.assert_called_once()
        call_args = mock_supabase.table.return_value.insert.call_args[0][0]
        assert len(call_args) == 3


# ===================
# UPDATE TESTS
# ===================

class TestUpdate:
    """Tests for update method."""

    def test_update_returns_updated_snapshot(self, inventory_service, mock_supabase, sample_snapshot_data):
        """update returns updated snapshot."""
        # Mock get_by_id
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_snapshot_data
        )
        # Mock update
        updated_data = {**sample_snapshot_data, "warehouse_qty": 2000.00}
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[updated_data]
        )

        data = InventorySnapshotUpdate(warehouse_qty=2000.00)
        snapshot = inventory_service.update("snapshot-uuid-123", data)

        assert snapshot.warehouse_qty == 2000.00

    def test_update_not_found_raises_error(self, inventory_service, mock_supabase):
        """update raises InventoryNotFoundError when not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=None
        )

        data = InventorySnapshotUpdate(warehouse_qty=2000.00)

        with pytest.raises(InventoryNotFoundError):
            inventory_service.update("nonexistent-id", data)

    def test_update_empty_data_returns_existing(self, inventory_service, mock_supabase, sample_snapshot_data):
        """update with no changes returns existing snapshot."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_snapshot_data
        )

        data = InventorySnapshotUpdate()  # No fields set
        snapshot = inventory_service.update("snapshot-uuid-123", data)

        # Should not call update
        mock_supabase.table.return_value.update.assert_not_called()
        assert snapshot.id == "snapshot-uuid-123"


# ===================
# DELETE TESTS
# ===================

class TestDelete:
    """Tests for delete method."""

    def test_delete_returns_true(self, inventory_service, mock_supabase, sample_snapshot_data):
        """delete returns True when successful."""
        # Mock get_by_id
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_snapshot_data
        )
        # Mock delete
        mock_supabase.table.return_value.delete.return_value.eq.return_value.execute.return_value = MagicMock()

        result = inventory_service.delete("snapshot-uuid-123")

        assert result is True

    def test_delete_not_found_raises_error(self, inventory_service, mock_supabase):
        """delete raises InventoryNotFoundError when not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=None
        )

        with pytest.raises(InventoryNotFoundError):
            inventory_service.delete("nonexistent-id")


# ===================
# COUNT TESTS
# ===================

class TestCount:
    """Tests for count method."""

    def test_count_returns_total(self, inventory_service, mock_supabase):
        """count returns total number of snapshots."""
        mock_supabase.table.return_value.select.return_value.execute.return_value = MagicMock(
            count=42
        )

        result = inventory_service.count()

        assert result == 42

    def test_count_with_product_filter(self, inventory_service, mock_supabase):
        """count filters by product_id."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            count=5
        )

        result = inventory_service.count(product_id="product-uuid-456")

        assert result == 5
        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("product_id", "product-uuid-456")


# ===================
# SINGLETON TESTS
# ===================

class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_inventory_service_returns_same_instance(self, mock_supabase):
        """get_inventory_service returns same instance."""
        # Reset singleton
        import services.inventory_service as module
        module._inventory_service = None

        service1 = get_inventory_service()
        service2 = get_inventory_service()

        assert service1 is service2


# ===================
# MODEL VALIDATION TESTS
# ===================

class TestModelValidation:
    """Tests for Pydantic model validation."""

    def test_create_model_rounds_quantities(self, yesterday):
        """InventorySnapshotCreate rounds quantities to 2 decimals."""
        data = InventorySnapshotCreate(
            product_id="product-uuid",
            warehouse_qty=1500.5678,
            in_transit_qty=250.1234,
            snapshot_date=yesterday
        )

        assert data.warehouse_qty == 1500.57
        assert data.in_transit_qty == 250.12

    def test_create_model_rejects_future_date(self):
        """InventorySnapshotCreate rejects future date."""
        future = date.today() + timedelta(days=30)

        with pytest.raises(ValueError) as exc_info:
            InventorySnapshotCreate(
                product_id="product-uuid",
                warehouse_qty=100,
                snapshot_date=future
            )

        assert "future" in str(exc_info.value).lower()

    def test_create_model_rejects_negative_quantity(self, yesterday):
        """InventorySnapshotCreate rejects negative quantities."""
        with pytest.raises(ValueError):
            InventorySnapshotCreate(
                product_id="product-uuid",
                warehouse_qty=-100,
                snapshot_date=yesterday
            )

    def test_update_model_allows_partial_update(self):
        """InventorySnapshotUpdate allows partial updates."""
        data = InventorySnapshotUpdate(warehouse_qty=500)

        assert data.warehouse_qty == 500
        assert data.in_transit_qty is None
        assert data.snapshot_date is None
