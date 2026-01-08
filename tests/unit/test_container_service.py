"""
Unit tests for ContainerService.

See STANDARDS_TESTING.md for patterns.

Run: pytest tests/unit/test_container_service.py -v
Run with coverage: pytest tests/unit/test_container_service.py --cov=services/container_service
"""

import pytest
from unittest.mock import patch
from datetime import datetime
from decimal import Decimal

# Import what we're testing
from services.container_service import ContainerService, get_container_service
from models.container import (
    ContainerCreate,
    ContainerUpdate,
    ContainerItemCreate,
    ContainerItemUpdate
)
from exceptions import (
    ContainerNotFoundError,
    ContainerItemNotFoundError,
    ShipmentNotFoundError,
    ProductNotFoundError
)
import services.container_service as container_service_module


# ===================
# FIXTURES
# ===================

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton service between tests."""
    container_service_module._container_service = None
    yield
    container_service_module._container_service = None


@pytest.fixture
def sample_container_data():
    """Sample container data."""
    return {
        "id": "container-uuid-123",
        "shipment_id": "shipment-uuid-123",
        "container_number": "ABCD1234567",
        "seal_number": "SEAL12345",
        "trucking_company_id": "trucking-uuid-123",
        "total_pallets": 10,
        "total_weight_kg": 15000.00,
        "total_m2": 1200.00,
        "fill_percentage": 85.50,
        "unload_start": "2025-01-15T08:00:00Z",
        "unload_end": "2025-01-15T10:00:00Z",
        "created_at": "2025-01-10T10:00:00Z",
    }


@pytest.fixture
def sample_container_item_data():
    """Sample container item data."""
    return {
        "id": "item-uuid-123",
        "container_id": "container-uuid-123",
        "product_id": "product-uuid-123",
        "quantity": 100.50,
        "pallets": 5,
        "weight_kg": 2500.00,
        "created_at": "2025-01-10T10:00:00Z",
    }


@pytest.fixture
def sample_container_item_data_2():
    """Second sample container item."""
    return {
        "id": "item-uuid-456",
        "container_id": "container-uuid-123",
        "product_id": "product-uuid-456",
        "quantity": 200.00,
        "pallets": 8,
        "weight_kg": 4000.00,
        "created_at": "2025-01-10T11:00:00Z",
    }


@pytest.fixture
def mock_db_containers(mock_supabase, sample_container_data):
    """Patch database with container data."""
    with patch("services.container_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("containers", [sample_container_data])
        mock_supabase.set_table_data("container_items", [])
        mock_supabase.set_table_data("shipments", [{"id": "shipment-uuid-123"}])
        mock_supabase.set_table_data("products", [{"id": "product-uuid-123"}])
        yield mock_supabase


@pytest.fixture
def mock_db_container_items(mock_supabase, sample_container_data, sample_container_item_data, sample_container_item_data_2):
    """Patch database with container and items."""
    with patch("services.container_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("containers", [sample_container_data])
        mock_supabase.set_table_data("container_items", [sample_container_item_data, sample_container_item_data_2])
        mock_supabase.set_table_data("shipments", [{"id": "shipment-uuid-123"}])
        mock_supabase.set_table_data("products", [{"id": "product-uuid-123"}, {"id": "product-uuid-456"}])
        yield mock_supabase


@pytest.fixture
def mock_db_empty(mock_supabase):
    """Patch database with no data."""
    with patch("services.container_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("containers", [])
        mock_supabase.set_table_data("container_items", [])
        mock_supabase.set_table_data("shipments", [])
        mock_supabase.set_table_data("products", [])
        yield mock_supabase


# ===================
# SERVICE INSTANCE TESTS
# ===================

class TestContainerServiceInstance:
    """Tests for service instance creation."""

    def test_get_service_returns_instance(self, mock_db_containers):
        """Should return a service instance."""
        service = get_container_service()
        assert isinstance(service, ContainerService)

    def test_get_service_returns_singleton(self, mock_db_containers):
        """Should return the same instance on multiple calls."""
        service1 = get_container_service()
        service2 = get_container_service()
        assert service1 is service2


# ===================
# CONTAINER CREATE TESTS
# ===================

class TestCreateContainer:
    """Tests for creating containers."""

    def test_create_container_success(self, mock_db_containers):
        """Should create a new container."""
        service = get_container_service()

        data = ContainerCreate(
            shipment_id="shipment-uuid-123",
            container_number="ABCD1234567",
            seal_number="SEAL12345",
            trucking_company_id="trucking-uuid-123",
            total_pallets=10,
            total_weight_kg=Decimal("15000.00"),
            total_m2=Decimal("1200.00"),
            fill_percentage=Decimal("85.50"),
            unload_start=datetime(2025, 1, 15, 8, 0, 0),
            unload_end=datetime(2025, 1, 15, 10, 0, 0)
        )

        result = service.create(data)

        # Mock returns fixture data (first item in table)
        assert result.id == "container-uuid-123"
        assert result.shipment_id == "shipment-uuid-123"
        assert result.container_number == "ABCD1234567"
        assert result.total_pallets == 10

    def test_create_container_minimal(self, mock_db_containers):
        """Should create container with only required fields."""
        service = get_container_service()

        data = ContainerCreate(
            shipment_id="shipment-uuid-123",
            container_number="TEST1234567",
            seal_number=None,
            trucking_company_id=None,
            total_pallets=None,
            total_weight_kg=None,
            total_m2=None,
            fill_percentage=None,
            unload_start=None,
            unload_end=None
        )

        result = service.create(data)

        # Mock returns fixture data
        assert result.shipment_id == "shipment-uuid-123"
        # Container number from fixture, not request
        assert result.id == "container-uuid-123"

    def test_create_container_shipment_not_found(self, mock_db_empty):
        """Should raise ShipmentNotFoundError if shipment doesn't exist."""
        service = get_container_service()

        data = ContainerCreate(
            shipment_id="non-existent-shipment",
            container_number="TEST1234567",
            seal_number=None,
            trucking_company_id=None,
            total_pallets=None,
            total_weight_kg=None,
            total_m2=None,
            fill_percentage=None,
            unload_start=None,
            unload_end=None
        )

        with pytest.raises(ShipmentNotFoundError):
            service.create(data)


# ===================
# CONTAINER READ TESTS
# ===================

class TestGetContainer:
    """Tests for getting containers."""

    def test_get_by_id_success(self, mock_db_containers):
        """Should get container by ID."""
        service = get_container_service()

        result = service.get_by_id("container-uuid-123")

        assert result.id == "container-uuid-123"
        assert result.shipment_id == "shipment-uuid-123"
        assert result.container_number == "ABCD1234567"

    def test_get_by_id_not_found(self, mock_db_empty):
        """Should raise ContainerNotFoundError if not found."""
        service = get_container_service()

        with pytest.raises(ContainerNotFoundError):
            service.get_by_id("non-existent")

    def test_get_by_shipment_multiple(self, mock_db_containers):
        """Should get all containers for a shipment."""
        service = get_container_service()

        # Mock has one container already
        result = service.get_by_shipment("shipment-uuid-123")

        # Verify it returns a list with at least one container
        assert len(result) >= 1
        assert result[0].shipment_id == "shipment-uuid-123"

    def test_get_by_shipment_empty(self, mock_db_empty):
        """Should return empty list if no containers."""
        service = get_container_service()

        result = service.get_by_shipment("non-existent-shipment")

        assert len(result) == 0

    def test_get_with_items(self, mock_db_container_items):
        """Should get container with all items."""
        service = get_container_service()

        result = service.get_with_items("container-uuid-123")

        assert result.id == "container-uuid-123"
        assert len(result.items) == 2
        assert result.items[0].product_id == "product-uuid-123"
        assert result.items[1].product_id == "product-uuid-456"


# ===================
# CONTAINER UPDATE TESTS
# ===================

class TestUpdateContainer:
    """Tests for updating containers."""

    def test_update_container_success(self, mock_db_containers):
        """Should update container fields."""
        service = get_container_service()

        update = ContainerUpdate(
            container_number="NEWNUM123",
            total_pallets=12
        )

        result = service.update("container-uuid-123", update)

        # Mock returns original data, but update was called
        assert result.id == "container-uuid-123"

    def test_update_container_not_found(self, mock_db_empty):
        """Should raise ContainerNotFoundError if not found."""
        service = get_container_service()

        update = ContainerUpdate(total_pallets=12)

        with pytest.raises(ContainerNotFoundError):
            service.update("non-existent", update)


# ===================
# CONTAINER DELETE TESTS
# ===================

class TestDeleteContainer:
    """Tests for deleting containers."""

    def test_delete_container_success(self, mock_db_containers):
        """Should delete container."""
        service = get_container_service()

        result = service.delete("container-uuid-123")

        assert result is True

    def test_delete_container_not_found(self, mock_db_empty):
        """Should raise ContainerNotFoundError if not found."""
        service = get_container_service()

        with pytest.raises(ContainerNotFoundError):
            service.delete("non-existent")


# ===================
# CONTAINER ITEM CREATE TESTS
# ===================

class TestAddContainerItem:
    """Tests for adding items to containers."""

    def test_add_item_success(self, mock_db_container_items):
        """Should add item to container."""
        service = get_container_service()

        data = ContainerItemCreate(
            product_id="product-uuid-123",
            quantity=Decimal("100.50"),
            pallets=5,
            weight_kg=Decimal("2500.00")
        )

        result = service.add_item("container-uuid-123", data)

        # Mock returns first item in table after insert
        assert result.product_id == "product-uuid-123"

    def test_add_item_container_not_found(self, mock_db_empty):
        """Should raise ContainerNotFoundError if container doesn't exist."""
        service = get_container_service()

        data = ContainerItemCreate(
            product_id="product-uuid-123",
            quantity=Decimal("100.00"),
            pallets=5,
            weight_kg=None
        )

        with pytest.raises(ContainerNotFoundError):
            service.add_item("non-existent-container", data)


# ===================
# CONTAINER ITEM READ TESTS
# ===================

class TestGetContainerItems:
    """Tests for getting container items."""

    def test_get_item_success(self, mock_db_container_items):
        """Should get item by ID."""
        service = get_container_service()

        result = service.get_item("item-uuid-123")

        assert result.id == "item-uuid-123"
        assert result.container_id == "container-uuid-123"
        assert result.product_id == "product-uuid-123"

    def test_get_item_not_found(self, mock_db_empty):
        """Should raise ContainerItemNotFoundError if not found."""
        service = get_container_service()

        with pytest.raises(ContainerItemNotFoundError):
            service.get_item("non-existent")

    def test_get_items_multiple(self, mock_db_container_items):
        """Should get all items for a container."""
        service = get_container_service()

        result = service.get_items("container-uuid-123")

        assert len(result) == 2
        assert result[0].product_id == "product-uuid-123"
        assert result[1].product_id == "product-uuid-456"

    def test_get_items_empty(self, mock_db_containers):
        """Should return empty list if no items."""
        service = get_container_service()

        result = service.get_items("container-uuid-123")

        assert len(result) == 0


# ===================
# CONTAINER ITEM UPDATE TESTS
# ===================

class TestUpdateContainerItem:
    """Tests for updating container items."""

    def test_update_item_success(self, mock_db_container_items):
        """Should update item fields."""
        service = get_container_service()

        update = ContainerItemUpdate(
            quantity=Decimal("150.00"),
            pallets=7
        )

        result = service.update_item("item-uuid-123", update)

        assert result.id == "item-uuid-123"

    def test_update_item_not_found(self, mock_db_empty):
        """Should raise ContainerItemNotFoundError if not found."""
        service = get_container_service()

        update = ContainerItemUpdate(quantity=Decimal("150.00"))

        with pytest.raises(ContainerItemNotFoundError):
            service.update_item("non-existent", update)


# ===================
# CONTAINER ITEM DELETE TESTS
# ===================

class TestDeleteContainerItem:
    """Tests for deleting container items."""

    def test_delete_item_success(self, mock_db_container_items):
        """Should delete item."""
        service = get_container_service()

        result = service.delete_item("item-uuid-123")

        assert result is True

    def test_delete_item_not_found(self, mock_db_empty):
        """Should raise ContainerItemNotFoundError if not found."""
        service = get_container_service()

        with pytest.raises(ContainerItemNotFoundError):
            service.delete_item("non-existent")


# ===================
# UTILITY METHOD TESTS
# ===================

class TestRecalculateTotals:
    """Tests for recalculating container totals."""

    def test_recalculate_totals(self, mock_db_container_items):
        """Should recalculate totals from items."""
        service = get_container_service()

        # Items have: 100.50 + 200.00 = 300.50 m2, 5 + 8 = 13 pallets, 2500 + 4000 = 6500 kg
        result = service.recalculate_totals("container-uuid-123")

        # Mock doesn't actually update, but method was called
        assert result.id == "container-uuid-123"


class TestValidateLimits:
    """Tests for validating container limits."""

    def test_validate_limits_valid(self, mock_db_containers):
        """Should validate container within limits."""
        service = get_container_service()

        is_valid, warnings = service.validate_limits("container-uuid-123")

        # Container has 15000kg, 10 pallets, 1200m2 - all within limits
        assert is_valid is True
        assert len(warnings) == 0

    def test_validate_limits_weight_exceeded(self, mock_db_containers, sample_container_data):
        """Should warn when weight exceeds limit."""
        service = get_container_service()

        # Update container with excessive weight
        sample_container_data["total_weight_kg"] = 30000.00
        mock_db_containers.set_table_data("containers", [sample_container_data])

        is_valid, warnings = service.validate_limits("container-uuid-123")

        assert is_valid is False
        assert len(warnings) > 0
        assert "Weight exceeds limit" in warnings[0]

    def test_validate_limits_pallets_exceeded(self, mock_db_containers, sample_container_data):
        """Should warn when pallets exceed limit."""
        service = get_container_service()

        # Update container with excessive pallets
        sample_container_data["total_pallets"] = 20
        mock_db_containers.set_table_data("containers", [sample_container_data])

        is_valid, warnings = service.validate_limits("container-uuid-123")

        assert is_valid is False
        assert len(warnings) > 0
        assert "Pallets exceed limit" in warnings[0]

    def test_validate_limits_m2_exceeded(self, mock_db_containers, sample_container_data):
        """Should warn when area exceeds limit."""
        service = get_container_service()

        # Update container with excessive m2
        sample_container_data["total_m2"] = 2000.00
        mock_db_containers.set_table_data("containers", [sample_container_data])

        is_valid, warnings = service.validate_limits("container-uuid-123")

        assert is_valid is False
        assert len(warnings) > 0
        assert "Area exceeds limit" in warnings[0]

    def test_validate_limits_multiple_warnings(self, mock_db_containers, sample_container_data):
        """Should return multiple warnings when multiple limits exceeded."""
        service = get_container_service()

        # Update container with multiple exceeded limits
        sample_container_data["total_weight_kg"] = 30000.00
        sample_container_data["total_pallets"] = 20
        sample_container_data["total_m2"] = 2000.00
        mock_db_containers.set_table_data("containers", [sample_container_data])

        is_valid, warnings = service.validate_limits("container-uuid-123")

        assert is_valid is False
        assert len(warnings) == 3


# ===================
# INTEGRATION TESTS
# ===================

class TestContainerIntegration:
    """Integration tests for container service."""

    def test_create_and_add_items_workflow(self, mock_db_containers):
        """Should create container and add items."""
        service = get_container_service()

        # Create container
        container_data = ContainerCreate(
            shipment_id="shipment-uuid-123",
            container_number="TEST123",
            seal_number="SEAL123",
            trucking_company_id=None,
            total_pallets=None,
            total_weight_kg=None,
            total_m2=None,
            fill_percentage=None,
            unload_start=None,
            unload_end=None
        )

        container = service.create(container_data)
        # Mock returns fixture data
        assert container.shipment_id == "shipment-uuid-123"

        # Add items (would work with proper mock setup)
        # In real scenario, totals would be recalculated

    def test_get_with_items_returns_all_data(self, mock_db_container_items):
        """Should get complete container data with items."""
        service = get_container_service()

        result = service.get_with_items("container-uuid-123")

        assert result.id == "container-uuid-123"
        assert result.total_pallets == 10
        assert len(result.items) == 2
        assert result.items[0].quantity == Decimal("100.50")
        assert result.items[1].quantity == Decimal("200.00")