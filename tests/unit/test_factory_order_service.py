"""
Unit tests for FactoryOrderService.

See STANDARDS_TESTING.md for patterns.

Run: pytest tests/unit/test_factory_order_service.py -v
Run with coverage: pytest tests/unit/test_factory_order_service.py --cov=services/factory_order_service
"""

import pytest
from unittest.mock import patch
from datetime import date
from decimal import Decimal

# Import what we're testing
from services.factory_order_service import FactoryOrderService, get_factory_order_service
from models.factory_order import (
    FactoryOrderCreate,
    FactoryOrderUpdate,
    FactoryOrderStatusUpdate,
    FactoryOrderItemCreate,
    OrderStatus,
    is_valid_status_transition,
)
from exceptions import (
    FactoryOrderNotFoundError,
    FactoryOrderPVExistsError,
    InvalidStatusTransitionError,
    DatabaseError,
)


# ===================
# FIXTURES
# ===================

@pytest.fixture
def sample_factory_order_data():
    """Sample factory order data."""
    return {
        "id": "order-uuid-123",
        "pv_number": "PV-00017759",
        "order_date": "2025-01-06",
        "status": "PENDING",
        "notes": "Test order",
        "active": True,
        "created_at": "2025-01-06T10:00:00Z",
        "updated_at": "2025-01-06T10:00:00Z",
    }


@pytest.fixture
def sample_factory_order_items_data():
    """Sample factory order items data."""
    return [
        {
            "id": "item-uuid-1",
            "factory_order_id": "order-uuid-123",
            "product_id": "product-uuid-1",
            "quantity_ordered": 500.0,
            "quantity_produced": 0,
            "estimated_ready_date": "2025-02-01",
            "actual_ready_date": None,
            "created_at": "2025-01-06T10:00:00Z",
            "products": {"sku": "NOGAL CAFÉ"},
        },
        {
            "id": "item-uuid-2",
            "factory_order_id": "order-uuid-123",
            "product_id": "product-uuid-2",
            "quantity_ordered": 300.0,
            "quantity_produced": 0,
            "estimated_ready_date": "2025-02-01",
            "actual_ready_date": None,
            "created_at": "2025-01-06T10:00:00Z",
            "products": {"sku": "CEIBA GRIS OSC"},
        },
    ]


@pytest.fixture
def mock_db_factory_orders(mock_supabase, sample_factory_order_data, sample_factory_order_items_data):
    """Patch database for factory order service."""
    with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("factory_orders", [sample_factory_order_data])
        mock_supabase.set_table_data("factory_order_items", sample_factory_order_items_data)
        yield mock_supabase


# ===================
# STATUS TRANSITION TESTS
# ===================

class TestStatusTransitions:
    """Tests for status transition validation."""

    def test_valid_forward_transitions(self):
        """Should allow forward status transitions."""
        assert is_valid_status_transition(OrderStatus.PENDING, OrderStatus.CONFIRMED) is True
        assert is_valid_status_transition(OrderStatus.PENDING, OrderStatus.IN_PRODUCTION) is True
        assert is_valid_status_transition(OrderStatus.PENDING, OrderStatus.READY) is True
        assert is_valid_status_transition(OrderStatus.PENDING, OrderStatus.SHIPPED) is True
        assert is_valid_status_transition(OrderStatus.CONFIRMED, OrderStatus.IN_PRODUCTION) is True
        assert is_valid_status_transition(OrderStatus.CONFIRMED, OrderStatus.SHIPPED) is True
        assert is_valid_status_transition(OrderStatus.IN_PRODUCTION, OrderStatus.READY) is True
        assert is_valid_status_transition(OrderStatus.READY, OrderStatus.SHIPPED) is True

    def test_invalid_backward_transitions(self):
        """Should not allow backward status transitions."""
        assert is_valid_status_transition(OrderStatus.CONFIRMED, OrderStatus.PENDING) is False
        assert is_valid_status_transition(OrderStatus.IN_PRODUCTION, OrderStatus.CONFIRMED) is False
        assert is_valid_status_transition(OrderStatus.READY, OrderStatus.PENDING) is False
        assert is_valid_status_transition(OrderStatus.SHIPPED, OrderStatus.READY) is False

    def test_shipped_is_terminal(self):
        """SHIPPED status should be terminal (no transitions allowed)."""
        assert is_valid_status_transition(OrderStatus.SHIPPED, OrderStatus.PENDING) is False
        assert is_valid_status_transition(OrderStatus.SHIPPED, OrderStatus.CONFIRMED) is False
        assert is_valid_status_transition(OrderStatus.SHIPPED, OrderStatus.IN_PRODUCTION) is False
        assert is_valid_status_transition(OrderStatus.SHIPPED, OrderStatus.READY) is False

    def test_same_status_transition_invalid(self):
        """Transitioning to same status should be invalid (no change)."""
        # Actually same status is not a forward transition, so it should be False
        assert is_valid_status_transition(OrderStatus.PENDING, OrderStatus.PENDING) is False


# ===================
# GET ALL TESTS
# ===================

class TestFactoryOrderServiceGetAll:
    """Tests for FactoryOrderService.get_all()"""

    def test_get_all_returns_orders(self, mock_db_factory_orders, sample_factory_order_data, sample_factory_order_items_data):
        """Should return list of orders with total count."""
        service = FactoryOrderService()

        orders, total = service.get_all()

        assert len(orders) == 1
        assert total == 1
        assert orders[0].pv_number == "PV-00017759"
        assert orders[0].total_m2 == Decimal("800.0")  # 500 + 300
        assert orders[0].item_count == 2

    def test_get_all_empty_returns_empty_list(self, mock_supabase):
        """Should return empty list when no orders exist."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [], count=0)
            mock_supabase.set_table_data("factory_order_items", [])
            service = FactoryOrderService()

            orders, total = service.get_all()

            assert orders == []
            assert total == 0

    def test_get_all_with_status_filter(self, mock_db_factory_orders):
        """Should filter by status."""
        service = FactoryOrderService()

        orders, total = service.get_all(status=OrderStatus.PENDING)

        assert len(orders) == 1
        assert orders[0].status == OrderStatus.PENDING


# ===================
# GET BY ID TESTS
# ===================

class TestFactoryOrderServiceGetById:
    """Tests for FactoryOrderService.get_by_id()"""

    def test_get_by_id_returns_order_with_items(self, mock_db_factory_orders):
        """Should return order with items when found."""
        service = FactoryOrderService()

        order = service.get_by_id("order-uuid-123")

        assert order.id == "order-uuid-123"
        assert order.pv_number == "PV-00017759"
        assert len(order.items) == 2
        assert order.items[0].product_sku == "NOGAL CAFÉ"
        assert order.total_m2 == Decimal("800.0")

    def test_get_by_id_not_found_raises_error(self, mock_supabase):
        """Should raise FactoryOrderNotFoundError when order doesn't exist."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [])
            service = FactoryOrderService()

            with pytest.raises(FactoryOrderNotFoundError) as exc_info:
                service.get_by_id("nonexistent-id")

            assert exc_info.value.status_code == 404
            assert "FACTORY_ORDER_NOT_FOUND" in exc_info.value.code


# ===================
# GET BY PV NUMBER TESTS
# ===================

class TestFactoryOrderServiceGetByPVNumber:
    """Tests for FactoryOrderService.get_by_pv_number()"""

    def test_get_by_pv_number_returns_order(self, mock_db_factory_orders):
        """Should return order when PV number found."""
        service = FactoryOrderService()

        order = service.get_by_pv_number("PV-00017759")

        assert order is not None
        assert order.pv_number == "PV-00017759"

    def test_get_by_pv_number_not_found_returns_none(self, mock_supabase):
        """Should return None when PV number not found."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [])
            service = FactoryOrderService()

            order = service.get_by_pv_number("NONEXISTENT")

            assert order is None

    def test_get_by_pv_number_case_insensitive(self, mock_db_factory_orders):
        """Should search with uppercase PV number."""
        service = FactoryOrderService()

        # Search with lowercase
        order = service.get_by_pv_number("pv-00017759")

        # Should convert to uppercase internally
        assert order is not None


# ===================
# CREATE TESTS
# ===================

class TestFactoryOrderServiceCreate:
    """Tests for FactoryOrderService.create()"""

    def test_create_order_success(self, mock_supabase):
        """Should create order with items and return it."""
        # Setup mock data - note: we test without PV to avoid mock duplicate check issue
        # PV duplicate check is tested in test_create_order_duplicate_pv_raises_error
        created_order = {
            "id": "test-uuid-123",
            "pv_number": None,
            "order_date": "2025-01-06",
            "status": "PENDING",
            "notes": "Test order",
            "active": True,
            "created_at": "2025-01-06T10:00:00Z",
            "updated_at": "2025-01-06T10:00:00Z",
        }
        created_items = [
            {
                "id": "item-uuid-1",
                "factory_order_id": "test-uuid-123",
                "product_id": "product-uuid-1",
                "quantity_ordered": 500.0,
                "quantity_produced": 0,
                "estimated_ready_date": None,
                "actual_ready_date": None,
                "created_at": "2025-01-06T10:00:00Z",
                "products": {"sku": "TEST SKU"},
            }
        ]

        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [created_order])
            mock_supabase.set_table_data("factory_order_items", created_items)
            service = FactoryOrderService()

            data = FactoryOrderCreate(
                order_date=date(2025, 1, 6),
                items=[
                    FactoryOrderItemCreate(
                        product_id="product-uuid-1",
                        quantity_ordered=Decimal("500.00"),
                    )
                ],
                notes="Test order",
            )

            order = service.create(data)

            assert order is not None
            assert order.status == OrderStatus.PENDING
            assert len(order.items) == 1

    def test_create_order_duplicate_pv_raises_error(self, mock_db_factory_orders, sample_factory_order_data):
        """Should raise FactoryOrderPVExistsError when PV already exists."""
        service = FactoryOrderService()

        data = FactoryOrderCreate(
            pv_number="PV-00017759",  # Already exists
            order_date=date(2025, 1, 6),
            items=[
                FactoryOrderItemCreate(
                    product_id="product-uuid-1",
                    quantity_ordered=Decimal("500.00"),
                )
            ],
        )

        with pytest.raises(FactoryOrderPVExistsError) as exc_info:
            service.create(data)

        assert exc_info.value.status_code == 409
        assert "EXISTS" in exc_info.value.code

    def test_create_order_without_pv_number(self, mock_supabase):
        """Should allow creating order without PV number."""
        # Setup mock data that will be returned after insert
        created_order = {
            "id": "test-uuid-123",
            "pv_number": None,
            "order_date": "2025-01-06",
            "status": "PENDING",
            "notes": None,
            "active": True,
            "created_at": "2025-01-06T10:00:00Z",
            "updated_at": "2025-01-06T10:00:00Z",
        }
        created_items = [
            {
                "id": "item-uuid-1",
                "factory_order_id": "test-uuid-123",
                "product_id": "product-uuid-1",
                "quantity_ordered": 500.0,
                "quantity_produced": 0,
                "estimated_ready_date": None,
                "actual_ready_date": None,
                "created_at": "2025-01-06T10:00:00Z",
                "products": {"sku": "TEST SKU"},
            }
        ]

        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [created_order])
            mock_supabase.set_table_data("factory_order_items", created_items)
            service = FactoryOrderService()

            data = FactoryOrderCreate(
                order_date=date(2025, 1, 6),
                items=[
                    FactoryOrderItemCreate(
                        product_id="product-uuid-1",
                        quantity_ordered=Decimal("500.00"),
                    )
                ],
            )

            order = service.create(data)

            assert order is not None
            assert order.pv_number is None


# ===================
# UPDATE TESTS
# ===================

class TestFactoryOrderServiceUpdate:
    """Tests for FactoryOrderService.update()"""

    def test_update_order_success(self, mock_db_factory_orders):
        """Should update order fields."""
        service = FactoryOrderService()

        data = FactoryOrderUpdate(notes="Updated notes")

        order = service.update("order-uuid-123", data)

        assert order is not None
        assert order.pv_number == "PV-00017759"  # Original preserved

    def test_update_order_not_found_raises_error(self, mock_supabase):
        """Should raise FactoryOrderNotFoundError when order doesn't exist."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [])
            service = FactoryOrderService()

            data = FactoryOrderUpdate(notes="Updated")

            with pytest.raises(FactoryOrderNotFoundError):
                service.update("nonexistent-id", data)


# ===================
# UPDATE STATUS TESTS
# ===================

class TestFactoryOrderServiceUpdateStatus:
    """Tests for FactoryOrderService.update_status()"""

    def test_update_status_forward_success(self, mock_db_factory_orders):
        """Should update status when transition is valid."""
        service = FactoryOrderService()

        data = FactoryOrderStatusUpdate(status=OrderStatus.CONFIRMED)

        order = service.update_status("order-uuid-123", data)

        assert order is not None

    def test_update_status_skip_forward_allowed(self, mock_db_factory_orders):
        """Should allow skipping forward (PENDING -> READY)."""
        service = FactoryOrderService()

        data = FactoryOrderStatusUpdate(status=OrderStatus.READY)

        # Should not raise
        order = service.update_status("order-uuid-123", data)
        assert order is not None

    def test_update_status_backward_raises_error(self, mock_db_factory_orders, mock_supabase, sample_factory_order_items_data):
        """Should raise InvalidStatusTransitionError for backward transition."""
        # Set order to CONFIRMED status
        confirmed_order = {
            "id": "order-uuid-123",
            "pv_number": "PV-00017759",
            "order_date": "2025-01-06",
            "status": "CONFIRMED",
            "notes": None,
            "active": True,
            "created_at": "2025-01-06T10:00:00Z",
            "updated_at": "2025-01-06T10:00:00Z",
        }
        mock_supabase.set_table_data("factory_orders", [confirmed_order])
        mock_supabase.set_table_data("factory_order_items", sample_factory_order_items_data)

        service = FactoryOrderService()

        data = FactoryOrderStatusUpdate(status=OrderStatus.PENDING)  # Backward!

        with pytest.raises(InvalidStatusTransitionError) as exc_info:
            service.update_status("order-uuid-123", data)

        assert exc_info.value.status_code == 422
        assert "INVALID_STATUS_TRANSITION" in exc_info.value.code

    def test_update_status_from_shipped_raises_error(self, mock_db_factory_orders, mock_supabase, sample_factory_order_items_data):
        """Should raise error when transitioning from SHIPPED (terminal)."""
        # Set order to SHIPPED status
        shipped_order = {
            "id": "order-uuid-123",
            "pv_number": "PV-00017759",
            "order_date": "2025-01-06",
            "status": "SHIPPED",
            "notes": None,
            "active": True,
            "created_at": "2025-01-06T10:00:00Z",
            "updated_at": "2025-01-06T10:00:00Z",
        }
        mock_supabase.set_table_data("factory_orders", [shipped_order])
        mock_supabase.set_table_data("factory_order_items", sample_factory_order_items_data)

        service = FactoryOrderService()

        data = FactoryOrderStatusUpdate(status=OrderStatus.READY)

        with pytest.raises(InvalidStatusTransitionError):
            service.update_status("order-uuid-123", data)


# ===================
# DELETE TESTS
# ===================

class TestFactoryOrderServiceDelete:
    """Tests for FactoryOrderService.delete()"""

    def test_delete_order_success(self, mock_db_factory_orders):
        """Should soft delete order (set active=False)."""
        service = FactoryOrderService()

        result = service.delete("order-uuid-123")

        assert result is True

    def test_delete_order_not_found_raises_error(self, mock_supabase):
        """Should raise FactoryOrderNotFoundError when order doesn't exist."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [])
            service = FactoryOrderService()

            with pytest.raises(FactoryOrderNotFoundError):
                service.delete("nonexistent-id")


# ===================
# GET ITEMS TESTS
# ===================

class TestFactoryOrderServiceGetItems:
    """Tests for FactoryOrderService.get_items()"""

    def test_get_items_returns_list(self, mock_db_factory_orders):
        """Should return list of order items."""
        service = FactoryOrderService()

        items = service.get_items("order-uuid-123")

        assert len(items) == 2
        assert items[0].product_sku == "NOGAL CAFÉ"
        assert items[1].product_sku == "CEIBA GRIS OSC"


# ===================
# UTILITY TESTS
# ===================

class TestFactoryOrderServiceUtilities:
    """Tests for utility methods."""

    def test_pv_exists_returns_true(self, mock_db_factory_orders):
        """Should return True when PV exists."""
        service = FactoryOrderService()

        assert service.pv_exists("PV-00017759") is True

    def test_pv_exists_returns_false(self, mock_supabase):
        """Should return False when PV doesn't exist."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("factory_orders", [])
            service = FactoryOrderService()

            assert service.pv_exists("NONEXISTENT") is False

    def test_count_returns_total(self, mock_db_factory_orders):
        """Should return total count of orders."""
        service = FactoryOrderService()

        count = service.count()

        assert count == 1


# ===================
# SINGLETON TESTS
# ===================

class TestGetFactoryOrderService:
    """Tests for get_factory_order_service() singleton."""

    def test_get_factory_order_service_returns_instance(self, mock_supabase):
        """Should return FactoryOrderService instance."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            # Reset singleton
            import services.factory_order_service as module
            module._factory_order_service = None

            service = get_factory_order_service()

            assert isinstance(service, FactoryOrderService)

    def test_get_factory_order_service_returns_same_instance(self, mock_supabase):
        """Should return same instance (singleton)."""
        with patch("services.factory_order_service.get_supabase_client", return_value=mock_supabase):
            # Reset singleton
            import services.factory_order_service as module
            module._factory_order_service = None

            service1 = get_factory_order_service()
            service2 = get_factory_order_service()

            assert service1 is service2
