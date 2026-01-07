"""
Unit tests for ShipmentService.

See STANDARDS_TESTING.md for patterns.

Run: pytest tests/unit/test_shipment_service.py -v
Run with coverage: pytest tests/unit/test_shipment_service.py --cov=services/shipment_service
"""

import pytest
from unittest.mock import patch
from datetime import date
from decimal import Decimal

# Import what we're testing
from services.shipment_service import ShipmentService, get_shipment_service
from models.shipment import (
    ShipmentCreate,
    ShipmentUpdate,
    ShipmentStatusUpdate,
    ShipmentStatus,
    is_valid_shipment_status_transition,
)
from exceptions import (
    ShipmentNotFoundError,
    ShipmentBookingExistsError,
    ShipmentSHPExistsError,
    InvalidStatusTransitionError,
    DatabaseError,
)


# ===================
# FIXTURES
# ===================

@pytest.fixture
def sample_shipment_data():
    """Sample shipment data."""
    return {
        "id": "shipment-uuid-123",
        "factory_order_id": "order-uuid-123",
        "boat_schedule_id": None,
        "shipping_company_id": "company-uuid-1",
        "origin_port_id": "port-uuid-origin",
        "destination_port_id": "port-uuid-dest",
        "status": "AT_FACTORY",
        "active": True,
        "booking_number": "BGA0505879",
        "shp_number": "SHP0065011",
        "bill_of_lading": "CMAU1234567",
        "vessel_name": "MSC OSCAR",
        "voyage_number": "VY123",
        "etd": "2025-01-15",
        "eta": "2025-02-15",
        "actual_departure": None,
        "actual_arrival": None,
        "free_days": 14,
        "free_days_expiry": None,
        "freight_cost_usd": 2500.00,
        "notes": "Test shipment",
        "created_at": "2025-01-06T10:00:00Z",
        "updated_at": "2025-01-06T10:00:00Z",
    }


@pytest.fixture
def mock_db_shipments(mock_supabase, sample_shipment_data):
    """Patch database for shipment service."""
    with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("shipments", [sample_shipment_data])
        yield mock_supabase


# ===================
# STATUS TRANSITION TESTS
# ===================

class TestShipmentStatusTransitions:
    """Tests for shipment status transition validation."""

    def test_valid_forward_transitions(self):
        """Should allow forward status transitions."""
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_FACTORY, ShipmentStatus.AT_ORIGIN_PORT) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_FACTORY, ShipmentStatus.IN_TRANSIT) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_FACTORY, ShipmentStatus.DELIVERED) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_ORIGIN_PORT, ShipmentStatus.IN_TRANSIT) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.IN_TRANSIT, ShipmentStatus.AT_DESTINATION_PORT) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_DESTINATION_PORT, ShipmentStatus.IN_CUSTOMS) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.IN_CUSTOMS, ShipmentStatus.IN_TRUCK) is True
        assert is_valid_shipment_status_transition(ShipmentStatus.IN_TRUCK, ShipmentStatus.DELIVERED) is True

    def test_invalid_backward_transitions(self):
        """Should not allow backward status transitions."""
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_ORIGIN_PORT, ShipmentStatus.AT_FACTORY) is False
        assert is_valid_shipment_status_transition(ShipmentStatus.IN_TRANSIT, ShipmentStatus.AT_ORIGIN_PORT) is False
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_DESTINATION_PORT, ShipmentStatus.IN_TRANSIT) is False
        assert is_valid_shipment_status_transition(ShipmentStatus.DELIVERED, ShipmentStatus.IN_TRUCK) is False

    def test_delivered_is_terminal(self):
        """DELIVERED status should be terminal (no transitions allowed)."""
        assert is_valid_shipment_status_transition(ShipmentStatus.DELIVERED, ShipmentStatus.AT_FACTORY) is False
        assert is_valid_shipment_status_transition(ShipmentStatus.DELIVERED, ShipmentStatus.IN_TRANSIT) is False
        assert is_valid_shipment_status_transition(ShipmentStatus.DELIVERED, ShipmentStatus.IN_CUSTOMS) is False

    def test_same_status_transition_invalid(self):
        """Transitioning to same status should be invalid (no change)."""
        assert is_valid_shipment_status_transition(ShipmentStatus.AT_FACTORY, ShipmentStatus.AT_FACTORY) is False
        assert is_valid_shipment_status_transition(ShipmentStatus.IN_TRANSIT, ShipmentStatus.IN_TRANSIT) is False


# ===================
# GET ALL TESTS
# ===================

class TestShipmentServiceGetAll:
    """Tests for ShipmentService.get_all()"""

    def test_get_all_returns_shipments(self, mock_db_shipments, sample_shipment_data):
        """Should return list of shipments with total count."""
        service = ShipmentService()

        shipments, total = service.get_all()

        assert len(shipments) == 1
        assert total == 1
        assert shipments[0].booking_number == "BGA0505879"
        assert shipments[0].shp_number == "SHP0065011"

    def test_get_all_empty_returns_empty_list(self, mock_supabase):
        """Should return empty list when no shipments exist."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [], count=0)
            service = ShipmentService()

            shipments, total = service.get_all()

            assert shipments == []
            assert total == 0

    def test_get_all_with_status_filter(self, mock_db_shipments):
        """Should filter by status."""
        service = ShipmentService()

        shipments, total = service.get_all(status=ShipmentStatus.AT_FACTORY)

        assert len(shipments) == 1
        assert shipments[0].status == ShipmentStatus.AT_FACTORY


# ===================
# GET BY ID TESTS
# ===================

class TestShipmentServiceGetById:
    """Tests for ShipmentService.get_by_id()"""

    def test_get_by_id_returns_shipment(self, mock_db_shipments):
        """Should return shipment when found."""
        service = ShipmentService()

        shipment = service.get_by_id("shipment-uuid-123")

        assert shipment.id == "shipment-uuid-123"
        assert shipment.booking_number == "BGA0505879"
        assert shipment.vessel_name == "MSC OSCAR"

    def test_get_by_id_not_found_raises_error(self, mock_supabase):
        """Should raise ShipmentNotFoundError when shipment doesn't exist."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            with pytest.raises(ShipmentNotFoundError) as exc_info:
                service.get_by_id("nonexistent-id")

            assert exc_info.value.status_code == 404
            assert "SHIPMENT_NOT_FOUND" in exc_info.value.code


# ===================
# GET BY BOOKING NUMBER TESTS
# ===================

class TestShipmentServiceGetByBookingNumber:
    """Tests for ShipmentService.get_by_booking_number()"""

    def test_get_by_booking_number_returns_shipment(self, mock_db_shipments):
        """Should return shipment when booking number found."""
        service = ShipmentService()

        shipment = service.get_by_booking_number("BGA0505879")

        assert shipment is not None
        assert shipment.booking_number == "BGA0505879"

    def test_get_by_booking_number_not_found_returns_none(self, mock_supabase):
        """Should return None when booking number not found."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            shipment = service.get_by_booking_number("NONEXISTENT")

            assert shipment is None

    def test_get_by_booking_number_case_insensitive(self, mock_db_shipments):
        """Should search with uppercase booking number."""
        service = ShipmentService()

        # Search with lowercase
        shipment = service.get_by_booking_number("bga0505879")

        # Should convert to uppercase internally
        assert shipment is not None


# ===================
# GET BY SHP NUMBER TESTS
# ===================

class TestShipmentServiceGetBySHPNumber:
    """Tests for ShipmentService.get_by_shp_number()"""

    def test_get_by_shp_number_returns_shipment(self, mock_db_shipments):
        """Should return shipment when SHP number found."""
        service = ShipmentService()

        shipment = service.get_by_shp_number("SHP0065011")

        assert shipment is not None
        assert shipment.shp_number == "SHP0065011"

    def test_get_by_shp_number_not_found_returns_none(self, mock_supabase):
        """Should return None when SHP number not found."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            shipment = service.get_by_shp_number("NONEXISTENT")

            assert shipment is None


# ===================
# GET BY FACTORY ORDER ID TESTS
# ===================

class TestShipmentServiceGetByFactoryOrderId:
    """Tests for ShipmentService.get_by_factory_order_id()"""

    def test_get_by_factory_order_id_returns_shipments(self, mock_db_shipments):
        """Should return shipments for factory order."""
        service = ShipmentService()

        shipments = service.get_by_factory_order_id("order-uuid-123")

        assert len(shipments) == 1
        assert shipments[0].factory_order_id == "order-uuid-123"


# ===================
# GET BY STATUS TESTS
# ===================

class TestShipmentServiceGetByStatus:
    """Tests for ShipmentService.get_by_status()"""

    def test_get_by_status_returns_shipments(self, mock_db_shipments):
        """Should return all shipments with given status."""
        service = ShipmentService()

        shipments = service.get_by_status(ShipmentStatus.AT_FACTORY)

        assert len(shipments) == 1
        assert shipments[0].status == ShipmentStatus.AT_FACTORY


# ===================
# CREATE TESTS
# ===================

class TestShipmentServiceCreate:
    """Tests for ShipmentService.create()"""

    def test_create_shipment_success(self, mock_supabase):
        """Should create shipment and return it."""
        created_shipment = {
            "id": "new-shipment-uuid",
            "factory_order_id": None,
            "boat_schedule_id": None,
            "shipping_company_id": None,
            "origin_port_id": "port-uuid-origin",
            "destination_port_id": "port-uuid-dest",
            "status": "AT_FACTORY",
            "active": True,
            "booking_number": None,
            "shp_number": None,
            "bill_of_lading": None,
            "vessel_name": None,
            "voyage_number": None,
            "etd": None,
            "eta": None,
            "actual_departure": None,
            "actual_arrival": None,
            "free_days": None,
            "free_days_expiry": None,
            "freight_cost_usd": None,
            "notes": "Test shipment",
            "created_at": "2025-01-06T10:00:00Z",
            "updated_at": "2025-01-06T10:00:00Z",
        }

        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [created_shipment])
            service = ShipmentService()

            data = ShipmentCreate(
                origin_port_id="port-uuid-origin",
                destination_port_id="port-uuid-dest",
                notes="Test shipment",
            )

            shipment = service.create(data)

            assert shipment is not None
            assert shipment.status == ShipmentStatus.AT_FACTORY

    def test_create_shipment_duplicate_booking_raises_error(self, mock_db_shipments):
        """Should raise ShipmentBookingExistsError when booking number already exists."""
        service = ShipmentService()

        data = ShipmentCreate(
            origin_port_id="port-uuid-origin",
            destination_port_id="port-uuid-dest",
            booking_number="BGA0505879",  # Already exists
        )

        with pytest.raises(ShipmentBookingExistsError) as exc_info:
            service.create(data)

        assert exc_info.value.status_code == 409
        assert "EXISTS" in exc_info.value.code

    def test_create_shipment_duplicate_shp_raises_error(self, mock_db_shipments):
        """Should raise ShipmentSHPExistsError when SHP number already exists."""
        service = ShipmentService()

        data = ShipmentCreate(
            origin_port_id="port-uuid-origin",
            destination_port_id="port-uuid-dest",
            shp_number="SHP0065011",  # Already exists
        )

        with pytest.raises(ShipmentSHPExistsError) as exc_info:
            service.create(data)

        assert exc_info.value.status_code == 409
        assert "EXISTS" in exc_info.value.code


# ===================
# UPDATE TESTS
# ===================

class TestShipmentServiceUpdate:
    """Tests for ShipmentService.update()"""

    def test_update_shipment_success(self, mock_db_shipments):
        """Should update shipment fields."""
        service = ShipmentService()

        data = ShipmentUpdate(notes="Updated notes")

        shipment = service.update("shipment-uuid-123", data)

        assert shipment is not None
        assert shipment.booking_number == "BGA0505879"  # Original preserved

    def test_update_shipment_not_found_raises_error(self, mock_supabase):
        """Should raise ShipmentNotFoundError when shipment doesn't exist."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            data = ShipmentUpdate(notes="Updated")

            with pytest.raises(ShipmentNotFoundError):
                service.update("nonexistent-id", data)

    def test_update_shipment_same_booking_allowed(self, mock_db_shipments):
        """Should allow keeping the same booking number (no duplicate error)."""
        service = ShipmentService()

        # Update with the same booking number - should not raise
        data = ShipmentUpdate(booking_number="BGA0505879")

        shipment = service.update("shipment-uuid-123", data)
        assert shipment.booking_number == "BGA0505879"


# ===================
# UPDATE STATUS TESTS
# ===================

class TestShipmentServiceUpdateStatus:
    """Tests for ShipmentService.update_status()"""

    def test_update_status_forward_success(self, mock_db_shipments):
        """Should update status when transition is valid."""
        service = ShipmentService()

        data = ShipmentStatusUpdate(status=ShipmentStatus.AT_ORIGIN_PORT)

        shipment = service.update_status("shipment-uuid-123", data)

        assert shipment is not None

    def test_update_status_skip_forward_allowed(self, mock_db_shipments):
        """Should allow skipping forward (AT_FACTORY -> IN_TRANSIT)."""
        service = ShipmentService()

        data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRANSIT)

        # Should not raise
        shipment = service.update_status("shipment-uuid-123", data)
        assert shipment is not None

    def test_update_status_backward_raises_error(self, mock_supabase, sample_shipment_data):
        """Should raise InvalidStatusTransitionError for backward transition."""
        # Set shipment to IN_TRANSIT status
        in_transit_shipment = {
            **sample_shipment_data,
            "status": "IN_TRANSIT",
        }
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [in_transit_shipment])
            service = ShipmentService()

            data = ShipmentStatusUpdate(status=ShipmentStatus.AT_FACTORY)  # Backward!

            with pytest.raises(InvalidStatusTransitionError) as exc_info:
                service.update_status("shipment-uuid-123", data)

            assert exc_info.value.status_code == 422
            assert "INVALID_STATUS_TRANSITION" in exc_info.value.code

    def test_update_status_from_delivered_raises_error(self, mock_supabase, sample_shipment_data):
        """Should raise error when transitioning from DELIVERED (terminal)."""
        # Set shipment to DELIVERED status
        delivered_shipment = {
            **sample_shipment_data,
            "status": "DELIVERED",
        }
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [delivered_shipment])
            service = ShipmentService()

            data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRUCK)

            with pytest.raises(InvalidStatusTransitionError):
                service.update_status("shipment-uuid-123", data)


# ===================
# DELETE TESTS
# ===================

class TestShipmentServiceDelete:
    """Tests for ShipmentService.delete()"""

    def test_delete_shipment_success(self, mock_db_shipments):
        """Should soft delete shipment (set active=False)."""
        service = ShipmentService()

        result = service.delete("shipment-uuid-123")

        assert result is True

    def test_delete_shipment_not_found_raises_error(self, mock_supabase):
        """Should raise ShipmentNotFoundError when shipment doesn't exist."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            with pytest.raises(ShipmentNotFoundError):
                service.delete("nonexistent-id")


# ===================
# UTILITY TESTS
# ===================

class TestShipmentServiceUtilities:
    """Tests for utility methods."""

    def test_booking_exists_returns_true(self, mock_db_shipments):
        """Should return True when booking number exists."""
        service = ShipmentService()

        assert service.booking_exists("BGA0505879") is True

    def test_booking_exists_returns_false(self, mock_supabase):
        """Should return False when booking number doesn't exist."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            assert service.booking_exists("NONEXISTENT") is False

    def test_shp_exists_returns_true(self, mock_db_shipments):
        """Should return True when SHP number exists."""
        service = ShipmentService()

        assert service.shp_exists("SHP0065011") is True

    def test_shp_exists_returns_false(self, mock_supabase):
        """Should return False when SHP number doesn't exist."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            mock_supabase.set_table_data("shipments", [])
            service = ShipmentService()

            assert service.shp_exists("NONEXISTENT") is False

    def test_count_returns_total(self, mock_db_shipments):
        """Should return total count of shipments."""
        service = ShipmentService()

        count = service.count()

        assert count == 1


# ===================
# SINGLETON TESTS
# ===================

class TestGetShipmentService:
    """Tests for get_shipment_service() singleton."""

    def test_get_shipment_service_returns_instance(self, mock_supabase):
        """Should return ShipmentService instance."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            # Reset singleton
            import services.shipment_service as module
            module._shipment_service = None

            service = get_shipment_service()

            assert isinstance(service, ShipmentService)

    def test_get_shipment_service_returns_same_instance(self, mock_supabase):
        """Should return same instance (singleton)."""
        with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
            # Reset singleton
            import services.shipment_service as module
            module._shipment_service = None

            service1 = get_shipment_service()
            service2 = get_shipment_service()

            assert service1 is service2