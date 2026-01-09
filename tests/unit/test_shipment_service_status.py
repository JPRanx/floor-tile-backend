"""
Unit tests for ShipmentService status update methods.

Run: pytest tests/unit/test_shipment_service_status.py -v
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from services.shipment_service import ShipmentService, get_shipment_service
from models.shipment import (
    ShipmentStatus,
    ShipmentStatusUpdate,
    is_valid_shipment_status_transition,
)
from exceptions import DatabaseError
from exceptions.errors import InvalidStatusTransitionError


class TestShipmentStatusTransitionValidation:
    """Tests for is_valid_shipment_status_transition()"""

    def test_valid_transition_factory_to_transit(self):
        """Should allow AT_FACTORY → IN_TRANSIT."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.AT_FACTORY,
            ShipmentStatus.IN_TRANSIT
        )
        assert result is True

    def test_valid_transition_transit_to_port(self):
        """Should allow IN_TRANSIT → AT_DESTINATION_PORT."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.IN_TRANSIT,
            ShipmentStatus.AT_DESTINATION_PORT
        )
        assert result is True

    def test_valid_transition_port_to_customs(self):
        """Should allow AT_DESTINATION_PORT → IN_CUSTOMS."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.AT_DESTINATION_PORT,
            ShipmentStatus.IN_CUSTOMS
        )
        assert result is True

    def test_valid_transition_customs_to_delivered(self):
        """Should allow IN_CUSTOMS → DELIVERED."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.IN_CUSTOMS,
            ShipmentStatus.DELIVERED
        )
        assert result is True

    def test_valid_transition_skip_steps(self):
        """Should allow skipping steps (AT_FACTORY → AT_DESTINATION_PORT)."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.AT_FACTORY,
            ShipmentStatus.AT_DESTINATION_PORT
        )
        assert result is True

    def test_valid_transition_direct_to_delivered(self):
        """Should allow jumping directly to DELIVERED."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.AT_FACTORY,
            ShipmentStatus.DELIVERED
        )
        assert result is True

    def test_invalid_transition_backward(self):
        """Should NOT allow going backward (IN_TRANSIT → AT_FACTORY)."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.IN_TRANSIT,
            ShipmentStatus.AT_FACTORY
        )
        assert result is False

    def test_invalid_transition_from_delivered(self):
        """Should NOT allow transitions from DELIVERED (terminal)."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.DELIVERED,
            ShipmentStatus.IN_TRANSIT
        )
        assert result is False

    def test_invalid_transition_backward_after_customs(self):
        """Should NOT allow IN_CUSTOMS → IN_TRANSIT."""
        result = is_valid_shipment_status_transition(
            ShipmentStatus.IN_CUSTOMS,
            ShipmentStatus.IN_TRANSIT
        )
        assert result is False


class TestShipmentServiceUpdateStatus:
    """Tests for ShipmentService.update_status()"""

    def test_update_status_valid_transition(self, mock_db, mock_supabase):
        """Should update status for valid transition."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "booking_number": "BGA0505879",
            "status": "AT_FACTORY",
            "vessel_name": "PERITO MORENO",
            "etd": "2026-01-15",
            "eta": "2026-02-01",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        # Mock event service and alert service
        with patch("services.shipment_service.get_shipment_event_service") as mock_event_svc:
            with patch("services.shipment_service.get_alert_service") as mock_alert_svc:
                mock_event_svc.return_value.create = MagicMock()
                mock_alert_svc.return_value.create = MagicMock()

                service = ShipmentService()
                data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRANSIT)

                # Act
                result = service.update_status("ship-123", data)

                # Assert
                assert result is not None
                mock_event_svc.return_value.create.assert_called_once()

    def test_update_status_same_status_returns_unchanged(self, mock_db, mock_supabase):
        """Should return unchanged when status is the same."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "IN_TRANSIT",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        service = ShipmentService()
        data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRANSIT)

        # Act
        result = service.update_status("ship-123", data)

        # Assert
        assert result is not None
        assert result.status == "IN_TRANSIT"

    def test_update_status_invalid_transition_raises_error(self, mock_db, mock_supabase):
        """Should raise InvalidStatusTransitionError for invalid transition."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "IN_TRANSIT",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        service = ShipmentService()
        data = ShipmentStatusUpdate(status=ShipmentStatus.AT_FACTORY)

        # Act & Assert
        with pytest.raises(InvalidStatusTransitionError) as exc_info:
            service.update_status("ship-123", data)

        assert "IN_TRANSIT" in str(exc_info.value)
        assert "AT_FACTORY" in str(exc_info.value)

    def test_update_status_from_delivered_raises_error(self, mock_db, mock_supabase):
        """Should raise InvalidStatusTransitionError when transitioning from DELIVERED."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "DELIVERED",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        service = ShipmentService()
        data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRANSIT)

        # Act & Assert
        with pytest.raises(InvalidStatusTransitionError):
            service.update_status("ship-123", data)

    def test_update_status_sends_alert_for_transit(self, mock_db, mock_supabase):
        """Should send Telegram alert when status changes to IN_TRANSIT."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "AT_FACTORY",
            "vessel_name": "PERITO MORENO",
            "eta": "2026-02-01",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        with patch("services.shipment_service.get_shipment_event_service") as mock_event_svc:
            with patch("services.shipment_service.get_alert_service") as mock_alert_svc:
                mock_event_svc.return_value.create = MagicMock()
                mock_alert_svc.return_value.create = MagicMock()

                service = ShipmentService()
                data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRANSIT)

                # Act
                service.update_status("ship-123", data)

                # Assert - alert should be created
                mock_alert_svc.return_value.create.assert_called_once()
                call_args = mock_alert_svc.return_value.create.call_args
                alert_create = call_args[0][0]  # First positional arg
                assert "zarpó" in alert_create.title or "DEPARTED" in alert_create.type.value

    def test_update_status_sends_alert_for_arrival(self, mock_db, mock_supabase):
        """Should send Telegram alert when status changes to AT_DESTINATION_PORT."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "IN_TRANSIT",
            "vessel_name": "PERITO MORENO",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        with patch("services.shipment_service.get_shipment_event_service") as mock_event_svc:
            with patch("services.shipment_service.get_alert_service") as mock_alert_svc:
                mock_event_svc.return_value.create = MagicMock()
                mock_alert_svc.return_value.create = MagicMock()

                service = ShipmentService()
                data = ShipmentStatusUpdate(status=ShipmentStatus.AT_DESTINATION_PORT)

                # Act
                service.update_status("ship-123", data)

                # Assert - alert should be created
                mock_alert_svc.return_value.create.assert_called_once()
                call_args = mock_alert_svc.return_value.create.call_args
                alert_create = call_args[0][0]
                assert "llegó" in alert_create.title or "ARRIVED" in alert_create.type.value

    def test_update_status_sends_alert_for_delivered(self, mock_db, mock_supabase):
        """Should send Telegram alert when status changes to DELIVERED."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "IN_CUSTOMS",
            "vessel_name": "PERITO MORENO",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        with patch("services.shipment_service.get_shipment_event_service") as mock_event_svc:
            with patch("services.shipment_service.get_alert_service") as mock_alert_svc:
                mock_event_svc.return_value.create = MagicMock()
                mock_alert_svc.return_value.create = MagicMock()

                service = ShipmentService()
                data = ShipmentStatusUpdate(status=ShipmentStatus.DELIVERED)

                # Act
                service.update_status("ship-123", data)

                # Assert - alert should be created
                mock_alert_svc.return_value.create.assert_called_once()
                call_args = mock_alert_svc.return_value.create.call_args
                alert_create = call_args[0][0]
                assert "entregado" in alert_create.title or "ARRIVED" in alert_create.type.value

    def test_update_status_creates_shipment_event(self, mock_db, mock_supabase):
        """Should create a shipment event for status change."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "AT_FACTORY",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])

        with patch("services.shipment_service.get_shipment_event_service") as mock_event_svc:
            with patch("services.shipment_service.get_alert_service") as mock_alert_svc:
                mock_event_instance = MagicMock()
                mock_event_svc.return_value = mock_event_instance
                mock_alert_svc.return_value.create = MagicMock()

                service = ShipmentService()
                data = ShipmentStatusUpdate(status=ShipmentStatus.IN_TRANSIT)

                # Act
                service.update_status("ship-123", data)

                # Assert - event should be created
                mock_event_instance.create.assert_called_once()
                call_args = mock_event_instance.create.call_args[0][0]
                assert call_args.shipment_id == "ship-123"
                assert call_args.status == ShipmentStatus.IN_TRANSIT


class TestShipmentServiceFlexibleSHPMatching:
    """Tests for flexible SHP number matching."""

    def test_find_by_shp_exact_match(self, mock_db, mock_supabase):
        """Should find shipment with exact SHP number."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "AT_FACTORY",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])
        service = ShipmentService()

        # Act
        result = service.get_by_shp_number("SHP0065011")

        # Assert
        assert result is not None
        assert result.shp_number == "SHP0065011"

    def test_find_by_shp_without_prefix(self, mock_db, mock_supabase):
        """Should find shipment when searching without SHP prefix."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "SHP0065011",
            "status": "AT_FACTORY",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])
        service = ShipmentService()

        # Act
        result = service.get_by_shp_number("0065011")

        # Assert
        assert result is not None
        assert result.shp_number == "SHP0065011"

    def test_find_by_shp_with_prefix_when_stored_without(self, mock_db, mock_supabase):
        """Should find shipment when searching with prefix but stored without."""
        # Arrange
        shipment_data = {
            "id": "ship-123",
            "shp_number": "0065011",
            "status": "AT_FACTORY",
            "created_at": "2025-01-01T00:00:00Z",
        }
        mock_supabase.set_table_data("shipments", [shipment_data])
        service = ShipmentService()

        # Act
        result = service.get_by_shp_number("SHP0065011")

        # Assert
        assert result is not None
        assert result.shp_number == "0065011"


class TestGetShipmentService:
    """Tests for get_shipment_service() singleton."""

    def test_get_shipment_service_returns_instance(self, mock_db):
        """Should return ShipmentService instance."""
        service = get_shipment_service()

        assert isinstance(service, ShipmentService)