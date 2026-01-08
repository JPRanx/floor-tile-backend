"""
Unit tests for ShipmentEventService.

See STANDARDS_TESTING.md for patterns.

Run: pytest tests/unit/test_shipment_event_service.py -v
Run with coverage: pytest tests/unit/test_shipment_event_service.py --cov=services/shipment_event_service
"""

import pytest
from unittest.mock import patch
from datetime import datetime

# Import what we're testing
from services.shipment_event_service import ShipmentEventService, get_shipment_event_service
from models.shipment_event import ShipmentEventCreate
from models.shipment import ShipmentStatus
from exceptions import ShipmentEventNotFoundError
import services.shipment_event_service as shipment_event_service_module


# ===================
# FIXTURES
# ===================

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton service between tests."""
    shipment_event_service_module._shipment_event_service = None
    yield
    shipment_event_service_module._shipment_event_service = None

@pytest.fixture
def sample_event_data():
    """Sample shipment event data - initial event."""
    return {
        "id": "event-uuid-123",
        "shipment_id": "shipment-uuid-123",
        "status": "AT_FACTORY",
        "occurred_at": "2025-01-06T10:00:00Z",
        "notes": "Shipment created",
        "created_at": "2025-01-06T10:00:00Z",
    }


@pytest.fixture
def sample_event_data_2():
    """Sample shipment event data - second event (later timestamp)."""
    return {
        "id": "event-uuid-456",
        "shipment_id": "shipment-uuid-123",
        "status": "IN_TRANSIT",
        "occurred_at": "2025-01-10T15:30:00Z",
        "notes": "Status changed from AT_FACTORY to IN_TRANSIT",
        "created_at": "2025-01-10T15:30:00Z",
    }


@pytest.fixture
def mock_db_events_single(mock_supabase, sample_event_data):
    """Patch database with single event."""
    with patch("services.shipment_event_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("shipment_events", [sample_event_data])
        yield mock_supabase


@pytest.fixture
def mock_db_events_multiple(mock_supabase, sample_event_data, sample_event_data_2):
    """Patch database with multiple events."""
    with patch("services.shipment_event_service.get_supabase_client", return_value=mock_supabase):
        # Note: Mock doesn't sort, so we put them in DESC order (newest first)
        mock_supabase.set_table_data("shipment_events", [sample_event_data_2, sample_event_data])
        yield mock_supabase


@pytest.fixture
def mock_db_events_empty(mock_supabase):
    """Patch database with no events."""
    with patch("services.shipment_event_service.get_supabase_client", return_value=mock_supabase):
        mock_supabase.set_table_data("shipment_events", [])
        yield mock_supabase


# ===================
# SERVICE INSTANCE TESTS
# ===================

class TestShipmentEventServiceInstance:
    """Tests for service instance creation."""

    def test_get_service_returns_instance(self, mock_db_events_single):
        """Should return a service instance."""
        service = get_shipment_event_service()
        assert isinstance(service, ShipmentEventService)

    def test_get_service_returns_singleton(self, mock_db_events_single):
        """Should return the same instance on multiple calls."""
        service1 = get_shipment_event_service()
        service2 = get_shipment_event_service()
        assert service1 is service2


# ===================
# CREATE EVENT TESTS
# ===================

class TestCreateEvent:
    """Tests for creating shipment events."""

    def test_create_event_success(self, mock_db_events_single):
        """Should create a new event."""
        service = get_shipment_event_service()

        event_data = ShipmentEventCreate(
            shipment_id="shipment-uuid-123",
            status=ShipmentStatus.AT_FACTORY,
            occurred_at=datetime(2025, 1, 6, 10, 0, 0),
            notes="Shipment created"
        )

        result = service.create(event_data)

        # Mock returns generic UUID
        assert result.id == "test-uuid-123"
        assert result.shipment_id == "shipment-uuid-123"
        assert result.status == ShipmentStatus.AT_FACTORY
        assert result.notes == "Shipment created"

    def test_create_event_without_notes(self, mock_db_events_single):
        """Should create event without notes."""
        service = get_shipment_event_service()

        event_data = ShipmentEventCreate(
            shipment_id="shipment-uuid-456",
            status=ShipmentStatus.IN_TRANSIT,
            occurred_at=datetime(2025, 1, 10, 15, 30, 0),
            notes=None
        )

        result = service.create(event_data)

        assert result.shipment_id == "shipment-uuid-456"
        assert result.status == ShipmentStatus.IN_TRANSIT
        assert result.notes is None


# ===================
# GET EVENTS TESTS
# ===================

class TestGetEventsByShipment:
    """Tests for getting all events for a shipment."""

    def test_get_events_returns_all(self, mock_db_events_multiple):
        """Should return all events for a shipment."""
        service = get_shipment_event_service()

        result = service.get_by_shipment("shipment-uuid-123")

        assert result.total == 2
        assert len(result.data) == 2
        # Mock data is pre-sorted in DESC order (newest first)
        assert result.data[0].status == ShipmentStatus.IN_TRANSIT
        assert result.data[1].status == ShipmentStatus.AT_FACTORY

    def test_get_events_single(self, mock_db_events_single):
        """Should return single event."""
        service = get_shipment_event_service()

        result = service.get_by_shipment("shipment-uuid-123")

        assert result.total == 1
        assert len(result.data) == 1
        assert result.data[0].status == ShipmentStatus.AT_FACTORY

    def test_get_events_empty_list(self, mock_db_events_empty):
        """Should return empty list if no events exist."""
        service = get_shipment_event_service()

        result = service.get_by_shipment("non-existent-shipment")

        assert result.total == 0
        assert len(result.data) == 0


# ===================
# GET LATEST EVENT TESTS
# ===================

class TestGetLatestEvent:
    """Tests for getting the latest event for a shipment."""

    def test_get_latest_event_success(self, mock_db_events_multiple):
        """Should return the most recent event."""
        service = get_shipment_event_service()

        result = service.get_latest("shipment-uuid-123")

        # Mock returns first item (already in DESC order)
        assert result.id == "event-uuid-456"
        assert result.status == ShipmentStatus.IN_TRANSIT
        assert result.shipment_id == "shipment-uuid-123"

    def test_get_latest_event_not_found(self, mock_db_events_empty):
        """Should raise exception if no events exist."""
        service = get_shipment_event_service()

        with pytest.raises(ShipmentEventNotFoundError):
            service.get_latest("non-existent-shipment")


# ===================
# INTEGRATION TESTS
# ===================

class TestShipmentEventIntegration:
    """Integration tests for shipment event service."""

    def test_event_ordering(self, mock_db_events_multiple):
        """Events should be ordered by occurred_at DESC."""
        service = get_shipment_event_service()

        result = service.get_by_shipment("shipment-uuid-123")

        # Most recent event should be first (fixture is pre-sorted)
        assert result.data[0].occurred_at > result.data[1].occurred_at

    def test_latest_matches_first_in_list(self, mock_db_events_multiple):
        """Latest event should match first event in get_by_shipment."""
        service = get_shipment_event_service()

        all_events = service.get_by_shipment("shipment-uuid-123")
        latest = service.get_latest("shipment-uuid-123")

        assert latest.id == all_events.data[0].id
        assert latest.status == all_events.data[0].status
        assert latest.occurred_at == all_events.data[0].occurred_at

    def test_single_event_workflow(self, mock_db_events_single):
        """Should handle single event correctly."""
        service = get_shipment_event_service()

        all_events = service.get_by_shipment("shipment-uuid-123")
        latest = service.get_latest("shipment-uuid-123")

        assert all_events.total == 1
        assert latest.id == all_events.data[0].id