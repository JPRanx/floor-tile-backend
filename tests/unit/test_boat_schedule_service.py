"""
Unit tests for Boat Schedule service.

Tests CRUD operations, status updates, and Excel import.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch
import pytest

from services.boat_schedule_service import BoatScheduleService, get_boat_schedule_service
from models.boat_schedule import (
    BoatScheduleCreate,
    BoatScheduleUpdate,
    BoatScheduleStatusUpdate,
    BoatScheduleResponse,
    BoatStatus,
    RouteType,
)
from exceptions import BoatScheduleNotFoundError, DatabaseError


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_supabase():
    """Mock Supabase client."""
    with patch("services.boat_schedule_service.get_supabase_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def boat_service(mock_supabase):
    """Create BoatScheduleService with mocked Supabase."""
    # Reset singleton
    import services.boat_schedule_service as module
    module._boat_schedule_service = None
    return BoatScheduleService()


@pytest.fixture
def sample_schedule_data():
    """Sample boat schedule data from database."""
    return {
        "id": "schedule-uuid-123",
        "vessel_name": "CMA CGM FORT ST LOUIS",
        "shipping_line": "CMA CGM",
        "departure_date": "2026-01-15",
        "arrival_date": "2026-01-24",
        "transit_days": 9,
        "origin_port": "Cartagena",
        "destination_port": "Puerto Quetzal",
        "route_type": "direct",
        "booking_deadline": "2026-01-12",
        "status": "available",
        "source_file": "Tabla de Booking.xlsx",
        "created_at": "2025-12-10T10:00:00Z",
        "updated_at": None
    }


@pytest.fixture
def future_departure():
    """A future departure date."""
    return date.today() + timedelta(days=30)


@pytest.fixture
def future_arrival(future_departure):
    """Arrival date 9 days after departure."""
    return future_departure + timedelta(days=9)


# ===================
# GET ALL TESTS
# ===================

class TestGetAll:
    """Tests for get_all method."""

    def test_get_all_returns_schedules(self, boat_service, mock_supabase, sample_schedule_data):
        """get_all returns list of schedules."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[sample_schedule_data],
            count=1
        )

        schedules, total = boat_service.get_all()

        assert len(schedules) == 1
        assert total == 1
        assert schedules[0].id == "schedule-uuid-123"
        assert schedules[0].vessel_name == "CMA CGM FORT ST LOUIS"

    def test_get_all_with_status_filter(self, boat_service, mock_supabase, sample_schedule_data):
        """get_all filters by status."""
        mock_result = MagicMock(data=[sample_schedule_data], count=1)
        mock_supabase.table.return_value.select.return_value.eq.return_value.range.return_value.order.return_value.execute.return_value = mock_result

        schedules, total = boat_service.get_all(status="available")

        assert len(schedules) == 1
        mock_supabase.table.return_value.select.return_value.eq.assert_called_once()

    def test_get_all_with_date_filter(self, boat_service, mock_supabase, sample_schedule_data, future_departure):
        """get_all filters by date range."""
        mock_result = MagicMock(data=[sample_schedule_data], count=1)
        mock_supabase.table.return_value.select.return_value.gte.return_value.range.return_value.order.return_value.execute.return_value = mock_result

        schedules, total = boat_service.get_all(from_date=future_departure)

        assert len(schedules) == 1

    def test_get_all_empty_returns_empty_list(self, boat_service, mock_supabase):
        """get_all returns empty list when no schedules."""
        mock_supabase.table.return_value.select.return_value.range.return_value.order.return_value.execute.return_value = MagicMock(
            data=[],
            count=0
        )

        schedules, total = boat_service.get_all()

        assert len(schedules) == 0
        assert total == 0


# ===================
# GET BY ID TESTS
# ===================

class TestGetById:
    """Tests for get_by_id method."""

    def test_get_by_id_returns_schedule(self, boat_service, mock_supabase, sample_schedule_data):
        """get_by_id returns schedule when found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_schedule_data
        )

        schedule = boat_service.get_by_id("schedule-uuid-123")

        assert schedule.id == "schedule-uuid-123"
        assert schedule.vessel_name == "CMA CGM FORT ST LOUIS"

    def test_get_by_id_not_found_raises_error(self, boat_service, mock_supabase):
        """get_by_id raises error when not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("0 rows")

        with pytest.raises(BoatScheduleNotFoundError):
            boat_service.get_by_id("nonexistent-uuid")


# ===================
# GET AVAILABLE TESTS
# ===================

class TestGetAvailable:
    """Tests for get_available method."""

    def test_get_available_returns_available_only(self, boat_service, mock_supabase, sample_schedule_data):
        """get_available returns only available schedules."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.gte.return_value.execute.return_value = MagicMock(
            data=[sample_schedule_data]
        )

        schedules = boat_service.get_available()

        assert len(schedules) == 1
        mock_supabase.table.return_value.select.return_value.eq.assert_called_with("status", "available")

    def test_get_available_respects_limit(self, boat_service, mock_supabase, sample_schedule_data):
        """get_available respects limit parameter."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.gte.return_value.execute.return_value = MagicMock(
            data=[sample_schedule_data]
        )

        boat_service.get_available(limit=5)

        mock_supabase.table.return_value.select.return_value.eq.return_value.order.return_value.limit.assert_called_with(5)


# ===================
# CREATE TESTS
# ===================

class TestCreate:
    """Tests for create method."""

    def test_create_returns_schedule(self, boat_service, mock_supabase, sample_schedule_data, future_departure, future_arrival):
        """create returns created schedule."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_schedule_data]
        )

        create_data = BoatScheduleCreate(
            vessel_name="CMA CGM FORT ST LOUIS",
            shipping_line="CMA CGM",
            departure_date=future_departure,
            arrival_date=future_arrival,
            transit_days=9,
            origin_port="Cartagena",
            destination_port="Puerto Quetzal",
            route_type=RouteType.DIRECT,
        )

        schedule = boat_service.create(create_data)

        assert schedule.id == "schedule-uuid-123"
        mock_supabase.table.return_value.insert.assert_called_once()

    def test_create_calculates_booking_deadline(self, boat_service, mock_supabase, sample_schedule_data, future_departure, future_arrival):
        """create calculates booking_deadline as departure - 3 days."""
        mock_supabase.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[sample_schedule_data]
        )

        create_data = BoatScheduleCreate(
            departure_date=future_departure,
            arrival_date=future_arrival,
            transit_days=9,
        )

        boat_service.create(create_data)

        # Verify the insert was called with correct booking_deadline
        call_args = mock_supabase.table.return_value.insert.call_args
        insert_data = call_args[0][0]
        expected_deadline = (future_departure - timedelta(days=3)).isoformat()
        assert insert_data["booking_deadline"] == expected_deadline


# ===================
# UPDATE TESTS
# ===================

class TestUpdate:
    """Tests for update method."""

    def test_update_returns_updated_schedule(self, boat_service, mock_supabase, sample_schedule_data):
        """update returns updated schedule."""
        # Mock get_by_id
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_schedule_data
        )
        # Mock update
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[sample_schedule_data]
        )

        update_data = BoatScheduleUpdate(vessel_name="NEW VESSEL NAME")
        schedule = boat_service.update("schedule-uuid-123", update_data)

        assert schedule.id == "schedule-uuid-123"
        mock_supabase.table.return_value.update.assert_called_once()

    def test_update_not_found_raises_error(self, boat_service, mock_supabase):
        """update raises error when schedule not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("0 rows")

        update_data = BoatScheduleUpdate(vessel_name="NEW VESSEL NAME")

        with pytest.raises(BoatScheduleNotFoundError):
            boat_service.update("nonexistent-uuid", update_data)


# ===================
# STATUS UPDATE TESTS
# ===================

class TestUpdateStatus:
    """Tests for update_status method."""

    def test_update_status_to_booked(self, boat_service, mock_supabase, sample_schedule_data):
        """update_status changes status to booked."""
        # Mock get_by_id
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_schedule_data
        )
        # Mock update - return booked status
        booked_data = {**sample_schedule_data, "status": "booked"}
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[booked_data]
        )

        status_data = BoatScheduleStatusUpdate(status=BoatStatus.BOOKED)
        schedule = boat_service.update_status("schedule-uuid-123", status_data)

        assert schedule.status == BoatStatus.BOOKED

    def test_update_status_to_departed(self, boat_service, mock_supabase, sample_schedule_data):
        """update_status changes status to departed."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_schedule_data
        )
        departed_data = {**sample_schedule_data, "status": "departed"}
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[departed_data]
        )

        status_data = BoatScheduleStatusUpdate(status=BoatStatus.DEPARTED)
        schedule = boat_service.update_status("schedule-uuid-123", status_data)

        assert schedule.status == BoatStatus.DEPARTED


# ===================
# DELETE TESTS
# ===================

class TestDelete:
    """Tests for delete method."""

    def test_delete_returns_true(self, boat_service, mock_supabase, sample_schedule_data):
        """delete returns True on success."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
            data=sample_schedule_data
        )
        mock_supabase.table.return_value.delete.return_value.eq.return_value.execute.return_value = MagicMock()

        result = boat_service.delete("schedule-uuid-123")

        assert result is True

    def test_delete_not_found_raises_error(self, boat_service, mock_supabase):
        """delete raises error when schedule not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("0 rows")

        with pytest.raises(BoatScheduleNotFoundError):
            boat_service.delete("nonexistent-uuid")


# ===================
# SINGLETON TESTS
# ===================

class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_boat_schedule_service_returns_same_instance(self, mock_supabase):
        """get_boat_schedule_service returns the same instance."""
        import services.boat_schedule_service as module
        module._boat_schedule_service = None

        service1 = get_boat_schedule_service()
        service2 = get_boat_schedule_service()

        assert service1 is service2


# ===================
# RESPONSE COMPUTED FIELDS TESTS
# ===================

class TestResponseComputedFields:
    """Tests for BoatScheduleResponse computed fields."""

    def test_days_until_departure_calculated(self, sample_schedule_data):
        """days_until_departure is calculated correctly."""
        # Set departure to 10 days from now
        future_date = date.today() + timedelta(days=10)
        sample_schedule_data["departure_date"] = future_date.isoformat()
        sample_schedule_data["booking_deadline"] = (future_date - timedelta(days=3)).isoformat()

        response = BoatScheduleResponse.from_db(sample_schedule_data)

        assert response.days_until_departure == 10

    def test_days_until_deadline_calculated(self, sample_schedule_data):
        """days_until_deadline is calculated correctly."""
        future_date = date.today() + timedelta(days=10)
        deadline = future_date - timedelta(days=3)
        sample_schedule_data["departure_date"] = future_date.isoformat()
        sample_schedule_data["booking_deadline"] = deadline.isoformat()

        response = BoatScheduleResponse.from_db(sample_schedule_data)

        assert response.days_until_deadline == 7

    def test_past_departure_returns_none(self, sample_schedule_data):
        """days_until_departure is None for past dates."""
        past_date = date.today() - timedelta(days=5)
        sample_schedule_data["departure_date"] = past_date.isoformat()
        sample_schedule_data["booking_deadline"] = (past_date - timedelta(days=3)).isoformat()

        response = BoatScheduleResponse.from_db(sample_schedule_data)

        assert response.days_until_departure is None
        assert response.days_until_deadline is None
