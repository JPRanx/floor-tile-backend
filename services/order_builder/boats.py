"""Boat lookup and coverage buffer mixin for Order Builder."""

from typing import Optional
from datetime import date, timedelta

import structlog

from config.shipping import (
    WAREHOUSE_BUFFER_DAYS,
    ORDERING_CYCLE_DAYS,
)
from models.order_builder import OrderBuilderBoat

logger = structlog.get_logger(__name__)


class BoatsMixin:
    """Boat lookup and coverage buffer logic."""

    def _get_boats(
        self,
        boat_id: Optional[str]
    ) -> tuple[Optional[OrderBuilderBoat], Optional[OrderBuilderBoat]]:
        """Get target boat and next boat after that."""
        today = date.today()

        if boat_id:
            # Get specific boat
            try:
                boat_data = self.boat_service.get_by_id(boat_id)
                boat = self._to_order_builder_boat(boat_data, today)
            except Exception:
                logger.warning("boat_not_found", boat_id=boat_id)
                boat = None
        else:
            # Get next available boat
            boat_data = self.boat_service.get_next_available()
            boat = self._to_order_builder_boat(boat_data, today) if boat_data else None

        # Get the next boat after this one
        next_boat = None
        if boat:
            # Query boats departing after the selected boat
            boats_after = self.boat_service.get_available(
                from_date=boat.departure_date + timedelta(days=1),
                limit=1
            )
            if boats_after:
                next_boat = self._to_order_builder_boat(boats_after[0], today)
        else:
            available_boats = self.boat_service.get_available(limit=2)
            if len(available_boats) > 1:
                next_boat = self._to_order_builder_boat(available_boats[1], today)

        return boat, next_boat

    def _to_order_builder_boat(self, boat_data, today: date) -> OrderBuilderBoat:
        """Convert BoatScheduleResponse to OrderBuilderBoat."""
        days_until_departure = (boat_data.departure_date - today).days
        days_until_arrival = (boat_data.arrival_date - today).days
        days_until_deadline = (boat_data.booking_deadline - today).days

        # Order deadline is 30 days before departure (from boat_data)
        order_deadline = boat_data.order_deadline
        days_until_order_deadline = (order_deadline - today).days  # Can be negative
        past_order_deadline = today > order_deadline

        # days_until_warehouse = arrival + port buffer + trucking
        # This is the TRUE lead time for coverage calculation
        days_until_warehouse = days_until_arrival + WAREHOUSE_BUFFER_DAYS

        return OrderBuilderBoat(
            boat_id=boat_data.id,
            name=boat_data.vessel_name or f"Boat {boat_data.departure_date}",
            departure_date=boat_data.departure_date,
            arrival_date=boat_data.arrival_date,
            days_until_departure=max(0, days_until_departure),
            days_until_arrival=max(0, days_until_arrival),
            days_until_warehouse=max(0, days_until_warehouse),
            order_deadline=order_deadline,
            days_until_order_deadline=days_until_order_deadline,
            past_order_deadline=past_order_deadline,
            booking_deadline=boat_data.booking_deadline,
            days_until_deadline=max(0, days_until_deadline),
            max_containers=5,  # Default, could be configurable per boat
            carrier=boat_data.carrier,
        )

    def _get_coverage_buffer(
        self,
        current_boat: Optional[OrderBuilderBoat],
        next_boat: Optional[OrderBuilderBoat]
    ) -> int:
        """
        Calculate days of coverage needed until next boat arrives at warehouse.

        Dynamic calculation based on actual boat schedule instead of hardcoded 30 days.

        Logic:
        - If next_boat exists: days between current boat arrival and next boat arrival
        - Otherwise: fall back to ORDERING_CYCLE_DAYS (30 days)

        This ensures we order enough to last until the NEXT shipment is in warehouse.
        """
        if current_boat and next_boat and next_boat.arrival_date and current_boat.arrival_date:
            # Days between current boat warehouse arrival and next boat warehouse arrival
            buffer = (next_boat.arrival_date - current_boat.arrival_date).days
            # Add warehouse buffer (port + trucking) since we need to cover until next is IN warehouse
            buffer += WAREHOUSE_BUFFER_DAYS
            # Sanity check: at least 14 days, at most 60 days
            buffer = max(14, min(60, buffer))
            logger.debug(
                "dynamic_coverage_buffer",
                current_arrival=str(current_boat.arrival_date),
                next_arrival=str(next_boat.arrival_date),
                buffer_days=buffer
            )
            return buffer

        # Fallback to static value if no next boat scheduled
        logger.debug("coverage_buffer_fallback", reason="no_next_boat")
        return ORDERING_CYCLE_DAYS
