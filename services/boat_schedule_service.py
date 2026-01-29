"""
Boat schedule service for business logic operations.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
from datetime import date, timedelta
from io import BytesIO
import structlog

from config import get_supabase_client
from models.boat_schedule import (
    BoatScheduleCreate,
    BoatScheduleUpdate,
    BoatScheduleStatusUpdate,
    BoatScheduleResponse,
    BoatStatus,
    BoatUploadResult,
    BOOKING_BUFFER_DAYS,
)
from parsers.tiba_parser import parse_tiba_excel, BoatScheduleRecord
from exceptions import (
    BoatScheduleNotFoundError,
    BoatScheduleUploadError,
    DatabaseError,
)

logger = structlog.get_logger(__name__)


class BoatScheduleService:
    """
    Boat schedule business logic.

    Handles CRUD operations and Excel imports for boat schedules.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "boat_schedules"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        status: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> tuple[list[BoatScheduleResponse], int]:
        """
        Get all boat schedules with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Filter by status (available, booked, departed, arrived)
            from_date: Filter departures after this date
            to_date: Filter departures before this date

        Returns:
            Tuple of (schedules list, total count)
        """
        logger.info(
            "getting_boat_schedules",
            page=page,
            page_size=page_size,
            status=status,
            from_date=from_date,
            to_date=to_date
        )

        try:
            # Build query
            query = self.db.table(self.table).select("*", count="exact")

            # Apply filters
            if status:
                query = query.eq("status", status)
            if from_date:
                query = query.gte("departure_date", from_date.isoformat())
            if to_date:
                query = query.lte("departure_date", to_date.isoformat())

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by departure date ascending (next departure first)
            query = query.order("departure_date", desc=False)

            # Execute
            result = query.execute()

            schedules = [
                BoatScheduleResponse.from_db(row)
                for row in result.data
            ]
            total = result.count or 0

            logger.info(
                "boat_schedules_retrieved",
                count=len(schedules),
                total=total
            )

            return schedules, total

        except Exception as e:
            logger.error("get_boat_schedules_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, schedule_id: str) -> BoatScheduleResponse:
        """
        Get a single boat schedule by ID.

        Args:
            schedule_id: Schedule UUID

        Returns:
            BoatScheduleResponse

        Raises:
            BoatScheduleNotFoundError: If schedule doesn't exist
        """
        logger.debug("getting_boat_schedule", schedule_id=schedule_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", schedule_id)
                .single()
                .execute()
            )

            if not result.data:
                raise BoatScheduleNotFoundError(schedule_id)

            return BoatScheduleResponse.from_db(result.data)

        except BoatScheduleNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "get_boat_schedule_failed",
                schedule_id=schedule_id,
                error=str(e)
            )
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise BoatScheduleNotFoundError(schedule_id)
            raise DatabaseError("select", str(e))

    def get_available(
        self,
        from_date: Optional[date] = None,
        limit: int = 10
    ) -> list[BoatScheduleResponse]:
        """
        Get available boat schedules for booking.

        Args:
            from_date: Only schedules with departure after this date
            limit: Maximum number to return

        Returns:
            List of available schedules ordered by departure date
        """
        logger.debug("getting_available_boats", from_date=from_date, limit=limit)

        try:
            query = (
                self.db.table(self.table)
                .select("*")
                .eq("status", "available")
                .order("departure_date", desc=False)
                .limit(limit)
            )

            if from_date:
                query = query.gte("departure_date", from_date.isoformat())
            else:
                # Default to future departures only
                query = query.gte("departure_date", date.today().isoformat())

            result = query.execute()

            return [BoatScheduleResponse.from_db(row) for row in result.data]

        except Exception as e:
            logger.error("get_available_boats_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_next_available(self) -> Optional[BoatScheduleResponse]:
        """
        Get the next available boat schedule.

        Returns:
            Next available schedule or None
        """
        available = self.get_available(limit=1)
        return available[0] if available else None

    def get_next_two_arrivals(self) -> tuple[Optional[date], Optional[date]]:
        """
        Get arrival dates of next 2 boats.

        Used for priority calculations:
        - HIGH_PRIORITY: stockout before next boat
        - CONSIDER: stockout before second boat
        - WELL_COVERED: stockout after second boat

        Returns:
            Tuple of (next_arrival_date, second_arrival_date)
            Either can be None if no boats scheduled
        """
        available = self.get_available(limit=2)

        next_arrival = available[0].arrival_date if len(available) > 0 else None
        second_arrival = available[1].arrival_date if len(available) > 1 else None

        logger.debug(
            "got_boat_arrivals",
            next_arrival=next_arrival,
            second_arrival=second_arrival
        )

        return next_arrival, second_arrival

    def get_next_two_departures(self) -> tuple[Optional[date], Optional[date]]:
        """
        Get departure dates of next 2 boats.

        Used for user-facing display (simpler than booking deadlines).

        Returns:
            Tuple of (next_departure_date, second_departure_date)
            Either can be None if no boats scheduled
        """
        available = self.get_available(limit=2)

        next_departure = available[0].departure_date if len(available) > 0 else None
        second_departure = available[1].departure_date if len(available) > 1 else None

        logger.debug(
            "got_boat_departures",
            next_departure=next_departure,
            second_departure=second_departure
        )

        return next_departure, second_departure

    def get_first_boat_after(
        self,
        ready_date: date,
        limit: int = 5
    ) -> Optional[BoatScheduleResponse]:
        """
        Get the first boat departing after a given date.

        Used for factory request calculations to determine which boat
        a production request would catch.

        Args:
            ready_date: Date when production will be ready
            limit: Max boats to check (for efficiency)

        Returns:
            First boat departing after ready_date, or None
        """
        logger.debug("getting_first_boat_after", ready_date=str(ready_date))

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("status", "available")
                .gt("departure_date", ready_date.isoformat())
                .order("departure_date", desc=False)
                .limit(limit)
                .execute()
            )

            if result.data:
                boat = BoatScheduleResponse.from_db(result.data[0])
                logger.debug(
                    "first_boat_after_found",
                    ready_date=str(ready_date),
                    boat_departure=str(boat.departure_date),
                    boat_name=boat.vessel_name
                )
                return boat

            logger.debug("no_boat_after_date", ready_date=str(ready_date))
            return None

        except Exception as e:
            logger.error("get_first_boat_after_failed", error=str(e))
            return None

    def get_boats_after(
        self,
        ready_date: date,
        limit: int = 3
    ) -> list[BoatScheduleResponse]:
        """
        Get boats departing after a given date.

        Args:
            ready_date: Date when production will be ready
            limit: Max boats to return

        Returns:
            List of boats departing after ready_date
        """
        logger.debug("getting_boats_after", ready_date=str(ready_date), limit=limit)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("status", "available")
                .gt("departure_date", ready_date.isoformat())
                .order("departure_date", desc=False)
                .limit(limit)
                .execute()
            )

            return [BoatScheduleResponse.from_db(row) for row in result.data]

        except Exception as e:
            logger.error("get_boats_after_failed", error=str(e))
            return []

    # ===================
    # WRITE OPERATIONS
    # ===================

    def create(self, data: BoatScheduleCreate) -> BoatScheduleResponse:
        """
        Create a new boat schedule.

        Args:
            data: Schedule data

        Returns:
            Created schedule
        """
        logger.info(
            "creating_boat_schedule",
            departure=data.departure_date,
            vessel=data.vessel_name
        )

        try:
            # Calculate booking deadline
            booking_deadline = data.departure_date - timedelta(days=BOOKING_BUFFER_DAYS)

            insert_data = {
                "vessel_name": data.vessel_name,
                "shipping_line": data.shipping_line,
                "departure_date": data.departure_date.isoformat(),
                "arrival_date": data.arrival_date.isoformat(),
                "transit_days": data.transit_days,
                "origin_port": data.origin_port,
                "destination_port": data.destination_port,
                "route_type": data.route_type.value if data.route_type else None,
                "booking_deadline": booking_deadline.isoformat(),
                "status": "available",
                "source_file": data.source_file,
            }

            result = (
                self.db.table(self.table)
                .insert(insert_data)
                .execute()
            )

            logger.info(
                "boat_schedule_created",
                schedule_id=result.data[0]["id"]
            )

            return BoatScheduleResponse.from_db(result.data[0])

        except Exception as e:
            logger.error("create_boat_schedule_failed", error=str(e))
            raise DatabaseError("insert", str(e))

    def update(
        self,
        schedule_id: str,
        data: BoatScheduleUpdate
    ) -> BoatScheduleResponse:
        """
        Update a boat schedule.

        Args:
            schedule_id: Schedule UUID
            data: Fields to update

        Returns:
            Updated schedule

        Raises:
            BoatScheduleNotFoundError: If schedule doesn't exist
        """
        logger.info("updating_boat_schedule", schedule_id=schedule_id)

        # Verify exists
        self.get_by_id(schedule_id)

        try:
            # Build update data (only non-None fields)
            update_data = {}
            if data.vessel_name is not None:
                update_data["vessel_name"] = data.vessel_name
            if data.shipping_line is not None:
                update_data["shipping_line"] = data.shipping_line
            if data.departure_date is not None:
                update_data["departure_date"] = data.departure_date.isoformat()
                # Recalculate booking deadline
                update_data["booking_deadline"] = (
                    data.departure_date - timedelta(days=BOOKING_BUFFER_DAYS)
                ).isoformat()
            if data.arrival_date is not None:
                update_data["arrival_date"] = data.arrival_date.isoformat()
            if data.transit_days is not None:
                update_data["transit_days"] = data.transit_days
            if data.origin_port is not None:
                update_data["origin_port"] = data.origin_port
            if data.destination_port is not None:
                update_data["destination_port"] = data.destination_port
            if data.route_type is not None:
                update_data["route_type"] = data.route_type.value
            if data.status is not None:
                update_data["status"] = data.status.value

            if not update_data:
                # Nothing to update, return existing
                return self.get_by_id(schedule_id)

            result = (
                self.db.table(self.table)
                .update(update_data)
                .eq("id", schedule_id)
                .execute()
            )

            logger.info("boat_schedule_updated", schedule_id=schedule_id)

            return BoatScheduleResponse.from_db(result.data[0])

        except BoatScheduleNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "update_boat_schedule_failed",
                schedule_id=schedule_id,
                error=str(e)
            )
            raise DatabaseError("update", str(e))

    def update_status(
        self,
        schedule_id: str,
        data: BoatScheduleStatusUpdate
    ) -> BoatScheduleResponse:
        """
        Update only the status of a boat schedule.

        Args:
            schedule_id: Schedule UUID
            data: New status

        Returns:
            Updated schedule
        """
        logger.info(
            "updating_boat_schedule_status",
            schedule_id=schedule_id,
            new_status=data.status.value
        )

        # Verify exists
        self.get_by_id(schedule_id)

        try:
            result = (
                self.db.table(self.table)
                .update({"status": data.status.value})
                .eq("id", schedule_id)
                .execute()
            )

            logger.info(
                "boat_schedule_status_updated",
                schedule_id=schedule_id,
                status=data.status.value
            )

            return BoatScheduleResponse.from_db(result.data[0])

        except BoatScheduleNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "update_status_failed",
                schedule_id=schedule_id,
                error=str(e)
            )
            raise DatabaseError("update", str(e))

    def delete(self, schedule_id: str) -> bool:
        """
        Delete a boat schedule.

        Args:
            schedule_id: Schedule UUID

        Returns:
            True if deleted

        Raises:
            BoatScheduleNotFoundError: If schedule doesn't exist
        """
        logger.info("deleting_boat_schedule", schedule_id=schedule_id)

        # Verify exists
        self.get_by_id(schedule_id)

        try:
            self.db.table(self.table).delete().eq("id", schedule_id).execute()

            logger.info("boat_schedule_deleted", schedule_id=schedule_id)
            return True

        except BoatScheduleNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "delete_boat_schedule_failed",
                schedule_id=schedule_id,
                error=str(e)
            )
            raise DatabaseError("delete", str(e))

    # ===================
    # IMPORT OPERATIONS
    # ===================

    def import_from_excel(
        self,
        file: BytesIO,
        filename: str
    ) -> BoatUploadResult:
        """
        Import boat schedules from TIBA Excel file.

        Args:
            file: Excel file as BytesIO
            filename: Original filename for tracking

        Returns:
            BoatUploadResult with counts and errors
        """
        logger.info("importing_boat_schedules", filename=filename)

        # Parse Excel
        parse_result = parse_tiba_excel(file)

        if not parse_result.success:
            # Convert parse errors to dicts
            errors = [
                {"row": e.row, "field": e.field, "error": e.error}
                for e in parse_result.errors
            ]
            raise BoatScheduleUploadError(errors)

        # Track results
        imported = 0
        updated = 0
        skipped = 0
        errors = []

        for record in parse_result.schedules:
            try:
                result = self._upsert_schedule(record, filename)
                if result == "inserted":
                    imported += 1
                elif result == "updated":
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                errors.append(f"Failed to save schedule {record.departure_date}: {str(e)}")
                logger.error(
                    "schedule_import_failed",
                    departure=record.departure_date,
                    error=str(e)
                )

        logger.info(
            "boat_schedules_imported",
            imported=imported,
            updated=updated,
            skipped=skipped,
            errors=len(errors)
        )

        return BoatUploadResult(
            imported=imported,
            updated=updated,
            skipped=skipped,
            errors=errors
        )

    def _upsert_schedule(
        self,
        record: BoatScheduleRecord,
        source_file: str
    ) -> str:
        """
        Insert or update a schedule record.

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        # Check for existing schedule with same departure + vessel
        existing = self._find_existing(
            record.departure_date,
            record.vessel_name
        )

        if existing:
            # Update if changed
            if self._needs_update(existing, record):
                self._update_from_record(existing["id"], record, source_file)
                return "updated"
            return "skipped"

        # Insert new
        self._insert_from_record(record, source_file)
        return "inserted"

    def _find_existing(
        self,
        departure_date: date,
        vessel_name: Optional[str]
    ) -> Optional[dict]:
        """Find existing schedule by departure date and vessel name."""
        try:
            query = (
                self.db.table(self.table)
                .select("*")
                .eq("departure_date", departure_date.isoformat())
            )

            if vessel_name:
                query = query.eq("vessel_name", vessel_name)
            else:
                query = query.is_("vessel_name", "null")

            result = query.execute()

            return result.data[0] if result.data else None

        except Exception:
            return None

    def _needs_update(self, existing: dict, record: BoatScheduleRecord) -> bool:
        """Check if existing record needs updating."""
        # Compare key fields
        if str(existing["arrival_date"]) != record.arrival_date.isoformat():
            return True
        if existing["transit_days"] != record.transit_days:
            return True
        if existing.get("shipping_line") != record.shipping_line:
            return True
        if existing.get("route_type") != record.route_type:
            return True
        return False

    def _insert_from_record(
        self,
        record: BoatScheduleRecord,
        source_file: str
    ) -> None:
        """Insert a new schedule from parsed record."""
        insert_data = {
            "vessel_name": record.vessel_name,
            "shipping_line": record.shipping_line,
            "departure_date": record.departure_date.isoformat(),
            "arrival_date": record.arrival_date.isoformat(),
            "transit_days": record.transit_days,
            "origin_port": record.origin_port,
            "destination_port": record.destination_port,
            "route_type": record.route_type,
            "booking_deadline": record.booking_deadline.isoformat(),
            "status": "available",
            "source_file": source_file,
        }

        self.db.table(self.table).insert(insert_data).execute()

    def _update_from_record(
        self,
        schedule_id: str,
        record: BoatScheduleRecord,
        source_file: str
    ) -> None:
        """Update existing schedule from parsed record."""
        update_data = {
            "arrival_date": record.arrival_date.isoformat(),
            "transit_days": record.transit_days,
            "shipping_line": record.shipping_line,
            "route_type": record.route_type,
            "booking_deadline": record.booking_deadline.isoformat(),
            "source_file": source_file,
        }

        self.db.table(self.table).update(update_data).eq("id", schedule_id).execute()


# ===================
# SINGLETON
# ===================

_boat_schedule_service: Optional[BoatScheduleService] = None


def get_boat_schedule_service() -> BoatScheduleService:
    """Get singleton instance of BoatScheduleService."""
    global _boat_schedule_service
    if _boat_schedule_service is None:
        _boat_schedule_service = BoatScheduleService()
    return _boat_schedule_service
