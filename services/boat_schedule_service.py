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
from config.settings import settings
from models.boat_schedule import (
    BoatScheduleCreate,
    BoatScheduleUpdate,
    BoatScheduleStatusUpdate,
    BoatScheduleResponse,
    BoatStatus,
    BoatUploadResult,
    SkippedRowInfo,
    BOOKING_BUFFER_DAYS,
    ORDER_DEADLINE_DAYS,
    HARD_DEADLINE_DAYS,
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

        Uses HARD_DEADLINE_DAYS (10 days before departure) as the visibility cutoff.
        This provides a 10-day grace period after the soft order deadline (20 days).

        Example: Boat departs Mar 20
        - Order deadline displayed: Mar 1 (departure - 20 days)
        - Boat visible until: Mar 10 (departure - 10 days)
        - After Mar 10: Boat hidden, next boat shown

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
                # Use hard deadline: only show boats departing > HARD_DEADLINE_DAYS from now
                # This keeps boats visible for 10 days after the soft order deadline
                cutoff_date = date.today() + timedelta(days=HARD_DEADLINE_DAYS)
                query = query.gte("departure_date", cutoff_date.isoformat())
                logger.debug(
                    "boat_visibility_cutoff",
                    today=date.today().isoformat(),
                    cutoff_date=cutoff_date.isoformat(),
                    hard_deadline_days=HARD_DEADLINE_DAYS
                )

            result = query.execute()

            # Filter out boats with ordered/confirmed drafts
            schedules = result.data
            if schedules:
                ordered_ids = self._get_ordered_boat_ids(
                    [s["id"] for s in schedules]
                )
                if ordered_ids:
                    schedules = [
                        s for s in schedules if s["id"] not in ordered_ids
                    ]

            return [BoatScheduleResponse.from_db(row) for row in schedules]

        except Exception as e:
            logger.error("get_available_boats_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def _get_ordered_boat_ids(self, boat_ids: list[str]) -> set[str]:
        """Get boat IDs that have any ordered/confirmed draft."""
        try:
            result = (
                self.db.table("boat_factory_drafts")
                .select("boat_id")
                .in_("boat_id", boat_ids)
                .in_("status", ["ordered", "confirmed"])
                .execute()
            )
            return {row["boat_id"] for row in result.data}
        except Exception:
            return set()

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
        buffer_days: Optional[int] = None,
        limit: int = 5
    ) -> Optional[BoatScheduleResponse]:
        """
        Get the first boat whose ORDER DEADLINE is safely after a given date.

        Used for factory request calculations to determine which boat
        a production request would catch. Matches against order_deadline
        (not departure_date) to ensure we have time to prepare booking docs.

        Applies a safety buffer (default from settings.production_buffer_days)
        to account for production schedule slippage.

        Formula: order_deadline > (ready_date + buffer)
        Since order_deadline = departure - ORDER_DEADLINE_DAYS:
        departure > ready_date + ORDER_DEADLINE_DAYS + buffer

        Args:
            ready_date: Date when production will be ready
            buffer_days: Safety buffer days (default from settings)
            limit: Max boats to check (for efficiency)

        Returns:
            First boat with order_deadline > (ready_date + buffer), or None
        """
        # Use settings buffer if not specified
        if buffer_days is None:
            buffer_days = settings.production_buffer_days

        # Calculate safe ready date (ready + buffer)
        safe_ready_date = ready_date + timedelta(days=buffer_days)

        # To find order_deadline > safe_ready_date, we need:
        # departure > safe_ready_date + ORDER_DEADLINE_DAYS
        min_departure = safe_ready_date + timedelta(days=ORDER_DEADLINE_DAYS)

        logger.debug(
            "getting_first_boat_after",
            ready_date=str(ready_date),
            buffer_days=buffer_days,
            safe_ready_date=str(safe_ready_date),
            min_departure=str(min_departure)
        )

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("status", "available")
                .gt("departure_date", min_departure.isoformat())
                .order("departure_date", desc=False)
                .limit(limit)
                .execute()
            )

            if result.data:
                boat = BoatScheduleResponse.from_db(result.data[0])
                logger.debug(
                    "first_boat_after_found",
                    ready_date=str(ready_date),
                    buffer_days=buffer_days,
                    safe_ready_date=str(safe_ready_date),
                    boat_departure=str(boat.departure_date),
                    boat_order_deadline=str(boat.order_deadline),
                    boat_name=boat.vessel_name
                )
                return boat

            logger.debug(
                "no_boat_after_date",
                ready_date=str(ready_date),
                safe_ready_date=str(safe_ready_date)
            )
            return None

        except Exception as e:
            logger.error("get_first_boat_after_failed", error=str(e))
            return None

    def get_boats_after(
        self,
        ready_date: date,
        buffer_days: Optional[int] = None,
        limit: int = 3
    ) -> list[BoatScheduleResponse]:
        """
        Get boats whose ORDER DEADLINE is safely after a given date.

        Matches against order_deadline (not departure_date) to ensure
        we have time to prepare booking documents.

        Applies a safety buffer (default from settings.production_buffer_days)
        to account for production schedule slippage.

        Args:
            ready_date: Date when production will be ready
            buffer_days: Safety buffer days (default from settings)
            limit: Max boats to return

        Returns:
            List of boats with order_deadline > (ready_date + buffer)
        """
        # Use settings buffer if not specified
        if buffer_days is None:
            buffer_days = settings.production_buffer_days

        # Calculate safe ready date (ready + buffer)
        safe_ready_date = ready_date + timedelta(days=buffer_days)

        # To find order_deadline > safe_ready_date, we need:
        # departure > safe_ready_date + ORDER_DEADLINE_DAYS
        min_departure = safe_ready_date + timedelta(days=ORDER_DEADLINE_DAYS)

        logger.debug(
            "getting_boats_after",
            ready_date=str(ready_date),
            buffer_days=buffer_days,
            safe_ready_date=str(safe_ready_date),
            min_departure=str(min_departure),
            limit=limit
        )

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("status", "available")
                .gt("departure_date", min_departure.isoformat())
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
                "carrier": data.carrier,
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

        # Verify exists and capture old state for draft invalidation (5b)
        existing_schedule = self.get_by_id(schedule_id)

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

            # Flag related drafts when departure date changes (5b)
            if data.departure_date is not None:
                old_departure = existing_schedule.departure_date
                if old_departure != data.departure_date:
                    try:
                        self._flag_drafts_on_reschedule(
                            schedule_id,
                            str(old_departure),
                            str(data.departure_date),
                        )
                    except Exception as flag_err:
                        logger.warning(
                            "draft_flag_on_reschedule_failed",
                            schedule_id=schedule_id,
                            error=str(flag_err),
                        )

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

        Uses wipe-and-replace strategy: positionally replaces existing boats
        with file contents to avoid date-matching issues. Preserves IDs for
        FK-referenced boats by updating in-place.

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
            # Fatal parse errors (missing sheet, missing columns, etc.)
            errors = [
                {"row": e.row, "field": e.field, "error": e.error}
                for e in parse_result.errors
            ]
            raise BoatScheduleUploadError(errors)

        # Convert skipped rows from parser
        skipped_rows = [
            SkippedRowInfo(row=s.row, reason=s.reason)
            for s in parse_result.skipped_rows
        ]

        if skipped_rows:
            logger.warning(
                "boat_import_skipped_rows",
                count=len(skipped_rows),
                rows=[s.row for s in skipped_rows],
            )

        return self._wipe_and_replace(
            parse_result.schedules, filename, skipped_rows
        )

    def import_from_records(
        self,
        records: list[BoatScheduleRecord],
        filename: str,
    ) -> BoatUploadResult:
        """
        Import boat schedules from pre-parsed records (used by confirm with modifications).

        Uses the same wipe-and-replace strategy as import_from_excel.
        """
        return self._wipe_and_replace(records, filename, [])

    def _wipe_and_replace(
        self,
        new_records: list[BoatScheduleRecord],
        filename: str,
        skipped_rows: list[SkippedRowInfo],
    ) -> BoatUploadResult:
        """
        Positional wipe-and-replace: update existing boats in-place,
        insert extras, delete surplus. Preserves IDs for FK safety.
        """
        # Sort new boats by departure date
        new_boats = sorted(new_records, key=lambda r: r.departure_date)

        # Get all existing boats sorted by departure date
        existing_boats = self._get_all_sorted()

        n_new = len(new_boats)
        n_existing = len(existing_boats)

        logger.info(
            "wipe_and_replace_start",
            new_count=n_new,
            existing_count=n_existing,
            filename=filename,
        )

        imported = 0
        updated = 0
        errors = []

        # Phase 1: Update existing boats in-place (positional replacement)
        for i in range(min(n_new, n_existing)):
            try:
                self._replace_boat(
                    existing_boats[i]["id"], new_boats[i], filename
                )
                updated += 1
            except Exception as e:
                errors.append(
                    f"Failed to update boat {new_boats[i].departure_date}: {str(e)}"
                )
                logger.error(
                    "boat_replace_failed",
                    boat_id=existing_boats[i]["id"],
                    error=str(e),
                )

        # Phase 2: Insert excess new boats
        for i in range(n_existing, n_new):
            try:
                self._insert_from_record(new_boats[i], filename)
                imported += 1
            except Exception as e:
                errors.append(
                    f"Failed to insert boat {new_boats[i].departure_date}: {str(e)}"
                )
                logger.error(
                    "boat_insert_failed",
                    departure=new_boats[i].departure_date,
                    error=str(e),
                )

        # Phase 3: Delete surplus existing boats
        deleted = 0
        for i in range(n_new, n_existing):
            try:
                self._delete_boat_cascade(existing_boats[i]["id"])
                deleted += 1
            except Exception as e:
                errors.append(
                    f"Failed to delete surplus boat {existing_boats[i]['id']}: {str(e)}"
                )
                logger.error(
                    "boat_delete_failed",
                    boat_id=existing_boats[i]["id"],
                    error=str(e),
                )

        logger.info(
            "wipe_and_replace_complete",
            imported=imported,
            updated=updated,
            deleted=deleted,
            errors=len(errors),
        )

        return BoatUploadResult(
            imported=imported,
            updated=updated,
            skipped=0,
            skipped_rows=skipped_rows,
            errors=errors,
        )

    def _get_all_sorted(self) -> list[dict]:
        """Get all boat schedules sorted by departure date."""
        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .order("departure_date", desc=False)
                .execute()
            )
            return result.data
        except Exception as e:
            logger.error("get_all_sorted_failed", error=str(e))
            return []

    def _replace_boat(
        self,
        boat_id: str,
        record: BoatScheduleRecord,
        source_file: str,
    ) -> None:
        """Replace all fields of an existing boat (preserving ID for FK safety)."""
        today = date.today()

        # Auto-assign status based on dates
        if record.arrival_date < today:
            status = "arrived"
        elif record.departure_date < today:
            status = "departed"
        else:
            status = "available"

        update_data = {
            "vessel_name": record.vessel_name,
            "shipping_line": record.shipping_line,
            "departure_date": record.departure_date.isoformat(),
            "arrival_date": record.arrival_date.isoformat(),
            "transit_days": record.transit_days,
            "origin_port": record.origin_port,
            "destination_port": record.destination_port,
            "route_type": record.route_type,
            "booking_deadline": record.booking_deadline.isoformat(),
            "source_file": source_file,
            "carrier": "TIBA",
            "status": status,
        }

        self.db.table(self.table).update(update_data).eq("id", boat_id).execute()

    def _delete_boat_cascade(self, boat_id: str) -> None:
        """Delete a boat and its child references."""
        # Delete child records that have NOT NULL FK
        self.db.table("boat_factory_drafts").delete().eq("boat_id", boat_id).execute()
        # Null out nullable FK references
        self.db.table("shipments").update(
            {"boat_schedule_id": None}
        ).eq("boat_schedule_id", boat_id).execute()
        self.db.table("warehouse_orders").update(
            {"boat_id": None}
        ).eq("boat_id", boat_id).execute()
        # Now delete the boat
        self.db.table(self.table).delete().eq("id", boat_id).execute()

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
        today = date.today()

        # Auto-assign status based on dates
        if record.arrival_date < today:
            status = "arrived"
        elif record.departure_date < today:
            status = "departed"
        else:
            status = "available"

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
            "status": status,
            "source_file": source_file,
            "carrier": "TIBA",
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
        # Don't overwrite carrier on update — it may have been set manually

        self.db.table(self.table).update(update_data).eq("id", schedule_id).execute()

    def _flag_drafts_on_reschedule(
        self, boat_id: str, old_departure: str, new_departure: str
    ) -> None:
        """
        Flag all active drafts for a boat when its departure date changes.

        Only flags drafts in 'drafting' or 'action_needed' status — ordered/confirmed
        drafts are already committed and should not be disturbed.

        Args:
            boat_id: Boat schedule UUID
            old_departure: Previous departure date (string for logging)
            new_departure: New departure date (string for logging)
        """
        drafts = (
            self.db.table("boat_factory_drafts")
            .select("id, factory_id, status")
            .eq("boat_id", boat_id)
            .in_("status", ["drafting", "action_needed"])
            .execute()
        )

        flagged = 0
        for draft in (drafts.data or []):
            try:
                self.db.table("boat_factory_drafts").update({
                    "status": "action_needed",
                    "notes": f"Barco reprogramado: {old_departure} → {new_departure}",
                }).eq("id", draft["id"]).execute()
                flagged += 1
            except Exception as e:
                logger.warning(
                    "draft_flag_individual_failed",
                    draft_id=draft["id"],
                    error=str(e),
                )

        if flagged > 0:
            logger.info(
                "drafts_flagged_on_reschedule",
                boat_id=boat_id,
                old_departure=old_departure,
                new_departure=new_departure,
                drafts_flagged=flagged,
            )


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
