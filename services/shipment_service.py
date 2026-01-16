"""
Shipment service for business logic operations.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
from decimal import Decimal
from datetime import datetime
import structlog

from config import get_supabase_client
from models.shipment import (
    ShipmentCreate,
    ShipmentUpdate,
    ShipmentStatusUpdate,
    ShipmentResponse,
    ShipmentStatus,
    is_valid_shipment_status_transition,
)
from models.shipment_event import ShipmentEventCreate
from models.factory_order import OrderStatus, FactoryOrderStatusUpdate
from models.alert import AlertType, AlertSeverity, AlertCreate
from services.shipment_event_service import get_shipment_event_service
from services.factory_order_service import get_factory_order_service
from services.alert_service import get_alert_service
from integrations.telegram_messages import get_message
from exceptions import (
    ShipmentNotFoundError,
    ShipmentBookingExistsError,
    ShipmentSHPExistsError,
    InvalidStatusTransitionError,
    DatabaseError,
    FactoryOrderNotFoundError,
)

logger = structlog.get_logger(__name__)


class ShipmentService:
    """
    Shipment business logic.

    Handles CRUD operations for shipments.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "shipments"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        status: Optional[ShipmentStatus] = None,
        factory_order_id: Optional[str] = None,
        active_only: bool = True
    ) -> tuple[list[ShipmentResponse], int]:
        """
        Get all shipments with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Filter by status
            factory_order_id: Filter by factory order
            active_only: Only return active shipments

        Returns:
            Tuple of (shipments list, total count)
        """
        logger.info(
            "getting_shipments",
            page=page,
            page_size=page_size,
            status=status,
            factory_order_id=factory_order_id
        )

        try:
            # Build query
            query = self.db.table(self.table).select("*", count="exact")

            # Apply filters
            if active_only:
                query = query.eq("active", True)
            if status:
                query = query.eq("status", status.value)
            if factory_order_id:
                query = query.eq("factory_order_id", factory_order_id)

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by eta descending (upcoming shipments first)
            query = query.order("eta", desc=True, nullsfirst=False)

            # Execute
            result = query.execute()

            shipments = [
                self._row_to_response(row) for row in result.data
            ]

            total = result.count or 0

            logger.info(
                "shipments_retrieved",
                count=len(shipments),
                total=total
            )

            return shipments, total

        except Exception as e:
            logger.error("get_shipments_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, shipment_id: str) -> ShipmentResponse:
        """
        Get a single shipment by ID.

        Args:
            shipment_id: Shipment UUID

        Returns:
            ShipmentResponse

        Raises:
            ShipmentNotFoundError: If shipment doesn't exist
        """
        logger.debug("getting_shipment", shipment_id=shipment_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", shipment_id)
                .single()
                .execute()
            )

            if not result.data:
                raise ShipmentNotFoundError(shipment_id)

            return self._row_to_response(result.data)

        except ShipmentNotFoundError:
            raise
        except Exception as e:
            logger.error("get_shipment_failed", shipment_id=shipment_id, error=str(e))
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise ShipmentNotFoundError(shipment_id)
            raise DatabaseError("select", str(e))

    def get_by_booking_number(self, booking_number: str) -> Optional[ShipmentResponse]:
        """
        Get a shipment by booking number.

        Args:
            booking_number: CMA CGM booking reference

        Returns:
            ShipmentResponse or None if not found
        """
        logger.debug("getting_shipment_by_booking", booking_number=booking_number)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("booking_number", booking_number.upper())
                .execute()
            )

            if not result.data:
                return None

            return self._row_to_response(result.data[0])

        except Exception as e:
            logger.error("get_shipment_by_booking_failed", booking_number=booking_number, error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_shp_number(self, shp_number: str) -> Optional[ShipmentResponse]:
        """
        Get a shipment by SHP number.

        Handles flexible matching:
        - "0049831" matches "SHP0049831" (adds prefix)
        - "SHP0049831" matches "0049831" (strips prefix)
        - Exact match always tried first

        Args:
            shp_number: TIBA shipment reference (with or without SHP prefix)

        Returns:
            ShipmentResponse or None if not found
        """
        logger.debug("getting_shipment_by_shp", shp_number=shp_number)

        shp_upper = shp_number.upper().strip()

        # Build list of variants to try
        variants = [shp_upper]

        # If doesn't have SHP prefix, try with it
        if not shp_upper.startswith("SHP"):
            variants.append(f"SHP{shp_upper}")

        # If has SHP prefix, try without it
        if shp_upper.startswith("SHP"):
            variants.append(shp_upper[3:])  # Remove "SHP" prefix

        try:
            for variant in variants:
                result = (
                    self.db.table(self.table)
                    .select("*")
                    .eq("shp_number", variant)
                    .execute()
                )

                if result.data:
                    logger.debug(
                        "shipment_found_by_shp_variant",
                        original=shp_number,
                        matched_variant=variant
                    )
                    return self._row_to_response(result.data[0])

            return None

        except Exception as e:
            logger.error("get_shipment_by_shp_failed", shp_number=shp_number, error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_container_numbers(self, container_numbers: list[str]) -> Optional[ShipmentResponse]:
        """
        Get a shipment by container numbers.

        Finds shipment that has ANY of the provided containers.
        Useful for HBL/MBL matching when booking number is not available.

        Args:
            container_numbers: List of container numbers (e.g., ['OOLU1234567', 'OOLU7654321'])

        Returns:
            ShipmentResponse or None if no matching shipment found
        """
        if not container_numbers:
            return None

        logger.debug("getting_shipment_by_containers", container_count=len(container_numbers))

        # Normalize container numbers
        normalized = [c.upper().strip().replace(" ", "") for c in container_numbers if c]

        if not normalized:
            return None

        try:
            # DEBUG: Log what we're searching for
            print(f"=== CONTAINER MATCH DEBUG ===")
            print(f"Looking for containers: {normalized}")

            # Query containers table to find shipment_id for any matching container
            container_result = (
                self.db.table("containers")
                .select("shipment_id, container_number")
                .in_("container_number", normalized)
                .limit(1)
                .execute()
            )

            print(f"Query result: {container_result.data}")
            print(f"==============================")

            if not container_result.data:
                logger.debug("no_shipment_found_by_containers", containers=normalized)
                return None

            # Get the shipment by its ID
            shipment_id = container_result.data[0]["shipment_id"]

            logger.info(
                "shipment_found_by_container",
                shipment_id=shipment_id,
                matched_container=container_result.data[0]
            )

            return self.get_by_id(shipment_id)

        except ShipmentNotFoundError:
            return None
        except Exception as e:
            logger.error("get_shipment_by_containers_failed", containers=normalized, error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_factory_order_id(self, factory_order_id: str) -> list[ShipmentResponse]:
        """
        Get all shipments for a factory order.

        Args:
            factory_order_id: Factory order UUID

        Returns:
            List of shipments for that order
        """
        shipments, _ = self.get_all(
            page=1,
            page_size=1000,
            factory_order_id=factory_order_id
        )
        return shipments

    def get_by_status(self, status: ShipmentStatus) -> list[ShipmentResponse]:
        """
        Get all shipments with a specific status.

        Args:
            status: Shipment status to filter by

        Returns:
            List of shipments with that status
        """
        shipments, _ = self.get_all(page=1, page_size=1000, status=status)
        return shipments

    def get_awaiting_hbl(self, limit: int = 10) -> list[ShipmentResponse]:
        """
        Get shipments that have booking but no HBL/SHP data yet.

        These are candidates for manual HBL assignment from pending documents.

        Args:
            limit: Maximum number of results (default 10)

        Returns:
            List of shipments with booking_number but missing shp_number,
            ordered by created_at DESC (most recent first)
        """
        logger.debug("getting_shipments_awaiting_hbl", limit=limit)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .not_.is_("booking_number", "null")
                .is_("shp_number", "null")
                .eq("active", True)
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )

            shipments = [self._row_to_response(row) for row in result.data]

            logger.info(
                "shipments_awaiting_hbl_retrieved",
                count=len(shipments)
            )

            return shipments

        except Exception as e:
            logger.error("get_shipments_awaiting_hbl_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_recent(self, limit: int = 10) -> list[ShipmentResponse]:
        """
        Get most recent shipments.

        Used as fallback for candidate selection when document type
        doesn't have specific matching criteria.

        Args:
            limit: Maximum number of results (default 10)

        Returns:
            List of recent active shipments, ordered by created_at DESC
        """
        logger.debug("getting_recent_shipments", limit=limit)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("active", True)
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )

            shipments = [self._row_to_response(row) for row in result.data]

            logger.info(
                "recent_shipments_retrieved",
                count=len(shipments)
            )

            return shipments

        except Exception as e:
            logger.error("get_recent_shipments_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # WRITE OPERATIONS
    # ===================

    def create(self, data: ShipmentCreate) -> ShipmentResponse:
        """
        Create a new shipment.

        Args:
            data: Shipment creation data

        Returns:
            Created ShipmentResponse

        Raises:
            ShipmentBookingExistsError: If booking number already exists
            ShipmentSHPExistsError: If SHP number already exists
        """
        logger.info(
            "creating_shipment",
            booking_number=data.booking_number,
            shp_number=data.shp_number
        )

        # Check for duplicate booking number
        if data.booking_number:
            existing = self.get_by_booking_number(data.booking_number)
            if existing:
                raise ShipmentBookingExistsError(data.booking_number)

        # Check for duplicate SHP number
        if data.shp_number:
            existing = self.get_by_shp_number(data.shp_number)
            if existing:
                raise ShipmentSHPExistsError(data.shp_number)

        try:
            # Build shipment data
            shipment_data = {
                "factory_order_id": data.factory_order_id,
                "boat_schedule_id": data.boat_schedule_id,
                "shipping_company_id": data.shipping_company_id,
                "origin_port_id": data.origin_port_id,
                "destination_port_id": data.destination_port_id,
                "status": ShipmentStatus.AT_FACTORY.value,
                "booking_number": data.booking_number.upper() if data.booking_number else None,
                "shp_number": data.shp_number.upper() if data.shp_number else None,
                "bill_of_lading": data.bill_of_lading.upper() if data.bill_of_lading else None,
                "vessel_name": data.vessel_name,
                "voyage_number": data.voyage_number,
                "etd": data.etd.isoformat() if data.etd else None,
                "eta": data.eta.isoformat() if data.eta else None,
                "free_days": data.free_days,
                "freight_cost_usd": float(data.freight_cost_usd) if data.freight_cost_usd else None,
                "notes": data.notes,
                "active": True,
            }

            result = (
                self.db.table(self.table)
                .insert(shipment_data)
                .execute()
            )

            shipment_id = result.data[0]["id"]

            logger.info(
                "shipment_created",
                shipment_id=shipment_id,
                booking_number=data.booking_number,
                shp_number=data.shp_number
            )

            # Create initial shipment event
            event_service = get_shipment_event_service()
            event_service.create(ShipmentEventCreate(
                shipment_id=shipment_id,
                status=ShipmentStatus.AT_FACTORY,
                occurred_at=datetime.utcnow(),
                notes="Shipment created"
            ))

            # Auto-update factory order status to SHIPPED if linked
            if data.factory_order_id:
                try:
                    factory_order_service = get_factory_order_service()
                    factory_order_service.update_status(
                        order_id=data.factory_order_id,
                        data=FactoryOrderStatusUpdate(status=OrderStatus.SHIPPED)
                    )
                    logger.info(
                        "factory_order_auto_shipped",
                        factory_order_id=data.factory_order_id,
                        shipment_id=shipment_id
                    )
                except FactoryOrderNotFoundError:
                    # Log warning but don't fail shipment creation
                    # FK constraint already validated the ID exists
                    logger.warning(
                        "factory_order_not_found_for_auto_ship",
                        factory_order_id=data.factory_order_id
                    )
                except Exception as e:
                    # Log error but don't fail shipment creation
                    logger.error(
                        "factory_order_auto_ship_failed",
                        factory_order_id=data.factory_order_id,
                        error=str(e)
                    )

            return self.get_by_id(shipment_id)

        except (ShipmentBookingExistsError, ShipmentSHPExistsError):
            raise
        except Exception as e:
            logger.error("create_shipment_failed", error=str(e))
            raise DatabaseError("insert", str(e))

    def update(self, shipment_id: str, data: ShipmentUpdate) -> ShipmentResponse:
        """
        Update an existing shipment.

        Args:
            shipment_id: Shipment UUID
            data: Fields to update

        Returns:
            Updated ShipmentResponse

        Raises:
            ShipmentNotFoundError: If shipment doesn't exist
            ShipmentBookingExistsError: If new booking number already exists
            ShipmentSHPExistsError: If new SHP number already exists
        """
        logger.info("updating_shipment", shipment_id=shipment_id)

        # Check shipment exists
        existing = self.get_by_id(shipment_id)

        # If changing booking number, check for duplicates
        if data.booking_number and data.booking_number.upper() != existing.booking_number:
            booking_check = self.get_by_booking_number(data.booking_number)
            if booking_check:
                raise ShipmentBookingExistsError(data.booking_number)

        # If changing SHP number, check for duplicates
        if data.shp_number and data.shp_number.upper() != existing.shp_number:
            shp_check = self.get_by_shp_number(data.shp_number)
            if shp_check:
                raise ShipmentSHPExistsError(data.shp_number)

        try:
            # Build update dict (only include non-None fields)
            update_data = {}

            if data.factory_order_id is not None:
                update_data["factory_order_id"] = data.factory_order_id
            if data.boat_schedule_id is not None:
                update_data["boat_schedule_id"] = data.boat_schedule_id
            if data.shipping_company_id is not None:
                update_data["shipping_company_id"] = data.shipping_company_id
            if data.origin_port_id is not None:
                update_data["origin_port_id"] = data.origin_port_id
            if data.destination_port_id is not None:
                update_data["destination_port_id"] = data.destination_port_id
            if data.booking_number is not None:
                update_data["booking_number"] = data.booking_number.upper()
            if data.shp_number is not None:
                update_data["shp_number"] = data.shp_number.upper()
            if data.bill_of_lading is not None:
                update_data["bill_of_lading"] = data.bill_of_lading.upper()
            if data.vessel_name is not None:
                update_data["vessel_name"] = data.vessel_name
            if data.voyage_number is not None:
                update_data["voyage_number"] = data.voyage_number
            if data.etd is not None:
                update_data["etd"] = data.etd.isoformat()
            if data.eta is not None:
                update_data["eta"] = data.eta.isoformat()
            if data.actual_departure is not None:
                update_data["actual_departure"] = data.actual_departure.isoformat()
            if data.actual_arrival is not None:
                update_data["actual_arrival"] = data.actual_arrival.isoformat()
            if data.free_days is not None:
                update_data["free_days"] = data.free_days
            if data.free_days_expiry is not None:
                update_data["free_days_expiry"] = data.free_days_expiry.isoformat()
            if data.freight_cost_usd is not None:
                update_data["freight_cost_usd"] = float(data.freight_cost_usd)
            if data.notes is not None:
                update_data["notes"] = data.notes

            if not update_data:
                return self.get_by_id(shipment_id)

            self.db.table(self.table).update(update_data).eq("id", shipment_id).execute()

            logger.info(
                "shipment_updated",
                shipment_id=shipment_id,
                fields=list(update_data.keys())
            )

            return self.get_by_id(shipment_id)

        except (ShipmentNotFoundError, ShipmentBookingExistsError, ShipmentSHPExistsError):
            raise
        except Exception as e:
            logger.error("update_shipment_failed", shipment_id=shipment_id, error=str(e))
            raise DatabaseError("update", str(e))

    def update_status(self, shipment_id: str, data: ShipmentStatusUpdate) -> ShipmentResponse:
        """
        Update shipment status.

        Args:
            shipment_id: Shipment UUID
            data: New status

        Returns:
            Updated ShipmentResponse

        Raises:
            ShipmentNotFoundError: If shipment doesn't exist
            InvalidStatusTransitionError: If transition is not allowed
        """
        logger.info("updating_shipment_status", shipment_id=shipment_id, new_status=data.status)

        # Get current shipment
        existing = self.get_by_id(shipment_id)
        current_status = ShipmentStatus(existing.status)
        new_status = data.status

        # Validate transition
        if current_status == new_status:
            return self.get_by_id(shipment_id)

        if not is_valid_shipment_status_transition(current_status, new_status):
            raise InvalidStatusTransitionError(
                current_status=current_status.value,
                new_status=new_status.value,
                terminal_status="DELIVERED"
            )

        try:
            self.db.table(self.table).update({
                "status": new_status.value
            }).eq("id", shipment_id).execute()

            logger.info(
                "shipment_status_updated",
                shipment_id=shipment_id,
                from_status=current_status.value,
                to_status=new_status.value
            )

            # Create shipment event for status change
            event_service = get_shipment_event_service()
            event_service.create(ShipmentEventCreate(
                shipment_id=shipment_id,
                status=new_status,
                occurred_at=datetime.utcnow(),
                notes=f"Status changed from {current_status.value} to {new_status.value}"
            ))

            # Send Telegram alerts for key status changes
            try:
                alert_service = get_alert_service()
                shp_number = existing.shp_number or existing.booking_number or shipment_id[:8]

                if new_status == ShipmentStatus.IN_TRANSIT:
                    alert_service.create(
                        AlertCreate(
                            type=AlertType.SHIPMENT_DEPARTED,
                            severity=AlertSeverity.INFO,
                            title=get_message("title_shipment_departed", shp_number=shp_number),
                            message=get_message(
                                "shipment_departed",
                                shp_number=shp_number,
                                vessel=existing.vessel_name or "N/A",
                                eta=str(existing.eta) if existing.eta else "N/A"
                            ),
                            shipment_id=shipment_id,
                        ),
                        send_telegram=True
                    )

                elif new_status == ShipmentStatus.AT_DESTINATION_PORT:
                    # Get container count for the message
                    container_count = len(existing.containers) if existing.containers else 0
                    alert_service.create(
                        AlertCreate(
                            type=AlertType.SHIPMENT_ARRIVED,
                            severity=AlertSeverity.INFO,
                            title=get_message("title_shipment_at_port", shp_number=shp_number),
                            message=get_message(
                                "shipment_at_port",
                                shp_number=shp_number,
                                vessel=existing.vessel_name or "N/A",
                                containers=container_count
                            ),
                            shipment_id=shipment_id,
                        ),
                        send_telegram=True
                    )

                elif new_status == ShipmentStatus.DELIVERED:
                    alert_service.create(
                        AlertCreate(
                            type=AlertType.SHIPMENT_ARRIVED,
                            severity=AlertSeverity.INFO,
                            title=get_message("title_shipment_delivered", shp_number=shp_number),
                            message=get_message(
                                "shipment_delivered",
                                shp_number=shp_number,
                                vessel=existing.vessel_name or "N/A"
                            ),
                            shipment_id=shipment_id,
                        ),
                        send_telegram=True
                    )

            except Exception as alert_error:
                logger.warning("shipment_status_alert_failed", error=str(alert_error))

            return self.get_by_id(shipment_id)

        except (ShipmentNotFoundError, InvalidStatusTransitionError):
            raise
        except Exception as e:
            logger.error("update_shipment_status_failed", shipment_id=shipment_id, error=str(e))
            raise DatabaseError("update", str(e))

    def delete(self, shipment_id: str) -> bool:
        """
        Soft delete a shipment (set active=False).

        Args:
            shipment_id: Shipment UUID

        Returns:
            True if deleted

        Raises:
            ShipmentNotFoundError: If shipment doesn't exist
        """
        logger.info("deleting_shipment", shipment_id=shipment_id)

        # Check shipment exists
        self.get_by_id(shipment_id)

        try:
            self.db.table(self.table).update({
                "active": False
            }).eq("id", shipment_id).execute()

            logger.info("shipment_deleted", shipment_id=shipment_id)

            return True

        except ShipmentNotFoundError:
            raise
        except Exception as e:
            logger.error("delete_shipment_failed", shipment_id=shipment_id, error=str(e))
            raise DatabaseError("update", str(e))

    # ===================
    # UTILITY METHODS
    # ===================

    def booking_exists(self, booking_number: str) -> bool:
        """Check if a booking number already exists."""
        return self.get_by_booking_number(booking_number) is not None

    def shp_exists(self, shp_number: str) -> bool:
        """Check if a SHP number already exists."""
        return self.get_by_shp_number(shp_number) is not None

    def count(self, status: Optional[ShipmentStatus] = None, active_only: bool = True) -> int:
        """Count total shipments."""
        try:
            query = self.db.table(self.table).select("id", count="exact")
            if active_only:
                query = query.eq("active", True)
            if status:
                query = query.eq("status", status.value)
            result = query.execute()
            return result.count or 0
        except Exception as e:
            logger.error("count_shipments_failed", error=str(e))
            raise DatabaseError("count", str(e))

    def _row_to_response(self, row: dict) -> ShipmentResponse:
        """Convert database row to ShipmentResponse."""
        return ShipmentResponse(
            id=row["id"],
            factory_order_id=row.get("factory_order_id"),
            boat_schedule_id=row.get("boat_schedule_id"),
            shipping_company_id=row.get("shipping_company_id"),
            origin_port_id=row.get("origin_port_id"),
            destination_port_id=row.get("destination_port_id"),
            status=row["status"],
            active=row.get("active", True),
            booking_number=row.get("booking_number"),
            shp_number=row.get("shp_number"),
            bill_of_lading=row.get("bill_of_lading"),
            vessel_name=row.get("vessel_name"),
            voyage_number=row.get("voyage_number"),
            etd=row.get("etd"),
            eta=row.get("eta"),
            actual_departure=row.get("actual_departure"),
            actual_arrival=row.get("actual_arrival"),
            free_days=row.get("free_days"),
            free_days_expiry=row.get("free_days_expiry"),
            freight_cost_usd=Decimal(str(row["freight_cost_usd"])) if row.get("freight_cost_usd") else None,
            notes=row.get("notes"),
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
        )


# Singleton instance
_shipment_service: Optional[ShipmentService] = None


def get_shipment_service() -> ShipmentService:
    """Get or create ShipmentService instance."""
    global _shipment_service
    if _shipment_service is None:
        _shipment_service = ShipmentService()
    return _shipment_service
