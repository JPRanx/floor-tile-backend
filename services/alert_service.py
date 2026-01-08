"""
Alert service for business logic operations.

Handles CRUD operations for alerts and generates alerts based on system state.
"""

from typing import Optional
from datetime import date, timedelta
import structlog

from config import get_supabase_client
from models.alert import (
    AlertType,
    AlertSeverity,
    AlertCreate,
    AlertUpdate,
    AlertResponse,
)
from services.stockout_service import get_stockout_service
from services.boat_schedule_service import get_boat_schedule_service
from integrations.telegram import send_alert_to_telegram, TelegramError
from exceptions import NotFoundError, DatabaseError

logger = structlog.get_logger(__name__)


class AlertService:
    """
    Alert business logic.

    Handles CRUD operations for alerts and alert generation.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "alerts"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        is_read: Optional[bool] = None,
        severity: Optional[AlertSeverity] = None,
    ) -> tuple[list[AlertResponse], int]:
        """
        Get all alerts with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            is_read: Filter by read status
            severity: Filter by severity

        Returns:
            Tuple of (alerts list, total count)
        """
        logger.info(
            "getting_alerts",
            page=page,
            page_size=page_size,
            is_read=is_read,
            severity=severity
        )

        try:
            # Build query with joins for enriched data
            query = self.db.table(self.table).select(
                "*, products(sku), shipments(booking_number)",
                count="exact"
            )

            # Apply filters
            if is_read is not None:
                query = query.eq("is_read", is_read)
            if severity:
                query = query.eq("severity", severity.value)

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by created_at descending (newest first)
            query = query.order("created_at", desc=True)

            # Execute
            result = query.execute()

            alerts = [
                self._row_to_response(row) for row in result.data
            ]

            total = result.count or 0

            logger.info(
                "alerts_retrieved",
                count=len(alerts),
                total=total
            )

            return alerts, total

        except Exception as e:
            logger.error("get_alerts_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, alert_id: str) -> AlertResponse:
        """
        Get a single alert by ID.

        Args:
            alert_id: Alert UUID

        Returns:
            AlertResponse

        Raises:
            NotFoundError: If alert doesn't exist
        """
        logger.debug("getting_alert", alert_id=alert_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*, products(sku), shipments(booking_number)")
                .eq("id", alert_id)
                .limit(1)
                .execute()
            )

            if not result.data or len(result.data) == 0:
                raise NotFoundError("Alert", alert_id)

            return self._row_to_response(result.data[0])

        except NotFoundError:
            raise
        except Exception as e:
            logger.error("get_alert_failed", alert_id=alert_id, error=str(e))
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise NotFoundError("Alert", alert_id)
            raise DatabaseError("select", str(e))

    def get_unread(self) -> list[AlertResponse]:
        """
        Get all unread alerts.

        Returns:
            List of unread alerts
        """
        alerts, _ = self.get_all(page=1, page_size=1000, is_read=False)
        return alerts

    # ===================
    # WRITE OPERATIONS
    # ===================

    def create(self, data: AlertCreate, send_telegram: bool = True) -> AlertResponse:
        """
        Create a new alert.

        Args:
            data: Alert creation data
            send_telegram: Whether to send to Telegram

        Returns:
            Created AlertResponse
        """
        logger.info(
            "creating_alert",
            type=data.type,
            severity=data.severity,
            send_telegram=send_telegram
        )

        try:
            # Build alert data
            alert_data = {
                "type": data.type.value,
                "severity": data.severity.value,
                "title": data.title,
                "message": data.message,
                "product_id": data.product_id,
                "shipment_id": data.shipment_id,
                "is_read": False,
                "is_sent": False,
            }

            result = (
                self.db.table(self.table)
                .insert(alert_data)
                .execute()
            )

            alert_id = result.data[0]["id"]

            logger.info(
                "alert_created",
                alert_id=alert_id,
                type=data.type,
                severity=data.severity
            )

            # Get the created alert with enriched data
            alert = self.get_by_id(alert_id)

            # Send to Telegram if requested
            if send_telegram:
                try:
                    if send_alert_to_telegram(alert):
                        # Mark as sent
                        self.db.table(self.table).update({
                            "is_sent": True
                        }).eq("id", alert_id).execute()

                        alert.is_sent = True

                        logger.info("alert_sent_to_telegram", alert_id=alert_id)
                except TelegramError as e:
                    logger.warning(
                        "telegram_send_failed",
                        alert_id=alert_id,
                        error=str(e)
                    )
                    # Don't fail alert creation if Telegram fails

            return alert

        except Exception as e:
            logger.error("create_alert_failed", error=str(e))
            raise DatabaseError("insert", str(e))

    def mark_as_read(self, alert_id: str) -> AlertResponse:
        """
        Mark an alert as read.

        Args:
            alert_id: Alert UUID

        Returns:
            Updated AlertResponse

        Raises:
            NotFoundError: If alert doesn't exist
        """
        logger.info("marking_alert_read", alert_id=alert_id)

        # Check alert exists
        self.get_by_id(alert_id)

        try:
            self.db.table(self.table).update({
                "is_read": True
            }).eq("id", alert_id).execute()

            logger.info("alert_marked_read", alert_id=alert_id)

            return self.get_by_id(alert_id)

        except NotFoundError:
            raise
        except Exception as e:
            logger.error("mark_alert_read_failed", alert_id=alert_id, error=str(e))
            raise DatabaseError("update", str(e))

    def dismiss(self, alert_id: str) -> bool:
        """
        Dismiss an alert (mark as read and hide).

        Args:
            alert_id: Alert UUID

        Returns:
            True if dismissed

        Raises:
            NotFoundError: If alert doesn't exist
        """
        logger.info("dismissing_alert", alert_id=alert_id)

        # For now, dismiss just marks as read
        # In future, could add is_dismissed field
        self.mark_as_read(alert_id)

        return True

    # ===================
    # ALERT GENERATORS
    # ===================

    def generate_stockout_alerts(self) -> list[AlertResponse]:
        """
        Generate stockout alerts for products running low.

        Creates alerts for:
        - Products with < 14 days stock (CRITICAL)
        - Products with < 30 days stock (WARNING)

        Returns:
            List of created alerts
        """
        logger.info("generating_stockout_alerts")

        stockout_service = get_stockout_service()
        stockout_data = stockout_service.calculate_all()

        alerts_created = []

        for product in stockout_data.products:
            # Skip if no stockout concern
            if product.status not in ["HIGH_PRIORITY", "CONSIDER"]:
                continue

            # Determine severity based on days to stockout
            days_to_stockout = int(product.days_to_stockout) if product.days_to_stockout else 999

            if days_to_stockout < 14:
                severity = AlertSeverity.CRITICAL
            elif days_to_stockout < 30:
                severity = AlertSeverity.WARNING
            else:
                continue  # Not urgent enough

            # Build alert
            title = f"Stockout warning: {product.sku}"
            message = (
                f"Low stock alert for {product.sku}\n\n"
                f"Stockout in {days_to_stockout} days\n"
                f"Current: {product.warehouse_qty} m2\n"
                f"Daily demand: {product.avg_daily_sales} m2\n\n"
                f"Action: Order on next boat"
            )

            # Check if similar alert exists in last 7 days
            if self._alert_exists_recently(
                alert_type=AlertType.STOCKOUT_WARNING,
                product_id=product.product_id,
                days=7
            ):
                logger.debug(
                    "skipping_duplicate_alert",
                    product_id=product.product_id,
                    sku=product.sku
                )
                continue

            alert_data = AlertCreate(
                type=AlertType.STOCKOUT_WARNING,
                severity=severity,
                title=title,
                message=message,
                product_id=product.product_id,
            )

            alert = self.create(alert_data, send_telegram=True)
            alerts_created.append(alert)

        logger.info(
            "stockout_alerts_generated",
            count=len(alerts_created)
        )

        return alerts_created

    def generate_booking_deadline_alerts(self) -> list[AlertResponse]:
        """
        Generate alerts for upcoming boat booking deadlines.

        Creates alerts for boats with booking deadline in < 3 days (CRITICAL).

        Returns:
            List of created alerts
        """
        logger.info("generating_booking_deadline_alerts")

        boat_service = get_boat_schedule_service()

        # Get available boats
        boats, _ = boat_service.get_all(page=1, page_size=100)

        alerts_created = []
        today = date.today()

        for boat in boats:
            # Skip if no booking deadline
            if not boat.booking_deadline:
                continue

            # Calculate days until deadline
            days_until_deadline = (boat.booking_deadline - today).days

            # Only alert if < 3 days
            if days_until_deadline >= 3:
                continue

            # Only alert if boat hasn't departed yet
            if boat.departure_date < today:
                continue

            severity = AlertSeverity.CRITICAL

            title = f"Booking deadline: {boat.vessel_name}"
            message = (
                f"Booking deadline approaching for {boat.vessel_name}\n\n"
                f"Deadline: {boat.booking_deadline.strftime('%Y-%m-%d')} "
                f"({days_until_deadline} days)\n"
                f"Departure: {boat.departure_date.strftime('%Y-%m-%d')}\n"
                f"Route: {boat.origin_port} -> {boat.destination_port}\n\n"
                f"Action: Finalize orders now!"
            )

            # Check if similar alert exists in last 3 days
            if self._alert_exists_recently(
                alert_type=AlertType.ORDER_OPPORTUNITY,
                days=3
            ):
                logger.debug(
                    "skipping_duplicate_booking_alert",
                    boat_id=boat.id,
                    vessel_name=boat.vessel_name
                )
                continue

            alert_data = AlertCreate(
                type=AlertType.ORDER_OPPORTUNITY,
                severity=severity,
                title=title,
                message=message,
            )

            alert = self.create(alert_data, send_telegram=True)
            alerts_created.append(alert)

        logger.info(
            "booking_deadline_alerts_generated",
            count=len(alerts_created)
        )

        return alerts_created

    # ===================
    # UTILITY METHODS
    # ===================

    def _alert_exists_recently(
        self,
        alert_type: AlertType,
        product_id: Optional[str] = None,
        shipment_id: Optional[str] = None,
        days: int = 7
    ) -> bool:
        """
        Check if a similar alert exists in the last N days.

        Args:
            alert_type: Alert type to check
            product_id: Optional product filter
            shipment_id: Optional shipment filter
            days: Number of days to look back

        Returns:
            True if similar alert exists
        """
        try:
            cutoff = date.today() - timedelta(days=days)

            query = (
                self.db.table(self.table)
                .select("id", count="exact")
                .eq("type", alert_type.value)
                .gte("created_at", cutoff.isoformat())
            )

            if product_id:
                query = query.eq("product_id", product_id)
            if shipment_id:
                query = query.eq("shipment_id", shipment_id)

            result = query.execute()

            return (result.count or 0) > 0

        except Exception as e:
            logger.error("alert_exists_check_failed", error=str(e))
            return False

    def _row_to_response(self, row: dict) -> AlertResponse:
        """Convert database row to AlertResponse."""
        # Extract product SKU if joined
        product_sku = None
        if row.get("products") and isinstance(row["products"], dict):
            product_sku = row["products"].get("sku")

        # Extract shipment booking number if joined
        shipment_booking = None
        if row.get("shipments") and isinstance(row["shipments"], dict):
            shipment_booking = row["shipments"].get("booking_number")

        return AlertResponse(
            id=row["id"],
            type=row["type"],
            severity=row["severity"],
            title=row["title"],
            message=row["message"],
            product_id=row.get("product_id"),
            shipment_id=row.get("shipment_id"),
            is_read=row.get("is_read", False),
            is_sent=row.get("is_sent", False),
            created_at=row["created_at"],
            product_sku=product_sku,
            shipment_booking_number=shipment_booking,
        )


# Singleton instance
_alert_service: Optional[AlertService] = None


def get_alert_service() -> AlertService:
    """Get or create AlertService instance."""
    global _alert_service
    if _alert_service is None:
        _alert_service = AlertService()
    return _alert_service