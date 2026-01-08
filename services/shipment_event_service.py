"""
Shipment Event Service - CRUD operations for shipment events.

Handles audit trail of shipment status changes.
Events are read-only from API - created internally by shipment_service.
"""

import structlog
from typing import Optional
from datetime import datetime

from config import get_supabase_client
from models.shipment_event import (
    ShipmentEventCreate,
    ShipmentEventResponse,
    ShipmentEventListResponse
)
from exceptions import ShipmentEventNotFoundError

logger = structlog.get_logger(__name__)


class ShipmentEventService:
    """Service for shipment event operations."""

    def __init__(self):
        """Initialize the shipment event service."""
        self.db = get_supabase_client()
        self.table = "shipment_events"

    def create(self, data: ShipmentEventCreate) -> ShipmentEventResponse:
        """
        Create a new shipment event.

        This is used internally by shipment_service when:
        1. A shipment is created (initial AT_FACTORY event)
        2. A shipment status is updated

        Args:
            data: ShipmentEventCreate schema

        Returns:
            ShipmentEventResponse with the created event

        Raises:
            Exception: If database operation fails
        """
        logger.info(
            "creating_shipment_event",
            shipment_id=data.shipment_id,
            status=data.status.value
        )

        try:
            result = self.db.table(self.table).insert({
                "shipment_id": data.shipment_id,
                "status": data.status.value,
                "occurred_at": data.occurred_at.isoformat(),
                "notes": data.notes
            }).execute()

            event = result.data[0]
            logger.info(
                "shipment_event_created",
                event_id=event["id"],
                shipment_id=data.shipment_id,
                status=data.status.value
            )

            return ShipmentEventResponse(**event)

        except Exception as e:
            logger.error(
                "shipment_event_creation_failed",
                shipment_id=data.shipment_id,
                error=str(e)
            )
            raise

    def get_by_shipment(self, shipment_id: str) -> ShipmentEventListResponse:
        """
        Get all events for a shipment, ordered by occurred_at DESC.

        Args:
            shipment_id: Shipment UUID

        Returns:
            ShipmentEventListResponse with all events for the shipment

        Raises:
            Exception: If database operation fails
        """
        logger.info("getting_shipment_events", shipment_id=shipment_id)

        try:
            result = self.db.table(self.table).select("*").eq(
                "shipment_id", shipment_id
            ).order("occurred_at", desc=True).execute()

            events = [ShipmentEventResponse(**event) for event in result.data]

            logger.info(
                "shipment_events_retrieved",
                shipment_id=shipment_id,
                count=len(events)
            )

            return ShipmentEventListResponse(
                data=events,
                total=len(events)
            )

        except Exception as e:
            logger.error(
                "shipment_events_retrieval_failed",
                shipment_id=shipment_id,
                error=str(e)
            )
            raise

    def get_latest(self, shipment_id: str) -> ShipmentEventResponse:
        """
        Get the most recent event for a shipment.

        Args:
            shipment_id: Shipment UUID

        Returns:
            ShipmentEventResponse with the latest event

        Raises:
            ShipmentEventNotFoundError: If no events exist for this shipment
        """
        logger.info("getting_latest_shipment_event", shipment_id=shipment_id)

        try:
            result = self.db.table(self.table).select("*").eq(
                "shipment_id", shipment_id
            ).order("occurred_at", desc=True).limit(1).execute()

            if not result.data:
                logger.warning(
                    "no_events_found",
                    shipment_id=shipment_id
                )
                raise ShipmentEventNotFoundError(shipment_id)

            event = result.data[0]
            logger.info(
                "latest_shipment_event_retrieved",
                shipment_id=shipment_id,
                event_id=event["id"],
                status=event["status"]
            )

            return ShipmentEventResponse(**event)

        except ShipmentEventNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "latest_shipment_event_retrieval_failed",
                shipment_id=shipment_id,
                error=str(e)
            )
            raise


# Singleton instance
_shipment_event_service: Optional[ShipmentEventService] = None


def get_shipment_event_service() -> ShipmentEventService:
    """Get the singleton shipment event service instance."""
    global _shipment_event_service
    if _shipment_event_service is None:
        _shipment_event_service = ShipmentEventService()
    return _shipment_event_service