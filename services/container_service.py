"""
Container Service - CRUD operations for containers and container items.

Handles containers and their items within shipments.
"""

import structlog
from typing import Optional, List, Tuple
from decimal import Decimal

from config import get_supabase_client
from models.container import (
    ContainerCreate,
    ContainerUpdate,
    ContainerResponse,
    ContainerItemCreate,
    ContainerItemUpdate,
    ContainerItemResponse,
    ContainerWithItemsResponse
)
from exceptions import (
    ContainerNotFoundError,
    ContainerItemNotFoundError,
    ShipmentNotFoundError,
    ProductNotFoundError,
    DatabaseError
)

logger = structlog.get_logger(__name__)


# Container limits (soft warnings)
MAX_WEIGHT_KG = Decimal("28000")
MAX_PALLETS = 14
MAX_M2 = Decimal("1881")


class ContainerService:
    """
    Container business logic.

    Handles CRUD operations for containers and their items.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.container_table = "containers"
        self.item_table = "container_items"

    # ===================
    # CONTAINER READ OPERATIONS
    # ===================

    def get_by_id(self, container_id: str) -> ContainerResponse:
        """
        Get container by ID.

        Args:
            container_id: Container UUID

        Returns:
            ContainerResponse

        Raises:
            ContainerNotFoundError: If container not found
        """
        logger.debug("getting_container", container_id=container_id)

        try:
            result = self.db.table(self.container_table).select("*").eq(
                "id", container_id
            ).execute()

            if not result.data:
                raise ContainerNotFoundError(container_id)

            return ContainerResponse(**result.data[0])

        except ContainerNotFoundError:
            raise
        except Exception as e:
            logger.error("get_container_failed", container_id=container_id, error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_shipment(self, shipment_id: str) -> List[ContainerResponse]:
        """
        Get all containers for a shipment.

        Args:
            shipment_id: Shipment UUID

        Returns:
            List of ContainerResponse
        """
        logger.info("getting_containers_by_shipment", shipment_id=shipment_id)

        try:
            result = self.db.table(self.container_table).select("*").eq(
                "shipment_id", shipment_id
            ).order("created_at", desc=False).execute()

            containers = [ContainerResponse(**container) for container in result.data]
            logger.info("containers_retrieved", shipment_id=shipment_id, count=len(containers))

            return containers

        except Exception as e:
            logger.error("get_containers_by_shipment_failed", shipment_id=shipment_id, error=str(e))
            raise DatabaseError("select", str(e))

    def get_with_items(self, container_id: str) -> ContainerWithItemsResponse:
        """
        Get container with all its items.

        Args:
            container_id: Container UUID

        Returns:
            ContainerWithItemsResponse

        Raises:
            ContainerNotFoundError: If container not found
        """
        logger.debug("getting_container_with_items", container_id=container_id)

        # Get container
        container = self.get_by_id(container_id)

        # Get items
        items = self.get_items(container_id)

        # Combine
        return ContainerWithItemsResponse(
            **container.model_dump(),
            items=items
        )

    # ===================
    # CONTAINER CREATE/UPDATE/DELETE
    # ===================

    def create(self, data: ContainerCreate) -> ContainerResponse:
        """
        Create a new container.

        Args:
            data: ContainerCreate schema

        Returns:
            ContainerResponse with the created container

        Raises:
            ShipmentNotFoundError: If shipment doesn't exist
            DatabaseError: If creation fails
        """
        logger.info("creating_container", shipment_id=data.shipment_id)

        # Validate shipment exists
        if not self._shipment_exists(data.shipment_id):
            raise ShipmentNotFoundError(data.shipment_id)

        try:
            result = self.db.table(self.container_table).insert({
                "shipment_id": data.shipment_id,
                "container_number": data.container_number,
                "seal_number": data.seal_number,
                "trucking_company_id": data.trucking_company_id,
                "total_pallets": data.total_pallets,
                "total_weight_kg": float(data.total_weight_kg) if data.total_weight_kg else None,
                "total_m2": float(data.total_m2) if data.total_m2 else None,
                "fill_percentage": float(data.fill_percentage) if data.fill_percentage else None,
                "unload_start": data.unload_start.isoformat() if data.unload_start else None,
                "unload_end": data.unload_end.isoformat() if data.unload_end else None,
            }).execute()

            container_id = result.data[0]["id"]
            logger.info("container_created", container_id=container_id, shipment_id=data.shipment_id)

            return self.get_by_id(container_id)

        except (ShipmentNotFoundError,):
            raise
        except Exception as e:
            logger.error("create_container_failed", error=str(e))
            raise DatabaseError("insert", str(e))

    def update(self, container_id: str, data: ContainerUpdate) -> ContainerResponse:
        """
        Update container.

        Args:
            container_id: Container UUID
            data: ContainerUpdate schema

        Returns:
            ContainerResponse with updated container

        Raises:
            ContainerNotFoundError: If container not found
        """
        logger.info("updating_container", container_id=container_id)

        # Verify container exists
        _ = self.get_by_id(container_id)

        # Build update dict (only include provided fields)
        update_data = {}
        if data.container_number is not None:
            update_data["container_number"] = data.container_number
        if data.seal_number is not None:
            update_data["seal_number"] = data.seal_number
        if data.trucking_company_id is not None:
            update_data["trucking_company_id"] = data.trucking_company_id
        if data.total_pallets is not None:
            update_data["total_pallets"] = data.total_pallets
        if data.total_weight_kg is not None:
            update_data["total_weight_kg"] = float(data.total_weight_kg)
        if data.total_m2 is not None:
            update_data["total_m2"] = float(data.total_m2)
        if data.fill_percentage is not None:
            update_data["fill_percentage"] = float(data.fill_percentage)
        if data.unload_start is not None:
            update_data["unload_start"] = data.unload_start.isoformat()
        if data.unload_end is not None:
            update_data["unload_end"] = data.unload_end.isoformat()

        try:
            self.db.table(self.container_table).update(update_data).eq(
                "id", container_id
            ).execute()

            logger.info("container_updated", container_id=container_id)
            return self.get_by_id(container_id)

        except ContainerNotFoundError:
            raise
        except Exception as e:
            logger.error("update_container_failed", container_id=container_id, error=str(e))
            raise DatabaseError("update", str(e))

    def delete(self, container_id: str) -> bool:
        """
        Delete container (CASCADE deletes items).

        Args:
            container_id: Container UUID

        Returns:
            True if deleted successfully

        Raises:
            ContainerNotFoundError: If container not found
        """
        logger.info("deleting_container", container_id=container_id)

        # Verify container exists
        _ = self.get_by_id(container_id)

        try:
            self.db.table(self.container_table).delete().eq(
                "id", container_id
            ).execute()

            logger.info("container_deleted", container_id=container_id)
            return True

        except ContainerNotFoundError:
            raise
        except Exception as e:
            logger.error("delete_container_failed", container_id=container_id, error=str(e))
            raise DatabaseError("delete", str(e))

    # ===================
    # CONTAINER ITEM READ OPERATIONS
    # ===================

    def get_item(self, item_id: str) -> ContainerItemResponse:
        """
        Get container item by ID.

        Args:
            item_id: Container item UUID

        Returns:
            ContainerItemResponse

        Raises:
            ContainerItemNotFoundError: If item not found
        """
        logger.debug("getting_container_item", item_id=item_id)

        try:
            result = self.db.table(self.item_table).select("*").eq(
                "id", item_id
            ).execute()

            if not result.data:
                raise ContainerItemNotFoundError(item_id)

            return ContainerItemResponse(**result.data[0])

        except ContainerItemNotFoundError:
            raise
        except Exception as e:
            logger.error("get_container_item_failed", item_id=item_id, error=str(e))
            raise DatabaseError("select", str(e))

    def get_items(self, container_id: str) -> List[ContainerItemResponse]:
        """
        Get all items for a container.

        Args:
            container_id: Container UUID

        Returns:
            List of ContainerItemResponse
        """
        logger.debug("getting_container_items", container_id=container_id)

        try:
            result = self.db.table(self.item_table).select("*").eq(
                "container_id", container_id
            ).order("created_at", desc=False).execute()

            items = [ContainerItemResponse(**item) for item in result.data]
            logger.debug("container_items_retrieved", container_id=container_id, count=len(items))

            return items

        except Exception as e:
            logger.error("get_container_items_failed", container_id=container_id, error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # CONTAINER ITEM CREATE/UPDATE/DELETE
    # ===================

    def add_item(self, container_id: str, data: ContainerItemCreate) -> ContainerItemResponse:
        """
        Add item to container.

        Args:
            container_id: Container UUID
            data: ContainerItemCreate schema

        Returns:
            ContainerItemResponse with the created item

        Raises:
            ContainerNotFoundError: If container doesn't exist
            ProductNotFoundError: If product doesn't exist
        """
        logger.info("adding_item_to_container", container_id=container_id, product_id=data.product_id)

        # Validate container exists
        _ = self.get_by_id(container_id)

        # Validate product exists
        if not self._product_exists(data.product_id):
            raise ProductNotFoundError(data.product_id)

        try:
            result = self.db.table(self.item_table).insert({
                "container_id": container_id,
                "product_id": data.product_id,
                "quantity": float(data.quantity),
                "pallets": data.pallets,
                "weight_kg": float(data.weight_kg) if data.weight_kg else None,
            }).execute()

            item_id = result.data[0]["id"]
            logger.info("container_item_added", item_id=item_id, container_id=container_id)

            return self.get_item(item_id)

        except (ContainerNotFoundError, ProductNotFoundError):
            raise
        except Exception as e:
            logger.error("add_item_failed", container_id=container_id, error=str(e))
            raise DatabaseError("insert", str(e))

    def update_item(self, item_id: str, data: ContainerItemUpdate) -> ContainerItemResponse:
        """
        Update container item.

        Args:
            item_id: Container item UUID
            data: ContainerItemUpdate schema

        Returns:
            ContainerItemResponse with updated item

        Raises:
            ContainerItemNotFoundError: If item not found
        """
        logger.info("updating_container_item", item_id=item_id)

        # Verify item exists
        _ = self.get_item(item_id)

        # Build update dict
        update_data = {}
        if data.quantity is not None:
            update_data["quantity"] = float(data.quantity)
        if data.pallets is not None:
            update_data["pallets"] = data.pallets
        if data.weight_kg is not None:
            update_data["weight_kg"] = float(data.weight_kg)

        try:
            self.db.table(self.item_table).update(update_data).eq(
                "id", item_id
            ).execute()

            logger.info("container_item_updated", item_id=item_id)
            return self.get_item(item_id)

        except ContainerItemNotFoundError:
            raise
        except Exception as e:
            logger.error("update_item_failed", item_id=item_id, error=str(e))
            raise DatabaseError("update", str(e))

    def delete_item(self, item_id: str) -> bool:
        """
        Delete container item.

        Args:
            item_id: Container item UUID

        Returns:
            True if deleted successfully

        Raises:
            ContainerItemNotFoundError: If item not found
        """
        logger.info("deleting_container_item", item_id=item_id)

        # Verify item exists
        _ = self.get_item(item_id)

        try:
            self.db.table(self.item_table).delete().eq(
                "id", item_id
            ).execute()

            logger.info("container_item_deleted", item_id=item_id)
            return True

        except ContainerItemNotFoundError:
            raise
        except Exception as e:
            logger.error("delete_item_failed", item_id=item_id, error=str(e))
            raise DatabaseError("delete", str(e))

    # ===================
    # UTILITY METHODS
    # ===================

    def recalculate_totals(self, container_id: str) -> ContainerResponse:
        """
        Recalculate container totals from items.

        Args:
            container_id: Container UUID

        Returns:
            ContainerResponse with updated totals
        """
        logger.info("recalculating_container_totals", container_id=container_id)

        # Get all items
        items = self.get_items(container_id)

        # Calculate totals
        total_m2 = sum(item.quantity for item in items)
        total_pallets = sum(item.pallets or 0 for item in items)
        total_weight_kg = sum(item.weight_kg or Decimal("0") for item in items)

        # Update container
        update = ContainerUpdate(
            total_m2=total_m2,
            total_pallets=total_pallets,
            total_weight_kg=total_weight_kg
        )

        logger.info(
            "totals_calculated",
            container_id=container_id,
            total_m2=float(total_m2),
            total_pallets=total_pallets,
            total_weight_kg=float(total_weight_kg)
        )

        return self.update(container_id, update)

    def validate_limits(self, container_id: str) -> Tuple[bool, List[str]]:
        """
        Validate container against soft limits.

        Args:
            container_id: Container UUID

        Returns:
            Tuple of (is_valid, list_of_warnings)
        """
        container = self.get_by_id(container_id)
        warnings = []

        if container.total_weight_kg and container.total_weight_kg > MAX_WEIGHT_KG:
            warnings.append(f"Weight exceeds limit: {container.total_weight_kg}kg > {MAX_WEIGHT_KG}kg")

        if container.total_pallets and container.total_pallets > MAX_PALLETS:
            warnings.append(f"Pallets exceed limit: {container.total_pallets} > {MAX_PALLETS}")

        if container.total_m2 and container.total_m2 > MAX_M2:
            warnings.append(f"Area exceeds limit: {container.total_m2}m² > {MAX_M2}m²")

        is_valid = len(warnings) == 0

        if not is_valid:
            logger.warning("container_exceeds_limits", container_id=container_id, warnings=warnings)

        return is_valid, warnings

    def _shipment_exists(self, shipment_id: str) -> bool:
        """Check if shipment exists."""
        try:
            result = self.db.table("shipments").select("id").eq("id", shipment_id).execute()
            return len(result.data) > 0
        except Exception:
            return False

    def _product_exists(self, product_id: str) -> bool:
        """Check if product exists."""
        try:
            result = self.db.table("products").select("id").eq("id", product_id).execute()
            return len(result.data) > 0
        except Exception:
            return False


# Singleton instance
_container_service: Optional[ContainerService] = None


def get_container_service() -> ContainerService:
    """Get the singleton container service instance."""
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service