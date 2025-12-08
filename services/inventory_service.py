"""
Inventory service for business logic operations.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
from datetime import date
import structlog

from config import get_supabase_client
from models.inventory import (
    InventorySnapshotCreate,
    InventorySnapshotUpdate,
    InventorySnapshotResponse,
    InventorySnapshotWithProduct,
)
from exceptions import (
    InventoryNotFoundError,
    DatabaseError,
    ValidationError,
)

logger = structlog.get_logger(__name__)


class InventoryService:
    """
    Inventory business logic.

    Handles CRUD operations for inventory snapshots.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "inventory_snapshots"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        product_id: Optional[str] = None,
    ) -> tuple[list[InventorySnapshotResponse], int]:
        """
        Get all inventory snapshots with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            product_id: Filter by product

        Returns:
            Tuple of (snapshots list, total count)
        """
        logger.info(
            "getting_inventory_snapshots",
            page=page,
            page_size=page_size,
            product_id=product_id
        )

        try:
            # Build query
            query = self.db.table(self.table).select("*", count="exact")

            # Apply filters
            if product_id:
                query = query.eq("product_id", product_id)

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by date descending
            query = query.order("snapshot_date", desc=True)

            # Execute
            result = query.execute()

            snapshots = [InventorySnapshotResponse(**row) for row in result.data]
            total = result.count or 0

            logger.info(
                "inventory_snapshots_retrieved",
                count=len(snapshots),
                total=total
            )

            return snapshots, total

        except Exception as e:
            logger.error("get_inventory_snapshots_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, snapshot_id: str) -> InventorySnapshotResponse:
        """
        Get a single inventory snapshot by ID.

        Args:
            snapshot_id: Snapshot UUID

        Returns:
            InventorySnapshotResponse

        Raises:
            InventoryNotFoundError: If snapshot doesn't exist
        """
        logger.debug("getting_inventory_snapshot", snapshot_id=snapshot_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", snapshot_id)
                .single()
                .execute()
            )

            if not result.data:
                raise InventoryNotFoundError(snapshot_id)

            return InventorySnapshotResponse(**result.data)

        except InventoryNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "get_inventory_snapshot_failed",
                snapshot_id=snapshot_id,
                error=str(e)
            )
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise InventoryNotFoundError(snapshot_id)
            raise DatabaseError("select", str(e))

    def get_history(
        self,
        product_id: str,
        limit: int = 30
    ) -> list[InventorySnapshotResponse]:
        """
        Get inventory history for a single product.

        Args:
            product_id: Product UUID
            limit: Maximum records to return

        Returns:
            List of snapshots ordered by date descending
        """
        logger.debug("getting_inventory_history", product_id=product_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("product_id", product_id)
                .order("snapshot_date", desc=True)
                .limit(limit)
                .execute()
            )

            return [InventorySnapshotResponse(**row) for row in result.data]

        except Exception as e:
            logger.error(
                "get_inventory_history_failed",
                product_id=product_id,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_latest(self) -> list[InventorySnapshotWithProduct]:
        """
        Get the most recent inventory snapshot for each product.

        Uses a window function approach: selects latest snapshot_date per product.

        Returns:
            List of latest snapshots with product details
        """
        logger.info("getting_latest_inventory")

        try:
            # Get latest snapshot for each product using distinct on
            # Supabase doesn't support DISTINCT ON directly, so we use a subquery approach
            # First, get all snapshots with product info
            result = (
                self.db.table(self.table)
                .select(
                    "*, products(sku, category, rotation)"
                )
                .order("product_id")
                .order("snapshot_date", desc=True)
                .execute()
            )

            if not result.data:
                return []

            # Filter to get only the latest per product
            latest_by_product = {}
            for row in result.data:
                pid = row["product_id"]
                if pid not in latest_by_product:
                    latest_by_product[pid] = row

            # Convert to response objects
            snapshots = []
            for row in latest_by_product.values():
                product_data = row.pop("products", {}) or {}
                snapshots.append(InventorySnapshotWithProduct(
                    **row,
                    sku=product_data.get("sku"),
                    category=product_data.get("category"),
                    rotation=product_data.get("rotation"),
                ))

            logger.info("latest_inventory_retrieved", count=len(snapshots))

            return snapshots

        except Exception as e:
            logger.error("get_latest_inventory_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # WRITE OPERATIONS
    # ===================

    def create(self, data: InventorySnapshotCreate) -> InventorySnapshotResponse:
        """
        Create a new inventory snapshot.

        Args:
            data: Snapshot creation data

        Returns:
            Created InventorySnapshotResponse
        """
        logger.info(
            "creating_inventory_snapshot",
            product_id=data.product_id,
            snapshot_date=str(data.snapshot_date)
        )

        try:
            # Prepare data for insert
            insert_data = {
                "product_id": data.product_id,
                "warehouse_qty": data.warehouse_qty,
                "in_transit_qty": data.in_transit_qty,
                "snapshot_date": data.snapshot_date.isoformat(),
                "notes": data.notes,
            }

            result = (
                self.db.table(self.table)
                .insert(insert_data)
                .execute()
            )

            snapshot = InventorySnapshotResponse(**result.data[0])

            logger.info(
                "inventory_snapshot_created",
                snapshot_id=snapshot.id,
                product_id=snapshot.product_id
            )

            return snapshot

        except Exception as e:
            logger.error(
                "create_inventory_snapshot_failed",
                product_id=data.product_id,
                error=str(e)
            )
            raise DatabaseError("insert", str(e))

    def bulk_create(
        self,
        snapshots: list[InventorySnapshotCreate]
    ) -> list[InventorySnapshotResponse]:
        """
        Create multiple inventory snapshots at once.

        Used by the upload endpoint after parsing Excel.

        Args:
            snapshots: List of snapshot creation data

        Returns:
            List of created InventorySnapshotResponse
        """
        if not snapshots:
            return []

        logger.info("bulk_creating_inventory_snapshots", count=len(snapshots))

        try:
            # Prepare data for bulk insert
            insert_data = [
                {
                    "product_id": s.product_id,
                    "warehouse_qty": s.warehouse_qty,
                    "in_transit_qty": s.in_transit_qty,
                    "snapshot_date": s.snapshot_date.isoformat(),
                    "notes": s.notes,
                }
                for s in snapshots
            ]

            result = (
                self.db.table(self.table)
                .insert(insert_data)
                .execute()
            )

            created = [InventorySnapshotResponse(**row) for row in result.data]

            logger.info(
                "inventory_snapshots_bulk_created",
                count=len(created)
            )

            return created

        except Exception as e:
            logger.error(
                "bulk_create_inventory_snapshots_failed",
                count=len(snapshots),
                error=str(e)
            )
            raise DatabaseError("insert", str(e))

    def update(
        self,
        snapshot_id: str,
        data: InventorySnapshotUpdate
    ) -> InventorySnapshotResponse:
        """
        Update an existing inventory snapshot.

        Args:
            snapshot_id: Snapshot UUID
            data: Fields to update

        Returns:
            Updated InventorySnapshotResponse

        Raises:
            InventoryNotFoundError: If snapshot doesn't exist
        """
        logger.info("updating_inventory_snapshot", snapshot_id=snapshot_id)

        # Check snapshot exists
        self.get_by_id(snapshot_id)

        try:
            # Build update dict with only provided fields
            update_data = {}
            if data.warehouse_qty is not None:
                update_data["warehouse_qty"] = data.warehouse_qty
            if data.in_transit_qty is not None:
                update_data["in_transit_qty"] = data.in_transit_qty
            if data.snapshot_date is not None:
                update_data["snapshot_date"] = data.snapshot_date.isoformat()
            if data.notes is not None:
                update_data["notes"] = data.notes

            if not update_data:
                # Nothing to update, return existing
                return self.get_by_id(snapshot_id)

            result = (
                self.db.table(self.table)
                .update(update_data)
                .eq("id", snapshot_id)
                .execute()
            )

            snapshot = InventorySnapshotResponse(**result.data[0])

            logger.info(
                "inventory_snapshot_updated",
                snapshot_id=snapshot_id,
                fields=list(update_data.keys())
            )

            return snapshot

        except Exception as e:
            logger.error(
                "update_inventory_snapshot_failed",
                snapshot_id=snapshot_id,
                error=str(e)
            )
            raise DatabaseError("update", str(e))

    def delete(self, snapshot_id: str) -> bool:
        """
        Delete an inventory snapshot.

        Args:
            snapshot_id: Snapshot UUID

        Returns:
            True if deleted

        Raises:
            InventoryNotFoundError: If snapshot doesn't exist
        """
        logger.info("deleting_inventory_snapshot", snapshot_id=snapshot_id)

        # Check snapshot exists
        self.get_by_id(snapshot_id)

        try:
            self.db.table(self.table).delete().eq("id", snapshot_id).execute()

            logger.info("inventory_snapshot_deleted", snapshot_id=snapshot_id)

            return True

        except Exception as e:
            logger.error(
                "delete_inventory_snapshot_failed",
                snapshot_id=snapshot_id,
                error=str(e)
            )
            raise DatabaseError("delete", str(e))

    # ===================
    # UTILITY METHODS
    # ===================

    def count(self, product_id: Optional[str] = None) -> int:
        """Count total inventory snapshots."""
        try:
            query = self.db.table(self.table).select("id", count="exact")
            if product_id:
                query = query.eq("product_id", product_id)
            result = query.execute()
            return result.count or 0
        except Exception as e:
            logger.error("count_inventory_snapshots_failed", error=str(e))
            raise DatabaseError("count", str(e))


# Singleton instance for convenience
_inventory_service: Optional[InventoryService] = None


def get_inventory_service() -> InventoryService:
    """Get or create InventoryService instance."""
    global _inventory_service
    if _inventory_service is None:
        _inventory_service = InventoryService()
    return _inventory_service
