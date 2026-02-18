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
        Get the most recent inventory for each product.

        Uses the inventory_current view which composes the latest value
        from each independent source table (warehouse, factory, transit).

        Returns:
            List of latest snapshots with product details
        """
        logger.info("getting_latest_inventory")

        try:
            # Query the view — one row per product, no dedup needed
            inv_result = (
                self.db.table("inventory_current")
                .select("*")
                .execute()
            )

            if not inv_result.data:
                return []

            # Get product details separately (view doesn't support FK joins)
            product_ids = [row["product_id"] for row in inv_result.data]
            products_result = (
                self.db.table("products")
                .select("id, sku, category, rotation")
                .in_("id", product_ids)
                .execute()
            )
            products_by_id = {p["id"]: p for p in (products_result.data or [])}

            # Convert to response objects
            snapshots = []
            for row in inv_result.data:
                pid = row["product_id"]
                product_data = products_by_id.get(pid, {})
                snapshots.append(InventorySnapshotWithProduct(
                    id=pid,  # Use product_id as id since view has no row id
                    product_id=pid,
                    warehouse_qty=float(row.get("warehouse_qty") or 0),
                    in_transit_qty=float(row.get("in_transit_qty") or 0),
                    factory_available_m2=float(row.get("factory_available_m2") or 0),
                    factory_lot_count=row.get("factory_lot_count") or 0,
                    factory_largest_lot_m2=float(row["factory_largest_lot_m2"]) if row.get("factory_largest_lot_m2") else None,
                    factory_largest_lot_code=row.get("factory_largest_lot_code"),
                    snapshot_date=row.get("snapshot_date"),
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
        Create/update a warehouse inventory snapshot.

        Upserts to warehouse_snapshots table.
        """
        logger.info(
            "creating_warehouse_snapshot",
            product_id=data.product_id,
            snapshot_date=str(data.snapshot_date)
        )

        try:
            result = (
                self.db.table("warehouse_snapshots")
                .upsert({
                    "product_id": data.product_id,
                    "warehouse_qty": data.warehouse_qty,
                    "snapshot_date": data.snapshot_date.isoformat(),
                }, on_conflict="product_id,snapshot_date")
                .execute()
            )

            row = result.data[0]
            snapshot = InventorySnapshotResponse(
                id=row["id"],
                product_id=row["product_id"],
                warehouse_qty=float(row["warehouse_qty"]),
                in_transit_qty=data.in_transit_qty,
                snapshot_date=row["snapshot_date"],
                created_at=row.get("created_at"),
            )

            logger.info(
                "warehouse_snapshot_created",
                snapshot_id=snapshot.id,
                product_id=snapshot.product_id
            )

            return snapshot

        except Exception as e:
            logger.error(
                "create_warehouse_snapshot_failed",
                product_id=data.product_id,
                error=str(e)
            )
            raise DatabaseError("insert", str(e))

    def delete_by_dates(self, snapshot_dates: list[date]) -> int:
        """
        Delete warehouse snapshots matching specific dates.

        Args:
            snapshot_dates: List of dates to delete

        Returns:
            Number of records deleted
        """
        if not snapshot_dates:
            return 0

        logger.info("deleting_warehouse_by_dates", dates=len(snapshot_dates))

        try:
            date_strings = [d.isoformat() for d in snapshot_dates]

            result = (
                self.db.table("warehouse_snapshots")
                .delete()
                .in_("snapshot_date", date_strings)
                .execute()
            )

            deleted = len(result.data) if result.data else 0

            logger.info("warehouse_deleted_by_dates", count=deleted)

            return deleted

        except Exception as e:
            logger.error("delete_warehouse_by_dates_failed", error=str(e))
            raise DatabaseError("delete", str(e))

    def bulk_create(
        self,
        snapshots: list[InventorySnapshotCreate]
    ) -> list[InventorySnapshotResponse]:
        """
        Bulk upsert warehouse inventory snapshots.

        Upserts to warehouse_snapshots table.
        """
        if not snapshots:
            return []

        logger.info("bulk_upserting_warehouse_snapshots", count=len(snapshots))

        try:
            rows = [
                {
                    "product_id": s.product_id,
                    "warehouse_qty": s.warehouse_qty,
                    "snapshot_date": s.snapshot_date.isoformat(),
                }
                for s in snapshots
            ]

            # Batch in chunks
            all_created = []
            chunk_size = 100
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                result = (
                    self.db.table("warehouse_snapshots")
                    .upsert(chunk, on_conflict="product_id,snapshot_date")
                    .execute()
                )
                all_created.extend(result.data)

            logger.info(
                "warehouse_snapshots_bulk_upserted",
                count=len(all_created)
            )

            return []  # Callers don't use the return value

        except Exception as e:
            logger.error(
                "bulk_upsert_warehouse_snapshots_failed",
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
        Update inventory data by routing to the correct source table.

        Routes warehouse_qty to warehouse_snapshots, factory fields to
        factory_snapshots, in_transit_qty to transit_snapshots.

        Note: snapshot_id is used to look up the product_id and date,
        then each source table is updated by (product_id, snapshot_date).
        """
        logger.info("updating_inventory_snapshot", snapshot_id=snapshot_id)

        # Get the existing record to find product_id and date
        existing = self.get_by_id(snapshot_id)

        try:
            pid = existing.product_id
            snap_date = (data.snapshot_date or existing.snapshot_date).isoformat()

            # Route warehouse_qty to warehouse_snapshots
            if data.warehouse_qty is not None:
                self.db.table("warehouse_snapshots").upsert({
                    "product_id": pid,
                    "snapshot_date": snap_date,
                    "warehouse_qty": data.warehouse_qty,
                }, on_conflict="product_id,snapshot_date").execute()

            # Route in_transit_qty to transit_snapshots
            if data.in_transit_qty is not None:
                self.db.table("transit_snapshots").upsert({
                    "product_id": pid,
                    "snapshot_date": snap_date,
                    "in_transit_qty": data.in_transit_qty,
                }, on_conflict="product_id,snapshot_date").execute()

            # Route factory fields to factory_snapshots
            factory_data = {}
            if data.factory_available_m2 is not None:
                factory_data["factory_available_m2"] = data.factory_available_m2
            if data.factory_largest_lot_m2 is not None:
                factory_data["factory_largest_lot_m2"] = data.factory_largest_lot_m2
            if data.factory_largest_lot_code is not None:
                factory_data["factory_largest_lot_code"] = data.factory_largest_lot_code
            if data.factory_lot_count is not None:
                factory_data["factory_lot_count"] = data.factory_lot_count

            if factory_data:
                self.db.table("factory_snapshots").upsert({
                    "product_id": pid,
                    "snapshot_date": snap_date,
                    **factory_data,
                }, on_conflict="product_id,snapshot_date").execute()

            # Return the updated view
            return existing  # Caller can re-fetch if needed

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
