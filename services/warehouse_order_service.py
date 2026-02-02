"""
Warehouse Order Service — Tracks Order Builder exports.

Warehouse orders represent SIESA stock selected for shipment on a specific boat.
Used to prevent double-ordering and calculate pending coverage.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from datetime import datetime, date
from decimal import Decimal
from math import ceil
from typing import Optional

import structlog

from config import get_supabase_client
from config.shipping import M2_PER_PALLET, CONTAINER_MAX_PALLETS, WAREHOUSE_BUFFER_DAYS
from exceptions import DatabaseError, WarehouseOrderNotFoundError
from models.warehouse_order import (
    WarehouseOrderCreate,
    WarehouseOrderUpdate,
    WarehouseOrderStatusUpdate,
    WarehouseOrderResponse,
    WarehouseOrderWithItemsResponse,
    WarehouseOrderItemResponse,
    WarehouseOrderStatus,
    PendingOrdersForBoat,
    PendingOrdersBySku,
)

logger = structlog.get_logger(__name__)


class WarehouseOrderService:
    """
    Warehouse order business logic.

    Handles CRUD operations for warehouse orders and items.
    Tracks Order Builder exports to prevent double-ordering.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "warehouse_orders"
        self.items_table = "warehouse_order_items"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        status: Optional[WarehouseOrderStatus] = None,
        boat_id: Optional[str] = None,
    ) -> tuple[list[WarehouseOrderResponse], int]:
        """
        Get all warehouse orders with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Filter by status
            boat_id: Filter by boat

        Returns:
            Tuple of (orders list, total count)
        """
        logger.info(
            "getting_warehouse_orders",
            page=page,
            page_size=page_size,
            status=status,
            boat_id=boat_id,
        )

        try:
            # Build query
            query = self.db.table(self.table).select("*", count="exact")

            # Apply filters
            if status:
                query = query.eq("status", status.value)
            if boat_id:
                query = query.eq("boat_id", boat_id)

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by created_at descending
            query = query.order("created_at", desc=True)

            result = query.execute()

            orders = []
            for row in result.data:
                # Get item count
                items_result = (
                    self.db.table(self.items_table)
                    .select("id", count="exact")
                    .eq("warehouse_order_id", row["id"])
                    .execute()
                )
                item_count = items_result.count or 0

                orders.append(self._row_to_response(row, item_count=item_count))

            total = result.count or 0

            logger.info(
                "warehouse_orders_retrieved",
                count=len(orders),
                total=total,
            )

            return orders, total

        except Exception as e:
            logger.error("get_warehouse_orders_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(
        self, order_id: str, include_items: bool = True
    ) -> WarehouseOrderWithItemsResponse:
        """
        Get a single warehouse order by ID.

        Args:
            order_id: Order UUID
            include_items: Whether to include line items

        Returns:
            WarehouseOrderWithItemsResponse

        Raises:
            WarehouseOrderNotFoundError: If order doesn't exist
        """
        logger.debug("getting_warehouse_order", order_id=order_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", order_id)
                .single()
                .execute()
            )

            if not result.data:
                raise WarehouseOrderNotFoundError(order_id)

            row = result.data

            # Get items
            items = []
            if include_items:
                items_result = (
                    self.db.table(self.items_table)
                    .select("*")
                    .eq("warehouse_order_id", order_id)
                    .order("created_at")
                    .execute()
                )

                for item in items_result.data:
                    items.append(self._item_row_to_response(item))

            return WarehouseOrderWithItemsResponse(
                **self._row_to_response(row, item_count=len(items)).model_dump(),
                items=items,
            )

        except WarehouseOrderNotFoundError:
            raise
        except Exception as e:
            logger.error("get_warehouse_order_failed", order_id=order_id, error=str(e))
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise WarehouseOrderNotFoundError(order_id)
            raise DatabaseError("select", str(e))

    def get_by_boat_id(
        self,
        boat_id: str,
        status: Optional[WarehouseOrderStatus] = None,
    ) -> list[WarehouseOrderResponse]:
        """
        Get all warehouse orders for a specific boat.

        Args:
            boat_id: Boat schedule UUID
            status: Filter by status (optional)

        Returns:
            List of orders for this boat
        """
        logger.debug("getting_warehouse_orders_by_boat", boat_id=boat_id, status=status)

        try:
            query = (
                self.db.table(self.table)
                .select("*")
                .eq("boat_id", boat_id)
                .order("created_at", desc=True)
            )

            if status:
                query = query.eq("status", status.value)

            result = query.execute()

            orders = []
            for row in result.data:
                items_result = (
                    self.db.table(self.items_table)
                    .select("id", count="exact")
                    .eq("warehouse_order_id", row["id"])
                    .execute()
                )
                item_count = items_result.count or 0
                orders.append(self._row_to_response(row, item_count=item_count))

            return orders

        except Exception as e:
            logger.error(
                "get_warehouse_orders_by_boat_failed", boat_id=boat_id, error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_pending_for_boat(self, boat_id: str) -> Optional[PendingOrdersForBoat]:
        """
        Get aggregated pending order info for a specific boat.

        Args:
            boat_id: Boat schedule UUID

        Returns:
            Aggregated pending order info or None if no pending orders
        """
        logger.debug("getting_pending_for_boat", boat_id=boat_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("boat_id", boat_id)
                .eq("status", WarehouseOrderStatus.PENDING.value)
                .execute()
            )

            if not result.data:
                return None

            total_pallets = sum(row.get("total_pallets", 0) or 0 for row in result.data)
            total_m2 = sum(
                Decimal(str(row.get("total_m2", 0) or 0)) for row in result.data
            )
            boat_departure = None
            for row in result.data:
                if row.get("boat_departure_date"):
                    boat_departure = row["boat_departure_date"]
                    break

            return PendingOrdersForBoat(
                boat_id=boat_id,
                boat_departure_date=boat_departure or date.today(),
                total_pallets=total_pallets,
                total_m2=total_m2,
                order_count=len(result.data),
            )

        except Exception as e:
            logger.error(
                "get_pending_for_boat_failed", boat_id=boat_id, error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_pending_by_sku(self) -> list[PendingOrdersBySku]:
        """
        Get pending order quantities grouped by SKU.

        Used in Order Builder to calculate coverage:
        coverage_gap = adjusted_need - warehouse_m2 - in_transit_m2 - pending_order_m2

        Returns:
            List of pending order quantities by SKU
        """
        logger.debug("getting_pending_by_sku")

        try:
            # Get all pending orders
            orders_result = (
                self.db.table(self.table)
                .select("id, boat_id")
                .eq("status", WarehouseOrderStatus.PENDING.value)
                .execute()
            )

            if not orders_result.data:
                return []

            order_ids = [row["id"] for row in orders_result.data]
            order_boat_map = {row["id"]: row["boat_id"] for row in orders_result.data}

            # Get all items for pending orders
            items_result = (
                self.db.table(self.items_table)
                .select("*")
                .in_("warehouse_order_id", order_ids)
                .execute()
            )

            # Group by SKU
            sku_data: dict[str, dict] = {}
            for item in items_result.data:
                sku = item["sku"]
                if sku not in sku_data:
                    sku_data[sku] = {
                        "product_id": item.get("product_id"),
                        "total_pallets": 0,
                        "total_m2": Decimal("0"),
                        "order_count": 0,
                        "boat_ids": set(),
                    }

                sku_data[sku]["total_pallets"] += item.get("pallets", 0) or 0
                sku_data[sku]["total_m2"] += Decimal(str(item.get("m2", 0) or 0))
                sku_data[sku]["order_count"] += 1
                boat_id = order_boat_map.get(item["warehouse_order_id"])
                if boat_id:
                    sku_data[sku]["boat_ids"].add(boat_id)

            # Convert to response model
            result = []
            for sku, data in sku_data.items():
                result.append(
                    PendingOrdersBySku(
                        sku=sku,
                        product_id=data["product_id"],
                        total_pallets=data["total_pallets"],
                        total_m2=data["total_m2"],
                        order_count=data["order_count"],
                        boat_ids=list(data["boat_ids"]),
                    )
                )

            logger.info("pending_by_sku_retrieved", sku_count=len(result))
            return result

        except Exception as e:
            logger.error("get_pending_by_sku_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_pending_by_sku_dict(self) -> dict[str, dict]:
        """
        Get all pending order m² grouped by SKU as a dictionary.

        Used in Order Builder coverage calculation to prevent double-ordering.
        Includes boat info and estimated warehouse dates for timing checks.

        Returns:
            {
                "NOGAL CAFE BTE": {
                    "total_m2": Decimal("2822.4"),
                    "total_pallets": 21,
                    "boat_name": "AIAS",
                    "boat_id": "uuid",
                    "estimated_warehouse_date": "2026-04-15",
                    "orders": [
                        {
                            "order_id": "uuid",
                            "m2": Decimal("2822.4"),
                            "pallets": 21,
                            "boat_name": "AIAS",
                            "estimated_warehouse_date": "2026-04-15"
                        }
                    ]
                },
                ...
            }
        """
        logger.debug("getting_pending_by_sku_dict")

        try:
            # Get all pending orders with full details
            orders_result = (
                self.db.table(self.table)
                .select("id, boat_id, boat_name, estimated_warehouse_date")
                .eq("status", WarehouseOrderStatus.PENDING.value)
                .execute()
            )

            if not orders_result.data:
                return {}

            order_ids = [row["id"] for row in orders_result.data]
            order_info_map = {
                row["id"]: {
                    "boat_id": row["boat_id"],
                    "boat_name": row.get("boat_name"),
                    "estimated_warehouse_date": row.get("estimated_warehouse_date"),
                }
                for row in orders_result.data
            }

            # Get all items for pending orders
            items_result = (
                self.db.table(self.items_table)
                .select("*")
                .in_("warehouse_order_id", order_ids)
                .execute()
            )

            # Group by SKU with full details
            by_sku: dict[str, dict] = {}
            for item in items_result.data:
                sku = item["sku"]
                order_id = item["warehouse_order_id"]
                order_info = order_info_map.get(order_id, {})

                if sku not in by_sku:
                    by_sku[sku] = {
                        "total_m2": Decimal("0"),
                        "total_pallets": 0,
                        "boat_name": order_info.get("boat_name"),
                        "boat_id": order_info.get("boat_id"),
                        "estimated_warehouse_date": order_info.get("estimated_warehouse_date"),
                        "orders": [],
                    }

                item_m2 = Decimal(str(item.get("m2", 0) or 0))
                item_pallets = item.get("pallets", 0) or 0

                by_sku[sku]["total_m2"] += item_m2
                by_sku[sku]["total_pallets"] += item_pallets
                by_sku[sku]["orders"].append({
                    "order_id": order_id,
                    "m2": item_m2,
                    "pallets": item_pallets,
                    "boat_name": order_info.get("boat_name"),
                    "boat_id": order_info.get("boat_id"),
                    "estimated_warehouse_date": order_info.get("estimated_warehouse_date"),
                })

            logger.info("pending_by_sku_dict_retrieved", sku_count=len(by_sku))
            return by_sku

        except Exception as e:
            logger.error("get_pending_by_sku_dict_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # WRITE OPERATIONS
    # ===================

    def create(self, data: WarehouseOrderCreate) -> WarehouseOrderWithItemsResponse:
        """
        Create a new warehouse order with items.

        Re-export logic: If a PENDING order exists for the same boat,
        cancel it before creating the new one.

        Args:
            data: Order creation data with items

        Returns:
            Created WarehouseOrderWithItemsResponse
        """
        logger.info(
            "creating_warehouse_order",
            boat_id=data.boat_id,
            item_count=len(data.items),
        )

        try:
            # Re-export logic: Cancel existing pending order for this boat
            existing_pending = self.get_by_boat_id(
                data.boat_id, status=WarehouseOrderStatus.PENDING
            )
            for existing in existing_pending:
                logger.info(
                    "cancelling_previous_pending_order",
                    order_id=existing.id,
                    boat_id=data.boat_id,
                )
                self._cancel_order(existing.id, reason="Replaced by new export")

            # Calculate totals
            total_pallets = sum(item.pallets for item in data.items)
            total_m2 = sum(item.m2 for item in data.items)
            total_weight_kg = sum(item.weight_kg for item in data.items)
            total_containers = ceil(total_pallets / CONTAINER_MAX_PALLETS)

            # Create order
            order_data = {
                "boat_id": data.boat_id,
                "status": WarehouseOrderStatus.PENDING.value,
                "boat_departure_date": (
                    data.boat_departure_date.isoformat()
                    if data.boat_departure_date
                    else None
                ),
                "boat_arrival_date": (
                    data.boat_arrival_date.isoformat()
                    if data.boat_arrival_date
                    else None
                ),
                "estimated_warehouse_date": (
                    data.estimated_warehouse_date.isoformat()
                    if data.estimated_warehouse_date
                    else None
                ),
                "boat_name": data.boat_name,
                "export_date": datetime.now().isoformat(),
                "exported_by": data.exported_by,
                "excel_filename": data.excel_filename,
                "total_pallets": total_pallets,
                "total_m2": float(total_m2),
                "total_containers": total_containers,
                "total_weight_kg": float(total_weight_kg),
                "notes": data.notes,
            }

            order_result = self.db.table(self.table).insert(order_data).execute()
            order_id = order_result.data[0]["id"]

            # Create items
            items_data = [
                {
                    "warehouse_order_id": order_id,
                    "product_id": item.product_id,
                    "sku": item.sku,
                    "description": item.description,
                    "pallets": item.pallets,
                    "m2": float(item.m2),
                    "weight_kg": float(item.weight_kg),
                    "score": item.score,
                    "priority": item.priority.value if item.priority else None,
                    "is_critical": item.is_critical,
                    "primary_customer": item.primary_customer,
                    "bl_number": item.bl_number,
                }
                for item in data.items
            ]

            self.db.table(self.items_table).insert(items_data).execute()

            logger.info(
                "warehouse_order_created",
                order_id=order_id,
                boat_id=data.boat_id,
                total_pallets=total_pallets,
                total_m2=float(total_m2),
                item_count=len(data.items),
            )

            return self.get_by_id(order_id)

        except Exception as e:
            logger.error("create_warehouse_order_failed", error=str(e))
            raise DatabaseError("insert", str(e))

    def update_status(
        self, order_id: str, data: WarehouseOrderStatusUpdate
    ) -> WarehouseOrderWithItemsResponse:
        """
        Update warehouse order status.

        Valid transitions:
        - pending -> shipped -> received
        - pending -> cancelled
        - shipped -> received

        Args:
            order_id: Order UUID
            data: New status

        Returns:
            Updated WarehouseOrderWithItemsResponse

        Raises:
            WarehouseOrderNotFoundError: If order doesn't exist
        """
        logger.info(
            "updating_warehouse_order_status",
            order_id=order_id,
            new_status=data.status,
        )

        # Get current order
        existing = self.get_by_id(order_id, include_items=False)
        current_status = WarehouseOrderStatus(existing.status)
        new_status = data.status

        # No-op if same status
        if current_status == new_status:
            return self.get_by_id(order_id)

        # Validate transition
        valid_transitions = {
            WarehouseOrderStatus.PENDING: [
                WarehouseOrderStatus.SHIPPED,
                WarehouseOrderStatus.CANCELLED,
            ],
            WarehouseOrderStatus.SHIPPED: [WarehouseOrderStatus.RECEIVED],
            WarehouseOrderStatus.RECEIVED: [],  # Terminal
            WarehouseOrderStatus.CANCELLED: [],  # Terminal
        }

        if new_status not in valid_transitions.get(current_status, []):
            logger.warning(
                "invalid_status_transition",
                order_id=order_id,
                current=current_status.value,
                new=new_status.value,
            )
            raise ValueError(
                f"Cannot transition from {current_status.value} to {new_status.value}"
            )

        try:
            update_data = {"status": new_status.value}

            # Set timestamp fields based on new status
            now = datetime.now().isoformat()
            if new_status == WarehouseOrderStatus.SHIPPED:
                update_data["shipped_at"] = now
            elif new_status == WarehouseOrderStatus.RECEIVED:
                update_data["received_at"] = now
            elif new_status == WarehouseOrderStatus.CANCELLED:
                update_data["cancelled_at"] = now

            self.db.table(self.table).update(update_data).eq("id", order_id).execute()

            logger.info(
                "warehouse_order_status_updated",
                order_id=order_id,
                from_status=current_status.value,
                to_status=new_status.value,
            )

            return self.get_by_id(order_id)

        except WarehouseOrderNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "update_warehouse_order_status_failed", order_id=order_id, error=str(e)
            )
            raise DatabaseError("update", str(e))

    def cancel(self, order_id: str, reason: Optional[str] = None) -> bool:
        """
        Cancel a warehouse order.

        Only PENDING orders can be cancelled.

        Args:
            order_id: Order UUID
            reason: Optional cancellation reason

        Returns:
            True if cancelled

        Raises:
            WarehouseOrderNotFoundError: If order doesn't exist
            ValueError: If order is not PENDING
        """
        logger.info("cancelling_warehouse_order", order_id=order_id, reason=reason)

        existing = self.get_by_id(order_id, include_items=False)

        if existing.status != WarehouseOrderStatus.PENDING:
            raise ValueError(
                f"Cannot cancel order with status {existing.status}. "
                "Only PENDING orders can be cancelled."
            )

        return self._cancel_order(order_id, reason)

    def _cancel_order(self, order_id: str, reason: Optional[str] = None) -> bool:
        """Internal method to cancel an order without status validation."""
        try:
            update_data = {
                "status": WarehouseOrderStatus.CANCELLED.value,
                "cancelled_at": datetime.now().isoformat(),
            }

            if reason:
                # Append reason to notes
                existing = self.get_by_id(order_id, include_items=False)
                current_notes = existing.notes or ""
                new_notes = (
                    f"{current_notes}\nCancelled: {reason}".strip()
                    if current_notes
                    else f"Cancelled: {reason}"
                )
                update_data["notes"] = new_notes

            self.db.table(self.table).update(update_data).eq("id", order_id).execute()

            logger.info("warehouse_order_cancelled", order_id=order_id, reason=reason)
            return True

        except Exception as e:
            logger.error(
                "cancel_warehouse_order_failed", order_id=order_id, error=str(e)
            )
            raise DatabaseError("update", str(e))

    # ===================
    # UTILITY METHODS
    # ===================

    def count(
        self,
        status: Optional[WarehouseOrderStatus] = None,
        boat_id: Optional[str] = None,
    ) -> int:
        """Count total warehouse orders."""
        try:
            query = self.db.table(self.table).select("id", count="exact")
            if status:
                query = query.eq("status", status.value)
            if boat_id:
                query = query.eq("boat_id", boat_id)
            result = query.execute()
            return result.count or 0
        except Exception as e:
            logger.error("count_warehouse_orders_failed", error=str(e))
            raise DatabaseError("count", str(e))

    def get_pending_m2_for_sku(self, sku: str) -> Decimal:
        """
        Get total pending m2 for a specific SKU.

        Args:
            sku: Product SKU

        Returns:
            Total pending m2 for this SKU
        """
        try:
            # Get pending order IDs
            orders_result = (
                self.db.table(self.table)
                .select("id")
                .eq("status", WarehouseOrderStatus.PENDING.value)
                .execute()
            )

            if not orders_result.data:
                return Decimal("0")

            order_ids = [row["id"] for row in orders_result.data]

            # Get items matching SKU
            items_result = (
                self.db.table(self.items_table)
                .select("m2")
                .in_("warehouse_order_id", order_ids)
                .eq("sku", sku)
                .execute()
            )

            total_m2 = sum(
                Decimal(str(item.get("m2", 0) or 0)) for item in items_result.data
            )

            return total_m2

        except Exception as e:
            logger.error("get_pending_m2_for_sku_failed", sku=sku, error=str(e))
            return Decimal("0")

    def _row_to_response(
        self, row: dict, item_count: Optional[int] = None
    ) -> WarehouseOrderResponse:
        """Convert database row to response model."""
        return WarehouseOrderResponse(
            id=row["id"],
            boat_id=row.get("boat_id"),
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
            shipped_at=row.get("shipped_at"),
            received_at=row.get("received_at"),
            cancelled_at=row.get("cancelled_at"),
            boat_departure_date=row.get("boat_departure_date"),
            boat_arrival_date=row.get("boat_arrival_date"),
            estimated_warehouse_date=row.get("estimated_warehouse_date"),
            boat_name=row.get("boat_name"),
            export_date=row.get("export_date"),
            exported_by=row.get("exported_by"),
            excel_filename=row.get("excel_filename"),
            total_pallets=row.get("total_pallets", 0) or 0,
            total_m2=Decimal(str(row.get("total_m2", 0) or 0)),
            total_containers=row.get("total_containers", 0) or 0,
            total_weight_kg=Decimal(str(row.get("total_weight_kg", 0) or 0)),
            notes=row.get("notes"),
            item_count=item_count,
        )

    def _item_row_to_response(self, row: dict) -> WarehouseOrderItemResponse:
        """Convert database item row to response model."""
        return WarehouseOrderItemResponse(
            id=row["id"],
            warehouse_order_id=row["warehouse_order_id"],
            product_id=row.get("product_id"),
            sku=row["sku"],
            description=row.get("description"),
            pallets=row.get("pallets", 0) or 0,
            m2=Decimal(str(row.get("m2", 0) or 0)),
            weight_kg=Decimal(str(row.get("weight_kg", 0) or 0)),
            score=row.get("score"),
            priority=row.get("priority"),
            is_critical=row.get("is_critical", False) or False,
            primary_customer=row.get("primary_customer"),
            bl_number=row.get("bl_number"),
            created_at=row["created_at"],
        )


# Singleton instance
_warehouse_order_service: Optional[WarehouseOrderService] = None


def get_warehouse_order_service() -> WarehouseOrderService:
    """Get or create WarehouseOrderService instance."""
    global _warehouse_order_service
    if _warehouse_order_service is None:
        _warehouse_order_service = WarehouseOrderService()
    return _warehouse_order_service
