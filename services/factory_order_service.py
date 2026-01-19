"""
Factory order service for business logic operations.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
from decimal import Decimal
from datetime import date
import structlog

from config import get_supabase_client
from models.factory_order import (
    FactoryOrderCreate,
    FactoryOrderUpdate,
    FactoryOrderStatusUpdate,
    FactoryOrderResponse,
    FactoryOrderWithItemsResponse,
    FactoryOrderItemResponse,
    OrderStatus,
    is_valid_status_transition,
)
from exceptions import (
    FactoryOrderNotFoundError,
    FactoryOrderPVExistsError,
    InvalidStatusTransitionError,
    DatabaseError,
)

logger = structlog.get_logger(__name__)


class FactoryOrderService:
    """
    Factory order business logic.

    Handles CRUD operations for factory orders and items.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "factory_orders"
        self.items_table = "factory_order_items"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        status: Optional[OrderStatus] = None,
        active_only: bool = True
    ) -> tuple[list[FactoryOrderResponse], int]:
        """
        Get all factory orders with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Filter by status
            active_only: Only return active orders

        Returns:
            Tuple of (orders list, total count)
        """
        logger.info(
            "getting_factory_orders",
            page=page,
            page_size=page_size,
            status=status
        )

        try:
            # Build query
            query = self.db.table(self.table).select("*", count="exact")

            # Apply filters
            if active_only:
                query = query.eq("active", True)
            if status:
                query = query.eq("status", status.value)

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by order_date descending
            query = query.order("order_date", desc=True)

            # Execute
            result = query.execute()

            orders = []
            for row in result.data:
                # Get item count and total for each order
                items_result = (
                    self.db.table(self.items_table)
                    .select("quantity_ordered")
                    .eq("factory_order_id", row["id"])
                    .execute()
                )

                item_count = len(items_result.data)
                total_m2 = sum(
                    Decimal(str(item["quantity_ordered"]))
                    for item in items_result.data
                )

                orders.append(FactoryOrderResponse(
                    id=row["id"],
                    pv_number=row.get("pv_number"),
                    order_date=row["order_date"],
                    status=row["status"],
                    notes=row.get("notes"),
                    active=row.get("active", True),
                    created_at=row["created_at"],
                    updated_at=row.get("updated_at"),
                    total_m2=total_m2,
                    item_count=item_count,
                ))

            total = result.count or 0

            logger.info(
                "factory_orders_retrieved",
                count=len(orders),
                total=total
            )

            return orders, total

        except Exception as e:
            logger.error("get_factory_orders_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, order_id: str, include_items: bool = True) -> FactoryOrderWithItemsResponse:
        """
        Get a single factory order by ID.

        Args:
            order_id: Order UUID
            include_items: Whether to include line items

        Returns:
            FactoryOrderWithItemsResponse

        Raises:
            FactoryOrderNotFoundError: If order doesn't exist
        """
        logger.debug("getting_factory_order", order_id=order_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", order_id)
                .single()
                .execute()
            )

            if not result.data:
                raise FactoryOrderNotFoundError(order_id)

            row = result.data

            # Get items
            items = []
            total_m2 = Decimal("0")

            if include_items:
                items_result = (
                    self.db.table(self.items_table)
                    .select("*, products(sku)")
                    .eq("factory_order_id", order_id)
                    .execute()
                )

                for item in items_result.data:
                    qty = Decimal(str(item["quantity_ordered"]))
                    total_m2 += qty

                    items.append(FactoryOrderItemResponse(
                        id=item["id"],
                        factory_order_id=item["factory_order_id"],
                        product_id=item["product_id"],
                        quantity_ordered=qty,
                        quantity_produced=Decimal(str(item.get("quantity_produced", 0))),
                        estimated_ready_date=item.get("estimated_ready_date"),
                        actual_ready_date=item.get("actual_ready_date"),
                        created_at=item["created_at"],
                        product_sku=item.get("products", {}).get("sku") if item.get("products") else None,
                    ))

            return FactoryOrderWithItemsResponse(
                id=row["id"],
                pv_number=row.get("pv_number"),
                order_date=row["order_date"],
                status=row["status"],
                notes=row.get("notes"),
                active=row.get("active", True),
                created_at=row["created_at"],
                updated_at=row.get("updated_at"),
                total_m2=total_m2,
                item_count=len(items),
                items=items,
            )

        except FactoryOrderNotFoundError:
            raise
        except Exception as e:
            logger.error("get_factory_order_failed", order_id=order_id, error=str(e))
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise FactoryOrderNotFoundError(order_id)
            raise DatabaseError("select", str(e))

    def get_by_pv_number(self, pv_number: str) -> Optional[FactoryOrderWithItemsResponse]:
        """
        Get a factory order by PV number.

        Args:
            pv_number: Pedido de Ventas number

        Returns:
            FactoryOrderWithItemsResponse or None if not found
        """
        logger.debug("getting_factory_order_by_pv", pv_number=pv_number)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("pv_number", pv_number.upper())
                .execute()
            )

            if not result.data:
                return None

            # Use get_by_id to get full response with items
            return self.get_by_id(result.data[0]["id"])

        except Exception as e:
            logger.error("get_factory_order_by_pv_failed", pv_number=pv_number, error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_status(self, status: OrderStatus) -> list[FactoryOrderResponse]:
        """
        Get all orders with a specific status.

        Args:
            status: Order status to filter by

        Returns:
            List of orders with that status
        """
        orders, _ = self.get_all(page=1, page_size=1000, status=status)
        return orders

    # ===================
    # WRITE OPERATIONS
    # ===================

    def _generate_pv_number(self, order_date: date) -> str:
        """
        Generate a sequential PV number for a given date.

        Format: PV-YYYYMMDD-NNN (e.g., PV-20260119-001)

        Args:
            order_date: Date to generate PV number for

        Returns:
            Generated PV number string
        """
        count = self.count_by_date(order_date)
        sequence = count + 1
        date_str = order_date.strftime("%Y%m%d")
        return f"PV-{date_str}-{sequence:03d}"

    def create(self, data: FactoryOrderCreate) -> FactoryOrderWithItemsResponse:
        """
        Create a new factory order with items.

        Args:
            data: Order creation data with items

        Returns:
            Created FactoryOrderWithItemsResponse

        Raises:
            FactoryOrderPVExistsError: If PV number already exists
        """
        # Auto-generate PV number if not provided
        pv_number = data.pv_number
        if not pv_number:
            pv_number = self._generate_pv_number(data.order_date)
            logger.info("auto_generated_pv_number", pv_number=pv_number)

        logger.info("creating_factory_order", pv_number=pv_number)

        # Check for duplicate PV number
        if pv_number:
            existing = self.get_by_pv_number(pv_number)
            if existing:
                raise FactoryOrderPVExistsError(pv_number)

        try:
            # Create order
            order_data = {
                "pv_number": pv_number,
                "order_date": data.order_date.isoformat(),
                "status": OrderStatus.PENDING.value,
                "notes": data.notes,
                "active": True,
            }

            order_result = (
                self.db.table(self.table)
                .insert(order_data)
                .execute()
            )

            order_id = order_result.data[0]["id"]

            # Create items
            items_data = [
                {
                    "factory_order_id": order_id,
                    "product_id": item.product_id,
                    "quantity_ordered": float(item.quantity_ordered),
                    "quantity_produced": 0,
                    "estimated_ready_date": item.estimated_ready_date.isoformat() if item.estimated_ready_date else None,
                }
                for item in data.items
            ]

            self.db.table(self.items_table).insert(items_data).execute()

            logger.info(
                "factory_order_created",
                order_id=order_id,
                pv_number=data.pv_number,
                item_count=len(data.items)
            )

            return self.get_by_id(order_id)

        except FactoryOrderPVExistsError:
            raise
        except Exception as e:
            logger.error("create_factory_order_failed", error=str(e))
            raise DatabaseError("insert", str(e))

    def update(self, order_id: str, data: FactoryOrderUpdate) -> FactoryOrderWithItemsResponse:
        """
        Update an existing factory order.

        Args:
            order_id: Order UUID
            data: Fields to update

        Returns:
            Updated FactoryOrderWithItemsResponse

        Raises:
            FactoryOrderNotFoundError: If order doesn't exist
            FactoryOrderPVExistsError: If new PV number already exists
        """
        logger.info("updating_factory_order", order_id=order_id)

        # Check order exists
        existing = self.get_by_id(order_id, include_items=False)

        # If changing PV number, check for duplicates
        if data.pv_number and data.pv_number.upper() != existing.pv_number:
            pv_check = self.get_by_pv_number(data.pv_number)
            if pv_check:
                raise FactoryOrderPVExistsError(data.pv_number)

        try:
            # Build update dict
            update_data = {}
            if data.pv_number is not None:
                update_data["pv_number"] = data.pv_number.upper()
            if data.order_date is not None:
                update_data["order_date"] = data.order_date.isoformat()
            if data.notes is not None:
                update_data["notes"] = data.notes
            if data.status is not None:
                # Validate status transition
                if not is_valid_status_transition(existing.status, data.status):
                    raise InvalidStatusTransitionError(existing.status.value, data.status.value)
                update_data["status"] = data.status.value

            if not update_data:
                return self.get_by_id(order_id)

            self.db.table(self.table).update(update_data).eq("id", order_id).execute()

            logger.info(
                "factory_order_updated",
                order_id=order_id,
                fields=list(update_data.keys())
            )

            return self.get_by_id(order_id)

        except (FactoryOrderNotFoundError, FactoryOrderPVExistsError):
            raise
        except Exception as e:
            logger.error("update_factory_order_failed", order_id=order_id, error=str(e))
            raise DatabaseError("update", str(e))

    def update_status(self, order_id: str, data: FactoryOrderStatusUpdate) -> FactoryOrderWithItemsResponse:
        """
        Update factory order status.

        Args:
            order_id: Order UUID
            data: New status

        Returns:
            Updated FactoryOrderWithItemsResponse

        Raises:
            FactoryOrderNotFoundError: If order doesn't exist
            InvalidStatusTransitionError: If transition is not allowed
        """
        logger.info("updating_factory_order_status", order_id=order_id, new_status=data.status)

        # Get current order
        existing = self.get_by_id(order_id, include_items=False)
        current_status = OrderStatus(existing.status)
        new_status = data.status

        # Validate transition
        if current_status == new_status:
            return self.get_by_id(order_id)

        if not is_valid_status_transition(current_status, new_status):
            raise InvalidStatusTransitionError(
                current_status=current_status.value,
                new_status=new_status.value
            )

        try:
            self.db.table(self.table).update({
                "status": new_status.value
            }).eq("id", order_id).execute()

            logger.info(
                "factory_order_status_updated",
                order_id=order_id,
                from_status=current_status.value,
                to_status=new_status.value
            )

            return self.get_by_id(order_id)

        except (FactoryOrderNotFoundError, InvalidStatusTransitionError):
            raise
        except Exception as e:
            logger.error("update_factory_order_status_failed", order_id=order_id, error=str(e))
            raise DatabaseError("update", str(e))

    def delete(self, order_id: str) -> bool:
        """
        Soft delete a factory order (set active=False).

        Args:
            order_id: Order UUID

        Returns:
            True if deleted

        Raises:
            FactoryOrderNotFoundError: If order doesn't exist
        """
        logger.info("deleting_factory_order", order_id=order_id)

        # Check order exists
        self.get_by_id(order_id, include_items=False)

        try:
            self.db.table(self.table).update({
                "active": False
            }).eq("id", order_id).execute()

            logger.info("factory_order_deleted", order_id=order_id)

            return True

        except FactoryOrderNotFoundError:
            raise
        except Exception as e:
            logger.error("delete_factory_order_failed", order_id=order_id, error=str(e))
            raise DatabaseError("update", str(e))

    # ===================
    # ITEM OPERATIONS
    # ===================

    def get_items(self, order_id: str) -> list[FactoryOrderItemResponse]:
        """
        Get all items for a factory order.

        Args:
            order_id: Order UUID

        Returns:
            List of order items
        """
        # This validates the order exists
        order = self.get_by_id(order_id, include_items=True)
        return order.items

    # ===================
    # UTILITY METHODS
    # ===================

    def pv_exists(self, pv_number: str) -> bool:
        """Check if a PV number already exists."""
        return self.get_by_pv_number(pv_number) is not None

    def search_by_pv(
        self,
        query: str,
        limit: int = 10,
        exclude_shipped: bool = True
    ) -> list[FactoryOrderResponse]:
        """
        Search factory orders by PV number (fuzzy prefix match).

        Used for typeahead/autocomplete in shipment linking UI.

        Args:
            query: Search string (e.g., "PV-2026" or "001")
            limit: Maximum results to return
            exclude_shipped: Exclude orders already shipped

        Returns:
            List of matching factory orders
        """
        logger.debug("searching_factory_orders_by_pv", query=query, limit=limit)

        if not query or len(query) < 2:
            return []

        try:
            # Use ilike for case-insensitive prefix match
            search_pattern = f"%{query.upper()}%"

            query_builder = (
                self.db.table(self.table)
                .select("*")
                .ilike("pv_number", search_pattern)
                .eq("active", True)
            )

            if exclude_shipped:
                query_builder = query_builder.neq("status", "SHIPPED")

            query_builder = query_builder.order("order_date", desc=True).limit(limit)

            result = query_builder.execute()

            orders = []
            for row in result.data:
                # Get item count and total for each order
                items_result = (
                    self.db.table(self.items_table)
                    .select("quantity_ordered")
                    .eq("factory_order_id", row["id"])
                    .execute()
                )

                item_count = len(items_result.data)
                total_m2 = sum(
                    Decimal(str(item["quantity_ordered"]))
                    for item in items_result.data
                )

                orders.append(FactoryOrderResponse(
                    id=row["id"],
                    pv_number=row.get("pv_number"),
                    order_date=row["order_date"],
                    status=row["status"],
                    notes=row.get("notes"),
                    active=row.get("active", True),
                    created_at=row["created_at"],
                    updated_at=row.get("updated_at"),
                    total_m2=total_m2,
                    item_count=item_count,
                ))

            logger.debug(
                "factory_orders_search_complete",
                query=query,
                results=len(orders)
            )

            return orders

        except Exception as e:
            logger.error("search_factory_orders_failed", query=query, error=str(e))
            raise DatabaseError("select", str(e))

    def count(self, status: Optional[OrderStatus] = None, active_only: bool = True) -> int:
        """Count total factory orders."""
        try:
            query = self.db.table(self.table).select("id", count="exact")
            if active_only:
                query = query.eq("active", True)
            if status:
                query = query.eq("status", status.value)
            result = query.execute()
            return result.count or 0
        except Exception as e:
            logger.error("count_factory_orders_failed", error=str(e))
            raise DatabaseError("count", str(e))

    def count_by_date(self, order_date: date) -> int:
        """
        Count factory orders created on a specific date.

        Used for auto-generating sequential PV numbers (e.g., PV-20260108-001).

        Args:
            order_date: Date to count orders for

        Returns:
            Number of orders with this order_date
        """
        try:
            result = (
                self.db.table(self.table)
                .select("id", count="exact")
                .eq("order_date", order_date.isoformat())
                .execute()
            )
            return result.count or 0
        except Exception as e:
            logger.error("count_by_date_failed", date=order_date.isoformat(), error=str(e))
            raise DatabaseError("count", str(e))


# Singleton instance
_factory_order_service: Optional[FactoryOrderService] = None


def get_factory_order_service() -> FactoryOrderService:
    """Get or create FactoryOrderService instance."""
    global _factory_order_service
    if _factory_order_service is None:
        _factory_order_service = FactoryOrderService()
    return _factory_order_service
