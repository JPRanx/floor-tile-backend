"""
Sales service for business logic operations.

Handles weekly sales records from owner Excel uploads.
"""

from typing import Optional
from datetime import date
from decimal import Decimal
import structlog

from config import get_supabase_client
from models.sales import (
    SalesRecordCreate,
    SalesRecordUpdate,
    SalesRecordResponse,
    SalesHistoryResponse,
)
from exceptions import (
    SalesNotFoundError,
    DatabaseError
)

logger = structlog.get_logger(__name__)


class SalesService:
    """
    Sales business logic.

    Handles CRUD operations for weekly sales records.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "sales"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        product_id: Optional[str] = None,
        week_start: Optional[date] = None,
    ) -> tuple[list[SalesRecordResponse], int]:
        """
        Get all sales records with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            product_id: Filter by product
            week_start: Filter by week

        Returns:
            Tuple of (sales list, total count)
        """
        logger.info(
            "getting_sales",
            page=page,
            page_size=page_size,
            product_id=product_id
        )

        try:
            query = self.db.table(self.table).select("*", count="exact")

            if product_id:
                query = query.eq("product_id", product_id)
            if week_start:
                query = query.eq("week_start", week_start.isoformat())

            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)
            query = query.order("week_start", desc=True)

            result = query.execute()

            records = [SalesRecordResponse(**row) for row in result.data]
            total = result.count or 0

            logger.info(
                "sales_retrieved",
                count=len(records),
                total=total
            )

            return records, total

        except Exception as e:
            logger.error("get_sales_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, record_id: str) -> SalesRecordResponse:
        """
        Get a single sales record by ID.

        Args:
            record_id: Sales record UUID

        Returns:
            SalesRecordResponse

        Raises:
            SalesNotFoundError: If record doesn't exist
        """
        logger.debug("getting_sales_record", record_id=record_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", record_id)
                .single()
                .execute()
            )

            if not result.data:
                raise SalesNotFoundError(record_id)

            return SalesRecordResponse(**result.data)

        except SalesNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "get_sales_record_failed",
                record_id=record_id,
                error=str(e)
            )
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise SalesNotFoundError(record_id)
            raise DatabaseError("select", str(e))

    def get_history(
        self,
        product_id: str,
        limit: int = 52
    ) -> list[SalesRecordResponse]:
        """
        Get sales history for a product.

        Args:
            product_id: Product UUID
            limit: Maximum records to return (default 52 weeks = 1 year)

        Returns:
            List of sales records ordered by week descending
        """
        logger.debug(
            "getting_sales_history",
            product_id=product_id,
            limit=limit
        )

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("product_id", product_id)
                .order("week_start", desc=True)
                .limit(limit)
                .execute()
            )

            return [SalesRecordResponse(**row) for row in result.data]

        except Exception as e:
            logger.error(
                "get_sales_history_failed",
                product_id=product_id,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_weekly_totals(
        self,
        week_start: date
    ) -> list[SalesRecordResponse]:
        """
        Get all sales for a specific week.

        Args:
            week_start: Start of the week

        Returns:
            List of sales records for that week
        """
        logger.debug("getting_weekly_totals", week_start=week_start)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("week_start", week_start.isoformat())
                .order("product_id")
                .execute()
            )

            return [SalesRecordResponse(**row) for row in result.data]

        except Exception as e:
            logger.error(
                "get_weekly_totals_failed",
                week_start=week_start,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    # ===================
    # WRITE OPERATIONS
    # ===================

    def create(self, data: SalesRecordCreate) -> SalesRecordResponse:
        """
        Create a new sales record.

        Args:
            data: Sales record creation data

        Returns:
            Created SalesRecordResponse
        """
        logger.info(
            "creating_sales_record",
            product_id=data.product_id,
            week_start=str(data.week_start)
        )

        try:
            insert_data = {
                "product_id": data.product_id,
                "week_start": data.week_start.isoformat(),
                "quantity_m2": float(data.quantity_m2),
                "customer": data.customer,
                "customer_normalized": data.customer_normalized,
            }

            result = (
                self.db.table(self.table)
                .insert(insert_data)
                .execute()
            )

            record = SalesRecordResponse(**result.data[0])

            logger.info(
                "sales_record_created",
                record_id=record.id,
                product_id=record.product_id
            )

            return record

        except Exception as e:
            logger.error(
                "create_sales_record_failed",
                product_id=data.product_id,
                error=str(e)
            )
            raise DatabaseError("insert", str(e))

    def delete_by_date_range(self, start_date: date, end_date: date) -> int:
        """
        Delete sales records within a date range (inclusive).

        Used to make uploads idempotent - delete existing before re-inserting.

        Args:
            start_date: Start of date range
            end_date: End of date range

        Returns:
            Number of records deleted
        """
        logger.info(
            "deleting_sales_by_date_range",
            start=start_date.isoformat(),
            end=end_date.isoformat()
        )

        try:
            result = (
                self.db.table(self.table)
                .delete()
                .gte("week_start", start_date.isoformat())
                .lte("week_start", end_date.isoformat())
                .execute()
            )

            deleted = len(result.data) if result.data else 0

            logger.info("sales_deleted_by_date_range", count=deleted)

            return deleted

        except Exception as e:
            logger.error("delete_sales_by_date_range_failed", error=str(e))
            raise DatabaseError("delete", str(e))

    def bulk_create(
        self,
        records: list[SalesRecordCreate]
    ) -> list[SalesRecordResponse]:
        """
        Create multiple sales records at once.

        Used for Excel upload processing.

        Args:
            records: List of sales records to create

        Returns:
            List of created SalesRecordResponse
        """
        if not records:
            return []

        logger.info("bulk_creating_sales", count=len(records))

        try:
            insert_data = [
                {
                    "product_id": r.product_id,
                    "week_start": r.week_start.isoformat(),
                    "quantity_m2": float(r.quantity_m2),
                    "customer": r.customer,
                    "customer_normalized": r.customer_normalized,
                }
                for r in records
            ]

            result = (
                self.db.table(self.table)
                .insert(insert_data)
                .execute()
            )

            created = [SalesRecordResponse(**row) for row in result.data]

            logger.info("sales_bulk_created", count=len(created))

            return created

        except Exception as e:
            logger.error(
                "bulk_create_sales_failed",
                count=len(records),
                error=str(e)
            )
            raise DatabaseError("insert", str(e))

    def update(
        self,
        record_id: str,
        data: SalesRecordUpdate
    ) -> SalesRecordResponse:
        """
        Update an existing sales record.

        Args:
            record_id: Sales record UUID
            data: Fields to update

        Returns:
            Updated SalesRecordResponse

        Raises:
            SalesNotFoundError: If record doesn't exist
        """
        logger.info("updating_sales_record", record_id=record_id)

        existing = self.get_by_id(record_id)

        try:
            update_data = {}
            if data.week_start is not None:
                update_data["week_start"] = data.week_start.isoformat()
            if data.quantity_m2 is not None:
                update_data["quantity_m2"] = float(data.quantity_m2)

            if not update_data:
                return existing

            result = (
                self.db.table(self.table)
                .update(update_data)
                .eq("id", record_id)
                .execute()
            )

            record = SalesRecordResponse(**result.data[0])

            logger.info(
                "sales_record_updated",
                record_id=record_id,
                fields=list(update_data.keys())
            )

            return record

        except Exception as e:
            logger.error(
                "update_sales_record_failed",
                record_id=record_id,
                error=str(e)
            )
            raise DatabaseError("update", str(e))

    def delete(self, record_id: str) -> bool:
        """
        Delete a sales record.

        Args:
            record_id: Sales record UUID

        Returns:
            True if deleted

        Raises:
            SalesNotFoundError: If record doesn't exist
        """
        logger.info("deleting_sales_record", record_id=record_id)

        self.get_by_id(record_id)

        try:
            self.db.table(self.table).delete().eq("id", record_id).execute()

            logger.info("sales_record_deleted", record_id=record_id)

            return True

        except Exception as e:
            logger.error(
                "delete_sales_record_failed",
                record_id=record_id,
                error=str(e)
            )
            raise DatabaseError("delete", str(e))

    # ===================
    # BATCH OPERATIONS
    # ===================

    def get_recent_sales_all(
        self,
        weeks: int = 4
    ) -> dict[str, list[SalesRecordResponse]]:
        """
        Get recent sales for ALL products in a single query.

        This is much more efficient than calling get_history() per product.

        Args:
            weeks: Number of recent weeks to include

        Returns:
            Dictionary mapping product_id -> list of sales records
        """
        logger.debug("getting_recent_sales_all", weeks=weeks)

        try:
            # Calculate the date cutoff (N weeks ago)
            from datetime import timedelta
            cutoff_date = date.today() - timedelta(weeks=weeks)

            result = (
                self.db.table(self.table)
                .select("*")
                .gte("week_start", cutoff_date.isoformat())
                .order("week_start", desc=True)
                .execute()
            )

            # Group by product_id
            sales_by_product: dict[str, list[SalesRecordResponse]] = {}
            for row in result.data:
                record = SalesRecordResponse(**row)
                if record.product_id not in sales_by_product:
                    sales_by_product[record.product_id] = []
                sales_by_product[record.product_id].append(record)

            logger.info(
                "recent_sales_all_retrieved",
                products=len(sales_by_product),
                total_records=len(result.data)
            )

            return sales_by_product

        except Exception as e:
            logger.error("get_recent_sales_all_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # UTILITY METHODS
    # ===================

    def count(self, product_id: Optional[str] = None) -> int:
        """Count total sales records."""
        try:
            query = self.db.table(self.table).select("id", count="exact")
            if product_id:
                query = query.eq("product_id", product_id)
            result = query.execute()
            return result.count or 0
        except Exception as e:
            logger.error("count_sales_failed", error=str(e))
            raise DatabaseError("count", str(e))

    def get_product_total(self, product_id: str) -> Decimal:
        """Get total sales m² for a product across all time."""
        try:
            result = (
                self.db.table(self.table)
                .select("quantity_m2")
                .eq("product_id", product_id)
                .execute()
            )

            total = sum(
                Decimal(str(row["quantity_m2"]))
                for row in result.data
            )

            return total

        except Exception as e:
            logger.error(
                "get_product_total_failed",
                product_id=product_id,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_customer_analysis(
        self,
        product_id: str,
        weeks: int = 12
    ) -> dict:
        """
        Get customer analysis for a product (for confidence calculation).

        Args:
            product_id: Product UUID
            weeks: Number of recent weeks to analyze

        Returns:
            Dictionary with customer metrics:
            - unique_customers: Number of distinct customers
            - total_m2: Total sales volume
            - customer_breakdown: List of {customer, customer_normalized, total_m2, order_count}
            - top_customer_name: Original name of top customer
            - top_customer_share: Top customer's share of total
            - recurring_count: Customers with 2+ orders
            - recurring_share: Share of sales from recurring customers
        """
        logger.debug(
            "getting_customer_analysis",
            product_id=product_id,
            weeks=weeks
        )

        try:
            from datetime import timedelta
            cutoff_date = date.today() - timedelta(weeks=weeks)

            result = (
                self.db.table(self.table)
                .select("customer, customer_normalized, quantity_m2")
                .eq("product_id", product_id)
                .gte("week_start", cutoff_date.isoformat())
                .execute()
            )

            if not result.data:
                return {
                    "unique_customers": 0,
                    "total_m2": Decimal("0"),
                    "customer_breakdown": [],
                    "top_customer_name": None,
                    "top_customer_share": Decimal("0"),
                    "recurring_count": 0,
                    "recurring_share": Decimal("0"),
                }

            # Group by normalized customer
            customer_stats: dict[str, dict] = {}
            total_m2 = Decimal("0")

            for row in result.data:
                qty = Decimal(str(row["quantity_m2"]))
                total_m2 += qty

                # Use "UNKNOWN" for null/empty customer
                normalized = row.get("customer_normalized") or "UNKNOWN"
                original = row.get("customer") or "Unknown"

                if normalized not in customer_stats:
                    customer_stats[normalized] = {
                        "customer": original,
                        "customer_normalized": normalized,
                        "total_m2": Decimal("0"),
                        "order_count": 0,
                    }
                customer_stats[normalized]["total_m2"] += qty
                customer_stats[normalized]["order_count"] += 1

            # Sort by total_m2 descending
            breakdown = sorted(
                customer_stats.values(),
                key=lambda x: x["total_m2"],
                reverse=True
            )

            # Calculate metrics
            unique_customers = len(breakdown)
            top_customer = breakdown[0] if breakdown else None
            top_customer_share = (
                top_customer["total_m2"] / total_m2
                if top_customer and total_m2 > 0
                else Decimal("0")
            )

            # Recurring customers (2+ orders)
            recurring = [c for c in breakdown if c["order_count"] >= 2]
            recurring_m2 = sum(c["total_m2"] for c in recurring)
            recurring_share = recurring_m2 / total_m2 if total_m2 > 0 else Decimal("0")

            return {
                "unique_customers": unique_customers,
                "total_m2": total_m2,
                "customer_breakdown": breakdown,
                "top_customer_name": top_customer["customer"] if top_customer else None,
                "top_customer_share": round(top_customer_share, 4),
                "recurring_count": len(recurring),
                "recurring_share": round(recurring_share, 4),
            }

        except Exception as e:
            logger.error(
                "get_customer_analysis_failed",
                product_id=product_id,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_customer_analysis_batch(
        self,
        product_ids: list[str],
        weeks: int = 12
    ) -> dict[str, dict]:
        """
        Get customer analysis for multiple products in a single query.

        More efficient than calling get_customer_analysis() per product.

        Args:
            product_ids: List of product UUIDs
            weeks: Number of recent weeks to analyze

        Returns:
            Dictionary mapping product_id -> customer analysis dict
        """
        if not product_ids:
            return {}

        logger.debug(
            "getting_customer_analysis_batch",
            product_count=len(product_ids),
            weeks=weeks
        )

        try:
            from datetime import timedelta
            cutoff_date = date.today() - timedelta(weeks=weeks)

            result = (
                self.db.table(self.table)
                .select("product_id, customer, customer_normalized, quantity_m2")
                .in_("product_id", product_ids)
                .gte("week_start", cutoff_date.isoformat())
                .execute()
            )

            # Group by product, then by customer
            product_data: dict[str, list] = {pid: [] for pid in product_ids}
            for row in result.data:
                pid = row["product_id"]
                if pid in product_data:
                    product_data[pid].append(row)

            # Calculate analysis for each product
            analyses = {}
            for pid, rows in product_data.items():
                if not rows:
                    analyses[pid] = {
                        "unique_customers": 0,
                        "total_m2": Decimal("0"),
                        "top_customer_name": None,
                        "top_customer_share": Decimal("0"),
                        "recurring_count": 0,
                        "recurring_share": Decimal("0"),
                    }
                    continue

                # Group by normalized customer
                customer_stats: dict[str, dict] = {}
                total_m2 = Decimal("0")

                for row in rows:
                    qty = Decimal(str(row["quantity_m2"]))
                    total_m2 += qty

                    normalized = row.get("customer_normalized") or "UNKNOWN"
                    original = row.get("customer") or "Unknown"

                    if normalized not in customer_stats:
                        customer_stats[normalized] = {
                            "customer": original,
                            "total_m2": Decimal("0"),
                            "order_count": 0,
                        }
                    customer_stats[normalized]["total_m2"] += qty
                    customer_stats[normalized]["order_count"] += 1

                # Sort by total_m2 descending
                breakdown = sorted(
                    customer_stats.values(),
                    key=lambda x: x["total_m2"],
                    reverse=True
                )

                unique_customers = len(breakdown)
                top_customer = breakdown[0] if breakdown else None
                top_customer_share = (
                    top_customer["total_m2"] / total_m2
                    if top_customer and total_m2 > 0
                    else Decimal("0")
                )

                recurring = [c for c in breakdown if c["order_count"] >= 2]
                recurring_m2 = sum(c["total_m2"] for c in recurring)
                recurring_share = recurring_m2 / total_m2 if total_m2 > 0 else Decimal("0")

                analyses[pid] = {
                    "unique_customers": unique_customers,
                    "total_m2": total_m2,
                    "top_customer_name": top_customer["customer"] if top_customer else None,
                    "top_customer_share": round(top_customer_share, 4),
                    "recurring_count": len(recurring),
                    "recurring_share": round(recurring_share, 4),
                }

            logger.info(
                "customer_analysis_batch_completed",
                products=len(analyses)
            )

            return analyses

        except Exception as e:
            logger.error("get_customer_analysis_batch_failed", error=str(e))
            raise DatabaseError("select", str(e))


# Singleton instance
_sales_service: Optional[SalesService] = None


def get_sales_service() -> SalesService:
    """Get or create SalesService instance."""
    global _sales_service
    if _sales_service is None:
        _sales_service = SalesService()
    return _sales_service
