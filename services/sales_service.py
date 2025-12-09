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


# Singleton instance
_sales_service: Optional[SalesService] = None


def get_sales_service() -> SalesService:
    """Get or create SalesService instance."""
    global _sales_service
    if _sales_service is None:
        _sales_service = SalesService()
    return _sales_service
