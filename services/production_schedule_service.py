"""
Production Schedule service for CRUD operations.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
from datetime import date, timedelta
from decimal import Decimal
import structlog

from config import get_supabase_client
from models.production_schedule import (
    ParsedProductionSchedule,
    ProductionScheduleResponse,
    UpcomingProductionItem,
)
from services.product_service import get_product_service
from exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class ProductionScheduleService:
    """
    Production schedule business logic.

    Handles saving parsed schedules and querying upcoming production.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "production_schedule"
        self.product_service = get_product_service()

    # ===================
    # UPSERT OPERATIONS
    # ===================

    def save_parsed_schedule(
        self,
        parsed_data: ParsedProductionSchedule,
        filename: Optional[str] = None
    ) -> tuple[int, int, list[str]]:
        """
        Save parsed production schedule to database.

        Uses upsert to handle re-uploading same schedule with updates.
        Matches factory codes to products where possible.

        Args:
            parsed_data: Parsed schedule from Claude Vision
            filename: Original PDF filename

        Returns:
            Tuple of (items_saved, products_matched, unmatched_factory_codes)
        """
        logger.info(
            "saving_production_schedule",
            schedule_date=str(parsed_data.schedule_date),
            line_items_count=len(parsed_data.line_items),
            filename=filename
        )

        items_saved = 0
        products_matched = 0
        unmatched_codes = set()

        # Get all unique factory codes and batch lookup products
        factory_codes = list(set(item.factory_code for item in parsed_data.line_items))
        products_by_code = {}

        if factory_codes:
            products = self.product_service.get_by_factory_codes(factory_codes)
            products_by_code = {p.factory_code: p for p in products if p.factory_code}

        for item in parsed_data.line_items:
            try:
                # Match product by factory code
                product = products_by_code.get(item.factory_code)
                product_id = product.id if product else None

                if product:
                    products_matched += 1
                else:
                    unmatched_codes.add(item.factory_code)

                # Build upsert data
                upsert_data = {
                    "schedule_date": parsed_data.schedule_date.isoformat(),
                    "schedule_version": parsed_data.schedule_version,
                    "source_filename": filename,
                    "production_date": item.production_date.isoformat(),
                    "factory_code": item.factory_code,
                    "product_name": item.product_name,
                    "product_id": product_id,
                    "plant": item.plant,
                    "format": item.format,
                    "design": item.design,
                    "finish": item.finish,
                    "shifts": float(item.shifts) if item.shifts else None,
                    "quality_target_pct": float(item.quality_target_pct) if item.quality_target_pct else None,
                    "quality_actual_pct": float(item.quality_actual_pct) if item.quality_actual_pct else None,
                    "m2_total_net": float(item.m2_total_net) if item.m2_total_net else None,
                    "m2_export_first": float(item.m2_export_first) if item.m2_export_first else None,
                    "pct_showroom": item.pct_showroom,
                    "pct_distribution": item.pct_distribution,
                }

                # Upsert (insert or update on conflict)
                self.db.table(self.table).upsert(
                    upsert_data,
                    on_conflict="schedule_date,production_date,factory_code,plant"
                ).execute()

                items_saved += 1

            except Exception as e:
                logger.error(
                    "save_schedule_item_failed",
                    factory_code=item.factory_code,
                    production_date=str(item.production_date),
                    error=str(e)
                )
                continue

        logger.info(
            "production_schedule_saved",
            items_saved=items_saved,
            products_matched=products_matched,
            unmatched_count=len(unmatched_codes)
        )

        return items_saved, products_matched, list(unmatched_codes)

    # ===================
    # READ OPERATIONS
    # ===================

    def get_by_schedule_date(
        self,
        schedule_date: date,
        plant: Optional[int] = None
    ) -> list[ProductionScheduleResponse]:
        """
        Get all production items for a specific schedule date.

        Args:
            schedule_date: The schedule generation date
            plant: Optional filter by plant (1 or 2)

        Returns:
            List of ProductionScheduleResponse
        """
        logger.debug("getting_schedule_by_date", schedule_date=str(schedule_date), plant=plant)

        try:
            query = (
                self.db.table(self.table)
                .select("*")
                .eq("schedule_date", schedule_date.isoformat())
                .order("production_date")
                .order("plant")
            )

            if plant:
                query = query.eq("plant", plant)

            result = query.execute()

            return [ProductionScheduleResponse(**row) for row in result.data]

        except Exception as e:
            logger.error("get_schedule_by_date_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_upcoming_production(
        self,
        days_ahead: int = 30,
        product_id: Optional[str] = None,
        factory_code: Optional[str] = None
    ) -> list[UpcomingProductionItem]:
        """
        Get upcoming production within a date range.

        Args:
            days_ahead: Number of days to look ahead (default 30)
            product_id: Optional filter by product UUID
            factory_code: Optional filter by factory code

        Returns:
            List of UpcomingProductionItem sorted by date
        """
        logger.debug(
            "getting_upcoming_production",
            days_ahead=days_ahead,
            product_id=product_id,
            factory_code=factory_code
        )

        try:
            today = date.today()
            end_date = today + timedelta(days=days_ahead)

            query = (
                self.db.table(self.table)
                .select("*, products(sku)")
                .gte("production_date", today.isoformat())
                .lte("production_date", end_date.isoformat())
                .order("production_date")
            )

            if product_id:
                query = query.eq("product_id", product_id)
            if factory_code:
                query = query.eq("factory_code", factory_code)

            result = query.execute()

            items = []
            for row in result.data:
                # Calculate days until production
                prod_date = date.fromisoformat(row["production_date"])
                days_until = (prod_date - today).days

                # Get SKU from joined products table
                sku = None
                if row.get("products"):
                    sku = row["products"].get("sku")

                items.append(UpcomingProductionItem(
                    production_date=prod_date,
                    factory_code=row["factory_code"],
                    product_name=row.get("product_name"),
                    product_id=row.get("product_id"),
                    sku=sku,
                    plant=row["plant"],
                    m2_export_first=Decimal(str(row["m2_export_first"])) if row.get("m2_export_first") else None,
                    days_until_production=days_until
                ))

            return items

        except Exception as e:
            logger.error("get_upcoming_production_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_production_for_product(
        self,
        product_id: str,
        include_past: bool = False
    ) -> list[ProductionScheduleResponse]:
        """
        Get all production schedule entries for a specific product.

        Args:
            product_id: Product UUID
            include_past: If True, include past production dates

        Returns:
            List of ProductionScheduleResponse
        """
        logger.debug("getting_production_for_product", product_id=product_id)

        try:
            query = (
                self.db.table(self.table)
                .select("*")
                .eq("product_id", product_id)
                .order("production_date", desc=True)
            )

            if not include_past:
                query = query.gte("production_date", date.today().isoformat())

            result = query.execute()

            return [ProductionScheduleResponse(**row) for row in result.data]

        except Exception as e:
            logger.error("get_production_for_product_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_unmatched_factory_codes(self) -> list[dict]:
        """
        Get factory codes that haven't been matched to products.

        Useful for identifying products that need factory_code set.

        Returns:
            List of dicts with factory_code, product_name, count
        """
        logger.debug("getting_unmatched_factory_codes")

        try:
            # Get distinct unmatched codes with their product names
            result = (
                self.db.table(self.table)
                .select("factory_code, product_name")
                .is_("product_id", "null")
                .execute()
            )

            # Aggregate by factory code
            codes = {}
            for row in result.data:
                code = row["factory_code"]
                if code not in codes:
                    codes[code] = {
                        "factory_code": code,
                        "product_name": row.get("product_name"),
                        "count": 0
                    }
                codes[code]["count"] += 1

            return list(codes.values())

        except Exception as e:
            logger.error("get_unmatched_factory_codes_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_schedule_dates(self, limit: int = 10) -> list[dict]:
        """
        Get list of distinct schedule dates (most recent first).

        Returns:
            List of dicts with schedule_date, schedule_version, item_count
        """
        logger.debug("getting_schedule_dates", limit=limit)

        try:
            # Get all records and aggregate in Python (Supabase doesn't support GROUP BY well)
            result = (
                self.db.table(self.table)
                .select("schedule_date, schedule_version")
                .order("schedule_date", desc=True)
                .execute()
            )

            # Aggregate by schedule_date
            schedules = {}
            for row in result.data:
                sched_date = row["schedule_date"]
                if sched_date not in schedules:
                    schedules[sched_date] = {
                        "schedule_date": sched_date,
                        "schedule_version": row.get("schedule_version"),
                        "item_count": 0
                    }
                schedules[sched_date]["item_count"] += 1

            # Sort and limit
            sorted_schedules = sorted(
                schedules.values(),
                key=lambda x: x["schedule_date"],
                reverse=True
            )[:limit]

            return sorted_schedules

        except Exception as e:
            logger.error("get_schedule_dates_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # UTILITY OPERATIONS
    # ===================

    def rematch_products(self) -> tuple[int, int]:
        """
        Re-match all schedule items to products by factory code.

        Useful after updating factory_code on products.

        Returns:
            Tuple of (total_processed, newly_matched)
        """
        logger.info("rematching_products")

        try:
            # Get all unmatched items
            unmatched_result = (
                self.db.table(self.table)
                .select("id, factory_code")
                .is_("product_id", "null")
                .execute()
            )

            if not unmatched_result.data:
                return 0, 0

            # Get unique factory codes
            factory_codes = list(set(row["factory_code"] for row in unmatched_result.data))
            products = self.product_service.get_by_factory_codes(factory_codes)
            products_by_code = {p.factory_code: p for p in products if p.factory_code}

            newly_matched = 0
            for row in unmatched_result.data:
                product = products_by_code.get(row["factory_code"])
                if product:
                    self.db.table(self.table).update(
                        {"product_id": product.id}
                    ).eq("id", row["id"]).execute()
                    newly_matched += 1

            logger.info(
                "products_rematched",
                total_processed=len(unmatched_result.data),
                newly_matched=newly_matched
            )

            return len(unmatched_result.data), newly_matched

        except Exception as e:
            logger.error("rematch_products_failed", error=str(e))
            raise DatabaseError("update", str(e))


# Singleton instance
_schedule_service: Optional[ProductionScheduleService] = None


def get_production_schedule_service() -> ProductionScheduleService:
    """Get or create ProductionScheduleService instance."""
    global _schedule_service
    if _schedule_service is None:
        _schedule_service = ProductionScheduleService()
    return _schedule_service
