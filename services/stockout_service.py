"""
Stockout calculation service — Core business logic.

Calculates days until stockout for each product based on
inventory levels and sales velocity.

Now uses MetricsService as single source of truth for coverage calculations.
See BUILDER_BLUEPRINT.md for calculation logic.
"""

from typing import Optional
from decimal import Decimal
from enum import Enum
from datetime import date, timedelta
import structlog

from config import settings
from services.inventory_service import get_inventory_service
from services.sales_service import get_sales_service
from services.product_service import get_product_service
from services.boat_schedule_service import get_boat_schedule_service
from services.metrics_service import get_metrics_service
from models.base import BaseSchema

logger = structlog.get_logger(__name__)


class StockoutStatus(str, Enum):
    """Stockout status levels based on boat arrival dates."""
    # Priority tied to boat arrivals
    HIGH_PRIORITY = "HIGH_PRIORITY"  # Will stock out before next boat arrives
    CONSIDER = "CONSIDER"            # Will stock out before second boat arrives
    WELL_COVERED = "WELL_COVERED"    # Won't stock out for 2+ boat cycles
    YOUR_CALL = "YOUR_CALL"          # No data / needs manual review


class ProductStockout(BaseSchema):
    """Stockout calculation result for a single product."""

    product_id: str
    sku: str
    category: Optional[str] = None
    rotation: Optional[str] = None
    active: bool = True  # Whether product is active in catalog

    # Inventory
    warehouse_qty: Decimal
    in_transit_qty: Decimal
    total_qty: Decimal

    # Factory inventory (SIESA)
    factory_available_m2: Decimal = Decimal("0")
    factory_lot_count: int = 0

    # Sales velocity
    avg_daily_sales: Decimal
    weekly_sales: Decimal
    weeks_of_data: int

    # Stockout calculation
    days_to_stockout: Optional[Decimal] = None
    stockout_date: Optional[date] = None

    # Status
    status: StockoutStatus
    status_reason: str


class StockoutSummary(BaseSchema):
    """Summary of stockout calculations across all products."""

    total_products: int

    # Counts by priority (boat-based)
    high_priority_count: int = 0
    consider_count: int = 0
    well_covered_count: int = 0
    your_call_count: int = 0

    # Boat departure info (for user-facing display)
    next_boat_departure: Optional[date] = None
    second_boat_departure: Optional[date] = None
    days_to_next_boat_departure: Optional[int] = None
    days_to_second_boat_departure: Optional[int] = None

    # Boat arrival info (kept for internal use / backwards compatibility)
    next_boat_arrival: Optional[date] = None
    second_boat_arrival: Optional[date] = None
    days_to_next_boat: Optional[int] = None
    days_to_second_boat: Optional[int] = None

    products: list[ProductStockout]


class StockoutService:
    """
    Stockout calculation business logic.

    Calculates days until stockout for each product using:
    - Latest inventory (warehouse + in_transit)
    - Sales velocity (last 12 weeks average)
    - Boat arrival dates for priority determination
    """

    def __init__(self):
        self.inventory_service = get_inventory_service()
        self.sales_service = get_sales_service()
        self.product_service = get_product_service()
        self.boat_service = get_boat_schedule_service()

        # Settings
        self.lead_time = settings.lead_time_days  # 45 days fallback
        self.sales_weeks = settings.velocity_window_weeks  # 12 weeks default

    def _get_boat_thresholds(self) -> tuple[int, int]:
        """
        Get days to next 2 boat arrivals for priority thresholds.

        Returns:
            (days_to_next_boat, days_to_second_boat)
            Falls back to settings if no boats in system.
        """
        today = date.today()
        next_arrival, second_arrival = self.boat_service.get_next_two_arrivals()

        if next_arrival:
            days_to_next = (next_arrival - today).days
        else:
            # Fallback: use lead_time as threshold
            days_to_next = self.lead_time

        if second_arrival:
            days_to_second = (second_arrival - today).days
        else:
            # Fallback: use 2x lead_time as threshold
            days_to_second = self.lead_time * 2

        return days_to_next, days_to_second

    def calculate_all(self) -> StockoutSummary:
        """
        Calculate stockout status for all products.

        Uses MetricsService as single source of truth for coverage calculations.
        Adds boat-based priority classification on top.

        Returns:
            StockoutSummary with all product calculations
        """
        logger.info("calculating_stockout_all")

        # Get boat arrival thresholds (for internal priority calculations)
        today = date.today()
        next_arrival, second_arrival = self.boat_service.get_next_two_arrivals()
        days_to_next, days_to_second = self._get_boat_thresholds()

        # Get boat departure dates (for user-facing display)
        next_departure, second_departure = self.boat_service.get_next_two_departures()
        days_to_next_departure = (next_departure - today).days if next_departure else None
        days_to_second_departure = (second_departure - today).days if second_departure else None

        logger.info(
            "boat_thresholds",
            next_arrival=next_arrival,
            second_arrival=second_arrival,
            days_to_next=days_to_next,
            days_to_second=days_to_second,
            next_departure=next_departure,
            days_to_next_departure=days_to_next_departure
        )

        # Get all metrics from MetricsService (single source of truth)
        # Uses 90-day velocity and warehouse-only for days calculation
        metrics_service = get_metrics_service()
        all_metrics = metrics_service.get_all_product_metrics(
            next_boat_arrival_days=days_to_next
        )

        # Build results with boat-based priority classification
        results = []
        for metrics in all_metrics:
            coverage = metrics.coverage

            # Use warehouse_days from MetricsService (warehouse / 90-day velocity)
            days_to_stockout = coverage.warehouse_days
            stockout_date = coverage.stockout_date

            # Determine status based on boat arrivals
            if days_to_stockout is None or coverage.velocity_m2_day == 0:
                status = StockoutStatus.YOUR_CALL
                status_reason = "No sales velocity — needs manual review"
            elif days_to_stockout < days_to_next:
                status = StockoutStatus.HIGH_PRIORITY
                status_reason = f"Stockout in {int(days_to_stockout)} days — before next boat arrives ({days_to_next} days)"
            elif days_to_stockout < days_to_second:
                status = StockoutStatus.CONSIDER
                status_reason = f"Stockout in {int(days_to_stockout)} days — before second boat ({days_to_second} days)"
            else:
                status = StockoutStatus.WELL_COVERED
                status_reason = f"Covered for {int(days_to_stockout)} days — beyond 2 boat cycles"

            # Calculate weekly sales for compatibility
            weekly_sales = coverage.velocity_m2_day * 7

            result = ProductStockout(
                product_id=metrics.product_id,
                sku=metrics.sku,
                category=metrics.category,
                rotation=None,  # Not in metrics, keep for backwards compatibility
                active=metrics.active,
                warehouse_qty=coverage.warehouse_m2,
                in_transit_qty=coverage.in_transit_m2,
                total_qty=coverage.warehouse_m2 + coverage.in_transit_m2,
                factory_available_m2=coverage.factory_available_m2,
                factory_lot_count=coverage.factory_lot_count,
                avg_daily_sales=coverage.velocity_m2_day,
                weekly_sales=round(weekly_sales, 2),
                weeks_of_data=metrics.sample_count,
                days_to_stockout=days_to_stockout,
                stockout_date=stockout_date,
                status=status,
                status_reason=status_reason
            )
            results.append(result)

        # Sort by priority severity, then alphabetically by SKU
        status_order = {
            StockoutStatus.HIGH_PRIORITY: 0,
            StockoutStatus.CONSIDER: 1,
            StockoutStatus.WELL_COVERED: 2,
            StockoutStatus.YOUR_CALL: 3,
        }
        results.sort(key=lambda r: (status_order[r.status], r.sku))

        # Count by status
        high_priority = sum(1 for r in results if r.status == StockoutStatus.HIGH_PRIORITY)
        consider = sum(1 for r in results if r.status == StockoutStatus.CONSIDER)
        well_covered = sum(1 for r in results if r.status == StockoutStatus.WELL_COVERED)
        your_call = sum(1 for r in results if r.status == StockoutStatus.YOUR_CALL)

        summary = StockoutSummary(
            total_products=len(results),
            high_priority_count=high_priority,
            consider_count=consider,
            well_covered_count=well_covered,
            your_call_count=your_call,
            # Departure info (for user-facing display)
            next_boat_departure=next_departure,
            second_boat_departure=second_departure,
            days_to_next_boat_departure=days_to_next_departure,
            days_to_second_boat_departure=days_to_second_departure,
            # Arrival info (for internal use / backwards compatibility)
            next_boat_arrival=next_arrival,
            second_boat_arrival=second_arrival,
            days_to_next_boat=days_to_next if next_arrival else None,
            days_to_second_boat=days_to_second if second_arrival else None,
            products=results
        )

        logger.info(
            "stockout_calculation_complete",
            total=len(results),
            high_priority=high_priority,
            consider=consider,
            well_covered=well_covered,
            your_call=your_call
        )

        return summary

    def calculate_for_product(self, product_id: str) -> ProductStockout:
        """
        Calculate stockout status for a single product.

        Args:
            product_id: Product UUID

        Returns:
            ProductStockout with calculation result
        """
        logger.debug("calculating_stockout", product_id=product_id)

        # Get boat thresholds
        days_to_next, days_to_second = self._get_boat_thresholds()

        # Get product info
        product = self.product_service.get_by_id(product_id)

        # Get latest inventory
        history = self.inventory_service.get_history(product_id, limit=1)
        inventory = history[0] if history else None

        return self._calculate_for_product(
            product_id=product_id,
            sku=product.sku,
            category=product.category.value if product.category else None,
            rotation=product.rotation.value if product.rotation else None,
            inventory=inventory,
            days_to_next_boat=days_to_next,
            days_to_second_boat=days_to_second,
        )

    def _calculate_for_product(
        self,
        product_id: str,
        sku: str,
        category: Optional[str],
        rotation: Optional[str],
        inventory,
        days_to_next_boat: int,
        days_to_second_boat: int,
    ) -> ProductStockout:
        """
        Internal calculation for a single product (fetches sales).

        Used by calculate_for_product() for single-product lookups.
        For batch operations, use _calculate_with_sales() instead.
        """
        # Get recent sales (12 weeks) for velocity calculation
        recent_sales_records = self.sales_service.get_history(
            product_id,
            limit=self.sales_weeks
        )
        # Get historical sales (52 weeks) for YOUR_CALL detection
        historical_sales_records = self.sales_service.get_history(
            product_id,
            limit=settings.historical_window_weeks
        )
        return self._calculate_with_sales(
            product_id, sku, category, rotation, inventory,
            recent_sales_records, historical_sales_records,
            days_to_next_boat, days_to_second_boat
        )

    def _calculate_with_sales(
        self,
        product_id: str,
        sku: str,
        category: Optional[str],
        rotation: Optional[str],
        inventory,
        recent_sales_records: list,
        historical_sales_records: list,
        days_to_next_boat: int,
        days_to_second_boat: int,
    ) -> ProductStockout:
        """
        Internal calculation with pre-fetched sales data.

        Uses boat arrival-based classification:
        - HIGH_PRIORITY: stockout before next boat arrives
        - CONSIDER: stockout before second boat arrives
        - WELL_COVERED: won't stock out for 2+ boat cycles
        - YOUR_CALL: no data / needs manual review

        Args:
            product_id: Product UUID
            sku: Product SKU
            category: Product category
            rotation: Product rotation
            inventory: Latest inventory snapshot (or None)
            recent_sales_records: Sales records from last 12 weeks
            historical_sales_records: Sales records from last 52 weeks
            days_to_next_boat: Days until next boat arrives
            days_to_second_boat: Days until second boat arrives

        Returns:
            ProductStockout with calculation result
        """
        # Get inventory quantities
        if inventory:
            warehouse_qty = Decimal(str(inventory.warehouse_qty))
            in_transit_qty = Decimal(str(inventory.in_transit_qty))
        else:
            warehouse_qty = Decimal("0")
            in_transit_qty = Decimal("0")

        total_qty = warehouse_qty + in_transit_qty

        # Check recent sales (12 weeks)
        recent_weeks = len(recent_sales_records)
        historical_weeks = len(historical_sales_records)

        # CASE 1: No recent sales in 12 weeks → YOUR_CALL
        if recent_weeks == 0 or sum(Decimal(str(r.quantity_m2)) for r in recent_sales_records) == 0:
            # Check if there's historical sales (52 weeks)
            if historical_weeks > 0 and sum(Decimal(str(r.quantity_m2)) for r in historical_sales_records) > 0:
                reason = "No sales in last 12 weeks — has older history, needs review"
            else:
                reason = "No sales history — needs manual review"

            return ProductStockout(
                product_id=product_id,
                sku=sku,
                category=category,
                rotation=rotation,
                warehouse_qty=warehouse_qty,
                in_transit_qty=in_transit_qty,
                total_qty=total_qty,
                avg_daily_sales=Decimal("0"),
                weekly_sales=Decimal("0"),
                weeks_of_data=0,
                days_to_stockout=None,
                stockout_date=None,
                status=StockoutStatus.YOUR_CALL,
                status_reason=reason
            )

        # CASE 2: Has recent sales - calculate velocity
        total_recent_sales = sum(
            Decimal(str(r.quantity_m2))
            for r in recent_sales_records
        )

        weekly_sales = total_recent_sales / recent_weeks
        days_in_period = recent_weeks * 7
        avg_daily_sales = total_recent_sales / days_in_period

        # Calculate days to stockout (if avg_daily_sales > 0)
        if avg_daily_sales > 0:
            days_to_stockout = total_qty / avg_daily_sales
            stockout_date = date.today() + timedelta(days=int(days_to_stockout))
        else:
            days_to_stockout = None
            stockout_date = None

        # Determine status based on boat arrivals
        if days_to_stockout is None:
            # Edge case: can't calculate stockout
            status = StockoutStatus.YOUR_CALL
            status_reason = "Unable to calculate stockout — needs manual review"
        elif days_to_stockout < days_to_next_boat:
            # Will stock out BEFORE next boat arrives
            status = StockoutStatus.HIGH_PRIORITY
            status_reason = f"Stockout in {int(days_to_stockout)} days — before next boat arrives ({days_to_next_boat} days)"
        elif days_to_stockout < days_to_second_boat:
            # Will stock out BEFORE second boat arrives
            status = StockoutStatus.CONSIDER
            status_reason = f"Stockout in {int(days_to_stockout)} days — before second boat ({days_to_second_boat} days)"
        else:
            # Won't stock out for 2+ boat cycles
            status = StockoutStatus.WELL_COVERED
            status_reason = f"Covered for {int(days_to_stockout)} days — beyond 2 boat cycles"

        return ProductStockout(
            product_id=product_id,
            sku=sku,
            category=category,
            rotation=rotation,
            warehouse_qty=warehouse_qty,
            in_transit_qty=in_transit_qty,
            total_qty=total_qty,
            avg_daily_sales=round(avg_daily_sales, 2),
            weekly_sales=round(weekly_sales, 2),
            weeks_of_data=recent_weeks,
            days_to_stockout=round(days_to_stockout, 1) if days_to_stockout else None,
            stockout_date=stockout_date,
            status=status,
            status_reason=status_reason
        )

    def get_high_priority_products(self) -> list[ProductStockout]:
        """Get all products with HIGH_PRIORITY status."""
        summary = self.calculate_all()
        return [p for p in summary.products if p.status == StockoutStatus.HIGH_PRIORITY]

    def get_consider_products(self) -> list[ProductStockout]:
        """Get all products with CONSIDER status."""
        summary = self.calculate_all()
        return [p for p in summary.products if p.status == StockoutStatus.CONSIDER]

    def get_products_by_status(
        self,
        status: StockoutStatus
    ) -> list[ProductStockout]:
        """Get all products with a specific status."""
        summary = self.calculate_all()
        return [p for p in summary.products if p.status == status]


# Singleton instance
_stockout_service: Optional[StockoutService] = None


def get_stockout_service() -> StockoutService:
    """Get or create StockoutService instance."""
    global _stockout_service
    if _stockout_service is None:
        _stockout_service = StockoutService()
    return _stockout_service
