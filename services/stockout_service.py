"""
Stockout calculation service — Core business logic.

Calculates days until stockout for each product based on
inventory levels and sales velocity.

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
from models.base import BaseSchema

logger = structlog.get_logger(__name__)


class StockoutStatus(str, Enum):
    """Stockout status levels."""
    CRITICAL = "CRITICAL"  # days < lead_time — too late to order
    WARNING = "WARNING"    # lead_time <= days < lead_time + 14 — order now
    OK = "OK"              # days >= lead_time + 14 — covered
    NO_SALES = "NO_SALES"  # No sales history — can't calculate


class ProductStockout(BaseSchema):
    """Stockout calculation result for a single product."""

    product_id: str
    sku: str
    category: Optional[str] = None
    rotation: Optional[str] = None

    # Inventory
    warehouse_qty: Decimal
    in_transit_qty: Decimal
    total_qty: Decimal

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
    critical_count: int
    warning_count: int
    ok_count: int
    no_sales_count: int

    lead_time_days: int
    warning_threshold_days: int

    products: list[ProductStockout]


class StockoutService:
    """
    Stockout calculation business logic.

    Calculates days until stockout for each product using:
    - Latest inventory (warehouse + in_transit)
    - Sales velocity (last 4 weeks average)
    - Lead time (45 days default)
    """

    def __init__(self):
        self.inventory_service = get_inventory_service()
        self.sales_service = get_sales_service()
        self.product_service = get_product_service()

        # Settings
        self.lead_time = settings.lead_time_days  # 45 days
        self.warning_buffer = 14  # Days of buffer for WARNING status
        self.sales_weeks = 4  # Weeks of sales data to average

    @property
    def warning_threshold(self) -> int:
        """Days threshold for WARNING status."""
        return self.lead_time + self.warning_buffer

    def calculate_all(self) -> StockoutSummary:
        """
        Calculate stockout status for all products.

        Returns:
            StockoutSummary with all product calculations
        """
        logger.info("calculating_stockout_all")

        # Get all products
        products, _ = self.product_service.get_all(
            page=1,
            page_size=1000,
            active_only=True
        )

        # Get latest inventory for all products
        inventory_snapshots = self.inventory_service.get_latest()
        inventory_by_product = {
            snap.product_id: snap
            for snap in inventory_snapshots
        }

        # Calculate for each product
        results = []
        for product in products:
            result = self._calculate_for_product(
                product_id=product.id,
                sku=product.sku,
                category=product.category.value if product.category else None,
                rotation=product.rotation.value if product.rotation else None,
                inventory=inventory_by_product.get(product.id)
            )
            results.append(result)

        # Count by status
        critical = sum(1 for r in results if r.status == StockoutStatus.CRITICAL)
        warning = sum(1 for r in results if r.status == StockoutStatus.WARNING)
        ok = sum(1 for r in results if r.status == StockoutStatus.OK)
        no_sales = sum(1 for r in results if r.status == StockoutStatus.NO_SALES)

        summary = StockoutSummary(
            total_products=len(results),
            critical_count=critical,
            warning_count=warning,
            ok_count=ok,
            no_sales_count=no_sales,
            lead_time_days=self.lead_time,
            warning_threshold_days=self.warning_threshold,
            products=results
        )

        logger.info(
            "stockout_calculation_complete",
            total=len(results),
            critical=critical,
            warning=warning,
            ok=ok,
            no_sales=no_sales
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
            inventory=inventory
        )

    def _calculate_for_product(
        self,
        product_id: str,
        sku: str,
        category: Optional[str],
        rotation: Optional[str],
        inventory
    ) -> ProductStockout:
        """
        Internal calculation for a single product.

        Args:
            product_id: Product UUID
            sku: Product SKU
            category: Product category
            rotation: Product rotation
            inventory: Latest inventory snapshot (or None)

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

        # Get sales history (last 4 weeks)
        sales_records = self.sales_service.get_history(
            product_id,
            limit=self.sales_weeks
        )

        # Calculate average daily sales
        weeks_of_data = len(sales_records)

        if weeks_of_data == 0:
            # No sales data — can't calculate
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
                status=StockoutStatus.NO_SALES,
                status_reason="No sales history available"
            )

        # Sum sales over available weeks
        total_sales = sum(
            Decimal(str(r.quantity_m2))
            for r in sales_records
        )

        # Calculate averages
        weekly_sales = total_sales / weeks_of_data
        days_in_period = weeks_of_data * 7
        avg_daily_sales = total_sales / days_in_period

        # Handle zero sales
        if avg_daily_sales == 0:
            return ProductStockout(
                product_id=product_id,
                sku=sku,
                category=category,
                rotation=rotation,
                warehouse_qty=warehouse_qty,
                in_transit_qty=in_transit_qty,
                total_qty=total_qty,
                avg_daily_sales=Decimal("0"),
                weekly_sales=weekly_sales,
                weeks_of_data=weeks_of_data,
                days_to_stockout=None,
                stockout_date=None,
                status=StockoutStatus.NO_SALES,
                status_reason=f"Zero sales over {weeks_of_data} weeks"
            )

        # Calculate days to stockout
        days_to_stockout = total_qty / avg_daily_sales
        stockout_date = date.today() + timedelta(days=int(days_to_stockout))

        # Determine status
        if days_to_stockout < self.lead_time:
            status = StockoutStatus.CRITICAL
            status_reason = f"Only {int(days_to_stockout)} days of stock, less than {self.lead_time} day lead time"
        elif days_to_stockout < self.warning_threshold:
            status = StockoutStatus.WARNING
            status_reason = f"{int(days_to_stockout)} days of stock, within warning threshold"
        else:
            status = StockoutStatus.OK
            status_reason = f"{int(days_to_stockout)} days of stock, above {self.warning_threshold} day threshold"

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
            weeks_of_data=weeks_of_data,
            days_to_stockout=round(days_to_stockout, 1),
            stockout_date=stockout_date,
            status=status,
            status_reason=status_reason
        )

    def get_critical_products(self) -> list[ProductStockout]:
        """Get all products with CRITICAL status."""
        summary = self.calculate_all()
        return [p for p in summary.products if p.status == StockoutStatus.CRITICAL]

    def get_warning_products(self) -> list[ProductStockout]:
        """Get all products with WARNING status."""
        summary = self.calculate_all()
        return [p for p in summary.products if p.status == StockoutStatus.WARNING]

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
