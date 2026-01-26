"""
MetricsService — Single source of truth for all business metrics.

All pages call this service. Same input → Same output → Everywhere.

USAGE BY PAGE:

Dashboard:
  → Display coverage.warehouse_days (the urgent number)
  → Alert if coverage.has_gap = True

Intelligence:
  → Display coverage.warehouse_days AND coverage.with_transit_days
  → Show coverage.gap_days if coverage.has_gap

Order Builder:
  → Display coverage.warehouse_days (current reality)
  → Show gap if exists
  → Calculate additional coverage from order using coverage.velocity_m2_day
"""

from typing import Dict, List, Optional
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, timedelta
from collections import defaultdict
import structlog

from config import get_supabase_client
from models.metrics import StockCoverage, ProductMetrics, CategoryMetrics, CategoryInsight
from models.product import TILE_CATEGORIES

logger = structlog.get_logger(__name__)

# Configuration — Default periods (can be overridden via parameter)
DEFAULT_VELOCITY_PERIOD_DAYS = 90
DEFAULT_COMPARISON_PERIOD_DAYS = 90


def round_decimal(value: Decimal, places: int = 2) -> Decimal:
    """Round Decimal to specified decimal places."""
    return value.quantize(Decimal(f"0.{'0' * places}"), rounding=ROUND_HALF_UP)


class MetricsService:
    """Single source of truth for all business metrics."""

    def __init__(self):
        self.db = get_supabase_client()

    def get_all_product_metrics(
        self,
        period_days: int = DEFAULT_VELOCITY_PERIOD_DAYS,
        next_boat_arrival_days: Optional[int] = None
    ) -> List[ProductMetrics]:
        """
        Calculate all metrics for all products in a single batch.

        This is the main entry point. Other methods should call this
        or use cached results from this.

        Args:
            period_days: Number of days for velocity calculation (default 90)
            next_boat_arrival_days: Days until next boat arrives (for gap analysis)

        Returns:
            List of ProductMetrics for all active products
        """
        logger.info("calculating_all_product_metrics", period_days=period_days)

        # === 1. FETCH ALL DATA IN BATCH ===

        # Products (tiles only - excludes FURNITURE, SINK, SURCHARGE)
        tile_categories = [cat.value for cat in TILE_CATEGORIES]
        products_result = self.db.table("products").select(
            "id, sku, category"
        ).eq("active", True).in_("category", tile_categories).execute()
        products_by_id = {p["id"]: p for p in products_result.data}

        # Inventory (from canonical source: inventory_snapshots)
        inventory_result = self.db.table("inventory_snapshots").select(
            "product_id, warehouse_qty, in_transit_qty, snapshot_date"
        ).order("snapshot_date", desc=True).execute()

        # Get latest inventory per product
        inventory_by_product: Dict[str, dict] = {}
        for snap in inventory_result.data:
            pid = snap.get("product_id")
            if pid and pid not in inventory_by_product:
                inventory_by_product[pid] = snap

        # Sales (fetch period + comparison period)
        today = date.today()
        comparison_days = period_days  # Compare against same-length previous period
        sales_start = today - timedelta(days=period_days + comparison_days)
        current_start = today - timedelta(days=period_days)

        sales_result = self.db.table("sales").select(
            "product_id, week_start, quantity_m2"
        ).gte("week_start", sales_start.isoformat()).execute()

        # Aggregate sales by product and period
        current_sales: Dict[str, Decimal] = defaultdict(Decimal)
        previous_sales: Dict[str, Decimal] = defaultdict(Decimal)
        sample_counts: Dict[str, int] = defaultdict(int)

        for sale in sales_result.data:
            pid = sale.get("product_id")
            week_str = sale.get("week_start")
            qty = Decimal(str(sale.get("quantity_m2") or 0))

            if not pid or not week_str:
                continue

            try:
                week_date = date.fromisoformat(week_str[:10])
            except (ValueError, TypeError):
                continue

            if week_date >= current_start:
                current_sales[pid] += qty
                sample_counts[pid] += 1
            elif week_date >= sales_start:
                previous_sales[pid] += qty

        # === 2. CALCULATE METRICS FOR EACH PRODUCT ===

        results = []
        for pid, product in products_by_id.items():
            inv = inventory_by_product.get(pid, {})

            warehouse_m2 = Decimal(str(inv.get("warehouse_qty") or 0))
            in_transit_m2 = Decimal(str(inv.get("in_transit_qty") or 0))

            # Velocity (based on period_days, 2 decimal precision)
            total_current = current_sales.get(pid, Decimal("0"))
            velocity = total_current / Decimal(str(period_days)) if period_days > 0 else Decimal("0")
            velocity = round_decimal(velocity, 2)

            # Trend calculation
            total_previous = previous_sales.get(pid, Decimal("0"))
            if total_previous > 0:
                velocity_change_pct = ((total_current - total_previous) / total_previous) * 100
            elif total_current > 0:
                velocity_change_pct = Decimal("100")
            else:
                velocity_change_pct = Decimal("0")
            velocity_change_pct = round_decimal(velocity_change_pct, 2)

            direction, strength = self._classify_trend(velocity_change_pct)

            # Coverage calculations (2 decimal precision)
            warehouse_days: Optional[Decimal] = None
            with_transit_days: Optional[Decimal] = None
            stockout_date: Optional[date] = None

            if velocity > 0:
                warehouse_days = round_decimal(warehouse_m2 / velocity, 2)
                with_transit_days = round_decimal((warehouse_m2 + in_transit_m2) / velocity, 2)
                stockout_date = today + timedelta(days=int(warehouse_days))

            # Gap analysis (2 decimal precision)
            has_gap = False
            gap_days: Optional[Decimal] = None
            days_until_boat_stockout: Optional[Decimal] = None
            next_boat_decimal: Optional[Decimal] = None

            if next_boat_arrival_days is not None:
                next_boat_decimal = Decimal(str(next_boat_arrival_days))
                if warehouse_days is not None:
                    days_until_boat_stockout = round_decimal(warehouse_days - next_boat_decimal, 2)
                    if days_until_boat_stockout < 0:
                        has_gap = True
                        gap_days = round_decimal(abs(days_until_boat_stockout), 2)

            # Build coverage
            coverage = StockCoverage(
                product_id=pid,
                sku=product.get("sku", ""),
                warehouse_m2=round_decimal(warehouse_m2, 2),
                in_transit_m2=round_decimal(in_transit_m2, 2),
                in_transit_arrival_date=None,  # TODO: Get from shipments
                in_transit_arrival_days=None,
                velocity_m2_day=velocity,
                warehouse_days=warehouse_days,
                with_transit_days=with_transit_days,
                gap_days=gap_days,
                has_gap=has_gap,
                stockout_date=stockout_date,
                next_boat_arrival_days=next_boat_decimal,
                days_until_boat_stockout=days_until_boat_stockout,
            )

            # Confidence based on sample count
            count = sample_counts.get(pid, 0)
            confidence = "HIGH" if count >= 8 else "MEDIUM" if count >= 4 else "LOW"

            results.append(ProductMetrics(
                product_id=pid,
                sku=product.get("sku", ""),
                category=product.get("category"),
                coverage=coverage,
                velocity_change_pct=velocity_change_pct,
                trend_direction=direction,
                trend_strength=strength,
                confidence=confidence,
                sample_count=count,
            ))

        logger.info("product_metrics_calculated", count=len(results))
        return results

    def get_product_metrics(
        self,
        product_id: str,
        period_days: int = DEFAULT_VELOCITY_PERIOD_DAYS,
        next_boat_arrival_days: Optional[int] = None
    ) -> Optional[ProductMetrics]:
        """Get metrics for a single product.

        This calls get_all_product_metrics and filters.
        For bulk operations, call get_all_product_metrics directly.
        """
        all_metrics = self.get_all_product_metrics(period_days, next_boat_arrival_days)
        return next((m for m in all_metrics if m.product_id == product_id), None)

    def _classify_trend(self, change_pct: Decimal) -> tuple[str, str]:
        """Classify trend direction and strength.

        Returns:
            Tuple of (direction, strength) where:
            - direction: "UP", "DOWN", or "STABLE"
            - strength: "STRONG", "MODERATE", or "WEAK"
        """
        abs_change = abs(change_pct)

        if abs_change < 5:
            return "STABLE", "WEAK"
        elif abs_change < 20:
            strength = "MODERATE"
        else:
            strength = "STRONG"

        direction = "UP" if change_pct > 0 else "DOWN"
        return direction, strength

    def get_category_metrics(
        self,
        period_days: int = DEFAULT_VELOCITY_PERIOD_DAYS,
    ) -> List[CategoryMetrics]:
        """
        Get aggregated metrics by category.

        Only includes tile categories (MADERAS, EXTERIORES, MARMOLIZADOS, OTHER).
        Excludes FURNITURE, SINK, SURCHARGE.

        Args:
            period_days: Number of days for velocity calculation

        Returns:
            List of CategoryMetrics, one per tile category
        """
        logger.info("calculating_category_metrics", period_days=period_days)

        # Get all product metrics
        all_metrics = self.get_all_product_metrics(period_days=period_days)

        # Filter to tile categories only
        tile_category_names = {cat.value for cat in TILE_CATEGORIES}
        tile_metrics = [m for m in all_metrics if m.category in tile_category_names]

        # Calculate total warehouse m² for percentage calculation
        total_warehouse_m2 = sum(m.coverage.warehouse_m2 for m in tile_metrics)

        # Group by category
        by_category: Dict[str, List[ProductMetrics]] = defaultdict(list)
        for m in tile_metrics:
            by_category[m.category or "OTHER"].append(m)

        # Build CategoryMetrics for each category
        results = []
        for cat_name, products in by_category.items():
            # Warehouse composition
            cat_warehouse_m2 = sum(p.coverage.warehouse_m2 for p in products)
            cat_warehouse_pct = (
                round_decimal((cat_warehouse_m2 / total_warehouse_m2) * 100, 2)
                if total_warehouse_m2 > 0 else Decimal("0")
            )

            # Velocity
            total_velocity = sum(p.coverage.velocity_m2_day for p in products)
            avg_velocity = (
                round_decimal(total_velocity / len(products), 2)
                if products else Decimal("0")
            )

            # Trend - weighted average by warehouse m²
            if cat_warehouse_m2 > 0:
                weighted_change = sum(
                    p.velocity_change_pct * p.coverage.warehouse_m2
                    for p in products
                )
                avg_change_pct = round_decimal(weighted_change / cat_warehouse_m2, 2)
            else:
                avg_change_pct = Decimal("0")

            direction, strength = self._classify_trend(avg_change_pct)

            # Coverage
            products_with_days = [
                p for p in products
                if p.coverage.warehouse_days is not None
            ]
            if products_with_days:
                avg_days = sum(p.coverage.warehouse_days for p in products_with_days) / len(products_with_days)
                avg_warehouse_days = round_decimal(avg_days, 2)
            else:
                avg_warehouse_days = None

            # Products at risk (< 30 days of stock)
            products_at_risk = sum(
                1 for p in products
                if p.coverage.warehouse_days is not None and p.coverage.warehouse_days < 30
            )

            results.append(CategoryMetrics(
                category=cat_name,
                warehouse_m2=round_decimal(cat_warehouse_m2, 2),
                warehouse_pct=cat_warehouse_pct,
                product_count=len(products),
                total_velocity_m2_day=round_decimal(total_velocity, 2),
                avg_velocity_m2_day=avg_velocity,
                velocity_change_pct=avg_change_pct,
                trend_direction=direction,
                trend_strength=strength,
                avg_warehouse_days=avg_warehouse_days,
                products_at_risk=products_at_risk,
            ))

        # Sort by warehouse percentage descending
        results.sort(key=lambda c: c.warehouse_pct, reverse=True)

        logger.info("category_metrics_calculated", count=len(results))
        return results

    def get_category_insights(
        self,
        period_days: int = DEFAULT_VELOCITY_PERIOD_DAYS,
    ) -> List[CategoryInsight]:
        """
        Generate actionable insights based on category metrics.

        Looks for:
        - IMBALANCE: High warehouse % but declining trend
        - GROWTH_OPPORTUNITY: Growing trend but low warehouse %
        - RISK: Multiple products at risk in a category

        Returns:
            List of CategoryInsight with recommendations
        """
        logger.info("generating_category_insights")

        categories = self.get_category_metrics(period_days=period_days)
        insights = []

        for cat in categories:
            # IMBALANCE: Declining category occupies significant warehouse space
            if (
                cat.trend_direction == "DOWN"
                and cat.velocity_change_pct <= Decimal("-10")
                and cat.warehouse_pct >= Decimal("20")
            ):
                insights.append(CategoryInsight(
                    category=cat.category,
                    insight_type="IMBALANCE",
                    message=f"{cat.category} is declining {abs(cat.velocity_change_pct):.0f}% but occupies {cat.warehouse_pct:.0f}% of warehouse",
                    severity="WARNING",
                ))

            # GROWTH_OPPORTUNITY: Growing category with low inventory
            if (
                cat.trend_direction == "UP"
                and cat.velocity_change_pct >= Decimal("10")
                and cat.warehouse_pct <= Decimal("25")
            ):
                insights.append(CategoryInsight(
                    category=cat.category,
                    insight_type="GROWTH_OPPORTUNITY",
                    message=f"{cat.category} is growing {cat.velocity_change_pct:.0f}% but only has {cat.warehouse_pct:.0f}% warehouse share",
                    severity="INFO",
                ))

            # RISK: Many products at risk
            if cat.products_at_risk >= 3:
                insights.append(CategoryInsight(
                    category=cat.category,
                    insight_type="RISK",
                    message=f"{cat.category} has {cat.products_at_risk} products with <30 days stock",
                    severity="CRITICAL" if cat.products_at_risk >= 5 else "WARNING",
                ))

            # LOW_COVERAGE: Category average coverage is dangerously low
            if (
                cat.avg_warehouse_days is not None
                and cat.avg_warehouse_days < Decimal("45")
            ):
                insights.append(CategoryInsight(
                    category=cat.category,
                    insight_type="LOW_COVERAGE",
                    message=f"{cat.category} has only {cat.avg_warehouse_days:.0f} days avg coverage",
                    severity="WARNING",
                ))

        logger.info("category_insights_generated", count=len(insights))
        return insights


# Singleton instance
_metrics_service: Optional[MetricsService] = None


def get_metrics_service() -> MetricsService:
    """Get or create MetricsService singleton instance."""
    global _metrics_service
    if _metrics_service is None:
        _metrics_service = MetricsService()
    return _metrics_service
