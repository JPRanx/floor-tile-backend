"""Liquidation and clearance identification."""

from typing import Optional
from decimal import Decimal
from datetime import date
import math
import structlog

from models.order_builder import LiquidationCandidate, LiquidationClearanceProduct
from config.shipping import (
    M2_PER_PALLET,
    LIQUIDATION_DECLINING_TREND_PCT_MAX,
    LIQUIDATION_DECLINING_DAYS_MIN,
    LIQUIDATION_EXTREME_DAYS_MIN,
)

logger = structlog.get_logger(__name__)


class LiquidationMixin:
    """Liquidation clearance and slow-mover identification."""

    def _get_days_since_last_sale(self, product_id: str) -> Optional[int]:
        """Days since last sale for a product."""
        sales_result = (
            self.inventory_service.db.table("sales")
            .select("week_start")
            .eq("product_id", product_id)
            .order("week_start", desc=True)
            .limit(1)
            .execute()
        )
        if sales_result.data:
            last_sale = date.fromisoformat(sales_result.data[0]["week_start"])
            return (date.today() - last_sale).days
        return None

    def _get_liquidation_clearance(self) -> list[LiquidationClearanceProduct]:
        """
        Find deactivated products with SIESA factory stock.
        These should be ordered to Guatemala to sell off.
        """
        # 1. Query inactive products
        products_result = (
            self.inventory_service.db.table("products")
            .select("id, sku, inactive_reason, inactive_date")
            .eq("active", False)
            .execute()
        )
        if not products_result.data:
            return []

        # 2. Get latest inventory from inventory_current view
        product_ids = [p["id"] for p in products_result.data]
        inventory_result = (
            self.inventory_service.db.table("inventory_current")
            .select("product_id, factory_available_m2, factory_lot_count, warehouse_qty, snapshot_date")
            .in_("product_id", product_ids)
            .execute()
        )

        # Build dict — view already returns one row per product, no dedup needed
        latest_inv = {}
        seen = set()
        for inv in (inventory_result.data or []):
            pid = inv["product_id"]
            if pid not in seen:
                seen.add(pid)
                latest_inv[pid] = inv

        # 3. Filter to factory_available_m2 > 0
        candidates = []
        for product in products_result.data:
            inv = latest_inv.get(product["id"])
            if not inv:
                continue
            factory_m2 = Decimal(str(inv.get("factory_available_m2") or 0))
            if factory_m2 <= 0:
                continue

            warehouse_m2 = Decimal(str(inv.get("warehouse_qty") or 0))
            suggested_pallets = math.ceil(float(factory_m2 / M2_PER_PALLET))

            # Get days since last sale
            days_since_last_sale = self._get_days_since_last_sale(product["id"])

            candidates.append(LiquidationClearanceProduct(
                product_id=product["id"],
                sku=product["sku"],
                description=product.get("description"),
                factory_available_m2=factory_m2,
                factory_lot_count=int(inv.get("factory_lot_count") or 0),
                warehouse_m2=warehouse_m2,
                suggested_pallets=suggested_pallets,
                suggested_m2=factory_m2,
                days_since_last_sale=days_since_last_sale,
                inactive_reason=product.get("inactive_reason"),
                inactive_date=product.get("inactive_date"),
            ))

        # Sort by factory stock DESC (biggest clearance opportunity first)
        candidates.sort(key=lambda c: c.factory_available_m2, reverse=True)

        logger.info(
            "liquidation_clearance_found",
            count=len(candidates),
        )

        return candidates

    def _identify_liquidation_candidates(
        self,
        trend_data: dict[str, dict],
        inventory_snapshots: Optional[list] = None,
    ) -> list[LiquidationCandidate]:
        """
        Find slow movers that could be cleared to make room for fast movers.

        Uses LIQUIDATION_THRESHOLDS from config to identify:
        - declining_overstocked: Declining trend + high inventory
        - no_sales: No recent sales
        - extreme_overstock: Very high stock regardless of trend
        """
        candidates = []

        # Get inventory data (use cached snapshot if provided)
        if inventory_snapshots is None:
            inventory_snapshots = self.inventory_service.get_latest()

        for inv in inventory_snapshots:
            warehouse_m2 = Decimal(str(inv.warehouse_qty or 0))

            # Skip products with no warehouse stock
            if warehouse_m2 <= 0:
                continue

            product_id = inv.product_id
            sku = inv.sku if hasattr(inv, 'sku') else None

            # Get trend data for this product
            trend = trend_data.get(sku, {}) if sku else {}
            days_of_stock = trend.get("days_of_stock")
            trend_pct = Decimal(str(trend.get("velocity_change_pct", 0)))
            direction = trend.get("direction", "stable")
            daily_velocity_m2 = Decimal(str(trend.get("daily_velocity_m2", 0)))

            reason = None
            reason_display = ""

            # Check: Declining + Overstocked
            if (trend_pct <= LIQUIDATION_DECLINING_TREND_PCT_MAX and
                days_of_stock is not None and
                days_of_stock >= LIQUIDATION_DECLINING_DAYS_MIN):
                reason = "declining_overstocked"
                reason_display = f"Declining {trend_pct:+.0f}%, {days_of_stock} days stock"

            # Check: No sales in 90 days (days_of_stock is None or extremely high)
            elif days_of_stock is None or days_of_stock >= 365:
                reason = "no_sales"
                reason_display = "No sales in 90+ days"

            # Check: Extreme overstock (any trend)
            elif days_of_stock is not None and days_of_stock >= LIQUIDATION_EXTREME_DAYS_MIN:
                reason = "extreme_overstock"
                reason_display = f"{days_of_stock} days of stock"

            if reason:
                current_pallets = math.ceil(float(warehouse_m2 / M2_PER_PALLET))

                candidates.append(LiquidationCandidate(
                    product_id=product_id,
                    sku=sku or product_id,
                    description=None,
                    current_m2=warehouse_m2,
                    current_pallets=current_pallets,
                    days_of_stock=days_of_stock,
                    trend_direction=direction,
                    trend_pct=trend_pct,
                    daily_velocity_m2=daily_velocity_m2,
                    reason=reason,
                    reason_display=reason_display,
                    potential_space_freed_m2=warehouse_m2,
                    potential_space_freed_pallets=current_pallets,
                ))

        # Sort by most clearable (highest stock first, then most declining)
        candidates.sort(key=lambda c: (-c.current_pallets, float(c.trend_pct)))

        logger.debug(
            "liquidation_candidates_identified",
            count=len(candidates),
            total_pallets=sum(c.current_pallets for c in candidates),
        )

        return candidates
