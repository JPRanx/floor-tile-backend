"""
Demand Intelligence — Single source of truth for trend factors,
customer demand scoring, and urgency classification.

Both Forward Simulation and Order Builder import from here.
No other module should compute these independently.
"""

from decimal import Decimal
from datetime import date
from typing import Optional

import structlog

from config.shipping import SEASONAL_DAMPENING

logger = structlog.get_logger(__name__)

# Trend thresholds
_GROWING_THRESHOLD = Decimal("1.20")    # 90d > 180d by 20%+
_DECLINING_THRESHOLD = Decimal("0.80")  # 90d < 180d by 20%+

# Urgency thresholds in days of stock
URGENCY_CRITICAL_DAYS = 7
URGENCY_URGENT_DAYS = 14
URGENCY_SOON_DAYS = 30

# Customer demand tier weights and overdue multipliers
_TIER_WEIGHTS = {"A": 100, "B": 50, "C": 25}
_OVERDUE_MULTIPLIERS = [
    (14, 1.0),    # due soon
    (30, 1.5),    # moderately overdue
    (60, 2.0),    # significantly overdue
    (999, 2.5),   # severely overdue
]


def _calculate_trend_factor(direction: str, strength: str) -> Decimal:
    """Return a multiplier for trend-adjusted demand.

    up/strong → 1.20, up/moderate → 1.10, up/weak → 1.05,
    down/strong → 0.80, down/moderate → 0.90, down/weak → 0.95,
    stable → 1.0.
    """
    if direction == "up":
        return {"strong": Decimal("1.20"), "moderate": Decimal("1.10"),
                "weak": Decimal("1.05")}.get(strength, Decimal("1.0"))
    if direction == "down":
        return {"strong": Decimal("0.80"), "moderate": Decimal("0.90"),
                "weak": Decimal("0.95")}.get(strength, Decimal("1.0"))
    return Decimal("1.0")


def classify_urgency(days_of_stock: Optional[int]) -> str:
    """Classify urgency based on days of stock.

    Returns lowercase strings: "critical", "urgent", "soon", "ok".
    Used by both OB and FS for consistent thresholds.
    """
    if days_of_stock is None:
        return "ok"
    if days_of_stock < URGENCY_CRITICAL_DAYS:
        return "critical"
    if days_of_stock < URGENCY_URGENT_DAYS:
        return "urgent"
    if days_of_stock < URGENCY_SOON_DAYS:
        return "soon"
    return "ok"


def compute_trend_factors(trend_service, products: list[dict], prefetched_metrics=None) -> dict[str, dict]:
    """Compute trend direction/strength/factor per product.

    Uses dual-velocity comparison (90d vs 180d) with seasonal dampening.
    Returns keyed by product_id OR by SKU depending on caller needs.

    Returns ``{key: {"direction": str, "strength": str, "factor": Decimal,
    "velocity_90d_m2": Decimal, "velocity_180d_m2": Decimal,
    "velocity_trend_signal": str, "velocity_trend_ratio": Decimal,
    "velocity_trend_ratio_raw": Decimal,
    "velocity_change_pct": Decimal, "days_of_stock": ...,
    "confidence": str, "daily_velocity_m2": Decimal}}``.

    Args:
        trend_service: TrendService instance
        products: List of product dicts with "id" and "sku" keys.
                  If empty, returns keyed by SKU only (for OB compatibility).
        prefetched_metrics: Optional pre-fetched MetricsService results.
                           Avoids redundant DB queries when called from OB.
    """
    try:
        trends_90d = trend_service.get_product_trends(
            period_days=90, comparison_period_days=90, limit=200,
            prefetched_metrics=prefetched_metrics,
        )
        trends_180d = trend_service.get_product_trends(
            period_days=180, comparison_period_days=180, limit=200,
            prefetched_metrics=prefetched_metrics,
        )

        velocity_180d_by_sku = {
            t.sku: t.current_velocity_m2_day for t in trends_180d
        }

        sku_to_pid = {p["sku"]: p["id"] for p in products} if products else {}

        current_month = date.today().month
        seasonal_factor = SEASONAL_DAMPENING.get(current_month, 1.0)

        result: dict[str, dict] = {}
        for t in trends_90d:
            velocity_90d = t.current_velocity_m2_day
            velocity_180d = velocity_180d_by_sku.get(t.sku, Decimal("0"))

            # Dual-velocity comparison with seasonal dampening
            if velocity_180d > 0:
                trend_ratio_raw = velocity_90d / velocity_180d
                trend_ratio = Decimal("1.0") + (
                    trend_ratio_raw - Decimal("1.0")
                ) * Decimal(str(seasonal_factor))
            else:
                trend_ratio_raw = Decimal("1.0")
                trend_ratio = Decimal("1.0")

            # Direction from 90d trend object
            direction = (
                t.direction.value
                if hasattr(t.direction, "value")
                else str(t.direction)
            )
            strength = (
                t.strength.value
                if hasattr(t.strength, "value")
                else str(t.strength)
            )

            # Override direction if dampened ratio disagrees
            if velocity_180d > 0:
                if trend_ratio >= _GROWING_THRESHOLD:
                    trend_signal = "growing"
                elif trend_ratio <= _DECLINING_THRESHOLD:
                    trend_signal = "declining"
                else:
                    trend_signal = "stable"
            else:
                trend_signal = "growing" if velocity_90d > 0 else "stable"

            # For FS: override direction to match dampened signal
            dampened_direction = direction
            if velocity_180d > 0:
                if trend_ratio >= _GROWING_THRESHOLD:
                    dampened_direction = "up"
                elif trend_ratio <= _DECLINING_THRESHOLD:
                    dampened_direction = "down"
                else:
                    dampened_direction = "stable"

            factor = _calculate_trend_factor(dampened_direction, strength)

            confidence = (
                t.confidence.value
                if hasattr(t.confidence, "value")
                else str(t.confidence)
            )

            entry = {
                "direction": direction,
                "strength": strength,
                "factor": factor,
                "velocity_90d_m2": velocity_90d,
                "velocity_180d_m2": velocity_180d,
                "velocity_trend_signal": trend_signal,
                "velocity_trend_ratio": round(trend_ratio, 2),
                "velocity_trend_ratio_raw": round(trend_ratio_raw, 2),
                "velocity_change_pct": t.velocity_change_pct,
                "days_of_stock": t.days_of_stock,
                "confidence": confidence,
                "daily_velocity_m2": velocity_90d,
                "dampened_direction": dampened_direction,
            }

            # Key by SKU (for OB) and optionally by product_id (for FS)
            result[t.sku] = entry
            pid = sku_to_pid.get(t.sku)
            if pid:
                result[pid] = entry

        return result

    except Exception as e:
        logger.warning("compute_trend_factors_failed", error=str(e))
        return {}


def compute_customer_demand(trend_service, products: list[dict]) -> dict[str, dict]:
    """Compute customer demand scores and expected orders per product.

    Returns keyed by both SKU and product_id.

    Returns ``{key: {"score": int, "customers_count": int,
    "expected_m2": Decimal, "customer_names": list[str]}}``.
    """
    try:
        customer_trends = trend_service.get_customer_trends(
            period_days=90, comparison_period_days=90, limit=100,
        )

        sku_to_pid = {p["sku"]: p["id"] for p in products} if products else {}

        sku_demand: dict[str, dict] = {}

        for customer in customer_trends:
            if not customer.avg_days_between_orders or customer.order_count < 2:
                continue
            if customer.days_overdue < -14:
                continue

            days_overdue = customer.days_overdue
            overdue_mult = 2.5  # default for 60+
            for threshold, mult in _OVERDUE_MULTIPLIERS:
                if days_overdue <= threshold:
                    overdue_mult = mult
                    break

            tier = (
                customer.tier.value
                if hasattr(customer.tier, "value")
                else str(customer.tier)
            )
            customer_score = int(_TIER_WEIGHTS.get(tier, 25) * overdue_mult)

            for prod in customer.top_products[:5]:
                sku = prod.sku
                if sku not in sku_demand:
                    sku_demand[sku] = {
                        "score": 0,
                        "customers": set(),
                        "expected_m2": Decimal("0"),
                    }
                sku_demand[sku]["score"] += customer_score
                sku_demand[sku]["customers"].add(customer.customer_normalized)
                if customer.order_count > 0 and prod.total_m2:
                    avg_m2 = Decimal(str(prod.total_m2)) / customer.order_count
                    sku_demand[sku]["expected_m2"] += avg_m2

        # Build result keyed by both SKU and product_id
        result: dict[str, dict] = {}
        for sku, data in sku_demand.items():
            entry = {
                "score": data["score"],
                "customers_count": len(data["customers"]),
                "expected_m2": round(data["expected_m2"], 2),
                "customer_names": list(data["customers"])[:5],
            }
            result[sku] = entry
            pid = sku_to_pid.get(sku)
            if pid:
                result[pid] = entry

        return result

    except Exception as e:
        logger.warning("compute_customer_demand_failed", error=str(e))
        return {}
