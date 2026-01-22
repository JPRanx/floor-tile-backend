"""
Trend calculation service for the Intelligence system.

Calculates product, country, and customer trends with statistical confidence metrics.
"""

import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import structlog

from config import get_supabase_client
from models.trends import (
    ConfidenceLevel,
    CountryBreakdown,
    CountryTrend,
    CustomerStatus,
    CustomerTier,
    CustomerTrend,
    IntelligenceDashboard,
    ProductMixChange,
    ProductPurchase,
    ProductTrend,
    SparklinePoint,
    TrendDirection,
    TrendStrength,
)

logger = structlog.get_logger(__name__)

# Country inference patterns - Central American focus
COUNTRY_PATTERNS = {
    "GT": {
        "keywords": ["guatemala", "ciudad de guatemala", "quetzaltenango", "escuintla"],
    },
    "HN": {
        "keywords": ["honduras", "tegucigalpa", "san pedro sula"],
    },
    "SV": {
        "keywords": ["el salvador", "san salvador", "santa ana"],
    },
    "NI": {
        "keywords": ["nicaragua", "managua", "leon"],
    },
    "CR": {
        "keywords": ["costa rica", "san jose"],
    },
    "PA": {
        "keywords": ["panama", "ciudad de panama"],
    },
}

# Country names mapping
COUNTRY_NAMES = {
    "GT": "Guatemala",
    "HN": "Honduras",
    "SV": "El Salvador",
    "NI": "Nicaragua",
    "CR": "Costa Rica",
    "PA": "Panamá",
    "OTHER": "Other",
}


def calculate_coefficient_of_variation(values: List[Decimal]) -> Decimal:
    """
    Calculate the coefficient of variation (CV = std_dev / mean).

    Lower CV indicates more consistent sales.
    Returns 0 if mean is 0 or insufficient data.
    """
    if len(values) < 2:
        return Decimal("0")

    mean = sum(values) / len(values)
    if mean == 0:
        return Decimal("0")

    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std_dev = Decimal(str(math.sqrt(float(variance))))

    return round(std_dev / mean, 4)


def determine_confidence_level(
    sample_count: int, cv: Decimal, min_samples: int = 4
) -> ConfidenceLevel:
    """
    Determine confidence level based on sample count and CV.

    - HIGH: 8+ samples and CV < 0.5
    - MEDIUM: 4+ samples and CV < 1.0
    - LOW: everything else
    """
    if sample_count >= 8 and cv < Decimal("0.5"):
        return ConfidenceLevel.HIGH
    elif sample_count >= min_samples and cv < Decimal("1.0"):
        return ConfidenceLevel.MEDIUM
    else:
        return ConfidenceLevel.LOW


def classify_trend(
    change_pct: Decimal, threshold_strong: Decimal = Decimal("20"), threshold_weak: Decimal = Decimal("5")
) -> Tuple[TrendDirection, TrendStrength]:
    """
    Classify trend direction and strength based on percentage change.

    - STRONG: |change| >= 20%
    - MODERATE: 5% <= |change| < 20%
    - WEAK: |change| < 5%
    """
    abs_change = abs(change_pct)

    if abs_change < threshold_weak:
        return TrendDirection.STABLE, TrendStrength.WEAK
    elif abs_change < threshold_strong:
        direction = TrendDirection.UP if change_pct > 0 else TrendDirection.DOWN
        return direction, TrendStrength.MODERATE
    else:
        direction = TrendDirection.UP if change_pct > 0 else TrendDirection.DOWN
        return direction, TrendStrength.STRONG


def infer_country_code(customer_name: str, nit: Optional[str] = None) -> Optional[str]:
    """
    Infer country code from customer name or NIT pattern.

    Returns ISO 2-letter country code. Defaults to GT (Guatemala)
    since this is a Central American business.
    """
    import re

    customer_lower = customer_name.lower()

    # Check keywords first
    for country_code, patterns in COUNTRY_PATTERNS.items():
        keywords = patterns.get("keywords", [])
        for keyword in keywords:
            if keyword in customer_lower:
                return country_code

    # Check NIT patterns if provided
    if nit:
        nit_clean = re.sub(r"[^0-9]", "", nit)

        # El Salvador: 14 digits, often starts with 06
        if len(nit_clean) == 14 and nit_clean.startswith("06"):
            return "SV"

        # Guatemala: 7-9 digits (most common customer base)
        if 7 <= len(nit_clean) <= 9:
            return "GT"

        # Honduras: 13-14 digits (RTN format)
        if 13 <= len(nit_clean) <= 14:
            return "HN"

    # Default to Guatemala for this Central American business
    return "GT"


def generate_sparkline(
    data_points: List[Tuple[date, Decimal]],
    num_buckets: int = 12,
    period_days: int = 90,
) -> List[SparklinePoint]:
    """
    Generate sparkline data by bucketing values into time periods.

    Args:
        data_points: List of (date, value) tuples
        num_buckets: Number of time buckets
        period_days: Total period to cover in days

    Returns:
        List of SparklinePoint with period labels and aggregated values
    """
    if not data_points:
        return []

    end_date = date.today()
    start_date = end_date - timedelta(days=period_days)
    bucket_days = period_days // num_buckets

    # Initialize buckets
    buckets: Dict[int, Decimal] = {i: Decimal("0") for i in range(num_buckets)}

    for dt, value in data_points:
        if dt < start_date:
            continue
        days_from_start = (dt - start_date).days
        bucket_idx = min(days_from_start // bucket_days, num_buckets - 1)
        buckets[bucket_idx] += value

    # Generate sparkline points
    sparkline = []
    for i in range(num_buckets):
        # Label as W1, W2, etc. for weekly buckets
        if bucket_days <= 7:
            label = f"W{i + 1}"
        else:
            # For longer periods, use month abbreviations
            bucket_start = start_date + timedelta(days=i * bucket_days)
            label = bucket_start.strftime("%b")

        sparkline.append(SparklinePoint(period=label, value=buckets[i]))

    return sparkline


class TrendService:
    """Service for calculating product, country, and customer trends."""

    def __init__(self):
        self.db = get_supabase_client()

    # ==================
    # PRODUCT TRENDS
    # ==================

    def get_product_trends(
        self,
        period_days: int = 90,
        comparison_period_days: int = 90,
        limit: int = 50,
    ) -> List[ProductTrend]:
        """
        Calculate trends for all products.

        Args:
            period_days: Current period to analyze
            comparison_period_days: Previous period for comparison
            limit: Maximum products to return

        Returns:
            List of ProductTrend sorted by velocity change
        """
        today = date.today()
        current_start = today - timedelta(days=period_days)
        previous_start = current_start - timedelta(days=comparison_period_days)
        previous_end = current_start - timedelta(days=1)

        logger.info(
            "calculating_product_trends",
            current_period=f"{current_start} to {today}",
            previous_period=f"{previous_start} to {previous_end}",
        )

        # Fetch all sales in both periods
        sales_result = self.db.table("sales").select(
            "product_id, week_start, quantity_m2, total_price_usd"
        ).gte("week_start", previous_start.isoformat()).execute()

        # Fetch products for SKU mapping
        products_result = self.db.table("products").select(
            "id, sku, category"
        ).execute()
        products_by_id = {p["id"]: p for p in products_result.data}

        # Fetch current inventory
        inventory_result = self.db.table("inventory_lots").select(
            "product_id, quantity_m2"
        ).execute()

        # Aggregate inventory by product
        inventory_by_product: Dict[str, Decimal] = defaultdict(Decimal)
        for lot in inventory_result.data:
            pid = lot.get("product_id")
            qty = Decimal(str(lot.get("quantity_m2") or 0))
            inventory_by_product[pid] += qty

        # Aggregate sales by product and period
        current_sales: Dict[str, List[Tuple[date, Decimal]]] = defaultdict(list)
        previous_sales: Dict[str, List[Tuple[date, Decimal]]] = defaultdict(list)
        current_revenue: Dict[str, Decimal] = defaultdict(Decimal)
        previous_revenue: Dict[str, Decimal] = defaultdict(Decimal)

        for sale in sales_result.data:
            pid = sale.get("product_id")
            week_start_str = sale.get("week_start")
            qty = Decimal(str(sale.get("quantity_m2") or 0))
            revenue = Decimal(str(sale.get("total_price_usd") or 0))

            if not week_start_str:
                continue

            # Parse week_start
            try:
                week_start = datetime.fromisoformat(week_start_str.replace("Z", "+00:00")).date()
            except (ValueError, AttributeError):
                try:
                    week_start = datetime.strptime(week_start_str[:10], "%Y-%m-%d").date()
                except (ValueError, AttributeError):
                    continue

            if week_start >= current_start:
                current_sales[pid].append((week_start, qty))
                current_revenue[pid] += revenue
            elif week_start >= previous_start:
                previous_sales[pid].append((week_start, qty))
                previous_revenue[pid] += revenue

        # Calculate trends for each product
        trends = []
        all_products = set(current_sales.keys()) | set(previous_sales.keys())

        for pid in all_products:
            product = products_by_id.get(pid, {})
            sku = product.get("sku", "Unknown")
            category = product.get("category")

            # Calculate volumes
            current_volumes = [v for _, v in current_sales.get(pid, [])]
            previous_volumes = [v for _, v in previous_sales.get(pid, [])]

            total_current = sum(current_volumes) if current_volumes else Decimal("0")
            total_previous = sum(previous_volumes) if previous_volumes else Decimal("0")

            # Calculate velocities (m²/day)
            current_velocity = total_current / period_days if period_days > 0 else Decimal("0")
            previous_velocity = total_previous / comparison_period_days if comparison_period_days > 0 else Decimal("0")

            # Calculate velocity change
            if previous_velocity > 0:
                velocity_change_pct = ((current_velocity - previous_velocity) / previous_velocity) * 100
            elif current_velocity > 0:
                velocity_change_pct = Decimal("100")  # New activity
            else:
                velocity_change_pct = Decimal("0")

            # Calculate statistical metrics
            all_volumes = current_volumes + previous_volumes
            cv = calculate_coefficient_of_variation(all_volumes)
            confidence = determine_confidence_level(len(all_volumes), cv)

            # Classify trend
            direction, strength = classify_trend(velocity_change_pct)

            # Calculate days of stock
            current_stock = inventory_by_product.get(pid, Decimal("0"))
            days_of_stock = None
            if current_velocity > 0:
                days_of_stock = int(current_stock / current_velocity)

            # Generate sparkline
            sparkline_data = current_sales.get(pid, []) + previous_sales.get(pid, [])
            sparkline = generate_sparkline(sparkline_data, num_buckets=12, period_days=period_days + comparison_period_days)

            trends.append(ProductTrend(
                product_id=pid,
                sku=sku,
                category=category,
                current_velocity_m2_day=round(current_velocity, 4),
                previous_velocity_m2_day=round(previous_velocity, 4),
                velocity_change_pct=round(velocity_change_pct, 2),
                total_volume_m2=round(total_current, 2),
                total_revenue_usd=round(current_revenue.get(pid, Decimal("0")), 2),
                direction=direction,
                strength=strength,
                coefficient_of_variation=cv,
                confidence=confidence,
                sample_count=len(all_volumes),
                days_of_stock=days_of_stock,
                current_stock_m2=round(current_stock, 2) if current_stock else None,
                sparkline=sparkline,
            ))

        # Sort by velocity change descending
        trends.sort(key=lambda t: t.velocity_change_pct, reverse=True)

        logger.info("product_trends_calculated", count=len(trends))
        return trends[:limit]

    # ==================
    # COUNTRY TRENDS
    # ==================

    def get_country_trends(
        self,
        period_days: int = 90,
        comparison_period_days: int = 90,
    ) -> CountryTrend:
        """
        Calculate revenue trends by country.

        Infers country from customer name patterns.
        """
        today = date.today()
        current_start = today - timedelta(days=period_days)
        previous_start = current_start - timedelta(days=comparison_period_days)

        logger.info(
            "calculating_country_trends",
            current_period=f"{current_start} to {today}",
        )

        # Fetch sales with customer info
        sales_result = self.db.table("sales").select(
            "customer_normalized, week_start, quantity_m2, total_price_usd"
        ).gte("week_start", previous_start.isoformat()).execute()

        # Aggregate by country
        country_data: Dict[str, Dict] = defaultdict(lambda: {
            "current_revenue": Decimal("0"),
            "current_volume": Decimal("0"),
            "previous_revenue": Decimal("0"),
            "customers": set(),
            "orders": 0,
        })

        for sale in sales_result.data:
            customer = sale.get("customer_normalized", "")
            week_start_str = sale.get("week_start")
            qty = Decimal(str(sale.get("quantity_m2") or 0))
            revenue = Decimal(str(sale.get("total_price_usd") or 0))

            if not week_start_str:
                continue

            # Parse date
            try:
                week_start = datetime.fromisoformat(week_start_str.replace("Z", "+00:00")).date()
            except (ValueError, AttributeError):
                try:
                    week_start = datetime.strptime(week_start_str[:10], "%Y-%m-%d").date()
                except (ValueError, AttributeError):
                    continue

            country_code = infer_country_code(customer)
            if not country_code:
                country_code = "OTHER"

            if week_start >= current_start:
                country_data[country_code]["current_revenue"] += revenue
                country_data[country_code]["current_volume"] += qty
                country_data[country_code]["customers"].add(customer)
                country_data[country_code]["orders"] += 1
            elif week_start >= previous_start:
                country_data[country_code]["previous_revenue"] += revenue

        # Calculate total revenue for share calculation
        total_revenue = sum(d["current_revenue"] for d in country_data.values())
        total_previous = sum(d["previous_revenue"] for d in country_data.values())

        # Build country breakdowns
        countries = []
        for code, data in country_data.items():
            if data["current_revenue"] > 0:
                share = (data["current_revenue"] / total_revenue * 100) if total_revenue > 0 else Decimal("0")
                countries.append(CountryBreakdown(
                    country_code=code,
                    country_name=COUNTRY_NAMES.get(code, code),
                    total_revenue_usd=round(data["current_revenue"], 2),
                    total_volume_m2=round(data["current_volume"], 2),
                    customer_count=len(data["customers"]),
                    order_count=data["orders"],
                    revenue_share_pct=round(share, 2),
                ))

        # Sort by revenue
        countries.sort(key=lambda c: c.total_revenue_usd, reverse=True)

        # Calculate overall trend
        if total_previous > 0:
            revenue_change = ((total_revenue - total_previous) / total_previous) * 100
        elif total_revenue > 0:
            revenue_change = Decimal("100")
        else:
            revenue_change = Decimal("0")

        direction, _ = classify_trend(revenue_change)

        logger.info("country_trends_calculated", countries=len(countries))

        return CountryTrend(
            period_start=current_start,
            period_end=today,
            total_revenue_usd=round(total_revenue, 2),
            countries=countries,
            revenue_change_pct=round(revenue_change, 2),
            direction=direction,
        )

    # ==================
    # CUSTOMER TRENDS
    # ==================

    def get_customer_trends(
        self,
        period_days: int = 90,
        comparison_period_days: int = 90,
        limit: int = 50,
    ) -> List[CustomerTrend]:
        """
        Calculate trends for all customers.

        Includes tier classification, activity status, and product preferences.
        """
        today = date.today()
        current_start = today - timedelta(days=period_days)
        previous_start = current_start - timedelta(days=comparison_period_days)

        logger.info(
            "calculating_customer_trends",
            current_period=f"{current_start} to {today}",
        )

        # Fetch all sales
        sales_result = self.db.table("sales").select(
            "customer_normalized, customer, product_id, week_start, quantity_m2, total_price_usd"
        ).execute()

        # Fetch products for SKU mapping
        products_result = self.db.table("products").select("id, sku").execute()
        products_by_id = {p["id"]: p["sku"] for p in products_result.data}

        # Aggregate by customer
        customer_data: Dict[str, Dict] = defaultdict(lambda: {
            "original_name": None,
            "current_revenue": Decimal("0"),
            "current_volume": Decimal("0"),
            "previous_revenue": Decimal("0"),
            "previous_volume": Decimal("0"),
            "total_revenue": Decimal("0"),
            "total_volume": Decimal("0"),
            "orders": [],
            "products": defaultdict(lambda: {
                "current_m2": Decimal("0"),
                "current_usd": Decimal("0"),
                "previous_m2": Decimal("0"),
                "previous_usd": Decimal("0"),
                "total_m2": Decimal("0"),
                "total_usd": Decimal("0"),
                "count": 0,
                "last_purchase": None,
            }),
        })

        for sale in sales_result.data:
            customer_norm = sale.get("customer_normalized", "")
            customer_orig = sale.get("customer")
            product_id = sale.get("product_id")
            week_start_str = sale.get("week_start")
            qty = Decimal(str(sale.get("quantity_m2") or 0))
            revenue = Decimal(str(sale.get("total_price_usd") or 0))

            if not customer_norm or not week_start_str:
                continue

            # Parse date
            try:
                week_start = datetime.fromisoformat(week_start_str.replace("Z", "+00:00")).date()
            except (ValueError, AttributeError):
                try:
                    week_start = datetime.strptime(week_start_str[:10], "%Y-%m-%d").date()
                except (ValueError, AttributeError):
                    continue

            data = customer_data[customer_norm]
            if customer_orig and not data["original_name"]:
                data["original_name"] = customer_orig

            data["total_revenue"] += revenue
            data["total_volume"] += qty
            data["orders"].append(week_start)

            # Track by period
            if week_start >= current_start:
                data["current_revenue"] += revenue
                data["current_volume"] += qty
            elif week_start >= previous_start:
                data["previous_revenue"] += revenue
                data["previous_volume"] += qty

            # Track product preferences
            if product_id:
                prod_data = data["products"][product_id]
                prod_data["total_m2"] += qty
                prod_data["total_usd"] += revenue
                prod_data["count"] += 1
                if not prod_data["last_purchase"] or week_start > prod_data["last_purchase"]:
                    prod_data["last_purchase"] = week_start

                if week_start >= current_start:
                    prod_data["current_m2"] += qty
                    prod_data["current_usd"] += revenue
                elif week_start >= previous_start:
                    prod_data["previous_m2"] += qty
                    prod_data["previous_usd"] += revenue

        # Calculate tier thresholds (A = top 20%, B = next 30%, C = bottom 50%)
        all_revenues = sorted([d["total_revenue"] for d in customer_data.values()], reverse=True)
        if all_revenues:
            cumulative = Decimal("0")
            total_all = sum(all_revenues)
            tier_a_threshold = None
            tier_b_threshold = None

            for i, rev in enumerate(all_revenues):
                cumulative += rev
                pct = cumulative / total_all * 100 if total_all > 0 else Decimal("0")
                if tier_a_threshold is None and pct >= 20:
                    tier_a_threshold = rev
                if tier_b_threshold is None and pct >= 50:
                    tier_b_threshold = rev
                    break

            tier_a_threshold = tier_a_threshold or Decimal("0")
            tier_b_threshold = tier_b_threshold or Decimal("0")
        else:
            tier_a_threshold = Decimal("0")
            tier_b_threshold = Decimal("0")

        # Build customer trends
        trends = []
        for customer_norm, data in customer_data.items():
            if not data["orders"]:
                continue

            orders = sorted(data["orders"])
            first_purchase = orders[0]
            last_purchase = orders[-1]
            days_since_last = (today - last_purchase).days

            # Determine status
            if days_since_last <= 30:
                status = CustomerStatus.ACTIVE
            elif days_since_last <= 90:
                status = CustomerStatus.COOLING
            else:
                status = CustomerStatus.DORMANT

            # Determine tier
            if data["total_revenue"] >= tier_a_threshold:
                tier = CustomerTier.A
            elif data["total_revenue"] >= tier_b_threshold:
                tier = CustomerTier.B
            else:
                tier = CustomerTier.C

            # Calculate revenue change
            if data["previous_revenue"] > 0:
                revenue_change = ((data["current_revenue"] - data["previous_revenue"]) / data["previous_revenue"]) * 100
            elif data["current_revenue"] > 0:
                revenue_change = Decimal("100")
            else:
                revenue_change = None

            # Calculate average order value
            order_count = len(orders)
            avg_order_value = data["total_revenue"] / order_count if order_count > 0 else Decimal("0")

            # Calculate average days between orders
            if order_count > 1:
                total_days = (orders[-1] - orders[0]).days
                avg_days_between = Decimal(str(total_days / (order_count - 1)))
            else:
                avg_days_between = None

            # Build top products
            top_products = []
            for pid, prod_data in sorted(
                data["products"].items(),
                key=lambda x: x[1]["total_usd"],
                reverse=True
            )[:5]:
                sku = products_by_id.get(pid, "Unknown")
                top_products.append(ProductPurchase(
                    product_id=pid,
                    sku=sku,
                    total_m2=round(prod_data["total_m2"], 2),
                    total_usd=round(prod_data["total_usd"], 2),
                    purchase_count=prod_data["count"],
                    last_purchase=prod_data["last_purchase"] or first_purchase,
                ))

            # Calculate product mix changes
            product_mix_changes = []
            for pid, prod_data in data["products"].items():
                if data["current_revenue"] > 0 and data["previous_revenue"] > 0:
                    current_share = (prod_data["current_usd"] / data["current_revenue"]) * 100
                    previous_share = (prod_data["previous_usd"] / data["previous_revenue"]) * 100
                    change = current_share - previous_share
                    if abs(change) >= 5:  # Only include significant changes
                        sku = products_by_id.get(pid, "Unknown")
                        product_mix_changes.append(ProductMixChange(
                            sku=sku,
                            previous_share_pct=round(previous_share, 2),
                            current_share_pct=round(current_share, 2),
                            change_pct=round(change, 2),
                        ))

            # Sort mix changes by absolute change
            product_mix_changes.sort(key=lambda x: abs(x.change_pct), reverse=True)

            # Classify trend
            if revenue_change is not None:
                direction, _ = classify_trend(revenue_change)
            else:
                direction = TrendDirection.STABLE

            # Calculate confidence based on order count
            if order_count >= 8:
                confidence = ConfidenceLevel.HIGH
            elif order_count >= 4:
                confidence = ConfidenceLevel.MEDIUM
            else:
                confidence = ConfidenceLevel.LOW

            # Generate sparkline
            sparkline_data = [(d, data["total_revenue"] / order_count) for d in orders]
            sparkline = generate_sparkline(sparkline_data, num_buckets=12, period_days=period_days + comparison_period_days)

            # Infer country
            country_code = infer_country_code(customer_norm)

            trends.append(CustomerTrend(
                customer_normalized=customer_norm,
                customer_original=data["original_name"],
                tier=tier,
                status=status,
                country_code=country_code,
                total_revenue_usd=round(data["total_revenue"], 2),
                period_revenue_usd=round(data["current_revenue"], 2),
                revenue_change_pct=round(revenue_change, 2) if revenue_change is not None else None,
                total_volume_m2=round(data["total_volume"], 2),
                period_volume_m2=round(data["current_volume"], 2),
                order_count=order_count,
                avg_order_value_usd=round(avg_order_value, 2),
                first_purchase=first_purchase,
                last_purchase=last_purchase,
                days_since_last_purchase=days_since_last,
                avg_days_between_orders=round(avg_days_between, 2) if avg_days_between else None,
                top_products=top_products[:5],
                product_mix_changes=product_mix_changes[:5],
                direction=direction,
                confidence=confidence,
                sparkline=sparkline,
            ))

        # Sort by total revenue
        trends.sort(key=lambda t: t.total_revenue_usd, reverse=True)

        logger.info("customer_trends_calculated", count=len(trends))
        return trends[:limit]

    # ==================
    # DASHBOARD
    # ==================

    def get_intelligence_dashboard(
        self,
        period_days: int = 90,
    ) -> IntelligenceDashboard:
        """
        Get summary dashboard with key metrics and top movers.
        """
        today = date.today()
        period_start = today - timedelta(days=period_days)

        logger.info("building_intelligence_dashboard", period_days=period_days)

        # Get all trends
        product_trends = self.get_product_trends(period_days=period_days, limit=100)
        customer_trends = self.get_customer_trends(period_days=period_days, limit=100)
        country_trend = self.get_country_trends(period_days=period_days)

        # Aggregate metrics
        total_revenue = sum(t.total_revenue_usd for t in product_trends)
        total_volume = sum(t.total_volume_m2 for t in product_trends)
        active_products = len([t for t in product_trends if t.total_volume_m2 > 0])

        # Count trends by direction
        products_up = len([t for t in product_trends if t.direction == TrendDirection.UP])
        products_down = len([t for t in product_trends if t.direction == TrendDirection.DOWN])
        products_stable = len([t for t in product_trends if t.direction == TrendDirection.STABLE])

        # Count customers who had ANY sales in the period (period-based)
        customers_in_period = len([t for t in customer_trends if t.period_volume_m2 > 0])

        # Count customers by status (status-based: 30/90 day thresholds)
        customers_active = len([t for t in customer_trends if t.status == CustomerStatus.ACTIVE])
        customers_cooling = len([t for t in customer_trends if t.status == CustomerStatus.COOLING])
        customers_dormant = len([t for t in customer_trends if t.status == CustomerStatus.DORMANT])

        # Top movers
        growing = sorted([t for t in product_trends if t.direction == TrendDirection.UP],
                         key=lambda t: t.velocity_change_pct, reverse=True)[:5]
        declining = sorted([t for t in product_trends if t.direction == TrendDirection.DOWN],
                           key=lambda t: t.velocity_change_pct)[:5]

        return IntelligenceDashboard(
            period_start=period_start,
            period_end=today,
            total_revenue_usd=round(total_revenue, 2),
            total_volume_m2=round(total_volume, 2),
            active_customers=customers_in_period,  # Period-based: customers who ordered in period
            active_products=active_products,
            products_trending_up=products_up,
            products_trending_down=products_down,
            products_stable=products_stable,
            customers_active=customers_active,    # Status-based: last order <= 30 days
            customers_cooling=customers_cooling,  # Status-based: last order 31-90 days
            customers_dormant=customers_dormant,  # Status-based: last order > 90 days
            top_growing_products=growing,
            top_declining_products=declining,
            top_customers=customer_trends[:5],
            country_breakdown=country_trend.countries,
        )


# Singleton instance
_trend_service: Optional[TrendService] = None


def get_trend_service() -> TrendService:
    """Get singleton instance of TrendService."""
    global _trend_service
    if _trend_service is None:
        _trend_service = TrendService()
    return _trend_service
