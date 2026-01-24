"""
Customer Pattern Service for calculating ordering patterns.

Calculates predictability metrics and overdue status for each customer
based on their historical ordering behavior.
"""

import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import structlog

from config import get_supabase_client
from models.trends import Predictability

logger = structlog.get_logger(__name__)


class CustomerPattern:
    """Data class for customer pattern metrics."""

    def __init__(
        self,
        customer_normalized: str,
        order_count: int = 0,
        avg_gap_days: Optional[Decimal] = None,
        gap_std_days: Optional[Decimal] = None,
        coefficient_of_variation: Optional[Decimal] = None,
        first_order_date: Optional[date] = None,
        last_order_date: Optional[date] = None,
        expected_next_date: Optional[date] = None,
        days_since_last: int = 0,
        days_overdue: int = 0,
        total_volume_m2: Decimal = Decimal("0"),
        total_revenue_usd: Decimal = Decimal("0"),
        avg_order_m2: Decimal = Decimal("0"),
        avg_order_usd: Decimal = Decimal("0"),
        tier: Optional[str] = None,
        predictability: Optional[str] = None,
    ):
        self.customer_normalized = customer_normalized
        self.order_count = order_count
        self.avg_gap_days = avg_gap_days
        self.gap_std_days = gap_std_days
        self.coefficient_of_variation = coefficient_of_variation
        self.first_order_date = first_order_date
        self.last_order_date = last_order_date
        self.expected_next_date = expected_next_date
        self.days_since_last = days_since_last
        self.days_overdue = days_overdue
        self.total_volume_m2 = total_volume_m2
        self.total_revenue_usd = total_revenue_usd
        self.avg_order_m2 = avg_order_m2
        self.avg_order_usd = avg_order_usd
        self.tier = tier
        self.predictability = predictability

    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            "customer_normalized": self.customer_normalized,
            "order_count": self.order_count,
            "avg_gap_days": float(self.avg_gap_days) if self.avg_gap_days else None,
            "gap_std_days": float(self.gap_std_days) if self.gap_std_days else None,
            "coefficient_of_variation": float(self.coefficient_of_variation) if self.coefficient_of_variation else None,
            "first_order_date": self.first_order_date.isoformat() if self.first_order_date else None,
            "last_order_date": self.last_order_date.isoformat() if self.last_order_date else None,
            "expected_next_date": self.expected_next_date.isoformat() if self.expected_next_date else None,
            "days_since_last": self.days_since_last,
            "days_overdue": self.days_overdue,
            "total_volume_m2": float(self.total_volume_m2),
            "total_revenue_usd": float(self.total_revenue_usd),
            "avg_order_m2": float(self.avg_order_m2),
            "avg_order_usd": float(self.avg_order_usd),
            "tier": self.tier,
            "predictability": self.predictability,
            "calculated_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }


class CustomerPatternService:
    """Service for calculating and managing customer ordering patterns."""

    def __init__(self):
        self.db = get_supabase_client()

    def _classify_predictability(self, cv: Optional[Decimal]) -> str:
        """
        Classify predictability based on coefficient of variation.

        - CLOCKWORK: CV < 0.3 (very consistent, orders like clockwork)
        - PREDICTABLE: CV < 0.5 (reasonably predictable)
        - MODERATE: CV < 1.0 (some variability but patterns exist)
        - ERRATIC: CV >= 1.0 (unpredictable ordering)
        """
        if cv is None:
            return Predictability.ERRATIC.value

        if cv < Decimal("0.3"):
            return Predictability.CLOCKWORK.value
        elif cv < Decimal("0.5"):
            return Predictability.PREDICTABLE.value
        elif cv < Decimal("1.0"):
            return Predictability.MODERATE.value
        else:
            return Predictability.ERRATIC.value

    def _calculate_tier(
        self,
        revenue: Decimal,
        tier_a_threshold: Decimal,
        tier_b_threshold: Decimal,
    ) -> str:
        """
        Calculate customer tier based on revenue.

        - A: Top 20% by revenue
        - B: Next 30% by revenue
        - C: Bottom 50% by revenue
        """
        if revenue >= tier_a_threshold:
            return "A"
        elif revenue >= tier_b_threshold:
            return "B"
        else:
            return "C"

    def _calculate_gaps(self, order_dates: List[date]) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Calculate average gap and standard deviation between orders.

        Returns (avg_gap_days, std_gap_days) or (None, None) if insufficient data.
        """
        if len(order_dates) < 2:
            return None, None

        # Sort dates
        sorted_dates = sorted(order_dates)

        # Calculate gaps between consecutive orders
        gaps = []
        for i in range(1, len(sorted_dates)):
            gap = (sorted_dates[i] - sorted_dates[i-1]).days
            gaps.append(Decimal(str(gap)))

        if not gaps:
            return None, None

        # Calculate average
        avg_gap = sum(gaps) / len(gaps)

        # Calculate standard deviation
        if len(gaps) >= 2:
            variance = sum((g - avg_gap) ** 2 for g in gaps) / len(gaps)
            std_gap = Decimal(str(math.sqrt(float(variance))))
        else:
            std_gap = Decimal("0")

        return round(avg_gap, 2), round(std_gap, 2)

    def calculate_all_patterns(self) -> List[CustomerPattern]:
        """
        Calculate ordering patterns for all customers.

        Returns list of CustomerPattern objects with all metrics calculated.
        """
        today = date.today()

        logger.info("calculating_all_customer_patterns")

        # Fetch all sales data
        sales_result = self.db.table("sales").select(
            "customer_normalized, week_start, quantity_m2, total_price_usd"
        ).execute()

        if not sales_result.data:
            logger.warning("no_sales_data_found")
            return []

        # Aggregate by customer
        customer_data: Dict[str, Dict] = defaultdict(lambda: {
            "order_dates": [],
            "total_volume_m2": Decimal("0"),
            "total_revenue_usd": Decimal("0"),
            "order_count": 0,
        })

        for sale in sales_result.data:
            customer_norm = sale.get("customer_normalized", "")
            week_start_str = sale.get("week_start")
            qty = Decimal(str(sale.get("quantity_m2") or 0))
            revenue = Decimal(str(sale.get("total_price_usd") or 0))

            if not customer_norm or not week_start_str:
                continue

            # Parse date
            try:
                week_start = datetime.fromisoformat(
                    week_start_str.replace("Z", "+00:00")
                ).date()
            except (ValueError, AttributeError):
                try:
                    week_start = datetime.strptime(
                        week_start_str[:10], "%Y-%m-%d"
                    ).date()
                except (ValueError, AttributeError):
                    continue

            data = customer_data[customer_norm]
            data["order_dates"].append(week_start)
            data["total_volume_m2"] += qty
            data["total_revenue_usd"] += revenue
            data["order_count"] += 1

        # Calculate tier thresholds
        all_revenues = sorted(
            [d["total_revenue_usd"] for d in customer_data.values()],
            reverse=True
        )

        if all_revenues:
            total_all = sum(all_revenues)
            cumulative = Decimal("0")
            tier_a_threshold = Decimal("0")
            tier_b_threshold = Decimal("0")

            for rev in all_revenues:
                cumulative += rev
                pct = cumulative / total_all * 100 if total_all > 0 else Decimal("0")
                if tier_a_threshold == 0 and pct >= 20:
                    tier_a_threshold = rev
                if tier_b_threshold == 0 and pct >= 50:
                    tier_b_threshold = rev
                    break
        else:
            tier_a_threshold = Decimal("0")
            tier_b_threshold = Decimal("0")

        # Calculate patterns for each customer
        patterns = []

        for customer_norm, data in customer_data.items():
            order_dates = data["order_dates"]
            order_count = data["order_count"]
            total_volume = data["total_volume_m2"]
            total_revenue = data["total_revenue_usd"]

            # Calculate gap statistics
            avg_gap, std_gap = self._calculate_gaps(order_dates)

            # Calculate coefficient of variation
            cv = None
            if avg_gap and avg_gap > 0 and std_gap is not None:
                cv = round(std_gap / avg_gap, 3)

            # Get first and last order dates
            sorted_dates = sorted(order_dates)
            first_order = sorted_dates[0] if sorted_dates else None
            last_order = sorted_dates[-1] if sorted_dates else None

            # Calculate days since last order
            days_since_last = (today - last_order).days if last_order else 0

            # Calculate expected next date and days overdue
            expected_next = None
            days_overdue = 0

            if avg_gap and last_order:
                expected_next = last_order + timedelta(days=int(avg_gap))
                if today > expected_next:
                    days_overdue = (today - expected_next).days

            # Calculate averages
            avg_order_m2 = total_volume / order_count if order_count > 0 else Decimal("0")
            avg_order_usd = total_revenue / order_count if order_count > 0 else Decimal("0")

            # Classify
            tier = self._calculate_tier(total_revenue, tier_a_threshold, tier_b_threshold)
            predictability = self._classify_predictability(cv)

            pattern = CustomerPattern(
                customer_normalized=customer_norm,
                order_count=order_count,
                avg_gap_days=avg_gap,
                gap_std_days=std_gap,
                coefficient_of_variation=cv,
                first_order_date=first_order,
                last_order_date=last_order,
                expected_next_date=expected_next,
                days_since_last=days_since_last,
                days_overdue=days_overdue,
                total_volume_m2=round(total_volume, 2),
                total_revenue_usd=round(total_revenue, 2),
                avg_order_m2=round(avg_order_m2, 2),
                avg_order_usd=round(avg_order_usd, 2),
                tier=tier,
                predictability=predictability,
            )
            patterns.append(pattern)

        # Sort by days_overdue descending
        patterns.sort(key=lambda p: p.days_overdue, reverse=True)

        logger.info(
            "customer_patterns_calculated",
            total_customers=len(patterns),
            overdue_count=len([p for p in patterns if p.days_overdue > 0]),
        )

        return patterns

    def refresh_patterns(self) -> int:
        """
        Recalculate all patterns and save to database.

        Returns count of patterns saved.
        """
        logger.info("refreshing_customer_patterns")

        # Calculate all patterns
        patterns = self.calculate_all_patterns()

        if not patterns:
            return 0

        # Upsert to database
        for pattern in patterns:
            try:
                self.db.table("customer_patterns").upsert(
                    pattern.to_dict(),
                    on_conflict="customer_normalized"
                ).execute()
            except Exception as e:
                logger.error(
                    "failed_to_save_pattern",
                    customer=pattern.customer_normalized,
                    error=str(e),
                )

        logger.info("customer_patterns_refreshed", count=len(patterns))
        return len(patterns)

    def get_overdue_customers(
        self,
        min_days_overdue: int = 1,
        tier: Optional[str] = None,
        limit: int = 50,
    ) -> List[CustomerPattern]:
        """
        Get customers who are past their expected order date.

        Args:
            min_days_overdue: Minimum days past expected date (default 1)
            tier: Optional tier filter ("A", "B", or "C")
            limit: Maximum results to return

        Returns:
            List of CustomerPattern sorted by days_overdue DESC
        """
        query = self.db.table("customer_patterns").select("*").gte(
            "days_overdue", min_days_overdue
        )

        if tier:
            query = query.eq("tier", tier)

        query = query.order("days_overdue", desc=True).limit(limit)
        result = query.execute()

        patterns = []
        for row in result.data:
            pattern = CustomerPattern(
                customer_normalized=row["customer_normalized"],
                order_count=row.get("order_count", 0),
                avg_gap_days=Decimal(str(row["avg_gap_days"])) if row.get("avg_gap_days") else None,
                gap_std_days=Decimal(str(row["gap_std_days"])) if row.get("gap_std_days") else None,
                coefficient_of_variation=Decimal(str(row["coefficient_of_variation"])) if row.get("coefficient_of_variation") else None,
                first_order_date=datetime.fromisoformat(row["first_order_date"]).date() if row.get("first_order_date") else None,
                last_order_date=datetime.fromisoformat(row["last_order_date"]).date() if row.get("last_order_date") else None,
                expected_next_date=datetime.fromisoformat(row["expected_next_date"]).date() if row.get("expected_next_date") else None,
                days_since_last=row.get("days_since_last", 0),
                days_overdue=row.get("days_overdue", 0),
                total_volume_m2=Decimal(str(row.get("total_volume_m2", 0))),
                total_revenue_usd=Decimal(str(row.get("total_revenue_usd", 0))),
                avg_order_m2=Decimal(str(row.get("avg_order_m2", 0))),
                avg_order_usd=Decimal(str(row.get("avg_order_usd", 0))),
                tier=row.get("tier"),
                predictability=row.get("predictability"),
            )
            patterns.append(pattern)

        return patterns

    def get_overdue_summary(self) -> dict:
        """
        Get summary of overdue customers.

        Returns:
            Dictionary with:
            - total_overdue: Count of overdue customers
            - total_value_at_risk: Sum of avg_order_usd for overdue customers
            - tier_a_overdue: Count of Tier A customers overdue
            - tier_b_overdue: Count of Tier B customers overdue
            - tier_c_overdue: Count of Tier C customers overdue
            - most_overdue: Customer with highest days_overdue
        """
        result = self.db.table("customer_patterns").select(
            "customer_normalized, tier, days_overdue, avg_order_usd"
        ).gt("days_overdue", 0).execute()

        if not result.data:
            return {
                "total_overdue": 0,
                "total_value_at_risk": Decimal("0"),
                "tier_a_overdue": 0,
                "tier_b_overdue": 0,
                "tier_c_overdue": 0,
                "most_overdue": None,
            }

        total_value = Decimal("0")
        tier_counts = {"A": 0, "B": 0, "C": 0}
        most_overdue = None
        max_days = 0

        for row in result.data:
            tier = row.get("tier")
            if tier in tier_counts:
                tier_counts[tier] += 1

            avg_order = Decimal(str(row.get("avg_order_usd", 0)))
            total_value += avg_order

            days = row.get("days_overdue", 0)
            if days > max_days:
                max_days = days
                most_overdue = {
                    "customer": row["customer_normalized"],
                    "days_overdue": days,
                    "avg_order_usd": float(avg_order),
                    "tier": tier,
                }

        return {
            "total_overdue": len(result.data),
            "total_value_at_risk": round(total_value, 2),
            "tier_a_overdue": tier_counts["A"],
            "tier_b_overdue": tier_counts["B"],
            "tier_c_overdue": tier_counts["C"],
            "most_overdue": most_overdue,
        }

    def get_due_soon(
        self,
        days_ahead: int = 7,
        tier: Optional[str] = None,
        limit: int = 20,
    ) -> List[CustomerPattern]:
        """
        Get customers expected to order within the next N days.

        Args:
            days_ahead: Number of days to look ahead
            tier: Optional tier filter
            limit: Maximum results

        Returns:
            List of CustomerPattern for customers due soon
        """
        today = date.today()
        future_date = today + timedelta(days=days_ahead)

        query = self.db.table("customer_patterns").select("*").gte(
            "expected_next_date", today.isoformat()
        ).lte(
            "expected_next_date", future_date.isoformat()
        ).eq(
            "days_overdue", 0  # Not already overdue
        )

        if tier:
            query = query.eq("tier", tier)

        query = query.order("expected_next_date", desc=False).limit(limit)
        result = query.execute()

        patterns = []
        for row in result.data:
            pattern = CustomerPattern(
                customer_normalized=row["customer_normalized"],
                order_count=row.get("order_count", 0),
                avg_gap_days=Decimal(str(row["avg_gap_days"])) if row.get("avg_gap_days") else None,
                gap_std_days=Decimal(str(row["gap_std_days"])) if row.get("gap_std_days") else None,
                coefficient_of_variation=Decimal(str(row["coefficient_of_variation"])) if row.get("coefficient_of_variation") else None,
                first_order_date=datetime.fromisoformat(row["first_order_date"]).date() if row.get("first_order_date") else None,
                last_order_date=datetime.fromisoformat(row["last_order_date"]).date() if row.get("last_order_date") else None,
                expected_next_date=datetime.fromisoformat(row["expected_next_date"]).date() if row.get("expected_next_date") else None,
                days_since_last=row.get("days_since_last", 0),
                days_overdue=row.get("days_overdue", 0),
                total_volume_m2=Decimal(str(row.get("total_volume_m2", 0))),
                total_revenue_usd=Decimal(str(row.get("total_revenue_usd", 0))),
                avg_order_m2=Decimal(str(row.get("avg_order_m2", 0))),
                avg_order_usd=Decimal(str(row.get("avg_order_usd", 0))),
                tier=row.get("tier"),
                predictability=row.get("predictability"),
            )
            patterns.append(pattern)

        return patterns


# Singleton instance
_customer_pattern_service: Optional[CustomerPatternService] = None


def get_customer_pattern_service() -> CustomerPatternService:
    """Get singleton instance of CustomerPatternService."""
    global _customer_pattern_service
    if _customer_pattern_service is None:
        _customer_pattern_service = CustomerPatternService()
    return _customer_pattern_service
