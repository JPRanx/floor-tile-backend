"""
Order Builder constants and shared data structures.
"""

from typing import Optional
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date, timedelta

from config import settings
from config.shipping import CONTAINER_MAX_PALLETS, M2_PER_PALLET

# Use config.shipping constants via imports above
# M2_PER_PALLET, CONTAINER_MAX_PALLETS imported from config.shipping
PALLETS_PER_CONTAINER = CONTAINER_MAX_PALLETS  # Alias for readability
MAX_CONTAINERS_PER_BL = 5  # Each BL can hold up to 5 containers
WAREHOUSE_CAPACITY = settings.warehouse_max_pallets  # From config.settings

# Factory request constants
MIN_CONTAINER_M2 = M2_PER_PALLET * PALLETS_PER_CONTAINER  # 1,881.6 m²
LOW_VOLUME_THRESHOLD_DAYS = 365  # 1 year — products that take longer to consume 1 container are flagged


@dataclass
class ProductAnalysis:
    """
    Single intermediate between FS/inventory and OrderBuilderProduct.

    ONE branch at the top decides which analyzer fills this.
    Everything downstream reads from here -- zero forks.
    """
    # Source flag
    uses_projection: bool

    # Stock position
    days_of_stock: Optional[int]
    urgency: str

    # Trend (may be overridden by FS)
    trend_direction: str
    trend_strength: str

    # Quantity chain
    base_quantity_m2: Decimal
    trend_adjustment_m2: Decimal
    trend_adjustment_pct: Decimal
    adjusted_quantity_m2: Decimal
    buffer_days: int
    total_coverage_days: int

    # Deductions
    minus_current: Decimal
    minus_incoming: Decimal
    pending_order_m2: Decimal
    pending_order_pallets: int
    pending_order_boat: Optional[str]

    # Suggestion
    final_suggestion_m2: Decimal
    final_suggestion_pallets: int
    adjusted_coverage_gap: Decimal

    # Customer demand
    customer_demand_score: int
    customers_expecting_count: int
    expected_customer_orders_m2: Decimal
    customer_names: list = field(default_factory=list)

    # Factory cascade-aware m2 (for computation: coverage gap, fill status, SIESA cap)
    factory_cascade_m2: Decimal = Decimal("0")

    # FS transparency
    projected_stock_m2: Optional[Decimal] = None
    earlier_drafts_consumed_m2: Optional[Decimal] = None

    # For CalculationBreakdown
    lead_time_days_for_breakdown: int = 0
    ordering_cycle_days_for_breakdown: int = 30


def _get_next_monday(from_date: date) -> date:
    """
    Get next Monday from a given date.
    Factory adds new items to production schedule on Mondays.
    """
    days_ahead = (7 - from_date.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7  # If today is Monday, get next Monday
    return from_date + timedelta(days=days_ahead)
