"""
Single source of truth for coverage math.

Two services were independently computing "days until product runs out from
warehouse" with the same formula. They happened to agree but the duplication
was a future drift risk: if someone changes one (smarter velocity, trend
adjustment, etc.) and not the other, Dashboard and Horizon will silently
disagree on Ashley's most-asked-about number.

This module owns the formula. Both `lib/brain.py` and
`services/metrics_service.py` import from here.
"""

from decimal import Decimal
from typing import Optional


def days_of_stock(
    warehouse_m2: Decimal | float,
    velocity_per_day: Decimal | float,
) -> Optional[Decimal]:
    """Days until the warehouse runs out at the given daily velocity.

    Returns None when velocity is zero or negative — the math doesn't have a
    meaningful answer there. Callers decide how to display that (Horizon shows
    999, Dashboard shows blank/N/A).

    Both inputs in m². Output is unrounded; callers can quantize for display.
    """
    wh = Decimal(str(warehouse_m2))
    vel = Decimal(str(velocity_per_day))
    if vel <= 0:
        return None
    return wh / vel
