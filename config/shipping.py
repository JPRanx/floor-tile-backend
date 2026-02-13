"""
Shipping configuration and container calculations.

Used for weight-based container planning from SIESA inventory data.
"""

import math
from decimal import Decimal
from typing import Optional

# =============================================================================
# LEAD TIME CONSTANTS
# =============================================================================
# These define how many days until product is IN WAREHOUSE (not just at port)

# Days container typically sits at port awaiting customs/clearance
PORT_BUFFER_DAYS = 5

# Days from port to warehouse (trucking)
TRUCKING_DAYS = 1

# Total buffer beyond boat arrival: PORT_BUFFER_DAYS + TRUCKING_DAYS = 6 days
WAREHOUSE_BUFFER_DAYS = PORT_BUFFER_DAYS + TRUCKING_DAYS

# Ordering cycle coverage (days until NEXT boat arrives)
# FALLBACK value - Order Builder now calculates this dynamically based on actual
# next boat arrival date. This value is only used when no next boat is scheduled.
# Ashley orders monthly (~20th), so boats arrive ~30 days apart.
# When Boat 1 arrives, we need enough stock to last until Boat 2.
ORDERING_CYCLE_DAYS = 30  # Fallback when no next boat scheduled


# =============================================================================
# CONTAINER CONSTANTS (20ft Container)
# =============================================================================

# Maximum weight per 20ft container in kg
# This is the true constraint - weight limits before pallet count
CONTAINER_MAX_WEIGHT_KG = 27500

# Physical pallet limit per 20ft container
CONTAINER_MAX_PALLETS = 14

# Standard tile weight for 51x51 format (kg per m²)
# From SIESA data: All products show 14.90 kg/m² uniformly
DEFAULT_WEIGHT_PER_M2_KG = Decimal("14.90")

# Standard m² per pallet
M2_PER_PALLET = Decimal("134.4")

# Weight per pallet at standard density: 134.4 m² × 14.90 kg/m² = 2002.56 kg
WEIGHT_PER_PALLET_KG = float(M2_PER_PALLET * DEFAULT_WEIGHT_PER_M2_KG)

# Maximum pallets by weight: 27500 / 2002.56 = 13.73 pallets
# Weight is the limiting factor, not physical pallet count!
MAX_PALLETS_BY_WEIGHT = CONTAINER_MAX_WEIGHT_KG / WEIGHT_PER_PALLET_KG

# Legacy constant (deprecated - use CONTAINER_MAX_WEIGHT_KG)
CONTAINER_WEIGHT_LIMIT_KG = CONTAINER_MAX_WEIGHT_KG


# =============================================================================
# LIQUIDATION THRESHOLDS
# =============================================================================
# Criteria for identifying slow movers that could be cleared to make room

LIQUIDATION_THRESHOLDS = {
    # Declining demand + high inventory
    "declining_overstocked": {
        "trend_pct_max": -10,        # Declining by 10%+
        "days_of_stock_min": 120,    # 4+ months of inventory
    },
    # No sales in 90 days
    "no_sales": {
        "days_since_last_sale": 90,  # No sales in 90 days (days_of_stock is None or very high)
    },
    # Extreme overstock (any trend)
    "extreme_overstock": {
        "days_of_stock_min": 180,    # 6+ months (any trend)
    },
}

# Thresholds extracted as constants for easy access
LIQUIDATION_DECLINING_TREND_PCT_MAX = LIQUIDATION_THRESHOLDS["declining_overstocked"]["trend_pct_max"]
LIQUIDATION_DECLINING_DAYS_MIN = LIQUIDATION_THRESHOLDS["declining_overstocked"]["days_of_stock_min"]
LIQUIDATION_NO_SALES_DAYS = LIQUIDATION_THRESHOLDS["no_sales"]["days_since_last_sale"]
LIQUIDATION_EXTREME_DAYS_MIN = LIQUIDATION_THRESHOLDS["extreme_overstock"]["days_of_stock_min"]


# =============================================================================
# SEASONAL TREND DAMPENING
# =============================================================================
# Central America Seasonal Dampening (GT, SV, HN unified pattern)
# Reduces trend signal strength during seasonal peaks/troughs to prevent
# false growth/decline signals from window comparisons.
#
# Problem: The system compares 90-day velocity vs 180-day velocity.
# - DRY SEASON (Nov-Apr): Peak construction. 90d window captures peak,
#   180d includes slow months → false "growth" signal
# - WET SEASON (May-Oct): Slow construction. 90d window captures slow,
#   180d includes peak → false "decline" signal
#
# Solution: Dampen the trend ratio toward 1.0 (neutral) during distortion periods.
# Formula: dampened = 1.0 + (raw_ratio - 1.0) * factor
#
# Factor of 0.5 means: a +60% raw signal becomes +30% dampened
# Factor of 0.75 means: a +60% raw signal becomes +45% dampened
# Factor of 1.0 means: no dampening (pass-through)

SEASONAL_DAMPENING = {
    1: 0.5,    # January - peak dry season
    2: 0.5,    # February - peak dry season
    3: 0.5,    # March - peak dry season
    4: 0.75,   # April - transitioning to wet
    5: 0.75,   # May - wet season starts
    6: 0.75,   # June - wet season
    7: 0.75,   # July - wet season
    8: 0.75,   # August - wet season
    9: 0.75,   # September - peak wet season
    10: 0.75,  # October - transitioning to dry
    11: 0.5,   # November - dry season starts
    12: 0.5,   # December - peak dry season
}


def get_shipping_config() -> dict:
    """Get shipping config from database with fallbacks to module-level constants."""
    from services.config_service import get_config_service
    try:
        config = get_config_service()
        return {
            "M2_PER_PALLET": config.get_decimal("m2_per_pallet", M2_PER_PALLET),
            "CONTAINER_MAX_WEIGHT_KG": config.get_int("container_max_weight_kg", CONTAINER_MAX_WEIGHT_KG),
            "CONTAINER_MAX_PALLETS": config.get_int("container_max_pallets", CONTAINER_MAX_PALLETS),
            "DEFAULT_WEIGHT_PER_M2_KG": config.get_decimal("weight_per_m2_kg", DEFAULT_WEIGHT_PER_M2_KG),
            "PORT_BUFFER_DAYS": config.get_int("port_buffer_days", PORT_BUFFER_DAYS),
            "TRUCKING_DAYS": config.get_int("trucking_days", TRUCKING_DAYS),
            "ORDERING_CYCLE_DAYS": config.get_int("ordering_cycle_days", ORDERING_CYCLE_DAYS),
        }
    except Exception:
        # DB not available — use hardcoded defaults
        return {
            "M2_PER_PALLET": M2_PER_PALLET,
            "CONTAINER_MAX_WEIGHT_KG": CONTAINER_MAX_WEIGHT_KG,
            "CONTAINER_MAX_PALLETS": CONTAINER_MAX_PALLETS,
            "DEFAULT_WEIGHT_PER_M2_KG": DEFAULT_WEIGHT_PER_M2_KG,
            "PORT_BUFFER_DAYS": PORT_BUFFER_DAYS,
            "TRUCKING_DAYS": TRUCKING_DAYS,
            "ORDERING_CYCLE_DAYS": ORDERING_CYCLE_DAYS,
        }


def get_container_weight_limit(db_value: Optional[Decimal] = None) -> float:
    """
    Get container weight limit from database or use default.

    Args:
        db_value: Value from shipping_config table, if available

    Returns:
        Weight limit in kg
    """
    if db_value is not None:
        return float(db_value)
    return float(CONTAINER_WEIGHT_LIMIT_KG)


def calculate_containers_needed(total_weight_kg: float, limit_kg: float = CONTAINER_WEIGHT_LIMIT_KG) -> int:
    """
    Calculate minimum containers needed for given weight.

    Args:
        total_weight_kg: Total weight to ship
        limit_kg: Weight limit per container

    Returns:
        Number of containers needed (rounded up)
    """
    if total_weight_kg <= 0:
        return 0
    return math.ceil(total_weight_kg / limit_kg)


def calculate_container_utilization(weight_kg: float, limit_kg: float = CONTAINER_WEIGHT_LIMIT_KG) -> float:
    """
    Calculate percentage utilization of a single container.

    Args:
        weight_kg: Weight in the container
        limit_kg: Weight limit per container

    Returns:
        Utilization percentage (0-100)
    """
    if limit_kg <= 0:
        return 0.0
    return min(100.0, (weight_kg / limit_kg) * 100)


def calculate_utilization_breakdown(
    total_weight_kg: float,
    limit_kg: float = CONTAINER_WEIGHT_LIMIT_KG
) -> list[dict]:
    """
    Calculate utilization for each container needed.

    Args:
        total_weight_kg: Total weight to ship
        limit_kg: Weight limit per container

    Returns:
        List of container utilization info:
        [
            {"container": 1, "weight_kg": 1881, "utilization_pct": 100.0},
            {"container": 2, "weight_kg": 1238, "utilization_pct": 65.8}
        ]
    """
    if total_weight_kg <= 0:
        return []

    containers = []
    remaining_weight = total_weight_kg
    container_num = 1

    while remaining_weight > 0:
        container_weight = min(remaining_weight, limit_kg)
        utilization = calculate_container_utilization(container_weight, limit_kg)
        containers.append({
            "container": container_num,
            "weight_kg": round(container_weight, 2),
            "utilization_pct": round(utilization, 1)
        })
        remaining_weight -= container_weight
        container_num += 1

    return containers
