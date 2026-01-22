"""
Shipping configuration and container calculations.

Used for weight-based container planning from SIESA inventory data.
"""

import math
from decimal import Decimal
from typing import Optional

# Default container weight limit in kg
# This value should be verified with logistics and can be overridden from database
CONTAINER_WEIGHT_LIMIT_KG = 1881


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
