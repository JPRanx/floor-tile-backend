"""
Order Builder package — split from the monolithic order_builder_service.py.

Re-exports for backward compatibility.
"""

from services.order_builder.service import OrderBuilderService, get_order_builder_service
from services.order_builder.constants import (
    ProductAnalysis,
    PALLETS_PER_CONTAINER,
    MAX_CONTAINERS_PER_BL,
    WAREHOUSE_CAPACITY,
    MIN_CONTAINER_M2,
    LOW_VOLUME_THRESHOLD_DAYS,
    _get_next_monday,
)

__all__ = [
    "OrderBuilderService",
    "get_order_builder_service",
    "ProductAnalysis",
    "PALLETS_PER_CONTAINER",
    "MAX_CONTAINERS_PER_BL",
    "WAREHOUSE_CAPACITY",
    "MIN_CONTAINER_M2",
    "LOW_VOLUME_THRESHOLD_DAYS",
]
