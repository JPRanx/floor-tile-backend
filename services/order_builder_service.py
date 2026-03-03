"""
Order Builder service — backward-compatibility shim.

The real implementation lives in services/order_builder/ (package).
This file re-exports symbols so existing imports still work.
"""

from services.order_builder import (  # noqa: F401
    OrderBuilderService,
    get_order_builder_service,
    ProductAnalysis,
    PALLETS_PER_CONTAINER,
    MAX_CONTAINERS_PER_BL,
    WAREHOUSE_CAPACITY,
    MIN_CONTAINER_M2,
    LOW_VOLUME_THRESHOLD_DAYS,
)
