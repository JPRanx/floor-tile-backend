"""
API route modules.

Each module defines routes for one domain area.
"""

from routes.products import router as products_router
from routes.inventory import router as inventory_router
from routes.sales import router as sales_router
from routes.dashboard import router as dashboard_router
from routes.settings import router as settings_router
from routes.recommendations import router as recommendations_router

__all__ = [
    "products_router",
    "inventory_router",
    "sales_router",
    "dashboard_router",
    "settings_router",
    "recommendations_router",
]
