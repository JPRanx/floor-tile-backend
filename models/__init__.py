"""
Pydantic models for validation and serialization.

See STANDARDS_VALIDATION.md for patterns.
"""

from models.base import (
    BaseSchema,
    TimestampMixin,
    PaginationParams,
    PaginatedResponse
)
from models.product import (
    Category,
    Rotation,
    ProductCreate,
    ProductUpdate,
    ProductResponse,
    ProductListResponse,
    ProductWithStats
)
from models.inventory import (
    InventorySnapshotCreate,
    InventorySnapshotUpdate,
    InventorySnapshotResponse,
    InventorySnapshotWithProduct,
    InventoryListResponse,
    InventoryCurrentResponse,
    InventoryUploadResponse,
    BulkInventoryCreate,
)
from models.sales import (
    SalesRecordCreate,
    SalesRecordUpdate,
    SalesRecordResponse,
    SalesRecordWithProduct,
    SalesListResponse,
    SalesHistoryResponse,
    SalesUploadResponse,
    BulkSalesCreate,
)
from models.settings import (
    SettingCategory,
    SettingUpdate,
    SettingResponse,
    SettingListResponse,
)
from models.recommendation import (
    RecommendationPriority,
    WarningType,
    ProductAllocation,
    ProductRecommendation,
    RecommendationWarning,
    WarehouseStatus,
    OrderRecommendations,
)

__all__ = [
    # Base
    "BaseSchema",
    "TimestampMixin",
    "PaginationParams",
    "PaginatedResponse",

    # Product
    "Category",
    "Rotation",
    "ProductCreate",
    "ProductUpdate",
    "ProductResponse",
    "ProductListResponse",
    "ProductWithStats",

    # Inventory
    "InventorySnapshotCreate",
    "InventorySnapshotUpdate",
    "InventorySnapshotResponse",
    "InventorySnapshotWithProduct",
    "InventoryListResponse",
    "InventoryCurrentResponse",
    "InventoryUploadResponse",
    "BulkInventoryCreate",

    # Sales
    "SalesRecordCreate",
    "SalesRecordUpdate",
    "SalesRecordResponse",
    "SalesRecordWithProduct",
    "SalesListResponse",
    "SalesHistoryResponse",
    "SalesUploadResponse",
    "BulkSalesCreate",

    # Settings
    "SettingCategory",
    "SettingUpdate",
    "SettingResponse",
    "SettingListResponse",

    # Recommendations
    "RecommendationPriority",
    "WarningType",
    "ProductAllocation",
    "ProductRecommendation",
    "RecommendationWarning",
    "WarehouseStatus",
    "OrderRecommendations",
]
