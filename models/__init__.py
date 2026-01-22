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
from models.port import (
    PortCreate,
    PortResponse,
)
from models.ingest import (
    ParsedFieldConfidence,
    ParsedDocumentData,
    ConfirmIngestRequest,
    IngestResponse,
)
from models.trends import (
    TrendDirection,
    TrendStrength,
    ConfidenceLevel,
    CustomerTier,
    CustomerStatus,
    SparklinePoint,
    ProductTrend,
    CountryBreakdown,
    CountryTrend,
    ProductPurchase,
    ProductMixChange,
    CustomerTrend,
    IntelligenceDashboard,
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

    # Ports
    "PortCreate",
    "PortResponse",

    # Document Ingestion
    "ParsedFieldConfidence",
    "ParsedDocumentData",
    "ConfirmIngestRequest",
    "IngestResponse",

    # Trends / Intelligence
    "TrendDirection",
    "TrendStrength",
    "ConfidenceLevel",
    "CustomerTier",
    "CustomerStatus",
    "SparklinePoint",
    "ProductTrend",
    "CountryBreakdown",
    "CountryTrend",
    "ProductPurchase",
    "ProductMixChange",
    "CustomerTrend",
    "IntelligenceDashboard",
]
