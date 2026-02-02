"""
Custom exceptions module.

See STANDARDS_ERRORS.md for usage patterns.
"""

from exceptions.errors import (
    # Base exceptions
    AppError,
    NotFoundError,
    ValidationError,
    ConflictError,
    DuplicateError,
    ExternalServiceError,
    DatabaseError,

    # Product-specific
    ProductNotFoundError,
    ProductSKUExistsError,
    InvalidCategoryError,
    InvalidRotationError,

    # Excel parser
    ExcelParseError,
    InvalidSKUError,

    # Inventory
    InventoryNotFoundError,
    InventoryUploadError,

    # Sales
    SalesNotFoundError,

    # Settings
    SettingNotFoundError,

    # Boat Schedules
    BoatScheduleNotFoundError,
    BoatScheduleUploadError,

    # Factory Orders
    FactoryOrderNotFoundError,
    FactoryOrderPVExistsError,
    InvalidStatusTransitionError,

    # Shipments
    ShipmentNotFoundError,
    ShipmentBookingExistsError,
    ShipmentSHPExistsError,

    # Shipment Events
    ShipmentEventNotFoundError,

    # Containers
    ContainerNotFoundError,
    ContainerItemNotFoundError,

    # SAC Parser
    SACParseError,
    SACMissingColumnsError,

    # SIESA Parser
    SIESAParseError,
    SIESAMissingColumnsError,

    # Warehouse Orders
    WarehouseOrderNotFoundError,
)

__all__ = [
    # Base
    "AppError",
    "NotFoundError",
    "ValidationError",
    "ConflictError",
    "DuplicateError",
    "ExternalServiceError",
    "DatabaseError",

    # Product
    "ProductNotFoundError",
    "ProductSKUExistsError",
    "InvalidCategoryError",
    "InvalidRotationError",

    # Excel parser
    "ExcelParseError",
    "InvalidSKUError",

    # Inventory
    "InventoryNotFoundError",
    "InventoryUploadError",

    # Sales
    "SalesNotFoundError",

    # Settings
    "SettingNotFoundError",

    # Boat Schedules
    "BoatScheduleNotFoundError",
    "BoatScheduleUploadError",

    # Factory Orders
    "FactoryOrderNotFoundError",
    "FactoryOrderPVExistsError",
    "InvalidStatusTransitionError",

    # Shipments
    "ShipmentNotFoundError",
    "ShipmentBookingExistsError",
    "ShipmentSHPExistsError",

    # Shipment Events
    "ShipmentEventNotFoundError",

    # Containers
    "ContainerNotFoundError",
    "ContainerItemNotFoundError",

    # SAC Parser
    "SACParseError",
    "SACMissingColumnsError",

    # SIESA Parser
    "SIESAParseError",
    "SIESAMissingColumnsError",

    # Warehouse Orders
    "WarehouseOrderNotFoundError",
]
