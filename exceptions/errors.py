"""
Custom exception classes for the application.

See STANDARDS_ERRORS.md for patterns and error codes.
"""

from typing import Optional, Any
from datetime import datetime


class AppError(Exception):
    """
    Base exception for all application errors.
    
    All custom exceptions inherit from this.
    
    Attributes:
        code: Error code (e.g., "PRODUCT_NOT_FOUND")
        message: Human-readable message
        status_code: HTTP status code
        details: Additional context
    """
    
    def __init__(
        self,
        code: str,
        message: str,
        status_code: int = 500,
        details: Optional[dict[str, Any]] = None
    ):
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        self.timestamp = datetime.utcnow().isoformat()
        super().__init__(message)
    
    def to_dict(self) -> dict:
        """Convert to API response format."""
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
                "timestamp": self.timestamp
            }
        }


class NotFoundError(AppError):
    """Resource not found (404)."""
    
    def __init__(
        self,
        resource: str,
        identifier: str,
        code: Optional[str] = None
    ):
        super().__init__(
            code=code or f"{resource.upper()}_NOT_FOUND",
            message=f"{resource} not found",
            status_code=404,
            details={"id": identifier}
        )


class ValidationError(AppError):
    """Validation failed (422)."""
    
    def __init__(
        self,
        message: str,
        code: str = "VALIDATION_ERROR",
        details: Optional[dict] = None
    ):
        super().__init__(
            code=code,
            message=message,
            status_code=422,
            details=details
        )


class ConflictError(AppError):
    """Conflict with existing resource (409)."""
    
    def __init__(
        self,
        message: str,
        code: str = "CONFLICT",
        details: Optional[dict] = None
    ):
        super().__init__(
            code=code,
            message=message,
            status_code=409,
            details=details
        )


class DuplicateError(ConflictError):
    """Duplicate resource (409)."""
    
    def __init__(
        self,
        resource: str,
        field: str,
        value: str
    ):
        super().__init__(
            code=f"{resource.upper()}_{field.upper()}_EXISTS",
            message=f"{resource} with this {field} already exists",
            details={field: value}
        )


class ExternalServiceError(AppError):
    """External service failure (503)."""
    
    def __init__(
        self,
        service: str,
        message: str,
        details: Optional[dict] = None
    ):
        super().__init__(
            code=f"{service.upper()}_ERROR",
            message=message,
            status_code=503,
            details={"service": service, **(details or {})}
        )


class DatabaseError(AppError):
    """Database operation failed (500)."""
    
    def __init__(
        self,
        operation: str,
        message: str,
        details: Optional[dict] = None
    ):
        super().__init__(
            code="DATABASE_ERROR",
            message=f"Database {operation} failed: {message}",
            status_code=500,
            details={"operation": operation, **(details or {})}
        )


# ===================
# SPECIFIC ERRORS
# ===================

class ProductNotFoundError(NotFoundError):
    """Product not found."""
    
    def __init__(self, product_id: str):
        super().__init__(
            resource="Product",
            identifier=product_id,
            code="PRODUCT_NOT_FOUND"
        )


class ProductSKUExistsError(DuplicateError):
    """Product SKU already exists."""
    
    def __init__(self, sku: str):
        super().__init__(
            resource="Product",
            field="sku",
            value=sku
        )


class InvalidCategoryError(ValidationError):
    """Invalid product category."""
    
    def __init__(self, category: str):
        super().__init__(
            code="PRODUCT_INVALID_CATEGORY",
            message="Category must be MADERAS, EXTERIORES, or MARMOLIZADOS",
            details={"provided": category, "valid": ["MADERAS", "EXTERIORES", "MARMOLIZADOS"]}
        )


class InvalidRotationError(ValidationError):
    """Invalid product rotation."""

    def __init__(self, rotation: str):
        super().__init__(
            code="PRODUCT_INVALID_ROTATION",
            message="Rotation must be ALTA, MEDIA-ALTA, MEDIA, or BAJA",
            details={"provided": rotation, "valid": ["ALTA", "MEDIA-ALTA", "MEDIA", "BAJA"]}
        )


# ===================
# EXCEL PARSER ERRORS
# ===================

class ExcelParseError(ValidationError):
    """Excel file parsing failed."""

    def __init__(
        self,
        message: str,
        details: Optional[dict] = None
    ):
        super().__init__(
            code="EXCEL_PARSE_ERROR",
            message=message,
            details=details
        )


class InvalidSKUError(ValidationError):
    """SKU not found in products table."""

    def __init__(self, sku: str, row: int, sheet: str = "INVENTARIO"):
        super().__init__(
            code="INVALID_SKU",
            message=f"Unknown SKU: {sku}",
            details={"sku": sku, "row": row, "sheet": sheet}
        )


# ===================
# INVENTORY ERRORS
# ===================

class InventoryNotFoundError(NotFoundError):
    """Inventory snapshot not found."""

    def __init__(self, snapshot_id: str):
        super().__init__(
            resource="Inventory snapshot",
            identifier=snapshot_id,
            code="INVENTORY_NOT_FOUND"
        )


class InventoryUploadError(ValidationError):
    """Inventory upload validation failed."""

    def __init__(self, errors: list[dict]):
        super().__init__(
            code="INVENTORY_UPLOAD_FAILED",
            message=f"Upload validation failed with {len(errors)} errors",
            details={"errors": errors}
        )


# ===================
# SALES ERRORS
# ===================

class SalesNotFoundError(NotFoundError):
    """Sales record not found."""

    def __init__(self, record_id: str):
        super().__init__(
            resource="Sales record",
            identifier=record_id,
            code="SALES_NOT_FOUND"
        )


# ===================
# SETTINGS ERRORS
# ===================

class SettingNotFoundError(NotFoundError):
    """Setting not found."""

    def __init__(self, key: str):
        super().__init__(
            resource="Setting",
            identifier=key,
            code="SETTING_NOT_FOUND"
        )


# ===================
# BOAT SCHEDULE ERRORS
# ===================

class BoatScheduleNotFoundError(NotFoundError):
    """Boat schedule not found."""

    def __init__(self, schedule_id: str):
        super().__init__(
            resource="Boat schedule",
            identifier=schedule_id,
            code="BOAT_SCHEDULE_NOT_FOUND"
        )


class BoatScheduleUploadError(ValidationError):
    """Boat schedule upload validation failed."""

    def __init__(self, errors: list[dict]):
        super().__init__(
            code="BOAT_SCHEDULE_UPLOAD_FAILED",
            message=f"Upload validation failed with {len(errors)} errors",
            details={"errors": errors}
        )


# ===================
# FACTORY ORDER ERRORS
# ===================

class FactoryOrderNotFoundError(NotFoundError):
    """Factory order not found."""

    def __init__(self, order_id: str):
        super().__init__(
            resource="Factory order",
            identifier=order_id,
            code="FACTORY_ORDER_NOT_FOUND"
        )


class FactoryOrderPVExistsError(DuplicateError):
    """Factory order PV number already exists."""

    def __init__(self, pv_number: str):
        super().__init__(
            resource="Factory order",
            field="pv_number",
            value=pv_number
        )


class InvalidStatusTransitionError(ValidationError):
    """Invalid status transition."""

    def __init__(self, current_status: str, new_status: str, terminal_status: str = "SHIPPED"):
        super().__init__(
            code="INVALID_STATUS_TRANSITION",
            message=f"Cannot transition from {current_status} to {new_status}",
            details={
                "current_status": current_status,
                "new_status": new_status,
                "reason": f"Status can only move forward, and {terminal_status} is terminal"
            }
        )


# ===================
# SHIPMENT ERRORS
# ===================

class ShipmentNotFoundError(NotFoundError):
    """Shipment not found."""

    def __init__(self, shipment_id: str):
        super().__init__(
            resource="Shipment",
            identifier=shipment_id,
            code="SHIPMENT_NOT_FOUND"
        )


class ShipmentBookingExistsError(DuplicateError):
    """Shipment booking number already exists."""

    def __init__(self, booking_number: str):
        super().__init__(
            resource="Shipment",
            field="booking_number",
            value=booking_number
        )


class ShipmentSHPExistsError(DuplicateError):
    """Shipment SHP number already exists."""

    def __init__(self, shp_number: str):
        super().__init__(
            resource="Shipment",
            field="shp_number",
            value=shp_number
        )


# ===================
# SHIPMENT EVENT ERRORS
# ===================

class ShipmentEventNotFoundError(NotFoundError):
    """No events found for shipment."""

    def __init__(self, shipment_id: str):
        super().__init__(
            resource="Shipment events",
            identifier=shipment_id,
            code="SHIPMENT_EVENT_NOT_FOUND"
        )


# ===================
# CONTAINER ERRORS
# ===================

class ContainerNotFoundError(NotFoundError):
    """Container not found."""

    def __init__(self, container_id: str):
        super().__init__(
            resource="Container",
            identifier=container_id,
            code="CONTAINER_NOT_FOUND"
        )


class ContainerItemNotFoundError(NotFoundError):
    """Container item not found."""

    def __init__(self, item_id: str):
        super().__init__(
            resource="Container item",
            identifier=item_id,
            code="CONTAINER_ITEM_NOT_FOUND"
        )


# ===================
# ALERT ERRORS
# ===================

class AlertNotFoundError(NotFoundError):
    """Alert not found."""

    def __init__(self, alert_id: str):
        super().__init__(
            resource="Alert",
            identifier=alert_id,
            code="ALERT_NOT_FOUND"
        )


class TelegramError(AppError):
    """Telegram API error."""

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(
            code="TELEGRAM_ERROR",
            message=message,
            status_code=500,
            details=details
        )
