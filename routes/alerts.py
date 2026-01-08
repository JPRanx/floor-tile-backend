"""
Alerts API routes.

Provides endpoints for alert management and generation.
"""

from fastapi import APIRouter, Query, status
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.alert import (
    AlertResponse,
    AlertListResponse,
    AlertSeverity,
)
from services.alert_service import get_alert_service
from integrations.telegram import test_connection, TelegramError
from exceptions import AppError

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])


# ===================
# EXCEPTION HANDLER
# ===================

def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


# ===================
# ALERT CRUD ROUTES
# ===================

@router.get("", response_model=AlertListResponse)
async def list_alerts(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    is_read: Optional[bool] = Query(None, description="Filter by read status"),
    severity: Optional[AlertSeverity] = Query(None, description="Filter by severity"),
):
    """
    List alerts with optional filters.

    Query parameters:
    - is_read: Filter by read/unread (true/false)
    - severity: Filter by severity (CRITICAL, WARNING, INFO)
    """
    try:
        service = get_alert_service()
        alerts, total = service.get_all(
            page=page,
            page_size=page_size,
            is_read=is_read,
            severity=severity,
        )

        total_pages = (total + page_size - 1) // page_size

        return AlertListResponse(
            data=alerts,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    except Exception as e:
        return handle_error(e)


@router.get("/unread", response_model=list[AlertResponse])
async def get_unread_alerts():
    """
    Get all unread alerts.

    Returns unread alerts sorted by creation date (newest first).
    """
    try:
        service = get_alert_service()
        return service.get_unread()

    except Exception as e:
        return handle_error(e)


@router.get("/{alert_id}", response_model=AlertResponse)
async def get_alert(alert_id: str):
    """
    Get a single alert by ID.

    Args:
        alert_id: Alert UUID
    """
    try:
        service = get_alert_service()
        return service.get_by_id(alert_id)

    except Exception as e:
        return handle_error(e)


@router.patch("/{alert_id}/read", response_model=AlertResponse)
async def mark_alert_read(alert_id: str):
    """
    Mark an alert as read.

    Args:
        alert_id: Alert UUID
    """
    try:
        service = get_alert_service()
        return service.mark_as_read(alert_id)

    except Exception as e:
        return handle_error(e)


@router.patch("/{alert_id}/dismiss", status_code=status.HTTP_204_NO_CONTENT)
async def dismiss_alert(alert_id: str):
    """
    Dismiss an alert (mark as read and hide).

    Args:
        alert_id: Alert UUID
    """
    try:
        service = get_alert_service()
        service.dismiss(alert_id)
        return None

    except Exception as e:
        return handle_error(e)


# ===================
# ALERT GENERATION ROUTES
# ===================

@router.post("/generate/stockout", response_model=list[AlertResponse])
async def generate_stockout_alerts():
    """
    Generate stockout alerts for products running low.

    Creates alerts for:
    - Products with < 14 days stock (CRITICAL)
    - Products with < 30 days stock (WARNING)

    Sends alerts to Telegram automatically.
    """
    try:
        service = get_alert_service()
        alerts = service.generate_stockout_alerts()

        logger.info("stockout_alerts_generated_via_api", count=len(alerts))

        return alerts

    except Exception as e:
        return handle_error(e)


@router.post("/generate/booking-deadline", response_model=list[AlertResponse])
async def generate_booking_deadline_alerts():
    """
    Generate alerts for upcoming boat booking deadlines.

    Creates alerts for boats with booking deadline in < 3 days (CRITICAL).

    Sends alerts to Telegram automatically.
    """
    try:
        service = get_alert_service()
        alerts = service.generate_booking_deadline_alerts()

        logger.info("booking_deadline_alerts_generated_via_api", count=len(alerts))

        return alerts

    except Exception as e:
        return handle_error(e)


# ===================
# TELEGRAM ROUTES
# ===================

@router.post("/test-telegram")
async def test_telegram_connection():
    """
    Test Telegram bot connection.

    Sends a test message to configured Telegram chat.

    Returns bot info and connection status.
    """
    try:
        result = test_connection()

        logger.info("telegram_test_completed", configured=result.get("configured"))

        return result

    except TelegramError as e:
        logger.error("telegram_test_failed", error=str(e))
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "TELEGRAM_ERROR",
                    "message": str(e)
                }
            }
        )
    except Exception as e:
        return handle_error(e)