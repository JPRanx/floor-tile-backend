"""
Settings API routes.

Settings are pre-seeded key-value pairs. Only updates are allowed.
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.settings import (
    SettingUpdate,
    SettingResponse,
    SettingListResponse,
)
from services.settings_service import get_settings_service
from exceptions import AppError
from exceptions.errors import SettingNotFoundError

logger = structlog.get_logger(__name__)

router = APIRouter()


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
# ROUTES
# ===================

@router.get("", response_model=SettingListResponse)
async def list_settings(
    category: Optional[str] = Query(None, description="Filter by category")
):
    """
    List all settings with optional category filter.

    Returns all settings ordered by key.
    """
    try:
        service = get_settings_service()
        settings = service.get_all(category=category)

        return SettingListResponse(
            data=settings,
            total=len(settings)
        )

    except Exception as e:
        return handle_error(e)


@router.get("/{key}", response_model=SettingResponse)
async def get_setting(key: str):
    """
    Get setting by key.

    Returns single setting.
    """
    try:
        service = get_settings_service()
        return service.get_by_key(key)

    except SettingNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{key}", response_model=SettingResponse)
async def update_setting(key: str, data: SettingUpdate):
    """
    Update setting value.

    Only the value can be updated - key is immutable.
    """
    try:
        service = get_settings_service()
        return service.update(key, data)

    except SettingNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)
