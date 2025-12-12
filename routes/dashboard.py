"""
Dashboard API routes.

Provides summary views of stockout status and key metrics.
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from services.stockout_service import (
    get_stockout_service,
    StockoutStatus,
    StockoutSummary,
    ProductStockout,
)
from exceptions import AppError

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
# STOCKOUT ROUTES
# ===================

@router.get("/stockout", response_model=StockoutSummary)
async def get_stockout_status():
    """
    Get stockout status for all products.

    Returns complete stockout analysis including:
    - Status counts (critical, warning, ok, no_sales)
    - Individual product calculations
    - Lead time and threshold info
    """
    try:
        service = get_stockout_service()
        return service.calculate_all()

    except Exception as e:
        return handle_error(e)


@router.get("/stockout/critical", response_model=list[ProductStockout])
async def get_critical_products():
    """
    Get products with CRITICAL stockout status.

    These products have less than 45 days of stock remaining
    and need immediate attention.
    """
    try:
        service = get_stockout_service()
        return service.get_critical_products()

    except Exception as e:
        return handle_error(e)


@router.get("/stockout/warning", response_model=list[ProductStockout])
async def get_warning_products():
    """
    Get products with WARNING stockout status.

    These products have 45-59 days of stock remaining
    and should be ordered soon.
    """
    try:
        service = get_stockout_service()
        return service.get_warning_products()

    except Exception as e:
        return handle_error(e)


@router.get("/stockout/by-status", response_model=list[ProductStockout])
async def get_products_by_status(
    status: StockoutStatus = Query(..., description="Filter by status")
):
    """
    Get products filtered by stockout status.

    Status options: CRITICAL, WARNING, OK, NO_SALES
    """
    try:
        service = get_stockout_service()
        return service.get_products_by_status(status)

    except Exception as e:
        return handle_error(e)


@router.get("/stockout/{product_id}", response_model=ProductStockout)
async def get_product_stockout(product_id: str):
    """
    Get stockout status for a single product.

    Args:
        product_id: Product UUID
    """
    try:
        service = get_stockout_service()
        return service.calculate_for_product(product_id)

    except Exception as e:
        return handle_error(e)


# ===================
# SUMMARY ROUTES
# ===================

@router.get("/summary")
async def get_dashboard_summary():
    """
    Get high-level dashboard summary.

    Returns quick counts without full product details.
    Useful for dashboard widgets.
    """
    try:
        stockout_service = get_stockout_service()
        stockout = stockout_service.calculate_all()

        return {
            "stockout": {
                "total_products": stockout.total_products,
                "critical": stockout.critical_count,
                "warning": stockout.warning_count,
                "ok": stockout.ok_count,
                "low_volume": stockout.low_volume_count,
                "overstock": stockout.overstock_count,
                "no_recent_sales": stockout.no_recent_sales_count,
                "no_history": stockout.no_history_count,
                "lead_time_days": stockout.lead_time_days,
                "warning_threshold_days": stockout.warning_threshold_days,
                "low_volume_threshold_m2_week": stockout.low_volume_threshold_m2_week,
            },
            "alerts": {
                "urgent_count": stockout.critical_count,
                "attention_count": stockout.warning_count + stockout.low_volume_count,
            }
        }

    except Exception as e:
        return handle_error(e)
