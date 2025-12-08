"""
Recommendations API routes.

Exposes order recommendations based on warehouse allocation
and current stock levels.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
import structlog

from models.recommendation import (
    OrderRecommendations,
    ProductAllocation,
)
from services.recommendation_service import get_recommendation_service
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
# ROUTES
# ===================

@router.get("/orders", response_model=OrderRecommendations)
async def get_order_recommendations():
    """
    Get order recommendations for all products.

    Calculates target warehouse allocation per product based on
    sales velocity and safety stock, then compares against current
    inventory to recommend what to order.

    Returns recommendations sorted by priority (CRITICAL > HIGH > MEDIUM > LOW),
    then by days until stockout.
    """
    try:
        service = get_recommendation_service()
        return service.get_recommendations()

    except Exception as e:
        return handle_error(e)


@router.get("/allocations", response_model=list[ProductAllocation])
async def get_warehouse_allocations():
    """
    Get detailed warehouse allocation breakdown.

    Shows target allocation per product based on:
    - Sales velocity (daily/weekly average)
    - Variability (standard deviation of weekly sales)
    - Safety stock calculation (Z-score × std_dev × √lead_time)

    If total allocation exceeds warehouse capacity (740 pallets),
    allocations are scaled down proportionally.
    """
    try:
        service = get_recommendation_service()
        return service.get_allocation_details()

    except Exception as e:
        return handle_error(e)


@router.get("/summary")
async def get_recommendations_summary():
    """
    Get high-level summary of recommendations.

    Returns just the counts and totals without full product details.
    Useful for dashboard widgets.
    """
    try:
        service = get_recommendation_service()
        result = service.get_recommendations()

        return {
            "warehouse_status": result.warehouse_status,
            "lead_time_days": result.lead_time_days,
            "calculation_date": result.calculation_date,
            "total_recommendations": len(result.recommendations),
            "total_recommended_pallets": result.total_recommended_pallets,
            "total_recommended_m2": result.total_recommended_m2,
            "total_warnings": len(result.warnings),
            "by_priority": {
                "critical": result.critical_count,
                "high": result.high_count,
                "medium": result.medium_count,
                "low": result.low_count,
            }
        }

    except Exception as e:
        return handle_error(e)
