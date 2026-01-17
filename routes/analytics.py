"""
Analytics API routes.

Business intelligence endpoints for customer revenue, costs, and margins.
"""

from datetime import date
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.analytics import (
    CustomerAnalyticsResponse,
    CostSummary,
    MarginSummary,
    MoneyFlowResponse,
    FinancialOverview,
)
from services.analytics_service import get_analytics_service
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
# CUSTOMER ANALYTICS
# ===================

@router.get("/customers", response_model=CustomerAnalyticsResponse)
async def get_customer_analytics(
    start_date: Optional[date] = Query(None, description="Filter sales from this date"),
    end_date: Optional[date] = Query(None, description="Filter sales until this date"),
    limit: int = Query(20, ge=1, le=100, description="Maximum customers to return")
):
    """
    Get customer revenue rankings.

    Returns customers sorted by total revenue descending.
    Includes total revenue, quantity sold, order count, and purchase dates.

    Args:
        start_date: Filter sales from this date (inclusive)
        end_date: Filter sales until this date (inclusive)
        limit: Maximum number of customers to return (1-100)

    Returns:
        CustomerAnalyticsResponse with customer summaries
    """
    try:
        service = get_analytics_service()
        return service.get_customer_analytics(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
    except Exception as e:
        return handle_error(e)


# ===================
# PRODUCT ANALYTICS
# ===================

@router.get("/products")
async def get_top_products(
    limit: int = Query(5, ge=1, le=100, description="Number of products to return")
):
    """
    Get top products by revenue.

    Returns products sorted by total revenue descending.
    Includes SKU, total revenue, and quantity sold in m².

    Args:
        limit: Maximum number of products to return (1-20)

    Returns:
        Dict with data list of product summaries
    """
    try:
        service = get_analytics_service()
        return service.get_top_products(limit=limit)
    except Exception as e:
        return handle_error(e)


# ===================
# COST ANALYTICS
# ===================

@router.get("/costs", response_model=CostSummary)
async def get_cost_summary(
    start_date: Optional[date] = Query(None, description="Filter shipments from this date"),
    end_date: Optional[date] = Query(None, description="Filter shipments until this date")
):
    """
    Get shipment cost breakdown.

    Returns total costs by category: freight, customs, duties, insurance,
    demurrage, and other costs.

    Args:
        start_date: Filter shipments from this date (by created_at)
        end_date: Filter shipments until this date

    Returns:
        CostSummary with breakdown by category
    """
    try:
        service = get_analytics_service()
        return service.get_cost_summary(
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        return handle_error(e)


# ===================
# MARGIN ANALYTICS
# ===================

@router.get("/margins", response_model=MarginSummary)
async def get_margin_summary(
    start_date: Optional[date] = Query(None, description="Filter from this date"),
    end_date: Optional[date] = Query(None, description="Filter until this date")
):
    """
    Get revenue vs costs margin.

    Calculates gross margin as total revenue minus total costs.
    Returns margin both as absolute value and percentage.

    Args:
        start_date: Filter from this date
        end_date: Filter until this date

    Returns:
        MarginSummary with revenue, costs, and margin
    """
    try:
        service = get_analytics_service()
        return service.get_margin_summary(
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        return handle_error(e)


# ===================
# MONEY FLOW (SANKEY)
# ===================

@router.get("/money-flow", response_model=MoneyFlowResponse)
async def get_money_flow(
    start_date: Optional[date] = Query(None, description="Filter from this date"),
    end_date: Optional[date] = Query(None, description="Filter until this date"),
    group_by: str = Query("customer", pattern="^(customer|product)$", description="Group inflows by customer or product")
):
    """
    Get data shaped for Sankey diagram visualization.

    Returns:
    - inflows: Top 5 customers/products by revenue + "Otros" for remainder
    - outflows: Cost categories (Flete, Aduanas, Impuestos, Seguro, Demoras, Otros)

    Args:
        start_date: Filter from this date
        end_date: Filter until this date
        group_by: Group inflows by "customer" or "product"

    Returns:
        MoneyFlowResponse with inflows and outflows for Sankey diagram
    """
    try:
        service = get_analytics_service()
        return service.get_money_flow(
            start_date=start_date,
            end_date=end_date,
            group_by=group_by
        )
    except Exception as e:
        return handle_error(e)


# ===================
# OVERVIEW
# ===================

@router.get("/overview", response_model=FinancialOverview)
async def get_financial_overview():
    """
    Get high-level financial metrics for dashboard.

    Returns summary metrics including:
    - Total revenue
    - Total costs
    - Gross margin (absolute and percentage)
    - Top 5 customers by revenue
    - Cost breakdown by category

    Returns:
        FinancialOverview with all dashboard metrics
    """
    try:
        service = get_analytics_service()
        return service.get_financial_overview()
    except Exception as e:
        return handle_error(e)
