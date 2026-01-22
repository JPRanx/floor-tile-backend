"""
Intelligence API endpoints.

Exposes product, country, and customer trend data for the Intelligence dashboard.
"""

from typing import List

from fastapi import APIRouter, Query

from models.trends import (
    CountryTrend,
    CustomerTrend,
    IntelligenceDashboard,
    ProductTrend,
)
from services.trend_service import get_trend_service

router = APIRouter(prefix="/api/intelligence", tags=["Intelligence"])


@router.get("/products", response_model=List[ProductTrend])
async def get_product_trends(
    period_days: int = Query(90, ge=7, le=365, description="Current analysis period in days"),
    comparison_days: int = Query(90, ge=7, le=365, description="Previous period for comparison"),
    limit: int = Query(50, ge=1, le=200, description="Maximum products to return"),
):
    """
    Get product trends with velocity, volume, and statistical confidence.

    Products are sorted by velocity change (highest growth first).

    **Response fields:**
    - `velocity_change_pct`: Percent change in daily velocity vs previous period
    - `direction`: UP, DOWN, or STABLE
    - `strength`: STRONG (>20%), MODERATE (5-20%), or WEAK (<5%)
    - `confidence`: HIGH (8+ samples, CV<0.5), MEDIUM (4+ samples, CV<1.0), or LOW
    - `sparkline`: 12-point time series for visualization
    """
    service = get_trend_service()
    return service.get_product_trends(
        period_days=period_days,
        comparison_period_days=comparison_days,
        limit=limit,
    )


@router.get("/countries", response_model=CountryTrend)
async def get_country_trends(
    period_days: int = Query(90, ge=7, le=365, description="Analysis period in days"),
    comparison_days: int = Query(90, ge=7, le=365, description="Previous period for comparison"),
):
    """
    Get revenue trends by country.

    Countries are inferred from customer name patterns (Colombia, Ecuador, Peru, Panama).

    **Response fields:**
    - `countries`: List of country breakdowns with revenue, volume, and customer counts
    - `revenue_change_pct`: Overall revenue change vs previous period
    - `direction`: Overall trend direction
    """
    service = get_trend_service()
    return service.get_country_trends(
        period_days=period_days,
        comparison_period_days=comparison_days,
    )


@router.get("/customers", response_model=List[CustomerTrend])
async def get_customer_trends(
    period_days: int = Query(90, ge=7, le=365, description="Current analysis period in days"),
    comparison_days: int = Query(90, ge=7, le=365, description="Previous period for comparison"),
    limit: int = Query(50, ge=1, le=200, description="Maximum customers to return"),
):
    """
    Get customer trends with tier, status, and purchase patterns.

    Customers are sorted by total revenue (highest first).

    **Response fields:**
    - `tier`: A (top 20% revenue), B (next 30%), or C (bottom 50%)
    - `status`: ACTIVE (30 days), COOLING (31-90 days), or DORMANT (90+ days)
    - `top_products`: Customer's most purchased products
    - `product_mix_changes`: Significant shifts in product preferences
    - `sparkline`: 12-point revenue time series
    """
    service = get_trend_service()
    return service.get_customer_trends(
        period_days=period_days,
        comparison_period_days=comparison_days,
        limit=limit,
    )


@router.get("/dashboard", response_model=IntelligenceDashboard)
async def get_intelligence_dashboard(
    period_days: int = Query(90, ge=7, le=365, description="Analysis period in days"),
):
    """
    Get intelligence dashboard summary.

    Aggregates key metrics from products, customers, and countries.

    **Response fields:**
    - `products_trending_up/down/stable`: Count of products by trend direction
    - `customers_active/cooling/dormant`: Count of customers by activity status
    - `top_growing_products`: Top 5 products by velocity growth
    - `top_declining_products`: Top 5 products by velocity decline
    - `top_customers`: Top 5 customers by revenue
    - `country_breakdown`: Revenue distribution by country
    """
    service = get_trend_service()
    return service.get_intelligence_dashboard(period_days=period_days)
