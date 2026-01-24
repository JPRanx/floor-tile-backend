"""
Intelligence API endpoints.

Exposes product, country, and customer trend data for the Intelligence dashboard.
"""

from typing import List, Optional

from fastapi import APIRouter, Query

from models.trends import (
    CountryTrend,
    CustomerTrend,
    IntelligenceDashboard,
    ProductTrend,
)
from services.trend_service import get_trend_service
from services.customer_pattern_service import get_customer_pattern_service

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


# =======================
# CUSTOMER PATTERN ENDPOINTS
# =======================


@router.post("/patterns/refresh")
async def refresh_customer_patterns():
    """
    Recalculate all customer ordering patterns.

    This endpoint triggers a full recalculation of:
    - Average gap between orders
    - Standard deviation and coefficient of variation
    - Expected next order date
    - Days overdue
    - Tier and predictability classification

    Returns the count of patterns updated.
    """
    service = get_customer_pattern_service()
    count = service.refresh_patterns()
    return {"success": True, "patterns_updated": count}


@router.get("/patterns/overdue")
async def get_overdue_customers(
    min_days: int = Query(1, ge=1, description="Minimum days overdue"),
    tier: Optional[str] = Query(None, description="Filter by tier (A, B, or C)"),
    limit: int = Query(50, ge=1, le=200, description="Maximum customers to return"),
):
    """
    Get customers who are past their expected order date.

    Returns customers sorted by days_overdue descending.

    **Response fields:**
    - `customer_normalized`: Customer name
    - `days_overdue`: Days past expected order date
    - `expected_next_date`: When they were expected to order
    - `avg_gap_days`: Their typical ordering interval
    - `predictability`: CLOCKWORK, PREDICTABLE, MODERATE, or ERRATIC
    - `tier`: A (top 20%), B (next 30%), or C (bottom 50%)
    """
    service = get_customer_pattern_service()
    patterns = service.get_overdue_customers(
        min_days_overdue=min_days,
        tier=tier,
        limit=limit,
    )
    return [
        {
            "customer_normalized": p.customer_normalized,
            "order_count": p.order_count,
            "avg_gap_days": float(p.avg_gap_days) if p.avg_gap_days else None,
            "gap_std_days": float(p.gap_std_days) if p.gap_std_days else None,
            "coefficient_of_variation": float(p.coefficient_of_variation) if p.coefficient_of_variation else None,
            "first_order_date": p.first_order_date.isoformat() if p.first_order_date else None,
            "last_order_date": p.last_order_date.isoformat() if p.last_order_date else None,
            "expected_next_date": p.expected_next_date.isoformat() if p.expected_next_date else None,
            "days_since_last": p.days_since_last,
            "days_overdue": p.days_overdue,
            "total_volume_m2": float(p.total_volume_m2),
            "total_revenue_usd": float(p.total_revenue_usd),
            "avg_order_m2": float(p.avg_order_m2),
            "avg_order_usd": float(p.avg_order_usd),
            "tier": p.tier,
            "predictability": p.predictability,
        }
        for p in patterns
    ]


@router.get("/patterns/overdue/summary")
async def get_overdue_summary():
    """
    Get summary of overdue customers.

    Returns aggregate metrics about customers past their expected order date.

    **Response fields:**
    - `total_overdue`: Count of overdue customers
    - `total_value_at_risk`: Sum of avg order value for overdue customers
    - `tier_a_overdue`: Count of Tier A (top 20%) customers overdue
    - `tier_b_overdue`: Count of Tier B (next 30%) customers overdue
    - `tier_c_overdue`: Count of Tier C (bottom 50%) customers overdue
    - `most_overdue`: Customer with highest days_overdue
    """
    service = get_customer_pattern_service()
    summary = service.get_overdue_summary()
    return {
        "total_overdue": summary["total_overdue"],
        "total_value_at_risk": float(summary["total_value_at_risk"]),
        "tier_a_overdue": summary["tier_a_overdue"],
        "tier_b_overdue": summary["tier_b_overdue"],
        "tier_c_overdue": summary["tier_c_overdue"],
        "most_overdue": summary["most_overdue"],
    }


@router.get("/patterns/due-soon")
async def get_customers_due_soon(
    days_ahead: int = Query(7, ge=1, le=30, description="Days to look ahead"),
    tier: Optional[str] = Query(None, description="Filter by tier (A, B, or C)"),
    limit: int = Query(20, ge=1, le=100, description="Maximum customers to return"),
):
    """
    Get customers expected to order within the next N days.

    Returns customers who are due to order soon but not yet overdue.

    **Response fields:**
    - `customer_normalized`: Customer name
    - `expected_next_date`: When they are expected to order
    - `avg_gap_days`: Their typical ordering interval
    - `predictability`: CLOCKWORK, PREDICTABLE, MODERATE, or ERRATIC
    - `tier`: A (top 20%), B (next 30%), or C (bottom 50%)
    """
    service = get_customer_pattern_service()
    patterns = service.get_due_soon(
        days_ahead=days_ahead,
        tier=tier,
        limit=limit,
    )
    return [
        {
            "customer_normalized": p.customer_normalized,
            "order_count": p.order_count,
            "avg_gap_days": float(p.avg_gap_days) if p.avg_gap_days else None,
            "expected_next_date": p.expected_next_date.isoformat() if p.expected_next_date else None,
            "days_since_last": p.days_since_last,
            "total_volume_m2": float(p.total_volume_m2),
            "total_revenue_usd": float(p.total_revenue_usd),
            "avg_order_m2": float(p.avg_order_m2),
            "avg_order_usd": float(p.avg_order_usd),
            "tier": p.tier,
            "predictability": p.predictability,
        }
        for p in patterns
    ]
