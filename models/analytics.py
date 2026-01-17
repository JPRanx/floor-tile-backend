"""
Analytics schemas for business intelligence endpoints.

Provides aggregated views of customer revenue, shipment costs, and margins.
"""

from datetime import date
from typing import Optional, List
from decimal import Decimal
from pydantic import Field

from models.base import BaseSchema


# ===================
# CUSTOMER ANALYTICS
# ===================

class CustomerSummary(BaseSchema):
    """Aggregated customer metrics."""

    customer_normalized: str = Field(..., description="Normalized customer name")
    total_revenue_usd: Decimal = Field(..., description="Total revenue from customer")
    total_quantity_m2: Decimal = Field(..., description="Total quantity sold in m²")
    order_count: int = Field(..., description="Number of orders")
    first_purchase: Optional[date] = Field(None, description="Date of first purchase")
    last_purchase: Optional[date] = Field(None, description="Date of most recent purchase")
    avg_order_value_usd: Decimal = Field(..., description="Average order value in USD")


class CustomerAnalyticsResponse(BaseSchema):
    """Response for customer analytics endpoint."""

    data: List[CustomerSummary] = Field(..., description="Customer summaries sorted by revenue")
    total_customers: int = Field(..., description="Total number of unique customers")
    total_revenue_usd: Decimal = Field(..., description="Total revenue across all customers")
    period_start: Optional[date] = Field(None, description="Start of analysis period")
    period_end: Optional[date] = Field(None, description="End of analysis period")


# ===================
# COST ANALYTICS
# ===================

class CostSummary(BaseSchema):
    """Aggregated shipment costs breakdown."""

    total_fob_usd: Decimal = Field(default=Decimal("0"), description="Total FOB product cost")
    total_freight_usd: Decimal = Field(default=Decimal("0"), description="Total freight costs")
    total_customs_usd: Decimal = Field(default=Decimal("0"), description="Total customs costs")
    total_duties_usd: Decimal = Field(default=Decimal("0"), description="Total duties/tariffs")
    total_insurance_usd: Decimal = Field(default=Decimal("0"), description="Total insurance costs")
    total_demurrage_usd: Decimal = Field(default=Decimal("0"), description="Total demurrage costs")
    total_other_usd: Decimal = Field(default=Decimal("0"), description="Total other costs")
    total_costs_usd: Decimal = Field(default=Decimal("0"), description="Sum of all costs")
    shipment_count: int = Field(default=0, description="Number of shipments included")


# ===================
# MARGIN ANALYTICS
# ===================

class MarginSummary(BaseSchema):
    """Revenue vs costs margin calculation."""

    total_revenue_usd: Decimal = Field(..., description="Total revenue")
    total_costs_usd: Decimal = Field(..., description="Total costs")
    gross_margin_usd: Decimal = Field(..., description="Revenue - Costs")
    margin_percentage: Decimal = Field(..., description="Margin as percentage of revenue")
    period_start: Optional[date] = Field(None, description="Start of analysis period")
    period_end: Optional[date] = Field(None, description="End of analysis period")


# ===================
# MONEY FLOW (SANKEY)
# ===================

class MoneyFlowInflow(BaseSchema):
    """Single inflow source for Sankey diagram."""

    source: str = Field(..., description="Customer name or 'Otros'")
    amount: Decimal = Field(..., description="Revenue amount in USD")


class MoneyFlowOutflow(BaseSchema):
    """Single outflow category for Sankey diagram."""

    category: str = Field(..., description="Cost category (FOB, Flete, Aduanas, etc.)")
    amount: Decimal = Field(..., description="Cost amount in USD")


class MoneyFlowResponse(BaseSchema):
    """Response shaped for Sankey diagram visualization."""

    inflows: List[MoneyFlowInflow] = Field(..., description="Top 5 customers + Otros")
    outflows: List[MoneyFlowOutflow] = Field(..., description="Cost categories")
    total_revenue: Decimal = Field(..., description="Total revenue")
    total_costs: Decimal = Field(..., description="Total costs")
    margin: Decimal = Field(..., description="Revenue - Costs")


# ===================
# OVERVIEW
# ===================

class FinancialOverview(BaseSchema):
    """High-level financial metrics for dashboard."""

    revenue: Decimal = Field(..., description="Total revenue")
    costs: Decimal = Field(..., description="Total costs")
    margin: Decimal = Field(..., description="Revenue - Costs")
    margin_pct: Decimal = Field(..., description="Margin as percentage")
    top_customers: List[CustomerSummary] = Field(..., description="Top 5 customers by revenue")
    cost_breakdown: CostSummary = Field(..., description="Detailed cost breakdown")
