"""
Analytics service for business intelligence operations.

Handles aggregation and reporting of business metrics:
- Customer revenue analytics
- Shipment cost breakdown
- Margin calculations
- Money flow for Sankey diagrams
"""

from typing import Optional
from datetime import date, timedelta
from decimal import Decimal
import structlog

from config import get_supabase_client
from models.product import TILE_CATEGORIES
from models.analytics import (
    CustomerSummary,
    CustomerAnalyticsResponse,
    CostSummary,
    MarginSummary,
    MoneyFlowInflow,
    MoneyFlowOutflow,
    MoneyFlowResponse,
    FinancialOverview,
)
from exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class AnalyticsService:
    """
    Analytics business logic.

    Handles aggregation of sales revenue, shipment costs, and margins.
    """

    def __init__(self):
        self.db = get_supabase_client()

    # ===================
    # CUSTOMER ANALYTICS
    # ===================

    def get_customer_analytics(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20
    ) -> CustomerAnalyticsResponse:
        """
        Aggregate revenue by customer.

        Args:
            start_date: Filter sales from this date
            end_date: Filter sales until this date
            limit: Maximum customers to return (default 20)

        Returns:
            CustomerAnalyticsResponse with ranked customers
        """
        logger.info(
            "getting_customer_analytics",
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )

        try:
            # Build query
            query = self.db.table("sales").select(
                "customer, customer_normalized, quantity_m2, total_price_usd, week_start"
            )

            if start_date:
                query = query.gte("week_start", start_date.isoformat())
            if end_date:
                query = query.lte("week_start", end_date.isoformat())

            result = query.execute()

            if not result.data:
                return CustomerAnalyticsResponse(
                    data=[],
                    total_customers=0,
                    total_revenue_usd=Decimal("0"),
                    period_start=start_date,
                    period_end=end_date
                )

            # Group by customer_normalized
            customer_stats: dict[str, dict] = {}
            total_revenue = Decimal("0")

            for row in result.data:
                normalized = row.get("customer_normalized") or "UNKNOWN"
                original = row.get("customer") or "Unknown"
                qty = Decimal(str(row.get("quantity_m2") or 0))
                revenue = Decimal(str(row.get("total_price_usd") or 0))
                week = row.get("week_start")

                total_revenue += revenue

                if normalized not in customer_stats:
                    customer_stats[normalized] = {
                        "customer_normalized": normalized,
                        "customer": original,
                        "total_revenue_usd": Decimal("0"),
                        "total_quantity_m2": Decimal("0"),
                        "order_count": 0,
                        "first_purchase": None,
                        "last_purchase": None,
                    }

                stats = customer_stats[normalized]
                stats["total_revenue_usd"] += revenue
                stats["total_quantity_m2"] += qty
                stats["order_count"] += 1

                # Track first/last purchase
                if week:
                    week_date = date.fromisoformat(week) if isinstance(week, str) else week
                    if stats["first_purchase"] is None or week_date < stats["first_purchase"]:
                        stats["first_purchase"] = week_date
                    if stats["last_purchase"] is None or week_date > stats["last_purchase"]:
                        stats["last_purchase"] = week_date

            # Sort by revenue descending and take top N
            sorted_customers = sorted(
                customer_stats.values(),
                key=lambda x: x["total_revenue_usd"],
                reverse=True
            )[:limit]

            # Convert to response models
            summaries = []
            for c in sorted_customers:
                avg_order = (
                    c["total_revenue_usd"] / c["order_count"]
                    if c["order_count"] > 0
                    else Decimal("0")
                )
                summaries.append(CustomerSummary(
                    customer_normalized=c["customer_normalized"],
                    total_revenue_usd=round(c["total_revenue_usd"], 2),
                    total_quantity_m2=round(c["total_quantity_m2"], 2),
                    order_count=c["order_count"],
                    first_purchase=c["first_purchase"],
                    last_purchase=c["last_purchase"],
                    avg_order_value_usd=round(avg_order, 2)
                ))

            logger.info(
                "customer_analytics_calculated",
                total_customers=len(customer_stats),
                total_revenue=float(total_revenue)
            )

            return CustomerAnalyticsResponse(
                data=summaries,
                total_customers=len(customer_stats),
                total_revenue_usd=round(total_revenue, 2),
                period_start=start_date,
                period_end=end_date
            )

        except Exception as e:
            logger.error("get_customer_analytics_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # COST ANALYTICS
    # ===================

    def get_cost_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> CostSummary:
        """
        Aggregate all shipment costs.

        Args:
            start_date: Filter shipments from this date (by created_at)
            end_date: Filter shipments until this date

        Returns:
            CostSummary with breakdown by category
        """
        logger.info(
            "getting_cost_summary",
            start_date=start_date,
            end_date=end_date
        )

        try:
            query = self.db.table("shipments").select(
                "freight_cost_usd, customs_cost_usd, duties_cost_usd, "
                "insurance_cost_usd, demurrage_cost_usd, other_costs_usd, "
                "total_cost_usd, created_at"
            )

            if start_date:
                query = query.gte("created_at", start_date.isoformat())
            if end_date:
                query = query.lte("created_at", end_date.isoformat())

            result = query.execute()

            # Initialize totals
            totals = {
                "freight": Decimal("0"),
                "customs": Decimal("0"),
                "duties": Decimal("0"),
                "insurance": Decimal("0"),
                "demurrage": Decimal("0"),
                "other": Decimal("0"),
            }
            shipment_count = 0

            for row in result.data:
                shipment_count += 1
                totals["freight"] += Decimal(str(row.get("freight_cost_usd") or 0))
                totals["customs"] += Decimal(str(row.get("customs_cost_usd") or 0))
                totals["duties"] += Decimal(str(row.get("duties_cost_usd") or 0))
                totals["insurance"] += Decimal(str(row.get("insurance_cost_usd") or 0))
                totals["demurrage"] += Decimal(str(row.get("demurrage_cost_usd") or 0))
                totals["other"] += Decimal(str(row.get("other_costs_usd") or 0))

            shipment_costs = sum(totals.values())

            # Calculate FOB cost from sales × product.fob_cost_usd
            fob_total = Decimal("0")

            # Get all tile products with FOB cost (excludes FURNITURE, SINK, SURCHARGE)
            tile_categories = [cat.value for cat in TILE_CATEGORIES]
            products_result = self.db.table("products").select("id, fob_cost_usd").eq("active", True).in_("category", tile_categories).execute()
            products_by_id = {
                p["id"]: Decimal(str(p.get("fob_cost_usd") or 0))
                for p in products_result.data
            }

            # Get all sales with quantities (with date filter if provided)
            sales_query = self.db.table("sales").select("product_id, quantity_m2")
            if start_date:
                sales_query = sales_query.gte("week_start", start_date.isoformat())
            if end_date:
                sales_query = sales_query.lte("week_start", end_date.isoformat())
            sales_result = sales_query.execute()

            for sale in sales_result.data:
                product_id = sale.get("product_id")
                qty = Decimal(str(sale.get("quantity_m2") or 0))
                fob_cost = products_by_id.get(product_id, Decimal("0"))
                fob_total += qty * fob_cost

            # Total costs = FOB + shipment costs
            total_costs = fob_total + shipment_costs

            logger.info(
                "cost_summary_calculated",
                shipment_count=shipment_count,
                fob_cost=float(fob_total),
                shipment_costs=float(shipment_costs),
                total_costs=float(total_costs)
            )

            return CostSummary(
                total_fob_usd=round(fob_total, 2),
                total_freight_usd=round(totals["freight"], 2),
                total_customs_usd=round(totals["customs"], 2),
                total_duties_usd=round(totals["duties"], 2),
                total_insurance_usd=round(totals["insurance"], 2),
                total_demurrage_usd=round(totals["demurrage"], 2),
                total_other_usd=round(totals["other"], 2),
                total_costs_usd=round(total_costs, 2),
                shipment_count=shipment_count
            )

        except Exception as e:
            logger.error("get_cost_summary_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # MARGIN ANALYTICS
    # ===================

    def get_margin_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> MarginSummary:
        """
        Calculate revenue - costs = margin.

        Args:
            start_date: Filter from this date
            end_date: Filter until this date

        Returns:
            MarginSummary with revenue, costs, and margin
        """
        logger.info(
            "getting_margin_summary",
            start_date=start_date,
            end_date=end_date
        )

        try:
            # Get total revenue from sales
            sales_query = self.db.table("sales").select("total_price_usd")
            if start_date:
                sales_query = sales_query.gte("week_start", start_date.isoformat())
            if end_date:
                sales_query = sales_query.lte("week_start", end_date.isoformat())

            sales_result = sales_query.execute()

            total_revenue = Decimal("0")
            for row in sales_result.data:
                total_revenue += Decimal(str(row.get("total_price_usd") or 0))

            # Get total costs from shipments
            cost_summary = self.get_cost_summary(start_date, end_date)
            total_costs = cost_summary.total_costs_usd

            # Calculate margin
            gross_margin = total_revenue - total_costs
            margin_pct = (
                (gross_margin / total_revenue * 100)
                if total_revenue > 0
                else Decimal("0")
            )

            logger.info(
                "margin_summary_calculated",
                revenue=float(total_revenue),
                costs=float(total_costs),
                margin=float(gross_margin)
            )

            return MarginSummary(
                total_revenue_usd=round(total_revenue, 2),
                total_costs_usd=round(total_costs, 2),
                gross_margin_usd=round(gross_margin, 2),
                margin_percentage=round(margin_pct, 2),
                period_start=start_date,
                period_end=end_date
            )

        except Exception as e:
            logger.error("get_margin_summary_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # MONEY FLOW (SANKEY)
    # ===================

    def get_money_flow(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        group_by: str = "customer"
    ) -> MoneyFlowResponse:
        """
        Get data shaped for Sankey diagram visualization.

        Returns top 5 customers or products as inflows + "Otros" for remainder.
        Returns cost categories as outflows.

        Args:
            start_date: Filter from this date
            end_date: Filter until this date
            group_by: Group inflows by "customer" or "product"

        Returns:
            MoneyFlowResponse with inflows and outflows
        """
        logger.info(
            "getting_money_flow",
            start_date=start_date,
            end_date=end_date,
            group_by=group_by
        )

        try:
            # Build inflows based on group_by parameter
            inflows: list[MoneyFlowInflow] = []
            total_revenue = Decimal("0")

            if group_by == "product":
                # Get product inflows (aggregated by SKU)
                inflows, total_revenue = self._get_product_inflows(start_date, end_date)
            else:
                # Get customer inflows (default)
                customer_data = self.get_customer_analytics(
                    start_date=start_date,
                    end_date=end_date,
                    limit=100  # Get all to calculate "Otros"
                )

                # Build inflows: top 5 customers + "Otros"
                top_5_revenue = Decimal("0")

                for customer in customer_data.data[:5]:
                    inflows.append(MoneyFlowInflow(
                        source=customer.customer_normalized,
                        amount=customer.total_revenue_usd
                    ))
                    top_5_revenue += customer.total_revenue_usd

                # Add "Otros" for remaining customers
                otros_revenue = customer_data.total_revenue_usd - top_5_revenue
                if otros_revenue > 0:
                    inflows.append(MoneyFlowInflow(
                        source="Otros",
                        amount=round(otros_revenue, 2)
                    ))

                total_revenue = customer_data.total_revenue_usd

            # Get cost summary for outflows
            cost_data = self.get_cost_summary(start_date, end_date)

            # Build outflows: cost categories (in Spanish for UI)
            outflows: list[MoneyFlowOutflow] = []

            cost_categories = [
                ("Costo Fábrica 2025", cost_data.total_fob_usd),
                ("Flete Marítimo", cost_data.total_freight_usd),
                ("Aduanas", cost_data.total_customs_usd),
                ("Impuestos", cost_data.total_duties_usd),
                ("Seguro", cost_data.total_insurance_usd),
                ("Demoras", cost_data.total_demurrage_usd),
                ("Otros Costos", cost_data.total_other_usd),
            ]

            for category, amount in cost_categories:
                if amount > 0:
                    outflows.append(MoneyFlowOutflow(
                        category=category,
                        amount=amount
                    ))

            # Calculate margin
            margin = total_revenue - cost_data.total_costs_usd

            logger.info(
                "money_flow_calculated",
                group_by=group_by,
                inflow_count=len(inflows),
                outflow_count=len(outflows),
                margin=float(margin)
            )

            return MoneyFlowResponse(
                inflows=inflows,
                outflows=outflows,
                total_revenue=total_revenue,
                total_costs=cost_data.total_costs_usd,
                margin=round(margin, 2)
            )

        except Exception as e:
            logger.error("get_money_flow_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def _get_product_inflows(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> tuple[list[MoneyFlowInflow], Decimal]:
        """
        Get inflows grouped by product SKU.

        Args:
            start_date: Filter from this date
            end_date: Filter until this date

        Returns:
            Tuple of (inflows list, total revenue)
        """
        # Get all tile products to map id -> sku (excludes FURNITURE, SINK, SURCHARGE)
        tile_categories = [cat.value for cat in TILE_CATEGORIES]
        products_result = self.db.table("products").select("id, sku").eq("active", True).in_("category", tile_categories).execute()
        products_by_id = {
            p["id"]: p.get("sku") or "Unknown"
            for p in products_result.data
        }

        # Get sales with date filters
        sales_query = self.db.table("sales").select("product_id, total_price_usd")
        if start_date:
            sales_query = sales_query.gte("week_start", start_date.isoformat())
        if end_date:
            sales_query = sales_query.lte("week_start", end_date.isoformat())

        sales_result = sales_query.execute()

        # Aggregate revenue by product SKU
        product_revenue: dict[str, Decimal] = {}
        total_revenue = Decimal("0")

        for sale in sales_result.data:
            product_id = sale.get("product_id")
            revenue = Decimal(str(sale.get("total_price_usd") or 0))
            total_revenue += revenue

            sku = products_by_id.get(product_id, "Unknown")
            if sku not in product_revenue:
                product_revenue[sku] = Decimal("0")
            product_revenue[sku] += revenue

        # Sort by revenue descending
        sorted_products = sorted(
            product_revenue.items(),
            key=lambda x: x[1],
            reverse=True
        )

        # Build inflows: top 5 products + "Otros"
        inflows: list[MoneyFlowInflow] = []
        top_5_revenue = Decimal("0")

        for sku, revenue in sorted_products[:5]:
            inflows.append(MoneyFlowInflow(
                source=sku,
                amount=round(revenue, 2)
            ))
            top_5_revenue += revenue

        # Add "Otros" for remaining products
        otros_revenue = total_revenue - top_5_revenue
        if otros_revenue > 0:
            inflows.append(MoneyFlowInflow(
                source="Otros",
                amount=round(otros_revenue, 2)
            ))

        return inflows, total_revenue

    # ===================
    # PRODUCT ANALYTICS
    # ===================

    def get_top_products(self, limit: int = 5) -> dict:
        """
        Aggregate revenue by product SKU.

        Args:
            limit: Maximum products to return (default 5)

        Returns:
            Dict with data list of product summaries
        """
        logger.info("getting_top_products", limit=limit)

        try:
            # Query all sales
            sales_result = self.db.table("sales").select(
                "product_id, total_price_usd, quantity_m2"
            ).execute()

            # Get tile product SKUs (excludes FURNITURE, SINK, SURCHARGE)
            tile_categories = [cat.value for cat in TILE_CATEGORIES]
            products_result = self.db.table("products").select("id, sku").eq("active", True).in_("category", tile_categories).execute()
            product_skus = {
                p["id"]: p.get("sku") or "Unknown"
                for p in products_result.data
            }

            # Aggregate by product
            product_totals: dict[str, dict] = {}
            for sale in sales_result.data:
                pid = sale.get("product_id")
                sku = product_skus.get(pid, "Unknown")
                if sku not in product_totals:
                    product_totals[sku] = {
                        "revenue": Decimal("0"),
                        "quantity": Decimal("0")
                    }
                product_totals[sku]["revenue"] += Decimal(str(sale.get("total_price_usd") or 0))
                product_totals[sku]["quantity"] += Decimal(str(sale.get("quantity_m2") or 0))

            # Sort and return top N
            sorted_products = sorted(
                product_totals.items(),
                key=lambda x: x[1]["revenue"],
                reverse=True
            )[:limit]

            logger.info(
                "top_products_calculated",
                product_count=len(sorted_products)
            )

            return {
                "data": [
                    {
                        "sku": sku,
                        "total_revenue_usd": str(round(data["revenue"], 2)),
                        "quantity_sold_m2": float(round(data["quantity"], 2))
                    }
                    for sku, data in sorted_products
                ]
            }

        except Exception as e:
            logger.error("get_top_products_failed", error=str(e))
            raise DatabaseError("select", str(e))

    # ===================
    # OVERVIEW
    # ===================

    def get_financial_overview(self) -> FinancialOverview:
        """
        Get high-level financial metrics for dashboard.

        Returns:
            FinancialOverview with revenue, costs, margin, top customers
        """
        logger.info("getting_financial_overview")

        try:
            # Get margin summary (includes revenue and costs)
            margin_data = self.get_margin_summary()

            # Get top 5 customers
            customer_data = self.get_customer_analytics(limit=5)

            # Get cost breakdown
            cost_data = self.get_cost_summary()

            # Calculate margin percentage
            margin_pct = (
                (margin_data.gross_margin_usd / margin_data.total_revenue_usd * 100)
                if margin_data.total_revenue_usd > 0
                else Decimal("0")
            )

            logger.info(
                "financial_overview_calculated",
                revenue=float(margin_data.total_revenue_usd),
                costs=float(margin_data.total_costs_usd)
            )

            return FinancialOverview(
                revenue=margin_data.total_revenue_usd,
                costs=margin_data.total_costs_usd,
                margin=margin_data.gross_margin_usd,
                margin_pct=round(margin_pct, 2),
                top_customers=customer_data.data,
                cost_breakdown=cost_data
            )

        except Exception as e:
            logger.error("get_financial_overview_failed", error=str(e))
            raise DatabaseError("select", str(e))


# Singleton instance
_analytics_service: Optional[AnalyticsService] = None


def get_analytics_service() -> AnalyticsService:
    """Get or create AnalyticsService instance."""
    global _analytics_service
    if _analytics_service is None:
        _analytics_service = AnalyticsService()
    return _analytics_service
