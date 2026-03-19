"""Product analysis mixin for Order Builder service."""

from decimal import Decimal
import math
import structlog

from services.order_builder.constants import ProductAnalysis
from services.demand_intelligence import classify_urgency, compute_trend_factors

logger = structlog.get_logger(__name__)


class AnalysisMixin:
    """Product analysis: builds ProductAnalysis from FS projection data."""

    def _get_product_trends(self, prefetched_metrics=None) -> dict[str, dict]:
        """Fetch trend data via shared demand_intelligence module.

        Returns dict keyed by SKU with trend metrics including:
        - 90-day velocity (recent)
        - 180-day velocity (historical)
        - Trend signal (growing/stable/declining based on 90d vs 180d comparison)
        """
        # Delegate to shared module — pass empty products list
        # so results are keyed by SKU (not product_id)
        return compute_trend_factors(self.trend_service, [], prefetched_metrics)

    @staticmethod
    def _derive_action_type(urgency: str, suggested_pallets: int) -> str:
        """Derive action_type from urgency and suggestion."""
        if suggested_pallets == 0:
            return "WELL_STOCKED"
        if urgency in ("critical", "urgent"):
            return "ORDER_NOW"
        if urgency == "soon":
            return "ORDER_SOON"
        return "REVIEW"

    def _analyze_from_projection(
        self,
        projection: dict,
        daily_velocity_m2: Decimal,
        buffer_days: int,
        pallet_factor: Decimal,
        factory_availability_map: dict,
        pending_orders_map: dict,
        sku: str,
    ) -> ProductAnalysis:
        """Build ProductAnalysis from Forward Simulation projection.

        FS already computed everything (trend, customer demand, cascade).
        We just read its values, then subtract pending warehouse orders.
        """
        # Days of stock at arrival (FS-projected)
        proj_days = projection.get("days_of_stock_at_arrival")
        days_of_stock = int(round(proj_days)) if proj_days is not None else None
        urgency = classify_urgency(days_of_stock)

        # Trend from FS
        direction = projection.get("trend_direction", "stable")
        strength = projection.get("trend_strength", "weak")

        # Suggestion from FS
        final_suggestion_m2 = Decimal(str(projection.get("coverage_gap_m2", 0)))
        final_suggestion_pallets = projection.get("suggested_pallets", 0)

        # Breakdown display values
        buffer_days_from_fs = projection.get("buffer_days", buffer_days)
        trend_adjustment_pct = Decimal(str(projection.get("trend_adjustment_pct", 0)))
        base_quantity_m2 = daily_velocity_m2 * Decimal(buffer_days_from_fs)
        trend_adjustment_m2 = base_quantity_m2 * trend_adjustment_pct / Decimal("100")

        # Warehouse-projected for breakdown display
        supply = projection["supply_breakdown"]
        factory_supply_m2 = supply["factory_siesa_m2"] + supply["production_pipeline_m2"]
        warehouse_projected = projection["projected_stock_m2"] - factory_supply_m2
        minus_current = max(Decimal("0"), warehouse_projected)

        # Customer demand from FS
        customers_expecting_count = projection.get("customers_expecting_count", 0)
        expected_customer_orders_m2 = Decimal(str(projection.get("customer_demand_m2", 0)))
        customer_names = projection.get("customer_names", [])

        # Factory cascade-aware SIESA
        factory_cascade_m2 = supply["factory_siesa_m2"] + supply["production_pipeline_m2"]

        # Pending warehouse orders (FS doesn't know about these)
        pending_info = pending_orders_map.get(sku, {})
        pending_order_m2 = Decimal(str(pending_info.get("total_m2", 0)))
        pending_order_pallets = int(pending_info.get("total_pallets", 0))
        pending_order_boat = pending_info.get("boat_name")

        # Subtract pending from FS suggestion
        adj_suggestion_m2 = max(Decimal("0"), final_suggestion_m2 - pending_order_m2)
        adj_suggestion_pallets = max(0, math.ceil(float(adj_suggestion_m2 / pallet_factor)))

        return ProductAnalysis(
            uses_projection=True,
            days_of_stock=days_of_stock,
            urgency=urgency,
            trend_direction=direction,
            trend_strength=strength,
            base_quantity_m2=base_quantity_m2,
            trend_adjustment_m2=trend_adjustment_m2,
            trend_adjustment_pct=trend_adjustment_pct,
            adjusted_quantity_m2=base_quantity_m2 + trend_adjustment_m2,
            buffer_days=buffer_days_from_fs,
            total_coverage_days=buffer_days_from_fs,
            minus_current=minus_current,
            minus_incoming=Decimal("0"),
            pending_order_m2=pending_order_m2,
            pending_order_pallets=pending_order_pallets,
            pending_order_boat=pending_order_boat,
            final_suggestion_m2=adj_suggestion_m2,
            final_suggestion_pallets=adj_suggestion_pallets,
            adjusted_coverage_gap=Decimal(str(projection["coverage_gap_m2"])),
            customer_demand_score=projection.get("customer_demand_score", 0),
            customers_expecting_count=customers_expecting_count,
            expected_customer_orders_m2=expected_customer_orders_m2,
            customer_names=customer_names,
            factory_cascade_m2=factory_cascade_m2,
            projected_stock_m2=Decimal(str(projection["projected_stock_m2"])),
            earlier_drafts_consumed_m2=Decimal(str(projection["earlier_drafts_consumed_m2"])),
            lead_time_days_for_breakdown=0,
            ordering_cycle_days_for_breakdown=buffer_days_from_fs,
        )
