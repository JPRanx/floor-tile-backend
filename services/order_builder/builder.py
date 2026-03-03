from typing import Optional
from decimal import Decimal
from datetime import date
import math
import structlog

from models.order_builder import (
    OrderBuilderProduct,
    CalculationBreakdown,
    PrimaryFactor,
    AvailabilityBreakdown,
    FullCalculationBreakdown,
    CoverageCalculation,
    CustomerDemandCalculation,
    SelectionCalculation,
    ProductScore,
    ProductReasoningDisplay,
    DominantFactor,
    Urgency,
    ProductReasoning,
    StockAnalysis,
    DemandAnalysis,
    QuantityReasoning,
)
from models.recommendation import RecommendationPriority
from services.order_builder.constants import ProductAnalysis, PALLETS_PER_CONTAINER, WAREHOUSE_CAPACITY
from config.shipping import M2_PER_PALLET, CONTAINER_MAX_WEIGHT_KG, DEFAULT_WEIGHT_PER_M2_KG, ORDERING_CYCLE_DAYS
from services.config_service import get_config_service

logger = structlog.get_logger(__name__)


class BuilderMixin:
    """Product construction from analysis, availability breakdown, calculation breakdown."""

    def _determine_primary_factor(
        self,
        days_of_stock: Optional[int],
        trend_pct: Decimal,
        velocity: Decimal,
        days_to_boat: int
    ) -> str:
        """
        Determine the primary factor driving this product's recommendation.

        Returns one of: LOW_STOCK, TRENDING_UP, OVERSTOCKED, DECLINING, NO_SALES, NO_DATA, STABLE
        """
        # No sales data
        if velocity is None or velocity == 0:
            return PrimaryFactor.NO_SALES.value

        # No stock data
        if days_of_stock is None:
            return PrimaryFactor.NO_DATA.value

        # Overstocked with declining demand
        if days_of_stock > 180 and trend_pct < -25:
            return PrimaryFactor.OVERSTOCKED.value

        # Significant demand decline even with moderate stock
        if days_of_stock > 90 and trend_pct < -50:
            return PrimaryFactor.DECLINING.value

        # Low stock - will stockout before boat or within 14 days
        if days_of_stock < 14 or days_of_stock < days_to_boat:
            return PrimaryFactor.LOW_STOCK.value

        # Strong upward trend
        if trend_pct > 30:
            return PrimaryFactor.TRENDING_UP.value

        return PrimaryFactor.STABLE.value

    # ===================
    # AVAILABILITY BREAKDOWN
    # ===================

    def _calculate_availability_breakdown(
        self,
        factory_available_m2: Decimal,
        production_status: str,
        production_requested_m2: Decimal,
        production_completed_m2: Decimal,
        production_estimated_ready: Optional[date],
        order_deadline: date,
        suggested_pallets: int,
    ) -> AvailabilityBreakdown:
        """
        Calculate full availability breakdown for a product.

        Args:
            factory_available_m2: Current SIESA finished goods stock
            production_status: 'scheduled', 'in_progress', 'completed', 'not_scheduled'
            production_requested_m2: Total m² requested for this production run
            production_completed_m2: M² already completed in production
            production_estimated_ready: When production is expected to complete
            order_deadline: When order must be placed for this boat
            suggested_pallets: System's recommended order quantity

        Returns:
            AvailabilityBreakdown with all fields populated
        """
        suggested_m2 = Decimal(suggested_pallets) * M2_PER_PALLET

        # SIESA: Current finished goods at factory
        # NOTE: SIESA = finished goods = WHERE completed production goes
        # Completed production IS already in SIESA (or will be very soon)
        siesa_now = factory_available_m2

        # Production completing before deadline
        # Count scheduled + in_progress production that delivers before the boat loads
        # Do NOT count "completed" — already in SIESA (factory_available_m2)
        production_completing = Decimal("0")

        if production_status == "in_progress":
            if production_estimated_ready and production_estimated_ready <= order_deadline:
                # In-progress: remaining portion not yet in SIESA
                production_completing = max(Decimal("0"), production_requested_m2 - production_completed_m2)
        elif production_status == "scheduled":
            if production_estimated_ready and production_estimated_ready <= order_deadline:
                # Scheduled: full amount will be produced (nothing in SIESA yet)
                production_completing = production_requested_m2

        # Total available
        total_available = siesa_now + production_completing

        # Gap analysis
        can_fulfill = total_available >= suggested_m2
        shortfall = max(Decimal("0"), suggested_m2 - total_available)

        # Build shortfall note
        shortfall_note = None
        if shortfall > 0:
            shortfall_note = f"{int(shortfall):,} m² needs future production"
        elif production_completing > 0:
            shortfall_note = f"Includes {int(production_completing):,} m² from production"

        return AvailabilityBreakdown(
            siesa_now_m2=siesa_now,
            production_completing_m2=production_completing,
            total_available_m2=total_available,
            suggested_order_m2=suggested_m2,
            shortfall_m2=shortfall,
            can_fulfill=can_fulfill,
            shortfall_note=shortfall_note,
        )

    def _build_full_calculation_breakdown(
        self,
        analysis: ProductAnalysis,
        days_to_cover: int,
        daily_velocity_m2: Decimal,
        velocity_source: str,
        velocity_change_pct: Decimal,
        factory_available_m2: Decimal,
        final_selected_pallets: int,
        minimum_applied: bool = False,
        selection_constraint_note: Optional[str] = None,
    ) -> FullCalculationBreakdown:
        """Build complete calculation breakdown from ProductAnalysis. Zero forks."""
        a = analysis
        target_days = days_to_cover + a.buffer_days

        # === COVERAGE ===
        need_for_target = daily_velocity_m2 * Decimal(target_days)
        trend_adj_m2 = need_for_target * (a.trend_adjustment_pct / Decimal("100"))
        adjusted_need = need_for_target + trend_adj_m2
        coverage_gap_m2 = max(
            Decimal("0"),
            adjusted_need - a.minus_current - a.minus_incoming - a.pending_order_m2
        )

        # FS override: use FS suggested_pallets directly (already cascade-aware)
        if a.uses_projection:
            coverage_suggested_pallets = a.final_suggestion_pallets
        else:
            coverage_suggested_pallets = math.ceil(float(coverage_gap_m2 / M2_PER_PALLET)) if coverage_gap_m2 > 0 else 0
        coverage_suggested_m2 = Decimal(coverage_suggested_pallets) * M2_PER_PALLET

        coverage = CoverageCalculation(
            target_coverage_days=target_days,
            days_to_warehouse=days_to_cover,
            buffer_days=a.buffer_days,
            velocity_m2_per_day=daily_velocity_m2,
            velocity_source=velocity_source,
            need_for_target_m2=round(need_for_target, 2),
            trend_direction=a.trend_direction,
            velocity_change_pct=velocity_change_pct,
            trend_adjustment_pct=a.trend_adjustment_pct,
            trend_adjustment_m2=round(trend_adj_m2, 2),
            adjusted_need_m2=round(adjusted_need, 2),
            warehouse_m2=a.minus_current,
            in_transit_m2=a.minus_incoming,
            pending_order_m2=a.pending_order_m2,
            uses_projection=a.uses_projection,
            projected_stock_m2=a.projected_stock_m2,
            earlier_drafts_consumed_m2=a.earlier_drafts_consumed_m2,
            coverage_gap_m2=round(coverage_gap_m2, 2),
            coverage_gap_pallets=max(0, math.ceil(float(coverage_gap_m2 / M2_PER_PALLET))) if coverage_gap_m2 > 0 else 0,
            suggested_pallets=coverage_suggested_pallets,
            suggested_m2=coverage_suggested_m2,
        )

        # === CUSTOMER DEMAND ===
        expected_orders_m2 = a.expected_customer_orders_m2
        expected_orders_pallets = math.ceil(float(expected_orders_m2 / M2_PER_PALLET)) if expected_orders_m2 > 0 else 0

        customer_demand = CustomerDemandCalculation(
            customers_expecting_count=a.customers_expecting_count,
            customers_list=a.customer_names[:5],
            expected_orders_m2=expected_orders_m2,
            expected_orders_pallets=expected_orders_pallets,
            customer_breakdown=[{"name": n, "tier": "?", "days_overdue": 0} for n in a.customer_names[:5]],
            suggested_pallets=expected_orders_pallets,
            customer_demand_score=a.customer_demand_score,
        )

        # === SELECTION ===
        combined = max(coverage_suggested_pallets, expected_orders_pallets)
        if coverage_suggested_pallets > expected_orders_pallets:
            combination_reason = "coverage_driven"
        elif expected_orders_pallets > coverage_suggested_pallets:
            combination_reason = "customer_driven"
        else:
            combination_reason = "equal"

        minimum_container_pallets = PALLETS_PER_CONTAINER
        after_minimum = combined
        if combined > 0 and combined < minimum_container_pallets and minimum_applied:
            after_minimum = minimum_container_pallets

        siesa_available_pallets = math.floor(float(factory_available_m2 / M2_PER_PALLET))
        siesa_limited = after_minimum > siesa_available_pallets > 0
        if siesa_limited:
            after_minimum = siesa_available_pallets

        reason_parts = []
        if combination_reason == "customer_driven" and a.customers_expecting_count > 0:
            reason_parts.append(f"{a.customers_expecting_count} customers expecting")
        elif combination_reason == "coverage_driven":
            reason_parts.append(f"Coverage gap ({coverage_suggested_pallets}p)")
        else:
            reason_parts.append("Combined need")
        if minimum_applied and combined < minimum_container_pallets:
            reason_parts.append("min container rule")
        if siesa_limited:
            reason_parts.append(f"capped at SIESA ({siesa_available_pallets}p)")

        constraint_notes = []
        if selection_constraint_note:
            constraint_notes.append(selection_constraint_note)
        if siesa_limited:
            constraint_notes.append(f"Limited by SIESA stock: {int(factory_available_m2):,} m²")
        if minimum_applied and combined < minimum_container_pallets:
            constraint_notes.append(f"Minimum 1 container ({minimum_container_pallets}p) applied")

        selection = SelectionCalculation(
            coverage_suggested_pallets=coverage_suggested_pallets,
            customer_suggested_pallets=expected_orders_pallets,
            combined_pallets=combined,
            combination_reason=combination_reason,
            minimum_container_applied=minimum_applied and combined < minimum_container_pallets,
            minimum_container_pallets=minimum_container_pallets,
            after_minimum_pallets=after_minimum,
            siesa_available_m2=factory_available_m2,
            siesa_available_pallets=siesa_available_pallets,
            siesa_limited=siesa_limited,
            final_selected_pallets=final_selected_pallets,
            final_selected_m2=Decimal(final_selected_pallets) * M2_PER_PALLET,
            selection_reason=" + ".join(reason_parts) if reason_parts else "Standard calculation",
            constraint_notes=constraint_notes,
        )

        # Summary sentence
        if coverage_suggested_pallets == 0 and a.customers_expecting_count > 0:
            summary = f"Selected {final_selected_pallets}p: 0p coverage + {a.customers_expecting_count} customers expecting"
        elif a.customers_expecting_count == 0 and coverage_suggested_pallets > 0:
            summary = f"Selected {final_selected_pallets}p: {coverage_suggested_pallets}p coverage gap"
        else:
            summary = f"Selected {final_selected_pallets}p: {coverage_suggested_pallets}p coverage + {a.customers_expecting_count} customers"
        if siesa_limited:
            summary += " (capped at SIESA)"

        return FullCalculationBreakdown(
            coverage=coverage,
            customer_demand=customer_demand,
            selection=selection,
            summary_sentence=summary,
        )

    def _build_product_from_analysis(
        self,
        product_rec,
        analysis: ProductAnalysis,
        days_to_cover: int,
        velocity_change_pct: Decimal,
        daily_velocity_m2: Decimal,
        velocity_90d_m2: Decimal,
        velocity_180d_m2: Decimal,
        velocity_trend_signal: str,
        velocity_trend_ratio: Decimal,
        factory_status_map: dict,
        factory_availability_map: dict,
        production_schedule_map: dict,
        committed_map: dict,
        unfulfilled_map: dict,
        boat_departure: Optional[date],
        order_deadline: Optional[date],
        pallet_factor: Decimal,
        projection: Optional[dict],
    ) -> OrderBuilderProduct:
        """Build OrderBuilderProduct from ProductAnalysis. Zero forks."""
        a = analysis  # short alias

        # Effective priority: derive entirely from analysis
        suggested = a.final_suggestion_pallets
        coverage_gap_pallets = max(0, math.ceil(float(a.adjusted_coverage_gap / pallet_factor))) if a.adjusted_coverage_gap > 0 else 0

        if suggested == 0:
            effective_priority = "WELL_COVERED"
        elif a.urgency in ("critical", "urgent"):
            effective_priority = "HIGH_PRIORITY"
        elif a.urgency == "soon":
            effective_priority = "CONSIDER"
        elif a.days_of_stock is None:
            effective_priority = "YOUR_CALL"
        else:
            effective_priority = "WELL_COVERED"

        # Primary factor
        primary_factor = self._determine_primary_factor(
            days_of_stock=a.days_of_stock,
            trend_pct=velocity_change_pct,
            velocity=daily_velocity_m2,
            days_to_boat=days_to_cover,
        )

        # Gap days
        gap_days = None
        if a.days_of_stock is not None:
            gap_days = Decimal(str(a.days_of_stock)) - Decimal(str(days_to_cover))

        # Exclusion reason
        exclusion_reason = None
        if suggested == 0:
            factor_to_reason = {
                PrimaryFactor.OVERSTOCKED.value: "OVERSTOCKED",
                PrimaryFactor.NO_SALES.value: "NO_SALES",
                PrimaryFactor.DECLINING.value: "DECLINING",
                PrimaryFactor.NO_DATA.value: "NO_DATA",
            }
            exclusion_reason = factor_to_reason.get(primary_factor)

        # Reasoning
        reasoning = ProductReasoning(
            primary_factor=primary_factor,
            stock=StockAnalysis(
                current_m2=a.minus_current,
                days_of_stock=Decimal(str(a.days_of_stock)) if a.days_of_stock is not None else None,
                days_to_boat=days_to_cover,
                gap_days=gap_days,
            ),
            demand=DemandAnalysis(
                velocity_m2_day=daily_velocity_m2,
                trend_pct=velocity_change_pct,
                trend_direction=a.trend_direction,
                sales_rank=None,
            ),
            quantity=QuantityReasoning(
                target_coverage_days=a.total_coverage_days,
                m2_needed=round(a.adjusted_quantity_m2, 2),
                m2_in_transit=a.minus_incoming,
                m2_in_stock=a.minus_current,
                m2_to_order=round(a.final_suggestion_m2, 2),
            ),
            exclusion_reason=exclusion_reason,
        )

        # Expected orders note
        expected_orders_note = None
        if a.expected_customer_orders_m2 > 0 and a.customers_expecting_count > 0:
            names_str = ", ".join(a.customer_names[:3])
            if len(a.customer_names) > 3:
                names_str += f" +{len(a.customer_names) - 3}"
            expected_orders_note = (
                f"Includes {int(a.expected_customer_orders_m2):,} m² expected from "
                f"{a.customers_expecting_count} customer(s): {names_str}"
            )

        # Factory production status
        factory_info = factory_status_map.get(product_rec.product_id)
        factory_status = "not_scheduled"
        factory_production_date = None
        factory_production_m2 = None
        days_until_factory_ready = None
        factory_ready_before_boat = None
        factory_timing_message = None

        if factory_info:
            factory_status = factory_info.status.value if hasattr(factory_info.status, 'value') else str(factory_info.status)
            factory_production_date = factory_info.production_date
            factory_production_m2 = factory_info.production_m2
            days_until_factory_ready = factory_info.days_until_ready
            factory_ready_before_boat = factory_info.ready_before_boat
            factory_timing_message = factory_info.timing_message

        # Factory availability (cascade-aware for display — matches FS and availability_breakdown)
        factory_avail = factory_availability_map.get(product_rec.product_id, {})
        factory_largest_lot_m2 = factory_avail.get("factory_largest_lot_m2")
        factory_largest_lot_code = factory_avail.get("factory_largest_lot_code")
        factory_lot_count = factory_avail.get("factory_lot_count", 0)

        # For computation (cascade-aware), use analysis value
        factory_for_computation = a.factory_cascade_m2

        # Factory fill status (uses cascade-aware factory m2)
        suggested_m2 = a.final_suggestion_m2
        if factory_for_computation <= 0:
            factory_fill_status = "no_stock"
            factory_fill_message = "No stock at factory"
        elif suggested_m2 <= 0:
            factory_fill_status = "available"
            factory_fill_message = f"{int(factory_for_computation):,} m² available at SIESA"
        elif factory_largest_lot_m2 and suggested_m2 <= factory_largest_lot_m2:
            factory_fill_status = "single_lot"
            factory_fill_message = f"Can fill from single lot ({factory_largest_lot_code})"
        elif suggested_m2 <= factory_for_computation:
            factory_fill_status = "mixed_lots"
            largest_str = f"{int(factory_largest_lot_m2):,}" if factory_largest_lot_m2 else "?"
            factory_fill_message = f"Will need mixed lots (largest: {largest_str} m²)"
        else:
            shortfall = suggested_m2 - factory_for_computation
            factory_fill_status = "partial_available"
            factory_fill_message = f"{int(factory_for_computation):,} m² available, need {int(shortfall):,} m² more"

        # Production schedule
        prod_schedule = production_schedule_map.get(product_rec.sku)
        production_status = "not_scheduled"
        production_requested_m2 = Decimal("0")
        production_completed_m2 = Decimal("0")
        production_can_add_more = False
        production_estimated_ready = None
        production_add_more_m2 = Decimal("0")
        production_add_more_alert = None

        if prod_schedule:
            production_status = prod_schedule.status.value if hasattr(prod_schedule.status, 'value') else str(prod_schedule.status)
            production_requested_m2 = prod_schedule.requested_m2 or Decimal("0")
            production_completed_m2 = prod_schedule.completed_m2 or Decimal("0")
            production_can_add_more = prod_schedule.can_add_more
            production_estimated_ready = prod_schedule.estimated_delivery_date

            if production_can_add_more and suggested_m2 > production_requested_m2:
                gap_m2 = suggested_m2 - production_requested_m2
                production_add_more_m2 = gap_m2
                production_add_more_alert = f"Add {int(gap_m2):,} m² before production starts!"

        # Availability breakdown
        availability_breakdown = None
        if order_deadline:
            availability_breakdown = self._calculate_availability_breakdown(
                factory_available_m2=factory_for_computation,
                production_status=production_status,
                production_requested_m2=production_requested_m2,
                production_completed_m2=production_completed_m2,
                production_estimated_ready=production_estimated_ready,
                order_deadline=order_deadline,
                suggested_pallets=suggested,
            )

        # CalculationBreakdown
        breakdown = None
        if daily_velocity_m2 > 0:
            breakdown = CalculationBreakdown(
                lead_time_days=a.lead_time_days_for_breakdown,
                ordering_cycle_days=a.ordering_cycle_days_for_breakdown,
                daily_velocity_m2=daily_velocity_m2,
                base_quantity_m2=round(a.base_quantity_m2, 2),
                trend_adjustment_m2=round(a.trend_adjustment_m2, 2),
                trend_adjustment_pct=round(a.trend_adjustment_pct, 1),
                minus_current_stock_m2=a.minus_current,
                minus_incoming_m2=a.minus_incoming,
                final_suggestion_m2=round(a.final_suggestion_m2, 2),
                final_suggestion_pallets=a.final_suggestion_pallets,
                uses_projection=a.uses_projection,
                projected_stock_m2=a.projected_stock_m2,
                earlier_drafts_consumed_m2=a.earlier_drafts_consumed_m2,
            )

        # Full calculation breakdown
        full_calculation_breakdown = self._build_full_calculation_breakdown(
            analysis=a,
            days_to_cover=days_to_cover,
            daily_velocity_m2=daily_velocity_m2,
            velocity_source="90d" if velocity_90d_m2 > 0 else "180d",
            velocity_change_pct=velocity_change_pct,
            factory_available_m2=factory_for_computation,
            final_selected_pallets=suggested,
        )

        # Weight
        product_weight_per_m2, _ = get_config_service().get_product_physics(product_rec.category)

        product = OrderBuilderProduct(
            product_id=product_rec.product_id,
            sku=product_rec.sku,
            description=None,
            weight_per_m2_kg=product_weight_per_m2,
            priority=effective_priority,
            action_type=self._derive_action_type(a.urgency, suggested),
            current_stock_m2=product_rec.warehouse_m2,
            in_transit_m2=product_rec.in_transit_m2,
            pending_order_m2=a.pending_order_m2,
            pending_order_pallets=a.pending_order_pallets,
            pending_order_boat=a.pending_order_boat,
            days_to_cover=days_to_cover,
            total_demand_m2=round(a.adjusted_quantity_m2, 2),
            coverage_gap_m2=a.adjusted_coverage_gap,
            coverage_gap_pallets=coverage_gap_pallets,
            suggested_pallets=suggested,
            confidence=product_rec.confidence.value,
            confidence_reason=product_rec.confidence_reason,
            unique_customers=product_rec.unique_customers,
            top_customer_name=product_rec.top_customer_name,
            top_customer_share=product_rec.top_customer_share,
            factory_status=factory_status,
            factory_production_date=factory_production_date,
            factory_production_m2=factory_production_m2,
            days_until_factory_ready=days_until_factory_ready,
            factory_ready_before_boat=factory_ready_before_boat,
            factory_timing_message=factory_timing_message,
            factory_available_m2=factory_for_computation,
            factory_largest_lot_m2=factory_largest_lot_m2,
            factory_largest_lot_code=factory_largest_lot_code,
            factory_lot_count=factory_lot_count,
            factory_fill_status=factory_fill_status,
            factory_fill_message=factory_fill_message,
            production_status=production_status,
            production_requested_m2=production_requested_m2,
            production_completed_m2=production_completed_m2,
            production_can_add_more=production_can_add_more,
            production_estimated_ready=production_estimated_ready,
            production_add_more_m2=production_add_more_m2,
            production_add_more_alert=production_add_more_alert,
            urgency=a.urgency,
            days_of_stock=a.days_of_stock,
            trend_direction=a.trend_direction,
            trend_strength=a.trend_strength,
            velocity_change_pct=velocity_change_pct,
            daily_velocity_m2=daily_velocity_m2,
            velocity_90d_m2=velocity_90d_m2,
            velocity_180d_m2=velocity_180d_m2,
            velocity_trend_signal=velocity_trend_signal,
            velocity_trend_ratio=velocity_trend_ratio,
            calculation_breakdown=breakdown,
            reasoning=reasoning,
            customer_demand_score=a.customer_demand_score,
            customers_expecting_count=a.customers_expecting_count,
            expected_customer_orders_m2=a.expected_customer_orders_m2,
            expected_orders_note=expected_orders_note,
            is_selected=False,
            selected_pallets=0,
            availability_breakdown=availability_breakdown,
            full_calculation_breakdown=full_calculation_breakdown,
            projected_stock_m2=a.projected_stock_m2,
            earlier_drafts_consumed_m2=a.earlier_drafts_consumed_m2,
            uses_forward_simulation=a.uses_projection,
            committed_orders_m2=float(committed_map.get(product_rec.product_id, {}).get("total_m2", 0)),
            committed_orders_customer=committed_map.get(product_rec.product_id, {}).get("customer"),
            committed_orders_count=committed_map.get(product_rec.product_id, {}).get("count", 0),
            unfulfilled_demand_m2=float(unfulfilled_map.get(product_rec.product_id, Decimal("0"))),
            has_unfulfilled_demand=unfulfilled_map.get(product_rec.product_id, Decimal("0")) > 0,
            pallet_conversion_factor=pallet_factor,
        )

        # Score and reasoning display (reads from fully-built product)
        product.score = self._calculate_priority_score(product)
        product.reasoning_display = self._generate_product_reasoning_display(product)

        return product
