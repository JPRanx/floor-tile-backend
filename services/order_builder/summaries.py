from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
import math
import structlog

from models.order_builder import (
    OrderBuilderProduct,
    OrderBuilderSummary,
    OrderBuilderBoat,
    WarehouseOrderSummary,
    AddToProductionSummary,
    AddToProductionItem,
    FactoryRequestSummary,
    FactoryRequestItem,
)
from services.order_builder.constants import (
    PALLETS_PER_CONTAINER,
    MIN_CONTAINER_M2,
    LOW_VOLUME_THRESHOLD_DAYS,
    WAREHOUSE_CAPACITY,
    _get_next_monday,
)
from config.shipping import M2_PER_PALLET, CONTAINER_MAX_WEIGHT_KG, DEFAULT_WEIGHT_PER_M2_KG
from config import settings

logger = structlog.get_logger(__name__)


class SummariesMixin:
    """Order summary and three-section summaries."""

    def _calculate_summary(
        self,
        products: list[OrderBuilderProduct],
        boat_max_containers: int,
        inventory_snapshots: Optional[list] = None,
        warehouse_at_arrival: Optional[int] = None,
    ) -> OrderBuilderSummary:
        """Calculate order summary from selected products with weight-based container limits."""
        # Get current warehouse level (use cached snapshot if provided)
        if inventory_snapshots is None:
            inventory_snapshots = self.inventory_service.get_latest()
        warehouse_current_m2 = sum(
            Decimal(str(inv.warehouse_qty))
            for inv in inventory_snapshots
        )
        warehouse_current_pallets = int(warehouse_current_m2 / M2_PER_PALLET)

        # Calculate selection totals
        selected = [p for p in products if p.is_selected]
        total_pallets = sum(p.selected_pallets for p in selected)

        # For unit-based products, use per-product pallet_conversion_factor
        # For tiles, fall back to M2_PER_PALLET
        total_m2 = Decimal("0")
        for p in selected:
            pcf = p.pallet_conversion_factor if p.pallet_conversion_factor else M2_PER_PALLET
            total_m2 += Decimal(p.selected_pallets) * pcf

        # Calculate weight-based container requirements
        # Each product may have different weight per m² (future support)
        # For unit-based products, weight_per_m2_kg is reinterpreted as weight per unit
        total_weight_kg = Decimal("0")
        for p in selected:
            pcf = p.pallet_conversion_factor if p.pallet_conversion_factor else M2_PER_PALLET
            product_qty = Decimal(p.selected_pallets) * pcf
            weight_per_unit = p.weight_per_m2_kg or DEFAULT_WEIGHT_PER_M2_KG
            product_weight = product_qty * weight_per_unit
            total_weight_kg += product_weight
            # Update product's total_weight_kg for UI display
            p.total_weight_kg = product_weight

        # Containers by pallet count (physical limit)
        containers_by_pallets = math.ceil(total_pallets / PALLETS_PER_CONTAINER) if total_pallets > 0 else 0

        # Containers by weight (27,500 kg limit per container)
        containers_by_weight = math.ceil(float(total_weight_kg) / CONTAINER_MAX_WEIGHT_KG) if total_weight_kg > 0 else 0

        # Total containers = max of both (weight is typically the constraint)
        # With standard tiles: 14 pallets × 134.4 m² × 14.90 kg/m² = 28,036 kg > 27,500 kg
        total_containers = max(containers_by_pallets, containers_by_weight)
        weight_is_limiting = containers_by_weight > containers_by_pallets

        # Warehouse after delivery (cascade-aware if available)
        warehouse_base = warehouse_at_arrival if warehouse_at_arrival is not None else warehouse_current_pallets
        warehouse_after = warehouse_base + total_pallets
        utilization_after = Decimal(warehouse_after) / Decimal(WAREHOUSE_CAPACITY) * 100

        return OrderBuilderSummary(
            total_pallets=total_pallets,
            total_containers=total_containers,
            total_m2=total_m2,
            # Weight-based calculations
            total_weight_kg=round(total_weight_kg, 2),
            containers_by_pallets=containers_by_pallets,
            containers_by_weight=containers_by_weight,
            weight_is_limiting=weight_is_limiting,
            # Capacity
            boat_max_containers=boat_max_containers,
            boat_remaining_containers=max(0, boat_max_containers - total_containers),
            warehouse_current_pallets=warehouse_current_pallets,
            warehouse_capacity=WAREHOUSE_CAPACITY,
            warehouse_after_delivery=warehouse_after,
            warehouse_utilization_after=round(utilization_after, 1),
            alerts=[],  # Populated later
        )

    def _calculate_section_summaries(
        self,
        all_products: list[OrderBuilderProduct],
        boat: OrderBuilderBoat,
        num_bls: int,
    ) -> tuple[WarehouseOrderSummary, AddToProductionSummary, FactoryRequestSummary]:
        """
        Calculate summaries for the three-section Order Builder view.

        Section 1: Warehouse Order — Products with SIESA stock available now
        Section 2: Add to Production — Items in scheduled production that can have more added
        Section 3: Factory Request — Products needing new production requests

        Args:
            all_products: All Order Builder products with production/factory data
            boat: Target boat info
            num_bls: Number of BLs (determines capacity)

        Returns:
            Tuple of (warehouse_summary, add_to_production_summary, factory_request_summary)
        """
        logger.debug("calculating_section_summaries", product_count=len(all_products))

        # Get boat schedules for matching production ready dates
        available_boats = self.boat_service.get_available(limit=5)
        boat_schedules = [
            (b.vessel_name, b.departure_date, b.order_deadline)
            for b in available_boats
        ]

        # === SECTION 1: WAREHOUSE ORDER ===
        # Products where factory_available_m2 > 0 (can ship from SIESA now)
        warehouse_products = [
            p for p in all_products
            if p.factory_available_m2 and p.factory_available_m2 > 0
        ]

        selected_warehouse = [p for p in warehouse_products if p.is_selected]
        warehouse_total_pallets = sum(p.selected_pallets for p in selected_warehouse)
        warehouse_total_m2 = Decimal(str(warehouse_total_pallets)) * M2_PER_PALLET
        warehouse_total_containers = math.ceil(warehouse_total_pallets / PALLETS_PER_CONTAINER)
        # Use per-product weight (already calculated in summary step)
        warehouse_total_weight = sum(p.total_weight_kg for p in selected_warehouse)

        warehouse_summary = WarehouseOrderSummary(
            product_count=len(warehouse_products),
            selected_count=len(selected_warehouse),
            total_m2=warehouse_total_m2,
            total_pallets=warehouse_total_pallets,
            total_containers=warehouse_total_containers,
            total_weight_kg=warehouse_total_weight,
            bl_count=num_bls,
            boat_name=boat.name,
            boat_departure=boat.departure_date,
        )

        # === SECTION 2: ADD TO PRODUCTION ===
        # Products where production_can_add_more=True AND suggested > requested
        add_to_production_items: list[AddToProductionItem] = []

        for p in all_products:
            if not p.production_can_add_more:
                continue

            # Calculate how much more to add
            # suggested_pallets is what Order Builder recommends, convert to m2
            suggested_m2 = Decimal(str(p.suggested_pallets)) * M2_PER_PALLET
            requested_m2 = p.production_requested_m2 or Decimal("0")
            additional_m2 = suggested_m2 - requested_m2

            if additional_m2 <= 0:
                continue

            additional_pallets = int(additional_m2 / M2_PER_PALLET)
            if additional_pallets <= 0:
                continue

            # Find matching boat based on estimated ready date
            target_boat_name = None
            target_boat_departure = None
            estimated_ready = p.production_estimated_ready

            if estimated_ready and boat_schedules:
                # Find first boat whose order deadline is after the ready date
                for b_name, b_departure, b_deadline in boat_schedules:
                    if estimated_ready <= b_deadline:
                        target_boat_name = b_name
                        target_boat_departure = b_departure
                        break

            # Get score from product
            score = p.score.total if p.score else 0
            is_critical = score >= 85

            # Get referencia from production data (or use SKU)
            referencia = p.sku  # Default to SKU

            add_to_production_items.append(AddToProductionItem(
                product_id=p.product_id,
                sku=p.sku,
                description=p.description,
                referencia=referencia,
                current_requested_m2=requested_m2,
                suggested_total_m2=suggested_m2,
                suggested_additional_m2=additional_m2,
                suggested_additional_pallets=additional_pallets,
                estimated_ready_date=estimated_ready,
                target_boat=target_boat_name,
                target_boat_departure=target_boat_departure,
                score=score,
                is_critical=is_critical,
                is_selected=True,  # Pre-select all recommended items
            ))

        # Sort by score (critical first)
        add_to_production_items.sort(key=lambda x: x.score, reverse=True)

        # Enrich Section 2 items with piggyback history
        section2_product_ids = [item.product_id for item in add_to_production_items]
        piggyback_map: dict[str, list[dict]] = {}
        if section2_product_ids:
            try:
                from config import get_supabase_client
                db = get_supabase_client()
                history_result = db.table("piggyback_history") \
                    .select("product_id, additional_m2, created_at") \
                    .in_("product_id", section2_product_ids) \
                    .order("created_at", desc=True) \
                    .execute()
                for h in history_result.data:
                    piggyback_map.setdefault(h["product_id"], []).append(h)
            except Exception as e:
                logger.warning("piggyback_history_fetch_failed", error=str(e))

        for item in add_to_production_items:
            product_piggybacks = piggyback_map.get(item.product_id, [])
            total_piggybacked = sum(Decimal(str(h["additional_m2"])) for h in product_piggybacks)
            remaining_headroom = item.suggested_additional_m2 - total_piggybacked
            item.piggyback_history = product_piggybacks
            item.total_piggybacked_m2 = total_piggybacked
            item.remaining_headroom_m2 = max(Decimal("0"), remaining_headroom)

        total_additional_m2 = sum(item.suggested_additional_m2 for item in add_to_production_items)
        total_additional_pallets = sum(item.suggested_additional_pallets for item in add_to_production_items)
        has_critical = any(item.is_critical for item in add_to_production_items)

        # No deadline for Add to Production — user can add before production starts
        # The deadline concept was removed as production scheduling is flexible

        add_to_production_summary = AddToProductionSummary(
            product_count=len(add_to_production_items),
            total_additional_m2=total_additional_m2,
            total_additional_pallets=total_additional_pallets,
            items=add_to_production_items,
            estimated_ready_range="4-7 days",
            has_critical_items=has_critical,
            action_deadline=None,  # No deadline — can add before production starts
            action_deadline_display="",  # Empty — no deadline to display
        )

        # === SECTION 3: FACTORY REQUEST ===
        # Dynamic calculation: Project stock at arrival, determine if request needed
        # Enforce 1 container minimum PER PRODUCT with low-volume detection
        factory_request_items: list[FactoryRequestItem] = []

        # Cross-section guard: Build map of m² already allocated in Section 1
        # so we don't double-count warehouse stock that's being shipped
        section1_allocated: dict[str, Decimal] = {}
        for ws_product in selected_warehouse:
            if ws_product.selected_pallets > 0:
                section1_allocated[ws_product.product_id] = (
                    Decimal(str(ws_product.selected_pallets)) * M2_PER_PALLET
                )

        today = date.today()

        # Get average production time from completed items (dynamic, not hardcoded)
        avg_production_days = self.production_schedule_service.get_average_production_time(fallback_days=7)

        # Calculate when production would be ready if requested now
        next_monday = _get_next_monday(today)  # Factory adds items on Mondays
        estimated_ready_date_global = next_monday + timedelta(days=avg_production_days)

        # Find target boat (first boat departing after production ready)
        target_boat_global = self.boat_service.get_first_boat_after(estimated_ready_date_global)

        # Get boats after target for buffer calculation
        boats_after_target = []
        if target_boat_global:
            boats_after_target = self.boat_service.get_boats_after(target_boat_global.arrival_date, limit=2)

        # Factory lead time: how long from placing a factory order to goods
        # arriving at warehouse. Used to extend Section 3 projection horizon
        # so we catch products that survive until this boat but will stockout
        # before the NEXT factory order could arrive.
        try:
            from services.factory_service import get_factory_service
            factory_svc = get_factory_service()
            ci_factory = factory_svc.get_by_id("d45d2c83-fe4b-4f4f-8e73-a3002a84e041")
            if ci_factory:
                factory_full_lead_days = (
                    ci_factory["production_lead_days"]
                    + ci_factory["transport_to_port_days"]
                    + 9  # average sea transit days
                )
            else:
                factory_full_lead_days = 49  # 35 + 5 + 9
        except Exception:
            factory_full_lead_days = 49

        for p in all_products:
            # Skip items in scheduled production with room to add more —
            # they belong in Section 2 (piggyback), not Section 3.
            # If can_add_more=False, the run is full so we evaluate for S3.
            if p.production_status == "scheduled" and p.production_can_add_more:
                continue

            # Get product data
            warehouse_m2 = p.current_stock_m2 or Decimal("0")
            in_transit_m2 = p.in_transit_m2 or Decimal("0")
            factory_available_m2 = p.factory_available_m2 or Decimal("0")
            # in_production = only scheduled + in_progress (NOT completed)
            # Completed production is at factory SIESA, counted in factory_available_m2
            if p.production_status in ("scheduled", "in_progress"):
                in_production_m2 = p.production_requested_m2 or Decimal("0")
            else:
                in_production_m2 = Decimal("0")
            velocity_m2_day = p.daily_velocity_m2 or Decimal("0")
            score = p.score.total if p.score else 0

            # If no target boat, use fallback calculation
            if not target_boat_global:
                # Fallback: Use simple gap calculation
                suggested_m2 = Decimal(str(p.suggested_pallets)) * M2_PER_PALLET
                # Cross-section guard: Section 1 allocated m² already covers part of demand
                s1_m2 = section1_allocated.get(p.product_id, Decimal("0"))
                total_available = warehouse_m2 + in_transit_m2 + factory_available_m2 + in_production_m2 + s1_m2
                gap_m2 = suggested_m2 - total_available

                if gap_m2 <= 0:
                    continue

                gap_pallets = int(gap_m2 / M2_PER_PALLET)
                if gap_pallets <= 0:
                    continue

                # Apply minimum with low-volume check
                request_pallets, request_m2, minimum_applied, minimum_note, is_low_volume, low_volume_reason, should_request, skip_reason, days_to_consume = self._apply_container_minimum(
                    gap_m2=gap_m2,
                    gap_pallets=gap_pallets,
                    velocity_m2_day=velocity_m2_day
                )

                factory_request_items.append(FactoryRequestItem(
                    product_id=p.product_id,
                    sku=p.sku,
                    description=p.description,
                    warehouse_m2=warehouse_m2,
                    in_transit_m2=in_transit_m2,
                    factory_available_m2=factory_available_m2,
                    in_production_m2=in_production_m2,
                    suggested_m2=suggested_m2,
                    gap_m2=gap_m2,
                    gap_pallets=gap_pallets,
                    request_m2=request_m2,
                    request_pallets=request_pallets,
                    estimated_ready=f"~{avg_production_days} days",
                    avg_production_days=avg_production_days,
                    velocity_m2_day=velocity_m2_day,
                    # Buffer transparency (no target boat found)
                    buffer_days_applied=settings.production_buffer_days,
                    buffer_note="No target boat found - using fallback calculation",
                    # Low volume detection
                    days_to_consume_container=days_to_consume,
                    is_low_volume=is_low_volume,
                    low_volume_reason=low_volume_reason,
                    should_request=should_request,
                    skip_reason=skip_reason if not should_request else None,
                    urgency=p.urgency,
                    score=score,
                    is_selected=should_request,
                    minimum_applied=minimum_applied,
                    minimum_note=minimum_note,
                ))
                continue

            # Dynamic calculation with target boat
            target_boat = target_boat_global
            arrival_date = target_boat.arrival_date
            days_until_arrival = (arrival_date - today).days

            # EXTENDED HORIZON: For factory requests, project further out.
            # A factory order placed today won't arrive for ~49 days
            # (35 production + 5 transport + 9 sea transit).
            # If stock survives until this boat but runs out before the NEXT
            # factory order could replenish, we need to flag it NOW.
            extended_days = days_until_arrival + factory_full_lead_days
            consumption_extended = velocity_m2_day * Decimal(str(extended_days))

            # Pipeline: in-transit + completed production + scheduled production
            # Include scheduled production since it will arrive within our
            # extended horizon in most cases
            pipeline_m2 = in_transit_m2 + (p.production_completed_m2 or Decimal("0")) + in_production_m2

            # Cross-section guard: Section 1 allocated m² already covers part of demand
            s1_m2 = section1_allocated.get(p.product_id, Decimal("0"))

            # Project stock through the extended horizon
            projected_stock = warehouse_m2 + pipeline_m2 + s1_m2 - consumption_extended

            # If projected stock >= 0, pipeline covers demand through next order cycle
            if projected_stock >= 0:
                continue  # No request needed

            # Will stockout — calculate need
            future_gap = abs(projected_stock)

            # Add buffer until next boat after target
            if boats_after_target:
                next_boat_arrival = boats_after_target[0].arrival_date
                days_to_next = (next_boat_arrival - arrival_date).days
                buffer_m2 = velocity_m2_day * Decimal(str(days_to_next))
            else:
                buffer_m2 = velocity_m2_day * Decimal("30")  # 30-day buffer fallback

            calculated_need = future_gap + buffer_m2
            gap_pallets = max(1, int(calculated_need / M2_PER_PALLET))

            # Apply 1 container minimum with low-volume detection
            request_pallets, request_m2, minimum_applied, minimum_note, is_low_volume, low_volume_reason, should_request, skip_reason, days_to_consume = self._apply_container_minimum(
                gap_m2=calculated_need,
                gap_pallets=gap_pallets,
                velocity_m2_day=velocity_m2_day
            )

            # Format estimated ready display
            estimated_ready_display = f"{estimated_ready_date_global.strftime('%b %d')} → {target_boat.vessel_name}"

            # Calculate buffer transparency
            buffer_days = settings.production_buffer_days
            safe_ready = estimated_ready_date_global + timedelta(days=buffer_days)
            ready_str = estimated_ready_date_global.strftime("%b %d")
            safe_str = safe_ready.strftime("%b %d")
            deadline_str = target_boat.order_deadline.strftime("%b %d")
            buffer_note = (
                f"{buffer_days}-day buffer applied. "
                f"Ready {ready_str} + {buffer_days} = {safe_str}. "
                f"Deadline {deadline_str}. "
                f"Extended horizon: {extended_days} days ({days_until_arrival} to arrival + {factory_full_lead_days} factory lead)"
            )

            factory_request_items.append(FactoryRequestItem(
                product_id=p.product_id,
                sku=p.sku,
                description=p.description,
                warehouse_m2=warehouse_m2,
                in_transit_m2=in_transit_m2,
                factory_available_m2=factory_available_m2,
                in_production_m2=in_production_m2,
                suggested_m2=calculated_need,
                gap_m2=future_gap,
                gap_pallets=gap_pallets,
                request_m2=request_m2,
                request_pallets=request_pallets,
                estimated_ready=estimated_ready_display,
                avg_production_days=avg_production_days,
                estimated_ready_date=estimated_ready_date_global,
                target_boat=target_boat.vessel_name,
                target_boat_departure=target_boat.departure_date,
                target_boat_order_deadline=target_boat.order_deadline,
                arrival_date=arrival_date,
                days_until_arrival=days_until_arrival,
                # Buffer transparency
                buffer_days_applied=buffer_days,
                safe_ready_date=safe_ready,
                buffer_note=buffer_note,
                # Velocity and consumption
                velocity_m2_day=velocity_m2_day,
                consumption_until_arrival_m2=consumption_extended,
                pipeline_m2=pipeline_m2,
                projected_stock_at_arrival_m2=projected_stock,
                calculated_need_m2=calculated_need,
                days_to_consume_container=days_to_consume,
                is_low_volume=is_low_volume,
                low_volume_reason=low_volume_reason,
                should_request=should_request,
                skip_reason=skip_reason if not should_request else None,
                urgency=p.urgency,
                score=score,
                is_selected=should_request,
                minimum_applied=minimum_applied,
                minimum_note=minimum_note,
            ))

        # Sort: should_request=True first, then by urgency/score
        urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}
        factory_request_items.sort(
            key=lambda x: (
                0 if x.should_request else 1,  # Recommended first
                urgency_order.get(x.urgency, 4),
                -x.score
            )
        )

        # Calculate totals (only for items that should be requested)
        recommended_items = [item for item in factory_request_items if item.should_request]
        total_request_m2 = sum(item.request_m2 for item in recommended_items)
        total_request_pallets = sum(item.request_pallets for item in recommended_items)

        # Monthly limit tracking (60k m²)
        monthly_limit = Decimal("60000")

        try:
            capacity = self.production_schedule_service.get_production_capacity()
            already_requested = capacity.already_requested_m2
        except Exception:
            already_requested = Decimal("0")

        remaining_m2 = monthly_limit - already_requested
        utilization_pct = (already_requested / monthly_limit * 100) if monthly_limit > 0 else Decimal("0")

        # Estimated ready string (dynamic)
        if target_boat_global:
            estimated_ready_str = f"{estimated_ready_date_global.strftime('%b %d')} → {target_boat_global.vessel_name}"
        else:
            estimated_ready_str = f"~{avg_production_days} days"

        # Calculate submit deadline (next Monday for factory schedule)
        submit_deadline = next_monday
        submit_deadline_display = f"Submit by {submit_deadline.strftime('%a, %b %d')}"

        factory_request_summary = FactoryRequestSummary(
            product_count=len(factory_request_items),
            total_request_m2=total_request_m2,
            total_request_pallets=total_request_pallets,
            items=factory_request_items,
            limit_m2=monthly_limit,
            utilization_pct=utilization_pct,
            remaining_m2=remaining_m2,
            estimated_ready=estimated_ready_str,
            submit_deadline=submit_deadline,
            submit_deadline_display=submit_deadline_display,
        )

        logger.info(
            "section_summaries_calculated",
            warehouse_products=len(warehouse_products),
            add_to_production_items=len(add_to_production_items),
            factory_request_items=len(factory_request_items),
            add_to_production_total_m2=float(total_additional_m2),
            factory_request_total_m2=float(total_request_m2),
        )

        return warehouse_summary, add_to_production_summary, factory_request_summary

    def _apply_container_minimum(
        self,
        gap_m2: Decimal,
        gap_pallets: int,
        velocity_m2_day: Decimal
    ) -> tuple[int, Decimal, bool, Optional[str], bool, Optional[str], bool, Optional[str], Optional[int]]:
        """
        Apply 1 container minimum rule with low-volume detection.

        Factory requires minimum 1 container (14 pallets = 1,881.6 m²) PER PRODUCT.
        Products that would take > 1 year to consume 1 container are flagged as low-volume.

        Args:
            gap_m2: Calculated gap in m²
            gap_pallets: Gap converted to pallets
            velocity_m2_day: Daily velocity for this product

        Returns:
            Tuple of:
            - request_pallets: Pallets to request
            - request_m2: m² to request
            - minimum_applied: True if rounded up to minimum
            - minimum_note: Explanation if minimum applied
            - is_low_volume: True if product is low-volume
            - low_volume_reason: Explanation for low-volume flag
            - should_request: True if should include in request
            - skip_reason: Why skipped (if should_request=False)
            - days_to_consume_container: Days to consume 1 container
        """
        # Calculate days to consume 1 container
        days_to_consume: Optional[int] = None
        if velocity_m2_day > 0:
            days_to_consume = int(MIN_CONTAINER_M2 / velocity_m2_day)
        else:
            days_to_consume = None  # No velocity = infinite

        # Case 1: Need >= 1 container
        if gap_pallets >= PALLETS_PER_CONTAINER:
            # Round UP to whole containers
            containers_needed = math.ceil(gap_pallets / PALLETS_PER_CONTAINER)
            request_pallets = containers_needed * PALLETS_PER_CONTAINER
            request_m2 = Decimal(str(request_pallets)) * M2_PER_PALLET
            return (
                request_pallets,
                request_m2,
                False,  # minimum_applied
                None,  # minimum_note
                False,  # is_low_volume
                None,  # low_volume_reason
                True,  # should_request
                None,  # skip_reason
                days_to_consume
            )

        # Case 2: Need < 1 container — check if low-volume
        if velocity_m2_day <= 0:
            # No velocity = definitely low-volume (or no sales data)
            return (
                0,
                Decimal("0"),
                False,
                None,
                True,  # is_low_volume
                "No sales velocity data. 1 container would sit indefinitely.",
                False,  # should_request
                "Low volume — no velocity data",
                None
            )

        if days_to_consume and days_to_consume > LOW_VOLUME_THRESHOLD_DAYS:
            # Low-volume: would take > 1 year to consume 1 container
            years = days_to_consume / 365
            return (
                0,
                Decimal("0"),
                False,
                None,
                True,  # is_low_volume
                f"At {float(velocity_m2_day):.1f} m²/day, 1 container would last {days_to_consume} days (~{years:.1f} years). Special order only.",
                False,  # should_request
                f"Low volume — 1 container lasts {years:.1f} years",
                days_to_consume
            )

        # Case 3: Will consume 1 container within 1 year — request it
        request_pallets = PALLETS_PER_CONTAINER
        request_m2 = MIN_CONTAINER_M2
        minimum_note = (
            f"Calculated need: {int(gap_m2)} m² → "
            f"Rounded to 1 container minimum ({int(MIN_CONTAINER_M2)} m²). "
            f"Will consume in ~{days_to_consume} days."
        )
        return (
            request_pallets,
            request_m2,
            True,  # minimum_applied
            minimum_note,
            False,  # is_low_volume
            None,
            True,  # should_request
            None,
            days_to_consume
        )
