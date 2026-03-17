from typing import Optional
from decimal import Decimal
import math
import structlog

from models.order_builder import (
    OrderBuilderProduct,
    OrderBuilderSummary,
    OrderBuilderBoat,
    WarehouseOrderSummary,
    AddToProductionSummary,
    AddToProductionItem,
)
from services.order_builder.constants import (
    PALLETS_PER_CONTAINER,
    WAREHOUSE_CAPACITY,
)
from config.shipping import M2_PER_PALLET, CONTAINER_MAX_WEIGHT_KG, DEFAULT_WEIGHT_PER_M2_KG

logger = structlog.get_logger(__name__)


class SummariesMixin:
    """Order summary and two-section summaries (Section 3 moved to Factory Request Builder)."""

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
    ) -> tuple[WarehouseOrderSummary, AddToProductionSummary]:
        """
        Calculate summaries for the two-section Order Builder view.

        Section 1: Warehouse Order — Products with SIESA stock available now
        Section 2: Add to Production — Items in scheduled production that can have more added

        Note: Section 3 (Factory Request) has been moved to Factory Request Builder.

        Args:
            all_products: All Order Builder products with production/factory data
            boat: Target boat info
            num_bls: Number of BLs (determines capacity)

        Returns:
            Tuple of (warehouse_summary, add_to_production_summary)
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

        logger.info(
            "section_summaries_calculated",
            warehouse_products=len(warehouse_products),
            add_to_production_items=len(add_to_production_items),
            add_to_production_total_m2=float(total_additional_m2),
        )

        return warehouse_summary, add_to_production_summary
