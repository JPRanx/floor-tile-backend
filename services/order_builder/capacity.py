from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
import math
import structlog

from models.order_builder import OrderBuilderProduct, ConstraintAnalysis
from services.order_builder.constants import PALLETS_PER_CONTAINER, MAX_CONTAINERS_PER_BL, WAREHOUSE_CAPACITY
from config.shipping import M2_PER_PALLET, CONTAINER_MAX_WEIGHT_KG, DEFAULT_WEIGHT_PER_M2_KG, WAREHOUSE_BUFFER_DAYS

logger = structlog.get_logger(__name__)


class CapacityMixin:
    """Warehouse capacity, BL calculations, and mode application."""

    def _get_warehouse_available_pallets(self) -> int:
        """Get available space in warehouse (in pallets)."""
        inventory_snapshots = self.inventory_service.get_latest()
        warehouse_current_m2 = sum(
            Decimal(str(inv.warehouse_qty))
            for inv in inventory_snapshots
        )
        warehouse_current_pallets = int(warehouse_current_m2 / M2_PER_PALLET)
        available = max(0, WAREHOUSE_CAPACITY - warehouse_current_pallets)
        return available

    def _get_warehouse_at_arrival(
        self, boat, inventory_snapshots, trend_data, factory_id: str
    ) -> int:
        """Compute projected warehouse pallets when this boat's goods arrive.

        Accounts for:
        - Current warehouse stock (snapshot)
        - Sales consumption during transit (velocity × days)
        - Earlier boats' deliveries arriving before this boat's departure
        """
        warehouse_current_m2 = sum(
            Decimal(str(inv.warehouse_qty)) for inv in inventory_snapshots
        )

        # Depletion: total velocity × days until warehouse arrival
        total_daily_m2 = sum(
            Decimal(str(t.get("daily_velocity_m2", 0)))
            for t in trend_data.values()
        )
        days = max(0, boat.days_until_warehouse or 0)
        depletion_m2 = total_daily_m2 * days

        # Earlier boats' deliveries: ordered/confirmed drafts arriving
        # before this boat departs (same query pattern as FS._get_in_transit_supply)
        earlier_m2 = Decimal("0")
        try:
            db = self.inventory_service.db
            drafts_result = (
                db.table("boat_factory_drafts")
                .select("id, boat_id, status")
                .eq("factory_id", factory_id)
                .in_("status", ["ordered", "confirmed"])
                .execute()
            )
            if drafts_result.data:
                filtered = [d for d in drafts_result.data if d["boat_id"] != boat.boat_id]
                if filtered:
                    boat_ids = list({d["boat_id"] for d in filtered})
                    boats_result = (
                        db.table("boat_schedules")
                        .select("id, arrival_date")
                        .in_("id", boat_ids)
                        .execute()
                    )
                    arrival_map = {b["id"]: b["arrival_date"] for b in boats_result.data}

                    draft_ids = [d["id"] for d in filtered]
                    items_result = (
                        db.table("draft_items")
                        .select("draft_id, selected_pallets")
                        .in_("draft_id", draft_ids)
                        .execute()
                    )
                    draft_to_boat = {d["id"]: d["boat_id"] for d in filtered}

                    for item in items_result.data:
                        bid = draft_to_boat.get(item["draft_id"])
                        arrival_str = arrival_map.get(bid) if bid else None
                        if not arrival_str:
                            continue
                        arrival_dt = date.fromisoformat(arrival_str)
                        warehouse_dt = arrival_dt + timedelta(days=WAREHOUSE_BUFFER_DAYS)
                        if warehouse_dt <= boat.departure_date:
                            earlier_m2 += Decimal(str(item["selected_pallets"])) * M2_PER_PALLET
        except Exception as e:
            logger.warning("warehouse_cascade_fallback", error=str(e))

        projected_m2 = warehouse_current_m2 - depletion_m2 + earlier_m2
        return max(0, int(projected_m2 / M2_PER_PALLET))

    def _apply_mode(
        self,
        products_by_priority: dict[str, list[OrderBuilderProduct]],
        num_bls: int,
        boat_max_containers: int,
        trend_data: dict[str, dict],
        warehouse_available_pallets: Optional[int] = None,
        inventory_snapshots: Optional[list] = None,
    ) -> tuple[list[OrderBuilderProduct], ConstraintAnalysis]:
        """
        Apply BL capacity logic to pre-select products.

        BL count determines capacity:
        - 1 BL  =  5 containers =  70 pallets
        - 2 BLs = 10 containers = 140 pallets
        - 3 BLs = 15 containers = 210 pallets
        - 4 BLs = 20 containers = 280 pallets
        - 5 BLs = 25 containers = 350 pallets

        Returns tuple of (products, constraint_analysis)
        """
        # BL capacity: num_bls × 5 containers × 14 pallets
        bl_capacity = num_bls * MAX_CONTAINERS_PER_BL * PALLETS_PER_CONTAINER

        boat_capacity = boat_max_containers * PALLETS_PER_CONTAINER

        # Get warehouse available if not provided
        if warehouse_available_pallets is None:
            warehouse_available_pallets = self._get_warehouse_available_pallets()

        # Calculate total needed pallets (sum of all suggestions)
        total_needed = sum(
            p.suggested_pallets
            for group in products_by_priority.values()
            for p in group
            if p.suggested_pallets > 0
        )
        total_needed_m2 = Decimal(total_needed) * M2_PER_PALLET

        # Determine limiting factor and effective limit
        # Note: boat_capacity is NOT included as a separate constraint because
        # bl_capacity already represents the logical limit (num_bls × 5 containers).
        # The boat's physical capacity (25 containers max) is handled by limiting num_bls to 5.
        constraints = {
            "bl_capacity": bl_capacity,
            "warehouse": warehouse_available_pallets,
        }
        limiting_factor = min(constraints, key=constraints.get)
        effective_limit = constraints[limiting_factor]

        # If all constraints allow more than needed, no constraint is active
        if effective_limit >= total_needed:
            limiting_factor = "none"
            effective_limit = total_needed

        max_pallets = effective_limit

        total_selected = 0
        all_products = []

        # Helper: Get effective available m² (SIESA + production completing before deadline)
        def get_effective_available(product) -> float:
            if product.availability_breakdown:
                return float(product.availability_breakdown.total_available_m2)
            return float(product.factory_available_m2 or 0)

        # Helper: Calculate max shippable pallets based on total availability
        def get_shippable_pallets(product) -> tuple[int, str]:
            """
            Returns (max_pallets, constraint_note).
            Uses total available (SIESA + production completing before deadline).
            Partial pallets are allowed for shipment orders (unlike factory requests).
            """
            available_m2 = get_effective_available(product)
            if available_m2 <= 0:
                return 0, "No stock available (SIESA + production)"

            max_pallets = max(1, int(available_m2 / float(M2_PER_PALLET)))
            note = ""
            if available_m2 < float(M2_PER_PALLET):
                note = f"Partial pallet ({int(available_m2)} m²)"
            elif product.availability_breakdown and float(product.availability_breakdown.production_completing_m2) > 0:
                note = f"Includes {int(product.availability_breakdown.production_completing_m2)} m² from production"
            return max_pallets, note

        # Single selection loop: HIGH_PRIORITY -> CONSIDER -> WELL_COVERED -> YOUR_CALL
        # Priority order determines who gets room first. Logic is identical per tier.
        for tier in ["HIGH_PRIORITY", "CONSIDER", "WELL_COVERED", "YOUR_CALL"]:
            for p in products_by_priority.get(tier, []):
                # YOUR_CALL: never auto-select
                if tier == "YOUR_CALL":
                    all_products.append(p)
                    continue

                max_shippable, constraint_note = get_shippable_pallets(p)

                # No factory stock: can't ship
                if max_shippable <= 0:
                    p.is_selected = False
                    p.selected_pallets = 0
                    p.selection_constraint_note = constraint_note
                    all_products.append(p)
                    continue

                # Nothing suggested: don't auto-select
                if p.suggested_pallets <= 0:
                    all_products.append(p)
                    continue

                # How many pallets we want, capped by factory availability
                pallets_wanted = min(p.suggested_pallets, max_shippable)
                remaining = max_pallets - total_selected

                if remaining <= 0:
                    # No room left
                    all_products.append(p)
                    continue

                # Allocate what fits
                pallets_to_add = min(pallets_wanted, remaining)
                p.is_selected = True
                p.selected_pallets = pallets_to_add
                total_selected += pallets_to_add

                if pallets_to_add < p.suggested_pallets:
                    p.selection_constraint_note = f"Capped at available ({int(get_effective_available(p)):,} m²)"

                all_products.append(p)

        # Track deferred SKUs (products that couldn't fully fit)
        deferred_skus = []
        for p in all_products:
            if p.suggested_pallets > 0:
                if not p.is_selected:
                    deferred_skus.append(p.sku)
                elif p.selected_pallets < p.suggested_pallets:
                    deferred_skus.append(p.sku)

        # Calculate deferred pallets
        deferred_pallets = max(0, total_needed - total_selected)

        # Calculate utilization percentage
        utilization_pct = Decimal("0")
        if effective_limit > 0:
            utilization_pct = round(Decimal(total_selected) / Decimal(effective_limit) * 100, 1)

        # Identify liquidation candidates (slow movers that could be cleared)
        liquidation_candidates = self._identify_liquidation_candidates(trend_data, inventory_snapshots=inventory_snapshots)
        total_liquidation_pallets = sum(c.current_pallets for c in liquidation_candidates)
        total_liquidation_m2 = sum(c.current_m2 for c in liquidation_candidates)

        # Determine if liquidation is needed and if it could help
        liquidation_needed = deferred_pallets > 0 and len(liquidation_candidates) > 0
        liquidation_could_fit = total_liquidation_pallets >= deferred_pallets

        # Build constraint analysis
        constraint_analysis = ConstraintAnalysis(
            total_needed_pallets=total_needed,
            total_needed_m2=total_needed_m2,
            warehouse_available_pallets=warehouse_available_pallets,
            boat_capacity_pallets=boat_capacity,
            bl_capacity_pallets=bl_capacity,
            limiting_factor=limiting_factor,
            effective_limit_pallets=effective_limit,
            can_order_pallets=total_selected,
            deferred_pallets=deferred_pallets,
            deferred_skus=deferred_skus[:10],  # Top 10 deferred
            constraint_utilization_pct=utilization_pct,
            # Liquidation insight
            liquidation_candidates=liquidation_candidates[:10],  # Top 10 candidates
            total_liquidation_potential_pallets=total_liquidation_pallets,
            total_liquidation_potential_m2=total_liquidation_m2,
            liquidation_needed=liquidation_needed,
            liquidation_could_fit_deferred=liquidation_could_fit,
        )

        logger.debug(
            "bl_capacity_applied",
            num_bls=num_bls,
            bl_capacity=bl_capacity,
            max_pallets=max_pallets,
            total_selected=total_selected,
            products_count=len(all_products),
            limiting_factor=limiting_factor,
            deferred_pallets=deferred_pallets,
            liquidation_candidates=len(liquidation_candidates),
            liquidation_potential=total_liquidation_pallets,
        )

        return all_products, constraint_analysis

    def _calculate_recommended_bls(
        self,
        products: list[OrderBuilderProduct]
    ) -> tuple[int, int, str]:
        """
        Calculate recommended BL count based on TRUE NEED and AVAILABLE stock.

        TRUE NEED = coverage_gap - in_transit - in_production
        (What you need, regardless of current factory stock)

        AVAILABLE = factory_available
        (What can ship right now from SIESA)

        Returns:
            tuple[int, int, str]: (recommended_bls, available_bls, reason_string)
        """
        # Calculate TRUE NEED: gap - transit - production
        total_true_need_m2 = Decimal("0")
        total_factory_available_m2 = Decimal("0")

        for p in products:
            # Coverage gap is the base need
            gap = Decimal(str(p.coverage_gap_m2 or 0))
            # Subtract what's already coming
            in_transit = Decimal(str(p.in_transit_m2 or 0))
            # in_production = only scheduled + in_progress (NOT completed)
            # Completed is at factory SIESA, counted in factory_available
            if p.production_status in ("scheduled", "in_progress"):
                in_production = Decimal(str(p.production_requested_m2 or 0))
            else:
                in_production = Decimal("0")
            # True need = gap - transit - production (floor at 0)
            true_need = max(Decimal("0"), gap - in_transit - in_production)
            total_true_need_m2 += true_need

            # Factory available is what can ship now
            factory_available = Decimal(str(p.factory_available_m2 or 0))
            total_factory_available_m2 += factory_available

        # Calculate recommended BLs based on TRUE NEED
        if total_true_need_m2 > 0:
            need_pallets = math.ceil(float(total_true_need_m2) / float(M2_PER_PALLET))
            need_containers = math.ceil(need_pallets / PALLETS_PER_CONTAINER)
            recommended_bls = max(1, min(5, math.ceil(need_containers / MAX_CONTAINERS_PER_BL)))
        else:
            recommended_bls = 1
            need_containers = 0

        # Calculate available BLs based on factory stock
        if total_factory_available_m2 > 0:
            available_pallets = math.ceil(float(total_factory_available_m2) / float(M2_PER_PALLET))
            available_containers = math.ceil(available_pallets / PALLETS_PER_CONTAINER)
            available_bls = max(1, min(5, math.ceil(available_containers / MAX_CONTAINERS_PER_BL)))
        else:
            available_bls = 0
            available_containers = 0

        # Build reason string showing BOTH need and available (Spanish)
        bl_need = "BL" if recommended_bls == 1 else "BLs"
        bl_avail = "BL" if available_bls == 1 else "BLs"
        if total_true_need_m2 <= 0:
            reason = "Sin brecha de cobertura (inventario adecuado)"
        elif total_factory_available_m2 <= 0:
            reason = f"Necesita: {recommended_bls} {bl_need} ({total_true_need_m2:,.0f} m\u00b2) \u2022 Disponible: 0 (SIESA vac\u00edo)"
        elif available_bls >= recommended_bls:
            reason = f"Necesita: {recommended_bls} {bl_need} ({total_true_need_m2:,.0f} m\u00b2) \u2022 Disponible: {available_bls} {bl_avail} ({total_factory_available_m2:,.0f} m\u00b2) \u2713"
        else:
            reason = f"Necesita: {recommended_bls} {bl_need} ({total_true_need_m2:,.0f} m\u00b2) \u2022 Disponible: {available_bls} {bl_avail} ({total_factory_available_m2:,.0f} m\u00b2)"

        return recommended_bls, available_bls, reason

    def _calculate_shippable_bls(
        self,
        products: list[OrderBuilderProduct]
    ) -> tuple[int, Decimal]:
        """
        Calculate BLs based on what's actually shippable from SIESA.

        Shippable = min(coverage_gap, factory_available) for each product
        This represents the overlap - what can actually be shipped to fill gaps.

        Returns:
            tuple[int, Decimal]: (shippable_bls, shippable_m2)
        """
        total_shippable_m2 = Decimal("0")

        for p in products:
            gap = Decimal(str(p.coverage_gap_m2 or 0))
            available = Decimal(str(p.factory_available_m2 or 0))

            # Only count if both gap and available are positive
            if gap > 0 and available > 0:
                shippable = min(gap, available)
                total_shippable_m2 += shippable

        # Convert to BLs
        if total_shippable_m2 > 0:
            ship_pallets = math.ceil(float(total_shippable_m2) / float(M2_PER_PALLET))
            ship_containers = math.ceil(ship_pallets / PALLETS_PER_CONTAINER)
            shippable_bls = max(1, min(5, math.ceil(ship_containers / MAX_CONTAINERS_PER_BL)))
        else:
            shippable_bls = 0

        return shippable_bls, total_shippable_m2
