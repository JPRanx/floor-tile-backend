from typing import Optional
from decimal import Decimal
from datetime import date, timedelta

import structlog

from models.order_builder import (
    OrderBuilderProduct,
    OrderBuilderBoat,
    StabilityForecast,
    StabilityStatus,
    SupplySource,
    RecoveryStatus,
    ProductRecovery,
    StabilityBlocker,
    StabilityTimeline,
)
from config.shipping import M2_PER_PALLET

logger = structlog.get_logger(__name__)


class StabilityMixin:
    """Stability forecast calculation."""

    def _convert_fs_stability(
        self,
        projection_data: dict,
        products: list[OrderBuilderProduct],
        current_boat: OrderBuilderBoat,
    ) -> StabilityForecast:
        """
        Convert Forward Simulation stability data to OB's StabilityForecast format.

        Uses the cascade-aware stability_impact from FS so that Planning View
        and Order Builder show consistent numbers.
        """
        si = projection_data["stability_impact"]
        horizon = projection_data.get("horizon_stability", [])
        today = date.today()

        # Build lookup: SKU → OB product
        product_by_sku: dict[str, OrderBuilderProduct] = {}
        for p in products:
            product_by_sku[p.sku] = p

        total = len(products)
        recovering_skus: list[str] = si.get("recovering_products", [])
        blocked_skus: list[str] = si.get("blocked_products", [])
        stable_count = total - len(recovering_skus) - len(blocked_skus)

        # Build recovering products — find which later boat stabilizes each
        recovering_products: list[ProductRecovery] = []
        for sku in recovering_skus:
            ob_product = product_by_sku.get(sku)
            coverage_days = ob_product.days_of_stock or 0 if ob_product else 0
            stockout_date = today + timedelta(days=coverage_days) if coverage_days > 0 else today

            # Find which later boat stabilizes this product
            ship_boat_name = None
            ship_boat_departure = None
            arrival_date = None
            supply_source = SupplySource.SIESA
            supply_amount = Decimal("0")

            for hb in horizon:
                hb_si = hb.get("stability_impact", {})
                if sku in hb_si.get("stabilizes_products", []):
                    ship_boat_name = hb["boat_name"]
                    arrival_raw = hb["arrival_date"]
                    # Parse arrival date
                    if isinstance(arrival_raw, str):
                        arrival_date = date.fromisoformat(arrival_raw)
                    elif isinstance(arrival_raw, date):
                        arrival_date = arrival_raw
                    # Find supply source from that boat's product details
                    for pd in hb.get("product_details", []):
                        if pd.get("sku") == sku:
                            sb = pd.get("supply_breakdown", {})
                            siesa = float(sb.get("factory_siesa_m2", 0))
                            prod = float(sb.get("production_pipeline_m2", 0))
                            transit = float(sb.get("in_transit_m2", 0))
                            if siesa > 0:
                                supply_source = SupplySource.SIESA
                                supply_amount = Decimal(str(siesa))
                            elif prod > 0:
                                supply_source = SupplySource.PRODUCTION
                                supply_amount = Decimal(str(prod))
                            elif transit > 0:
                                supply_source = SupplySource.IN_TRANSIT
                                supply_amount = Decimal(str(transit))
                            break
                    break

            status = RecoveryStatus.SHIPPING
            if supply_source == SupplySource.PRODUCTION:
                status = RecoveryStatus.IN_PRODUCTION
            elif supply_source == SupplySource.IN_TRANSIT:
                status = RecoveryStatus.IN_TRANSIT

            status_note = f"Supply on {ship_boat_name}" if ship_boat_name else "Supply on a later boat"

            recovering_products.append(ProductRecovery(
                sku=sku,
                product_name=ob_product.description if ob_product else None,
                current_coverage_days=coverage_days,
                stockout_date=stockout_date,
                supply_source=supply_source,
                supply_amount_m2=supply_amount,
                supply_ready_date=None,
                ship_boat_name=ship_boat_name,
                ship_boat_departure=ship_boat_departure,
                arrival_date=arrival_date,
                status=status,
                status_note=status_note,
            ))

        # Build blocked products
        blockers: list[StabilityBlocker] = []
        for sku in blocked_skus:
            ob_product = product_by_sku.get(sku)
            coverage_days = ob_product.days_of_stock or 0 if ob_product else 0
            stockout_date = today + timedelta(days=coverage_days) if coverage_days > 0 else today
            velocity = ob_product.daily_velocity_m2 if ob_product else Decimal("0")

            blockers.append(StabilityBlocker(
                sku=sku,
                product_name=ob_product.description if ob_product else None,
                current_coverage_days=coverage_days,
                stockout_date=stockout_date,
                velocity_m2_per_day=velocity,
                reason="No supply scheduled on any upcoming boat",
                suggested_action="Request production from factory",
            ))

        # Build timeline from horizon boats that stabilize products
        timeline: list[StabilityTimeline] = []
        remaining_unstable = len(recovering_skus) + len(blocked_skus)
        for hb in horizon:
            hb_si = hb.get("stability_impact", {})
            stabilizes = hb_si.get("stabilizes_count", 0)
            if stabilizes > 0:
                remaining_unstable -= stabilizes
                arrival_raw = hb["arrival_date"]
                if isinstance(arrival_raw, str):
                    event_date = date.fromisoformat(arrival_raw)
                else:
                    event_date = arrival_raw
                timeline.append(StabilityTimeline(
                    date=event_date,
                    event=f"{hb['boat_name']} arrives",
                    resolved_count=stabilizes,
                    remaining_unstable=max(0, remaining_unstable),
                    resolved_skus=hb_si.get("stabilizes_products", []),
                ))

        # Determine status
        blocker_count = len(blockers)
        recovering_count = len(recovering_products)

        if blocker_count > 0:
            status_enum = StabilityStatus.BLOCKED
            stable_date = None
            stable_date_note = f"{blocker_count} product(s) need supply scheduled"
        elif recovering_count > 0:
            status_enum = StabilityStatus.RECOVERING
            arrival_dates = [rp.arrival_date for rp in recovering_products if rp.arrival_date]
            stable_date = max(arrival_dates) if arrival_dates else None
            stable_date_note = f"After {timeline[-1].event}" if timeline else None
        else:
            status_enum = StabilityStatus.STABLE
            stable_date = today
            stable_date_note = "Already stable"

        # Status message
        if status_enum == StabilityStatus.STABLE:
            status_message = "All products have adequate coverage"
        elif status_enum == StabilityStatus.BLOCKED:
            status_message = f"{blocker_count} product(s) blocked — no supply scheduled"
        elif status_enum == StabilityStatus.RECOVERING:
            status_message = f"{recovering_count} product(s) recovering, stable by {stable_date.strftime('%b %d') if stable_date else 'TBD'}"
        else:
            status_message = f"{len(recovering_skus) + len(blocked_skus)} product(s) at risk"

        # Use FS progress percentage
        recovery_progress_pct = si.get("progress_after_pct", 0)

        logger.info(
            "stability_from_fs",
            status=status_enum.value,
            stable_count=stable_count,
            recovering_count=recovering_count,
            blocker_count=blocker_count,
            progress_pct=recovery_progress_pct,
        )

        return StabilityForecast(
            status=status_enum,
            status_message=status_message,
            total_products=total,
            stable_count=stable_count,
            unstable_count=recovering_count + blocker_count,
            blocker_count=blocker_count,
            stable_date=stable_date,
            stable_date_note=stable_date_note,
            timeline=timeline,
            recovering_products=recovering_products,
            blockers=blockers,
            recovery_progress_pct=recovery_progress_pct,
        )

    def _calculate_stability_forecast(
        self,
        products: list[OrderBuilderProduct],
        current_boat: OrderBuilderBoat,
        available_boats: list,
    ) -> StabilityForecast:
        """
        Calculate when the inventory cycle will be stable.

        A cycle is "stable" when all products have adequate coverage (30+ days).
        Products with low coverage are either:
        - RECOVERING: Supply exists (SIESA or production) and will ship
        - BLOCKED: No supply scheduled, needs action

        Args:
            products: All products from Order Builder
            current_boat: The boat being planned
            available_boats: Future boats for shipping

        Returns:
            StabilityForecast with recovery timeline and blockers
        """
        logger.info(
            "calculating_stability_forecast",
            product_count=len(products),
            boat_count=len(available_boats),
        )

        today = date.today()
        STABILITY_THRESHOLD_DAYS = 30  # <30 days = unstable

        # Step 1: Classify products by stability
        stable_products = []
        unstable_products = []

        for product in products:
            coverage_days = product.days_of_stock or 0
            if coverage_days >= STABILITY_THRESHOLD_DAYS:
                stable_products.append(product)
            else:
                unstable_products.append(product)

        # If all products are stable, return stable status
        if not unstable_products:
            return StabilityForecast(
                status=StabilityStatus.STABLE,
                status_message="All products have adequate coverage (30+ days)",
                total_products=len(products),
                stable_count=len(stable_products),
                unstable_count=0,
                blocker_count=0,
                stable_date=today,
                stable_date_note="Already stable",
                timeline=[],
                recovering_products=[],
                blockers=[],
                recovery_progress_pct=100,
            )

        # Step 2: For each unstable product, determine supply source and recovery
        recovering_products: list[ProductRecovery] = []
        blockers: list[StabilityBlocker] = []

        # Build boat lookup for assignment
        # Include current boat and available future boats
        boat_list = []
        if current_boat:
            boat_list.append({
                "name": current_boat.name,
                "departure": current_boat.departure_date,
                "arrival": current_boat.arrival_date,
                "order_deadline": current_boat.order_deadline,
                "days_until_warehouse": current_boat.days_until_warehouse,
            })
        for boat in available_boats:
            # Avoid duplicates
            if boat.departure_date != current_boat.departure_date:
                # Calculate days_until_warehouse (arrival + 14 days port/trucking)
                days_until_warehouse = (boat.arrival_date - today).days + 14
                boat_list.append({
                    "name": boat.vessel_name or f"Boat {boat.departure_date}",
                    "departure": boat.departure_date,
                    "arrival": boat.arrival_date,
                    "order_deadline": boat.order_deadline,
                    "days_until_warehouse": days_until_warehouse,
                })

        for product in unstable_products:
            coverage_days = product.days_of_stock or 0
            stockout_date = today + timedelta(days=coverage_days) if coverage_days > 0 else today

            # Check SIESA availability first (fastest to ship)
            siesa_available = float(product.factory_available_m2 or 0)

            # Check production schedule
            production_ready = product.production_estimated_ready
            production_m2 = float(product.production_completed_m2 or 0) + float(product.production_requested_m2 or 0)
            production_status = product.production_status

            # Determine supply source
            if siesa_available > 0:
                # SIESA has stock - find which boat can ship it
                supply_source = SupplySource.SIESA
                supply_amount = Decimal(str(siesa_available))
                supply_ready_date = today  # SIESA is ready now

                # Find earliest boat where order deadline hasn't passed
                assigned_boat = None
                for boat in boat_list:
                    if boat["order_deadline"] >= today:
                        assigned_boat = boat
                        break

                if assigned_boat:
                    arrival_date = assigned_boat["arrival"] + timedelta(days=14)  # Add port + trucking
                    recovering_products.append(ProductRecovery(
                        sku=product.sku,
                        product_name=product.description,
                        current_coverage_days=coverage_days,
                        stockout_date=stockout_date,
                        supply_source=supply_source,
                        supply_amount_m2=supply_amount,
                        supply_ready_date=supply_ready_date,
                        ship_boat_name=assigned_boat["name"],
                        ship_boat_departure=assigned_boat["departure"],
                        arrival_date=arrival_date,
                        status=RecoveryStatus.SHIPPING,
                        status_note=f"Ships on {assigned_boat['name']}, arrives {arrival_date.strftime('%b %d')}",
                    ))
                else:
                    # No boats available - treat as blocked
                    blockers.append(StabilityBlocker(
                        sku=product.sku,
                        product_name=product.description,
                        current_coverage_days=coverage_days,
                        stockout_date=stockout_date,
                        velocity_m2_per_day=product.daily_velocity_m2,
                        reason="SIESA has stock but no boats available",
                        suggested_action="Schedule a boat shipment",
                    ))

            elif production_status in ("scheduled", "in_progress") and production_ready:
                # Production scheduled - will ship when ready
                supply_source = SupplySource.PRODUCTION
                supply_amount = Decimal(str(production_m2))
                supply_ready_date = production_ready

                # Find first boat after production completes
                assigned_boat = None
                for boat in boat_list:
                    if boat["order_deadline"] >= production_ready:
                        assigned_boat = boat
                        break

                if assigned_boat:
                    arrival_date = assigned_boat["arrival"] + timedelta(days=14)
                    recovering_products.append(ProductRecovery(
                        sku=product.sku,
                        product_name=product.description,
                        current_coverage_days=coverage_days,
                        stockout_date=stockout_date,
                        supply_source=supply_source,
                        supply_amount_m2=supply_amount,
                        supply_ready_date=supply_ready_date,
                        ship_boat_name=assigned_boat["name"],
                        ship_boat_departure=assigned_boat["departure"],
                        arrival_date=arrival_date,
                        status=RecoveryStatus.IN_PRODUCTION,
                        status_note=f"Production completes {production_ready.strftime('%b %d')}, ships on {assigned_boat['name']}",
                    ))
                else:
                    # Production scheduled but no boats after completion
                    blockers.append(StabilityBlocker(
                        sku=product.sku,
                        product_name=product.description,
                        current_coverage_days=coverage_days,
                        stockout_date=stockout_date,
                        velocity_m2_per_day=product.daily_velocity_m2,
                        reason=f"Production completes {production_ready.strftime('%b %d')} but no boats scheduled after",
                        suggested_action="Schedule a boat after production completes",
                    ))

            elif float(product.in_transit_m2 or 0) > 0:
                # In-transit supply exists - recovering
                in_transit_amount = Decimal(str(product.in_transit_m2 or 0))
                recovering_products.append(ProductRecovery(
                    sku=product.sku,
                    product_name=product.description,
                    current_coverage_days=coverage_days,
                    stockout_date=stockout_date,
                    supply_source=SupplySource.IN_TRANSIT,
                    supply_amount_m2=in_transit_amount,
                    supply_ready_date=None,
                    ship_boat_name=None,
                    ship_boat_departure=None,
                    arrival_date=None,
                    status=RecoveryStatus.IN_TRANSIT,
                    status_note=f"{in_transit_amount:,.0f} m² in transit to warehouse",
                ))

            else:
                # Truly blocked - no SIESA, no production, no in-transit
                blockers.append(StabilityBlocker(
                    sku=product.sku,
                    product_name=product.description,
                    current_coverage_days=coverage_days,
                    stockout_date=stockout_date,
                    velocity_m2_per_day=product.daily_velocity_m2,
                    reason="No stock at SIESA, no production scheduled, and nothing in transit",
                    suggested_action="Request production from factory immediately",
                ))

        # Step 3: Build timeline of recovery events
        timeline: list[StabilityTimeline] = []
        remaining_unstable = len(unstable_products)

        # Group recovering products by arrival date
        recovery_by_date: dict[date, list[str]] = {}
        for rp in recovering_products:
            if rp.arrival_date:
                if rp.arrival_date not in recovery_by_date:
                    recovery_by_date[rp.arrival_date] = []
                recovery_by_date[rp.arrival_date].append(rp.sku)

        for arrival_date in sorted(recovery_by_date.keys()):
            skus = recovery_by_date[arrival_date]
            resolved_count = len(skus)
            remaining_unstable -= resolved_count

            # Find which boat arrives on this date
            boat_name = "Shipment"
            for rp in recovering_products:
                if rp.arrival_date == arrival_date and rp.ship_boat_name:
                    boat_name = rp.ship_boat_name
                    break

            timeline.append(StabilityTimeline(
                date=arrival_date,
                event=f"{boat_name} arrives",
                resolved_count=resolved_count,
                remaining_unstable=remaining_unstable,
                resolved_skus=skus,
            ))

        # Step 4: Determine overall status and stable date
        total_unstable = len(unstable_products)
        recovering_count = len(recovering_products)
        blocker_count = len(blockers)

        if blocker_count > 0:
            status = StabilityStatus.BLOCKED
            stable_date = None
            stable_date_note = f"{blocker_count} product(s) need supply scheduled"
        elif recovering_count > 0:
            status = StabilityStatus.RECOVERING
            # Stable date is when last product recovers
            stable_date = max(rp.arrival_date for rp in recovering_products if rp.arrival_date)
            stable_date_note = f"After {timeline[-1].event}" if timeline else None
        else:
            status = StabilityStatus.UNSTABLE
            stable_date = None
            stable_date_note = "No recovery plan"

        # Calculate progress percentage
        if total_unstable > 0:
            # Progress = (stable + recovering) / total * 100
            stable_pct = (len(stable_products) / len(products)) * 100 if products else 0
            recovery_progress_pct = int(stable_pct + (recovering_count / len(products)) * 50)
        else:
            recovery_progress_pct = 100

        # Build status message
        if status == StabilityStatus.STABLE:
            status_message = "All products have adequate coverage"
        elif status == StabilityStatus.BLOCKED:
            status_message = f"{blocker_count} product(s) blocked — no supply scheduled"
        elif status == StabilityStatus.RECOVERING:
            status_message = f"{recovering_count} product(s) recovering, stable by {stable_date.strftime('%b %d') if stable_date else 'TBD'}"
        else:
            status_message = f"{total_unstable} product(s) at risk"

        logger.info(
            "stability_forecast_calculated",
            status=status.value,
            stable_count=len(stable_products),
            recovering_count=recovering_count,
            blocker_count=blocker_count,
        )

        return StabilityForecast(
            status=status,
            status_message=status_message,
            total_products=len(products),
            stable_count=len(stable_products),
            unstable_count=total_unstable,
            blocker_count=blocker_count,
            stable_date=stable_date,
            stable_date_note=stable_date_note,
            timeline=timeline,
            recovering_products=recovering_products,
            blockers=blockers,
            recovery_progress_pct=recovery_progress_pct,
        )
