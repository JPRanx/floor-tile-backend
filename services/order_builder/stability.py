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

