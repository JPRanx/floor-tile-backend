"""
Order Builder service — Hero feature business logic.

Answers: "What should I order for the next boat?"
Combines coverage gap, confidence, and 4-level optimization.

See BUILDER_BLUEPRINT.md for algorithm details.
"""

from typing import Optional
from decimal import Decimal
from datetime import date, timedelta
import time
import structlog

from config.shipping import (
    M2_PER_PALLET,
    WAREHOUSE_BUFFER_DAYS,
    ORDERING_CYCLE_DAYS,
)
from services.config_service import get_config_service
from services.boat_schedule_service import get_boat_schedule_service
from services.recommendation_service import get_recommendation_service
from services.inventory_service import get_inventory_service
from services.trend_service import get_trend_service
from services.production_schedule_service import get_production_schedule_service
from services.warehouse_order_service import get_warehouse_order_service
from models.order_builder import (
    OrderBuilderMode,
    OrderBuilderProduct,
    OrderBuilderBoat,
    OrderBuilderAlert,
    OrderBuilderAlertType,
    OrderBuilderSummary,
    OrderBuilderResponse,
    ConstraintAnalysis,
    OrderSummaryReasoning,
    ExcludedProduct,
    ShippingCostConfig,
    FactoryCapabilities,
)

from services.order_builder.constants import (
    PALLETS_PER_CONTAINER,
    MAX_CONTAINERS_PER_BL,
    WAREHOUSE_CAPACITY,
    ProductAnalysis,
)
from services.order_builder.analysis import AnalysisMixin
from services.order_builder.builder import BuilderMixin
from services.order_builder.scoring import ScoringMixin
from services.order_builder.capacity import CapacityMixin
from services.order_builder.summaries import SummariesMixin
from services.order_builder.alerts import AlertsMixin
from services.order_builder.stability import StabilityMixin
from services.order_builder.liquidation import LiquidationMixin
from services.order_builder.boats import BoatsMixin

logger = structlog.get_logger(__name__)


class OrderBuilderService(
    AnalysisMixin,
    BuilderMixin,
    ScoringMixin,
    CapacityMixin,
    SummariesMixin,
    AlertsMixin,
    StabilityMixin,
    LiquidationMixin,
    BoatsMixin,
):
    """
    Order Builder business logic.

    Calculates:
    1. Which boat to target
    2. Products grouped by priority
    3. Pre-selection based on mode
    4. Summary with capacity checks
    5. Alerts for issues
    """

    def __init__(self):
        self.boat_service = get_boat_schedule_service()
        self.recommendation_service = get_recommendation_service()
        self.inventory_service = get_inventory_service()
        self.trend_service = get_trend_service()
        self.production_schedule_service = get_production_schedule_service()
        self.warehouse_order_service = get_warehouse_order_service()

    def get_order_builder(
        self,
        boat_id: Optional[str] = None,
        num_bls: int = 1,
        excluded_skus: Optional[list[str]] = None,
        factory_id: Optional[str] = None,
    ) -> OrderBuilderResponse:
        """
        Get complete Order Builder data.

        Args:
            boat_id: Optional specific boat ID. If None, uses next available.
            num_bls: Number of BLs (1-5). Determines capacity: num_bls × 5 × 14 pallets.
                     Default 1 (70 pallets) for backward compatibility.
            excluded_skus: Optional list of SKUs to exclude from optimization.
                          Used when user removes products and wants to recalculate.

        Returns:
            OrderBuilderResponse with all data needed for the UI
        """
        # num_bls=0 means "auto" — will be resolved after calculating recommended
        auto_bls = num_bls == 0
        if not auto_bls:
            num_bls = max(1, min(5, num_bls))

        # Normalize excluded_skus
        excluded_set = set(excluded_skus) if excluded_skus else set()

        timings = {}
        t0 = time.time()
        logger.info(
            "getting_order_builder",
            boat_id=boat_id,
            num_bls=num_bls,
            excluded_count=len(excluded_set)
        )

        # Step 1: Get boat info
        boat, next_boat = self._get_boats(boat_id)
        timings["1_boats"] = round(time.time() - t0, 2)

        # Default lead time for "no boat" mode (45 days)
        DEFAULT_LEAD_TIME_DAYS = 45

        if not boat:
            # No boats available - use default lead time and create dummy boat
            logger.info("no_boats_available_using_default_lead_time", days=DEFAULT_LEAD_TIME_DAYS)
            today = date.today()
            default_departure = today + timedelta(days=DEFAULT_LEAD_TIME_DAYS)
            default_arrival = default_departure + timedelta(days=25)  # ~25 days transit
            default_warehouse = DEFAULT_LEAD_TIME_DAYS + 25 + WAREHOUSE_BUFFER_DAYS  # departure + transit + buffer
            default_order_deadline = default_departure - timedelta(days=30)

            boat = OrderBuilderBoat(
                boat_id="",
                name="",
                departure_date=default_departure,
                arrival_date=default_arrival,
                days_until_departure=DEFAULT_LEAD_TIME_DAYS,
                days_until_arrival=DEFAULT_LEAD_TIME_DAYS + 25,  # departure + transit
                days_until_warehouse=default_warehouse,
                order_deadline=default_order_deadline,
                days_until_order_deadline=(default_order_deadline - today).days,
                past_order_deadline=today > default_order_deadline,
                booking_deadline=today,
                days_until_deadline=0,
                max_containers=5,
            )

        # Step 2: Get recommendations (has coverage gap, confidence, priority)
        t1 = time.time()
        recommendations = self.recommendation_service.get_recommendations(factory_id=factory_id)
        timings["2_recommendations"] = round(time.time() - t1, 2)

        # Step 2a: Filter out excluded SKUs (for recalculate)
        if excluded_set:
            original_count = len(recommendations.recommendations)
            recommendations.recommendations = [
                r for r in recommendations.recommendations
                if r.sku not in excluded_set
            ]
            logger.info(
                "excluded_skus_filtered",
                excluded_count=len(excluded_set),
                original_count=original_count,
                filtered_count=len(recommendations.recommendations)
            )

        # Step 2a¾: Filter by factory if specified (OB V2)
        factory_data = None
        factory_product_ids = None
        if factory_id:
            from services.factory_service import get_factory_service
            factory_svc = get_factory_service()
            factory_data = factory_svc.get_by_id(factory_id)
            if factory_data:
                # Get product IDs belonging to this factory
                from config import get_supabase_client
                db = get_supabase_client()
                factory_products = (
                    db.table("products")
                    .select("id")
                    .eq("factory_id", factory_id)
                    .eq("active", True)
                    .execute()
                )
                factory_product_ids = {p["id"] for p in factory_products.data}
                # Filter recommendations to only this factory's products
                original_count = len(recommendations.recommendations)
                recommendations.recommendations = [
                    r for r in recommendations.recommendations
                    if r.product_id in factory_product_ids
                ]
                logger.info(
                    "factory_filter_applied",
                    factory_id=factory_id,
                    factory_name=factory_data["name"],
                    original_count=original_count,
                    filtered_count=len(recommendations.recommendations),
                )

        # Step 2a⅞: Get unit config for factory (unit-based vs m²-based)
        unit_config = None
        is_unit_based = False
        if factory_id:
            from services.unit_config_service import get_unit_config
            from config import get_supabase_client
            unit_config = get_unit_config(get_supabase_client(), factory_id)
            is_unit_based = bool(unit_config and not unit_config["is_m2_based"])

        # Step 2a⅞½: Build factory capabilities
        factory_capabilities = None
        if factory_data:
            factory_capabilities = FactoryCapabilities(
                has_factory_inventory=factory_data.get("has_factory_inventory", True),
                has_logistics=factory_data.get("has_logistics", True),
                has_production=factory_data.get("has_production", True),
            )

        # Step 2a½: Get inventory snapshot once (reused by grouping, mode, summary)
        inventory_snapshots = self.inventory_service.get_latest()

        # Step 2b: Get trend data for products
        t2 = time.time()
        trend_data = self._get_product_trends()
        timings["3_trends"] = round(time.time() - t2, 2)

        # Step 2c: Calculate dynamic coverage buffer based on next boat arrival
        coverage_buffer_days = self._get_coverage_buffer(boat, next_boat)

        # Step 2d: Forward simulation for multi-boat awareness
        projection_map = None
        projection_data = None  # Full projection data including stability
        if factory_id and boat and boat.boat_id:
            try:
                from services.forward_simulation_service import get_forward_simulation_service
                fwd_sim = get_forward_simulation_service()
                projection_data = fwd_sim.get_projection_for_boat(factory_id, boat.boat_id)
                if projection_data:
                    projection_map = projection_data["products"]
                    logger.info(
                        "forward_simulation_loaded",
                        factory_id=factory_id,
                        boat_id=boat.boat_id,
                        products_projected=len(projection_map),
                        has_stability=projection_data.get("stability_impact") is not None,
                    )
            except Exception as e:
                logger.warning("forward_sim_fallback", error=str(e))

        # Build pallet factor map for unit-based factories
        pallet_factor_map: dict[str, Decimal] = {}
        if is_unit_based and factory_id:
            from services.product_service import get_product_service
            _prod_svc = get_product_service()
            _factory_products = _prod_svc.get_active_products_for_factory(factory_id)
            pallet_factor_map = {
                p.id: Decimal(str(p.units_per_pallet or 20)) for p in _factory_products
            }

        # Step 3: Convert to OrderBuilderProducts grouped by priority
        t3 = time.time()
        products_by_priority = self._group_products_by_priority(
            recommendations.recommendations,
            boat.days_until_warehouse,  # Use warehouse arrival (not departure!)
            trend_data,
            boat_departure=boat.departure_date if boat.boat_id else None,  # Pass departure for factory status
            order_deadline=boat.order_deadline,  # Pass for availability breakdown calculation
            coverage_buffer_days=coverage_buffer_days,  # Dynamic buffer based on next boat
            inventory_snapshots=inventory_snapshots,
            projection_map=projection_map,  # Forward simulation projections
            is_unit_based=is_unit_based,
            pallet_factor_map=pallet_factor_map,
        )
        timings["4_grouping"] = round(time.time() - t3, 2)

        logger.info("order_builder_timings", **timings)

        # Pre-compute warehouse availability from cached inventory
        warehouse_current_m2 = sum(
            Decimal(str(inv.warehouse_qty)) for inv in inventory_snapshots
        )
        # Cascade-aware: account for earlier boats' deliveries + consumption
        warehouse_at_arrival = None
        if factory_id and boat and boat.boat_id:
            warehouse_at_arrival = self._get_warehouse_at_arrival(
                boat, inventory_snapshots, trend_data, factory_id
            )
            warehouse_available_pallets = max(0, WAREHOUSE_CAPACITY - warehouse_at_arrival)
        else:
            warehouse_available_pallets = max(0, WAREHOUSE_CAPACITY - int(warehouse_current_m2 / M2_PER_PALLET))

        # Resolve auto BLs: calculate recommended before applying capacity
        if auto_bls:
            pre_mode_products = [p for tier in products_by_priority.values() for p in tier]
            num_bls, _, _ = self._calculate_recommended_bls(pre_mode_products)
            logger.info("auto_bls_resolved", recommended=num_bls)

        # Step 4: Apply BL capacity logic (pre-select products) with constraint analysis
        all_products, constraint_analysis = self._apply_mode(
            products_by_priority, num_bls, boat.max_containers, trend_data,
            warehouse_available_pallets=warehouse_available_pallets,
            inventory_snapshots=inventory_snapshots,
        )

        # Step 5: Calculate summary (use BL capacity, not boat capacity)
        bl_max_containers = num_bls * MAX_CONTAINERS_PER_BL
        summary = self._calculate_summary(
            all_products, bl_max_containers, inventory_snapshots,
            warehouse_at_arrival=warehouse_at_arrival if factory_id and boat and boat.boat_id else None,
        )

        # Step 6: Generate alerts
        alerts = self._generate_alerts(all_products, summary, boat)
        summary.alerts = alerts

        # Step 7: Generate summary reasoning
        summary_reasoning = self._generate_summary_reasoning(
            all_products, boat, summary, constraint_analysis
        )

        # Re-group after mode application
        high_priority = [p for p in all_products if p.priority == "HIGH_PRIORITY"]
        consider = [p for p in all_products if p.priority == "CONSIDER"]
        well_covered = [p for p in all_products if p.priority == "WELL_COVERED"]
        your_call = [p for p in all_products if p.priority == "YOUR_CALL"]

        # Sort each tier: selected first (alphabetically), then non-selected (alphabetically)
        def sort_by_selection_then_sku(products: list) -> list:
            return sorted(products, key=lambda p: (0 if p.is_selected else 1, p.sku.upper()))

        high_priority = sort_by_selection_then_sku(high_priority)
        consider = sort_by_selection_then_sku(consider)
        well_covered = sort_by_selection_then_sku(well_covered)
        your_call = sort_by_selection_then_sku(your_call)

        # Step 8: Calculate two-section summaries (Section 3 moved to Factory Request Builder)
        warehouse_summary, add_to_production_summary = \
            self._calculate_section_summaries(all_products, boat, num_bls)

        # Step 9: Calculate recommended BL count (based on true need) and available BLs
        recommended_bls, available_bls, recommended_bls_reason = self._calculate_recommended_bls(all_products)

        # Step 9b: Calculate shippable BLs (what can actually fill gaps)
        shippable_bls, shippable_m2 = self._calculate_shippable_bls(all_products)

        # Step 10: Calculate "Unable to Ship" alerts
        unable_to_ship = self._calculate_unable_to_ship_alerts(all_products, boat.order_deadline)

        # Step 11: Stability Forecast (from FS cascade-aware data)
        stability_forecast = None
        if projection_data and projection_data.get("stability_impact"):
            stability_forecast = self._convert_fs_stability(
                projection_data, all_products, boat,
            )

        # Step 12: Get liquidation clearance candidates (deactivated products with factory stock)
        liquidation_clearance = self._get_liquidation_clearance()

        # Step 13: Read shipping cost config for frontend cost estimation
        config_svc = get_config_service()
        freight = config_svc.get_decimal("freight_per_container_usd", Decimal("460"))
        destination = config_svc.get_decimal("destination_per_container_usd", Decimal("630"))
        trucking = config_svc.get_decimal("trucking_per_container_usd", Decimal("261.10"))
        other = config_svc.get_decimal("other_per_container_usd", Decimal("46.44"))
        bl_fixed = config_svc.get_decimal("bl_fixed_costs_usd", Decimal("180.53"))
        m2_container = config_svc.get_decimal("m2_per_container", Decimal("1881.6"))

        shipping_cost_config = ShippingCostConfig(
            freight_per_container_usd=freight,
            destination_per_container_usd=destination,
            trucking_per_container_usd=trucking,
            other_per_container_usd=other,
            bl_fixed_costs_usd=bl_fixed,
            m2_per_container=m2_container,
            per_container_total_usd=freight + destination + trucking + other,
        )

        # Step 14: Compute factory timeline milestones (OB V2)
        factory_timeline = None
        factory_name_str = None
        if factory_data and boat.boat_id:
            from services.factory_timeline_service import get_factory_timeline_service
            timeline_svc = get_factory_timeline_service()
            # Check if factory has scheduled production for this boat window
            has_production = any(
                p.production_status == "scheduled" and p.production_can_add_more
                for p in all_products
            ) if all_products else False
            factory_timeline = timeline_svc.compute_milestones(
                factory=factory_data,
                departure_date=boat.departure_date,
                arrival_date=boat.arrival_date,
                has_scheduled_production=has_production,
            )
            factory_name_str = factory_data["name"]

        result = OrderBuilderResponse(
            boat=boat,
            next_boat=next_boat,
            num_bls=num_bls,
            recommended_bls=recommended_bls,
            available_bls=available_bls,
            recommended_bls_reason=recommended_bls_reason,
            shippable_bls=shippable_bls,
            shippable_m2=shippable_m2,
            high_priority=high_priority,
            consider=consider,
            well_covered=well_covered,
            your_call=your_call,
            summary=summary,
            # Two-section summaries (Section 3 moved to Factory Request Builder)
            warehouse_order_summary=warehouse_summary,
            add_to_production_summary=add_to_production_summary,
            factory_request_summary=None,
            constraint_analysis=constraint_analysis,
            summary_reasoning=summary_reasoning,
            # Unable to ship alerts
            unable_to_ship=unable_to_ship,
            # Stability forecast
            stability_forecast=stability_forecast,
            # Liquidation clearance (deactivated products with factory stock)
            liquidation_clearance=liquidation_clearance,
            # Shipping cost config for frontend cost estimation
            shipping_cost_config=shipping_cost_config,
            # Factory-aware fields (OB V2)
            factory_id=factory_id,
            factory_name=factory_name_str,
            factory_timeline=factory_timeline,
            capabilities=factory_capabilities,
            # Unit-based factory fields
            unit_label=unit_config["unit_label"] if unit_config else "m²",
            is_unit_based=is_unit_based,
        )

        logger.info(
            "order_builder_generated",
            num_bls=num_bls,
            bl_capacity=num_bls * MAX_CONTAINERS_PER_BL * PALLETS_PER_CONTAINER,
            high_priority=len(high_priority),
            consider=len(consider),
            well_covered=len(well_covered),
            your_call=len(your_call),
            total_selected=summary.total_pallets,
        )

        return result

    def _group_products_by_priority(
        self,
        recommendations: list,
        days_to_cover: int,
        trend_data: dict[str, dict],
        boat_departure: Optional[date] = None,
        order_deadline: Optional[date] = None,
        coverage_buffer_days: Optional[int] = None,
        inventory_snapshots: Optional[list] = None,
        projection_map: Optional[dict] = None,
        is_unit_based: bool = False,
        pallet_factor_map: Optional[dict[str, Decimal]] = None,
    ) -> dict[str, list[OrderBuilderProduct]]:
        """Convert recommendations to OrderBuilderProducts grouped by priority."""
        groups = {
            "HIGH_PRIORITY": [],
            "CONSIDER": [],
            "WELL_COVERED": [],
            "YOUR_CALL": [],
        }

        # Dynamic coverage buffer: days until NEXT boat arrives at warehouse
        # Falls back to ORDERING_CYCLE_DAYS (30 days) if not provided
        buffer_days = coverage_buffer_days if coverage_buffer_days is not None else ORDERING_CYCLE_DAYS

        # Get factory production status for all products
        factory_status_map = {}
        if boat_departure:
            product_ids = [r.product_id for r in recommendations]
            try:
                factory_status_map = self.production_schedule_service.get_factory_status(
                    product_ids=product_ids,
                    boat_departure=boat_departure,
                    buffer_days=3
                )
            except Exception as e:
                logger.warning("factory_status_lookup_failed", error=str(e))

        # Get factory availability (SIESA finished goods) for all products
        factory_availability_map = {}
        try:
            if inventory_snapshots is None:
                inventory_snapshots = self.inventory_service.get_latest()
            for inv in inventory_snapshots:
                factory_availability_map[inv.product_id] = {
                    "factory_available_m2": Decimal(str(inv.factory_available_m2 or 0)),
                    "factory_largest_lot_m2": Decimal(str(inv.factory_largest_lot_m2)) if inv.factory_largest_lot_m2 else None,
                    "factory_largest_lot_code": inv.factory_largest_lot_code,
                    "factory_lot_count": inv.factory_lot_count or 0,
                }
        except Exception as e:
            logger.warning("factory_availability_lookup_failed", error=str(e))

        # Get production schedule data (from Programa de Produccion Excel)
        # This shows what's scheduled/in_progress/completed at the factory
        production_schedule_map = {}
        try:
            production_schedule_map = self.production_schedule_service.get_production_by_sku()
            logger.debug(
                "production_schedule_loaded",
                products_with_production=len(production_schedule_map)
            )
        except Exception as e:
            logger.warning("production_schedule_lookup_failed", error=str(e))

        # Get pending warehouse orders by SKU
        # This prevents double-ordering products already committed to a boat
        pending_orders_map = {}
        try:
            pending_orders_map = self.warehouse_order_service.get_pending_by_sku_dict()
            logger.debug(
                "pending_orders_loaded",
                skus_with_pending=len(pending_orders_map)
            )
        except Exception as e:
            logger.warning("pending_orders_lookup_failed", error=str(e))

        # Get committed orders per product (5e)
        committed_map: dict[str, dict] = {}
        try:
            product_ids = [r.product_id for r in recommendations]
            if product_ids:
                committed_result = self.inventory_service.db.table("committed_orders").select(
                    "product_id, quantity_committed, customer"
                ).in_("product_id", product_ids).execute()
                for row in (committed_result.data or []):
                    pid = row["product_id"]
                    if pid not in committed_map:
                        committed_map[pid] = {"total_m2": Decimal("0"), "customer": None, "count": 0}
                    committed_map[pid]["total_m2"] += Decimal(str(row.get("quantity_committed", 0) or 0))
                    committed_map[pid]["count"] += 1
                    if committed_map[pid]["customer"] is None and row.get("customer"):
                        committed_map[pid]["customer"] = row["customer"]
                logger.debug("committed_orders_loaded", products_with_committed=len(committed_map))
        except Exception as e:
            logger.warning("committed_orders_lookup_failed", error=str(e))

        # Get unfulfilled demand per product, last 90 days (5f)
        unfulfilled_map: dict[str, Decimal] = {}
        try:
            product_ids = [r.product_id for r in recommendations]
            if product_ids:
                cutoff = (date.today() - timedelta(days=90)).isoformat()
                unfulfilled_result = self.inventory_service.db.table("unfulfilled_demand").select(
                    "product_id, quantity_m2"
                ).in_("product_id", product_ids).gte("snapshot_date", cutoff).execute()
                for row in (unfulfilled_result.data or []):
                    pid = row["product_id"]
                    unfulfilled_map[pid] = unfulfilled_map.get(pid, Decimal("0")) + Decimal(str(row.get("quantity_m2", 0) or 0))
                logger.debug("unfulfilled_demand_loaded", products_with_unfulfilled=len(unfulfilled_map))
        except Exception as e:
            logger.warning("unfulfilled_demand_lookup_failed", error=str(e))

        for product_rec in recommendations:
            # Read trend data
            trend = trend_data.get(product_rec.sku, {})
            direction = trend.get("direction", "stable")
            strength = trend.get("strength", "weak")
            velocity_change_pct = Decimal(str(trend.get("velocity_change_pct", 0)))
            daily_velocity_m2 = Decimal(str(trend.get("daily_velocity_m2", 0)))
            days_of_stock = trend.get("days_of_stock")
            velocity_90d_m2 = Decimal(str(trend.get("velocity_90d_m2", 0)))
            velocity_180d_m2 = Decimal(str(trend.get("velocity_180d_m2", 0)))
            velocity_trend_signal = trend.get("velocity_trend_signal", "stable")
            velocity_trend_ratio = Decimal(str(trend.get("velocity_trend_ratio", 1.0)))

            # Ghost filter: skip products with zero velocity, zero warehouse, zero in-transit
            # and zero factory supply — they have no actionable data
            if daily_velocity_m2 == 0 and (product_rec.warehouse_m2 or 0) == 0 and (product_rec.in_transit_m2 or 0) == 0:
                factory_m2 = factory_availability_map.get(product_rec.product_id, {}).get("factory_available_m2", Decimal("0"))
                if factory_m2 == 0:
                    continue

            # Pallet factor
            _pf = (pallet_factor_map or {}).get(product_rec.product_id, M2_PER_PALLET) if is_unit_based else M2_PER_PALLET

            # === FS projection required — skip products without projection ===
            projection = projection_map.get(product_rec.product_id) if projection_map else None

            if projection is None:
                logger.debug(
                    "skipping_product_no_projection",
                    sku=product_rec.sku,
                    product_id=product_rec.product_id,
                )
                continue

            analysis = self._analyze_from_projection(
                projection, daily_velocity_m2, buffer_days, _pf,
                factory_availability_map, pending_orders_map, product_rec.sku,
            )

            # === SINGLE PATH: build product from analysis ===
            product = self._build_product_from_analysis(
                product_rec, analysis, days_to_cover,
                velocity_change_pct, daily_velocity_m2,
                velocity_90d_m2, velocity_180d_m2,
                velocity_trend_signal, velocity_trend_ratio,
                factory_status_map, factory_availability_map,
                production_schedule_map, committed_map, unfulfilled_map,
                boat_departure, order_deadline, _pf, projection,
            )

            priority_key = product.priority
            if priority_key in groups:
                groups[priority_key].append(product)
            else:
                groups["YOUR_CALL"].append(product)

        # Sort each tier by urgency, demand score, days of stock, velocity
        urgency_order = {"critical": 0, "urgent": 1, "soon": 2, "ok": 3}
        for priority_key in groups:
            groups[priority_key].sort(
                key=lambda p: (
                    urgency_order.get(p.urgency, 4),
                    -p.customer_demand_score,
                    p.days_of_stock if p.days_of_stock is not None else 999,
                    -float(p.daily_velocity_m2),
                )
            )

        return groups

    def _empty_response(self, mode: OrderBuilderMode) -> OrderBuilderResponse:
        """Return empty response when no boats available."""
        today = date.today()
        dummy_boat = OrderBuilderBoat(
            boat_id="",
            name="No boats scheduled",
            departure_date=today,
            arrival_date=today,
            days_until_departure=0,
            days_until_arrival=0,
            days_until_warehouse=0,
            order_deadline=today - timedelta(days=30),
            days_until_order_deadline=-30,
            past_order_deadline=True,
            booking_deadline=today,
            days_until_deadline=0,
            max_containers=5,
        )

        summary = OrderBuilderSummary(
            alerts=[
                OrderBuilderAlert(
                    type=OrderBuilderAlertType.WARNING,
                    icon="⚠️",
                    message="No boats available. Upload a boat schedule first."
                )
            ]
        )

        empty_reasoning = OrderSummaryReasoning(
            strategy="BALANCED",
            days_to_boat=0,
            boat_date=today.isoformat(),
            boat_name="No boats scheduled",
            critical_count=0,
            urgent_count=0,
            stable_count=0,
            excluded_count=0,
            key_insights=["No boat schedule available. Upload a boat schedule to get recommendations."],
            excluded_products=[],
        )

        return OrderBuilderResponse(
            boat=dummy_boat,
            next_boat=None,
            num_bls=1,
            recommended_bls=1,
            recommended_bls_reason="Sin barcos programados",
            high_priority=[],
            consider=[],
            well_covered=[],
            your_call=[],
            summary=summary,
            summary_reasoning=empty_reasoning,
        )


# Singleton instance
_order_builder_service: Optional[OrderBuilderService] = None


def get_order_builder_service() -> OrderBuilderService:
    """Get or create OrderBuilderService instance."""
    global _order_builder_service
    if _order_builder_service is None:
        _order_builder_service = OrderBuilderService()
    return _order_builder_service
