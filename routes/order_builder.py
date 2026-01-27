"""
Order Builder API routes.

GET /api/order-builder — Get Order Builder data for the hero feature.
POST /api/order-builder/confirm — Confirm order and create factory_order.
POST /api/order-builder/export — Export order to factory Excel format.
"""

from datetime import date, timedelta
from typing import Optional, List
from decimal import Decimal
import time
from fastapi import APIRouter, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import structlog

from models.order_builder import (
    OrderBuilderResponse,
    ConfirmOrderRequest,
    ConfirmOrderResponse,
    DemandForecastResponse,
    CustomerDue,
    CustomerProduct,
    OverdueAlert,
    OverdueSeverity,
    ProductDemand,
)
from models.bl_allocation import (
    BLAllocationRequest,
    BLAllocationResponse,
    BLAllocationReport,
)
from models.factory_order import FactoryOrderCreate, FactoryOrderItemCreate
from services.order_builder_service import get_order_builder_service
from services.export_service import get_export_service, MONTHS_ES
from services.factory_order_service import get_factory_order_service
from services.customer_pattern_service import get_customer_pattern_service
from services.trend_service import get_trend_service
from services.bl_allocation_service import get_bl_allocation_service
from exceptions import FactoryOrderPVExistsError, DatabaseError
from collections import defaultdict

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/order-builder", tags=["Order Builder"])

# Factory constant for pallet conversion (actual factory pallet dimensions)
M2_PER_PALLET_FACTORY = Decimal("134.4")


# Request models for export
class ExportProductItem(BaseModel):
    """Single product in export request."""
    sku: str
    pallets: int


class ExportOrderRequest(BaseModel):
    """Request body for order export."""
    products: List[ExportProductItem]
    boat_departure: date


@router.get("", response_model=OrderBuilderResponse)
def get_order_builder(
    boat_id: Optional[str] = Query(
        None,
        description="Specific boat ID. If not provided, uses next available boat."
    ),
    num_bls: int = Query(
        1,
        ge=1,
        le=5,
        description="Number of BLs (1-5). Determines capacity: num_bls × 5 × 14 pallets. Default 1 (70 pallets)."
    ),
) -> OrderBuilderResponse:
    """
    Get Order Builder data.

    Returns everything needed for the Order Builder hero page:
    - Target boat information
    - Products grouped by priority (HIGH_PRIORITY, CONSIDER, WELL_COVERED, YOUR_CALL)
    - Pre-selected products based on BL capacity
    - Order summary with capacity checks
    - Alerts for issues

    BL count determines capacity:
    - 1 BL  =  5 containers =  70 pallets
    - 2 BLs = 10 containers = 140 pallets
    - 3 BLs = 15 containers = 210 pallets
    - 4 BLs = 20 containers = 280 pallets
    - 5 BLs = 25 containers = 350 pallets
    """
    start_time = time.time()
    logger.info(
        "order_builder_request",
        boat_id=boat_id,
        num_bls=num_bls
    )

    service = get_order_builder_service()
    result = service.get_order_builder(boat_id=boat_id, num_bls=num_bls)

    elapsed = time.time() - start_time
    logger.info("order_builder_complete", elapsed_seconds=round(elapsed, 2))
    return result


@router.post("/confirm", response_model=ConfirmOrderResponse, status_code=status.HTTP_201_CREATED)
def confirm_order(request: ConfirmOrderRequest) -> ConfirmOrderResponse:
    """
    Confirm order and create factory_order record.

    This creates a persistent factory_order that can be:
    - Tracked through production (PENDING → CONFIRMED → READY → SHIPPED)
    - Linked to shipments later
    - Referenced by PV number

    Request body:
    {
        "boat_id": "uuid",
        "boat_name": "MSC Mediterranean",
        "boat_departure": "2026-02-15",
        "products": [
            {"product_id": "uuid", "sku": "ALMENDRO BEIGE BTE", "pallets": 14},
            {"product_id": "uuid", "sku": "CEIBA GRIS CLARO BTE", "pallets": 7}
        ],
        "pv_number": "PV-20260108-001",  // Optional - auto-generated if not provided
        "notes": "Urgent order for February shipment"
    }

    Returns: Created factory_order with PV number and details
    """
    logger.info(
        "confirm_order_request",
        boat_id=request.boat_id,
        product_count=len(request.products),
        pv_number=request.pv_number
    )

    factory_order_service = get_factory_order_service()

    # Auto-generate PV number if not provided
    pv_number = request.pv_number
    if not pv_number:
        today = date.today()
        daily_count = factory_order_service.count_by_date(today)
        pv_number = f"PV-{today.strftime('%Y%m%d')}-{daily_count + 1:03d}"

        logger.info(
            "pv_number_generated",
            pv_number=pv_number,
            daily_count=daily_count
        )

    # Convert Order Builder products to factory_order_items
    # Pallets → m² conversion: pallets × 134.4
    items = []
    total_pallets = 0

    for product in request.products:
        quantity_m2 = Decimal(product.pallets) * M2_PER_PALLET_FACTORY
        total_pallets += product.pallets

        # Estimated ready date: ~30 days before boat departure
        estimated_ready = request.boat_departure - timedelta(days=30)

        items.append(FactoryOrderItemCreate(
            product_id=product.product_id,
            quantity_ordered=quantity_m2,
            estimated_ready_date=estimated_ready
        ))

    # Build notes with boat reference
    notes_parts = []
    if request.notes:
        notes_parts.append(request.notes)
    notes_parts.append(f"Created from Order Builder for boat {request.boat_name} departing {request.boat_departure.strftime('%Y-%m-%d')}")
    combined_notes = " | ".join(notes_parts)

    # Create factory order
    factory_order_data = FactoryOrderCreate(
        pv_number=pv_number,
        order_date=date.today(),
        items=items,
        notes=combined_notes
    )

    try:
        factory_order = factory_order_service.create(factory_order_data)

        logger.info(
            "factory_order_confirmed",
            factory_order_id=factory_order.id,
            pv_number=factory_order.pv_number,
            items_count=len(items),
            total_m2=float(factory_order.total_m2)
        )

        return ConfirmOrderResponse(
            factory_order_id=factory_order.id,
            pv_number=factory_order.pv_number,
            status=factory_order.status.value,
            order_date=factory_order.order_date,
            items_count=factory_order.item_count,
            total_m2=factory_order.total_m2,
            total_pallets=total_pallets,
            created_at=factory_order.created_at.isoformat()
        )

    except FactoryOrderPVExistsError as e:
        logger.warning("pv_number_exists", pv_number=pv_number)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"PV number {pv_number} already exists. Please use a different PV number."
        )
    except DatabaseError as e:
        logger.error("confirm_order_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create factory order. Please try again."
        )


@router.post("/export")
def export_order(request: ExportOrderRequest) -> StreamingResponse:
    """
    Generate Excel file for factory order.

    Request body:
    {
        "products": [
            {"sku": "ALMENDRO BEIGE BTE", "pallets": 14},
            {"sku": "CEIBA GRIS CLARO BTE", "pallets": 7}
        ],
        "boat_departure": "2026-01-15"
    }

    Returns: Excel file download
    """
    logger.info(
        "export_order_request",
        product_count=len(request.products),
        boat_departure=str(request.boat_departure),
    )

    # Convert request to dict format expected by service
    products_data = [
        {"sku": p.sku, "pallets": p.pallets}
        for p in request.products
    ]

    export_service = get_export_service()
    excel_file = export_service.generate_factory_order_excel(
        products=products_data,
        boat_departure=request.boat_departure,
    )

    # Generate filename with month and year
    production_month = request.boat_departure.month + 1
    production_year = request.boat_departure.year
    if production_month > 12:
        production_month = 1
        production_year += 1

    month_name = MONTHS_ES[production_month]
    filename = f"PEDIDO_FABRICA_{month_name}_{production_year}.xlsx"

    return StreamingResponse(
        excel_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


@router.get("/demand-forecast", response_model=DemandForecastResponse)
def get_demand_forecast(
    boat_id: Optional[str] = Query(
        None,
        description="Specific boat ID. If not provided, uses next available boat."
    ),
) -> DemandForecastResponse:
    """
    Get demand forecast combining velocity and customer patterns.

    Returns:
    - velocity_based_demand_m2: Traditional demand based on current velocity × lead time
    - pattern_based_demand_m2: Demand based on customers expected to order
    - customers_due_soon: Customers with patterns suggesting imminent orders
    - overdue_alerts: Severely overdue customers (call before ordering)
    - demand_by_product: Product-level demand breakdown
    """
    start_time = time.time()
    timings = {}
    logger.info("demand_forecast_request", boat_id=boat_id)

    # Get order builder data for velocity-based demand and boat info
    t1 = time.time()
    order_builder_service = get_order_builder_service()
    order_data = order_builder_service.get_order_builder(boat_id=boat_id, num_bls=5)  # Max capacity to get all products
    timings["order_builder"] = round(time.time() - t1, 2)

    lead_time_days = order_data.boat.days_until_departure + 30  # +30 for transit

    # Calculate velocity-based demand (sum of all coverage gaps)
    velocity_demand = Decimal("0")
    product_velocity: dict[str, Decimal] = {}

    all_products = (
        order_data.high_priority +
        order_data.consider +
        order_data.well_covered +
        order_data.your_call
    )

    for product in all_products:
        velocity_demand += product.total_demand_m2
        product_velocity[product.sku] = product.total_demand_m2

    # Get customer patterns and trends
    t2 = time.time()
    pattern_service = get_customer_pattern_service()
    trend_service = get_trend_service()

    # Get customer trends (includes pattern data and top_products)
    customer_trends = trend_service.get_customer_trends(period_days=90, comparison_period_days=90, limit=100)
    timings["customer_trends"] = round(time.time() - t2, 2)

    logger.info("demand_forecast_timings", **timings, total=round(time.time() - start_time, 2))

    # Get overdue customers
    overdue_customers = pattern_service.get_overdue_customers(min_days_overdue=1, limit=50)

    # Build customers due soon list (overdue 0-60 days or due within 14 days)
    customers_due_soon: list[CustomerDue] = []
    overdue_alerts: list[OverdueAlert] = []
    pattern_demand = Decimal("0")

    # Aggregate demand by product from customer patterns
    product_pattern_demand: dict[str, dict] = defaultdict(lambda: {
        "m2": Decimal("0"),
        "customers": [],
    })

    for customer in customer_trends:
        # Skip if no pattern data
        if not customer.avg_days_between_orders or customer.order_count < 2:
            continue

        days_overdue = customer.days_overdue
        avg_order_m2 = customer.total_volume_m2 / customer.order_count if customer.order_count > 0 else Decimal("0")
        avg_order_usd = customer.avg_order_value_usd

        # Determine if customer is due soon (within lead time window)
        is_due_soon = days_overdue >= -14  # Due within 14 days OR overdue

        if is_due_soon:
            # Build top products list
            top_prods: list[CustomerProduct] = []
            total_product_m2 = sum(p.total_m2 for p in customer.top_products[:5])

            for prod in customer.top_products[:5]:
                share = (prod.total_m2 / total_product_m2 * 100) if total_product_m2 > 0 else Decimal("0")
                avg_per_order = prod.total_m2 / customer.order_count if customer.order_count > 0 else Decimal("0")

                top_prods.append(CustomerProduct(
                    sku=prod.sku,
                    avg_m2_per_order=round(avg_per_order, 2),
                    purchase_count=prod.purchase_count,
                    share_pct=round(share, 2),
                ))

                # Add to product pattern demand if customer is overdue or due within window
                if days_overdue >= 0:  # Currently overdue - likely to order
                    product_pattern_demand[prod.sku]["m2"] += avg_per_order
                    product_pattern_demand[prod.sku]["customers"].append(customer.customer_normalized)

            # Add customer to due soon list
            expected_date_str = customer.expected_next_date.isoformat() if customer.expected_next_date else None
            last_order_str = customer.last_purchase.isoformat() if customer.last_purchase else None
            customers_due_soon.append(CustomerDue(
                customer_normalized=customer.customer_normalized,
                tier=customer.tier.value,
                days_overdue=days_overdue,
                expected_date=expected_date_str,
                predictability=customer.predictability if customer.predictability else None,
                avg_order_m2=round(avg_order_m2, 2),
                avg_order_usd=round(avg_order_usd, 2),
                last_order_date=last_order_str,
                trend_direction=customer.direction.value.lower(),
                top_products=top_prods,
            ))

            # Add to pattern demand
            if days_overdue >= 0:
                pattern_demand += avg_order_m2

        # Check for severely overdue (need to call before ordering)
        if days_overdue >= 60:  # At least 60 days overdue
            severity = OverdueSeverity.CRITICAL if days_overdue >= 180 else OverdueSeverity.WARNING

            if severity == OverdueSeverity.CRITICAL:
                message = f"Sin órdenes hace {days_overdue} días. Llamar para confirmar si sigue activo."
            else:
                message = f"Atrasado {days_overdue} días. Verificar disponibilidad antes de incluir en pedido."

            alert_last_order = customer.last_purchase.isoformat() if customer.last_purchase else None
            overdue_alerts.append(OverdueAlert(
                customer_normalized=customer.customer_normalized,
                tier=customer.tier.value,
                days_overdue=days_overdue,
                severity=severity,
                avg_order_usd=round(avg_order_usd, 2),
                last_order_date=alert_last_order,
                message=message,
            ))

    # Sort customers due soon by days_overdue descending (most overdue first)
    customers_due_soon.sort(key=lambda c: c.days_overdue, reverse=True)

    # Sort overdue alerts by days_overdue descending
    overdue_alerts.sort(key=lambda a: a.days_overdue, reverse=True)

    # Build product demand list
    demand_by_product: list[ProductDemand] = []
    all_skus = set(product_velocity.keys()) | set(product_pattern_demand.keys())

    for sku in all_skus:
        velocity_m2 = product_velocity.get(sku, Decimal("0"))
        pattern_m2 = product_pattern_demand.get(sku, {"m2": Decimal("0"), "customers": []})["m2"]
        customers = product_pattern_demand.get(sku, {"m2": Decimal("0"), "customers": []})["customers"]

        recommended = max(velocity_m2, pattern_m2)

        demand_by_product.append(ProductDemand(
            sku=sku,
            velocity_demand_m2=round(velocity_m2, 2),
            pattern_demand_m2=round(pattern_m2, 2),
            recommended_m2=round(recommended, 2),
            customers_expecting=len(set(customers)),
            customer_names=list(set(customers))[:5],  # Top 5 unique customers
        ))

    # Sort by recommended demand descending
    demand_by_product.sort(key=lambda p: p.recommended_m2, reverse=True)

    recommended_demand = max(velocity_demand, pattern_demand)

    logger.info(
        "demand_forecast_calculated",
        velocity_demand=float(velocity_demand),
        pattern_demand=float(pattern_demand),
        customers_due=len(customers_due_soon),
        overdue_alerts=len(overdue_alerts),
    )

    return DemandForecastResponse(
        velocity_based_demand_m2=round(velocity_demand, 2),
        pattern_based_demand_m2=round(pattern_demand, 2),
        recommended_demand_m2=round(recommended_demand, 2),
        lead_time_days=lead_time_days,
        customers_due_soon=customers_due_soon[:20],  # Top 20
        overdue_alerts=overdue_alerts[:10],  # Top 10
        demand_by_product=demand_by_product[:20],  # Top 20 products
    )


@router.post("/generate-bl-allocation", response_model=BLAllocationResponse)
def generate_bl_allocation(
    request: BLAllocationRequest,
) -> BLAllocationResponse:
    """
    Generate BL allocation from Order Builder selection.

    Allocates products across BLs for customs safety:
    - Critical products (score >= 85) are SPREAD across BLs
    - Customer products are grouped together
    - General stock is distributed evenly
    - No BL exceeds 5 containers

    Request body:
    {
        "num_bls": 3,
        "boat_id": "uuid" (optional),
        "products": [{"sku": "...", "pallets": 14}] (optional, uses current selection)
    }

    Returns:
    - allocation: BLAllocationReport with per-BL breakdown
    - download_url: URL to download Excel file (if generated)
    """
    start_time = time.time()
    logger.info(
        "bl_allocation_request",
        num_bls=request.num_bls,
        boat_id=request.boat_id,
        product_count=len(request.products) if request.products else "from_order_builder",
    )

    # Get order builder data
    order_builder_service = get_order_builder_service()
    order_data = order_builder_service.get_order_builder(
        boat_id=request.boat_id,
        num_bls=5,  # Max capacity to get all products for allocation
    )

    # Get all products
    all_products = (
        order_data.high_priority +
        order_data.consider +
        order_data.well_covered +
        order_data.your_call
    )

    # If specific products provided, filter and update selection
    if request.products:
        product_pallets = {p["sku"]: p["pallets"] for p in request.products}
        for product in all_products:
            if product.sku in product_pallets:
                product.is_selected = True
                product.selected_pallets = product_pallets[product.sku]
            else:
                product.is_selected = False
                product.selected_pallets = 0

    # Get customer trends for primary customer lookup
    trend_service = get_trend_service()
    customer_trends = trend_service.get_customer_trends(
        period_days=90,
        comparison_period_days=90,
        limit=100,
    )

    # Allocate products to BLs
    bl_service = get_bl_allocation_service()
    allocation_report = bl_service.allocate_products_to_bls(
        products=all_products,
        num_bls=request.num_bls,
        customer_trends=customer_trends,
        boat_departure=order_data.boat.departure_date,
        boat_name=order_data.boat.name,
    )

    # Generate Excel file
    export_service = get_export_service()
    excel_file = export_service.generate_bl_allocation_excel(allocation_report)

    # For now, return without download URL (frontend can call export endpoint separately)
    # In the future, we could save to S3 and return URL

    elapsed = time.time() - start_time
    logger.info(
        "bl_allocation_complete",
        elapsed_seconds=round(elapsed, 2),
        num_bls=request.num_bls,
        total_containers=allocation_report.total_containers,
        total_critical=allocation_report.total_critical_products,
        risk_even=allocation_report.risk_distribution_even,
    )

    return BLAllocationResponse(
        allocation=allocation_report,
        download_url=None,  # Excel can be downloaded via separate endpoint
    )


@router.post("/export-bl-allocation")
def export_bl_allocation(
    request: BLAllocationRequest,
) -> StreamingResponse:
    """
    Export BL allocation to Excel file.

    Similar to generate-bl-allocation but returns Excel file directly.

    Request body:
    {
        "num_bls": 3,
        "boat_id": "uuid" (optional),
        "products": [{"sku": "...", "pallets": 14}] (optional)
    }

    Returns: Excel file download with BL breakdown
    """
    logger.info(
        "export_bl_allocation_request",
        num_bls=request.num_bls,
        boat_id=request.boat_id,
    )

    # Get order builder data
    order_builder_service = get_order_builder_service()
    order_data = order_builder_service.get_order_builder(
        boat_id=request.boat_id,
        num_bls=5,  # Max capacity to get all products for allocation
    )

    # Get all products
    all_products = (
        order_data.high_priority +
        order_data.consider +
        order_data.well_covered +
        order_data.your_call
    )

    # If specific products provided, filter and update selection
    if request.products:
        product_pallets = {p["sku"]: p["pallets"] for p in request.products}
        for product in all_products:
            if product.sku in product_pallets:
                product.is_selected = True
                product.selected_pallets = product_pallets[product.sku]
            else:
                product.is_selected = False
                product.selected_pallets = 0

    # Get customer trends
    trend_service = get_trend_service()
    customer_trends = trend_service.get_customer_trends(
        period_days=90,
        comparison_period_days=90,
        limit=100,
    )

    # Allocate products to BLs
    bl_service = get_bl_allocation_service()
    allocation_report = bl_service.allocate_products_to_bls(
        products=all_products,
        num_bls=request.num_bls,
        customer_trends=customer_trends,
        boat_departure=order_data.boat.departure_date,
        boat_name=order_data.boat.name,
    )

    # Generate Excel file
    export_service = get_export_service()
    excel_file = export_service.generate_bl_allocation_excel(allocation_report)

    # Generate filename
    departure_str = order_data.boat.departure_date.strftime("%Y%m%d")
    filename = f"BL_ALLOCATION_{departure_str}_{request.num_bls}BLs.xlsx"

    return StreamingResponse(
        excel_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )
