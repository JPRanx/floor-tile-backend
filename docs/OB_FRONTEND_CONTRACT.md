# OB Frontend Contract

> What the frontend actually reads from the Order Builder API response.
> Generated 2026-03-03. Source: full audit of all 23 frontend files that consume OB data.

## Purpose

This document is the **contract** between backend and frontend.
If the backend populates every field listed here with correct values,
the frontend works. Nothing else matters.

---

## Response Shape: `OrderBuilderResponse`

```
GET /api/order-builder?boat_id=<uuid>&factory_id=<uuid>&num_bls=<int>
```

### Top-Level Fields

| Field | Type | Used By | How |
|-------|------|---------|-----|
| `boat` | OrderBuilderBoat | Header, Page | Timeline, boat name, deadlines |
| `next_boat` | OrderBuilderBoat? | Page | "Next boat" info display |
| `high_priority` | Product[] | Page | Product array (tab 1) |
| `consider` | Product[] | Page | Product array (tab 2) |
| `well_covered` | Product[] | Page | Product array (tab 3) |
| `your_call` | Product[] | Page | Product array (tab 4) |
| `num_bls` | int | Page | BL count display |
| `recommended_bls` | int | Page | BL selector initial value |
| `available_bls` | int | Page | Available BL count |
| `recommended_bls_reason` | string | Page | Tooltip/label text |
| `shippable_bls` | int | Page | Shippable BL info |
| `shippable_m2` | Decimal | Page | Shippable m2 info |
| `summary` | OrderBuilderSummary | Summary component | Totals, capacity bars |
| `summary_reasoning` | OrderSummaryReasoning? | Strategy component, Page | Strategy display, counts |
| `warehouse_order_summary` | WarehouseOrderSummary? | Warehouse section | Section visibility (product_count > 0) |
| `add_to_production_summary` | AddToProductionSummary? | Production section | Section visibility + items |
| `factory_request_summary` | FactoryRequestSummary? | Factory section | Section visibility + items |
| `constraint_analysis` | ConstraintAnalysis? | RecalculateBar | Constraint display |
| `unable_to_ship` | UnableToShipSummary? | Alert component | Alert card (null = hidden) |
| `stability_forecast` | StabilityForecast? | Forecast card + modal | Recovery progress |
| `liquidation_clearance` | LiquidationClearanceProduct[] | Liquidation section | Shown if length > 0 |
| `capabilities` | FactoryCapabilities? | Page | Tab visibility flags |
| `shipping_cost_config` | ShippingCostConfig? | ShippingEstimate | Cost calculation |
| `unit_label` | string? | Page | "m2" or "uds" suffix |
| `is_unit_based` | bool? | Page | Unit type toggle |
| `factory_id` | string? | Page | Factory context |
| `factory_name` | string? | Page | Factory name display |
| `factory_timeline` | object? | Page | Milestone display |

---

## OrderBuilderBoat (Header + Page)

Read by: `OrderBuilderHeader.tsx`, `OrderBuilder.tsx`

| Field | Type | Usage |
|-------|------|-------|
| `boat_id` | string | Boat selector value |
| `name` | string | Display name, export |
| `departure_date` | date | Timeline, export |
| `arrival_date` | date | Timeline, warehouse date calc |
| `days_until_departure` | int | Timeline display |
| `days_until_arrival` | int | Timeline display |
| `days_until_warehouse` | int | Timeline display |
| `order_deadline` | date | Timeline milestone, deadline check |
| `days_until_order_deadline` | int | Deadline countdown, warning (<=7d) |
| `past_order_deadline` | bool | Conditional red styling |
| `booking_deadline` | date | Timeline milestone |
| `days_until_deadline` | int | Deadline alert threshold (<=3d) |
| `max_containers` | int | Capacity display |
| `carrier` | string? | Header display (e.g., "TIBA") |

---

## OrderBuilderProduct (Product Card -- THE CRITICAL CONTRACT)

Read by: `OrderBuilderProductCard.tsx`, `OrderBuilder.tsx`, `WarehouseOrderSection.tsx`

This is the core model. ~70 fields. The product card reads almost all of them.

### Identity
| Field | Type | Usage |
|-------|------|-------|
| `product_id` | string | All handlers (toggle, select, edit) |
| `sku` | string | Display, sort key, filtering |
| `description` | string? | Product name (dimmed in removed view) |

### Priority & Urgency -- BROKEN (Phase 4 fixes)
| Field | Type | Usage | Notes |
|-------|------|-------|-------|
| `priority` | enum | Group assignment (high_priority/consider/well_covered/your_call) | **Currently wrong**: FS products get downgraded to CONSIDER because suggested=0 |
| `urgency` | enum | Badge color (critical=red, urgent=orange, soon=yellow, ok=green) | **Currently wrong**: urgency recomputed from broken days_of_stock |
| `score.total` | int | Displayed in urgency badge (0-100) | |
| `score.stockout_risk` | int | Score breakdown | |
| `score.customer_demand` | int | Score breakdown | |
| `score.growth_trend` | int | Score breakdown | |
| `score.revenue_impact` | int | Score breakdown | |

### Stock Position
| Field | Type | Usage |
|-------|------|-------|
| `current_stock_m2` | Decimal | Warehouse stock display |
| `days_of_stock` | int? | Days remaining indicator, color coding (<=7d red, <=14d orange) |
| `daily_velocity_m2` | Decimal | Velocity display, base calc |
| `in_transit_m2` | Decimal | In-transit section (shown if > 0) |
| `pending_order_m2` | Decimal | Pending order section (shown if > 0) |
| `pending_order_pallets` | int | Pending order pallets |
| `pending_order_boat` | string? | Which boat pending is on |

### Forward Simulation Transparency
| Field | Type | Usage |
|-------|------|-------|
| `projected_stock_m2` | Decimal? | FS indicator display |
| `earlier_drafts_consumed_m2` | Decimal? | FS indicator display |
| `uses_forward_simulation` | bool | FS badge visibility condition |

### Suggestion -- THE BROKEN FIELDS
| Field | Type | Usage | Notes |
|-------|------|-------|-------|
| `suggested_pallets` | int | System suggestion display, snapshot save | **BROKEN**: Shows 0 when FS says 4/7/3 |
| `coverage_gap_m2` | Decimal | Gap info detail, removed products display | **BROKEN**: Shows 0 for FS products |
| `coverage_gap_pallets` | int | Removed product display, recalculate bar | **BROKEN**: Shows 0 for FS products |
| `days_to_cover` | int | Coverage period | |
| `total_demand_m2` | Decimal | Demand during period | |

### Selection State (User-Editable)
| Field | Type | Usage |
|-------|------|-------|
| `is_selected` | bool | Checkbox state, styling, filter for export |
| `selected_pallets` | int | Pallet input value |
| `pallet_conversion_factor` | Decimal? | m2/pallet conversion (defaults to 134.4 in frontend) |

### Trend Data
| Field | Type | Usage |
|-------|------|-------|
| `trend_direction` | enum | Trend arrow display (if !== 'stable') |
| `trend_strength` | enum | (Not directly displayed but available) |
| `velocity_change_pct` | Decimal | Trend percentage text |
| `velocity_90d_m2` | Decimal | 90-day velocity breakdown |
| `velocity_180d_m2` | Decimal | 6-month velocity breakdown |
| `velocity_trend_signal` | enum | Signal badge (growing/stable/declining) |
| `velocity_trend_ratio` | Decimal | Trend ratio calculation |

### Factory & Production
| Field | Type | Usage |
|-------|------|-------|
| `factory_available_m2` | Decimal | Factory section (shown if > 0), "SIESA: No stock" check |
| `factory_fill_status` | enum | Fill status icon/label |
| `factory_fill_message` | string? | (Available but not always shown) |
| `factory_lot_count` | int | Lot count display |
| `factory_largest_lot_m2` | Decimal? | (Available) |
| `factory_largest_lot_code` | string? | (Available) |
| `factory_status` | enum | Factory status display (in_production/not_scheduled) |
| `factory_production_date` | date? | Production date display |
| `factory_production_m2` | Decimal? | Production m2 display |
| `factory_ready_before_boat` | bool? | Color condition (green/amber) |
| `factory_timing_message` | string? | Factory timing text |
| `days_until_factory_ready` | int? | (Available) |
| `production_status` | enum | Status display (scheduled/in_progress/completed) |
| `production_requested_m2` | Decimal | Requested m2 |
| `production_completed_m2` | Decimal | Completed m2 |
| `production_can_add_more` | bool | "Can add more" indicator |
| `production_add_more_m2` | Decimal | Additional m2 suggestion |
| `production_add_more_alert` | string? | Alert text |

### Confidence
| Field | Type | Usage |
|-------|------|-------|
| `confidence` | enum | Confidence label + color (HIGH=green, MEDIUM=yellow, LOW=red) |
| `confidence_reason` | string | Reason text (shown if LOW) |

### Customer Demand
| Field | Type | Usage |
|-------|------|-------|
| `unique_customers` | int | Customer count display |
| `top_customer_name` | string? | Top customer display, export (primary_customer) |
| `top_customer_share` | Decimal? | Concentration % (threshold: > 0.3) |
| `customer_demand_score` | int | Priority score from customers |
| `customers_expecting_count` | int | Expecting customers count |
| `committed_orders_m2` | float | Committed orders display (shown if > 0) |
| `committed_orders_customer` | string? | Customer name for committed |
| `committed_orders_count` | int | Order count (shown if > 1) |
| `has_unfulfilled_demand` | bool | Section visibility |
| `unfulfilled_demand_m2` | float | Unfulfilled demand display |

### Calculation Breakdown (Expandable Detail)
| Field | Type | Usage |
|-------|------|-------|
| `calculation_breakdown.daily_velocity_m2` | Decimal | Base calculation display |
| `calculation_breakdown.lead_time_days` | int | Days in calculation |
| `calculation_breakdown.ordering_cycle_days` | int | Cycle days |
| `calculation_breakdown.base_quantity_m2` | Decimal | Base quantity display |
| `calculation_breakdown.trend_adjustment_m2` | Decimal | Trend adjustment line |
| `calculation_breakdown.trend_adjustment_pct` | Decimal | Trend percentage |
| `calculation_breakdown.minus_current_stock_m2` | Decimal | Minus warehouse line |
| `calculation_breakdown.minus_incoming_m2` | Decimal | Minus in-transit (shown if > 0) |
| `calculation_breakdown.final_suggestion_m2` | Decimal | Final suggestion line |
| `calculation_breakdown.final_suggestion_pallets` | int | Final pallets line |

### Availability Breakdown
| Field | Type | Usage |
|-------|------|-------|
| `availability_breakdown.siesa_now_m2` | Decimal | SIESA now display |
| `availability_breakdown.production_completing_m2` | Decimal | Production completing |
| `availability_breakdown.total_available_m2` | Decimal | Total available, capping logic |
| `availability_breakdown.suggested_order_m2` | Decimal | Suggested order |
| `availability_breakdown.shortfall_m2` | Decimal | Shortfall (shown if > 0) |
| `availability_breakdown.shortfall_note` | string? | Shortfall explanation |
| `availability_breakdown.can_fulfill` | bool | (Used for logic) |

### Reasoning Display
| Field | Type | Usage |
|-------|------|-------|
| `reasoning_display.why_product_sentence` | string | Fallback reasoning text |
| `reasoning_display.why_quantity_sentence` | string | Quantity reasoning |
| `reasoning_display.dominant_factor` | enum | Factor indicator |
| `reasoning_display.would_include_if` | string? | For excluded products |

### Full Calculation Breakdown (Deep Detail)
| Field | Type | Usage |
|-------|------|-------|
| `full_calculation_breakdown.coverage` | CoverageCalculation | Coverage math display |
| `full_calculation_breakdown.customer_demand` | CustomerDemandCalculation | Customer math display |
| `full_calculation_breakdown.selection` | SelectionCalculation | Selection logic display |
| `full_calculation_breakdown.summary_sentence` | string | One-line summary |

### Weight
| Field | Type | Usage |
|-------|------|-------|
| `weight_per_m2_kg` | Decimal | (Available for calc) |
| `total_weight_kg` | Decimal | Weight display, export |

---

## OrderBuilderSummary (Summary Component)

Read by: `OrderBuilderSummary.tsx`

| Field | Type | Usage |
|-------|------|-------|
| `total_pallets` | int | Pallet total |
| `total_containers` | int | Container total |
| `total_m2` | Decimal | m2 total |
| `total_weight_kg` | Decimal | Weight total |
| `containers_by_pallets` | int | Container calc by pallets |
| `containers_by_weight` | int | Container calc by weight |
| `weight_is_limiting` | bool | Weight warning |
| `boat_max_containers` | int | Boat capacity |
| `boat_remaining_containers` | int | Remaining capacity |
| `warehouse_current_pallets` | int | Current warehouse |
| `warehouse_capacity` | int | Max warehouse |
| `warehouse_after_delivery` | int | After delivery |
| `warehouse_utilization_after` | Decimal | Utilization % |
| `alerts` | Alert[] | Alert cards |

---

## OrderSummaryReasoning (Strategy Component + Page)

Read by: `OrderBuilderStrategy.tsx`, `OrderBuilder.tsx`

| Field | Type | Usage |
|-------|------|-------|
| `strategy` | enum | Strategy icon (STOCKOUT_PREVENTION/DEMAND_CAPTURE/BALANCED) |
| `critical_count` | int | Critical count badge |
| `urgent_count` | int | Urgent count badge |
| `stable_count` | int | Stable count |
| `excluded_count` | int | Excluded count |
| `excluded_products` | ExcludedProduct[] | Excluded list |
| `reasoning.strategy_sentence` | string | Strategy explanation |
| `reasoning.risk_sentence` | string | Risk explanation |
| `reasoning.constraint_sentence` | string | Constraint explanation |
| `reasoning.customer_sentence` | string? | Customer signal |
| `reasoning.limiting_factor` | string | Limiting factor badge |
| `reasoning.deferred_count` | int | Deferred count |
| `reasoning.customers_expecting` | int | Customer count |
| `reasoning.critical_count` | int | (Redundant with parent) |

---

## Section Summaries

### WarehouseOrderSummary
Read by: `WarehouseOrderSection.tsx`

| Field | Type | Usage |
|-------|------|-------|
| `product_count` | int | Section visibility (> 0) |
| `selected_count` | int | Count badge |
| `total_m2` | Decimal | Total |
| `total_pallets` | int | Total |
| `total_containers` | int | Total |
| `total_weight_kg` | Decimal | Total |
| `bl_count` | int | BL count |
| `boat_name` | string? | Display |
| `boat_departure` | date? | Display |

### AddToProductionSummary
Read by: `AddToProductionSection.tsx`

| Field | Type | Usage |
|-------|------|-------|
| `product_count` | int | Section visibility |
| `total_additional_m2` | Decimal | Total |
| `total_additional_pallets` | int | Total |
| `items` | AddToProductionItem[] | Item list |
| `estimated_ready_range` | string | Range display |
| `has_critical_items` | bool | Alert styling |
| `action_deadline` | string? | Deadline |
| `action_deadline_display` | string | Display text |

### FactoryRequestSummary
Read by: `FactoryRequestSection.tsx`

| Field | Type | Usage |
|-------|------|-------|
| `product_count` | int | Section visibility |
| `total_request_m2` | Decimal | Total |
| `total_request_pallets` | int | Total |
| `items` | FactoryRequestItem[] | Item list |
| `limit_m2` | Decimal | Quota limit |
| `utilization_pct` | Decimal | Quota bar |
| `remaining_m2` | Decimal | Remaining |
| `estimated_ready` | string | Ready date |
| `submit_deadline` | string? | Deadline |
| `submit_deadline_display` | string | Display text |

---

## What's Broken Right Now (Summary)

The bug: Forward Simulation correctly computes `suggested_pallets` (4, 7, 3 for critical products)
but OB's 470-line loop with 16 `if projection is not None:` forks corrupts them to 0.

**Broken fields for FS-path products:**
1. `suggested_pallets` = 0 (should be FS value)
2. `coverage_gap_m2` = 0 (should be FS coverage_gap_m2)
3. `coverage_gap_pallets` = 0 (should be FS coverage_gap_pallets)
4. `priority` = downgraded to CONSIDER (because suggested=0 triggers line 1627-1635)
5. `urgency` = recomputed incorrectly
6. `is_selected` = False (because suggested=0)
7. `selected_pallets` = 0 (because suggested=0)
8. All `calculation_breakdown` fields = stale/incorrect
9. All `full_calculation_breakdown` fields = stale/incorrect
10. `availability_breakdown` = may be incorrect
11. `reasoning_display` = based on wrong data

**Working fields (not affected by the loop):**
- All boat fields
- All velocity/trend fields (populated before the loop)
- All factory/production fields (populated independently)
- All customer demand fields (populated before the loop)
- `current_stock_m2`, `in_transit_m2` (from inventory, not loop)
- `weight_per_m2_kg` (from product data)
- Summary sections (warehouse, production, factory request) -- computed after products
- Stability forecast, unable-to-ship, constraint analysis

---

## Consumer Files (Reference)

| File | Imports |
|------|---------|
| `pages/OrderBuilder.tsx` | Main page, all top-level + products |
| `components/OrderBuilderProductCard.tsx` | Product fields |
| `components/OrderBuilderHeader.tsx` | Boat fields |
| `components/OrderBuilderSummary.tsx` | Summary fields |
| `components/OrderBuilderAlerts.tsx` | Alert fields |
| `components/OrderBuilderStrategy.tsx` | Strategy/reasoning |
| `components/WarehouseOrderSection.tsx` | Warehouse summary + products |
| `components/AddToProductionSection.tsx` | Production items |
| `components/FactoryRequestSection.tsx` | Factory items |
| `components/BLAllocationView.tsx` | BL allocation |
| `components/BLCard.tsx` | BL products |
| `components/RiskDistribution.tsx` | BL risk |
| `components/UnableToShipAlert.tsx` | Unable to ship |
| `components/StabilityForecastCard.tsx` | Stability |
| `components/StabilityForecastModal.tsx` | Stability detail |
| `components/ShippingEstimate.tsx` | Shipping costs |
| `components/RecalculateBar.tsx` | Constraints |
| `components/CustomersDueList.tsx` | Customer demand |
| `components/ExpectedDemandSection.tsx` | Demand viz |
| `components/order-builder/LiquidationClearanceSection.tsx` | Liquidation |
| `components/CallBeforeOrderingAlert.tsx` | Constraint alerts |
| `requests/orderBuilder.ts` | Type definitions (1086 lines) |
