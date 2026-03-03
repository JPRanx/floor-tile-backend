# OB Service Refactor: Extract `ProductAnalysis` Intermediate

## Context

The Order Builder service (`backend/services/order_builder_service.py`, 4091 lines) has a ~496-line per-product loop with 16 `if projection is not None:` forks scattered throughout. Each FS coherence fix added another override. This makes the code hard to trace, creates dead code, and produces triple-computed fields.

**Goal**: One branch at the top -> `ProductAnalysis` intermediate -> single formatting path. Zero behavior change.

---

## The Refactor

### New: `ProductAnalysis` dataclass (~30 fields)

Internal-only intermediate (NOT an API model). Captures every field that differs between FS and fallback paths:

- **Stock**: `days_of_stock`, `urgency`
- **Trend**: `direction`, `strength`
- **Quantity chain**: `base_quantity_m2`, `trend_adjustment_m2/pct`, `adjusted_quantity_m2`, `buffer_days`, `total_coverage_days`
- **Deductions**: `minus_current`, `minus_incoming`, `pending_order_m2/pallets/boat`
- **Suggestion**: `final_suggestion_m2`, `final_suggestion_pallets`, `adjusted_coverage_gap`
- **Customer demand**: `customer_demand_score`, `customers_expecting_count`, `expected_customer_orders_m2`, `customer_names`
- **Factory supply**: `factory_available_m2` (real SIESA for display), `factory_cascade_m2` (cascade-aware for computation)
- **FS transparency**: `uses_projection`, `projected_stock_m2`, `earlier_drafts_consumed_m2`
- **Breakdown helpers**: `lead_time_days_for_breakdown`, `ordering_cycle_days_for_breakdown`

Location: top of `order_builder_service.py` (internal, not in models/)

### New: `_analyze_from_projection()` (~50 lines)

Extracts FS path (currently lines 1517-1552 + scattered overrides). Maps `projection` dict -> `ProductAnalysis` directly. No computation duplication.

### New: `_analyze_from_inventory()` (~55 lines)

Extracts fallback path (currently lines 1553-1592 + scattered lookups). Computes `ProductAnalysis` from recommendations + inventory + trend. Includes depletion-during-transit adjustment.

### Modified: `_group_products_by_priority` loop body

**Before** (471 lines, 16 forks):
```
read trend -> FORK(FS: override 20 fields / fallback: compute 20 fields) ->
FORK(pallets) -> FORK(breakdown) -> FORK(customer_demand) ->
FORK(factory_avail) -> FORK(coverage_gap) -> FORK(full_breakdown) ->
build product
```

**After** (~280 lines, 1 branch):
```
read trend -> analysis = _analyze_from_projection() or _analyze_from_inventory()
-> build breakdown from analysis -> build reasoning from analysis
-> shared formatting -> build product from analysis
```

---

## Staging Plan

### Stage 1: Extract `ProductAnalysis` + analyzer methods
1. Add `ProductAnalysis` dataclass after imports
2. Add `_analyze_from_projection()` method -- direct extraction of FS path code
3. Add `_analyze_from_inventory()` method -- direct extraction of fallback path code
4. Replace fork in loop with 3-line dispatch
5. Replace all downstream reads with `analysis.field_name`
6. Remove customer_demand fork -- absorbed into analyzers
7. Remove factory_available_m2 override -- absorbed into analyzers
8. Remove coverage_gap fork -- absorbed into analyzers

### Stage 2: Collapse CalculationBreakdown construction
- Replace 5 ternary forks in `CalculationBreakdown(...)` with direct reads from `analysis`
- Zero forks in the breakdown constructor

### Stage 3: Collapse `_build_full_calculation_breakdown` fork
- Pass `analysis.final_suggestion_pallets` instead of `projection_coverage_pallets`
- Remove the 3 `projection_*` optional params from signature
- Remove internal `if uses_projection:` fork

---

## Files Modified

| File | Change |
|------|--------|
| `backend/services/order_builder_service.py` | Add `ProductAnalysis` dataclass, two `_analyze_*` methods, restructure loop |
| `backend/tests/unit/test_order_builder_service.py` | Add unit tests for analyzer methods |

## Files NOT Modified

- `backend/models/order_builder.py` -- API models untouched
- `frontend/` -- Zero frontend changes
- `backend/services/forward_simulation_service.py` -- FS output unchanged
- `backend/routes/order_builder.py` -- Routes unchanged

---

## Git Structure & Safe Deployment

### Repository Layout
- `backend/` -> GitHub: `JPRanx/floor-tile-backend` (deploys to **Render** from `main`)
- `frontend/` -> GitHub: `JPRanx/floor-tile-frontend` (deploys to **Vercel** from `main`)

Both auto-deploy on push to `main`.

### Branch Strategy
1. Create `refactor/product-analysis` branch in `backend/`
2. All 3 stages land on this branch with verified commits
3. Merge to `main` only after all stages pass verification
4. Users see zero change until merge -- Render only deploys `main`

### Commit Strategy
- One commit per stage
- Backend commits only
- Stage 1 is the big change (~200 lines moved); Stages 2-3 are small cleanups

---

## Cascade Safety: What's In vs Out of ProductAnalysis

Three fields have cascade behavior. Each handled explicitly:

| Field | In ProductAnalysis? | Display | Computation | Why? |
|-------|---|---|---|---|
| `factory_available_m2` | YES (real SIESA) | Product card | -- | Always shows physical SIESA inventory |
| `factory_cascade_m2` | YES (cascade-aware) | -- | Coverage gap, fill status, SIESA cap | What's LEFT after earlier boats consume |
| `current_stock_m2` | **NO** -- always `rec.warehouse_m2` | Product card | -- | Cascade inflates warehouse 30x. Stays outside analyzers. |

**Key rule**: display fields show reality (what's physically there). Computation fields use cascade-aware values (what's available for this boat considering earlier orders).

In the analyzer methods:
- `_analyze_from_projection()`: `factory_available_m2` = raw SIESA from inventory, `factory_cascade_m2` = supply_breakdown.factory_siesa + production
- `_analyze_from_inventory()`: both = raw SIESA (identical -- no cascade without FS)

In the shared formatting path:
- `OrderBuilderProduct.factory_available_m2` = `analysis.factory_available_m2` (real, for display)
- Coverage gap / fill status / SelectionCalculation.siesa_available = `analysis.factory_cascade_m2` (for computation)

## Dead Code Handling

Nothing is deleted -- code MOVES into the analyzer methods:
- `pending_orders_map` lookup -> inside `_analyze_from_inventory()`
- `customer_demand_data` lookup -> inside `_analyze_from_inventory()`
- `_calculate_trend_adjustment()` -> called inside `_analyze_from_inventory()`
- FS overrides (trend_direction, factory_available, etc.) -> inside `_analyze_from_projection()`

## Error Handling

- **Test failure mid-refactor**: Debug and fix on the branch. Pure extraction -- a test failure means a field mapping error.
- **Import error**: Fix syntax on the branch.
- **Something looks wrong locally**: Fix on the branch. Main is never touched until merge. If unfixable, delete the branch.

## Verification

1. **Import check**: `python -c "from services.order_builder_service import OrderBuilderService"`
2. **Run pytest**: `pytest backend/tests/unit/test_order_builder_service.py`
3. **Snapshot comparison**: Compare API responses before/after (see OB_REFACTOR_SNAPSHOTS.md)
4. **Coherence audit**: `python scripts/audit_fs_ob_coherence.py`

## Done Criteria

- Branch `refactor/product-analysis` pushed with 3 clean commits
- `pytest` passes
- Import check passes
- Summary: lines removed, forks eliminated, new methods added
- Ready-to-merge status
