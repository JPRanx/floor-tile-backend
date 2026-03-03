# OB Cut List

> What works, what's broken, and exactly what to cut.
> Generated 2026-03-03. Target file: `services/order_builder_service.py` (4091 lines)

## The Bug in One Sentence

Forward Simulation returns `suggested_pallets=4` for MANAURE BEIGE, but OB shows `0` because
the 470-line per-product loop (lines 1496-1992) has 16 `if projection is not None:` forks
that compute intermediate values, then line 1621 uses `suggested = final_suggestion_pallets if daily_velocity_m2 > 0 else coverage_gap_pallets` — but `coverage_gap_pallets` on line 1620 is from `rec.coverage_gap_pallets` (stockout service), NOT from FS. When `daily_velocity_m2 == 0` (zero-velocity product), `suggested` becomes `coverage_gap_pallets` (wrong source). When `daily_velocity_m2 > 0`, it uses `final_suggestion_pallets` which IS correct from FS (line 1597). But then line 1627: `if suggested == 0: effective_priority = "WELL_COVERED"` — any product where FS says 0 gap gets demoted even if it was HIGH_PRIORITY for other reasons.

**UPDATE after deeper trace**: The actual corruption path for MANAURE BEIGE (velocity > 0):
1. Line 1597: `final_suggestion_pallets = projection.get("suggested_pallets", 0)` = **4** (correct)
2. Line 1620: `coverage_gap_pallets = max(0, rec.coverage_gap_pallets or 0)` = from stockout service (stale)
3. Line 1621: `suggested = final_suggestion_pallets if daily_velocity_m2 > 0 else coverage_gap_pallets` = **4** (correct so far)
4. Line 1627-1635: Priority recalculation — if suggested > 0, keeps original priority (OK)
5. Line 1887: `coverage_gap_m2=adjusted_coverage_gap` — for FS path this is `projection["coverage_gap_m2"]` (line 1804), which is correct
6. Line 1888: `coverage_gap_pallets=coverage_gap_pallets` — this is **rec.coverage_gap_pallets** from line 1620, NOT FS value!
7. Line 1889: `suggested_pallets=suggested` = 4 (correct)

So `suggested_pallets` should be correct for velocity > 0 products. The question is: why does the API return 0? Let me check the selection path...

8. Line 1938: `is_selected=False` and line 1939: `selected_pallets=0` — ALWAYS set to 0/False in the loop
9. Lines 2334-2428: Selection pass uses `p.suggested_pallets` to determine selection
10. Line 2347: `pallets_needed = min(p.suggested_pallets, max_shippable)` — IF `max_shippable=0` (no factory stock), product is skipped

**Root cause candidates**:
- `factory_available_m2` override at line 1744-1748 computes cascade SIESA — if FS cascade says 0 available for this boat, `max_shippable=0` → not selected
- Or `suggested_pallets` is correctly 4 but product lands in WELL_COVERED/CONSIDER group instead of HIGH_PRIORITY

This needs live debugging. But the fix is the same either way: the ProductAnalysis pattern eliminates all these scattered overrides.

---

## What WORKS (Don't Touch)

### Pre-Loop Data Gathering (lines 1401-1495) -- KEEP ALL
These fetch external data, no FS forks, clean code:

| Lines | What | Status |
|-------|------|--------|
| 1401-1403 | Buffer days calculation | OK |
| 1406 | Customer demand scores | OK |
| 1409-1419 | Factory status map | OK |
| 1421-1434 | Factory availability map (SIESA) | OK |
| 1436-1446 | Production schedule map | OK |
| 1448-1458 | Pending orders map | OK |
| 1460-1478 | Committed orders map | OK |
| 1480-1494 | Unfulfilled demand map | OK |

### Trend Reading (lines 1496-1512) -- KEEP
| Lines | What | Status |
|-------|------|--------|
| 1498-1509 | Read trend data from trend_data dict | OK, no forks |
| 1512 | Calculate urgency from days_of_stock | OK, will be overridden by analyzer |

### Post-Loop Code (lines 1959-1992) -- KEEP
| Lines | What | Status |
|-------|------|--------|
| 1960 | Calculate priority score | OK |
| 1961 | Generate reasoning display | OK |
| 1963-1967 | Group into priority buckets | OK |
| 1969-1984 | Sort by urgency/demand/stock | OK |

### Helper Methods (called but not in loop) -- KEEP ALL
| Method | Lines | Status |
|--------|-------|--------|
| `_calculate_urgency()` | ~1200 | OK |
| `_determine_primary_factor()` | ~1220 | OK |
| `_calculate_trend_adjustment()` | ~1240 | OK |
| `_calculate_availability_breakdown()` | ~940 | OK |
| `_build_full_calculation_breakdown()` | 998-1194 | MOSTLY OK, has 3 FS forks |
| `_calculate_priority_score()` | ~1280 | OK |
| `_generate_product_reasoning_display()` | ~1350 | OK |
| `_get_customer_demand_scores()` | external | OK |

---

## What's BROKEN (The 16 Forks)

### Fork Map: Every `if projection is not None:` in the loop

| # | Lines | What It Does | Symptom |
|---|-------|-------------|---------|
| 1 | 1517-1552 | FS path: override days_of_stock, urgency, trend, compute suggestion from FS values | MAIN FORK - should be one analyzer method |
| 2 | 1553-1592 | Fallback path: compute from inventory + trend + pending | MAIN FORK - should be other analyzer method |
| 3 | 1596-1599 | Pallet conversion: FS uses `projection.get("suggested_pallets")`, fallback uses math | Small fork, absorbed into analyzers |
| 4 | 1603 | CalculationBreakdown.lead_time_days: 0 if FS, days_to_cover if fallback | Ternary, absorbed |
| 5 | 1604 | CalculationBreakdown.ordering_cycle_days: buffer_from_fs vs buffer_days | Ternary, absorbed |
| 6 | 1614 | CalculationBreakdown.uses_projection: True/False | Direct from analyzer |
| 7 | 1615 | CalculationBreakdown.projected_stock_m2 | Direct from analyzer |
| 8 | 1616 | CalculationBreakdown.earlier_drafts_consumed_m2 | Direct from analyzer |
| 9 | 1690-1705 | Customer demand: FS reads from projection, fallback from customer_demand_data | Fork, absorbed into analyzers |
| 10 | 1744-1748 | Factory available m2: FS uses cascade SIESA, fallback uses raw SIESA | Fork, absorbed into analyzers |
| 11 | 1803-1816 | Coverage gap: FS uses projection["coverage_gap_m2"], fallback adds customer demand | Fork, absorbed into analyzers |
| 12 | 1864 | projection_coverage_pallets for full breakdown | Ternary, absorbed |
| 13 | 1865 | projection_projected_stock_m2 for full breakdown | Ternary, absorbed |
| 14 | 1866 | projection_earlier_drafts_consumed_m2 for full breakdown | Ternary, absorbed |
| 15 | 1945 | projected_stock_m2 in OrderBuilderProduct | Direct from analyzer |
| 16 | 1946 | earlier_drafts_consumed_m2 in OrderBuilderProduct | Direct from analyzer |

Plus 3 forks inside `_build_full_calculation_breakdown()`:
| # | Lines | What |
|---|-------|------|
| B1 | 1055-1058 | Override coverage_suggested_pallets with FS value |
| B2 | 1075 | uses_projection flag |
| B3 | 1076-1077 | projected_stock / earlier_drafts |

**Total: 19 FS forks across the entire product building path.**

---

## The Cut

### What Gets Replaced (lines 1515-1957)

The entire block from `projection = projection_map.get(...)` through the `OrderBuilderProduct(...)` constructor (lines 1515-1957) gets replaced with:

```python
# === NEW: One branch, one intermediate ===
projection = projection_map.get(rec.product_id) if projection_map else None

if projection is not None:
    analysis = self._analyze_from_projection(rec, projection, ...)
else:
    analysis = self._analyze_from_inventory(rec, trend, ...)

# === Shared path: build product from analysis ===
product = self._build_product_from_analysis(rec, analysis, ...)
```

### New Methods

1. **`_analyze_from_projection()`** (~60 lines)
   - Reads FS projection dict
   - Returns ProductAnalysis with all fields populated from FS

2. **`_analyze_from_inventory()`** (~65 lines)
   - Reads recommendation + inventory + trend + pending_orders + customer_demand
   - Returns ProductAnalysis with all fields computed

3. **`_build_product_from_analysis()`** (~120 lines)
   - Takes ProductAnalysis + factory data + production data
   - Builds OrderBuilderProduct (no forks)
   - Calls _build_full_calculation_breakdown (simplified, no FS params)
   - Calls _calculate_availability_breakdown

### ProductAnalysis Fields (dataclass)

```python
@dataclass
class ProductAnalysis:
    """Intermediate between FS/inventory and OrderBuilderProduct. Internal only."""
    # Source
    uses_projection: bool

    # Stock position (as the analyzer sees it)
    days_of_stock: Optional[int]
    urgency: str

    # Trend (may be overridden by FS)
    trend_direction: str
    trend_strength: str

    # Quantity chain
    base_quantity_m2: Decimal
    trend_adjustment_m2: Decimal
    trend_adjustment_pct: Decimal
    adjusted_quantity_m2: Decimal
    buffer_days: int
    total_coverage_days: int

    # Deductions
    minus_current: Decimal
    minus_incoming: Decimal
    pending_order_m2: Decimal
    pending_order_pallets: int
    pending_order_boat: Optional[str]

    # Suggestion
    final_suggestion_m2: Decimal
    final_suggestion_pallets: int
    adjusted_coverage_gap: Decimal

    # Customer demand
    customer_demand_score: int
    customers_expecting_count: int
    expected_customer_orders_m2: Decimal
    customer_names: list[str]

    # Factory (cascade-aware for FS, raw for fallback)
    factory_cascade_m2: Decimal  # For computation (coverage gap, fill status, SIESA cap)

    # FS transparency
    projected_stock_m2: Optional[Decimal]
    earlier_drafts_consumed_m2: Optional[Decimal]

    # Breakdown helpers
    lead_time_days_for_breakdown: int
    ordering_cycle_days_for_breakdown: int
```

---

## Lines to Cut vs Keep (Summary)

| Line Range | Content | Action |
|------------|---------|--------|
| 1380-1403 | Method signature + buffer calc | KEEP |
| 1406-1494 | Data fetching (7 maps) | KEEP |
| 1496-1512 | Trend reading for current product | KEEP (move into analyzer calls) |
| **1515-1552** | **FS fork #1: main FS path** | **CUT -> _analyze_from_projection()** |
| **1553-1592** | **Fallback fork: main inventory path** | **CUT -> _analyze_from_inventory()** |
| **1594-1599** | **Pallet conversion fork** | **CUT -> inside analyzers** |
| **1601-1617** | **CalculationBreakdown with 5 ternaries** | **CUT -> inside _build_product_from_analysis()** |
| **1619-1661** | **Priority recalculation + exclusion** | **CUT -> inside _build_product_from_analysis()** |
| **1662-1685** | **ProductReasoning construction** | **CUT -> inside _build_product_from_analysis()** |
| **1687-1716** | **Customer demand fork** | **CUT -> inside analyzers** |
| 1718-1734 | Factory production status (no fork) | KEEP (move to _build_product) |
| 1736-1740 | Factory availability raw (no fork) | KEEP (move to _build_product) |
| **1742-1748** | **Factory cascade fork** | **CUT -> inside analyzers** |
| 1750-1800 | Factory fill status (no fork) | KEEP (move to _build_product) |
| **1802-1816** | **Coverage gap fork** | **CUT -> inside analyzers** |
| 1818-1831 | Availability breakdown (no fork) | KEEP (move to _build_product) |
| 1833-1867 | Full calc breakdown (3 ternary forks) | SIMPLIFY (pass analysis fields) |
| 1869-1870 | Weight lookup | KEEP (move to _build_product) |
| 1872-1957 | OrderBuilderProduct constructor (scattered forks) | SIMPLIFY (read from analysis) |
| 1959-1967 | Score + reasoning + grouping | KEEP |
| 1969-1992 | Sorting | KEEP |

**Total lines cut/rewritten: ~440 (lines 1515-1957)**
**New code: ~250 (dataclass + 2 analyzers + 1 builder)**
**Net reduction: ~190 lines**
**Forks eliminated: 19 -> 1**
