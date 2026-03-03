# OB Refactor — Snapshot Comparison Guide

## Purpose

Capture Order Builder API responses BEFORE the refactor, then compare AFTER to verify zero behavior change.

## Pre-Refactor: Capture Baseline Snapshots

Run these against the **production** backend (Render) before merging the refactor branch.

### 1. Main OB Response (one boat)

```bash
# Replace <BOAT_ID> with an active boat's UUID
# Replace <TOKEN> with a valid auth token

curl -s "https://floor-tile-backend.onrender.com/api/order-builder?boat_id=<BOAT_ID>" \
  -H "Authorization: Bearer <TOKEN>" \
  | python -m json.tool > snapshot_before_boat1.json
```

### 2. Recalculate Endpoint

```bash
curl -s -X POST "https://floor-tile-backend.onrender.com/api/order-builder/recalculate" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"boat_id": "<BOAT_ID>", "selections": []}' \
  | python -m json.tool > snapshot_before_recalc.json
```

### 3. Multiple Boats (if available)

Repeat step 1 for 2-3 different boats to cover cascade behavior on boats 2+.

```bash
curl -s "https://floor-tile-backend.onrender.com/api/order-builder?boat_id=<BOAT_ID_2>" \
  -H "Authorization: Bearer <TOKEN>" \
  | python -m json.tool > snapshot_before_boat2.json
```

## Post-Refactor: Capture and Compare

After the refactor is deployed (or running locally), capture the same endpoints:

```bash
# Local (if uvicorn is running on port 8000):
curl -s "http://localhost:8000/api/order-builder?boat_id=<BOAT_ID>" \
  -H "Authorization: Bearer <TOKEN>" \
  | python -m json.tool > snapshot_after_boat1.json

# Compare:
diff snapshot_before_boat1.json snapshot_after_boat1.json
```

## What to Compare

### Must Be Identical

These fields must match exactly (the refactor is pure extraction — zero behavior change):

- `products[*].final_suggestion_m2`
- `products[*].final_suggestion_pallets`
- `products[*].current_stock_m2`
- `products[*].factory_available_m2`
- `products[*].days_of_stock`
- `products[*].urgency`
- `products[*].priority`
- `products[*].trend_direction`
- `products[*].trend_strength`
- `products[*].reasoning.*`
- `products[*].calculation_breakdown.*`
- `products[*].full_calculation_breakdown.*`
- `header.total_products`, `header.stockout_risk_count`, `header.at_risk_count`
- `header.can_ship_count`, `header.confidence_band`

### Acceptable Differences

- `metadata.generated_at` — timestamp will differ
- `metadata.processing_time_ms` — timing will differ
- Float precision (±0.01) due to intermediate rounding — unlikely but acceptable

## Quick Diff Script

```bash
# Strip timestamps and timing, then compare
jq 'del(.metadata.generated_at, .metadata.processing_time_ms)' snapshot_before_boat1.json > before_clean.json
jq 'del(.metadata.generated_at, .metadata.processing_time_ms)' snapshot_after_boat1.json > after_clean.json
diff before_clean.json after_clean.json
```

If `diff` produces no output, the refactor is behavior-identical.

## When to Run

1. **Before refactor**: Capture from production (Render) — this is the baseline
2. **After refactor on branch**: Run locally with `uvicorn main:app` if possible, or after merge to main
3. **Post-deploy**: Capture from production again after Render deploys the merge

## Notes

- Auth tokens can be obtained from the browser dev tools (Network tab → any API request → Authorization header)
- If local testing isn't possible (DB/env issues), rely on pytest + import check, then compare post-deploy
- The coherence audit script (`scripts/audit_fs_ob_coherence.py`) provides additional verification
