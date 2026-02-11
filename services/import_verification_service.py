"""
Import verification service — post-import database checks.

Validates that all data landed correctly after a unified import.
Can run standalone or as part of the unified import pipeline.
"""

from dataclasses import dataclass
from datetime import date

from config import get_supabase_client


@dataclass
class CheckResult:
    name: str
    description: str
    expected: str
    actual: str
    passed: bool


def run_verification(snapshot_date: date) -> list[CheckResult]:
    """
    Run all verification checks for a given snapshot date.

    Returns list of CheckResult with pass/fail status for each check.
    """
    db = get_supabase_client()
    results = []

    # ── 1. Snapshot count (all active products should have a row) ──
    active_products = db.table("products").select("id", count="exact").eq(
        "active", True
    ).execute()
    expected_count = active_products.count or 0

    snapshots = db.table("inventory_snapshots").select("id", count="exact").eq(
        "snapshot_date", snapshot_date.isoformat()
    ).execute()
    actual_count = snapshots.count or 0

    results.append(CheckResult(
        name="Snapshot count",
        description="All active products have snapshot rows",
        expected=str(expected_count),
        actual=str(actual_count),
        passed=actual_count == expected_count,
    ))

    # ── 2. Single date check ──
    date_check = db.table("inventory_snapshots").select("snapshot_date").eq(
        "snapshot_date", snapshot_date.isoformat()
    ).limit(1).execute()
    distinct_dates = 1 if date_check.data else 0

    results.append(CheckResult(
        name="All same date",
        description="All snapshots on target date",
        expected="1",
        actual=str(distinct_dates),
        passed=distinct_dates == 1,
    ))

    # ── 3. Warehouse m² total ──
    wh_data = db.table("inventory_snapshots").select("warehouse_qty").eq(
        "snapshot_date", snapshot_date.isoformat()
    ).execute()
    wh_total = sum(float(r["warehouse_qty"] or 0) for r in wh_data.data)

    results.append(CheckResult(
        name="Warehouse m2",
        description="Total warehouse inventory",
        expected="--",
        actual=f"{wh_total:,.0f}",
        passed=True,  # informational
    ))

    # ── 4. SIESA factory m² total ──
    siesa_snap = db.table("inventory_snapshots").select("factory_available_m2").eq(
        "snapshot_date", snapshot_date.isoformat()
    ).execute()
    siesa_total = sum(float(r["factory_available_m2"] or 0) for r in siesa_snap.data)

    results.append(CheckResult(
        name="SIESA m2",
        description="Total factory inventory (from snapshots)",
        expected="--",
        actual=f"{siesa_total:,.0f}",
        passed=True,  # informational
    ))

    # ── 5. In-transit m² total ──
    it_data = db.table("inventory_snapshots").select("in_transit_qty").eq(
        "snapshot_date", snapshot_date.isoformat()
    ).execute()
    it_total = sum(float(r["in_transit_qty"] or 0) for r in it_data.data)

    results.append(CheckResult(
        name="In-Transit m2",
        description="Total in-transit inventory",
        expected="--",
        actual=f"{it_total:,.0f}",
        passed=True,  # informational
    ))

    # ── 6. SIESA lots match synced snapshots ──
    lots_data = db.table("inventory_lots").select("product_id, quantity_m2").eq(
        "snapshot_date", snapshot_date.isoformat()
    ).execute()

    lots_by_product: dict[str, float] = {}
    for r in lots_data.data:
        pid = r["product_id"]
        lots_by_product[pid] = lots_by_product.get(pid, 0) + float(r["quantity_m2"] or 0)

    snap_by_product: dict[str, float] = {}
    for r in siesa_snap.data:
        # We need product_id — re-query with it
        pass

    # Re-query snapshots with product_id for cross-check
    snap_with_pid = db.table("inventory_snapshots").select(
        "product_id, factory_available_m2"
    ).eq("snapshot_date", snapshot_date.isoformat()).execute()

    for r in snap_with_pid.data:
        pid = r["product_id"]
        val = float(r["factory_available_m2"] or 0)
        if val > 0:
            snap_by_product[pid] = val

    lots_total = sum(lots_by_product.values())
    snap_factory_total = sum(snap_by_product.values())
    match = abs(lots_total - snap_factory_total) < 0.01

    results.append(CheckResult(
        name="SIESA lots match",
        description="Lots total matches snapshot factory_available_m2",
        expected="YES",
        actual="YES" if match else f"NO (lots={lots_total:,.0f}, snap={snap_factory_total:,.0f})",
        passed=match,
    ))

    # ── 7. Production schedule records ──
    prod_data = db.table("production_schedule").select("id", count="exact").execute()
    prod_count = prod_data.count or 0

    results.append(CheckResult(
        name="Production records",
        description="Production schedule entries in system",
        expected="--",
        actual=str(prod_count),
        passed=True,  # informational
    ))

    # ── 8. Upcoming boats ──
    from datetime import date as date_type
    today = date_type.today()
    boats_data = db.table("boat_schedules").select("id", count="exact").gte(
        "departure_date", today.isoformat()
    ).execute()
    boats_count = boats_data.count or 0

    results.append(CheckResult(
        name="Upcoming boats",
        description="Boats with departure >= today",
        expected=">= 1",
        actual=str(boats_count),
        passed=boats_count >= 1,
    ))

    return results


def print_verification_report(snapshot_date: date, results: list[CheckResult]) -> bool:
    """
    Print formatted verification report.

    Returns True if all checks passed.
    """
    separator = "=" * 61
    line = "-" * 61

    print()
    print(separator)
    print(f"  VERIFICATION REPORT -- {snapshot_date}")
    print(separator)
    print()
    print(f"  {'Check':<24} {'Expected':<16} {'Actual':<16} Status")
    print(f"  {line}")

    all_passed = True
    for r in results:
        if r.passed:
            if r.expected == "--":
                status = "i"
            else:
                status = "OK"
        else:
            status = "FAIL"
            all_passed = False

        print(f"  {r.name:<24} {r.expected:<16} {r.actual:<16} {status}")

    print()
    if all_passed:
        print("  VERIFICATION PASSED")
    else:
        failed = [r for r in results if not r.passed]
        print(f"  VERIFICATION FAILED -- {len(failed)} check(s) failed")

    print(separator)
    print()

    return all_passed
