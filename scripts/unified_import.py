"""
Unified Import Script — orchestrates all 6 data imports in dependency order.

Usage:
    # Full import with verification
    python backend/scripts/unified_import.py \
        --folder "data/uploads/Informes Tarragona 0210" \
        --snapshot-date 2026-02-09 \
        --received-orders OC002,OC003 \
        --verify

    # Verification only (no imports)
    python backend/scripts/unified_import.py \
        --verify-only --snapshot-date 2026-02-09
"""

import argparse
import glob
import os
import re
import sys
import time
from datetime import date, datetime

import requests

# Allow imports from backend/ when running as a script
_backend_dir = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _backend_dir)

from dotenv import load_dotenv
load_dotenv(os.path.join(_backend_dir, ".env"))

# ─────────────────────────────────────────────────────────────
# FILE DETECTION PATTERNS
# ─────────────────────────────────────────────────────────────

FILE_PATTERNS = {
    "boats": r"Tabla de Booking",
    "warehouse": r"INVENTARIO POR PRODUCTOS",
    "siesa": r"INVENTARIO FEBRERO|INVENTARIO \d{2}\.xlsx",
    "production": r"Programa de Produccion",
    "sales": r"REPORTE VENTAS",
    "dispatch": r"PROGRAMACI[OÓ]N DE DESPACHO|PROGRAMACION DE DESPACHO",
}

# Display names for each step
STEP_NAMES = {
    "boats": "Boat Schedule",
    "warehouse": "Warehouse Inventory",
    "siesa": "SIESA Factory",
    "production": "Production Schedule",
    "sales": "Sales Report",
    "dispatch": "In-Transit",
}

# Import execution order
IMPORT_ORDER = ["boats", "warehouse", "siesa", "production", "sales", "dispatch"]


# ─────────────────────────────────────────────────────────────
# FILE DETECTION
# ─────────────────────────────────────────────────────────────

def detect_files(folder: str) -> dict[str, str]:
    """
    Scan folder and match files to import types by regex pattern.

    Returns dict of import_key -> absolute file path.
    Raises ValueError if any required file is missing.
    """
    if not os.path.isdir(folder):
        raise ValueError(f"Folder not found: {folder}")

    # Use glob to get all files (handles encoding issues with special chars)
    all_files = glob.glob(os.path.join(folder, "*"))
    detected = {}

    for key, pattern in FILE_PATTERNS.items():
        for filepath in all_files:
            basename = os.path.basename(filepath)
            if re.search(pattern, basename, re.IGNORECASE):
                detected[key] = filepath
                break

    return detected


# ─────────────────────────────────────────────────────────────
# IMPORT STEPS
# ─────────────────────────────────────────────────────────────

def import_boats(base_url: str, filepath: str, **kwargs) -> str:
    """POST /api/boats/upload"""
    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/api/boats/upload",
            files={"file": (os.path.basename(filepath), f)},
        )
    resp.raise_for_status()
    data = resp.json()
    imported = data.get("imported", 0)
    updated = data.get("updated", 0)
    skipped_rows = data.get("skipped_rows", [])
    summary = f"{imported} imported, {updated} updated"
    if skipped_rows:
        summary += f" ({len(skipped_rows)} rows skipped)"
    return summary


def import_warehouse(base_url: str, filepath: str, **kwargs) -> str:
    """POST /api/inventory/upload"""
    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/api/inventory/upload",
            files={"file": (os.path.basename(filepath), f)},
        )
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records_created", 0)
    return f"{records} products"


def import_siesa(base_url: str, filepath: str, snapshot_date: str, **kwargs) -> str:
    """POST /api/inventory/siesa/upload?snapshot_date=X"""
    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/api/inventory/siesa/upload",
            files={"file": (os.path.basename(filepath), f)},
            params={"snapshot_date": snapshot_date},
        )
    resp.raise_for_status()
    data = resp.json()
    products = data.get("unique_products", 0)
    total_m2 = data.get("total_m2_available", 0)
    return f"{products} products synced ({total_m2:,.0f} m2)"


def import_production(base_url: str, filepath: str, **kwargs) -> str:
    """POST /api/production-schedule/upload-replace"""
    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/api/production-schedule/upload-replace",
            files={"file": (os.path.basename(filepath), f)},
        )
    resp.raise_for_status()
    data = resp.json()
    total = data.get("total_rows_parsed", 0)
    matched = data.get("matched_to_products", 0)
    return f"{total} records ({matched} matched)"


def import_sales(base_url: str, filepath: str, **kwargs) -> str:
    """POST /api/sales/upload"""
    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/api/sales/upload",
            files={"file": (os.path.basename(filepath), f)},
        )
    resp.raise_for_status()
    data = resp.json()
    created = data.get("created", 0)
    return f"{created} records"


def import_dispatch(
    base_url: str, filepath: str, snapshot_date: str, received_orders: str, **kwargs
) -> str:
    """POST /api/inventory/in-transit/upload?snapshot_date=X&received_orders=Y"""
    params = {"snapshot_date": snapshot_date}
    if received_orders:
        params["received_orders"] = received_orders

    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/api/inventory/in-transit/upload",
            files={"file": (os.path.basename(filepath), f)},
            params=params,
        )
    resp.raise_for_status()
    data = resp.json()
    updated = data.get("products_updated", 0)
    total_m2 = data.get("total_in_transit_m2", 0)
    return f"{updated} products ({total_m2:,.0f} m2)"


# Map import keys to their functions
IMPORT_FUNCTIONS = {
    "boats": import_boats,
    "warehouse": import_warehouse,
    "siesa": import_siesa,
    "production": import_production,
    "sales": import_sales,
    "dispatch": import_dispatch,
}


# ─────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────

def run_unified_import(
    folder: str,
    snapshot_date: str,
    received_orders: str,
    base_url: str,
    skip_steps: set[str] | None = None,
) -> bool:
    """
    Run all 6 imports in dependency order. Stops on first error.

    Returns True if all imports succeeded, False otherwise.
    """
    separator = "=" * 61

    print(separator)
    print(f"  UNIFIED IMPORT -- {snapshot_date}")
    print(separator)
    print()

    # ── Step 1: Detect files ──
    print("Files detected:")
    try:
        detected = detect_files(folder)
    except ValueError as e:
        print(f"  ERROR: {e}")
        return False

    missing = []
    for key in IMPORT_ORDER:
        name = STEP_NAMES[key]
        if key in detected:
            basename = os.path.basename(detected[key])
            # Truncate long filenames
            display = basename if len(basename) <= 50 else basename[:47] + "..."
            print(f"  + {name + ':':<25} {display}")
        else:
            print(f"  X {name + ':':<25} NOT FOUND")
            missing.append(name)

    print()

    if missing:
        print(f"ERROR: Missing {len(missing)} file(s): {', '.join(missing)}")
        print("Import aborted.")
        return False

    skip = skip_steps or set()

    # ── Step 2: Check server is reachable ──
    try:
        health = requests.get(f"{base_url}/docs", timeout=5)
        if health.status_code != 200:
            print(f"ERROR: Server at {base_url} returned status {health.status_code}")
            return False
    except requests.ConnectionError:
        print(f"ERROR: Cannot connect to server at {base_url}")
        print("Make sure the server is running: uvicorn main:app --reload")
        return False

    # ── Step 3: Execute imports in order ──
    print("Import Progress:")
    results = {}
    all_ok = True

    for i, key in enumerate(IMPORT_ORDER, 1):
        name = STEP_NAMES[key]
        label = f"  [{i}/6] {name}"
        padding = "." * (40 - len(label))

        if key in skip:
            print(f"{label}{padding} SKIP")
            continue

        # Print progress indicator without newline
        sys.stdout.write(f"{label}{padding} ")
        sys.stdout.flush()

        try:
            fn = IMPORT_FUNCTIONS[key]
            summary = fn(
                base_url=base_url,
                filepath=detected[key],
                snapshot_date=snapshot_date,
                received_orders=received_orders,
            )
            print(f"OK  {summary}")
            results[key] = summary

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            body = ""
            try:
                body = e.response.json() if e.response is not None else ""
            except Exception:
                body = e.response.text[:200] if e.response is not None else ""
            print(f"FAIL (HTTP {status})")
            print(f"         {body}")
            all_ok = False
            break

        except Exception as e:
            print(f"FAIL ({type(e).__name__}: {e})")
            all_ok = False
            break

    print()

    # ── Step 4: Summary ──
    if all_ok:
        print(f"IMPORT COMPLETE")
    else:
        print(f"IMPORT FAILED -- stopped at step {len(results) + 1}/6")

    print(separator)

    return all_ok


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def run_verification(snapshot_date_str: str) -> bool:
    """Run verification checks and print report."""
    from services.import_verification_service import (
        run_verification as _run_checks,
        print_verification_report,
    )
    sd = date.fromisoformat(snapshot_date_str)
    results = _run_checks(sd)
    return print_verification_report(sd, results)


def main():
    parser = argparse.ArgumentParser(
        description="Unified import: orchestrate all 6 data imports in dependency order."
    )
    parser.add_argument(
        "--folder",
        default="",
        help="Path to folder containing the 6 Excel files",
    )
    parser.add_argument(
        "--snapshot-date",
        required=True,
        help="Target snapshot date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--received-orders",
        default="",
        help="Comma-separated order numbers to exclude from in-transit (e.g., OC002,OC003)",
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Base URL of the running API server (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--skip",
        default="",
        help="Comma-separated steps to skip (e.g., boats,sales). "
             f"Valid: {','.join(IMPORT_ORDER)}",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run verification checks after import completes",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip imports, only run verification checks",
    )

    args = parser.parse_args()

    # Validate snapshot_date format
    try:
        datetime.strptime(args.snapshot_date, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: Invalid date format '{args.snapshot_date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    # ── Verify-only mode ──
    if args.verify_only:
        passed = run_verification(args.snapshot_date)
        sys.exit(0 if passed else 1)

    # ── Import mode (requires --folder) ──
    if not args.folder:
        print("ERROR: --folder is required for import mode.")
        sys.exit(1)

    # Resolve folder path
    folder = os.path.abspath(args.folder)

    # Parse skip steps
    skip_steps = set()
    if args.skip:
        skip_steps = {s.strip() for s in args.skip.split(",") if s.strip()}
        invalid = skip_steps - set(IMPORT_ORDER)
        if invalid:
            print(f"ERROR: Invalid skip steps: {invalid}")
            print(f"  Valid steps: {', '.join(IMPORT_ORDER)}")
            sys.exit(1)

    success = run_unified_import(
        folder=folder,
        snapshot_date=args.snapshot_date,
        received_orders=args.received_orders,
        base_url=args.base_url,
        skip_steps=skip_steps,
    )

    # ── Post-import verification ──
    if success and args.verify:
        passed = run_verification(args.snapshot_date)
        if not passed:
            success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
