"""
Historical sales import from VENTAS ANUAL.xls

Imports sales data from historical Excel file into database.
Filters out non-tile products and 20X61 size variants.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import date, datetime, timedelta
from decimal import Decimal
import requests
from typing import Optional
from collections import defaultdict

# Configuration
API_BASE = "http://localhost:8000/api"
EXCEL_PATH = r"C:\Users\Jorge Alexander\floor-tile-saas\data\phase2-samples\VENTAS ANUAL.xlsx"

# Products to skip (non-tile or 20X61 variants)
SKIP_PATTERNS = [
    "20X61",  # 20X61 size variants
    "ZOCALO", "CENEFA", "LISTELO",  # Accessories
    "PIEDRA",  # Stone products
    "FONDO PISCINA", "ESCALON",  # Pool/stair products
]

# SKU normalization mappings for Excel → Database
SKU_MAPPINGS = {
    # BTE variants
    "NOGAL GRIS OSC BTE": "NOGAL GRIS OSCURO BTE",
    "CEIBA GRIS OSC BTE": "CEIBA GRIS OSCURO BTE",
    "CEIBA GRIS CLR BTE": "CEIBA GRIS CLARO BTE",
    "BARANOA CAF BTE": "BARANOA CAFE BTE",
    # Non-BTE that should map to BTE
    "NOGAL GRIS OSCURO": "NOGAL GRIS OSCURO BTE",
    "CEIBA GRIS OSCURO": "CEIBA GRIS OSCURO BTE",
    "CEIBA GRIS CLARO": "CEIBA GRIS CLARO BTE",
    "BARANOA CAFE": "BARANOA CAFE BTE",
    # Encoding fixes
    "CEIBA CAFE": "CEIBA CAFÉ",
    "MOMPOX CAFE": "MOMPOX CAFÉ",
    "NOGAL CAFE": "NOGAL CAFÉ",
    "SAMAN CAFE": "SAMAN CAFÉ",
    # Truncated names
    "CEIBA GRIS OSC": "CEIBA GRIS OSC",
    "NOGAL GRIS OSC": "NOGAL GRIS OSC",
}


def should_skip_sku(sku: str) -> bool:
    """Check if SKU should be skipped."""
    sku_upper = sku.upper()
    for pattern in SKIP_PATTERNS:
        if pattern in sku_upper:
            return True
    return False


def normalize_sku(raw_sku: str) -> str:
    """Normalize SKU for matching to database."""
    import re
    import unicodedata

    sku = raw_sku.strip().upper()

    # Remove dimension suffix like "(T) 51X51-1" or "(T) 30,25X61-1"
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)

    # Also handle suffix without (T) like "51X51-1"
    sku = re.sub(r'\s+51X51-1$', '', sku)

    # Remove ALL accents using unicodedata (RÚSTICO → RUSTICO, TOLÚ → TOLU, etc.)
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')

    # Fix encoding issues - strip replacement character
    sku = sku.replace("�", "")  # Remove replacement character
    sku = sku.replace("Ã", "A")  # Mojibake fix

    # Apply known mappings
    if sku in SKU_MAPPINGS:
        sku = SKU_MAPPINGS[sku]

    return sku


def get_week_start(d: date) -> date:
    """Get Monday of the week containing date d."""
    # weekday() returns 0 for Monday
    days_since_monday = d.weekday()
    return d - timedelta(days=days_since_monday)


def get_products() -> dict:
    """Get all products as SKU -> ID mapping."""
    import unicodedata

    response = requests.get(f"{API_BASE}/products", params={"page_size": 100})
    response.raise_for_status()
    data = response.json()

    mapping = {}
    for p in data["data"]:
        sku = p["sku"].upper()
        product_id = p["id"]

        # Add the base SKU
        mapping[sku] = product_id

        # Also add normalized version (no accents)
        sku_normalized = unicodedata.normalize('NFD', sku)
        sku_normalized = ''.join(c for c in sku_normalized if unicodedata.category(c) != 'Mn')
        mapping[sku_normalized] = product_id

        # Add version without BTE suffix
        if sku.endswith(" BTE"):
            base = sku[:-4]
            mapping[base] = product_id
            # Also normalized
            base_norm = unicodedata.normalize('NFD', base)
            base_norm = ''.join(c for c in base_norm if unicodedata.category(c) != 'Mn')
            mapping[base_norm] = product_id

    return mapping


def parse_and_aggregate_excel(products: dict) -> tuple:
    """
    Parse Excel and aggregate sales by week and product.

    Returns:
        (aggregated_sales, stats) where aggregated_sales is dict of
        (week_start, product_id) -> total_quantity
    """
    print(f"Reading {EXCEL_PATH}...")
    df = pd.read_excel(EXCEL_PATH)

    print(f"Total rows in Excel: {len(df)}")

    # Track stats
    stats = {
        "total_rows": len(df),
        "skipped_pattern": 0,
        "skipped_no_match": 0,
        "processed": 0,
        "unmatched_skus": set(),
    }

    # Aggregate by (week_start, product_id)
    aggregated = defaultdict(Decimal)

    for idx, row in df.iterrows():
        raw_sku = str(row["SKU"]).strip()

        # Skip unwanted products
        if should_skip_sku(raw_sku):
            stats["skipped_pattern"] += 1
            continue

        # Normalize SKU
        sku = normalize_sku(raw_sku)

        # Find product ID
        product_id = products.get(sku)
        if not product_id:
            # Try without trailing spaces/chars
            sku_clean = sku.rstrip()
            product_id = products.get(sku_clean)

        if not product_id:
            stats["skipped_no_match"] += 1
            stats["unmatched_skus"].add(raw_sku)
            continue

        # Parse date and get week start
        fecha = row["FECHA"]
        if isinstance(fecha, str):
            fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
        elif isinstance(fecha, datetime):
            fecha = fecha.date()

        week_start = get_week_start(fecha)

        # Parse quantity
        qty = Decimal(str(row["MT2"]))

        # Aggregate
        key = (week_start, product_id)
        aggregated[key] += qty
        stats["processed"] += 1

    return aggregated, stats


def create_sales_records(aggregated: dict) -> int:
    """Create sales records via API."""
    created = 0
    errors = 0

    total = len(aggregated)
    print(f"\nCreating {total} aggregated sales records...")

    for i, ((week_start, product_id), quantity) in enumerate(aggregated.items()):
        try:
            response = requests.post(
                f"{API_BASE}/sales",
                json={
                    "product_id": product_id,
                    "week_start": week_start.isoformat(),
                    "quantity_m2": float(quantity)
                }
            )
            if response.status_code == 201:
                created += 1
            else:
                errors += 1
                if errors <= 5:
                    print(f"  Error: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Exception: {e}")

        # Progress indicator
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{total}")

    print(f"\nCreated: {created}, Errors: {errors}")
    return created


def main():
    print("=" * 60)
    print("HISTORICAL SALES IMPORT")
    print("=" * 60)

    # Get current product mappings
    print("\n1. Loading products...")
    products = get_products()
    print(f"   Loaded {len(products)} product SKU mappings")

    # Get current sales count
    response = requests.get(f"{API_BASE}/sales/count/total")
    before_count = response.json()["count"]
    print(f"   Current sales records: {before_count}")

    # Parse and aggregate Excel
    print("\n2. Parsing and aggregating Excel file...")
    aggregated, stats = parse_and_aggregate_excel(products)

    print(f"\n   STATISTICS:")
    print(f"   - Total rows:         {stats['total_rows']}")
    print(f"   - Skipped (pattern):  {stats['skipped_pattern']}")
    print(f"   - Skipped (no match): {stats['skipped_no_match']}")
    print(f"   - Processed:          {stats['processed']}")
    print(f"   - Unique week/product combinations: {len(aggregated)}")

    if stats["unmatched_skus"]:
        print(f"\n   UNMATCHED SKUs ({len(stats['unmatched_skus'])}):")
        for sku in sorted(stats["unmatched_skus"])[:10]:
            print(f"   - {sku}")
        if len(stats["unmatched_skus"]) > 10:
            print(f"   ... and {len(stats['unmatched_skus']) - 10} more")

    # Import data
    print("\n3. Creating sales records...")
    created = create_sales_records(aggregated)

    # Final count
    response = requests.get(f"{API_BASE}/sales/count/total")
    after_count = response.json()["count"]

    print("\n" + "=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"Sales records: {before_count} -> {after_count} (+{after_count - before_count})")
    print(f"Records created: {created}")


if __name__ == "__main__":
    main()
