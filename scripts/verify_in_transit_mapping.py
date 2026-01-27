"""
Script to verify SKU mapping for in-transit import.

Run from backend directory:
    python scripts/verify_in_transit_mapping.py
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import get_supabase_client
from unicodedata import normalize

# Excel products from dispatch file
EXCEL_PRODUCTS = {
    "ALMENDRO BEIGE BTE 51X51": 739.2,      # 604.8 + 134.4
    "TOLU CAFÉ 51X51": 1814.4,              # 67.2 + 1747.2
    "CEIBA GRIS OSCURO BTE 51X51": 1747.2,
    "GALERA RUSTICO GRIS BTE 51X51": 1075.2,
    "MIRACH 51X51": 2016.0,
    "QUIMBAYA GRIS 51X51": 1881.6,
    "TOLU BEIGE 51X51": 1881.6,
}

# M2 per pallet for conversion
M2_PER_PALLET = 134.4


def normalize_sku(name: str) -> str:
    """Normalize SKU for comparison - remove accents, format suffix, uppercase."""
    # Normalize unicode (é -> e)
    normalized = normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    # Remove common suffixes
    suffixes_to_remove = ['BTE 51X51', '51X51', 'BTE']
    for suffix in suffixes_to_remove:
        normalized = normalized.replace(suffix, '')
    # Uppercase and clean whitespace
    return ' '.join(normalized.upper().split())


def main():
    print("=" * 60)
    print("IN-TRANSIT IMPORT - SKU VERIFICATION")
    print("=" * 60)

    # Connect to database
    db = get_supabase_client()

    # Get all products from database
    result = db.table("products").select("id, sku").execute()
    db_products = {row['sku']: row['id'] for row in result.data}

    print(f"\nDatabase products: {len(db_products)}")
    print("-" * 40)
    for sku in sorted(db_products.keys()):
        print(f"  {sku}")

    print("\n" + "=" * 60)
    print("MAPPING EXCEL -> DATABASE")
    print("=" * 60)

    mappings = []
    unmatched = []

    for excel_name, m2 in EXCEL_PRODUCTS.items():
        # Try exact match first
        if excel_name in db_products:
            mappings.append({
                'excel': excel_name,
                'db_sku': excel_name,
                'db_id': db_products[excel_name],
                'm2': m2,
                'pallets': round(m2 / M2_PER_PALLET, 1)
            })
            continue

        # Try normalized match
        normalized_excel = normalize_sku(excel_name)
        matched = False

        for db_sku, db_id in db_products.items():
            normalized_db = normalize_sku(db_sku)
            if normalized_excel == normalized_db:
                mappings.append({
                    'excel': excel_name,
                    'db_sku': db_sku,
                    'db_id': db_id,
                    'm2': m2,
                    'pallets': round(m2 / M2_PER_PALLET, 1)
                })
                matched = True
                break

        if not matched:
            unmatched.append({
                'excel': excel_name,
                'normalized': normalized_excel,
                'm2': m2
            })

    # Print mapping table
    print(f"\n{'EXCEL NAME':<35} -> {'DATABASE SKU':<25} {'M²':>10} {'PALLETS':>8}")
    print("-" * 85)

    for m in mappings:
        print(f"{m['excel']:<35} -> {m['db_sku']:<25} {m['m2']:>10,.1f} {m['pallets']:>8}")

    if unmatched:
        print("\n" + "!" * 60)
        print("UNMATCHED PRODUCTS (NEED MANUAL REVIEW)")
        print("!" * 60)
        for u in unmatched:
            print(f"  {u['excel']} -> normalized: '{u['normalized']}' ({u['m2']:,.1f} m²)")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total Excel products: {len(EXCEL_PRODUCTS)}")
    print(f"  Matched to database:  {len(mappings)}")
    print(f"  Unmatched:            {len(unmatched)}")
    print(f"  Total m²:             {sum(m['m2'] for m in mappings):,.1f}")
    print(f"  Total pallets:        {sum(m['pallets'] for m in mappings):,.1f}")

    if len(unmatched) == 0:
        print("\n[OK] All products matched successfully!")
    else:
        print("\n[WARN]  Some products couldn't be matched. Review needed.")

    # Get current in-transit values
    print("\n" + "=" * 60)
    print("CURRENT IN-TRANSIT VALUES (before update)")
    print("=" * 60)

    for m in mappings:
        inv_result = db.table("inventory_snapshots")\
            .select("in_transit_qty, snapshot_date")\
            .eq("product_id", m['db_id'])\
            .order("snapshot_date", desc=True)\
            .limit(1)\
            .execute()

        current = inv_result.data[0]['in_transit_qty'] if inv_result.data else 0
        print(f"  {m['db_sku']:<25} current: {current:>8,.1f} m² -> new: {m['m2']:>8,.1f} m²")

    return mappings, unmatched


if __name__ == "__main__":
    main()
