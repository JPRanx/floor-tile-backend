"""
Script to update in-transit inventory from dispatch file.

Run from backend directory:
    python scripts/update_in_transit.py
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import get_supabase_client
from unicodedata import normalize

# In-transit quantities to set (m2)
IN_TRANSIT_DATA = {
    "ALMENDRO BEIGE BTE": 739.2,
    "TOLU CAFE": 1814.4,
    "CEIBA GRIS OSCURO BTE": 1747.2,
    "GALERA RUSTICO GRIS": 1075.2,
    "MIRACH": 2016.0,
    "QUIMBAYA GRIS": 1881.6,
    "TOLU BEIGE": 1881.6,
}


def main():
    print("=" * 60)
    print("IN-TRANSIT INVENTORY UPDATE")
    print("=" * 60)

    # Connect to database
    db = get_supabase_client()

    # Get product IDs
    result = db.table("products").select("id, sku").execute()
    sku_to_id = {row['sku']: row['id'] for row in result.data}

    updated_count = 0
    errors = []

    print("\nUpdating inventory_snapshots...")
    print("-" * 60)

    for sku, in_transit_m2 in IN_TRANSIT_DATA.items():
        if sku not in sku_to_id:
            errors.append(f"SKU not found: {sku}")
            continue

        product_id = sku_to_id[sku]

        # Get the latest snapshot for this product
        snapshot_result = db.table("inventory_snapshots")\
            .select("id, in_transit_qty, snapshot_date")\
            .eq("product_id", product_id)\
            .order("snapshot_date", desc=True)\
            .limit(1)\
            .execute()

        if not snapshot_result.data:
            errors.append(f"No snapshot found for: {sku}")
            continue

        snapshot = snapshot_result.data[0]
        snapshot_id = snapshot['id']
        old_value = snapshot['in_transit_qty'] or 0

        # Update the snapshot
        try:
            update_result = db.table("inventory_snapshots")\
                .update({"in_transit_qty": in_transit_m2})\
                .eq("id", snapshot_id)\
                .execute()

            if update_result.data:
                print(f"  [OK] {sku:<25} {old_value:>8.1f} -> {in_transit_m2:>8.1f} m2")
                updated_count += 1
            else:
                errors.append(f"Update returned no data for: {sku}")

        except Exception as e:
            errors.append(f"Error updating {sku}: {str(e)}")

    # Summary
    print("\n" + "=" * 60)
    print("UPDATE SUMMARY")
    print("=" * 60)
    print(f"  Records updated: {updated_count}")
    print(f"  Errors: {len(errors)}")

    if errors:
        print("\nERRORS:")
        for err in errors:
            print(f"  - {err}")

    # Verify by querying updated values
    print("\n" + "=" * 60)
    print("VERIFICATION - Current in_transit_qty values")
    print("=" * 60)

    for sku in IN_TRANSIT_DATA.keys():
        if sku not in sku_to_id:
            continue

        product_id = sku_to_id[sku]
        verify_result = db.table("inventory_snapshots")\
            .select("in_transit_qty, snapshot_date")\
            .eq("product_id", product_id)\
            .order("snapshot_date", desc=True)\
            .limit(1)\
            .execute()

        if verify_result.data:
            val = verify_result.data[0]['in_transit_qty']
            date = verify_result.data[0]['snapshot_date']
            print(f"  {sku:<25} in_transit_qty: {val:>8.1f} m2  (snapshot: {date})")

    print("\n[DONE] Update complete.")
    return updated_count, errors


if __name__ == "__main__":
    main()
