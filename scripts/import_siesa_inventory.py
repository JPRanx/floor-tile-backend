"""Import SIESA factory inventory from 20202 (1).xlsx.

Updates factory_available_m2 field in inventory_snapshots.
Does NOT touch warehouse_qty or in_transit_qty.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import unicodedata
from decimal import Decimal
from collections import defaultdict
from datetime import date

from config import get_supabase_client
from services.product_service import get_product_service

SIESA_FILE = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA\20202 (1).xlsx"
SNAPSHOT_DATE = date(2026, 1, 31)


def normalize_sku(raw):
    """Normalize SKU for matching."""
    sku = raw.strip().upper()
    # Remove dimension suffix like "(T) 51X51-1"
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)
    sku = re.sub(r'\s+51X51-1$', '', sku)
    sku = re.sub(r'\s+51X51$', '', sku)
    # Remove accents
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')
    # Fix encoding issues
    sku = sku.replace("�", "").replace("Ã", "A")
    return sku.strip()


def get_product_mapping():
    """Get SKU -> product_id mapping with normalized variants."""
    ps = get_product_service()
    products, _ = ps.get_all(page=1, page_size=100, active_only=True)

    mapping = {}
    for p in products:
        sku = p.sku.upper()
        mapping[sku] = p.id

        # Normalized version
        sku_norm = unicodedata.normalize('NFD', sku)
        sku_norm = ''.join(c for c in sku_norm if unicodedata.category(c) != 'Mn')
        mapping[sku_norm] = p.id

        # Without BTE suffix
        if sku.endswith(" BTE"):
            base = sku[:-4]
            mapping[base] = p.id
            base_norm = unicodedata.normalize('NFD', base)
            base_norm = ''.join(c for c in base_norm if unicodedata.category(c) != 'Mn')
            mapping[base_norm] = p.id

    return mapping


def main():
    print("=" * 60)
    print("SIESA FACTORY INVENTORY IMPORT")
    print("=" * 60)

    client = get_supabase_client()
    products = get_product_mapping()
    print(f"Loaded {len(products)} SKU mappings")

    # Read SIESA file - row 0 is header
    df = pd.read_excel(SIESA_FILE, header=0)
    print(f"Excel rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Aggregate by SKU (sum Cant. disponible)
    siesa_totals = defaultdict(Decimal)
    unmatched = []

    for _, row in df.iterrows():
        # Column "Desc. item" contains SKU like "CARACOLÍ (T) 51X51-1"
        raw_sku = str(row['Desc. item']) if pd.notna(row['Desc. item']) else ""
        if not raw_sku or raw_sku == "nan":
            continue

        # "Cant. disponible" = available quantity in m2
        disponible = row['Cant. disponible'] if pd.notna(row['Cant. disponible']) else 0
        try:
            disponible = Decimal(str(float(disponible)))
        except:
            continue

        if disponible <= 0:
            continue

        sku = normalize_sku(raw_sku)
        pid = products.get(sku)

        if pid:
            siesa_totals[pid] += disponible
        else:
            if raw_sku not in [u[0] for u in unmatched]:
                unmatched.append((raw_sku, sku))

    print(f"\nAggregated {len(siesa_totals)} products with SIESA stock")

    if unmatched:
        print(f"\nUnmatched SKUs ({len(unmatched)}):")
        for raw, norm in unmatched[:10]:
            print(f"  - {raw[:40]} -> {norm}")

    # Update inventory snapshots
    print("\nUpdating factory_available_m2...")
    updated = 0
    errors = 0

    for pid, total_m2 in siesa_totals.items():
        # Get latest snapshot for this product
        result = client.table('inventory_snapshots').select('id').eq(
            'product_id', pid
        ).eq('snapshot_date', SNAPSHOT_DATE.isoformat()).execute()

        if result.data:
            # Update existing snapshot
            snapshot_id = result.data[0]['id']
            try:
                client.table('inventory_snapshots').update({
                    'factory_available_m2': float(total_m2)
                }).eq('id', snapshot_id).execute()
                updated += 1
            except Exception as e:
                print(f"  Error updating {pid}: {e}")
                errors += 1
        else:
            # No snapshot for today - check if any exists
            result = client.table('inventory_snapshots').select('id').eq(
                'product_id', pid
            ).order('snapshot_date', desc=True).limit(1).execute()

            if result.data:
                snapshot_id = result.data[0]['id']
                try:
                    client.table('inventory_snapshots').update({
                        'factory_available_m2': float(total_m2)
                    }).eq('id', snapshot_id).execute()
                    updated += 1
                except Exception as e:
                    print(f"  Error updating {pid}: {e}")
                    errors += 1

    print(f"\nUpdated: {updated}, Errors: {errors}")

    # Verify CARACOLI
    print("\n" + "=" * 60)
    print("VERIFICATION - CARACOLI")
    print("=" * 60)

    caracoli_id = products.get('CARACOLI')
    if caracoli_id:
        result = client.table('inventory_snapshots').select('*').eq(
            'product_id', caracoli_id
        ).order('snapshot_date', desc=True).limit(1).execute()

        if result.data:
            r = result.data[0]
            print(f"  warehouse_qty: {r.get('warehouse_qty', 0)}")
            print(f"  in_transit_qty: {r.get('in_transit_qty', 0)}")
            print(f"  factory_available_m2: {r.get('factory_available_m2', 0)}")


if __name__ == "__main__":
    main()
