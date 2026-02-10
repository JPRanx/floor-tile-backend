"""Import in-transit inventory from PROGRAMACIÓN DE DESPACHO DE TARRAGONA.xlsx.

Updates in_transit_qty field in inventory_snapshots.
Does NOT touch warehouse_qty or factory_available_m2.

IMPORTANT: Only includes orders that are actually in-transit or pending departure.
Orders that have already been received into warehouse should be excluded.
The dispatch file may contain historical orders - we filter them out here.
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

DISPATCH_FILE = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\Informes Tarragona 0210\PROGRAMACIÓN DE DESPACHO DE TARRAGONA.xlsx"
SNAPSHOT_DATE = date(2026, 1, 31)

# Orders to EXCLUDE (already received into warehouse inventory)
# Update this list as shipments are confirmed received
RECEIVED_ORDERS = [
    "OC002",  # FEX338 - ETA Jan 31, received into warehouse
    "OC003",  # FEX339 - ETA Jan 31, received into warehouse
]


def normalize_sku(raw):
    """Normalize SKU for matching."""
    sku = raw.strip().upper()
    # Remove dimension suffix like "51X51" or "51X51-1"
    sku = re.sub(r'\s+51X51(-\d+)?$', '', sku)
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)
    # Remove BTE suffix (will be added back in mapping lookup)
    sku = re.sub(r'\s+BTE$', '', sku)
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
    print("IN-TRANSIT INVENTORY IMPORT")
    print("=" * 60)

    client = get_supabase_client()
    products = get_product_mapping()
    print(f"Loaded {len(products)} SKU mappings")

    # Read dispatch file - first sheet only, header at row 2
    df = pd.read_excel(DISPATCH_FILE, sheet_name=0, header=2)
    print(f"Excel rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Fill forward order numbers so each row knows its order
    df['Factura'] = df['Factura'].ffill()

    # Filter out received orders
    print(f"\nExcluding received orders: {RECEIVED_ORDERS}")
    original_count = len(df)
    df = df[~df['Factura'].astype(str).str.contains('|'.join(RECEIVED_ORDERS), case=False, na=False)]
    filtered_count = len(df)
    print(f"Filtered: {original_count} -> {filtered_count} rows")

    # Aggregate by SKU (sum Cantidad de Mts)
    transit_totals = defaultdict(Decimal)
    unmatched = []

    for _, row in df.iterrows():
        # Column "Nombre de Referencias" contains SKU like "ALMENDRO BEIGE BTE 51X51"
        raw_sku = str(row['Nombre de Referencias ']) if pd.notna(row.get('Nombre de Referencias ')) else ""
        if not raw_sku or raw_sku == "nan":
            continue

        # Skip TOTAL rows
        if "TOTAL" in raw_sku.upper():
            continue

        # "Cantidad de Mts" = quantity in m2
        cantidad = row['Cantidad de Mts'] if pd.notna(row.get('Cantidad de Mts')) else 0
        try:
            cantidad = Decimal(str(float(cantidad)))
        except:
            continue

        if cantidad <= 0:
            continue

        sku = normalize_sku(raw_sku)
        pid = products.get(sku)

        if pid:
            transit_totals[pid] += cantidad
        else:
            if raw_sku not in [u[0] for u in unmatched]:
                unmatched.append((raw_sku, sku))

    print(f"\nAggregated {len(transit_totals)} products with in-transit stock")
    print(f"Total in-transit: {sum(transit_totals.values()):,.1f} m2")

    if unmatched:
        print(f"\nUnmatched SKUs ({len(unmatched)}):")
        for raw, norm in unmatched[:10]:
            print(f"  - {raw[:40]} -> {norm}")

    # First, reset all in_transit_qty to 0 for products NOT in transit
    print("\nResetting in_transit_qty for products not in current transit...")
    all_products_result = client.table('products').select('id').execute()
    all_product_ids = {p['id'] for p in all_products_result.data}
    products_in_transit = set(transit_totals.keys())
    products_to_reset = all_product_ids - products_in_transit

    reset_count = 0
    for pid in products_to_reset:
        result = client.table('inventory_snapshots').select('id').eq(
            'product_id', pid
        ).order('snapshot_date', desc=True).limit(1).execute()
        if result.data:
            client.table('inventory_snapshots').update({
                'in_transit_qty': 0
            }).eq('id', result.data[0]['id']).execute()
            reset_count += 1
    print(f"Reset {reset_count} products to 0 in-transit")

    # Update inventory snapshots - ALWAYS update the LATEST snapshot
    print("\nUpdating in_transit_qty (latest snapshot per product)...")
    updated = 0
    errors = 0

    for pid, total_m2 in transit_totals.items():
        # Get LATEST snapshot for this product (regardless of date)
        result = client.table('inventory_snapshots').select('id, snapshot_date').eq(
            'product_id', pid
        ).order('snapshot_date', desc=True).limit(1).execute()

        if result.data:
            snapshot_id = result.data[0]['id']
            snapshot_date = result.data[0]['snapshot_date']
            try:
                client.table('inventory_snapshots').update({
                    'in_transit_qty': float(total_m2)
                }).eq('id', snapshot_id).execute()
                updated += 1
                print(f"  Updated {pid[:8]}... ({snapshot_date}): {total_m2} m2")
            except Exception as e:
                print(f"  Error updating {pid}: {e}")
                errors += 1
        else:
            print(f"  No snapshot found for {pid}")

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
