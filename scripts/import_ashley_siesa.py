"""Import SIESA factory inventory from Ashley's order file.

Updates factory_available_m2 in inventory_snapshots and stores lot details in inventory_lots.
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

SIESA_FILE = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA\what ashley wants to order 0102.xlsx"
SNAPSHOT_DATE = date(2026, 2, 1)


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
    sku = sku.replace("�", "").replace("Ã", "A").replace("Í", "I")
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
    print("ASHLEY'S SIESA FACTORY INVENTORY IMPORT")
    print(f"Snapshot Date: {SNAPSHOT_DATE}")
    print("=" * 60)

    client = get_supabase_client()
    products = get_product_mapping()
    print(f"Loaded {len(products)} SKU mappings")

    # Read file
    df = pd.read_excel(SIESA_FILE, header=0)
    print(f"Excel rows: {len(df)}")

    # Aggregate by product and collect lot details
    product_totals = defaultdict(Decimal)
    product_lots = defaultdict(list)  # product_id -> list of lots
    unmatched = []
    total_m2 = Decimal("0")

    for _, row in df.iterrows():
        # Skip "Gran total" row
        raw_sku = str(row['Desc. item']) if pd.notna(row['Desc. item']) else ""
        if not raw_sku or raw_sku == "nan" or "total" in raw_sku.lower():
            continue

        # Get available quantity
        disponible = row['Cant. disponible'] if pd.notna(row['Cant. disponible']) else 0
        try:
            disponible = Decimal(str(float(disponible)))
        except:
            continue

        if disponible < 0:
            disponible = Decimal("0")

        # Get lot info
        lot_code = str(row['Lote']).strip() if pd.notna(row['Lote']) else ""
        siesa_item = int(row['Item']) if pd.notna(row['Item']) else None
        bodega = str(row['Bodega']).strip() if pd.notna(row['Bodega']) else ""

        sku = normalize_sku(raw_sku)
        pid = products.get(sku)

        if pid:
            product_totals[pid] += disponible
            total_m2 += disponible

            # Store lot detail (even if 0, for completeness)
            product_lots[pid].append({
                'lot_number': lot_code,
                'quantity_m2': float(disponible),
                'siesa_item': siesa_item,
                'siesa_description': raw_sku,
                'warehouse_code': bodega,
            })
        else:
            if raw_sku not in [u[0] for u in unmatched]:
                unmatched.append((raw_sku, sku))

    print(f"\nAggregated {len(product_totals)} products with SIESA data")
    print(f"Total available m²: {total_m2:,.1f}")

    if unmatched:
        print(f"\nUnmatched SKUs ({len(unmatched)}):")
        for raw, norm in unmatched[:5]:
            print(f"  - {raw[:40]} -> {norm}")

    # Clear existing lots for this snapshot date
    print("\nClearing old lot data...")
    try:
        client.table('inventory_lots').delete().eq(
            'snapshot_date', SNAPSHOT_DATE.isoformat()
        ).execute()
    except Exception as e:
        print(f"  Warning clearing lots: {e}")

    # Insert lot details
    print("Inserting lot details...")
    lots_created = 0
    for pid, lots in product_lots.items():
        for lot in lots:
            try:
                client.table('inventory_lots').insert({
                    'product_id': pid,
                    'lot_number': lot['lot_number'],
                    'quantity_m2': lot['quantity_m2'],
                    'siesa_item': lot['siesa_item'],
                    'siesa_description': lot['siesa_description'],
                    'warehouse_code': lot['warehouse_code'],
                    'snapshot_date': SNAPSHOT_DATE.isoformat(),
                }).execute()
                lots_created += 1
            except Exception as e:
                print(f"  Error inserting lot: {e}")

    print(f"Lots created: {lots_created}")

    # Update inventory snapshots
    print("\nUpdating factory_available_m2 in inventory_snapshots...")
    updated = 0
    created = 0

    for pid, total in product_totals.items():
        # Find largest lot for this product
        lots = product_lots[pid]
        largest_lot = max(lots, key=lambda x: x['quantity_m2']) if lots else None
        lot_count = len([l for l in lots if l['quantity_m2'] > 0])

        # Get or create snapshot
        result = client.table('inventory_snapshots').select('id').eq(
            'product_id', pid
        ).eq('snapshot_date', SNAPSHOT_DATE.isoformat()).execute()

        snapshot_data = {
            'factory_available_m2': float(total),
            'factory_lot_count': lot_count,
        }
        if largest_lot:
            snapshot_data['factory_largest_lot_m2'] = largest_lot['quantity_m2']
            snapshot_data['factory_largest_lot_code'] = largest_lot['lot_number']

        if result.data:
            # Update existing
            try:
                client.table('inventory_snapshots').update(snapshot_data).eq(
                    'id', result.data[0]['id']
                ).execute()
                updated += 1
            except Exception as e:
                print(f"  Error updating {pid}: {e}")
        else:
            # Check for any snapshot to update
            result = client.table('inventory_snapshots').select('id').eq(
                'product_id', pid
            ).order('snapshot_date', desc=True).limit(1).execute()

            if result.data:
                try:
                    client.table('inventory_snapshots').update(snapshot_data).eq(
                        'id', result.data[0]['id']
                    ).execute()
                    updated += 1
                except Exception as e:
                    print(f"  Error updating {pid}: {e}")

    print(f"Updated: {updated}")

    # Verification
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Check a few products
    for sku in ['CARACOLI', 'CEIBA BEIGE BTE', 'ALMENDRO BEIGE BTE']:
        pid = products.get(sku)
        if not pid:
            continue

        result = client.table('inventory_snapshots').select(
            'factory_available_m2', 'factory_lot_count', 'factory_largest_lot_m2'
        ).eq('product_id', pid).order('snapshot_date', desc=True).limit(1).execute()

        if result.data:
            r = result.data[0]
            print(f"\n{sku}:")
            print(f"  factory_available_m2: {r.get('factory_available_m2', 0):,.1f}")
            print(f"  factory_lot_count: {r.get('factory_lot_count', 0)}")
            print(f"  factory_largest_lot_m2: {r.get('factory_largest_lot_m2', 0):,.1f}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Products updated: {updated}")
    print(f"Lots created: {lots_created}")
    print(f"Total m² available: {total_m2:,.1f}")


if __name__ == "__main__":
    main()
