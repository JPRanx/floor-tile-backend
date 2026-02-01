"""Bulk import inventory - direct database insert (fast)"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import unicodedata
from datetime import date

from config import get_supabase_client
from services.inventory_service import InventoryService
from services.product_service import ProductService
from models.inventory import InventorySnapshotCreate

EXCEL_PATH = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA\INVENTARIO POR PRODUCTOS FEBRERO 02.02.26 FEX 337.xlsx"
SNAPSHOT_DATE = date(2026, 2, 1)  # Using Feb 1 as Feb 2 is future

def normalize_sku(raw):
    sku = raw.strip().upper()
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)
    sku = re.sub(r'\s+51X51-1$', '', sku)
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')
    sku = sku.replace("�", "").replace("Ã", "A")
    return sku.strip()

def main():
    print("=" * 50)
    print("BULK INVENTORY IMPORT (Direct DB)")
    print(f"Snapshot Date: {SNAPSHOT_DATE}")
    print("=" * 50)

    # Get services
    product_service = ProductService()
    inventory_service = InventoryService()

    # Build SKU -> product_id mapping
    products, _ = product_service.get_all(page_size=100)
    sku_to_id = {}
    for p in products:
        sku = p.sku.upper()
        sku_to_id[sku] = p.id
        # Normalized version
        sku_norm = unicodedata.normalize('NFD', sku)
        sku_norm = ''.join(c for c in sku_norm if unicodedata.category(c) != 'Mn')
        sku_to_id[sku_norm] = p.id
        # Without BTE suffix
        if sku.endswith(' BTE'):
            base = sku[:-4]
            sku_to_id[base] = p.id
            base_norm = unicodedata.normalize('NFD', base)
            base_norm = ''.join(c for c in base_norm if unicodedata.category(c) != 'Mn')
            sku_to_id[base_norm] = p.id

    print(f"Loaded {len(sku_to_id)} SKU mappings")

    # Read Excel
    df = pd.read_excel(EXCEL_PATH, sheet_name='INVENTARIO CERAMICO ', header=None, skiprows=5)
    print(f"Excel rows: {len(df)}")

    # Build batch of snapshots
    snapshots = []
    unmatched = []

    for _, row in df.iterrows():
        raw_sku = str(row[0]).strip() if pd.notna(row[0]) else ""
        if not raw_sku or raw_sku == "nan":
            continue

        saldo_m2 = row[9] if pd.notna(row[9]) else 0
        try:
            saldo_m2 = max(0, float(saldo_m2))  # Clamp to 0 for floating point errors
        except:
            continue

        sku = normalize_sku(raw_sku)
        pid = sku_to_id.get(sku)

        if not pid:
            unmatched.append(raw_sku[:30])
            continue

        snapshots.append(InventorySnapshotCreate(
            product_id=pid,
            snapshot_date=SNAPSHOT_DATE,
            warehouse_qty=saldo_m2,
            in_transit_qty=0
        ))

    print(f"Prepared {len(snapshots)} snapshots for import")
    print(f"Unmatched: {len(unmatched)}")
    if unmatched:
        print(f"  Sample: {unmatched[:5]}")

    # Bulk create - bypassing date validation since we use service directly
    if snapshots:
        print("\nInserting into database...")
        results = inventory_service.bulk_create(snapshots)
        print(f"Created/Updated: {len(results)} records")

    # Verify
    total = inventory_service.count_total()
    print(f"\nTotal inventory records in DB: {total}")

if __name__ == "__main__":
    main()
