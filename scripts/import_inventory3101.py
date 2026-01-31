"""Import inventory from Inventory3101.xlsx"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import requests
import re
import unicodedata
from datetime import date

API_BASE = "http://localhost:8000/api"
EXCEL_PATH = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA\Inventory3101.xlsx"
SNAPSHOT_DATE = date(2026, 1, 31)

def normalize_sku(raw):
    sku = raw.strip().upper()
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)
    sku = re.sub(r'\s+51X51-1$', '', sku)
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')
    sku = sku.replace("�", "").replace("Ã", "A")
    return sku.strip()

def get_products():
    resp = requests.get(f"{API_BASE}/products", params={"page_size": 100})
    mapping = {}
    for p in resp.json()["data"]:
        sku = p["sku"].upper()
        pid = p["id"]
        mapping[sku] = pid
        sku_norm = unicodedata.normalize('NFD', sku)
        sku_norm = ''.join(c for c in sku_norm if unicodedata.category(c) != 'Mn')
        mapping[sku_norm] = pid
        if sku.endswith(" BTE"):
            base = sku[:-4]
            mapping[base] = pid
            base_norm = unicodedata.normalize('NFD', base)
            base_norm = ''.join(c for c in base_norm if unicodedata.category(c) != 'Mn')
            mapping[base_norm] = pid
    return mapping

def main():
    print("=" * 50)
    print("INVENTORY 3101 IMPORT")
    print("=" * 50)

    products = get_products()
    print(f"Loaded {len(products)} SKU mappings")

    # Read inventory with no header, data starts at row 5
    df = pd.read_excel(EXCEL_PATH, sheet_name='INVENTARIO CERAMICO ', header=None, skiprows=5)

    print(f"Excel rows: {len(df)}")

    # Columns: 0=SKU, 8=SALDO PALET, 9=SALDO M2
    created = 0
    updated = 0
    unmatched = []

    for _, row in df.iterrows():
        raw_sku = str(row[0]).strip() if pd.notna(row[0]) else ""
        if not raw_sku or raw_sku == "nan":
            continue

        saldo_m2 = row[9] if pd.notna(row[9]) else 0
        saldo_pallets = row[8] if pd.notna(row[8]) else 0

        try:
            saldo_m2 = float(saldo_m2)
            saldo_pallets = int(float(saldo_pallets))
        except:
            continue

        sku = normalize_sku(raw_sku)
        pid = products.get(sku)

        if not pid:
            unmatched.append(raw_sku[:30])
            continue

        # Create inventory snapshot
        resp = requests.post(f"{API_BASE}/inventory", json={
            "product_id": pid,
            "snapshot_date": SNAPSHOT_DATE.isoformat(),
            "warehouse_qty": saldo_m2,
            "in_transit_qty": 0
        })

        if resp.status_code == 201:
            created += 1
        elif resp.status_code == 200:
            updated += 1

    print(f"Created: {created}, Updated: {updated}")
    print(f"Unmatched: {len(unmatched)}")
    if unmatched:
        print(f"Sample: {unmatched[:5]}")

    # Verify
    resp = requests.get(f"{API_BASE}/inventory/count/total")
    print(f"Total inventory records: {resp.json()['count']}")

if __name__ == "__main__":
    main()
