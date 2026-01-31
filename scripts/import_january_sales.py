"""Import January 2026 sales from Sales3101.xls"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import requests
import re
import unicodedata
from datetime import datetime, timedelta
from collections import defaultdict

API_BASE = "http://localhost:8000/api"
EXCEL_PATH = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA\Sales3101.xls"

SKIP_PATTERNS = ["20X61", "ZOCALO", "CENEFA", "LISTELO", "PIEDRA", "FONDO PISCINA", "ESCALON"]

def should_skip(sku):
    for p in SKIP_PATTERNS:
        if p in sku.upper():
            return True
    return False

def normalize_sku(raw):
    sku = raw.strip().upper()
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)
    sku = re.sub(r'\s+51X51-1$', '', sku)
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')
    sku = sku.replace("�", "").replace("Ã", "A")
    return sku.strip()

def get_week_start(d):
    return d - timedelta(days=d.weekday())

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
    print("JANUARY 2026 SALES IMPORT")
    print("=" * 50)

    products = get_products()
    print(f"Loaded {len(products)} SKU mappings")

    df = pd.read_excel(EXCEL_PATH, header=1)
    print(f"Excel rows: {len(df)}")

    aggregated = defaultdict(float)
    matched = 0
    unmatched = set()

    for _, row in df.iterrows():
        raw = str(row["REFERENCIA"]).strip()
        if should_skip(raw):
            continue
        sku = normalize_sku(raw)
        pid = products.get(sku)
        if not pid:
            unmatched.add(raw[:40])
            continue
        fecha = row["FECHA"]
        if hasattr(fecha, 'date'):
            fecha = fecha.date()
        week = get_week_start(fecha)
        aggregated[(week, pid)] += float(row["MT2"])
        matched += 1

    print(f"Matched: {matched}, Unmatched: {len(unmatched)}")
    if unmatched:
        print(f"Unmatched: {list(unmatched)[:5]}")
    print(f"Unique week/product: {len(aggregated)}")

    created = 0
    for (week, pid), qty in aggregated.items():
        resp = requests.post(f"{API_BASE}/sales", json={
            "product_id": pid,
            "week_start": week.isoformat(),
            "quantity_m2": qty
        })
        if resp.status_code == 201:
            created += 1

    print(f"Created: {created}")

    resp = requests.get(f"{API_BASE}/sales/count/total")
    print(f"Total sales records: {resp.json()['count']}")

if __name__ == "__main__":
    main()
