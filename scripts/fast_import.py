"""Fast bulk import of sales data using direct service calls."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import re
import unicodedata
from datetime import datetime, timedelta, date
from decimal import Decimal
from collections import defaultdict

from services.sales_service import get_sales_service
from services.product_service import get_product_service
from models.sales import SalesRecordCreate

HISTORICAL_FILE = r"C:\Users\Jorge Alexander\floor-tile-saas\data\phase2-samples\VENTAS ANUAL.xlsx"
JANUARY_FILE = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA\Sales3101.xls"

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

def main():
    print("=" * 60)
    print("FAST BULK IMPORT")
    print("=" * 60)

    # Get services
    sales_service = get_sales_service()
    product_service = get_product_service()

    # Get products
    products, _ = product_service.get_all(page=1, page_size=100, active_only=True)
    mapping = {}
    for p in products:
        sku = p.sku.upper()
        mapping[sku] = p.id
        sku_norm = unicodedata.normalize('NFD', sku)
        sku_norm = ''.join(c for c in sku_norm if unicodedata.category(c) != 'Mn')
        mapping[sku_norm] = p.id
        if sku.endswith(" BTE"):
            base = sku[:-4]
            mapping[base] = p.id

    print(f"Loaded {len(mapping)} SKU mappings")

    # Clear existing sales
    print("\nClearing existing sales...")
    for year in range(2020, 2030):
        count = sales_service.delete_by_date_range(date(year, 1, 1), date(year, 12, 31))
        if count > 0:
            print(f"  Deleted {count} from {year}")

    # Aggregate all sales
    aggregated = defaultdict(Decimal)

    # Historical
    print(f"\nReading historical: {HISTORICAL_FILE}")
    df = pd.read_excel(HISTORICAL_FILE)
    matched = 0
    for _, row in df.iterrows():
        raw = str(row["SKU"]).strip()
        if should_skip(raw):
            continue
        sku = normalize_sku(raw)
        pid = mapping.get(sku)
        if not pid:
            continue
        fecha = row["FECHA"]
        if hasattr(fecha, 'date'):
            fecha = fecha.date()
        week = get_week_start(fecha)
        aggregated[(week, pid)] += Decimal(str(row["MT2"]))
        matched += 1
    print(f"  Historical matched: {matched}")

    # January
    print(f"\nReading January: {JANUARY_FILE}")
    df = pd.read_excel(JANUARY_FILE, header=1)
    matched = 0
    for _, row in df.iterrows():
        raw = str(row["REFERENCIA"]).strip()
        if should_skip(raw):
            continue
        sku = normalize_sku(raw)
        pid = mapping.get(sku)
        if not pid:
            continue
        fecha = row["FECHA"]
        if hasattr(fecha, 'date'):
            fecha = fecha.date()
        week = get_week_start(fecha)
        aggregated[(week, pid)] += Decimal(str(row["MT2"]))
        matched += 1
    print(f"  January matched: {matched}")

    print(f"\nTotal unique week/product: {len(aggregated)}")

    # Create bulk records
    print("\nBulk inserting...")
    records = []
    for (week, pid), qty in aggregated.items():
        records.append(SalesRecordCreate(
            product_id=pid,
            week_start=week,
            quantity_m2=qty
        ))

    # Bulk create in chunks
    chunk_size = 100
    created = 0
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        try:
            result = sales_service.bulk_create(chunk)
            created += len(result)
            print(f"  Chunk {i//chunk_size + 1}: {len(result)} created")
        except Exception as e:
            print(f"  Chunk {i//chunk_size + 1} error: {e}")

    print(f"\nTotal created: {created}")

    # Verify GALERA
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    galera_id = None
    for sku, pid in mapping.items():
        if "GALERA" in sku:
            galera_id = pid
            break

    if galera_id:
        history = sales_service.get_product_history(galera_id, limit=100)
        print(f"GALERA records: {history.weeks_count}")
        print(f"GALERA total m²: {history.total_m2}")

if __name__ == "__main__":
    main()
