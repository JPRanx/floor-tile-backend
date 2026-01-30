"""
Quick script to import production schedule and inventory files.
"""
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.production_schedule_service import get_production_schedule_service
from services.inventory_service import get_inventory_service
from parsers.excel_parser import parse_owner_excel


def import_production_schedule(file_path: str):
    """Import production schedule Excel."""
    print(f"\n{'='*60}")
    print("PRODUCTION SCHEDULE IMPORT")
    print(f"{'='*60}")
    print(f"File: {file_path}")

    service = get_production_schedule_service()

    # Parse the Excel
    print("\nParsing Excel file...")
    records = service.parse_production_excel(file_path, source_month="ENERO-26")
    print(f"Parsed {len(records)} records")

    # Show status breakdown
    status_counts = {}
    for r in records:
        status = r.status.value if hasattr(r.status, 'value') else str(r.status)
        status_counts[status] = status_counts.get(status, 0) + 1

    print("\nBy status:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")

    # Import to database
    print("\nImporting to database...")
    result = service.import_from_excel(records, match_products=True)

    print(f"\nImport Results:")
    print(f"  Total rows parsed: {result.total_rows_parsed}")
    print(f"  Inserted: {result.inserted}")
    print(f"  Updated: {result.updated}")
    print(f"  Skipped: {result.skipped}")
    print(f"  Matched products: {result.matched}")
    print(f"  Unmatched: {len(result.unmatched_referencias)}")

    if result.warnings:
        print(f"\nWarnings ({len(result.warnings)}):")
        for w in result.warnings[:10]:
            print(f"  - {w}")

    return result


def import_inventory(file_path: str):
    """Import warehouse inventory Excel."""
    print(f"\n{'='*60}")
    print("WAREHOUSE INVENTORY IMPORT")
    print(f"{'='*60}")
    print(f"File: {file_path}")

    # Parse the Excel using the owner template parser
    print("\nParsing Excel file...")

    with open(file_path, 'rb') as f:
        file_bytes = f.read()

    parsed = parse_owner_excel(file_bytes)

    print(f"Parsed {len(parsed.inventory_records)} inventory records")

    # Calculate totals
    total_m2 = sum(r.warehouse_m2 for r in parsed.inventory_records)
    total_transit = sum(r.in_transit_m2 for r in parsed.inventory_records)

    print(f"\nTotals:")
    print(f"  Warehouse m²: {total_m2:,.2f}")
    print(f"  In Transit m²: {total_transit:,.2f}")

    # Import to database
    print("\nImporting to database...")
    service = get_inventory_service()

    # Import each record
    imported = 0
    errors = []
    for record in parsed.inventory_records:
        try:
            service.create(record)
            imported += 1
        except Exception as e:
            errors.append(f"{record.sku}: {e}")

    print(f"\nImport Results:")
    print(f"  Imported: {imported}")
    print(f"  Errors: {len(errors)}")

    if errors:
        print(f"\nFirst 5 errors:")
        for e in errors[:5]:
            print(f"  - {e}")

    return imported, total_m2


if __name__ == "__main__":
    base_path = r"C:\Users\Jorge Alexander\floor-tile-saas\data\uploads\INFORMES TARRAGONA"

    # Production Schedule
    prod_file = os.path.join(base_path, "Programa de Produccion_ CASTELLON_ENERO_24-01-2026_V3 (5).xlsx")
    if os.path.exists(prod_file):
        import_production_schedule(prod_file)
    else:
        print(f"Production file not found: {prod_file}")

    # Warehouse Inventory
    inv_file = os.path.join(base_path, "20202 (1).xlsx")
    if os.path.exists(inv_file):
        import_inventory(inv_file)
    else:
        print(f"Inventory file not found: {inv_file}")

    print(f"\n{'='*60}")
    print("IMPORT COMPLETE")
    print(f"{'='*60}")
