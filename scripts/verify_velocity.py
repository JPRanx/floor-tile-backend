"""Verify velocity data after import"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Suppress logging
import logging
logging.disable(logging.CRITICAL)
os.environ['LOG_LEVEL'] = 'ERROR'

from services.order_builder_service import get_order_builder_service

service = get_order_builder_service()
result = service.get_order_builder()

all_products = result.high_priority + result.consider + result.well_covered + result.your_call

print("")
print("=" * 60)
print("VELOCITY VERIFICATION - AFTER FRESH IMPORT")
print("=" * 60)

for p in all_products:
    if 'GALERA' in p.sku.upper():
        print(f"\nGALERA RUSTICO GRIS:")
        print(f"  Velocity: {p.velocity_90d_m2:.2f} m2/day")
        print(f"  Warehouse: {p.warehouse_qty:.1f} m2")
        print(f"  Coverage: {p.coverage_days:.0f} days")
        print(f"  Recommended pallets: {p.recommended_pallets}")
        print(f"  Priority: {p.priority}")
        galera_pallets = p.recommended_pallets

for p in all_products:
    if 'CARACOLI' in p.sku.upper():
        print(f"\nCARACOLI:")
        print(f"  Velocity: {p.velocity_90d_m2:.2f} m2/day")
        print(f"  Warehouse: {p.warehouse_qty:.1f} m2")
        print(f"  Recommended pallets: {p.recommended_pallets}")
        print(f"  Priority: {p.priority}")

for p in all_products:
    if p.sku.upper() == 'TOLU BEIGE':
        print(f"\nTOLU BEIGE:")
        print(f"  Velocity: {p.velocity_90d_m2:.2f} m2/day")
        print(f"  Warehouse: {p.warehouse_qty:.1f} m2")
        print(f"  Recommended pallets: {p.recommended_pallets}")
        print(f"  Priority: {p.priority}")

print("\n" + "=" * 60)
print("EXPECTED VALUES:")
print("  GALERA: ~2-9 m2/day, NOT 28 m2/day")
print("  GALERA pallets: ~2 (NOT 17!)")
print("=" * 60)

# Check if fix worked
if galera_pallets <= 5:
    print("\n✓ SUCCESS: GALERA recommendation looks correct!")
else:
    print(f"\n✗ WARNING: GALERA still shows {galera_pallets} pallets")
