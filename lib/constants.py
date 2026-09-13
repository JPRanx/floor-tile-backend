from decimal import Decimal

M2_PER_PALLET = Decimal("134.4")
PALLETS_PER_CONTAINER = 13
PRODUCTION_LEAD_DAYS = 25
TRUCK_TO_PORT_DAYS = 5
TOTAL_LEAD_DAYS = 30
MIN_ORDER_PALLETS = 13
MIN_BLS_PER_BOAT = 3
IDEAL_BLS_PER_BOAT = 5
SAFETY_STOCK_PALLETS = 3
SAFETY_STOCK_M2 = Decimal("403.2")  # 3 × 134.4 (legacy fallback for zero-velocity products)

# ── Tier-based buffer config ─────────────────────────────────────────
# Products are classified by 90-day velocity into A/B/C tiers.
# Each tier has a target safety stock in weeks-of-coverage, with
# floor and ceiling pallets to bound the math against extreme cases.
#
# Top 25% velocity = A (highest stock-out risk if depleted)
# Mid 50% velocity = B
# Bottom 25%       = C
TIER_BUFFER_CONFIG = {
    "A": {"weeks": 4, "floor_pallets": 5, "ceiling_pallets": 999},  # effectively no cap
    "B": {"weeks": 3, "floor_pallets": 3, "ceiling_pallets": 15},
    "C": {"weeks": 2, "floor_pallets": 1, "ceiling_pallets": 8},
}
VELOCITY_PERIOD_DAYS = 90
MIN_BOAT_PALLETS = 39  # 3 containers × 13 pallets
LEAD_TIME_DAYS = 20    # Factory needs 20 days to prepare — brain won't suggest for closer boats
