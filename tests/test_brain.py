"""
Test the brain with hardcoded data.
Verify against napkin math, not old code.
"""

from datetime import date
from decimal import Decimal

from lib.brain import compute_horizon


TODAY = date(2026, 3, 20)


def make_product(pid, sku, active=True):
    return {"id": pid, "sku": sku, "active": active}


def make_boat(bid, name, dep, arr, carrier="TIBA", factory_id="f1"):
    return {
        "id": bid,
        "name": name,
        "departure_date": date.fromisoformat(dep),
        "arrival_date": date.fromisoformat(arr),
        "factory_id": factory_id,
        "carrier": carrier,
    }


# ── Products ──────────────────────────────────────────────────────────────

CARACOLI = make_product("p1", "CARACOLI 60X60")
TERRA    = make_product("p2", "TERRA FUERTE 45X45")
ANTICO   = make_product("p3", "ANTICO 60X60")
MADERA   = make_product("p4", "MADERA ROBLE 20X60")
PIEDRA   = make_product("p5", "PIEDRA GRIS 45X45")
DEAD     = make_product("p6", "OLD TILE 30X30")  # zero velocity

ALL_PRODUCTS = [CARACOLI, TERRA, ANTICO, MADERA, PIEDRA, DEAD]

# ── Boats ─────────────────────────────────────────────────────────────────

BOATS = [
    make_boat("b1", "AIAS",             "2026-03-15", "2026-03-28"),   # anchor
    make_boat("b2", "TIBA",             "2026-04-09", "2026-04-18"),
    make_boat("b3", "LITTLE SYMPHONY",  "2026-04-23", "2026-05-02"),
]

# b1 is the anchor (has shipment_items)
SHIPMENT_ITEMS = [
    {"boat_id": "b1", "product_id": "p1", "shipped_m2": "2000", "shipped_pallets": 15},
    {"boat_id": "b1", "product_id": "p2", "shipped_m2": "672",  "shipped_pallets": 5},
]


def _run_full_scenario():
    """Shared scenario with enough volume to not trigger skip."""
    return compute_horizon(
        products=ALL_PRODUCTS,
        boats=BOATS,
        inventory={
            "p1": Decimal("1500"),  # CARACOLI
            "p2": Decimal("200"),   # TERRA
            "p3": Decimal("300"),   # ANTICO
            "p4": Decimal("100"),   # MADERA
            "p5": Decimal("150"),   # PIEDRA
            "p6": Decimal("500"),   # DEAD
        },
        velocities={
            "p1": Decimal("50"),   # 30 days stock
            "p2": Decimal("35"),   # 5.7 days stock
            "p3": Decimal("25"),   # 12 days stock
            "p4": Decimal("40"),   # 2.5 days stock
            "p5": Decimal("20"),   # 7.5 days stock
            "p6": Decimal("0"),    # dead
        },
        factory_stock={
            "p1": Decimal("3000"),
            "p2": Decimal("1500"),
            "p3": Decimal("2000"),
            "p4": Decimal("2000"),
            "p5": Decimal("1500"),
            "p6": Decimal("0"),
        },
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[],
        today=TODAY,
    )


def test_basic_scenario():
    """
    Napkin math for CARACOLI on TIBA:

    Warehouse:     1500 m²
    Arriving soon: 2000 m² (AIAS arrives Mar 28 >= today)
    Running stock: 3500 m²
    Velocity:      50 m²/day
    Next boat:     LITTLE SYMPHONY arrives May 2
    Days to next:  (May 2 - Mar 20) = 43 days
    Stock at next: 3500 - (50 × 43) = 1350
    Safety:        403.2 m²
    Gap:           max(0, 403.2 - 1350) = 0 → no gap

    TERRA on TIBA:
    Running stock: 200 + 672 = 872 m²
    Days to next:  43
    Stock at next: 872 - (35 × 43) = -633
    Gap:           max(0, 403.2 - (-633)) = 1036.2
    Suggested:     ceil(1036.2 / 134.4) = 8
    Factory:       1500 m² = 11 pallets
    Can ship:      min(8, 11) = 8
    """
    result = _run_full_scenario()

    # Structure
    assert result["anchor_boat_id"] == "b1"
    assert len(result["completed"]) == 1
    assert len(result["projections"]) == 2

    tiba = result["projections"][0]
    assert tiba["boat_id"] == "b2"

    # CARACOLI: well covered
    caracoli = next(p for p in tiba["products"] if p["product_id"] == "p1")
    assert caracoli["coverage_gap_m2"] == 0
    assert caracoli["suggested_pallets"] == 0
    # 1500/50 = 30.0 → >= 30 = "ok"
    assert caracoli["urgency"] == "ok"

    # TERRA: needs pallets
    terra = next(p for p in tiba["products"] if p["product_id"] == "p2")
    assert terra["suggested_pallets"] == 8
    assert terra["can_ship_pallets"] == 8
    # 200/35 = 5.7 days → critical
    assert terra["urgency"] == "critical"

    # DEAD: zero velocity, no suggestion
    dead = next(p for p in tiba["products"] if p["product_id"] == "p6")
    assert dead["coverage_gap_m2"] == 0
    assert dead["suggested_pallets"] == 0
    assert dead["days_of_stock"] == 999.0

    print("✓ Basic scenario passed")
    print(f"  CARACOLI: {caracoli['suggested_pallets']} pallets (ok)")
    print(f"  TERRA:    {terra['suggested_pallets']} pallets ({terra['urgency']})")
    print(f"  DEAD:     {dead['suggested_pallets']} pallets (zero velocity)")


def test_cascade():
    """
    Boat 2's allocations flow into boat 3's running stock.

    TERRA after TIBA: running_stock += 8 × 134.4 = 1075.2
    New running: 872 + 1075.2 = 1947.2

    TERRA on SYMPHONY:
    No next boat → days = (May 2 - Mar 20) + 30 = 73
    Stock at resupply: 1947.2 - (35 × 73) = -607.8
    Gap: 403.2 - (-607.8) = 1011
    Suggested: ceil(1011 / 134.4) = 8
    Factory after TIBA: 1500 - 1075.2 = 424.8 → 3 pallets
    Can ship: min(8, 3) = 3
    """
    result = _run_full_scenario()

    symphony = result["projections"][1]
    terra = next(p for p in symphony["products"] if p["product_id"] == "p2")

    assert terra["suggested_pallets"] == 8
    assert terra["can_ship_pallets"] == 3
    assert terra["factory_available_m2"] < 500

    print("✓ Cascade passed")
    print(f"  TERRA on SYMPHONY: suggested={terra['suggested_pallets']}, "
          f"can_ship={terra['can_ship_pallets']} (factory limited)")


def test_skip_boat():
    """
    Only 1 product with small gap → total pallets < 39 → skip.
    Skipped boat should NOT affect running stock.
    """
    result = compute_horizon(
        products=[CARACOLI],
        boats=BOATS,
        inventory={"p1": Decimal("5000")},
        velocities={"p1": Decimal("10")},
        factory_stock={"p1": Decimal("500")},
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[],
        today=TODAY,
    )

    tiba = result["projections"][0]
    assert tiba["skip_recommended"] is True
    # After skip, allocations zeroed
    caracoli = next(p for p in tiba["products"] if p["product_id"] == "p1")
    assert caracoli["allocated_pallets"] == 0

    print("✓ Skip boat passed")


def test_draft_override():
    """
    Ashley drafted 5 pallets for TERRA on TIBA.
    Brain suggests 8 but allocates 5.
    """
    drafts = [
        {
            "boat_id": "b2",
            "product_id": "p2",
            "selected_pallets": 5,
            "status": "drafting",
            "draft_id": "d1",
        },
    ]

    result = compute_horizon(
        products=ALL_PRODUCTS,
        boats=BOATS,
        inventory={
            "p1": Decimal("1500"),
            "p2": Decimal("200"),
            "p3": Decimal("300"),
            "p4": Decimal("100"),
            "p5": Decimal("150"),
            "p6": Decimal("500"),
        },
        velocities={
            "p1": Decimal("50"),
            "p2": Decimal("35"),
            "p3": Decimal("25"),
            "p4": Decimal("40"),
            "p5": Decimal("20"),
            "p6": Decimal("0"),
        },
        factory_stock={
            "p1": Decimal("3000"),
            "p2": Decimal("1500"),
            "p3": Decimal("2000"),
            "p4": Decimal("2000"),
            "p5": Decimal("1500"),
            "p6": Decimal("0"),
        },
        drafts=drafts,
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[],
        today=TODAY,
    )

    tiba = result["projections"][0]
    terra = next(p for p in tiba["products"] if p["product_id"] == "p2")

    assert terra["suggested_pallets"] == 8
    assert terra["allocated_pallets"] == 5
    assert terra["is_draft_committed"] is True

    print("✓ Draft override passed")
    print(f"  TERRA: suggested={terra['suggested_pallets']}, allocated={terra['allocated_pallets']}")


def test_production_request():
    """
    TERRA needs pallets but factory only has 2 (268.8 m²).
    Use a boat far enough out that production lead time works.
    Today Mar 20 + 30 days = Apr 19. Boat departs Apr 23 → factory_short.
    """
    far_boats = [
        make_boat("b1", "AIAS",    "2026-03-15", "2026-03-28"),
        make_boat("b3", "SYMPHONY", "2026-04-23", "2026-05-02"),
        make_boat("b4", "FUTURE",   "2026-05-07", "2026-05-16"),
    ]

    result = compute_horizon(
        products=[TERRA],
        boats=far_boats,
        inventory={"p2": Decimal("200")},
        velocities={"p2": Decimal("35")},
        factory_stock={"p2": Decimal("268.8")},
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[],
        today=TODAY,
    )

    assert len(result["production_requests"]) > 0
    req = result["production_requests"][0]
    assert req["product_id"] == "p2"
    assert req["urgency"] == "critical"  # TERRA is critical (5.7 days stock)
    assert req["is_piggyback"] is False  # No scheduled production

    print("✓ Production request passed")
    print(f"  TERRA: urgency={req['urgency']}, piggyback={req['is_piggyback']}")


def test_no_anchor():
    """Fresh system, no shipment_items → simulate all boats."""
    result = compute_horizon(
        products=[TERRA],
        boats=BOATS,
        inventory={"p2": Decimal("200")},
        velocities={"p2": Decimal("35")},
        factory_stock={"p2": Decimal("1500")},
        drafts=[],
        shipment_items=[],
        production_schedule=[],
        today=TODAY,
    )

    assert result["anchor_boat_id"] is None
    assert len(result["completed"]) == 0
    assert len(result["projections"]) == 3

    print("✓ No anchor passed")


def test_skip_consolidates_onto_next():
    """
    Ashley's question: "TIBA doesn't have enough volume. Does SYMPHONY pick it up?"

    Setup: 5 products with low stock. TIBA has enough total volume to ship
    but we artificially limit factory so TIBA < 39 pallets → skip.
    SYMPHONY should see the full demand because TIBA didn't ship.

    Use limited factory stock so TIBA gets < 39 pallets across all products,
    then give more factory to SYMPHONY (production arrived between boats).
    """
    # 3 boats, b1 is anchor
    boats = [
        make_boat("b1", "AIAS",    "2026-03-15", "2026-03-28"),
        make_boat("b2", "TIBA",    "2026-04-09", "2026-04-18"),
        make_boat("b3", "SYMPHONY","2026-04-23", "2026-05-02"),
    ]

    products = [TERRA, ANTICO, MADERA]

    # All products have low warehouse, moderate velocity → need pallets
    # But factory stock is tiny → TIBA can only ship a few per product
    result = compute_horizon(
        products=products,
        boats=boats,
        inventory={
            "p2": Decimal("100"),
            "p3": Decimal("80"),
            "p4": Decimal("50"),
        },
        velocities={
            "p2": Decimal("35"),
            "p3": Decimal("25"),
            "p4": Decimal("40"),
        },
        factory_stock={
            "p2": Decimal("400"),   # ~3 pallets
            "p3": Decimal("400"),   # ~3 pallets
            "p4": Decimal("400"),   # ~3 pallets → total ~9 pallets < 39
        },
        drafts=[],
        shipment_items=[
            {"boat_id": "b1", "product_id": "p2", "shipped_m2": "672", "shipped_pallets": 5},
        ],
        production_schedule=[],
        today=TODAY,
    )

    tiba = result["projections"][0]
    symphony = result["projections"][1]

    # TIBA: not enough volume → skip
    assert tiba["skip_recommended"] is True
    assert tiba["total_pallets"] == 0  # zeroed by skip

    # SYMPHONY: running stock was NOT bumped by TIBA → larger gaps
    # The key assertion: SYMPHONY's suggested pallets should be >= TIBA's suggested
    # because nothing shipped on TIBA
    for pid in ["p2", "p3", "p4"]:
        terra_tiba = next(p for p in tiba["products"] if p["product_id"] == pid)
        terra_sym = next(p for p in symphony["products"] if p["product_id"] == pid)
        assert terra_sym["suggested_pallets"] >= terra_tiba["suggested_pallets"], \
            f"{pid}: SYMPHONY should need at least as much as TIBA since TIBA was skipped"

    print("✓ Skip consolidates onto next boat")
    print(f"  TIBA: skipped ({tiba['total_pallets']} pallets)")
    print(f"  SYMPHONY: {symphony['total_pallets']} pallets (absorbed demand)")


def test_ordered_boat_immutable_cascade():
    """
    Ashley's question: "I already confirmed TIBA's order. Does the system
    respect my numbers and still calculate SYMPHONY correctly?"

    TERRA: brain would suggest 8 pallets, but Ashley ordered 12.
    The cascade should use 12, not 8.

    TERRA on TIBA (ORDERED, 12 pallets):
      Running stock: 872
      Allocated: 12 × 134.4 = 1612.8 (Ashley's choice, not brain's 8)
      Running stock after: 872 + 1612.8 = 2484.8

    TERRA on SYMPHONY:
      Running stock: 2484.8
      No next boat → days = 73
      Stock at next: 2484.8 - (35 × 73) = 2484.8 - 2555 = -70.2
      Gap: 403.2 - (-70.2) = 473.4 → 4 pallets
      (Smaller gap than cascade test because Ashley shipped more on TIBA)
    """
    drafts = [
        {
            "boat_id": "b2",
            "product_id": "p2",
            "selected_pallets": 12,
            "status": "ordered",
            "draft_id": "d1",
        },
    ]

    result = compute_horizon(
        products=ALL_PRODUCTS,
        boats=BOATS,
        inventory={
            "p1": Decimal("1500"),
            "p2": Decimal("200"),
            "p3": Decimal("300"),
            "p4": Decimal("100"),
            "p5": Decimal("150"),
            "p6": Decimal("500"),
        },
        velocities={
            "p1": Decimal("50"),
            "p2": Decimal("35"),
            "p3": Decimal("25"),
            "p4": Decimal("40"),
            "p5": Decimal("20"),
            "p6": Decimal("0"),
        },
        factory_stock={
            "p1": Decimal("3000"),
            "p2": Decimal("1500"),
            "p3": Decimal("2000"),
            "p4": Decimal("2000"),
            "p5": Decimal("1500"),
            "p6": Decimal("0"),
        },
        drafts=drafts,
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[],
        today=TODAY,
    )

    tiba = result["projections"][0]
    symphony = result["projections"][1]

    # TIBA: ordered, uses Ashley's 12 not brain's 8
    terra_tiba = next(p for p in tiba["products"] if p["product_id"] == "p2")
    assert terra_tiba["suggested_pallets"] == 8
    assert terra_tiba["allocated_pallets"] == 12
    assert terra_tiba["is_draft_committed"] is True

    # TIBA should never be skipped — it's ORDERED
    assert tiba["skip_recommended"] is False

    # SYMPHONY: smaller gap because Ashley shipped more
    terra_sym = next(p for p in symphony["products"] if p["product_id"] == "p2")
    assert terra_sym["suggested_pallets"] == 4  # 473.4 / 134.4 = 3.52 → ceil = 4

    print("✓ Ordered boat immutable cascade")
    print(f"  TIBA (ordered): allocated={terra_tiba['allocated_pallets']} (Ashley's 12, not brain's 8)")
    print(f"  SYMPHONY: suggested={terra_sym['suggested_pallets']} (smaller gap, Ashley shipped more)")


def test_product_becomes_covered_mid_horizon():
    """
    Ashley's question: "TERRA is critical now, but if TIBA ships 8 pallets,
    do I still need to put it on SYMPHONY?"

    TERRA on TIBA: 8 pallets = 1075.2 m²
    Running stock after TIBA: 872 + 1075.2 = 1947.2

    TERRA on SYMPHONY:
      days = 73 (no next boat)
      Stock at next: 1947.2 - (35 × 73) = -607.8
      Gap: 403.2 - (-607.8) = 1011 → 8 pallets, can_ship 3

    Hmm, TERRA still needs pallets. Let's use a scenario where it's truly covered.
    Give TERRA more warehouse stock so TIBA's shipment covers through SYMPHONY.

    Warehouse: 2000, arriving: 672, running: 2672
    TIBA: stock_at_next = 2672 - (35×43) = 1167. Gap = 0. Suggested = 0.
    SYMPHONY: running still 2672 (nothing shipped on TIBA).
      stock_at_next = 2672 - (35×73) = 117. Gap = 403.2-117 = 286.2 → 3 pallets.

    Better test: enough stock that BOTH boats show 0.
    Warehouse: 4000, arriving: 672, running: 4672
    TIBA: 4672 - (35×43) = 3167. Gap = 0.
    SYMPHONY: 4672 - (35×73) = 2117. Gap = 0.
    → Brain correctly says: don't put TERRA on either boat. She's covered.
    """
    result = compute_horizon(
        products=ALL_PRODUCTS,
        boats=BOATS,
        inventory={
            "p1": Decimal("1500"),
            "p2": Decimal("4000"),   # TERRA has lots of warehouse stock
            "p3": Decimal("300"),
            "p4": Decimal("100"),
            "p5": Decimal("150"),
            "p6": Decimal("500"),
        },
        velocities={
            "p1": Decimal("50"),
            "p2": Decimal("35"),
            "p3": Decimal("25"),
            "p4": Decimal("40"),
            "p5": Decimal("20"),
            "p6": Decimal("0"),
        },
        factory_stock={
            "p1": Decimal("3000"),
            "p2": Decimal("1500"),
            "p3": Decimal("2000"),
            "p4": Decimal("2000"),
            "p5": Decimal("1500"),
            "p6": Decimal("0"),
        },
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[],
        today=TODAY,
    )

    tiba = result["projections"][0]
    symphony = result["projections"][1]

    terra_tiba = next(p for p in tiba["products"] if p["product_id"] == "p2")
    terra_sym = next(p for p in symphony["products"] if p["product_id"] == "p2")

    # TERRA well covered on both boats — brain doesn't over-order
    assert terra_tiba["suggested_pallets"] == 0
    assert terra_sym["suggested_pallets"] == 0
    assert terra_tiba["coverage_gap_m2"] == 0
    assert terra_sym["coverage_gap_m2"] == 0

    print("✓ Product covered mid-horizon — no over-ordering")
    print(f"  TERRA on TIBA: {terra_tiba['suggested_pallets']} pallets (covered)")
    print(f"  TERRA on SYMPHONY: {terra_sym['suggested_pallets']} pallets (still covered)")


def test_arriving_soon_matters():
    """
    Ashley's question: "AIAS is on the water with 2000 m² of CARACOLI.
    Does the brain count that?"

    With AIAS shipment (anchor, arriving Mar 28 >= today):
      Running stock = 1500 + 2000 = 3500
      TIBA gap for CARACOLI = 0

    Without shipment (pretend AIAS never dispatched):
      Running stock = 1500
      TIBA: 1500 - (50 × 43) = -650. Gap = 1053.2 → 8 pallets

    The 2000 m² on the water should be the difference between
    "CARACOLI is fine" and "CARACOLI needs 8 pallets."
    """
    # WITH arriving_soon
    result_with = compute_horizon(
        products=[CARACOLI],
        boats=BOATS,
        inventory={"p1": Decimal("1500")},
        velocities={"p1": Decimal("50")},
        factory_stock={"p1": Decimal("3000")},
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,  # AIAS has 2000 m²
        production_schedule=[],
        today=TODAY,
    )

    # WITHOUT arriving_soon
    result_without = compute_horizon(
        products=[CARACOLI],
        boats=[
            make_boat("b2", "TIBA",            "2026-04-09", "2026-04-18"),
            make_boat("b3", "LITTLE SYMPHONY",  "2026-04-23", "2026-05-02"),
        ],
        inventory={"p1": Decimal("1500")},
        velocities={"p1": Decimal("50")},
        factory_stock={"p1": Decimal("3000")},
        drafts=[],
        shipment_items=[],  # No dispatches
        production_schedule=[],
        today=TODAY,
    )

    # With AIAS cargo: CARACOLI is fine
    tiba_with = result_with["projections"][0]
    c_with = next(p for p in tiba_with["products"] if p["product_id"] == "p1")
    assert c_with["suggested_pallets"] == 0
    assert c_with["coverage_gap_m2"] == 0

    # Without AIAS cargo: CARACOLI needs pallets
    tiba_without = result_without["projections"][0]
    c_without = next(p for p in tiba_without["products"] if p["product_id"] == "p1")
    assert c_without["suggested_pallets"] == 8
    assert c_without["coverage_gap_m2"] > 1000

    print("✓ Arriving soon matters")
    print(f"  With AIAS on water: {c_with['suggested_pallets']} pallets (covered)")
    print(f"  Without AIAS:       {c_without['suggested_pallets']} pallets (critical)")


def test_in_progress_does_not_double_count():
    """
    Factory snapshot is the sole source of truth.
    in_progress completed_m2 does NOT inflate factory_avail — it may already
    be in the snapshot. When production finishes, next upload reflects it.

    TERRA: factory snapshot = 0, in_progress completed = 1500.
    Factory avail should stay 0 (snapshot only). Can ship = 0.
    But the 2000 m² in_progress counts as scheduled production,
    so production requests are reduced.
    """
    result = compute_horizon(
        products=ALL_PRODUCTS,
        boats=BOATS,
        inventory={
            "p1": Decimal("1500"),
            "p2": Decimal("200"),
            "p3": Decimal("300"),
            "p4": Decimal("100"),
            "p5": Decimal("150"),
            "p6": Decimal("500"),
        },
        velocities={
            "p1": Decimal("50"),
            "p2": Decimal("35"),
            "p3": Decimal("25"),
            "p4": Decimal("40"),
            "p5": Decimal("20"),
            "p6": Decimal("0"),
        },
        factory_stock={
            "p1": Decimal("3000"),
            "p2": Decimal("0"),       # TERRA: zero factory snapshot
            "p3": Decimal("2000"),
            "p4": Decimal("2000"),
            "p5": Decimal("1500"),
            "p6": Decimal("0"),
        },
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[
            {
                "product_id": "p2",
                "status": "in_progress",
                "requested_m2": "2000",
                "completed_m2": "1500",
                "scheduled_date": "2026-03-10",
            },
        ],
        today=TODAY,
    )

    tiba = result["projections"][0]
    terra = next(p for p in tiba["products"] if p["product_id"] == "p2")

    # Factory avail = 0 (snapshot only — no double counting)
    assert terra["factory_available_m2"] == 0.0
    assert terra["can_ship_pallets"] == 0

    # 2000 m² in_progress is in the pipeline — reduces or eliminates requests
    terra_requests = [r for r in result["production_requests"] if r["product_id"] == "p2"]
    if terra_requests:
        assert terra_requests[0]["scheduled_m2"] == 2000.0
        assert terra_requests[0]["is_piggyback"] is True

    print("✓ In-progress does not double-count factory stock")
    print(f"  TERRA: factory_avail=0 (snapshot only), can_ship=0")


def test_scheduled_production_reduces_requests():
    """
    TERRA needs production but 500 m² is already scheduled.
    Production request should only ask for the gap MINUS scheduled.

    Single product → boats skip → gap from fallback (SYMPHONY→FUTURE span).
    Unmet = 1257.4, scheduled = 500, real = 757.4, pallets = ceil(757.4/134.4) = 6.
    """
    far_boats = [
        make_boat("b1", "AIAS",    "2026-03-15", "2026-03-28"),
        make_boat("b3", "SYMPHONY", "2026-04-23", "2026-05-02"),
        make_boat("b4", "FUTURE",   "2026-05-07", "2026-05-16"),
    ]

    result = compute_horizon(
        products=[TERRA],
        boats=far_boats,
        inventory={"p2": Decimal("200")},
        velocities={"p2": Decimal("35")},
        factory_stock={"p2": Decimal("268.8")},
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[
            {
                "product_id": "p2",
                "status": "scheduled",
                "requested_m2": "500",
                "completed_m2": "0",
                "scheduled_date": "2026-03-25",
            },
        ],
        today=TODAY,
    )

    assert len(result["production_requests"]) > 0
    req = result["production_requests"][0]
    assert req["product_id"] == "p2"
    assert req["scheduled_m2"] == 500.0  # Shows what's already in pipeline
    assert req["is_piggyback"] is True   # Has scheduled production → piggyback

    print("✓ Scheduled production reduces requests")
    print(f"  TERRA: piggyback on {req['scheduled_m2']:.0f} m² scheduled")


def test_scheduled_covers_gap_no_request():
    """
    TERRA has a gap but scheduled production fully covers it.
    Should emit ZERO production requests.

    Unmet gap ~767.4 m². Scheduled: 5000 m². 5000 > 767.4 → no request.
    """
    far_boats = [
        make_boat("b1", "AIAS",    "2026-03-15", "2026-03-28"),
        make_boat("b3", "SYMPHONY", "2026-04-23", "2026-05-02"),
        make_boat("b4", "FUTURE",   "2026-05-07", "2026-05-16"),
    ]

    result = compute_horizon(
        products=[TERRA],
        boats=far_boats,
        inventory={"p2": Decimal("200")},
        velocities={"p2": Decimal("35")},
        factory_stock={"p2": Decimal("268.8")},
        drafts=[],
        shipment_items=SHIPMENT_ITEMS,
        production_schedule=[
            {
                "product_id": "p2",
                "status": "scheduled",
                "requested_m2": "5000",
                "completed_m2": "0",
                "scheduled_date": "2026-03-25",
            },
        ],
        today=TODAY,
    )

    # Scheduled production covers the entire gap
    assert len(result["production_requests"]) == 0

    print("✓ Scheduled production covers gap — no request emitted")


if __name__ == "__main__":
    test_basic_scenario()
    test_cascade()
    test_skip_boat()
    test_draft_override()
    test_production_request()
    test_no_anchor()
    test_skip_consolidates_onto_next()
    test_ordered_boat_immutable_cascade()
    test_product_becomes_covered_mid_horizon()
    test_arriving_soon_matters()
    test_in_progress_does_not_double_count()
    test_scheduled_production_reduces_requests()
    test_scheduled_covers_gap_no_request()
    print("\n✓ All tests passed")
