"""
Tests for the snapshot-aware cascade gate in the brain.

Rule: factory_avail (Cant. disponible from SIESA) is only deducted by
committed drafts that were ordered AFTER the snapshot was uploaded.

Pre-snapshot drafts → factory already accounted for them inside Disponible
(via Cant. comprometida). Cascading them again would double-count.

Post-snapshot drafts → factory's last export doesn't know about them.
We must cascade so later boats see honest availability until next upload.

running_stock cascades regardless — pallets physically arrive at our
warehouse whether the draft was committed before or after the snapshot.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from lib.brain import compute_horizon


TODAY = date(2026, 5, 4)
SNAPSHOT_UPLOADED = datetime(2026, 5, 4, 12, 30, tzinfo=timezone.utc)

# Two future boats. Boat with the committed draft is FIRST so the
# cascade question is meaningful for the SECOND boat.
BOAT_WITH_DRAFT = {
    "id": "b_pride",
    "name": "PRIDE",
    "departure_date": date(2026, 5, 16),
    "arrival_date": date(2026, 5, 21),
    "factory_id": "f1",
    "carrier": "TIBA",
}
BOAT_AFTER = {
    "id": "b_galaxi",
    "name": "GALAXI",
    "departure_date": date(2026, 5, 23),
    "arrival_date": date(2026, 5, 28),
    "factory_id": "f1",
    "carrier": "TIBA",
}

PRODUCT = {"id": "p_tolu", "sku": "TOLU GRIS", "active": True, "tier": "A"}

SNAPSHOT_VALUE = Decimal("11088.0")  # what's in factory_snapshots.factory_available_m2


def _run(draft_ordered_at):
    """Run the brain with one committed draft on PRIDE for 14 pallets."""
    return compute_horizon(
        products=[PRODUCT],
        boats=[BOAT_WITH_DRAFT, BOAT_AFTER],
        inventory={"p_tolu": Decimal("500")},
        velocities={"p_tolu": Decimal("10")},  # ensures it's not skipped
        peak_velocities={"p_tolu": Decimal("12")},
        factory_stock={"p_tolu": SNAPSHOT_VALUE},
        drafts=[
            {
                "boat_id": "b_pride",
                "product_id": "p_tolu",
                "selected_pallets": Decimal("14"),
                "status": "ordered",
                "draft_id": "d1",
            }
        ],
        draft_headers=[
            {
                "boat_id": "b_pride",
                "status": "ordered",
                "draft_id": "d1",
                "ordered_at": draft_ordered_at,
            }
        ],
        shipment_items=[],
        production_schedule=[],
        today=TODAY,
        snapshot_created_at=SNAPSHOT_UPLOADED,
    )


def _galaxi_factory_m2(result):
    """factory_available_m2 reported for TOLU GRIS on the boat AFTER the draft."""
    galaxi = next(
        b for b in result["projections"]
        if b["boat_id"] == "b_galaxi"
    )
    p = next(p for p in galaxi["products"] if p["product_id"] == "p_tolu")
    return Decimal(str(p["factory_available_m2"]))


def _galaxi_running_stock(result):
    """Running warehouse stock projection on the boat AFTER the draft."""
    galaxi = next(
        b for b in result["projections"]
        if b["boat_id"] == "b_galaxi"
    )
    p = next(p for p in galaxi["products"] if p["product_id"] == "p_tolu")
    return Decimal(str(p["running_stock_m2"]))


def test_pre_snapshot_draft_does_not_cascade_factory():
    """Draft ordered BEFORE snapshot upload → factory_avail unchanged on later boats."""
    pre_snapshot = datetime(2026, 4, 24, 15, 1, tzinfo=timezone.utc)
    result = _run(pre_snapshot)

    # GALAXI sees the full Disponible — factory already excluded those 14
    # pallets when generating the file.
    assert _galaxi_factory_m2(result) == SNAPSHOT_VALUE, (
        "Pre-snapshot draft is already in factory's Cant. disponible — "
        "cascading it again double-counts."
    )


def test_post_snapshot_draft_cascades_factory():
    """Draft ordered AFTER snapshot upload → factory_avail reduced on later boats."""
    post_snapshot = datetime(2026, 5, 5, 9, 0, tzinfo=timezone.utc)
    result = _run(post_snapshot)

    # 14 pallets * 134.4 m²/pallet = 1881.6 m² consumed
    expected = SNAPSHOT_VALUE - Decimal("14") * Decimal("134.4")
    assert _galaxi_factory_m2(result) == expected, (
        "Post-snapshot draft is unknown to the factory — must cascade so "
        "later boats see honest availability."
    )


def test_running_stock_cascades_regardless_of_snapshot_timing():
    """running_stock += alloc_m2 for FUTURE boats regardless of when the
    draft was committed. Physical arrival doesn't depend on snapshot timing."""
    pre_snapshot = datetime(2026, 4, 24, 15, 1, tzinfo=timezone.utc)
    post_snapshot = datetime(2026, 5, 5, 9, 0, tzinfo=timezone.utc)

    pre_result = _run(pre_snapshot)
    post_result = _run(post_snapshot)

    assert _galaxi_running_stock(pre_result) == _galaxi_running_stock(post_result), (
        "running_stock must include the allocated pallets regardless of when "
        "the draft was committed — physical arrival doesn't depend on snapshot timing."
    )


def test_past_arrived_shipment_does_not_inflate_running_stock():
    """Past dispatched boats already landed; their pallets are reflected
    (or absent, if sold) in the current warehouse snapshot. Cascading them
    again would project ghost inventory onto future boats."""
    PAST_BOAT = {
        "id": "b_past",
        "name": "AIAS",
        "departure_date": date(2026, 4, 23),
        "arrival_date": date(2026, 5, 2),  # before TODAY = 2026-05-04
        "factory_id": "f1",
        "carrier": "TIBA",
    }
    FUTURE_BOAT = {
        "id": "b_future",
        "name": "PIONEER",
        "departure_date": date(2026, 5, 9),
        "arrival_date": date(2026, 5, 14),
        "factory_id": "f1",
        "carrier": "TIBA",
    }
    PRODUCT = {"id": "p_manaure", "sku": "MANAURE GRIS BTE", "active": True, "tier": "B"}

    result = compute_horizon(
        products=[PRODUCT],
        boats=[PAST_BOAT, FUTURE_BOAT],
        inventory={"p_manaure": Decimal("0")},  # warehouse is empty NOW (sold)
        velocities={"p_manaure": Decimal("37")},
        peak_velocities={"p_manaure": Decimal("40")},
        factory_stock={"p_manaure": Decimal("2688")},
        drafts=[],
        draft_headers=[],
        # Past boat shipped 1,344 m² which already arrived and sold
        shipment_items=[
            {"boat_id": "b_past", "product_id": "p_manaure", "shipped_m2": Decimal("1344")},
        ],
        production_schedule=[],
        today=TODAY,
        snapshot_created_at=SNAPSHOT_UPLOADED,
    )

    future = next(b for b in result["projections"] if b["boat_id"] == "b_future")
    p = next(p for p in future["products"] if p["product_id"] == "p_manaure")

    assert p["running_stock_m2"] == 0.0, (
        f"Past-arrived shipment must not inflate future-boat running_stock. "
        f"Warehouse is 0 (already sold), so PIONEER's running_stock should be 0, "
        f"got {p['running_stock_m2']}. The brain was projecting ghost inventory."
    )
