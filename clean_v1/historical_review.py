"""Checksummed, frozen historical-table adapter for local review only."""
from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from .config import PlanningConfig
from .sailing_engine import SailingEngine

HISTORICAL_REPLAY_DATE = date(2026, 5, 4)
EXPECTED_HISTORICAL_CAPTURE_SHA256 = (
    "11ed64f2abeea21fadbe4cba5f48bc77a94747af31a767d6ed1973d757a93d85")
DEFAULT_CAPTURE_PATH = (Path(__file__).resolve().parents[2] / "proof" /
                        "dual-order-preflight-2026-08-16" /
                        "historical-table-snapshot.json")


def load_historical_capture(path=DEFAULT_CAPTURE_PATH, *, expected_sha256=None) -> dict:
    capture_path = Path(path)
    raw = capture_path.read_bytes()
    expected = expected_sha256 or EXPECTED_HISTORICAL_CAPTURE_SHA256
    actual = hashlib.sha256(raw).hexdigest()
    if actual != expected:
        raise ValueError(f"historical capture checksum mismatch: {actual}")
    try:
        capture = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("historical capture is not valid UTF-8 JSON") from exc
    if (capture.get("capture_kind") != "historical_table_backed_local_replay"
            or capture.get("replay_as_of") != HISTORICAL_REPLAY_DATE.isoformat()
            or capture.get("mutation_performed") is not False
            or capture.get("customer_or_contact_fields_included") is not False
            or len(capture.get("products", [])) != 12
            or len(capture.get("sailings", [])) != 4):
        raise ValueError("historical capture contract mismatch")
    product_ids = [row.get("legacy_product_id") for row in capture["products"]]
    if any(not pid or str(pid).startswith("TILE-") for pid in product_ids) \
            or len(set(product_ids)) != len(product_ids):
        raise ValueError("historical capture product identities are invalid")
    return capture


def build_historical_review_engine(path=DEFAULT_CAPTURE_PATH, *, expected_sha256=None):
    capture = load_historical_capture(path, expected_sha256=expected_sha256)
    products = [{
        "product_id": row["legacy_product_id"],
        "siesa_item": row.get("siesa_item"),
        "sku": row["sku"], "name": row["sku"],
        "category": row.get("category"), "rotation": row.get("rotation"),
        "tier": row.get("tier") or "C", "aliases": [],
        "source_unit": "m2", "weight_kg": None, "length_cm": None,
        "width_cm": None, "height_cm": None, "units_per_pallet": None,
        "m2_per_pallet": "134.40", "edit_increment_m2": "67.20",
    } for row in capture["products"]]
    eng = SailingEngine(config=PlanningConfig(default_voyage_days=15),
                        today=HISTORICAL_REPLAY_DATE, products=products)

    eng.ashley("LoadSiesaSnapshot", {
        "as_of": HISTORICAL_REPLAY_DATE,
        "raw_source_ref": "frozen:inventory_lots:2026-05-04",
        "rows": [{"product_ref": row["legacy_product_id"],
                  "available_m2": Decimal(row["siesa"]["available_m2"]),
                  "committed_m2": Decimal(row["siesa"]["committed_m2"])}
                 for row in capture["products"]]})
    eng.ashley("LoadWarehouseSnapshot", {
        "as_of": HISTORICAL_REPLAY_DATE,
        "raw_source_ref": "frozen:warehouse_snapshots:2026-05-04",
        "rows": [{"product_ref": row["legacy_product_id"],
                  "m2": Decimal(row["warehouse"]["m2"])}
                 for row in capture["products"]]})
    eng.ashley("LoadSalesSnapshot", {
        "as_of": HISTORICAL_REPLAY_DATE,
        "raw_source_ref": "frozen:sales:through-2026-05-04",
        "rows": [{"product_ref": row["legacy_product_id"],
                  "daily_velocity": (Decimal(row["sales"]["m2"]) / Decimal("365")),
                  "peak_weekly_m2": None}
                 for row in capture["products"]]})

    for row in capture["sailings"]:
        eng.ashley("RecordSailing", {
            "carrier": row.get("shipping_line") or row["carrier"],
            "name": row["vessel_name"],
            "departure": date.fromisoformat(row["departure_date"]),
            "voyage_days": row.get("transit_days"),
            "as_of": HISTORICAL_REPLAY_DATE,
            "raw_source_ref": row["legacy_sailing_id"],
        })
    chosen = max(eng.state.sailings.values(), key=lambda sailing: sailing.departure)
    eng.ashley("SetSailingDecision", {"sailing_id": chosen.sailing_id,
                                      "decision": "use"})
    eng.ashley("OpenShipmentPlan", {"sailing_id": chosen.sailing_id})

    eng.review_provenance = {
        "mode": "historical_local_replay", "as_of": "2026-05-04",
        "current_truth": False, "source": "frozen_table_capture"}
    eng.review_banner = (
        "Revisión histórica al 2026-05-04 — no representa verdad operativa actual; "
        "sin escritura en SIESA ni Supabase")
    eng.historical_factory_orders = [
        {"product_id": row["legacy_product_id"], "product_name": row["sku"],
         "lifecycle_inert": True, "orders": row.get("factory_orders", [])}
        for row in capture["products"] if row.get("factory_orders")]
    return eng
