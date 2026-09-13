"""Ashley-current local operating mode built from accepted sanitized artifacts.

This mode is loopback/in-memory only. It never reads Supabase or writes SIESA.
The historical review builder remains the default proof mode.
"""
from __future__ import annotations

import json
import hashlib
from datetime import date
from pathlib import Path

from .config import PlanningConfig
from .sailing_engine import SailingEngine

IMPLEMENTATION = Path(__file__).resolve().parents[2]
PROOF = IMPLEMENTATION / "proof" / "ashley-files-readonly-preview-2026-08-23"
CAPTURE = IMPLEMENTATION / "proof" / "dual-order-preflight-2026-08-16" / "historical-table-snapshot.json"
AS_OF = date(2026, 8, 23)

EXPECTED_SOURCE_DIGESTS = {
    "historical-table-snapshot.json": "11ed64f2abeea21fadbe4cba5f48bc77a94747af31a767d6ed1973d757a93d85",
    "warehouse_2026-08-21.txt": "1c593e7eb37740f5cb4663115bd0ccc09ec2dd9a18cdcd1142a045ad2f8e5d60",
    "sales_velocity_90d_through_2026-08-19.txt": "05b2bde9e3b6781f34993217affe55cbcfefe19ec926521c3c7f5431195bcd3e",
    "siesa_availability_2026-08-21.txt": "c6716626e0015c4defa6ba88c2baf83f2a2e6e3a6d7f1cf1103a26ddd8c9a5bb",
    "real-production-dispatch-adapter-report.json": "ddc6d7cd7f45c0bdae1a0974064c036d5dd244b7ff4839707fce64024a24a5a7",
    "future-boats-whatsapp-2026-08-18.json": "a37225ff546c845f8279645f940207359e3ceb8f142eda12b963b52e7161f3a8",
}


def read_verified_source(path: Path, expected_sha256: str) -> bytes:
    data = path.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise ValueError(
            f"Ashley current source checksum mismatch for {path.name}: {actual}")
    return data


def _proof_source(filename: str) -> bytes:
    return read_verified_source(PROOF / filename, EXPECTED_SOURCE_DIGESTS[filename])


def _products() -> list[dict]:
    capture = json.loads(read_verified_source(
        CAPTURE, EXPECTED_SOURCE_DIGESTS["historical-table-snapshot.json"]))
    return [{
        "product_id": row["legacy_product_id"],
        "siesa_item": row.get("siesa_item"),
        "sku": row["sku"], "name": row["sku"],
        "category": row.get("category"), "rotation": row.get("rotation"),
        "tier": row.get("tier") or "C", "aliases": [],
        "source_unit": "m2", "weight_kg": None, "length_cm": None,
        "width_cm": None, "height_cm": None, "units_per_pallet": None,
        "m2_per_pallet": "134.40", "edit_increment_m2": "67.20",
    } for row in capture["products"]]


def _tab_rows(filename: str, feed: str) -> list[dict]:
    rows = []
    for line in _proof_source(filename).decode("utf-8").splitlines():
        if not line.strip():
            continue
        cells = line.split("\t")
        if feed == "warehouse":
            rows.append({"product_ref": cells[0], "m2": cells[1]})
        elif feed == "sales":
            rows.append({"product_ref": cells[0], "daily_velocity": cells[1],
                         "peak_weekly_m2": cells[2]})
        elif feed == "siesa":
            rows.append({"product_ref": cells[0], "available_m2": cells[1],
                         "committed_m2": cells[2]})
    return rows


def build_ashley_current_review_engine() -> SailingEngine:
    """Build the deterministic current-data local engine and open Pioneer V.195."""
    engine = SailingEngine(
        config=PlanningConfig(default_voyage_days=9), today=AS_OF,
        products=_products())
    engine.ashley("LoadWarehouseSnapshot", {
        "as_of": date(2026, 8, 21),
        "rows": _tab_rows("warehouse_2026-08-21.txt", "warehouse"),
        "raw_source_ref": "ashley:warehouse-2026-08-21"})
    engine.ashley("LoadSalesSnapshot", {
        "as_of": date(2026, 8, 19),
        "rows": _tab_rows("sales_velocity_90d_through_2026-08-19.txt", "sales"),
        "raw_source_ref": "ashley:sales-through-2026-08-19"})
    engine.ashley("LoadSiesaSnapshot", {
        "as_of": date(2026, 8, 21),
        "rows": _tab_rows("siesa_availability_2026-08-21.txt", "siesa"),
        "raw_source_ref": "ashley:siesa-2026-08-21"})

    adapter_report = json.loads(_proof_source(
        "real-production-dispatch-adapter-report.json"))
    engine.ashley("LoadProductionPlanning", {
        "as_of": AS_OF, "rows": adapter_report["production"]["rows"],
        "raw_source_ref": "ashley:production-2026-08-23"})

    boats = json.loads(_proof_source("future-boats-whatsapp-2026-08-18.json"))
    pioneer_id = None
    for boat in boats["sailings"]:
        name = f"{boat['vessel']} V.{boat['voyage']}"
        sailing_id = engine.ashley("RecordSailing", {
            "carrier": boat["carrier"], "name": name,
            "departure": date.fromisoformat(boat["departure"]),
            "voyage_days": 9, "as_of": date(2026, 8, 18),
            "raw_source_ref": "whatsapp-boats-2026-08-18"})["sailing_id"]
        engine.ashley("SetSailingDecision", {
            "sailing_id": sailing_id, "decision": "watch"})
        if boat["vessel"] == "SEABOARD PIONEER" and boat["voyage"] == "195":
            pioneer_id = sailing_id
    if pioneer_id is None:
        raise ValueError("Pioneer V.195 is absent from the accepted boat calendar")
    engine.ashley("SetSailingDecision", {
        "sailing_id": pioneer_id, "decision": "use"})
    engine.ashley("OpenShipmentPlan", {"sailing_id": pioneer_id})

    scheduled = []
    for order in adapter_report["dispatch"]["rows"]:
        scheduled.append({**order, "counted_as_transit": False})
    engine.scheduled_dispatch_context = scheduled
    engine.review_provenance = {
        "mode": "ashley_current_local_rehearsal",
        "as_of": AS_OF.isoformat(), "current_truth": True,
        "source": "ashley_files_plus_whatsapp_boats",
        "source_digests": dict(EXPECTED_SOURCE_DIGESTS)}
    engine.review_banner = (
        "Rehearsal local con archivos de Ashley al 23-ago-2026 — "
        "sin escritura en SIESA, sin Supabase y sin despliegue")
    engine.historical_factory_orders = []
    return engine
