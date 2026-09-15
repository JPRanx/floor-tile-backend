"""Inventory-derived production catalog without automatic engine seeding.

The first explicitly accepted current Tarragona warehouse inventory is the
company roster authority.  This module derives deterministic product identities
from that source; it never reads a file or mutates a database by itself.
"""
from __future__ import annotations

import copy
import re
import unicodedata
import uuid

from .ashley_file_adapters import _norm, parse_siesa_xls, parse_warehouse_xlsx

_PRODUCT_NAMESPACE = uuid.UUID("f5bf1a7a-a552-46da-b218-9abc2256c8b2")


def _admission_identity(raw_reference: str) -> str:
    text = unicodedata.normalize("NFKD", str(raw_reference))
    text = text.encode("ascii", "ignore").decode().upper()
    return " ".join(re.sub(r"[^A-Z0-9]+", " ", text).split())


def _product(raw_reference: str, *, identity_key: str | None = None) -> dict:
    normalized = identity_key or _admission_identity(raw_reference)
    if not normalized:
        raise ValueError("inventory product reference cannot be empty")
    product_id = str(uuid.uuid5(_PRODUCT_NAMESPACE, normalized))
    return {
        "product_id": product_id,
        "siesa_item": None,
        "sku": raw_reference,
        "name": raw_reference,
        "category": "MADERAS",
        "rotation": "UNCLASSIFIED",
        "tier": "C",
        "aliases": [],
        "source_unit": "m2",
        "weight_kg": None,
        "length_cm": None,
        "width_cm": None,
        "height_cm": None,
        "units_per_pallet": None,
        "m2_per_pallet": "134.40",
        "edit_increment_m2": "67.20",
    }


def product_from_inventory_reference(raw_reference: str) -> dict:
    """Build the stable default product carried by an accepted inventory ref."""
    return copy.deepcopy(_product(str(raw_reference).strip()))


def production_catalog(warehouse_xlsx: bytes, siesa_xls: bytes) -> list[dict]:
    """Derive the initial roster from the two accepted current inventories.

    Zolic provides the preferred display reference where both sources identify
    the same normalized product.  SIESA contributes identities absent from
    Zolic.  Quantity—including zero—does not control roster membership.
    Neither source mutates runtime or durable state here.
    """
    if not isinstance(warehouse_xlsx, (bytes, bytearray)) or not warehouse_xlsx:
        raise ValueError("current warehouse inventory XLSX is required")
    if not isinstance(siesa_xls, (bytes, bytearray)) or not siesa_xls:
        raise ValueError("current SIESA inventory XLS is required")

    warehouse = parse_warehouse_xlsx(bytes(warehouse_xlsx), catalog=[])
    siesa = parse_siesa_xls(bytes(siesa_xls), catalog=[])
    diagnostics = warehouse["diagnostics"] + siesa["diagnostics"]
    errors = [item for item in diagnostics if item["severity"] == "error"]
    if errors:
        raise ValueError("current inventories contain invalid roster rows")

    by_identity: dict[str, dict] = {}
    for parsed, fallback_family in (
            (warehouse, "warehouse"),
            (siesa, "siesa_availability")):
        refs = parsed.get("source_refs") or [
            {"source_family": fallback_family,
             "normalized_identity": _norm(row["product_ref"]),
             "raw_ref": str(row["product_ref"]).strip()}
            for row in parsed["rows"]
        ]
        for source_ref in refs:
            raw = str(source_ref["raw_ref"]).strip()
            identity = str(source_ref.get("normalized_identity") or _norm(raw))
            entry = by_identity.setdefault(identity, {"source_aliases": []})
            alias = {"source_family": str(source_ref["source_family"]),
                     "raw_ref": raw}
            if alias not in entry["source_aliases"]:
                entry["source_aliases"].append(alias)

    if not by_identity:
        raise ValueError("current inventories contain no Tarragona products")
    if "" in by_identity:
        raise ValueError("current inventories contain an empty product identity")

    products = []
    for key in sorted(by_identity):
        entry = by_identity[key]
        family_order = {"warehouse": 0, "siesa_availability": 1}
        entry["source_aliases"].sort(
            key=lambda item: (family_order.get(item["source_family"], 9),
                              item["raw_ref"]))
        warehouse_labels = [item["raw_ref"] for item in entry["source_aliases"]
                            if item["source_family"] == "warehouse"]
        candidates = warehouse_labels or [item["raw_ref"]
                                          for item in entry["source_aliases"]]
        preferred = min(candidates, key=lambda raw: (
            0 if "(T)" in raw.upper() else 1, raw))
        product = _product(preferred, identity_key=key)
        product["source_aliases"] = copy.deepcopy(entry["source_aliases"])
        product["aliases"] = sorted({
            item["raw_ref"] for item in entry["source_aliases"]
            if item["raw_ref"] != preferred
        })
        products.append(product)
    ids = [product["product_id"] for product in products]
    if len(ids) != len(set(ids)):
        raise ValueError("current inventories contain colliding product identities")
    return [copy.deepcopy(product) for product in products]
