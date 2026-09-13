"""
Reconciled V1 — §11 input normalization [D2].

Fast direct entry and parser-assisted paste converge into IDENTICAL
normalized rows — same fields, same values, distinguishable only by
`entered_via` provenance. Parse failures are reported per row and never
silently dropped; unknown product references stay in the row with
`confident_match = False` so the engine can hold them out of every supply
bucket and open a `product_match` case (§10.6.1).

This module is the honest adapter boundary: pasted text stands in for the
future real feed adapters, using the same normalization path they would.
"""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

from .planning_math import q2

# feed → ordered column layout for pasted tabular text
FEED_COLUMNS = {
    "siesa_availability": ("product_ref", "available_m2", "committed_m2?"),
    "warehouse": ("product_ref", "m2"),
    "sales": ("product_ref", "daily_velocity", "peak_weekly_m2?"),
    "in_transit": ("product_ref", "m2", "reference?", "eta?", "sailing_id?"),
    "committed_orders": ("product_ref", "m2", "due_date"),
    "production_planning": (
        "product_ref", "m2", "scheduled_start?", "status?", "production_ref?",
        "estimated_ready_date?", "actual_ready_date?", "can_add_more?",
        "evidence_as_of?", "completion_confirmed?"),
}

_SPLIT = re.compile(r"\t|,|\s{2,}")


def _cells(line: str) -> list[str]:
    if "\t" in line:
        return [cell.strip() for cell in line.strip().split("\t")]
    return [c.strip() for c in _SPLIT.split(line.strip()) if c.strip()]


def _dec(raw, field: str) -> Decimal:
    try:
        v = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"{field}: {raw!r} is not a number")
    if v < 0:
        raise ValueError(f"{field}: negative quantities are invalid")
    return q2(v)


def _date(raw, field: str) -> date:
    try:
        return date.fromisoformat(str(raw))
    except ValueError:
        raise ValueError(f"{field}: {raw!r} is not an ISO date")


def _bool(raw, field: str) -> bool:
    """Freeze the adapter vocabulary to booleans and literal true/false."""
    if isinstance(raw, bool):
        return raw
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise ValueError(f"{field}: {raw!r} is not true or false")


# ── sailing calendar ───────────────────────────────────────────────────────

def parse_sailing_calendar_text(text: str) -> tuple[list[dict], list[str]]:
    """carrier / name / departure (ISO) / optional voyage days — one sailing
    per line. Returns (rows, per-line error strings)."""
    rows, errors = [], []
    for n, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        cells = _cells(line)
        try:
            if len(cells) < 3:
                raise ValueError("need carrier, name, departure date")
            departure = _date(cells[2], "departure")
            voyage = int(cells[3]) if len(cells) > 3 else None
            if voyage is not None and voyage < 0:
                raise ValueError("voyage days must be ≥ 0")
            rows.append({"carrier": cells[0], "name": cells[1],
                         "departure": departure, "voyage_days": voyage})
        except (ValueError, IndexError) as e:
            errors.append(f"line {n}: {e}")
    return rows, errors


def normalize_sailing_row(row: dict, *, entered_via: str) -> dict:
    return {
        "carrier": str(row["carrier"]).strip(),
        "name": str(row["name"]).strip(),
        "departure": row["departure"],
        "voyage_days": row.get("voyage_days"),
        "entered_via": entered_via,
    }


# ── quantity feeds ─────────────────────────────────────────────────────────

def parse_quantity_table_text(feed: str, text: str) -> tuple[list[dict], list[str]]:
    """Parse pasted tabular text for a quantity feed into raw rows with the
    same keys direct entry uses. Per-row errors reported, never dropped
    silently."""
    layout = FEED_COLUMNS.get(feed)
    if layout is None:
        raise ValueError(f"unknown feed {feed!r}")
    rows, errors = [], []
    for n, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        cells = _cells(line)
        try:
            row: dict = {}
            ci = 0
            for col in layout:
                optional = col.endswith("?")
                name = col.rstrip("?")
                if ci >= len(cells):
                    if optional:
                        row[name] = None
                        continue
                    raise ValueError(f"missing column {name}")
                raw = cells[ci]
                ci += 1
                if raw == "":
                    if optional:
                        row[name] = None
                        continue
                    raise ValueError(f"missing column {name}")
                if name in ("available_m2", "committed_m2", "m2",
                            "daily_velocity", "peak_weekly_m2"):
                    row[name] = _dec(raw, name)
                elif name in ("eta", "due_date", "scheduled_start",
                               "estimated_ready_date", "actual_ready_date",
                               "evidence_as_of"):
                    row[name] = _date(raw, name)
                elif name in ("completion_confirmed", "can_add_more"):
                    row[name] = _bool(raw, name)
                else:
                    row[name] = raw
            if ci != len(cells):
                raise ValueError("unexpected extra columns")
            rows.append(row)
        except ValueError as e:
            errors.append(f"line {n}: {e}")
    return rows, errors


def _match_product(ref: str, products: Sequence[dict],
                   mappings: Optional[dict]) -> Optional[str]:
    """Confident match only: exact id/sku/name/alias (case-insensitive) or a
    recorded product-match resolution. Anything else stays unmatched — held
    out of every bucket until Ashley resolves it (conservative)."""
    key = str(ref).strip()
    if mappings and key in mappings:
        return mappings[key]
    lowered = key.lower()
    for p in products:
        if lowered in (str(p["product_id"]).lower(), str(p["sku"]).lower(),
                       str(p["name"]).lower()):
            return p["product_id"]
        if any(lowered == str(a).lower() for a in p.get("aliases", ())):
            return p["product_id"]
    return None


# [FC1] Duplicate normalized product identities are handled deterministically
# and explicitly — never by dictionary last-write-wins:
#  • additive quantity feeds aggregate under the formal normalization law
#    "a product's total is the SUM of its rows; the optional committed
#    column is defined only when every contributing row carries it";
#  • non-additive feeds (velocity) have no valid aggregation law and are
#    rejected atomically at the load boundary;
#  • per-line feeds (in_transit, committed_orders, production_planning)
#    legitimately carry several rows per product and are never merged.
ADDITIVE_MERGE_FIELDS = {
    "siesa_availability": ("available_m2", "committed_m2"),
    "warehouse": ("m2",),
}
NON_ADDITIVE_UNIQUE_FEEDS = frozenset({"sales"})


def merge_duplicate_product_rows(feed: str, rows: Sequence[dict]) -> list:
    """Order-independent additive merge of confidently matched duplicate
    product rows (identity feeds only; unmatched rows pass through)."""
    fields = ADDITIVE_MERGE_FIELDS.get(feed)
    if fields is None:
        return list(rows)
    out: list = []
    index: dict = {}
    for row in rows:
        pid = row.get("product_id")
        if pid is None or not row.get("confident_match"):
            out.append(row)
            continue
        if pid in index:
            tgt = index[pid]
            for f in fields:
                a, b = tgt.get(f), row.get(f)
                tgt[f] = (q2(Decimal(str(a)) + Decimal(str(b)))
                          if (a is not None and b is not None) else None)
        else:
            row = dict(row)
            index[pid] = row
            out.append(row)
    return out


def apply_mappings(feed: str, rows: Sequence[dict], mappings: dict) -> list:
    """[FC1/FCB1] The single read-time mapping law: a recorded product-match
    resolution upgrades an unconfident row to confident; a mapping to None
    (or a typed row-discard key) removes the row. Pure over its inputs so
    the SAME law can be evaluated for the current snapshot, the historical
    baseline chain, and a PROPOSED mapping before it is recorded."""
    out = []
    for row in rows:
        row = dict(row)
        row_key = (f"discard-row:{feed}:{row.get('product_ref')}"
                   f"|{row.get('reference')}")
        if row_key in mappings:
            continue                                   # row discarded (typed)
        if not row.get("confident_match"):
            ref = str(row.get("product_ref"))
            if ref in mappings:
                target = mappings[ref]
                if target is None:
                    continue                           # discarded
                row["product_id"] = target
                row["confident_match"] = True
        out.append(row)
    return out


def effective_rows(feed: str, rows: Sequence[dict], mappings: dict) -> list:
    """The effective normalized snapshot: mappings applied, then the
    order-independent additive duplicate merge [FC1]."""
    return merge_duplicate_product_rows(feed, apply_mappings(feed, rows,
                                                             mappings))


def mapping_would_collide(feed: str, rows: Sequence[dict], mappings: dict, *,
                          raw_ref: str, product_id) -> bool:
    """[FCB1] Evaluate a PROPOSED mapping against the effective normalized
    snapshot under all existing mappings PLUS the proposed one: True when
    that effective snapshot would contain more than one confident row for
    any product. Collision detection works whether the other row was
    originally confident or became confident through an earlier mapping."""
    if product_id is None or feed not in NON_ADDITIVE_UNIQUE_FEEDS:
        return False
    proposed = dict(mappings)
    proposed[str(raw_ref)] = product_id
    counts: dict = {}
    for row in effective_rows(feed, rows, proposed):
        pid = row.get("product_id")
        if pid is None or not row.get("confident_match"):
            continue
        counts[pid] = counts.get(pid, 0) + 1
        if counts[pid] > 1:
            return True
    return False


def reject_duplicate_product_rows(feed: str, rows: Sequence[dict]) -> None:
    """[FC1] Atomic denial for feeds with no valid aggregation law."""
    if feed not in NON_ADDITIVE_UNIQUE_FEEDS:
        return
    seen: set = set()
    for row in rows:
        pid = row.get("product_id")
        if pid is None or not row.get("confident_match"):
            continue
        if pid in seen:
            raise ValueError(
                f"duplicate rows for product {pid} in the {feed} feed — "
                "velocity is not additive and no valid normalization law "
                "exists; correct the rows and reload (atomic denial) [FC1]")
        seen.add(pid)


def normalize_quantity_rows(feed: str, rows: Sequence[dict], *,
                            products: Sequence[dict], entered_via: str,
                            mappings: Optional[dict] = None) -> list[dict]:
    """Direct-entry rows and parsed rows pass through this SAME function —
    the output shape is identical by construction [D2, scenario A12]."""
    layout = FEED_COLUMNS.get(feed)
    if layout is None:
        raise ValueError(f"unknown feed {feed!r}")
    out = []
    for row in rows:
        norm: dict = {}
        for col in layout:
            name = col.rstrip("?")
            v = row.get(name)
            if v is not None and name in ("available_m2", "committed_m2",
                                          "m2", "daily_velocity",
                                          "peak_weekly_m2"):
                v = q2(Decimal(str(v)))
            elif v is not None and name in (
                    "eta", "due_date", "scheduled_start",
                    "estimated_ready_date", "actual_ready_date", "evidence_as_of"):
                v = _date(v, name)
            elif v is not None and name in ("completion_confirmed", "can_add_more"):
                v = _bool(v, name)
            norm[name] = v
        pid = _match_product(norm["product_ref"], products, mappings)
        norm["product_id"] = pid
        norm["confident_match"] = pid is not None
        norm["entered_via"] = entered_via
        out.append(norm)
    return out
