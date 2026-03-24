"""
Dispatch schedule Excel parser.

Parses PROGRAMACIÓN DE DESPACHO DE TARRAGONA.xlsx to extract
in-transit inventory quantities by product.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from typing import Optional
import re
import unicodedata
import structlog

import pandas as pd

logger = structlog.get_logger(__name__)


def _parse_date(value) -> Optional[date]:
    """Parse various date formats to date object (DD/MM/YYYY Latin American standard)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = str(value).strip()
    if not raw:
        return None
    # Try DD/MM/YYYY
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


@dataclass
class InTransitProduct:
    """Aggregated in-transit quantity for one product."""
    product_id: str
    sku: str
    in_transit_m2: float


@dataclass
class DispatchOrderItem:
    """One product line within a dispatch order."""
    product_id: str
    sku: str
    m2: float


@dataclass
class DispatchOrder:
    """A single order (factura) from the dispatch file with ETD."""
    factura: str
    etd: date | None
    items: list[DispatchOrderItem] = field(default_factory=list)


@dataclass
class DispatchParseResult:
    """Result of parsing a dispatch Excel file."""
    products: list[InTransitProduct] = field(default_factory=list)
    orders: list[DispatchOrder] = field(default_factory=list)
    total_m2: float = 0.0
    rows_processed: int = 0
    rows_filtered: int = 0
    excluded_orders: list[str] = field(default_factory=list)
    unmatched_skus: list[str] = field(default_factory=list)


def _normalize_dispatch_sku(raw: str) -> str:
    """Normalize SKU from dispatch file for matching."""
    sku = raw.strip().upper()
    # Remove dimension suffix like "51X51" or "51X51-1"
    sku = re.sub(r'\s+51X51(-\d+)?$', '', sku)
    sku = re.sub(r'\s*\(T\)\s*[\d,X\-]+$', '', sku)
    # Remove BTE suffix (will be added back in mapping lookup)
    sku = re.sub(r'\s+BTE$', '', sku)
    # Remove accents
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')
    # Fix encoding issues
    sku = sku.replace("\ufffd", "").replace("Ã", "A")
    return sku.strip()


def normalize_unmatched_sku(raw: str) -> str:
    """Convert a raw dispatch SKU into a clean product SKU for auto-creation.

    Strips dimension suffixes (51X51, 51X51-1) and format codes like (T),
    preserving the product name as it should appear in the products table.
    """
    sku = raw.strip().upper()
    # Remove dimension suffix like "51X51" or "51X51-1"
    sku = re.sub(r'\s+51X51(-\d+)?$', '', sku)
    sku = re.sub(r'\s*\(T\)(\s*[\d,X\-]+)?$', '', sku)
    # Fix encoding issues
    sku = sku.replace("\ufffd", "").replace("Ã", "A")
    return sku.strip()


def _build_sku_mapping(products: list) -> dict[str, tuple[str, str]]:
    """Build normalized SKU -> (product_id, sku) mapping with variants."""
    mapping: dict[str, tuple[str, str]] = {}
    for p in products:
        sku = p.sku.upper()
        mapping[sku] = (p.id, p.sku)

        # Accent-stripped version
        sku_norm = unicodedata.normalize('NFD', sku)
        sku_norm = ''.join(c for c in sku_norm if unicodedata.category(c) != 'Mn')
        mapping[sku_norm] = (p.id, p.sku)

        # Without BTE suffix
        if sku.endswith(" BTE"):
            base = sku[:-4]
            mapping[base] = (p.id, p.sku)
            base_norm = unicodedata.normalize('NFD', base)
            base_norm = ''.join(c for c in base_norm if unicodedata.category(c) != 'Mn')
            mapping[base_norm] = (p.id, p.sku)

    return mapping


def parse_dispatch_excel(
    file_content: bytes,
    products: list,
    received_orders: list[str],
) -> DispatchParseResult:
    """
    Parse dispatch Excel file and aggregate in-transit m² by product.

    Args:
        file_content: Raw Excel file bytes
        products: List of ProductResponse objects (with .id, .sku)
        received_orders: Order numbers to exclude (e.g., ["OC002", "OC003"])

    Returns:
        DispatchParseResult with per-product in-transit totals
    """
    result = DispatchParseResult(excluded_orders=list(received_orders))

    # Build SKU lookup
    sku_map = _build_sku_mapping(products)

    # Read Excel: try header rows 2, 1, 0 — Ashley's format may vary
    df = None
    for header_row in (2, 1, 0):
        try:
            candidate = pd.read_excel(BytesIO(file_content), sheet_name=0, header=header_row)
            cols_lower = [str(c).lower() for c in candidate.columns]
            # Check if we found real column names (not data values)
            has_ref = any("referencia" in c for c in cols_lower)
            has_mt = any("mt" in c and "cantidad" in c for c in cols_lower)
            if has_ref and has_mt:
                df = candidate
                logger.info("dispatch_header_detected", header_row=header_row)
                break
        except Exception:
            continue

    if df is None:
        try:
            df = pd.read_excel(BytesIO(file_content), sheet_name=0, header=2)
        except Exception as e:
            logger.error("dispatch_excel_read_failed", error=str(e))
            raise ValueError(f"Failed to read dispatch Excel: {e}")

    raw_columns = [str(c) for c in df.columns]
    logger.info("dispatch_columns_detected", raw_columns=raw_columns[:15])

    result.rows_processed = len(df)

    # Find key columns
    factura_col = None
    etd_col = None
    sku_col = None
    qty_col = None
    for col in df.columns:
        col_str = str(col).lower().strip()
        if "factura" in col_str or "orden" in col_str:
            factura_col = col
        elif "etd" in col_str or ("tentativa" in col_str and factura_col and not etd_col):
            etd_col = col
        elif "nombre" in col_str and "referencia" in col_str:
            sku_col = col
        elif "cantidad" in col_str and "mt" in col_str:
            qty_col = col

    if factura_col is None:
        logger.warning("dispatch_no_factura_column", columns=raw_columns)
        factura_col = df.columns[0]

    # Forward-fill factura and ETD so each row inherits its order's values
    df[factura_col] = df[factura_col].ffill()
    if etd_col is not None:
        df[etd_col] = df[etd_col].ffill()

    # Filter out received orders
    if received_orders:
        pattern = '|'.join(re.escape(o) for o in received_orders)
        mask = df[factura_col].astype(str).str.contains(pattern, case=False, na=False)
        result.rows_filtered = int(mask.sum())
        df = df[~mask]

    if sku_col is None or qty_col is None:
        logger.error(
            "dispatch_missing_columns",
            raw_columns=raw_columns,
            sku_col=sku_col, qty_col=qty_col,
            first_row=df.iloc[0].to_dict() if len(df) > 0 else None,
        )
        raise ValueError(
            f"Missing required columns. Found: {raw_columns[:15]}. "
            "Need 'Nombre de Referencias' and 'Cantidad de Mts'."
        )

    # Parse rows — aggregate by product AND group by order
    totals: dict[str, float] = {}  # product_id -> total_m2
    pid_to_sku: dict[str, str] = {}
    seen_unmatched: set[str] = set()
    orders_map: dict[str, DispatchOrder] = {}  # factura -> DispatchOrder

    for _, row in df.iterrows():
        raw_sku = str(row[sku_col]) if pd.notna(row.get(sku_col)) else ""
        if not raw_sku or raw_sku == "nan":
            continue
        if "TOTAL" in raw_sku.upper():
            continue

        cantidad = row[qty_col] if pd.notna(row.get(qty_col)) else 0
        try:
            cantidad = float(cantidad)
        except (ValueError, TypeError):
            continue
        if cantidad <= 0:
            continue

        normalized = _normalize_dispatch_sku(raw_sku)
        match = sku_map.get(normalized)

        if not match:
            if raw_sku not in seen_unmatched:
                seen_unmatched.add(raw_sku)
                result.unmatched_skus.append(raw_sku.strip()[:60])
            continue

        pid, sku = match
        totals[pid] = totals.get(pid, 0.0) + cantidad
        pid_to_sku[pid] = sku

        # Group by order
        factura_val = str(row[factura_col]).strip() if pd.notna(row.get(factura_col)) else ""
        if factura_val and factura_val not in orders_map:
            etd_val = _parse_date(row.get(etd_col)) if etd_col else None
            orders_map[factura_val] = DispatchOrder(factura=factura_val, etd=etd_val)
        if factura_val:
            orders_map[factura_val].items.append(DispatchOrderItem(product_id=pid, sku=sku, m2=cantidad))

    # Build aggregated products
    total_m2 = 0.0
    for pid, m2 in sorted(totals.items(), key=lambda x: x[1], reverse=True):
        result.products.append(InTransitProduct(
            product_id=pid,
            sku=pid_to_sku.get(pid, ""),
            in_transit_m2=round(m2, 2),
        ))
        total_m2 += m2

    result.total_m2 = round(total_m2, 2)
    result.orders = list(orders_map.values())

    logger.info(
        "dispatch_parsed",
        products=len(result.products),
        orders=len(result.orders),
        total_m2=result.total_m2,
        rows_processed=result.rows_processed,
        rows_filtered=result.rows_filtered,
        unmatched=len(result.unmatched_skus),
    )

    return result
