"""
SIESA Inventory Parser for lot-level inventory tracking.

Parses daily XLS exports from SIESA (Colombia factory ERP).
Each row represents a lot with its own quantity and weight.

Key columns:
- Item: SIESA item code (matches products.siesa_item)
- Desc. item: Product description (fallback matching via normalize_product_name)
- Lote: Lot number
- Cant. disponible: Available quantity in m²
- Peso en KG: Weight in kg
- Bodega: Warehouse code
- Desc. bodega: Warehouse name
- CERAMICA/CALIDAD: Quality grade
"""

import structlog
import pandas as pd
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional
from pathlib import Path

from exceptions import SIESAParseError, SIESAMissingColumnsError
from utils.text_utils import normalize_product_name

logger = structlog.get_logger(__name__)

# Required columns in SIESA export
REQUIRED_COLUMNS = ["Item", "Lote", "Cant. disponible"]

# Optional columns with fallbacks
OPTIONAL_COLUMNS = {
    "Desc. item": None,
    "Peso en KG": None,
    "Bodega": None,
    "Desc. bodega": None,
    "CERAMICA/CALIDAD": None,
}


@dataclass
class SIESALotRow:
    """Parsed lot from SIESA export."""
    siesa_item: int
    siesa_description: Optional[str]
    lot_number: str
    quantity_m2: Decimal
    weight_kg: Optional[Decimal]
    warehouse_code: Optional[str]
    warehouse_name: Optional[str]
    quality: Optional[str]
    row_number: int


@dataclass
class SIESAMatchResult:
    """Result of matching a SIESA row to a product."""
    lot: SIESALotRow
    product_id: Optional[str] = None
    product_sku: Optional[str] = None
    matched_by: Optional[str] = None  # "siesa_item" or "name"


@dataclass
class SIESARowError:
    """Error for a specific row."""
    row: int
    field: str
    error: str
    value: Optional[str] = None


@dataclass
class WarehouseSummaryData:
    """Aggregated data for a warehouse."""
    code: str
    name: str
    total_m2: Decimal = field(default_factory=lambda: Decimal("0"))
    total_weight_kg: Decimal = field(default_factory=lambda: Decimal("0"))
    lot_count: int = 0


@dataclass
class SIESAParseResult:
    """Complete result of parsing a SIESA inventory file."""
    success: bool
    snapshot_date: date
    total_rows: int
    processed_rows: int
    skipped_errors: int
    errors: list[SIESARowError] = field(default_factory=list)

    # Parsed lots with match info
    matched_lots: list[SIESAMatchResult] = field(default_factory=list)
    unmatched_lots: list[SIESAMatchResult] = field(default_factory=list)

    # Statistics
    unique_siesa_items: int = 0
    total_m2: Decimal = field(default_factory=lambda: Decimal("0"))
    total_weight_kg: Decimal = field(default_factory=lambda: Decimal("0"))

    # Match statistics
    matched_by_siesa_item: int = 0
    matched_by_name: int = 0
    unmatched_count: int = 0

    # Warehouse breakdown
    warehouses: dict[str, WarehouseSummaryData] = field(default_factory=dict)


def _safe_decimal(value, default: Optional[Decimal] = None) -> Optional[Decimal]:
    """Convert value to Decimal safely."""
    if pd.isna(value) or value is None or value == "":
        return default
    try:
        return Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return default


def _safe_int(value) -> Optional[int]:
    """Convert value to int safely."""
    if pd.isna(value) or value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _safe_str(value) -> Optional[str]:
    """Convert value to string safely, returning None for empty/NaN."""
    if pd.isna(value) or value is None:
        return None
    s = str(value).strip()
    return s if s else None


def parse_siesa_file(
    file_path: str,
    snapshot_date: date,
    products_by_siesa_item: dict[int, tuple[str, str]],  # siesa_item -> (product_id, sku)
    products_by_normalized_name: dict[str, tuple[str, str]],  # normalized_name -> (product_id, sku)
) -> SIESAParseResult:
    """
    Parse SIESA XLS inventory file.

    Args:
        file_path: Path to the .xls file
        snapshot_date: Date of this inventory snapshot
        products_by_siesa_item: Dict mapping siesa_item code to (product_id, sku)
        products_by_normalized_name: Dict mapping normalized product name to (product_id, sku)

    Returns:
        SIESAParseResult with parsed lots and statistics
    """
    logger.info("parsing_siesa_file", file_path=file_path, snapshot_date=snapshot_date)

    result = SIESAParseResult(
        success=False,
        snapshot_date=snapshot_date,
        total_rows=0,
        processed_rows=0,
        skipped_errors=0,
    )

    # Read Excel file
    try:
        # Detect file format by extension
        file_ext = Path(file_path).suffix.lower()
        if file_ext == ".xlsx":
            # Modern Excel format - use openpyxl
            df = pd.read_excel(file_path, engine="openpyxl")
        else:
            # Legacy .xls format - use xlrd
            df = pd.read_excel(file_path, engine="xlrd")
        # Strip whitespace from column names (SIESA exports have trailing spaces)
        df.columns = df.columns.str.strip()
    except Exception as e:
        logger.error("siesa_file_read_error", error=str(e))
        raise SIESAParseError(f"Failed to read SIESA file: {e}")

    # Validate required columns
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        logger.error("siesa_missing_columns", missing=missing_columns)
        raise SIESAMissingColumnsError(missing_columns)

    result.total_rows = len(df)
    unique_items = set()

    # Process each row
    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row number (1-indexed + header)

        # Parse required fields
        siesa_item = _safe_int(row.get("Item"))
        if siesa_item is None:
            result.errors.append(SIESARowError(
                row=row_num,
                field="Item",
                error="Missing or invalid SIESA item code",
                value=str(row.get("Item", ""))
            ))
            result.skipped_errors += 1
            continue

        lot_number = _safe_str(row.get("Lote"))
        if not lot_number:
            result.errors.append(SIESARowError(
                row=row_num,
                field="Lote",
                error="Missing lot number",
                value=None
            ))
            result.skipped_errors += 1
            continue

        quantity_m2 = _safe_decimal(row.get("Cant. disponible"))
        if quantity_m2 is None or quantity_m2 < 0:
            result.errors.append(SIESARowError(
                row=row_num,
                field="Cant. disponible",
                error="Missing or invalid quantity",
                value=str(row.get("Cant. disponible", ""))
            ))
            result.skipped_errors += 1
            continue

        # Parse optional fields
        siesa_description = _safe_str(row.get("Desc. item"))
        weight_kg = _safe_decimal(row.get("Peso en KG"))
        warehouse_code = _safe_str(row.get("Bodega"))
        warehouse_name = _safe_str(row.get("Desc. bodega"))
        quality = _safe_str(row.get("CERAMICA/CALIDAD"))

        # Create lot record
        lot = SIESALotRow(
            siesa_item=siesa_item,
            siesa_description=siesa_description,
            lot_number=lot_number,
            quantity_m2=quantity_m2,
            weight_kg=weight_kg,
            warehouse_code=warehouse_code,
            warehouse_name=warehouse_name,
            quality=quality,
            row_number=row_num,
        )

        # Track unique items
        unique_items.add(siesa_item)

        # Match to product
        match_result = SIESAMatchResult(lot=lot)

        # Try matching by siesa_item first
        if siesa_item in products_by_siesa_item:
            product_id, sku = products_by_siesa_item[siesa_item]
            match_result.product_id = product_id
            match_result.product_sku = sku
            match_result.matched_by = "siesa_item"
            result.matched_by_siesa_item += 1
            result.matched_lots.append(match_result)
        else:
            # Try matching by normalized name
            if siesa_description:
                normalized_name = normalize_product_name(siesa_description)
                if normalized_name and normalized_name in products_by_normalized_name:
                    product_id, sku = products_by_normalized_name[normalized_name]
                    match_result.product_id = product_id
                    match_result.product_sku = sku
                    match_result.matched_by = "name"
                    result.matched_by_name += 1
                    result.matched_lots.append(match_result)
                else:
                    result.unmatched_count += 1
                    result.unmatched_lots.append(match_result)
            else:
                result.unmatched_count += 1
                result.unmatched_lots.append(match_result)

        # Update totals
        result.total_m2 += quantity_m2
        if weight_kg:
            result.total_weight_kg += weight_kg

        # Update warehouse summary
        wh_key = warehouse_code or "UNKNOWN"
        if wh_key not in result.warehouses:
            result.warehouses[wh_key] = WarehouseSummaryData(
                code=wh_key,
                name=warehouse_name or "Unknown",
            )
        wh = result.warehouses[wh_key]
        wh.total_m2 += quantity_m2
        if weight_kg:
            wh.total_weight_kg += weight_kg
        wh.lot_count += 1

        result.processed_rows += 1

    result.unique_siesa_items = len(unique_items)
    result.success = result.processed_rows > 0

    logger.info(
        "siesa_parse_complete",
        total_rows=result.total_rows,
        processed_rows=result.processed_rows,
        matched_by_siesa_item=result.matched_by_siesa_item,
        matched_by_name=result.matched_by_name,
        unmatched=result.unmatched_count,
        total_m2=float(result.total_m2),
        total_weight_kg=float(result.total_weight_kg),
    )

    return result


def parse_siesa_bytes(
    file_content: bytes,
    filename: str,
    snapshot_date: date,
    products_by_siesa_item: dict[int, tuple[str, str]],
    products_by_normalized_name: dict[str, tuple[str, str]],
) -> SIESAParseResult:
    """
    Parse SIESA XLS from bytes (for file upload).

    Args:
        file_content: Raw file bytes
        filename: Original filename
        snapshot_date: Date of this inventory snapshot
        products_by_siesa_item: Dict mapping siesa_item code to (product_id, sku)
        products_by_normalized_name: Dict mapping normalized product name to (product_id, sku)

    Returns:
        SIESAParseResult with parsed lots and statistics
    """
    import tempfile
    import os

    # Write to temp file for xlrd to read
    suffix = Path(filename).suffix or ".xls"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name

    try:
        return parse_siesa_file(
            tmp_path,
            snapshot_date,
            products_by_siesa_item,
            products_by_normalized_name,
        )
    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
