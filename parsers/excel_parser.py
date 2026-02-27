"""
Excel parser for owner uploads.

Parses the owner's Excel template containing inventory counts and sales records.

See BUILDER_BLUEPRINT.md section "Excel Parser (Owner Upload)" for specifications.
See STANDARDS_VALIDATION.md for validation patterns.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, Union
import structlog

import pandas as pd

from exceptions import ExcelParseError
from utils.text_utils import PRODUCT_ALIASES, normalize_product_name

logger = structlog.get_logger(__name__)


@dataclass
class ProductExtract:
    """Product extracted from Excel for seeding."""
    sku: str
    category: str
    rotation: Optional[str] = None


@dataclass
class InventoryRecord:
    """Parsed inventory record ready for database insertion."""
    snapshot_date: date
    sku: str
    product_id: str
    warehouse_qty: float
    in_transit_qty: float
    notes: Optional[str] = None


@dataclass
class SalesRecord:
    """Parsed sales record ready for database insertion."""
    sale_date: date
    sku: str
    product_id: str
    quantity: float
    customer: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class ParseError:
    """Single validation error from parsing."""
    sheet: str
    row: int
    field: str
    error: str


@dataclass
class ExcelParseResult:
    """Result of parsing an Excel file."""
    inventory: list[InventoryRecord] = field(default_factory=list)
    sales: list[SalesRecord] = field(default_factory=list)
    errors: list[ParseError] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True if no errors occurred."""
        return len(self.errors) == 0

    @property
    def has_data(self) -> bool:
        """True if any data was parsed."""
        return len(self.inventory) > 0 or len(self.sales) > 0

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "inventory": [
                {
                    "snapshot_date": r.snapshot_date.isoformat(),
                    "sku": r.sku,
                    "product_id": r.product_id,
                    "warehouse_qty": r.warehouse_qty,
                    "in_transit_qty": r.in_transit_qty,
                    "notes": r.notes,
                }
                for r in self.inventory
            ],
            "sales": [
                {
                    "sale_date": r.sale_date.isoformat(),
                    "sku": r.sku,
                    "product_id": r.product_id,
                    "quantity": r.quantity,
                    "customer": r.customer,
                    "notes": r.notes,
                }
                for r in self.sales
            ],
            "errors": [
                {
                    "sheet": e.sheet,
                    "row": e.row,
                    "field": e.field,
                    "error": e.error,
                }
                for e in self.errors
            ],
        }


def _detect_excel_engine(file: Union[str, Path, BytesIO, bytes], filename: Optional[str] = None) -> str:
    """Detect the appropriate pandas engine for the Excel file format."""
    # Check filename extension first
    if filename:
        if filename.lower().endswith(".xls") and not filename.lower().endswith(".xlsx"):
            return "xlrd"
    # Check file path extension
    if isinstance(file, (str, Path)):
        path_str = str(file).lower()
        if path_str.endswith(".xls") and not path_str.endswith(".xlsx"):
            return "xlrd"
    return "openpyxl"


def parse_owner_excel(
    file: Union[str, Path, BytesIO, bytes],
    known_owner_codes: dict[str, str],
    known_sku_names: dict[str, str] = None,
    filename: Optional[str] = None,
) -> ExcelParseResult:
    """
    Parse owner upload Excel file.

    Args:
        file: File path (str/Path), file-like object (BytesIO), or raw bytes
        known_owner_codes: Dict mapping owner_code (with leading zeros) to product_id
                          e.g., {"0000102": "uuid-123", "0000119": "uuid-456", ...}
        known_sku_names: Optional dict mapping normalized SKU names to product_id
                        e.g., {"ALMENDRO BEIGE BTE": "uuid-123", ...}
        filename: Optional filename to detect .xls vs .xlsx format

    Returns:
        ExcelParseResult with inventory, sales, and any errors

    Raises:
        ExcelParseError: If file cannot be read or is invalid format
    """
    logger.info("parsing_excel", file_type=type(file).__name__, filename=filename)

    result = ExcelParseResult()

    # Wrap raw bytes in BytesIO
    if isinstance(file, bytes):
        file = BytesIO(file)

    # Detect engine based on file extension
    engine = _detect_excel_engine(file, filename)

    # Load Excel file
    try:
        excel = pd.ExcelFile(file, engine=engine)
    except Exception as e:
        logger.error("excel_read_failed", error=str(e), engine=engine)
        raise ExcelParseError(
            message="Failed to read Excel file",
            details={"original_error": str(e)}
        )

    # Parse INVENTARIO sheet if present (try multiple naming conventions)
    # Handle trailing spaces in sheet names
    inventory_sheet_names = [
        "Inventario", "INVENTARIO",
        "INVENTARIO CERÁMICO", "INVENTARIO CERAMICO",
        "INVENTARIO MUEBLES"
    ]
    inventory_found = False
    matched_sheet = None

    # First try exact match, then try with stripped names
    for sheet_name in inventory_sheet_names:
        if sheet_name in excel.sheet_names:
            matched_sheet = sheet_name
            break

    # If not found, try matching stripped names (handles trailing spaces)
    if not matched_sheet:
        for actual_sheet in excel.sheet_names:
            stripped = actual_sheet.strip()
            if stripped.upper() in [s.upper() for s in inventory_sheet_names]:
                matched_sheet = actual_sheet
                logger.info("matched_sheet_with_trailing_space", original=actual_sheet, stripped=stripped)
                break

    # If still not found, check if any sheet has movement-tracking or lot-level inventory content
    # (handles generic names like "Hoja 1", "Sheet1")
    if not matched_sheet:
        for actual_sheet in excel.sheet_names:
            if _is_movement_tracking_format(excel, actual_sheet):
                matched_sheet = actual_sheet
                logger.info("matched_inventory_by_content_detection", sheet=actual_sheet)
                break
            if _is_lot_format(excel, actual_sheet):
                matched_sheet = actual_sheet
                logger.info("matched_inventory_by_lot_detection", sheet=actual_sheet)
                break

    if matched_sheet:
        # Detect format and parse accordingly
        if _is_movement_tracking_format(excel, matched_sheet):
            logger.info("detected_movement_tracking_format", sheet=matched_sheet)
            _parse_movement_tracking_sheet(excel, known_owner_codes, result, sheet_name=matched_sheet, known_sku_names=known_sku_names)
        elif _is_lot_format(excel, matched_sheet):
            logger.info("detected_lot_format", sheet=matched_sheet)
            _parse_lot_sheet(excel, known_owner_codes, result, sheet_name=matched_sheet, known_sku_names=known_sku_names)
        else:
            _parse_inventory_sheet(excel, known_owner_codes, result, sheet_name=matched_sheet, known_sku_names=known_sku_names)
        inventory_found = True

    if not inventory_found:
        logger.debug("inventario_sheet_not_found")

    # Parse VENTAS sheet if present (try multiple naming conventions)
    sales_sheet_names = [
        "Ventas", "VENTAS",
        "VENTAS25CERAMICOS", "VENTAS24",
        "VENTAS25MUEBLES",
        "Sheet1"  # Fallback
    ]
    # Spanish month names (used in REPORTE VENTAS files where sheet = month name)
    _month_names = [
        "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
        "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
    ]
    sales_found = False
    for sheet_name in sales_sheet_names:
        if sheet_name in excel.sheet_names:
            _parse_sales_sheet(excel, known_owner_codes, result, sheet_name=sheet_name, known_sku_names=known_sku_names)
            sales_found = True
            break
    # Fallback: try month-named sheets (REPORTE VENTAS PERPETUO format)
    if not sales_found:
        for actual_sheet in excel.sheet_names:
            if actual_sheet.strip().upper() in _month_names:
                logger.info("matched_sales_sheet_by_month", sheet=actual_sheet)
                _parse_report_sales_sheet(excel, known_owner_codes, result, sheet_name=actual_sheet, known_sku_names=known_sku_names)
                sales_found = True
                break
    if not sales_found:
        logger.debug("ventas_sheet_not_found")

    logger.info(
        "excel_parsed",
        inventory_count=len(result.inventory),
        sales_count=len(result.sales),
        error_count=len(result.errors),
        success=result.success
    )

    return result


def extract_products_from_excel(
    file: Union[str, Path, BytesIO],
) -> list[ProductExtract]:
    """
    Extract unique products from INVENTARIO sheet for seeding.

    Reads the INVENTARIO sheet and extracts unique SKUs with their
    category and rotation. Used to seed products before parsing.

    Args:
        file: File path or file-like object

    Returns:
        List of ProductExtract with normalized SKU, category, and rotation
    """
    logger.info("extracting_products_from_excel")

    try:
        excel = pd.ExcelFile(file, engine="openpyxl")
    except Exception as e:
        logger.error("excel_read_failed", error=str(e))
        raise ExcelParseError(
            message="Failed to read Excel file",
            details={"original_error": str(e)}
        )

    # Find inventory sheet (handle trailing spaces)
    inventory_sheet_names = [
        "Inventario", "INVENTARIO",
        "INVENTARIO CERÁMICO", "INVENTARIO CERAMICO",
        "INVENTARIO MUEBLES"
    ]

    sheet_name = None
    # First try exact match
    for name in inventory_sheet_names:
        if name in excel.sheet_names:
            sheet_name = name
            break

    # If not found, try matching stripped names
    if not sheet_name:
        for actual_sheet in excel.sheet_names:
            stripped = actual_sheet.strip()
            if stripped.upper() in [s.upper() for s in inventory_sheet_names]:
                sheet_name = actual_sheet
                break

    # Fallback: detect inventory content in generic sheet names (e.g., "Hoja 1")
    if not sheet_name:
        for actual_sheet in excel.sheet_names:
            if _is_movement_tracking_format(excel, actual_sheet):
                sheet_name = actual_sheet
                logger.info("matched_inventory_by_content_detection", sheet=actual_sheet)
                break
            if _is_lot_format(excel, actual_sheet):
                sheet_name = actual_sheet
                logger.info("matched_inventory_by_lot_detection", sheet=actual_sheet)
                break

    if not sheet_name:
        logger.warning("no_inventory_sheet_found_for_products")
        return []

    try:
        df = excel.parse(sheet_name)
    except Exception as e:
        logger.error("failed_to_parse_inventory_sheet", error=str(e))
        return []

    # Detect format and handle appropriately
    if _is_movement_tracking_format(excel, sheet_name):
        logger.info("extracting_products_from_movement_tracking_format")
        return _extract_products_from_movement_tracking(excel, sheet_name)

    if _is_lot_format(excel, sheet_name):
        logger.info("extracting_products_from_lot_format")
        return _extract_products_from_lot(excel, sheet_name)

    # Normalize column names
    df.columns = [_normalize_column(col) for col in df.columns]

    # Check for required columns
    if "sku" not in df.columns:
        logger.warning("sku_column_not_found")
        return []

    # Extract unique products
    products_seen = set()
    products = []

    for _, row in df.iterrows():
        if pd.isna(row.get("sku")) or str(row.get("sku")).strip() == "":
            continue

        raw_sku = str(row["sku"]).strip()
        normalized_sku = _normalize_sku_name(raw_sku)

        if normalized_sku in products_seen:
            continue
        products_seen.add(normalized_sku)

        # Get category (map to our enum values)
        category = row.get("categoria", "")
        if pd.isna(category):
            category = "MADERAS"  # Default
        else:
            category = str(category).upper().strip()
            # Map their values to our enum
            category_map = {
                "MADERA": "MADERAS",
                "MADERAS": "MADERAS",
                "MARMOLIZADO": "MARMOLIZADOS",
                "MARMOLIZADOS": "MARMOLIZADOS",
                "EXTERIORES": "EXTERIORES",
            }
            category = category_map.get(category, "MADERAS")

        # Get rotation (map to our enum values)
        rotation = row.get("rotacion", None)
        if pd.isna(rotation):
            rotation = None
        else:
            rotation = str(rotation).upper().strip()
            # Map their values to our enum
            rotation_map = {
                "ALTA": "ALTA",
                "MEDIA ALTA": "MEDIA-ALTA",
                "MEDIA-ALTA": "MEDIA-ALTA",
                "MEDIA": "MEDIA",
                "BAJA": "BAJA",
            }
            rotation = rotation_map.get(rotation, None)

        products.append(ProductExtract(
            sku=normalized_sku,
            category=category,
            rotation=rotation,
        ))

    logger.info("products_extracted", count=len(products))
    return products


def _parse_inventory_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str = "Inventario",
    known_sku_names: dict[str, str] = None,
) -> None:
    """Parse the INVENTARIO sheet."""
    logger.debug("parsing_inventory_sheet", sheet=sheet_name)

    try:
        df = excel.parse(sheet_name)
    except Exception as e:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="sheet",
            error=f"Failed to read sheet: {str(e)}"
        ))
        return

    # Capture raw columns for debugging
    raw_columns = [str(c) for c in df.columns]

    # Normalize column names (handle variations)
    df.columns = [_normalize_column(col) for col in df.columns]
    normalized_columns = df.columns.tolist()

    logger.info("inventory_columns_detected", sheet=sheet_name, raw_columns=raw_columns[:15], normalized_columns=normalized_columns[:15])

    # Check required columns
    required = ["sku", "bodega_m2", "fecha_conteo"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(
            "inventory_missing_columns",
            sheet=sheet_name,
            missing=missing,
            raw_columns=raw_columns,
            normalized_columns=normalized_columns,
            first_row=df.iloc[0].to_dict() if len(df) > 0 else None,
        )
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="columns",
            error=f"Missing required columns: {', '.join(_denormalize_columns(missing))}. Found columns: {raw_columns[:15]}"
        ))
        return

    # Parse each row
    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row (1-indexed + header)

        # Skip empty rows
        if pd.isna(row.get("sku")) or str(row.get("sku")).strip() == "":
            continue

        row_errors = []
        raw_sku = str(row["sku"]).strip()
        product_id = None
        sku = raw_sku

        # Try to match by owner code first (numeric codes like 102, 119)
        if raw_sku.replace(".", "").isdigit():
            owner_code = raw_sku.split(".")[0].zfill(7)
            sku = owner_code
            product_id = known_owner_codes.get(owner_code)

        # If not numeric or not found, try matching by SKU name
        if product_id is None and known_sku_names:
            normalized_sku = _normalize_sku_name(raw_sku)
            product_id = known_sku_names.get(normalized_sku)
            if product_id:
                sku = normalized_sku

        # Still not found - report error
        if product_id is None:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="SKU",
                error=f"Unknown product: {raw_sku}"
            ))

        # Validate warehouse quantity
        warehouse_qty = row.get("bodega_m2")
        if pd.isna(warehouse_qty):
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Bodega (m²)",
                error="Required field is empty"
            ))
        elif not _is_valid_quantity(warehouse_qty):
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Bodega (m²)",
                error="Must be a non-negative number"
            ))

        # Validate date
        count_date = row.get("fecha_conteo")
        parsed_date = _parse_date(count_date)

        if parsed_date is None:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Fecha Conteo",
                error="Invalid or missing date (expected YYYY-MM-DD)"
            ))
        elif parsed_date > date.today():
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Fecha Conteo",
                error="Date cannot be in the future"
            ))

        # Collect errors or add valid record
        if row_errors:
            result.errors.extend(row_errors)
        else:
            # Get optional fields
            in_transit = row.get("en_transito_m2", 0)
            if pd.isna(in_transit):
                in_transit = 0

            notes = row.get("notas")
            if pd.isna(notes):
                notes = None

            result.inventory.append(InventoryRecord(
                snapshot_date=parsed_date,
                sku=sku,
                product_id=product_id,
                warehouse_qty=round(float(warehouse_qty), 2),
                in_transit_qty=round(float(in_transit), 2),
                notes=str(notes) if notes else None,
            ))


def _parse_sales_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str = "Ventas",
    known_sku_names: dict[str, str] = None,
) -> None:
    """Parse the VENTAS sheet."""
    logger.debug("parsing_sales_sheet", sheet=sheet_name)

    try:
        df = excel.parse(sheet_name)
    except Exception as e:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="sheet",
            error=f"Failed to read sheet: {str(e)}"
        ))
        return

    # Capture raw columns for debugging
    raw_columns = [str(c) for c in df.columns]

    # Normalize column names
    df.columns = [_normalize_column(col) for col in df.columns]
    normalized_columns = df.columns.tolist()

    logger.info("sales_columns_detected", sheet=sheet_name, raw_columns=raw_columns[:15], normalized_columns=normalized_columns[:15])

    # Check required columns
    required = ["sku", "cantidad_m2", "fecha"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(
            "sales_missing_columns",
            sheet=sheet_name,
            missing=missing,
            raw_columns=raw_columns,
            normalized_columns=normalized_columns,
            first_row=df.iloc[0].to_dict() if len(df) > 0 else None,
        )
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="columns",
            error=f"Missing required columns: {', '.join(_denormalize_columns(missing))}. Found columns: {raw_columns[:15]}"
        ))
        return

    # Parse each row
    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row (1-indexed + header)

        # Skip empty rows
        if pd.isna(row.get("sku")) or str(row.get("sku")).strip() == "":
            continue

        row_errors = []
        raw_sku = str(row["sku"]).strip()
        product_id = None
        sku = raw_sku

        # Try to match by owner code first (numeric codes like 102, 119)
        if raw_sku.replace(".", "").isdigit():
            owner_code = raw_sku.split(".")[0].zfill(7)
            sku = owner_code
            product_id = known_owner_codes.get(owner_code)

        # If not numeric or not found, try matching by SKU name
        if product_id is None and known_sku_names:
            normalized_sku = _normalize_sku_name(raw_sku)
            product_id = known_sku_names.get(normalized_sku)
            if product_id:
                sku = normalized_sku

        # Still not found - report error
        if product_id is None:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="SKU",
                error=f"Unknown product: {raw_sku}"
            ))

        # Validate quantity
        quantity = row.get("cantidad_m2")
        if pd.isna(quantity):
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Cantidad (m²)",
                error="Required field is empty"
            ))
        elif not _is_valid_quantity(quantity, allow_zero=False):
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Cantidad (m²)",
                error="Must be a positive number"
            ))

        # Validate date
        sale_date = row.get("fecha")
        parsed_date = _parse_date(sale_date)

        if parsed_date is None:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Fecha",
                error="Invalid or missing date (expected YYYY-MM-DD)"
            ))
        elif parsed_date > date.today():
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Fecha",
                error="Date cannot be in the future"
            ))

        # Collect errors or add valid record
        if row_errors:
            result.errors.extend(row_errors)
        else:
            # Get optional fields
            customer = row.get("cliente")
            if pd.isna(customer):
                customer = None

            notes = row.get("notas")
            if pd.isna(notes):
                notes = None

            result.sales.append(SalesRecord(
                sale_date=parsed_date,
                sku=sku,
                product_id=product_id,
                quantity=round(float(quantity), 2),
                customer=str(customer) if customer else None,
                notes=str(notes) if notes else None,
            ))


# ===================
# MOVEMENT-TRACKING FORMAT
# ===================

def _is_movement_tracking_format(excel: pd.ExcelFile, sheet_name: str) -> bool:
    """
    Detect if the sheet uses movement-tracking format (INICIAL/INGRESOS/SALIDAS/SALDO).

    Three known variants:
    1. Legacy format:
       - Row 2 (0-indexed): "FECHA ACTUALIZACION: DD/MM/YYYY"
       - Row 3: REFERENCIAS, FORMATO, INICIAL, INGRESOS, SALIDAS, SALDO
       - Data starts at row 5

    2. Barrios/daily inventory format:
       - Row 0: "INVENTARIO DIDARIO DD.MM.YY"
       - Row 1: PRODUCTO | INICIAL | INGRESOS | SALIDAS | SALDO
       - Row 2: Sub-headers (Referencia, Formato, Pallets, Cant ...)
       - Row 3+: Category sections (CERAMICO, MUEBLES)
       - Data starts at row 4

    3. Barrios compact format (no title row):
       - Row 0: PRODUCTO | (blank) | INICIAL | ... | SALDO | (blank)
       - Row 1: Referencia | Formato | Pallets | Cant | ...
       - Row 2+: Data rows (Formato column contains category)
       - Data starts at row 2
    """
    try:
        # Read first few rows without header
        df = excel.parse(sheet_name, header=None, nrows=5)

        # Check row 2 (index 2) for "FECHA ACTUALIZACION" (legacy format)
        if len(df) > 2:
            row2_text = str(df.iloc[2, 0]) if pd.notna(df.iloc[2, 0]) else ""
            if "FECHA ACTUALIZACION" in row2_text.upper():
                return True

        # Check row 0 for "INVENTARIO DIDARIO" (Barrios format)
        if len(df) > 0:
            row0_text = str(df.iloc[0, 0]) if pd.notna(df.iloc[0, 0]) else ""
            if "INVENTARIO" in row0_text.upper() and "DIDARIO" in row0_text.upper():
                return True

        # Also check for SALDO column header in rows 0-3
        for row_idx in range(0, min(4, len(df))):
            row_vals = df.iloc[row_idx].astype(str).str.upper()
            if any("SALDO" in str(cell) for cell in row_vals):
                return True

    except Exception as e:
        logger.warning("format_detection_failed", error=str(e))

    return False


def _is_lot_format(excel: pd.ExcelFile, sheet_name: str) -> bool:
    """
    Detect if the sheet uses lot-level inventory format from SIESA/ERP export.

    Known format:
    - Row 0 (header): Referencia, Item, Desc. item, Peso en KG, Bodega, ..., U.M., Existencia, ...
    - Key detection columns: "Existencia" and "Desc. item" and "Lote"
    """
    try:
        df = excel.parse(sheet_name, header=None, nrows=2)
        if len(df) == 0:
            return False

        row0_vals = [str(c).strip().lower() for c in df.iloc[0] if pd.notna(c)]
        has_existencia = any("existencia" in v for v in row0_vals)
        has_desc_item = any("desc. item" in v or "desc item" in v for v in row0_vals)
        has_lote = any("lote" in v for v in row0_vals)

        return has_existencia and has_desc_item and has_lote
    except Exception as e:
        logger.warning("lot_format_detection_failed", error=str(e))
        return False


def _parse_lot_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str,
    known_sku_names: dict[str, str] = None,
) -> None:
    """
    Parse lot-level inventory format.

    Format: Referencia | Item | Desc. item | Peso en KG | Bodega | ... | U.M. | Existencia | Cant. comprometida | Cant. disponible
    Multiple rows per product (one per lot). Sum Existencia per product.
    """
    df = excel.parse(sheet_name, dtype=str)

    # Capture raw columns for debugging
    raw_columns = [str(c) for c in df.columns]
    df.columns = [_normalize_column(col) for col in df.columns]
    normalized_columns = df.columns.tolist()

    logger.info("lot_columns_detected", sheet=sheet_name, raw_columns=raw_columns[:15], normalized_columns=normalized_columns[:15])

    # Find key columns
    desc_col = None
    existencia_col = None
    referencia_col = None
    um_col = None

    for col in df.columns:
        if "desc" in col and "item" in col:
            desc_col = col
        elif col.strip() == "existencia":
            existencia_col = col
        elif col.strip() == "referencia":
            referencia_col = col
        elif col.strip() in ("u.m.", "um", "u.m"):
            um_col = col

    if not desc_col or not existencia_col:
        logger.error(
            "lot_sheet_missing_columns",
            desc=desc_col, existencia=existencia_col,
            raw_columns=raw_columns,
            normalized_columns=normalized_columns,
            first_row=df.iloc[0].to_dict() if len(df) > 0 else None,
        )
        result.errors.append(ParseError(
            sheet=sheet_name, row=0, field="columns",
            error=f"Missing required columns: 'Desc. item' and 'Existencia'. Found columns: {raw_columns[:15]}"
        ))
        return

    # Aggregate: sum Existencia per product (by Desc. item)
    from collections import defaultdict
    product_totals: dict[str, float] = defaultdict(float)
    product_refs: dict[str, str] = {}  # desc -> referencia (owner_code)

    for idx, row in df.iterrows():
        desc = str(row.get(desc_col, "")).strip() if pd.notna(row.get(desc_col)) else ""
        if not desc:
            continue

        existencia_raw = row.get(existencia_col)
        if pd.isna(existencia_raw) or str(existencia_raw).strip() == "":
            continue

        try:
            existencia = float(str(existencia_raw).strip().replace(",", ""))
        except (ValueError, TypeError):
            continue

        product_totals[desc] += existencia

        # Track referencia for owner_code matching
        if referencia_col and pd.notna(row.get(referencia_col)):
            ref = str(row.get(referencia_col)).strip()
            if ref:
                product_refs[desc] = ref

    if not product_totals:
        logger.warning("lot_sheet_no_data", sheet=sheet_name)
        return

    snapshot_date_val = date.today()

    for desc, total_qty in product_totals.items():
        product_id = None

        # Try matching by referencia (owner_code)
        ref = product_refs.get(desc, "")
        if ref and ref in known_owner_codes:
            product_id = known_owner_codes[ref]

        # Try matching by normalized product name
        if not product_id and known_sku_names:
            normalized = normalize_product_name(desc)
            if normalized and normalized in known_sku_names:
                product_id = known_sku_names[normalized]

        if not product_id:
            result.errors.append(ParseError(
                sheet=sheet_name, row=0, field="product",
                error=f"Unknown product: {desc}"
            ))
            continue

        result.inventory.append(InventoryRecord(
            snapshot_date=snapshot_date_val,
            sku=desc,
            product_id=product_id,
            warehouse_qty=total_qty,
            in_transit_qty=0,
        ))

    logger.info(
        "lot_sheet_parsed",
        sheet=sheet_name,
        products=len(result.inventory),
        total_qty=sum(r.warehouse_qty for r in result.inventory),
    )


def _extract_date_from_header(df: pd.DataFrame) -> date:
    """
    Extract date from movement-tracking format headers.

    Supported formats:
    1. Legacy: Row 2 contains "FECHA ACTUALIZACION: DD/MM/YYYY"
    2. Barrios title: Row 0 contains "INVENTARIO DIDARIO DD.MM.YY"
    3. Barrios compact: No date in header — falls back to today's date

    Args:
        df: DataFrame with raw Excel data (no header parsing)

    Returns:
        Parsed date or today's date as fallback
    """
    import re

    try:
        # Try row 2: "FECHA ACTUALIZACION: 09/02/2026"
        if len(df) > 2 and pd.notna(df.iloc[2, 0]):
            header_text = str(df.iloc[2, 0])
            match = re.search(r'(\d{2}/\d{2}/\d{4})', header_text)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, "%d/%m/%Y").date()

        # Try row 0: "INVENTARIO DIDARIO 18.02.26" (DD.MM.YY)
        if len(df) > 0 and pd.notna(df.iloc[0, 0]):
            title_text = str(df.iloc[0, 0])
            # Match DD.MM.YY at end of title
            match = re.search(r'(\d{2})\.(\d{2})\.(\d{2,4})\s*$', title_text)
            if match:
                day, month, year = match.group(1), match.group(2), match.group(3)
                # Handle 2-digit year (26 → 2026)
                if len(year) == 2:
                    year = "20" + year
                return datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y").date()

    except Exception as e:
        logger.warning("date_extraction_failed", error=str(e))

    return date.today()


def _is_barrios_daily_format(df: pd.DataFrame) -> bool:
    """
    Check if this is the Barrios daily inventory format.

    Two sub-variants:
    1. Title variant: Row 0 contains "INVENTARIO DIDARIO DD.MM.YY"
    2. Compact variant: Row 0 contains "PRODUCTO" in col 0 and "SALDO" elsewhere
       (no title row — group headers are in row 0, sub-headers in row 1, data at row 2)
    """
    if len(df) == 0:
        return False
    if pd.notna(df.iloc[0, 0]):
        title = str(df.iloc[0, 0]).upper()
        # Title variant: "INVENTARIO DIDARIO 18.02.26"
        if "INVENTARIO" in title and "DIDARIO" in title:
            return True
        # Compact variant: "PRODUCTO" in col 0 + "SALDO" somewhere in row 0
        if "PRODUCTO" in title:
            row0_vals = [str(c).upper() for c in df.iloc[0] if pd.notna(c)]
            if any("SALDO" in v for v in row0_vals):
                return True
    return False


def _is_barrios_compact_format(df: pd.DataFrame) -> bool:
    """
    Check if this is the compact Barrios variant (no title row).

    Compact: Row 0 has PRODUCTO + SALDO as group headers, data starts at row 2.
    Title: Row 0 has "INVENTARIO DIDARIO", data starts at row 4.
    """
    if len(df) == 0:
        return False
    if pd.notna(df.iloc[0, 0]):
        title = str(df.iloc[0, 0]).upper()
        if "PRODUCTO" in title:
            row0_vals = [str(c).upper() for c in df.iloc[0] if pd.notna(c)]
            if any("SALDO" in v for v in row0_vals):
                return True
    return False


# Category labels found in Barrios format (col A value when it's a section header)
_BARRIOS_CATEGORY_LABELS = {"CERAMICO", "MUEBLES"}

# Map Barrios format categories to our product categories
_BARRIOS_CATEGORY_MAP = {
    "CERAMICO": "MADERAS",
    "MUEBLES": "MARMOLIZADOS",  # Furniture stored as MARMOLIZADOS
}


def _parse_movement_tracking_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str,
    known_sku_names: dict[str, str] = None,
) -> None:
    """
    Parse movement-tracking format inventory sheet.

    Supports three variants:

    1. Legacy format:
       - Row 0-1: Empty/title
       - Row 2: "FECHA ACTUALIZACION: DD/MM/YYYY"
       - Row 3: REFERENCIAS | FORMATO | INICIAL | ... | SALDO | OBSERVACIONES
       - Row 4: Sub-headers (PALETT | CANTIDAD M2 | ...)
       - Row 5+: Data (flat list, no category sections)

    2. Barrios daily format (title variant):
       - Row 0: "INVENTARIO DIDARIO DD.MM.YY"
       - Row 1: Group headers (PRODUCTO | INICIAL | INGRESOS | SALIDAS | SALDO)
       - Row 2: Sub-headers (Referencia | Formato | Pallets | Cant | ...)
       - Row 3: Category header "CERAMICO"
       - Rows 4-N: Ceramic product data
       - Row N+1: Subtotal (col A is NaN)
       - Row N+2: Category header "MUEBLES"
       - Rows N+3-M: Furniture product data
       - Row M+1: Subtotal

    3. Barrios compact format (no title row):
       - Row 0: Group headers (PRODUCTO | INICIAL | INGRESOS | SALIDAS | SALDO)
       - Row 1: Sub-headers (Referencia | Formato | Pallets | Cant | ...)
       - Row 2+: Data rows (Formato column has category per row)

    Column mapping (all formats, 0-indexed):
    - Column 0: SKU / Referencia
    - Column 1: Formato (Barrios only — "CERAMICO" or "MUEBLES")
    - Column 9: SALDO quantity (m² for tiles, units for furniture)
    """
    logger.info("parsing_movement_tracking_sheet", sheet=sheet_name)

    try:
        # Read raw data without header
        df = excel.parse(sheet_name, header=None)
    except Exception as e:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="sheet",
            error=f"Failed to read sheet: {str(e)}"
        ))
        return

    # Extract snapshot date from header
    snapshot_date = _extract_date_from_header(df)
    logger.info("extracted_snapshot_date", date=snapshot_date.isoformat())

    # Determine format variant and data start row
    is_barrios = _is_barrios_daily_format(df)
    is_compact = _is_barrios_compact_format(df)
    # Compact Barrios: data starts at row 2 (group headers + sub-headers only)
    # Title Barrios: data starts at row 4 (title + group headers + sub-headers + category)
    # Legacy: data starts at row 5
    if is_compact:
        data_start = 2
    elif is_barrios:
        data_start = 4
    else:
        data_start = 5
    data_df = df.iloc[data_start:].copy()

    variant_name = "barrios_compact" if is_compact else ("barrios" if is_barrios else "legacy")
    logger.info(
        "movement_tracking_variant",
        variant=variant_name,
        data_start=data_start,
        total_rows=len(df),
    )

    # Parse each row
    for idx, row in data_df.iterrows():
        row_num = idx + 1  # Excel row number (1-indexed)

        # Get SKU from column 0
        raw_sku = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

        # Skip empty rows, totals, and category section headers
        if not raw_sku or raw_sku == "nan" or raw_sku.upper() == "TOTAL":
            continue
        if raw_sku.upper() in _BARRIOS_CATEGORY_LABELS:
            continue

        # Get SALDO quantity from column 9
        saldo_qty = row.iloc[9] if len(row) > 9 and pd.notna(row.iloc[9]) else 0

        try:
            warehouse_qty = float(saldo_qty)
        except (ValueError, TypeError):
            warehouse_qty = 0

        # Clamp floating-point noise to zero (e.g., -1.13e-13)
        if abs(warehouse_qty) < 0.01:
            warehouse_qty = 0

        # Skip zero quantity rows
        if warehouse_qty <= 0:
            continue

        # Match product
        product_id = None
        sku = raw_sku

        # Try to match by owner code first (numeric codes)
        if raw_sku.replace(".", "").isdigit():
            owner_code = raw_sku.split(".")[0].zfill(7)
            sku = owner_code
            product_id = known_owner_codes.get(owner_code)

        # If not numeric or not found, try matching by SKU name
        if product_id is None and known_sku_names:
            normalized_sku = _normalize_sku_name(raw_sku)
            product_id = known_sku_names.get(normalized_sku)
            if product_id:
                sku = normalized_sku

        if product_id is None:
            result.errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="SKU",
                error=f"Unknown product: {raw_sku}"
            ))
            continue

        # Add valid record
        result.inventory.append(InventoryRecord(
            snapshot_date=snapshot_date,
            sku=sku,
            product_id=product_id,
            warehouse_qty=round(warehouse_qty, 2),
            in_transit_qty=0,  # In-transit handled separately
            notes=None,
        ))

    logger.info(
        "movement_tracking_parsed",
        sheet=sheet_name,
        variant=variant_name,
        records=len(result.inventory),
        errors=len(result.errors)
    )


def _extract_products_from_movement_tracking(
    excel: pd.ExcelFile,
    sheet_name: str,
) -> list[ProductExtract]:
    """
    Extract unique products from movement-tracking format inventory sheet.

    Handles legacy (SKUs at row 5+), Barrios title variant (SKUs at row 4+),
    and Barrios compact variant (SKUs at row 2+, with category in column 1).
    """
    try:
        df = excel.parse(sheet_name, header=None)
    except Exception as e:
        logger.error("failed_to_parse_movement_tracking_sheet", error=str(e))
        return []

    # Determine format and data start
    is_barrios = _is_barrios_daily_format(df)
    is_compact = _is_barrios_compact_format(df)
    if is_compact:
        data_start = 2
    elif is_barrios:
        data_start = 4
    else:
        data_start = 5
    data_df = df.iloc[data_start:].copy()

    products_seen = set()
    products = []

    for _, row in data_df.iterrows():
        # Get SKU from column 0
        raw_sku = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

        if not raw_sku or raw_sku == "nan" or raw_sku.upper() == "TOTAL":
            continue
        if raw_sku.upper() in _BARRIOS_CATEGORY_LABELS:
            continue

        normalized_sku = _normalize_sku_name(raw_sku)

        if normalized_sku in products_seen:
            continue
        products_seen.add(normalized_sku)

        # Determine category from column 1 (Barrios) or default (legacy)
        category = "MADERAS"
        if is_barrios and len(row) > 1 and pd.notna(row.iloc[1]):
            formato = str(row.iloc[1]).upper().strip()
            category = _BARRIOS_CATEGORY_MAP.get(formato, "MADERAS")

        products.append(ProductExtract(
            sku=normalized_sku,
            category=category,
            rotation=None,
        ))

    logger.info("products_extracted_from_movement_tracking", count=len(products))
    return products


def _extract_products_from_lot(
    excel: pd.ExcelFile,
    sheet_name: str,
) -> list[ProductExtract]:
    """
    Extract unique products from lot-level inventory format.

    Format: Referencia | Item | Desc. item | ... | TIPO | CERAMICA/FORMATO | CERAMICA/CALIDAD
    """
    try:
        df = excel.parse(sheet_name, dtype=str)
    except Exception as e:
        logger.error("failed_to_parse_lot_sheet_for_extract", error=str(e))
        return []

    df.columns = [_normalize_column(col) for col in df.columns]

    # Find desc column
    desc_col = None
    for col in df.columns:
        if "desc" in col and "item" in col:
            desc_col = col
            break

    if not desc_col:
        return []

    products_seen: set[str] = set()
    products: list[ProductExtract] = []

    for _, row in df.iterrows():
        desc = str(row.get(desc_col, "")).strip() if pd.notna(row.get(desc_col)) else ""
        if not desc:
            continue

        normalized = _normalize_sku_name(desc)
        if normalized in products_seen:
            continue
        products_seen.add(normalized)

        products.append(ProductExtract(
            sku=normalized,
            category="MADERAS",
            rotation=None,
        ))

    logger.info("products_extracted_from_lot_format", count=len(products))
    return products


# ===================
# REPORTE VENTAS PERPETUO FORMAT
# ===================

def _parse_report_sales_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str,
    known_sku_names: dict[str, str] = None,
) -> None:
    """
    Parse REPORTE VENTAS PERPETUO format (month-named sheets).

    Format:
    - Row 0: Company title (Cerámicas y Materiales Tarragona S.A.)
    - Row 1: Actual headers (FECHA, REFERENCIA, MT2, MUEBLES, CLIENTE, ...)
    - Row 2+: Data
    """
    logger.info("parsing_report_sales_sheet", sheet=sheet_name)

    try:
        # Read with header at row 1 (skip company title in row 0)
        df = excel.parse(sheet_name, header=1)
    except Exception as e:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="sheet",
            error=f"Failed to read sheet: {str(e)}"
        ))
        return

    # Capture raw columns for debugging
    raw_columns = [str(c) for c in df.columns]

    # Normalize column names
    df.columns = [_normalize_column(col) for col in df.columns]
    normalized_columns = df.columns.tolist()

    logger.info("report_sales_columns_detected", sheet=sheet_name, raw_columns=raw_columns[:15], normalized_columns=normalized_columns[:15])

    # Check required columns (after aliases: REFERENCIA→sku, MT2→cantidad_m2, FECHA→fecha)
    required = ["sku", "cantidad_m2", "fecha"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(
            "report_sales_missing_columns",
            sheet=sheet_name,
            missing=missing,
            raw_columns=raw_columns,
            normalized_columns=normalized_columns,
            first_row=df.iloc[0].to_dict() if len(df) > 0 else None,
        )
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="columns",
            error=f"Missing required columns: {', '.join(_denormalize_columns(missing))}. "
                  f"Found columns: {raw_columns[:15]}"
        ))
        return

    # Parse each row — reuse the same logic as _parse_sales_sheet
    for idx, row in df.iterrows():
        row_num = idx + 3  # Excel row (0=title, 1=header, 2+=data → 1-indexed)

        # Skip empty rows
        if pd.isna(row.get("sku")) or str(row.get("sku")).strip() == "":
            continue

        raw_sku = str(row["sku"]).strip()

        # Skip total/summary rows
        if raw_sku.upper() in ("TOTAL", "GRAN TOTAL", "SUBTOTAL"):
            continue

        row_errors = []
        product_id = None
        sku = raw_sku

        # Try matching by normalized SKU name (these use full names like "CARACOLI (T) 51X51-1")
        if known_sku_names:
            normalized_sku = _normalize_sku_name(raw_sku)
            product_id = known_sku_names.get(normalized_sku)
            if product_id:
                sku = normalized_sku

        # Fallback: try owner code if numeric
        if product_id is None and raw_sku.replace(".", "").isdigit():
            owner_code = raw_sku.split(".")[0].zfill(7)
            product_id = known_owner_codes.get(owner_code)
            if product_id:
                sku = owner_code

        if product_id is None:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="SKU",
                error=f"Unknown product: {raw_sku}"
            ))

        # Validate quantity
        quantity = row.get("cantidad_m2")
        if pd.isna(quantity):
            continue  # Skip rows with no m² (muebles-only rows)
        elif not _is_valid_quantity(quantity, allow_zero=False):
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="MT2",
                error="Must be a positive number"
            ))

        # Validate date
        sale_date = row.get("fecha")
        parsed_date = _parse_date(sale_date)

        if parsed_date is None:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="Fecha",
                error="Invalid or missing date"
            ))

        # Collect errors or add valid record
        if row_errors:
            result.errors.extend(row_errors)
        else:
            customer = row.get("cliente")
            if pd.isna(customer):
                customer = None

            result.sales.append(SalesRecord(
                sale_date=parsed_date,
                sku=sku,
                product_id=product_id,
                quantity=round(float(quantity), 2),
                customer=str(customer) if customer else None,
                notes=None,
            ))

    logger.info(
        "report_sales_parsed",
        sheet=sheet_name,
        records=len(result.sales),
        errors=len(result.errors)
    )


# ===================
# HELPER FUNCTIONS
# ===================

def _normalize_sku_name(sku: str) -> str:
    """
    Normalize SKU name for matching against product database.

    Handles variations like:
    - "TOLÚ GRIS (T) 51X51-1" -> "TOLU GRIS"
    - "CEIBA BEIGE BTE 51X51-1" -> "CEIBA BEIGE BTE"
    - "CARACOLÍ (T) 51X51-1" -> "CARACOLI"

    Strips:
    - Size suffix like "51X51-1", "20X61-1"
    - Quality markers like "(T)"
    - Normalizes accents to ASCII
    """
    import unicodedata
    import re

    if not sku:
        return ""

    # Convert to uppercase
    sku = sku.upper().strip()

    # Remove "BALDOSAS CERAMICAS / " prefix (REPORTE VENTAS format)
    sku = re.sub(r'^BALDOSAS\s+CER[AÁ]MICAS\s*/\s*', '', sku)

    # Remove size patterns like "51X51-1", "20X61-1", etc.
    sku = re.sub(r'\s*\d+X\d+(-\d+)?$', '', sku)

    # Remove quality markers like "(T)"
    sku = re.sub(r'\s*\([A-Z]+\)\s*', ' ', sku)

    # Normalize Unicode (NFD decomposition) and remove accent marks
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')

    # Clean up whitespace
    sku = ' '.join(sku.split())

    # Apply product aliases (e.g., MIRACLE → MIRACH, the canonical name)
    sku = PRODUCT_ALIASES.get(sku, sku)

    return sku


def _normalize_column(col: str) -> str:
    """
    Normalize column name for consistent matching.

    "Bodega (m²)" -> "bodega_m2"
    "Fecha Conteo" -> "fecha_conteo"
    "En Tránsito (m²)" -> "en_transito_m2"
    "MT2" -> "cantidad_m2" (alias)
    """
    col = str(col).lower().strip()
    col = col.replace("(m²)", "m2")
    col = col.replace("(m2)", "m2")
    col = col.replace(" ", "_")
    col = col.replace("á", "a").replace("é", "e").replace("í", "i")
    col = col.replace("ó", "o").replace("ú", "u")

    # Handle column aliases
    if col == "mt2":
        col = "cantidad_m2"
    if col == "referencia":
        col = "sku"

    return col


def _denormalize_columns(cols: list[str]) -> list[str]:
    """Convert normalized column names back to display names."""
    mapping = {
        "sku": "SKU",
        "bodega_m2": "Bodega (m²)",
        "fecha_conteo": "Fecha Conteo",
        "en_transito_m2": "En Tránsito (m²)",
        "cantidad_m2": "Cantidad (m²)",
        "fecha": "Fecha",
        "cliente": "Cliente",
        "notas": "Notas",
    }
    return [mapping.get(col, col) for col in cols]


def _parse_date(value) -> Optional[date]:
    """
    Parse various date formats to date object.

    IMPORTANT: This parser assumes DD/MM/YYYY format (Latin American standard).
    American MM/DD/YYYY format is NOT supported.
    """
    if pd.isna(value):
        return None

    # Already a date/datetime
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    # Try string parsing
    try:
        value_str = str(value).strip()
        # Try DD/MM/YYYY formats only (Latin American standard)
        # DO NOT include %m/%d/%Y (American format)
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue
        # Try pandas parsing as fallback with dayfirst=True
        return pd.to_datetime(value_str, dayfirst=True).date()
    except Exception:
        return None


def _is_valid_quantity(value, allow_zero: bool = True) -> bool:
    """Check if value is a valid quantity."""
    if pd.isna(value):
        return False
    try:
        num = float(value)
        if allow_zero:
            return num >= 0
        return num > 0
    except (ValueError, TypeError):
        return False
