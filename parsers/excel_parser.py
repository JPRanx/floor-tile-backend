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


def parse_owner_excel(
    file: Union[str, Path, BytesIO],
    known_owner_codes: dict[str, str],
    known_sku_names: dict[str, str] = None,
) -> ExcelParseResult:
    """
    Parse owner upload Excel file.

    Args:
        file: File path (str/Path) or file-like object (BytesIO)
        known_owner_codes: Dict mapping owner_code (with leading zeros) to product_id
                          e.g., {"0000102": "uuid-123", "0000119": "uuid-456", ...}
        known_sku_names: Optional dict mapping normalized SKU names to product_id
                        e.g., {"ALMENDRO BEIGE BTE": "uuid-123", ...}

    Returns:
        ExcelParseResult with inventory, sales, and any errors

    Raises:
        ExcelParseError: If file cannot be read or is invalid format
    """
    logger.info("parsing_excel", file_type=type(file).__name__)

    result = ExcelParseResult()

    # Load Excel file
    try:
        excel = pd.ExcelFile(file, engine="openpyxl")
    except Exception as e:
        logger.error("excel_read_failed", error=str(e))
        raise ExcelParseError(
            message="Failed to read Excel file",
            details={"original_error": str(e)}
        )

    # Parse INVENTARIO sheet if present (try multiple naming conventions)
    inventory_sheet_names = [
        "Inventario", "INVENTARIO",
        "INVENTARIO CERÁMICO", "INVENTARIO CERAMICO",
        "INVENTARIO MUEBLES"
    ]
    inventory_found = False
    for sheet_name in inventory_sheet_names:
        if sheet_name in excel.sheet_names:
            _parse_inventory_sheet(excel, known_owner_codes, result, sheet_name=sheet_name, known_sku_names=known_sku_names)
            inventory_found = True
            break
    if not inventory_found:
        logger.debug("inventario_sheet_not_found")

    # Parse VENTAS sheet if present (try multiple naming conventions)
    sales_sheet_names = [
        "Ventas", "VENTAS",
        "VENTAS25CERAMICOS", "VENTAS24",
        "VENTAS25MUEBLES",
        "Sheet1"  # Fallback
    ]
    sales_found = False
    for sheet_name in sales_sheet_names:
        if sheet_name in excel.sheet_names:
            _parse_sales_sheet(excel, known_owner_codes, result, sheet_name=sheet_name, known_sku_names=known_sku_names)
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

    # Find inventory sheet
    inventory_sheet_names = [
        "Inventario", "INVENTARIO",
        "INVENTARIO CERÁMICO", "INVENTARIO CERAMICO",
        "INVENTARIO MUEBLES"
    ]

    sheet_name = None
    for name in inventory_sheet_names:
        if name in excel.sheet_names:
            sheet_name = name
            break

    if not sheet_name:
        logger.warning("no_inventory_sheet_found_for_products")
        return []

    try:
        df = excel.parse(sheet_name)
    except Exception as e:
        logger.error("failed_to_parse_inventory_sheet", error=str(e))
        return []

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

    # Normalize column names (handle variations)
    df.columns = [_normalize_column(col) for col in df.columns]

    # Check required columns
    required = ["sku", "bodega_m2", "fecha_conteo"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="columns",
            error=f"Missing required columns: {', '.join(_denormalize_columns(missing))}"
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

    # Normalize column names
    df.columns = [_normalize_column(col) for col in df.columns]

    # Check required columns
    required = ["sku", "cantidad_m2", "fecha"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="columns",
            error=f"Missing required columns: {', '.join(_denormalize_columns(missing))}"
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

    # Remove size patterns like "51X51-1", "20X61-1", etc.
    sku = re.sub(r'\s*\d+X\d+(-\d+)?$', '', sku)

    # Remove quality markers like "(T)"
    sku = re.sub(r'\s*\([A-Z]+\)\s*', ' ', sku)

    # Normalize Unicode (NFD decomposition) and remove accent marks
    sku = unicodedata.normalize('NFD', sku)
    sku = ''.join(c for c in sku if unicodedata.category(c) != 'Mn')

    # Clean up whitespace
    sku = ' '.join(sku.split())

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
