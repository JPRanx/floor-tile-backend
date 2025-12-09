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
) -> ExcelParseResult:
    """
    Parse owner upload Excel file.

    Args:
        file: File path (str/Path) or file-like object (BytesIO)
        known_owner_codes: Dict mapping owner_code (with leading zeros) to product_id
                          e.g., {"0000102": "uuid-123", "0000119": "uuid-456", ...}

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

    # Parse INVENTARIO sheet if present
    if "Inventario" in excel.sheet_names:
        _parse_inventory_sheet(excel, known_owner_codes, result)
    elif "INVENTARIO" in excel.sheet_names:
        _parse_inventory_sheet(excel, known_owner_codes, result, sheet_name="INVENTARIO")
    else:
        logger.debug("inventario_sheet_not_found")

    # Parse VENTAS sheet if present
    if "Ventas" in excel.sheet_names:
        _parse_sales_sheet(excel, known_owner_codes, result)
    elif "VENTAS" in excel.sheet_names:
        _parse_sales_sheet(excel, known_owner_codes, result, sheet_name="VENTAS")
    else:
        logger.debug("ventas_sheet_not_found")

    logger.info(
        "excel_parsed",
        inventory_count=len(result.inventory),
        sales_count=len(result.sales),
        error_count=len(result.errors),
        success=result.success
    )

    return result


def _parse_inventory_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str = "Inventario"
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
        # Owner's Excel SKU column contains integer codes (102, 119, etc.)
        # Convert to string, pad to 7 chars to match DB format (0000102, 0000119)
        owner_code = str(row["sku"]).strip().split(".")[0].zfill(7)
        sku = owner_code  # Keep padded version for record

        # Validate owner_code against known mappings
        if owner_code not in known_owner_codes:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="SKU",
                error=f"Unknown product code: {owner_code}"
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
                product_id=known_owner_codes[owner_code],
                warehouse_qty=round(float(warehouse_qty), 2),
                in_transit_qty=round(float(in_transit), 2),
                notes=str(notes) if notes else None,
            ))


def _parse_sales_sheet(
    excel: pd.ExcelFile,
    known_owner_codes: dict[str, str],
    result: ExcelParseResult,
    sheet_name: str = "Ventas"
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
        # Owner's Excel SKU column contains integer codes (102, 119, etc.)
        # Convert to string, pad to 7 chars to match DB format (0000102, 0000119)
        owner_code = str(row["sku"]).strip().split(".")[0].zfill(7)
        sku = owner_code  # Keep padded version for record

        # Validate owner_code against known mappings
        if owner_code not in known_owner_codes:
            row_errors.append(ParseError(
                sheet=sheet_name,
                row=row_num,
                field="SKU",
                error=f"Unknown product code: {owner_code}"
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
                product_id=known_owner_codes[owner_code],
                quantity=round(float(quantity), 2),
                customer=str(customer) if customer else None,
                notes=str(notes) if notes else None,
            ))


# ===================
# HELPER FUNCTIONS
# ===================

def _normalize_column(col: str) -> str:
    """
    Normalize column name for consistent matching.

    "Bodega (m²)" -> "bodega_m2"
    "Fecha Conteo" -> "fecha_conteo"
    "En Tránsito (m²)" -> "en_transito_m2"
    """
    col = str(col).lower().strip()
    col = col.replace("(m²)", "m2")
    col = col.replace("(m2)", "m2")
    col = col.replace(" ", "_")
    col = col.replace("á", "a").replace("é", "e").replace("í", "i")
    col = col.replace("ó", "o").replace("ú", "u")
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
    """Parse various date formats to date object."""
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
        # Try common formats
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue
        # Try pandas parsing as fallback
        return pd.to_datetime(value_str).date()
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
