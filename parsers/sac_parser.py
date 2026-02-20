"""
SAC CSV parser for daily sales data.

Parses CSV exports from SAC (Guatemala ERP) containing daily sales records.

See BUILDER_BLUEPRINT.md for specifications.
See STANDARDS_VALIDATION.md for validation patterns.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO, StringIO
from pathlib import Path
from typing import Optional, Union
import unicodedata
import structlog

import pandas as pd

from exceptions import SACParseError, SACMissingColumnsError
from utils.text_utils import normalize_product_name, normalize_customer_name, clean_customer_name

logger = structlog.get_logger(__name__)


# ===================
# DATA CLASSES
# ===================

@dataclass
class SACSalesRecord:
    """Parsed SAC sales record ready for database insertion."""
    sale_date: date
    sac_sku: int
    product_id: str
    sku_name: str  # Original SAC description (for logging/debugging)
    quantity_m2: Decimal
    unit_price_usd: Optional[Decimal] = None
    total_price_usd: Optional[Decimal] = None
    customer: Optional[str] = None
    customer_normalized: Optional[str] = None
    invoice_number: Optional[str] = None


@dataclass
class SACParseErrorRecord:
    """Single validation error from parsing."""
    row: int
    field: str
    error: str
    value: Optional[str] = None


@dataclass
class SACParseResult:
    """Result of parsing a SAC CSV file."""
    sales: list[SACSalesRecord] = field(default_factory=list)
    errors: list[SACParseErrorRecord] = field(default_factory=list)

    # Statistics
    total_rows: int = 0
    matched_by_sac_sku: int = 0
    matched_by_name: int = 0
    unmatched_products: set = field(default_factory=set)
    skipped_non_tile: int = 0
    skipped_products: set = field(default_factory=set)  # Names of skipped non-tile products
    date_range: tuple[Optional[date], Optional[date]] = (None, None)

    # Summary stats
    total_m2_sold: Decimal = field(default_factory=lambda: Decimal("0"))
    unique_customers: set = field(default_factory=set)
    unique_products: set = field(default_factory=set)
    top_product: Optional[str] = None
    _product_totals: dict = field(default_factory=dict)  # product_id -> total_m2

    @property
    def success(self) -> bool:
        """True if no critical errors occurred."""
        return len(self.sales) > 0

    @property
    def match_rate(self) -> float:
        """Percentage of tile rows that matched a product."""
        tile_rows = self.total_rows - self.skipped_non_tile
        if tile_rows == 0:
            return 0.0
        return (len(self.sales) / tile_rows) * 100

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "sales_count": len(self.sales),
            "error_count": len(self.errors),
            "total_rows": self.total_rows,
            "matched_by_sac_sku": self.matched_by_sac_sku,
            "matched_by_name": self.matched_by_name,
            "unmatched_products": list(self.unmatched_products)[:20],
            "unmatched_count": len(self.unmatched_products),
            "skipped_non_tile": self.skipped_non_tile,
            "skipped_products": list(self.skipped_products)[:10],
            "match_rate_pct": round(self.match_rate, 1),
            "date_range": {
                "start": self.date_range[0].isoformat() if self.date_range[0] else None,
                "end": self.date_range[1].isoformat() if self.date_range[1] else None,
            },
            "total_m2_sold": float(self.total_m2_sold),
            "unique_customers": len(self.unique_customers),
            "unique_products": len(self.unique_products),
            "top_product": self.top_product,
            "errors": [
                {
                    "row": e.row,
                    "field": e.field,
                    "error": e.error,
                    "value": e.value,
                }
                for e in self.errors[:50]
            ],
        }


# ===================
# COLUMN MAPPINGS
# ===================

# Expected SAC CSV column names (Spanish) - normalized (lowercase, no accents)
SAC_REQUIRED_COLUMNS = {
    "date": ["fecha de factura", "fecha", "fecha factura", "date"],
    "sku": ["sku", "codigo", "codigo sku", "cod sku"],
    "description": ["descripcion sku", "descripcion", "producto", "nombre"],
    "quantity": ["unidades", "cantidad", "m2", "mt2", "qty"],
}

# Optional columns
SAC_OPTIONAL_COLUMNS = {
    "customer": ["nombre cliente", "cliente", "customer", "razon social"],
    "unit_price": ["precio base", "precio unitario", "precio", "unit price", "precio usd"],
    "total_price": ["facturado", "total", "valor total", "total usd", "monto"],
    "invoice": ["numero de factura", "factura", "no factura", "invoice", "numero factura"],
}

# Product category keywords for classification (no longer used for filtering)
CATEGORY_KEYWORDS = {
    "FURNITURE": ["mueble", "gabinete"],
    "SINK": ["lavamanos"],
    "TOILET": ["inodoro", "sanitario"],
    "FAUCET": ["grifo", "llave"],
    "ACCESSORY": ["accesorio"],
    "SURCHARGE": ["recargo"],
}


def _detect_product_category(description: str) -> str:
    """Detect product category from description."""
    desc_lower = description.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in desc_lower for kw in keywords):
            return category
    # Default to TILE for baldosas/ceramica or unknown
    return "TILE"


# ===================
# MAIN PARSER
# ===================

def parse_sac_csv(
    file: Union[str, Path, BytesIO, bytes],
    known_sac_skus: dict[int, str],
    known_product_names: dict[str, str],
    encoding: str = "utf-8-sig",
    filename: Optional[str] = None,
) -> SACParseResult:
    """
    Parse SAC sales file (CSV or Excel) with daily sales data.

    Args:
        file: File path, BytesIO, or raw bytes
        known_sac_skus: Dict mapping SAC SKU (int) to product_id
                       e.g., {177: "uuid-123", 193: "uuid-456"}
        known_product_names: Dict mapping normalized product name to product_id
                            e.g., {"NOGAL CAFE": "uuid-123", "CEIBA GRIS OSC": "uuid-789"}
        encoding: CSV file encoding (default UTF-8 with BOM)
        filename: Original filename to detect format (.xls/.xlsx vs .csv)

    Returns:
        SACParseResult with parsed sales and statistics

    Raises:
        SACParseError: If file cannot be read
        SACMissingColumnsError: If required columns are missing
    """
    is_excel = False
    if filename:
        lower_name = filename.lower()
        is_excel = lower_name.endswith('.xls') or lower_name.endswith('.xlsx')

    logger.info("parsing_sac_file", file_type=type(file).__name__, is_excel=is_excel, filename=filename)

    result = SACParseResult()

    # Load file (CSV or Excel)
    try:
        if is_excel:
            df = _load_excel(file)
        else:
            df = _load_csv(file, encoding)
    except Exception as e:
        logger.error("sac_file_read_failed", error=str(e), is_excel=is_excel)
        raise SACParseError(
            message=f"Failed to read file: {str(e)}",
            details={"original_error": str(e)}
        )

    result.total_rows = len(df)

    if result.total_rows == 0:
        logger.warning("sac_csv_empty")
        return result

    # Normalize column names
    df.columns = [_normalize_column(col) for col in df.columns]

    # Find and validate required columns
    column_mapping = _find_columns(df.columns.tolist())
    missing = [name for name, col in column_mapping.items() if col is None and name in SAC_REQUIRED_COLUMNS]

    if missing:
        raise SACMissingColumnsError(missing)

    # Parse each row
    min_date = None
    max_date = None

    for idx, row in df.iterrows():
        row_num = idx + 2  # CSV row (1-indexed + header)

        # Get values using column mapping
        date_col = column_mapping.get("date")
        sku_col = column_mapping.get("sku")
        desc_col = column_mapping.get("description")
        qty_col = column_mapping.get("quantity")
        customer_col = column_mapping.get("customer")
        unit_price_col = column_mapping.get("unit_price")
        total_price_col = column_mapping.get("total_price")
        invoice_col = column_mapping.get("invoice")

        # Skip empty rows
        if pd.isna(row.get(sku_col)) and pd.isna(row.get(desc_col)):
            continue

        row_errors = []

        # Parse date
        sale_date = _parse_date(row.get(date_col))
        if sale_date is None:
            row_errors.append(SACParseErrorRecord(
                row=row_num,
                field="Fecha",
                error="Invalid or missing date",
                value=str(row.get(date_col))[:50] if row.get(date_col) is not None else None
            ))
        else:
            # Track date range
            if min_date is None or sale_date < min_date:
                min_date = sale_date
            if max_date is None or sale_date > max_date:
                max_date = sale_date

        # Parse SAC SKU (integer)
        sac_sku = _parse_int(row.get(sku_col))

        # Get product description
        description = str(row.get(desc_col, "")).strip() if pd.notna(row.get(desc_col)) else ""

        # Detect product category (no longer filtering - include all products)
        product_category = _detect_product_category(description)

        # Try to match product
        product_id = None
        matched_by = None

        # First try: Match by SAC SKU
        if sac_sku is not None and sac_sku in known_sac_skus:
            product_id = known_sac_skus[sac_sku]
            matched_by = "sac_sku"
            result.matched_by_sac_sku += 1

        # Second try: Match by normalized product name
        if product_id is None and description:
            normalized_name = normalize_product_name(description)
            if normalized_name and normalized_name in known_product_names:
                product_id = known_product_names[normalized_name]
                matched_by = "name"
                result.matched_by_name += 1

        # No match found
        if product_id is None:
            identifier = f"SKU:{sac_sku}" if sac_sku else description[:40]
            result.unmatched_products.add(identifier)
            row_errors.append(SACParseErrorRecord(
                row=row_num,
                field="SKU/Descripción",
                error=f"Unknown product: {identifier}",
                value=identifier
            ))

        # Parse quantity
        quantity = _parse_decimal(row.get(qty_col))
        if quantity is None or quantity <= 0:
            row_errors.append(SACParseErrorRecord(
                row=row_num,
                field="Unidades",
                error="Invalid or missing quantity",
                value=str(row.get(qty_col))[:20] if row.get(qty_col) is not None else None
            ))

        # Skip row if critical errors
        if row_errors:
            result.errors.extend(row_errors)
            continue

        # Parse optional fields
        unit_price = _parse_decimal(row.get(unit_price_col)) if unit_price_col else None
        total_price = _parse_decimal(row.get(total_price_col)) if total_price_col else None

        # Calculate total if unit price given but total missing
        if unit_price and not total_price and quantity:
            total_price = round(unit_price * quantity, 2)

        # Customer
        customer_raw = row.get(customer_col) if customer_col else None
        customer = clean_customer_name(str(customer_raw)) if pd.notna(customer_raw) else None
        customer_normalized = normalize_customer_name(str(customer_raw)) if pd.notna(customer_raw) else None

        # Invoice number
        invoice = str(row.get(invoice_col)).strip() if invoice_col and pd.notna(row.get(invoice_col)) else None

        # Create record
        result.sales.append(SACSalesRecord(
            sale_date=sale_date,
            sac_sku=sac_sku,
            product_id=product_id,
            sku_name=description,
            quantity_m2=quantity,
            unit_price_usd=unit_price,
            total_price_usd=total_price,
            customer=customer,
            customer_normalized=customer_normalized,
            invoice_number=invoice,
        ))

        # Track statistics
        result.total_m2_sold += quantity
        if customer_normalized:
            result.unique_customers.add(customer_normalized)
        result.unique_products.add(product_id)

        # Track product totals for top product calculation
        if product_id not in result._product_totals:
            result._product_totals[product_id] = Decimal("0")
        result._product_totals[product_id] += quantity

    # Store date range
    result.date_range = (min_date, max_date)

    # Calculate top product by m²
    if result._product_totals:
        top_product_id = max(result._product_totals, key=result._product_totals.get)
        # Get the SKU name for the top product from one of the sales records
        for sale in result.sales:
            if sale.product_id == top_product_id:
                result.top_product = sale.sku_name
                break

    logger.info(
        "sac_csv_parsed",
        total_rows=result.total_rows,
        sales_count=len(result.sales),
        matched_by_sac_sku=result.matched_by_sac_sku,
        matched_by_name=result.matched_by_name,
        unmatched_count=len(result.unmatched_products),
        skipped_non_tile=result.skipped_non_tile,
        total_m2_sold=float(result.total_m2_sold),
        unique_customers=len(result.unique_customers),
        unique_products=len(result.unique_products),
        top_product=result.top_product,
        error_count=len(result.errors),
        match_rate=f"{result.match_rate:.1f}%"
    )

    return result


# ===================
# HELPER FUNCTIONS
# ===================

def _load_csv(file: Union[str, Path, BytesIO, bytes], encoding: str) -> pd.DataFrame:
    """Load CSV file into DataFrame, trying multiple encodings and detecting header rows."""
    # Keep original bytes for retry with different encodings
    original_bytes = None
    if isinstance(file, bytes):
        original_bytes = file
        file = BytesIO(file)
    elif isinstance(file, BytesIO):
        original_bytes = file.getvalue()
    elif isinstance(file, str):
        # Try to detect if it's a file path or CSV content
        if "\n" in file or "," in file:
            file = StringIO(file)

    # Try multiple encodings (Latin American files often use latin-1)
    encodings_to_try = [encoding, "latin-1", "iso-8859-1", "cp1252", "utf-8"]
    # Remove duplicates while preserving order
    encodings_to_try = list(dict.fromkeys(encodings_to_try))

    last_error = None

    # Try to find the header row by looking for known column names
    for enc in encodings_to_try:
        for skip_rows in [0, 1, 2, 3, 4, 5]:  # Try skipping up to 5 header rows
            # Reset file position or recreate BytesIO
            if original_bytes:
                file = BytesIO(original_bytes)
            elif isinstance(file, (BytesIO, StringIO)):
                file.seek(0)

            # Try common separators
            for sep in [",", ";", "\t"]:
                try:
                    if isinstance(file, (BytesIO, StringIO)):
                        file.seek(0)
                    df = pd.read_csv(file, sep=sep, encoding=enc, dtype=str, skiprows=skip_rows)

                    # Check if we found the real header (look for known column names)
                    normalized_cols = [_normalize_column(c) for c in df.columns]
                    has_date = any("fecha" in c for c in normalized_cols)
                    has_sku = any(c == "sku" for c in normalized_cols)
                    has_qty = any("unidades" in c or "cantidad" in c for c in normalized_cols)

                    if has_date and has_sku and has_qty and len(df.columns) > 5:
                        logger.debug(
                            "csv_loaded",
                            encoding=enc,
                            separator=sep,
                            skip_rows=skip_rows,
                            columns=len(df.columns)
                        )
                        return df
                except Exception as e:
                    last_error = e
                    continue

    # Final attempt with skiprows=3 (common SAC format)
    if original_bytes:
        file = BytesIO(original_bytes)
    elif isinstance(file, (BytesIO, StringIO)):
        file.seek(0)

    try:
        return pd.read_csv(file, encoding="latin-1", dtype=str, skiprows=3)
    except Exception:
        raise last_error or Exception("Could not parse CSV with any encoding")


def _load_excel(file: Union[str, Path, BytesIO, bytes]) -> pd.DataFrame:
    """Load Excel (.xls or .xlsx) file into DataFrame, detecting the header row."""
    if isinstance(file, bytes):
        file = BytesIO(file)

    # Detect engine: xlrd for .xls, openpyxl for .xlsx
    # Try openpyxl first, fall back to xlrd
    for engine in ["openpyxl", "xlrd"]:
        for skip_rows in [0, 1, 2, 3, 4, 5]:
            try:
                if isinstance(file, BytesIO):
                    file.seek(0)
                df = pd.read_excel(file, engine=engine, dtype=str, skiprows=skip_rows)

                # Check if we found the real header
                normalized_cols = [_normalize_column(c) for c in df.columns]
                has_date = any("fecha" in c for c in normalized_cols)
                has_sku = any(c == "sku" for c in normalized_cols)
                has_qty = any("unidades" in c or "cantidad" in c for c in normalized_cols)

                if has_date and has_sku and has_qty and len(df.columns) > 5:
                    logger.debug("excel_loaded", engine=engine, skip_rows=skip_rows, columns=len(df.columns))
                    return df
            except Exception:
                continue

    # Final fallback: skiprows=3 with xlrd (common SAC Excel format)
    if isinstance(file, BytesIO):
        file.seek(0)
    try:
        return pd.read_excel(file, engine="xlrd", dtype=str, skiprows=3)
    except Exception:
        if isinstance(file, BytesIO):
            file.seek(0)
        return pd.read_excel(file, engine="openpyxl", dtype=str, skiprows=3)


def _normalize_column(col: str) -> str:
    """Normalize column name for matching. Uses unicodedata to strip accents reliably across encodings."""
    col = str(col).lower().strip()
    col = unicodedata.normalize('NFKD', col)
    col = ''.join(c for c in col if not unicodedata.combining(c))
    return col


def _find_columns(columns: list[str]) -> dict[str, Optional[str]]:
    """Find matching columns from the CSV."""
    result = {}

    for name, aliases in {**SAC_REQUIRED_COLUMNS, **SAC_OPTIONAL_COLUMNS}.items():
        result[name] = None
        for alias in aliases:
            if alias in columns:
                result[name] = alias
                break

    return result


def _parse_date(value) -> Optional[date]:
    """Parse date from various formats.

    SAC (Guatemala) uses M/D/YYYY format (US style), so we try that first.
    Example: "1/6/2026" = January 6, 2026
    """
    if pd.isna(value):
        return None

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    try:
        value_str = str(value).strip()
        # SAC uses M/D/YYYY format (US style) - try this first
        # Then fall back to DD/MM/YYYY (Latin American) and ISO formats
        for fmt in ["%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue
        # Try pandas parsing with dayfirst=False (US format)
        return pd.to_datetime(value_str, dayfirst=False).date()
    except Exception:
        return None


def _parse_int(value) -> Optional[int]:
    """Parse integer value."""
    if pd.isna(value):
        return None
    try:
        # Handle float strings like "177.0"
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def _parse_decimal(value) -> Optional[Decimal]:
    """Parse decimal value."""
    if pd.isna(value):
        return None
    try:
        # Clean up string: remove currency symbols, thousands separators
        value_str = str(value).strip()
        value_str = value_str.replace("$", "").replace(",", "").replace(" ", "")
        return Decimal(value_str).quantize(Decimal("0.01"))
    except Exception:
        return None
