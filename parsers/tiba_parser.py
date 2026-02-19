"""
TIBA boat schedule Excel parser.

Parses the TIBA Excel file (Tabla de Booking) containing boat schedules.

See /data/phase2-samples/README.md for file format documentation.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional, Union
import re
import structlog

import pandas as pd

from exceptions import ExcelParseError

logger = structlog.get_logger(__name__)

# Constants
BOOKING_BUFFER_DAYS = 3  # Days before departure to book


@dataclass
class BoatScheduleRecord:
    """Parsed boat schedule record ready for database insertion."""
    departure_date: date
    arrival_date: date
    transit_days: int
    booking_deadline: date
    vessel_name: Optional[str] = None
    shipping_line: Optional[str] = None
    origin_port: str = "Castellon"
    destination_port: str = "Puerto Quetzal"
    route_type: Optional[str] = None  # 'direct' or 'with_stops'


@dataclass
class ParseError:
    """Single validation error from parsing."""
    sheet: str
    row: int
    field: str
    error: str


@dataclass
class SkippedRow:
    """A row that was skipped during parsing (non-fatal)."""
    row: int
    reason: str


@dataclass
class TibaParseResult:
    """Result of parsing a TIBA Excel file."""
    schedules: list[BoatScheduleRecord] = field(default_factory=list)
    errors: list[ParseError] = field(default_factory=list)
    skipped_rows: list[SkippedRow] = field(default_factory=list)
    origin_port: Optional[str] = None  # Extracted from file header

    @property
    def success(self) -> bool:
        """True if no fatal errors occurred (skipped rows are not fatal)."""
        return len(self.errors) == 0

    @property
    def has_data(self) -> bool:
        """True if any data was parsed."""
        return len(self.schedules) > 0

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "schedules": [
                {
                    "departure_date": r.departure_date.isoformat(),
                    "arrival_date": r.arrival_date.isoformat(),
                    "transit_days": r.transit_days,
                    "booking_deadline": r.booking_deadline.isoformat(),
                    "vessel_name": r.vessel_name,
                    "shipping_line": r.shipping_line,
                    "origin_port": r.origin_port,
                    "destination_port": r.destination_port,
                    "route_type": r.route_type,
                }
                for r in self.schedules
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
            "skipped_rows": [
                {"row": s.row, "reason": s.reason}
                for s in self.skipped_rows
            ],
            "origin_port": self.origin_port,
        }


def parse_tiba_excel(
    file: Union[str, Path, BytesIO],
) -> TibaParseResult:
    """
    Parse TIBA boat schedule Excel file.

    Args:
        file: File path (str/Path) or file-like object (BytesIO)

    Returns:
        TibaParseResult with schedules and any errors

    Raises:
        ExcelParseError: If file cannot be read or is invalid format
    """
    logger.info("parsing_tiba_excel", file_type=type(file).__name__)

    result = TibaParseResult()

    # Load Excel file
    try:
        excel = pd.ExcelFile(file, engine="openpyxl")
    except Exception as e:
        logger.error("excel_read_failed", error=str(e))
        raise ExcelParseError(
            message="Failed to read Excel file",
            details={"original_error": str(e)}
        )

    # Find the booking sheet
    sheet_name = _find_booking_sheet(excel.sheet_names)
    if sheet_name is None:
        raise ExcelParseError(
            message="No booking sheet found",
            details={"available_sheets": excel.sheet_names}
        )

    # Parse the sheet
    _parse_booking_sheet(excel, sheet_name, result)

    logger.info(
        "tiba_parsed",
        schedules_count=len(result.schedules),
        skipped_rows=len(result.skipped_rows),
        error_count=len(result.errors),
        success=result.success,
        origin_port=result.origin_port
    )

    return result


def _find_booking_sheet(sheet_names: list[str]) -> Optional[str]:
    """Find the booking sheet by name pattern."""
    for name in sheet_names:
        normalized = name.lower().strip()
        if "booking" in normalized or "tabla" in normalized:
            return name
    # Fallback to first sheet
    return sheet_names[0] if sheet_names else None


def _parse_booking_sheet(
    excel: pd.ExcelFile,
    sheet_name: str,
    result: TibaParseResult
) -> None:
    """Parse the TABLA DE BOOKING sheet."""
    logger.debug("parsing_booking_sheet", sheet=sheet_name)

    try:
        # Read without header first to find origin port and header row
        df_raw = pd.read_excel(excel, sheet_name=sheet_name, header=None)
    except Exception as e:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="sheet",
            error=f"Failed to read sheet: {str(e)}"
        ))
        return

    # Extract origin port from header rows (usually row 2)
    result.origin_port = _extract_origin_port(df_raw)

    # Find header row (contains "Fecha Salida" or similar)
    header_row = _find_header_row(df_raw)
    if header_row is None:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="header",
            error="Could not find header row with date columns"
        ))
        return

    # Re-read with correct header, keeping dates as strings to avoid locale issues
    try:
        df = pd.read_excel(
            excel,
            sheet_name=sheet_name,
            header=header_row,
            dtype=str  # Read all as strings to avoid date parsing issues
        )
    except Exception as e:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="sheet",
            error=f"Failed to parse with header: {str(e)}"
        ))
        return

    # Normalize column names
    df.columns = [_normalize_column(col) for col in df.columns]

    # Map expected columns
    col_mapping = _get_column_mapping(df.columns)

    # Check required columns
    if col_mapping["departure"] is None or col_mapping["arrival"] is None:
        result.errors.append(ParseError(
            sheet=sheet_name,
            row=0,
            field="columns",
            error="Missing required columns: Fecha Salida ETD, Fecha Llegada ETA"
        ))
        return

    # Parse each row
    for idx, row in df.iterrows():
        row_num = idx + header_row + 2  # Excel row number

        # Skip empty rows
        departure_val = row.get(col_mapping["departure"])
        if pd.isna(departure_val):
            continue

        row_errors = []

        # Parse departure date — skip row if unparseable
        departure_date = _parse_date(departure_val)
        if departure_date is None:
            result.skipped_rows.append(SkippedRow(
                row=row_num,
                reason=f"Invalid departure date: {departure_val}",
            ))
            logger.warning("skipping_bad_date_row", row=row_num, field="departure", value=str(departure_val))
            continue

        # Parse arrival date — skip row if unparseable
        arrival_val = row.get(col_mapping["arrival"])
        arrival_date = _parse_date(arrival_val)
        if arrival_date is None:
            result.skipped_rows.append(SkippedRow(
                row=row_num,
                reason=f"Invalid arrival date: {arrival_val}",
            ))
            logger.warning("skipping_bad_date_row", row=row_num, field="arrival", value=str(arrival_val))
            continue

        # Parse transit days
        transit_days = None
        if col_mapping["transit"] is not None:
            transit_val = row.get(col_mapping["transit"])
            transit_days = _parse_transit_days(transit_val)

        # If we have both dates but no transit days, calculate it
        if departure_date and arrival_date and transit_days is None:
            transit_days = (arrival_date - departure_date).days

        # Validate dates - try to fix misinterpreted dd/mm vs mm/dd
        if departure_date and arrival_date:
            if arrival_date <= departure_date:
                fixed = False

                # Try swapping day/month on arrival date only
                fixed_arrival = _swap_day_month(arrival_date)
                if fixed_arrival and fixed_arrival > departure_date:
                    logger.debug(
                        "fixed_arrival_date",
                        row=row_num,
                        original=arrival_date,
                        fixed=fixed_arrival
                    )
                    arrival_date = fixed_arrival
                    fixed = True

                # Try swapping day/month on departure date only
                if not fixed:
                    fixed_departure = _swap_day_month(departure_date)
                    if fixed_departure and arrival_date > fixed_departure:
                        logger.debug(
                            "fixed_departure_date",
                            row=row_num,
                            original=departure_date,
                            fixed=fixed_departure
                        )
                        departure_date = fixed_departure
                        fixed = True

                # Try swapping both dates
                if not fixed:
                    fixed_departure = _swap_day_month(departure_date)
                    fixed_arrival = _swap_day_month(arrival_date)
                    if fixed_departure and fixed_arrival and fixed_arrival > fixed_departure:
                        logger.debug(
                            "fixed_both_dates",
                            row=row_num,
                            original_dep=departure_date,
                            original_arr=arrival_date,
                            fixed_dep=fixed_departure,
                            fixed_arr=fixed_arrival
                        )
                        departure_date = fixed_departure
                        arrival_date = fixed_arrival
                        fixed = True

                if not fixed:
                    row_errors.append(ParseError(
                        sheet=sheet_name,
                        row=row_num,
                        field="Fecha Llegada ETA",
                        error="Arrival date must be after departure date"
                    ))

        # Recalculate transit days after any date fixes
        if departure_date and arrival_date and arrival_date > departure_date:
            transit_days = (arrival_date - departure_date).days

        # Skip past dates (optional - could be configurable)
        if departure_date and departure_date < date.today():
            logger.debug("skipping_past_departure", row=row_num, date=departure_date)
            continue

        # Collect errors or add valid record
        if row_errors:
            result.errors.extend(row_errors)
        elif departure_date and arrival_date and transit_days:
            # Get optional fields
            vessel_name = _get_string_value(row, col_mapping["vessel"])
            shipping_line = _get_string_value(row, col_mapping["shipping_line"])
            route = _get_string_value(row, col_mapping["route"])

            # Map route to route_type
            route_type = _parse_route_type(route)

            result.schedules.append(BoatScheduleRecord(
                departure_date=departure_date,
                arrival_date=arrival_date,
                transit_days=transit_days,
                booking_deadline=departure_date - timedelta(days=BOOKING_BUFFER_DAYS),
                vessel_name=vessel_name,
                shipping_line=shipping_line,
                origin_port=result.origin_port or "Castellon",
                destination_port="Puerto Quetzal",
                route_type=route_type,
            ))


def _extract_origin_port(df: pd.DataFrame) -> Optional[str]:
    """Extract origin port from header rows."""
    # Look in first 5 rows for port information
    for idx in range(min(5, len(df))):
        for col_idx in range(min(5, len(df.columns))):
            cell = df.iloc[idx, col_idx]
            if pd.isna(cell):
                continue
            cell_str = str(cell).lower()
            # Look for port patterns
            if "puerto" in cell_str or "origen" in cell_str or "fob" in cell_str:
                # Try known port names first (most reliable)
                if "cartagena" in cell_str:
                    return "Cartagena"
                if "castellon" in cell_str or "castellón" in cell_str:
                    return "Castellon"
                # Extract port name after FOB marker (e.g., "FOB CARTAGENA")
                fob_match = re.search(r'fob\s+(\w+)', cell_str, re.IGNORECASE)
                if fob_match:
                    port = fob_match.group(1).strip().title()
                    logger.debug("found_origin_port", port=port, row=idx)
                    return port
                # Extract port name after colon (e.g., "PUERTO DE ORIGEN: CARTAGENA")
                colon_match = re.search(r'(?:puerto|origen)[^:]*:\s*(?:fob\s+)?(\w+)', cell_str, re.IGNORECASE)
                if colon_match:
                    port = colon_match.group(1).strip().title()
                    logger.debug("found_origin_port", port=port, row=idx)
                    return port
    return None


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    """Find the row containing column headers."""
    header_patterns = ["fecha", "salida", "etd", "llegada", "eta"]

    for idx in range(min(10, len(df))):
        row_text = " ".join(str(v).lower() for v in df.iloc[idx] if not pd.isna(v))
        matches = sum(1 for p in header_patterns if p in row_text)
        if matches >= 2:
            logger.debug("found_header_row", row=idx)
            return idx

    return None


def _normalize_column(col) -> str:
    """Normalize column name for consistent matching."""
    col = str(col).lower().strip()
    col = col.replace(" ", "_")
    col = col.replace("á", "a").replace("é", "e").replace("í", "i")
    col = col.replace("ó", "o").replace("ú", "u")
    return col


def _get_column_mapping(columns: list[str]) -> dict:
    """Map expected columns to actual column names."""
    mapping = {
        "departure": None,
        "arrival": None,
        "transit": None,
        "vessel": None,
        "shipping_line": None,
        "route": None,
    }

    for col in columns:
        col_lower = col.lower()
        if "salida" in col_lower or "etd" in col_lower:
            mapping["departure"] = col
        elif "llegada" in col_lower or "eta" in col_lower:
            mapping["arrival"] = col
        elif "transito" in col_lower or "dias" in col_lower:
            mapping["transit"] = col
        elif "buque" in col_lower or "vessel" in col_lower:
            mapping["vessel"] = col
        elif "naviera" in col_lower or "shipping" in col_lower or "line" in col_lower:
            mapping["shipping_line"] = col
        elif "ruta" in col_lower or "route" in col_lower:
            mapping["route"] = col

    return mapping


def _swap_day_month(d: date) -> Optional[date]:
    """
    Try to swap day and month of a date.

    Useful for fixing dates that were misinterpreted as mm/dd when they were dd/mm.
    Returns None if the swap would create an invalid date.
    """
    try:
        # Only swap if day <= 12 (otherwise it couldn't have been a month)
        if d.day <= 12:
            return date(d.year, d.day, d.month)
        return None
    except ValueError:
        return None


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

        # Handle empty strings
        if not value_str:
            return None

        # Try DD/MM/YYYY formats only (Latin American standard)
        # DO NOT include %m/%d/%Y (American format)
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue

        # Try pandas parsing as fallback with dayfirst=True
        result = pd.to_datetime(value_str, dayfirst=True)
        if pd.isna(result):
            return None
        return result.date()
    except Exception:
        return None


def _parse_transit_days(value) -> Optional[int]:
    """Parse transit days from various formats like '9 DIAS' or just 9."""
    if pd.isna(value):
        return None

    # Already a number
    if isinstance(value, (int, float)):
        return int(value)

    # Try to extract number from string
    value_str = str(value).strip()
    match = re.search(r'(\d+)', value_str)
    if match:
        return int(match.group(1))

    return None


def _parse_route_type(route: Optional[str]) -> Optional[str]:
    """Convert route string to route_type enum value."""
    if route is None:
        return None

    route_lower = route.lower()
    if "directo" in route_lower or "direct" in route_lower:
        return "direct"
    if "escala" in route_lower or "stop" in route_lower:
        return "with_stops"

    return None


def _get_string_value(row: pd.Series, col: Optional[str]) -> Optional[str]:
    """Get string value from row, handling None column names."""
    if col is None:
        return None
    value = row.get(col)
    if pd.isna(value):
        return None
    return str(value).strip()
