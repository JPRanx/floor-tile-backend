"""
Packing List Parser Service.

Parses Excel packing lists from the factory (XLSX format).
Extracts product details, container assignments, and totals.
"""

from typing import Optional
from decimal import Decimal
from io import BytesIO
import re
import structlog
import pandas as pd

from models.ingest import ParsedFieldConfidence

logger = structlog.get_logger(__name__)


class PackingListLineItem:
    """Single line item from packing list."""

    def __init__(
        self,
        product_code: Optional[str] = None,
        product_name: Optional[str] = None,
        pallets: int = 0,
        cartons: int = 0,
        m2_total: Decimal = Decimal("0"),
        net_weight_kg: Decimal = Decimal("0"),
        gross_weight_kg: Decimal = Decimal("0"),
        volume_m3: Decimal = Decimal("0"),
        container_number: Optional[str] = None,
        seal_number: Optional[str] = None,
    ):
        self.product_code = product_code
        self.product_name = product_name
        self.pallets = pallets
        self.cartons = cartons
        self.m2_total = m2_total
        self.net_weight_kg = net_weight_kg
        self.gross_weight_kg = gross_weight_kg
        self.volume_m3 = volume_m3
        self.container_number = container_number
        self.seal_number = seal_number

    def to_dict(self) -> dict:
        return {
            "product_code": self.product_code,
            "product_name": self.product_name,
            "pallets": self.pallets,
            "cartons": self.cartons,
            "m2_total": str(self.m2_total),
            "net_weight_kg": str(self.net_weight_kg),
            "gross_weight_kg": str(self.gross_weight_kg),
            "volume_m3": str(self.volume_m3),
            "container_number": self.container_number,
            "seal_number": self.seal_number,
        }


class PackingListTotals:
    """Aggregated totals from packing list."""

    def __init__(
        self,
        total_pallets: int = 0,
        total_cartons: int = 0,
        total_m2: Decimal = Decimal("0"),
        total_net_weight_kg: Decimal = Decimal("0"),
        total_gross_weight_kg: Decimal = Decimal("0"),
        total_volume_m3: Decimal = Decimal("0"),
    ):
        self.total_pallets = total_pallets
        self.total_cartons = total_cartons
        self.total_m2 = total_m2
        self.total_net_weight_kg = total_net_weight_kg
        self.total_gross_weight_kg = total_gross_weight_kg
        self.total_volume_m3 = total_volume_m3

    def to_dict(self) -> dict:
        return {
            "total_pallets": self.total_pallets,
            "total_cartons": self.total_cartons,
            "total_m2": str(self.total_m2),
            "total_net_weight_kg": str(self.total_net_weight_kg),
            "total_gross_weight_kg": str(self.total_gross_weight_kg),
            "total_volume_m3": str(self.total_volume_m3),
        }


class ParsedPackingList:
    """Complete parsed packing list data."""

    def __init__(
        self,
        pv_number: Optional[str] = None,
        pv_number_confidence: float = 0.0,
        customer_name: Optional[str] = None,
        line_items: Optional[list[PackingListLineItem]] = None,
        totals: Optional[PackingListTotals] = None,
        containers: Optional[list[str]] = None,
        overall_confidence: float = 0.0,
        parsing_errors: Optional[list[str]] = None,
    ):
        self.pv_number = pv_number
        self.pv_number_confidence = pv_number_confidence
        self.customer_name = customer_name
        self.line_items = line_items or []
        self.totals = totals or PackingListTotals()
        self.containers = containers or []
        self.overall_confidence = overall_confidence
        self.parsing_errors = parsing_errors or []

    def to_dict(self) -> dict:
        return {
            "pv_number": self.pv_number,
            "pv_number_confidence": self.pv_number_confidence,
            "customer_name": self.customer_name,
            "line_items": [item.to_dict() for item in self.line_items],
            "totals": self.totals.to_dict(),
            "containers": self.containers,
            "overall_confidence": self.overall_confidence,
            "parsing_errors": self.parsing_errors,
        }


class PackingListParserService:
    """
    Parses Excel packing lists from the factory.

    Expected structure (LISTA sheet):
    - Row 8: Headers
    - Rows 9+: Line items (until "Total" row)
    - Row with "Total": Totals row
    - Row with "ORDER NUMBER": PV number
    - Row with "CUSTOMER": Customer name
    """

    # Column indices (0-based)
    COL_CTU = 1
    COL_PRODUCT_CODE = 2
    COL_ITEM = 3
    COL_BRAND = 4
    COL_PRODUCT_NAME = 5
    COL_TONE_SIZE = 6
    COL_FORMAT = 7
    COL_PALLETS = 8
    COL_CARTONS_PER_PALLET = 9
    COL_TOTAL_CARTONS = 10
    COL_M2_TOTAL = 11
    COL_NET_WEIGHT = 12
    COL_GROSS_WEIGHT = 13
    COL_VOLUME_M3 = 14
    COL_CONTAINER = 19
    COL_SEAL = 20

    # Sheet name
    SHEET_NAME = "LISTA"

    # Row markers
    HEADER_ROW = 8
    DATA_START_ROW = 9

    def __init__(self):
        self.logger = logger.bind(service="packing_list_parser")

    def parse(self, file_content: bytes, filename: str) -> ParsedPackingList:
        """
        Parse packing list Excel file.

        Args:
            file_content: Raw bytes of xlsx file
            filename: Original filename (for logging)

        Returns:
            ParsedPackingList with extracted data
        """
        self.logger.info("parsing_packing_list", filename=filename, size=len(file_content))

        result = ParsedPackingList()
        errors = []

        try:
            # Load Excel file
            xlsx = pd.ExcelFile(BytesIO(file_content))

            # Check for LISTA sheet
            if self.SHEET_NAME not in xlsx.sheet_names:
                self.logger.warning(
                    "sheet_not_found",
                    expected=self.SHEET_NAME,
                    available=xlsx.sheet_names
                )
                # Try first sheet as fallback
                sheet_name = xlsx.sheet_names[0]
                errors.append(f"Sheet '{self.SHEET_NAME}' not found, using '{sheet_name}'")
            else:
                sheet_name = self.SHEET_NAME

            # Read sheet without headers (we'll find them ourselves)
            df = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)

            # Extract PV number
            pv_number, pv_confidence = self._extract_pv_number(df)
            result.pv_number = pv_number
            result.pv_number_confidence = pv_confidence

            # Extract customer name
            result.customer_name = self._extract_customer_name(df)

            # Find data boundaries
            header_row, data_start, totals_row = self._find_data_boundaries(df)

            if data_start is None or totals_row is None:
                errors.append("Could not determine data boundaries")
                result.parsing_errors = errors
                result.overall_confidence = 0.3
                return result

            # Parse line items
            line_items, item_errors = self._parse_line_items(df, data_start, totals_row)
            result.line_items = line_items
            errors.extend(item_errors)

            # Parse totals
            result.totals = self._parse_totals(df, totals_row)

            # Extract unique containers
            containers = set()
            for item in line_items:
                if item.container_number:
                    containers.add(item.container_number)
            result.containers = sorted(list(containers))

            # Calculate overall confidence
            confidence_factors = []
            if result.pv_number:
                confidence_factors.append(pv_confidence)
            if len(result.line_items) > 0:
                confidence_factors.append(0.9)
            if result.totals.total_m2 > 0:
                confidence_factors.append(0.9)
            if len(result.containers) > 0:
                confidence_factors.append(0.85)

            if confidence_factors:
                result.overall_confidence = sum(confidence_factors) / len(confidence_factors)
            else:
                result.overall_confidence = 0.3

            result.parsing_errors = errors

            self.logger.info(
                "packing_list_parsed",
                pv_number=result.pv_number,
                line_items=len(result.line_items),
                containers=len(result.containers),
                total_m2=str(result.totals.total_m2),
                confidence=result.overall_confidence,
                errors=len(errors)
            )

            return result

        except Exception as e:
            self.logger.error("parsing_failed", error=str(e), filename=filename)
            result.parsing_errors = [f"Parsing failed: {str(e)}"]
            result.overall_confidence = 0.0
            return result

    def _extract_pv_number(self, df: pd.DataFrame) -> tuple[Optional[str], float]:
        """Extract PV number from packing list."""
        # Look for "ORDER NUMBER" label followed by PV-XXXXX pattern
        for row_idx in range(15, min(30, len(df))):
            row = df.iloc[row_idx]
            for col_idx in range(len(row)):
                cell_value = str(row.iloc[col_idx]).upper() if pd.notna(row.iloc[col_idx]) else ""
                if "ORDER" in cell_value and "NUMBER" in cell_value:
                    # PV number should be in column 5 or nearby
                    for check_col in range(max(0, col_idx), min(len(row), col_idx + 10)):
                        check_value = str(row.iloc[check_col]) if pd.notna(row.iloc[check_col]) else ""
                        pv_match = re.search(r'PV-?\d+', check_value, re.IGNORECASE)
                        if pv_match:
                            pv_number = pv_match.group(0).upper()
                            # Normalize format to PV-XXXXX
                            if not pv_number.startswith("PV-"):
                                pv_number = "PV-" + pv_number[2:]
                            self.logger.debug("pv_found", pv_number=pv_number, row=row_idx)
                            return pv_number, 0.95

        # Fallback: Search entire sheet for PV pattern
        for row_idx in range(len(df)):
            for col_idx in range(len(df.columns)):
                cell_value = str(df.iloc[row_idx, col_idx]) if pd.notna(df.iloc[row_idx, col_idx]) else ""
                pv_match = re.search(r'PV-?\d{4,6}', cell_value, re.IGNORECASE)
                if pv_match:
                    pv_number = pv_match.group(0).upper()
                    if not pv_number.startswith("PV-"):
                        pv_number = "PV-" + pv_number[2:]
                    self.logger.debug("pv_found_fallback", pv_number=pv_number, row=row_idx, col=col_idx)
                    return pv_number, 0.7

        return None, 0.0

    def _extract_customer_name(self, df: pd.DataFrame) -> Optional[str]:
        """Extract customer name from packing list."""
        # Look for "CUSTOMER" label
        for row_idx in range(15, min(35, len(df))):
            row = df.iloc[row_idx]
            for col_idx in range(len(row)):
                cell_value = str(row.iloc[col_idx]).upper() if pd.notna(row.iloc[col_idx]) else ""
                if "CUSTOMER" in cell_value or "CLIENTE" in cell_value:
                    # Customer name should be in column 5 or nearby
                    for check_col in range(max(0, col_idx), min(len(row), col_idx + 10)):
                        check_value = str(row.iloc[check_col]) if pd.notna(row.iloc[check_col]) else ""
                        # Skip if it's the label itself or a short value
                        if len(check_value) > 10 and "CUSTOMER" not in check_value.upper() and "CLIENTE" not in check_value.upper():
                            return check_value.strip()
        return None

    def _find_data_boundaries(self, df: pd.DataFrame) -> tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Find header row, data start, and totals row.

        Returns:
            (header_row, data_start_row, totals_row)
        """
        header_row = None
        totals_row = None

        # Find header row (look for "PALLETS" or "CARTON" in row, but also "CÓDIGO" or "CTU")
        for row_idx in range(5, 15):
            row = df.iloc[row_idx]
            row_text = " ".join([str(v).upper() for v in row if pd.notna(v)])
            # Header row has both data column headers and info column headers
            if ("PALLET" in row_text or "CARTON" in row_text) and ("CTU" in row_text or "CÓDIGO" in row_text or "CODIGO" in row_text):
                header_row = row_idx
                break
            # Fallback: just look for PALLETS/CARTONS
            if "PALLET" in row_text and "CARTON" in row_text:
                header_row = row_idx
                break

        # Find totals row (look for "Total" label)
        for row_idx in range(10, min(50, len(df))):
            row = df.iloc[row_idx]
            for col_idx in range(len(row)):
                cell_value = str(row.iloc[col_idx]) if pd.notna(row.iloc[col_idx]) else ""
                if cell_value.strip().lower() == "total":
                    totals_row = row_idx
                    break
            if totals_row:
                break

        # Data starts 2 rows after header (skip the column label row)
        # Row structure: Row 7 = sub-headers (PALLETS, CARTONS...), Row 8 = column labels (CTU, CÓDIGO...), Row 9 = first data
        data_start = header_row + 2 if header_row is not None else self.DATA_START_ROW

        self.logger.debug(
            "data_boundaries",
            header_row=header_row,
            data_start=data_start,
            totals_row=totals_row
        )

        return header_row, data_start, totals_row

    def _parse_line_items(
        self, df: pd.DataFrame, data_start: int, totals_row: int
    ) -> tuple[list[PackingListLineItem], list[str]]:
        """Parse line items from data rows."""
        items = []
        errors = []
        current_container = None
        current_seal = None

        for row_idx in range(data_start, totals_row):
            row = df.iloc[row_idx]

            # Check if row has meaningful data (product code or m2)
            product_code = self._safe_str(row.iloc[self.COL_PRODUCT_CODE])
            m2_value = self._safe_decimal(row.iloc[self.COL_M2_TOTAL])

            if not product_code and m2_value == 0:
                continue  # Skip empty rows

            # Update container/seal if present (some rows share container with previous)
            container = self._extract_container_number(row.iloc[self.COL_CONTAINER])
            seal = self._safe_str(row.iloc[self.COL_SEAL])

            if container:
                current_container = container
            if seal:
                current_seal = seal

            item = PackingListLineItem(
                product_code=product_code,
                product_name=self._safe_str(row.iloc[self.COL_PRODUCT_NAME]),
                pallets=self._safe_int(row.iloc[self.COL_PALLETS]),
                cartons=self._safe_int(row.iloc[self.COL_TOTAL_CARTONS]),
                m2_total=m2_value,
                net_weight_kg=self._safe_decimal(row.iloc[self.COL_NET_WEIGHT]),
                gross_weight_kg=self._safe_decimal(row.iloc[self.COL_GROSS_WEIGHT]),
                volume_m3=self._safe_decimal(row.iloc[self.COL_VOLUME_M3]),
                container_number=current_container,
                seal_number=current_seal,
            )

            items.append(item)

        self.logger.debug("line_items_parsed", count=len(items))
        return items, errors

    def _parse_totals(self, df: pd.DataFrame, totals_row: int) -> PackingListTotals:
        """Parse totals from totals row."""
        row = df.iloc[totals_row]

        return PackingListTotals(
            total_pallets=self._safe_int(row.iloc[self.COL_PALLETS]),
            total_cartons=self._safe_int(row.iloc[self.COL_TOTAL_CARTONS]),
            total_m2=self._safe_decimal(row.iloc[self.COL_M2_TOTAL]),
            total_net_weight_kg=self._safe_decimal(row.iloc[self.COL_NET_WEIGHT]),
            total_gross_weight_kg=self._safe_decimal(row.iloc[self.COL_GROSS_WEIGHT]),
            total_volume_m3=self._safe_decimal(row.iloc[self.COL_VOLUME_M3]),
        )

    def _extract_container_number(self, value) -> Optional[str]:
        """Extract and normalize container number."""
        if pd.isna(value):
            return None
        text = str(value).strip()
        if not text:
            return None

        # Skip header text
        skip_words = ["CONTENEDOR", "CONTAINER", "SELLO", "SEAL"]
        if text.upper() in skip_words:
            return None

        # Container format: XXXX NNNNNN-N or XXXXNNNNNNN
        # Standard ISO container number: 4 letters + 6 digits + check digit
        # Normalize to XXXX NNNNNN-N format
        match = re.search(r'([A-Z]{4})\s*(\d{6})-?(\d)', text, re.IGNORECASE)
        if match:
            return f"{match.group(1).upper()} {match.group(2)}-{match.group(3)}"

        # Return as-is if it looks like a container number (4 letters followed by digits)
        if len(text) >= 10 and re.match(r'[A-Z]{4}\s*\d', text, re.IGNORECASE):
            return text.upper()

        return None

    def _safe_str(self, value) -> Optional[str]:
        """Safely convert value to string."""
        if pd.isna(value):
            return None
        text = str(value).strip()
        return text if text else None

    def _safe_int(self, value) -> int:
        """Safely convert value to int."""
        if pd.isna(value):
            return 0
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def _safe_decimal(self, value) -> Decimal:
        """Safely convert value to Decimal."""
        if pd.isna(value):
            return Decimal("0")
        try:
            return Decimal(str(float(value))).quantize(Decimal("0.01"))
        except (ValueError, TypeError, Exception):
            return Decimal("0")


# Singleton instance
_packing_list_parser_service: Optional[PackingListParserService] = None


def get_packing_list_parser_service() -> PackingListParserService:
    """Get singleton packing list parser service instance."""
    global _packing_list_parser_service
    if _packing_list_parser_service is None:
        _packing_list_parser_service = PackingListParserService()
    return _packing_list_parser_service
