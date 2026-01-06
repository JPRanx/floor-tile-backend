"""
Export service — Generate factory order Excel files.

Converts Order Builder selections into the factory's expected format.
"""

import re
from datetime import date
from decimal import Decimal
from io import BytesIO
from math import ceil
from typing import Optional

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side
import structlog

logger = structlog.get_logger(__name__)

# Factory constants (must match factory expectations)
M2_PER_PALLET_FACTORY = Decimal("134.4")
PALLETS_PER_CONTAINER = 14

# Spanish month names
MONTHS_ES = {
    1: "ENERO",
    2: "FEBRERO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE",
}


def normalize_sku_for_factory(sku: str) -> str:
    """
    Convert system SKU to factory format.

    'ALMENDRO BEIGE BTE (T) 51X51-1' -> 'ALMENDRO BEIGE'
    'CEIBA GRIS CLARO BTE' -> 'CEIBA GRIS CLARO'
    'NOGAL CAFE BTE' -> 'NOGAL CAFE'
    """
    if not sku:
        return ""

    # Remove common suffixes in order
    sku = re.sub(r"\s*\(T\)", "", sku)  # Remove (T)
    sku = re.sub(r"\s*BTE\b", "", sku, flags=re.IGNORECASE)  # Remove BTE
    sku = re.sub(r"\s*\d+X\d+.*$", "", sku)  # Remove 51X51-1 and anything after
    sku = re.sub(r"\s*-\d+$", "", sku)  # Remove trailing -1, -2, etc.

    return sku.strip()


def calculate_m2(pallets: int) -> Decimal:
    """Calculate m2 from pallets using factory constant."""
    return Decimal(pallets) * M2_PER_PALLET_FACTORY


def calculate_containers(total_pallets: int) -> int:
    """Calculate number of containers needed."""
    if total_pallets <= 0:
        return 0
    return ceil(total_pallets / PALLETS_PER_CONTAINER)


def get_production_month(boat_departure: date) -> str:
    """
    Get the production month name in Spanish.

    Production is typically for the month after boat departure.
    """
    # Add one month for production planning
    month = boat_departure.month + 1
    year = boat_departure.year

    if month > 12:
        month = 1
        year += 1

    return MONTHS_ES[month]


class ExportService:
    """Service for generating factory export files."""

    def generate_factory_order_excel(
        self,
        products: list[dict],
        boat_departure: date,
        order_date: Optional[date] = None,
    ) -> BytesIO:
        """
        Generate Excel file for factory order.

        Args:
            products: List of dicts with 'sku' and 'pallets' keys
            boat_departure: Boat departure date (used to calculate production month)
            order_date: Order date (defaults to today)

        Returns:
            BytesIO containing the Excel file
        """
        if order_date is None:
            order_date = date.today()

        production_month = get_production_month(boat_departure)

        logger.info(
            "generating_factory_order",
            product_count=len(products),
            boat_departure=str(boat_departure),
            production_month=production_month,
        )

        wb = Workbook()
        ws = wb.active
        ws.title = "PEDIDO TARRAGONA"

        # Styles
        bold_font = Font(bold=True)
        header_font = Font(bold=True, size=14)
        thin_border = Border(
            bottom=Side(style="thin", color="000000")
        )

        # Set column widths
        ws.column_dimensions["A"].width = 30
        ws.column_dimensions["B"].width = 12
        ws.column_dimensions["C"].width = 15

        # Row 1: Title
        ws["A1"] = "Pedido Tarragona Guatemala"
        ws["A1"].font = header_font

        # Row 3: Order date
        ws["A3"] = "Fecha de pedido:"
        ws["B3"] = order_date.strftime("%d/%m/%Y")

        # Row 5: Production month
        ws["A5"] = "Fabricacion para:"
        ws["B5"] = production_month
        ws["B5"].font = bold_font

        # Row 7: Column headers
        ws["A7"] = "Referencia"
        ws["B7"] = "Formato"
        ws["C7"] = "M2 solicitados"
        ws["A7"].font = bold_font
        ws["B7"].font = bold_font
        ws["C7"].font = bold_font
        ws["A7"].border = thin_border
        ws["B7"].border = thin_border
        ws["C7"].border = thin_border

        # Products (starting row 8)
        row = 8
        total_m2 = Decimal("0")
        total_pallets = 0

        for product in products:
            sku = product.get("sku", "")
            pallets = product.get("pallets", 0)

            if pallets <= 0:
                continue

            referencia = normalize_sku_for_factory(sku)
            formato = "51X51"  # Default format
            m2 = calculate_m2(pallets)

            ws[f"A{row}"] = referencia
            ws[f"B{row}"] = formato
            ws[f"C{row}"] = float(m2)
            ws[f"C{row}"].number_format = "#,##0.0"

            total_m2 += m2
            total_pallets += pallets
            row += 1

        # Empty row
        row += 1

        # Total row
        ws[f"A{row}"] = "TOTAL"
        ws[f"A{row}"].font = bold_font
        ws[f"C{row}"] = float(total_m2)
        ws[f"C{row}"].font = bold_font
        ws[f"C{row}"].number_format = "#,##0.0"
        ws[f"A{row}"].border = thin_border
        ws[f"B{row}"].border = thin_border
        ws[f"C{row}"].border = thin_border

        # Container count
        row += 2
        containers = calculate_containers(total_pallets)
        ws[f"A{row}"] = f"{containers} CONTENEDORES"
        ws[f"A{row}"].font = bold_font

        logger.info(
            "factory_order_generated",
            total_products=len(products),
            total_m2=float(total_m2),
            total_pallets=total_pallets,
            containers=containers,
        )

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return output


# Singleton instance
_export_service: Optional[ExportService] = None


def get_export_service() -> ExportService:
    """Get or create ExportService instance."""
    global _export_service
    if _export_service is None:
        _export_service = ExportService()
    return _export_service
