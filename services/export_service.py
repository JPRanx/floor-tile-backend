"""
Export service — Generate factory order Excel files.

Converts Order Builder selections into the factory's expected format.
Also generates BL allocation reports for customs safety spreading.
"""

import re
from datetime import date
from decimal import Decimal
from io import BytesIO
from math import ceil
from typing import List, Optional

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
import structlog

from models.bl_allocation import BLAllocationReport
from models.order_builder import (
    OrderBuilderResponse,
    OrderBuilderProduct,
    AddToProductionItem,
    FactoryRequestItem,
)

logger = structlog.get_logger(__name__)

# Factory constants (actual factory pallet dimensions)
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
            ws[f"C{row}"] = round(float(m2))
            ws[f"C{row}"].number_format = "#,##0"

            total_m2 += m2
            total_pallets += pallets
            row += 1

        # Empty row
        row += 1

        # Total row
        ws[f"A{row}"] = "TOTAL"
        ws[f"A{row}"].font = bold_font
        ws[f"C{row}"] = round(float(total_m2))
        ws[f"C{row}"].font = bold_font
        ws[f"C{row}"].number_format = "#,##0"
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

    def generate_bl_allocation_excel(
        self,
        report: BLAllocationReport,
    ) -> BytesIO:
        """
        Generate Excel file for BL allocation report.

        Creates:
        - Summary sheet with risk distribution
        - One sheet per BL with product details

        Args:
            report: BLAllocationReport with allocation data

        Returns:
            BytesIO containing the Excel file
        """
        logger.info(
            "generating_bl_allocation_excel",
            num_bls=report.num_bls,
            total_products=sum(len(bl.products) for bl in report.allocations),
            total_critical=report.total_critical_products,
        )

        wb = Workbook()

        # Styles
        bold_font = Font(bold=True)
        title_font = Font(bold=True, size=14)
        header_font = Font(bold=True, size=11)
        thin_border = Border(
            bottom=Side(style="thin", color="000000")
        )
        critical_fill = PatternFill(start_color="FFE0E0", end_color="FFE0E0", fill_type="solid")
        header_fill = PatternFill(start_color="E0E8FF", end_color="E0E8FF", fill_type="solid")
        success_fill = PatternFill(start_color="E0FFE0", end_color="E0FFE0", fill_type="solid")
        warning_fill = PatternFill(start_color="FFF0E0", end_color="FFF0E0", fill_type="solid")

        # ===== SUMMARY SHEET =====
        ws_summary = wb.active
        ws_summary.title = "Summary"

        # Column widths
        ws_summary.column_dimensions["A"].width = 35
        ws_summary.column_dimensions["B"].width = 20
        ws_summary.column_dimensions["C"].width = 15
        ws_summary.column_dimensions["D"].width = 15

        row = 1

        # Title
        ws_summary[f"A{row}"] = "BL ALLOCATION REPORT"
        ws_summary[f"A{row}"].font = title_font
        row += 2

        # Metadata
        ws_summary[f"A{row}"] = "Generated:"
        ws_summary[f"B{row}"] = report.generated_at.strftime("%Y-%m-%d %H:%M")
        row += 1
        ws_summary[f"A{row}"] = "Boat:"
        ws_summary[f"B{row}"] = report.boat_name
        row += 1
        ws_summary[f"A{row}"] = "Departure:"
        ws_summary[f"B{row}"] = report.boat_departure.strftime("%Y-%m-%d")
        row += 2

        # Totals
        ws_summary[f"A{row}"] = "TOTALS"
        ws_summary[f"A{row}"].font = header_font
        ws_summary[f"A{row}"].fill = header_fill
        ws_summary[f"B{row}"].fill = header_fill
        row += 1

        ws_summary[f"A{row}"] = "Total BLs:"
        ws_summary[f"B{row}"] = report.num_bls
        row += 1
        ws_summary[f"A{row}"] = "Total Containers:"
        ws_summary[f"B{row}"] = report.total_containers
        row += 1
        ws_summary[f"A{row}"] = "Total Pallets:"
        ws_summary[f"B{row}"] = report.total_pallets
        row += 1
        ws_summary[f"A{row}"] = "Total M2:"
        ws_summary[f"B{row}"] = round(float(report.total_m2))
        ws_summary[f"B{row}"].number_format = "#,##0"
        row += 1
        ws_summary[f"A{row}"] = "Total Weight (kg):"
        ws_summary[f"B{row}"] = round(float(report.total_weight_kg))
        ws_summary[f"B{row}"].number_format = "#,##0"
        row += 2

        # Risk Distribution
        ws_summary[f"A{row}"] = "RISK DISTRIBUTION"
        ws_summary[f"A{row}"].font = header_font
        if report.risk_distribution_even:
            ws_summary[f"A{row}"].fill = success_fill
            ws_summary[f"B{row}"].fill = success_fill
        else:
            ws_summary[f"A{row}"].fill = warning_fill
            ws_summary[f"B{row}"].fill = warning_fill
        row += 1

        ws_summary[f"A{row}"] = "Total Critical Products:"
        ws_summary[f"B{row}"] = report.total_critical_products
        row += 1

        # Distribution per BL
        for bl in report.allocations:
            if report.total_critical_products > 0:
                pct = (bl.critical_product_count / report.total_critical_products) * 100
            else:
                pct = 0
            ws_summary[f"A{row}"] = f"BL {bl.bl_number} Critical:"
            ws_summary[f"B{row}"] = f"{bl.critical_product_count} ({pct:.0f}%)"
            row += 1

        row += 1

        # Risk assessment
        ws_summary[f"A{row}"] = "Risk Status:"
        if report.risk_distribution_even:
            ws_summary[f"B{row}"] = "EVENLY DISTRIBUTED"
            ws_summary[f"B{row}"].font = Font(bold=True, color="006600")
        else:
            ws_summary[f"B{row}"] = f"UNEVEN ({report.max_critical_pct:.0f}% in one BL)"
            ws_summary[f"B{row}"].font = Font(bold=True, color="CC6600")
        row += 1

        ws_summary[f"A{row}"] = "Max delay if BL held:"
        ws_summary[f"B{row}"] = f"{report.max_critical_pct:.0f}% of critical products"
        row += 2

        # Warnings
        if report.warnings:
            ws_summary[f"A{row}"] = "WARNINGS"
            ws_summary[f"A{row}"].font = header_font
            ws_summary[f"A{row}"].fill = warning_fill
            row += 1
            for warning in report.warnings:
                ws_summary[f"A{row}"] = warning
                row += 1
            row += 1

        # BL Overview Table
        ws_summary[f"A{row}"] = "BL OVERVIEW"
        ws_summary[f"A{row}"].font = header_font
        ws_summary[f"A{row}"].fill = header_fill
        row += 1

        # Headers
        ws_summary[f"A{row}"] = "BL"
        ws_summary[f"B{row}"] = "Customers"
        ws_summary[f"C{row}"] = "Containers"
        ws_summary[f"D{row}"] = "Critical"
        for col in ["A", "B", "C", "D"]:
            ws_summary[f"{col}{row}"].font = bold_font
            ws_summary[f"{col}{row}"].border = thin_border
        row += 1

        for bl in report.allocations:
            customers = ", ".join(bl.primary_customers[:3])
            if len(bl.primary_customers) > 3:
                customers += f" +{len(bl.primary_customers) - 3}"
            ws_summary[f"A{row}"] = f"BL {bl.bl_number}"
            ws_summary[f"B{row}"] = customers or "General Stock"
            ws_summary[f"C{row}"] = bl.total_containers
            ws_summary[f"D{row}"] = bl.critical_product_count
            row += 1

        # ===== PER-BL SHEETS =====
        for bl in report.allocations:
            ws = wb.create_sheet(title=f"BL {bl.bl_number}")

            # Column widths
            ws.column_dimensions["A"].width = 30
            ws.column_dimensions["B"].width = 10
            ws.column_dimensions["C"].width = 12
            ws.column_dimensions["D"].width = 20
            ws.column_dimensions["E"].width = 10
            ws.column_dimensions["F"].width = 10

            row = 1

            # BL Header
            customers_str = ", ".join(bl.primary_customers[:5])
            if len(bl.primary_customers) > 5:
                customers_str += f" +{len(bl.primary_customers) - 5}"

            ws[f"A{row}"] = f"BL {bl.bl_number} — Serving: {customers_str or 'General Stock'}"
            ws[f"A{row}"].font = title_font
            row += 2

            # BL Stats
            ws[f"A{row}"] = f"Containers: {bl.total_containers}"
            ws[f"B{row}"] = f"Pallets: {bl.total_pallets}"
            ws[f"C{row}"] = f"Critical: {bl.critical_product_count}"
            row += 2

            # Product Headers
            headers = ["Referencia", "Pallets", "M2", "Customer", "Score", "Critical"]
            for i, header in enumerate(headers):
                col = chr(65 + i)  # A, B, C, ...
                ws[f"{col}{row}"] = header
                ws[f"{col}{row}"].font = bold_font
                ws[f"{col}{row}"].border = thin_border
                ws[f"{col}{row}"].fill = header_fill
            row += 1

            # Products
            for product in bl.products:
                ws[f"A{row}"] = normalize_sku_for_factory(product.sku)
                ws[f"B{row}"] = product.pallets
                ws[f"C{row}"] = round(float(product.m2))
                ws[f"C{row}"].number_format = "#,##0"
                ws[f"D{row}"] = product.primary_customer or "(General)"
                ws[f"E{row}"] = product.score

                if product.is_critical:
                    ws[f"F{row}"] = "YES"
                    ws[f"F{row}"].font = Font(bold=True, color="CC0000")
                    # Highlight entire row
                    for col in ["A", "B", "C", "D", "E", "F"]:
                        ws[f"{col}{row}"].fill = critical_fill
                else:
                    ws[f"F{row}"] = ""

                row += 1

            # Subtotal
            row += 1
            ws[f"A{row}"] = "SUBTOTAL"
            ws[f"A{row}"].font = bold_font
            ws[f"B{row}"] = bl.total_pallets
            ws[f"B{row}"].font = bold_font
            ws[f"C{row}"] = round(float(bl.total_m2))
            ws[f"C{row}"].font = bold_font
            ws[f"C{row}"].number_format = "#,##0"
            for col in ["A", "B", "C"]:
                ws[f"{col}{row}"].border = thin_border

        logger.info(
            "bl_allocation_excel_generated",
            num_bls=report.num_bls,
            total_containers=report.total_containers,
        )

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return output

    def generate_order_report(
        self,
        response: OrderBuilderResponse,
        selected_warehouse_products: List[OrderBuilderProduct],
        selected_add_items: List[AddToProductionItem],
        selected_factory_items: List[FactoryRequestItem],
    ) -> BytesIO:
        """
        Generate comprehensive Order Builder report explaining WHY recommendations were made.

        Creates Excel with 6 sheets:
        1. EXECUTIVE SUMMARY - High-level order overview
        2. SHIP NOW - Warehouse order details
        3. ADD TO PRODUCTION - Items to add to scheduled production
        4. FACTORY REQUEST - New production requests
        5. WHY SHIP NOW - Reasoning for each ship now item
        6. WHY ADD / REQUEST - Reasoning for add/factory items

        Args:
            response: Full OrderBuilderResponse from the API
            selected_warehouse_products: Products selected for warehouse order
            selected_add_items: Items selected for add to production
            selected_factory_items: Items selected for factory request

        Returns:
            BytesIO containing the Excel file
        """
        logger.info(
            "generating_order_report",
            warehouse_count=len(selected_warehouse_products),
            add_count=len(selected_add_items),
            factory_count=len(selected_factory_items),
        )

        wb = Workbook()

        # Styles
        bold_font = Font(bold=True)
        title_font = Font(bold=True, size=16)
        section_font = Font(bold=True, size=12)
        thin_border = Border(bottom=Side(style="thin", color="000000"))
        header_fill = PatternFill(start_color="E0E8FF", end_color="E0E8FF", fill_type="solid")
        critical_fill = PatternFill(start_color="FFE0E0", end_color="FFE0E0", fill_type="solid")
        urgent_fill = PatternFill(start_color="FFF0E0", end_color="FFF0E0", fill_type="solid")
        ok_fill = PatternFill(start_color="E0FFE0", end_color="E0FFE0", fill_type="solid")

        # ===== SHEET 1: EXECUTIVE SUMMARY =====
        ws_summary = wb.active
        ws_summary.title = "EXECUTIVE SUMMARY"

        ws_summary.column_dimensions["A"].width = 40
        ws_summary.column_dimensions["B"].width = 25
        ws_summary.column_dimensions["C"].width = 25

        row = 1
        ws_summary[f"A{row}"] = "ORDER BUILDER REPORT"
        ws_summary[f"A{row}"].font = title_font
        row += 2

        # Boat info
        ws_summary[f"A{row}"] = "Target Boat:"
        ws_summary[f"B{row}"] = response.boat.name
        row += 1
        ws_summary[f"A{row}"] = "Departure Date:"
        ws_summary[f"B{row}"] = response.boat.departure_date.strftime("%Y-%m-%d")
        row += 1
        ws_summary[f"A{row}"] = "Days Until Departure:"
        ws_summary[f"B{row}"] = response.boat.days_until_departure
        row += 1
        ws_summary[f"A{row}"] = "Booking Deadline:"
        ws_summary[f"B{row}"] = response.boat.booking_deadline.strftime("%Y-%m-%d")
        row += 1
        ws_summary[f"A{row}"] = "Days Until Deadline:"
        ws_summary[f"B{row}"] = response.boat.days_until_deadline
        if response.boat.days_until_deadline <= 7:
            ws_summary[f"B{row}"].fill = urgent_fill
        row += 2

        # Summary metrics
        ws_summary[f"A{row}"] = "ORDER SUMMARY"
        ws_summary[f"A{row}"].font = section_font
        ws_summary[f"A{row}"].fill = header_fill
        ws_summary[f"B{row}"].fill = header_fill
        row += 1

        # Ship Now totals
        ship_now_pallets = sum(p.selected_pallets for p in selected_warehouse_products)
        ship_now_m2 = Decimal(str(ship_now_pallets)) * M2_PER_PALLET_FACTORY
        ws_summary[f"A{row}"] = "Ship Now (Warehouse Order):"
        ws_summary[f"B{row}"] = f"{ship_now_m2:,.0f} m² ({ship_now_pallets} pallets)"
        row += 1

        # Add to Production totals
        add_pallets = sum(item.suggested_additional_pallets for item in selected_add_items)
        add_m2 = sum(item.suggested_additional_m2 for item in selected_add_items)
        ws_summary[f"A{row}"] = "Add to Production:"
        ws_summary[f"B{row}"] = f"{add_m2:,.0f} m² ({add_pallets} pallets)"
        row += 1

        # Factory Request totals
        factory_pallets = sum(item.request_pallets for item in selected_factory_items)
        factory_m2 = sum(item.request_m2 for item in selected_factory_items)
        ws_summary[f"A{row}"] = "Factory Request:"
        ws_summary[f"B{row}"] = f"{factory_m2:,.0f} m² ({factory_pallets} pallets)"
        row += 2

        # BL info
        ws_summary[f"A{row}"] = "BL ALLOCATION"
        ws_summary[f"A{row}"].font = section_font
        ws_summary[f"A{row}"].fill = header_fill
        ws_summary[f"B{row}"].fill = header_fill
        row += 1
        ws_summary[f"A{row}"] = "Selected BLs:"
        ws_summary[f"B{row}"] = response.num_bls
        row += 1
        ws_summary[f"A{row}"] = "Recommended BLs (Need):"
        ws_summary[f"B{row}"] = response.recommended_bls
        row += 1
        ws_summary[f"A{row}"] = "Available BLs (Stock):"
        ws_summary[f"B{row}"] = response.available_bls
        row += 1
        ws_summary[f"A{row}"] = "BL Capacity:"
        ws_summary[f"B{row}"] = f"{response.num_bls * 70} pallets ({response.num_bls * 5} containers)"
        row += 2

        # Urgency breakdown
        ws_summary[f"A{row}"] = "URGENCY BREAKDOWN"
        ws_summary[f"A{row}"].font = section_font
        ws_summary[f"A{row}"].fill = header_fill
        ws_summary[f"B{row}"].fill = header_fill
        row += 1

        critical_count = len([p for p in selected_warehouse_products if p.score and p.score.total >= 85])
        urgent_count = len([p for p in selected_warehouse_products if p.score and 70 <= p.score.total < 85])
        other_count = len(selected_warehouse_products) - critical_count - urgent_count

        ws_summary[f"A{row}"] = "Critical (Score ≥ 85):"
        ws_summary[f"B{row}"] = critical_count
        ws_summary[f"B{row}"].fill = critical_fill
        row += 1
        ws_summary[f"A{row}"] = "Urgent (Score 70-84):"
        ws_summary[f"B{row}"] = urgent_count
        ws_summary[f"B{row}"].fill = urgent_fill
        row += 1
        ws_summary[f"A{row}"] = "Other:"
        ws_summary[f"B{row}"] = other_count
        row += 2

        # Key actions
        ws_summary[f"A{row}"] = "KEY ACTIONS"
        ws_summary[f"A{row}"].font = section_font
        ws_summary[f"A{row}"].fill = header_fill
        ws_summary[f"B{row}"].fill = header_fill
        row += 1

        if response.add_to_production_summary and response.add_to_production_summary.action_deadline_display:
            ws_summary[f"A{row}"] = "Add to Production Deadline:"
            ws_summary[f"B{row}"] = response.add_to_production_summary.action_deadline_display
            ws_summary[f"B{row}"].fill = urgent_fill
            row += 1

        if response.factory_request_summary and response.factory_request_summary.submit_deadline_display:
            ws_summary[f"A{row}"] = "Factory Request Deadline:"
            ws_summary[f"B{row}"] = response.factory_request_summary.submit_deadline_display
            row += 1

        # ===== SHEET 2: SHIP NOW =====
        ws_ship = wb.create_sheet(title="SHIP NOW")
        ws_ship.column_dimensions["A"].width = 30
        ws_ship.column_dimensions["B"].width = 12
        ws_ship.column_dimensions["C"].width = 12
        ws_ship.column_dimensions["D"].width = 15
        ws_ship.column_dimensions["E"].width = 12
        ws_ship.column_dimensions["F"].width = 15

        row = 1
        ws_ship[f"A{row}"] = "SHIP NOW - WAREHOUSE ORDER"
        ws_ship[f"A{row}"].font = title_font
        row += 2

        # Headers
        headers = ["SKU", "Pallets", "M²", "SIESA Stock", "Score", "Urgency"]
        for i, header in enumerate(headers):
            col = chr(65 + i)
            ws_ship[f"{col}{row}"] = header
            ws_ship[f"{col}{row}"].font = bold_font
            ws_ship[f"{col}{row}"].border = thin_border
            ws_ship[f"{col}{row}"].fill = header_fill
        row += 1

        # Products
        for p in selected_warehouse_products:
            ws_ship[f"A{row}"] = p.sku
            ws_ship[f"B{row}"] = p.selected_pallets
            ws_ship[f"C{row}"] = round(float(Decimal(p.selected_pallets) * M2_PER_PALLET_FACTORY))
            ws_ship[f"C{row}"].number_format = "#,##0"
            ws_ship[f"D{row}"] = round(float(p.factory_available_m2))
            ws_ship[f"D{row}"].number_format = "#,##0"
            ws_ship[f"E{row}"] = p.score.total if p.score else 0
            ws_ship[f"F{row}"] = p.urgency.upper()

            # Color by urgency
            if p.urgency == "critical":
                for col in ["A", "B", "C", "D", "E", "F"]:
                    ws_ship[f"{col}{row}"].fill = critical_fill
            elif p.urgency == "urgent":
                for col in ["A", "B", "C", "D", "E", "F"]:
                    ws_ship[f"{col}{row}"].fill = urgent_fill

            row += 1

        # Totals
        row += 1
        ws_ship[f"A{row}"] = "TOTAL"
        ws_ship[f"A{row}"].font = bold_font
        ws_ship[f"B{row}"] = ship_now_pallets
        ws_ship[f"B{row}"].font = bold_font
        ws_ship[f"C{row}"] = round(float(ship_now_m2))
        ws_ship[f"C{row}"].font = bold_font
        ws_ship[f"C{row}"].number_format = "#,##0"

        # ===== SHEET 3: ADD TO PRODUCTION =====
        ws_add = wb.create_sheet(title="ADD TO PRODUCTION")
        ws_add.column_dimensions["A"].width = 30
        ws_add.column_dimensions["B"].width = 15
        ws_add.column_dimensions["C"].width = 15
        ws_add.column_dimensions["D"].width = 15
        ws_add.column_dimensions["E"].width = 15
        ws_add.column_dimensions["F"].width = 12

        row = 1
        ws_add[f"A{row}"] = "ADD TO PRODUCTION"
        ws_add[f"A{row}"].font = title_font
        row += 1

        if response.add_to_production_summary and response.add_to_production_summary.action_deadline_display:
            ws_add[f"A{row}"] = f"⚠️ ACTION REQUIRED - {response.add_to_production_summary.action_deadline_display}"
            ws_add[f"A{row}"].font = Font(bold=True, color="CC6600")
        row += 2

        # Headers
        headers = ["SKU", "Current Sched.", "Suggested Add", "Total", "Target Boat", "Score"]
        for i, header in enumerate(headers):
            col = chr(65 + i)
            ws_add[f"{col}{row}"] = header
            ws_add[f"{col}{row}"].font = bold_font
            ws_add[f"{col}{row}"].border = thin_border
            ws_add[f"{col}{row}"].fill = header_fill
        row += 1

        # Items
        for item in selected_add_items:
            ws_add[f"A{row}"] = item.sku
            ws_add[f"B{row}"] = round(float(item.current_requested_m2))
            ws_add[f"B{row}"].number_format = "#,##0"
            ws_add[f"C{row}"] = round(float(item.suggested_additional_m2))
            ws_add[f"C{row}"].number_format = "#,##0"
            ws_add[f"D{row}"] = round(float(item.suggested_total_m2))
            ws_add[f"D{row}"].number_format = "#,##0"
            ws_add[f"E{row}"] = item.target_boat or "TBD"
            ws_add[f"F{row}"] = item.score

            if item.is_critical:
                for col in ["A", "B", "C", "D", "E", "F"]:
                    ws_add[f"{col}{row}"].fill = critical_fill

            row += 1

        # Totals
        row += 1
        ws_add[f"A{row}"] = "TOTAL TO ADD"
        ws_add[f"A{row}"].font = bold_font
        ws_add[f"C{row}"] = round(float(add_m2))
        ws_add[f"C{row}"].font = bold_font
        ws_add[f"C{row}"].number_format = "#,##0"

        # ===== SHEET 4: FACTORY REQUEST =====
        ws_factory = wb.create_sheet(title="FACTORY REQUEST")
        ws_factory.column_dimensions["A"].width = 30
        ws_factory.column_dimensions["B"].width = 12
        ws_factory.column_dimensions["C"].width = 12
        ws_factory.column_dimensions["D"].width = 12
        ws_factory.column_dimensions["E"].width = 12
        ws_factory.column_dimensions["F"].width = 12

        row = 1
        ws_factory[f"A{row}"] = "NEW FACTORY REQUEST"
        ws_factory[f"A{row}"].font = title_font
        row += 1

        if response.factory_request_summary and response.factory_request_summary.submit_deadline_display:
            ws_factory[f"A{row}"] = response.factory_request_summary.submit_deadline_display
            ws_factory[f"A{row}"].font = Font(bold=True)
        row += 2

        # Headers
        headers = ["SKU", "Gap M²", "Gap Pallets", "SIESA", "In Transit", "Urgency"]
        for i, header in enumerate(headers):
            col = chr(65 + i)
            ws_factory[f"{col}{row}"] = header
            ws_factory[f"{col}{row}"].font = bold_font
            ws_factory[f"{col}{row}"].border = thin_border
            ws_factory[f"{col}{row}"].fill = header_fill
        row += 1

        # Items
        for item in selected_factory_items:
            ws_factory[f"A{row}"] = item.sku
            ws_factory[f"B{row}"] = round(float(item.gap_m2))
            ws_factory[f"B{row}"].number_format = "#,##0"
            ws_factory[f"C{row}"] = item.gap_pallets
            ws_factory[f"D{row}"] = round(float(item.factory_available_m2))
            ws_factory[f"D{row}"].number_format = "#,##0"
            ws_factory[f"E{row}"] = round(float(item.in_transit_m2))
            ws_factory[f"E{row}"].number_format = "#,##0"
            ws_factory[f"F{row}"] = item.urgency.upper()

            if item.urgency == "critical":
                for col in ["A", "B", "C", "D", "E", "F"]:
                    ws_factory[f"{col}{row}"].fill = critical_fill
            elif item.urgency == "urgent":
                for col in ["A", "B", "C", "D", "E", "F"]:
                    ws_factory[f"{col}{row}"].fill = urgent_fill

            row += 1

        # Totals
        row += 1
        ws_factory[f"A{row}"] = "TOTAL REQUEST"
        ws_factory[f"A{row}"].font = bold_font
        ws_factory[f"B{row}"] = round(float(factory_m2))
        ws_factory[f"B{row}"].font = bold_font
        ws_factory[f"B{row}"].number_format = "#,##0"
        ws_factory[f"C{row}"] = factory_pallets
        ws_factory[f"C{row}"].font = bold_font

        # ===== SHEET 5: WHY SHIP NOW =====
        ws_why_ship = wb.create_sheet(title="WHY SHIP NOW")
        ws_why_ship.column_dimensions["A"].width = 25
        ws_why_ship.column_dimensions["B"].width = 45
        ws_why_ship.column_dimensions["C"].width = 35
        ws_why_ship.column_dimensions["D"].width = 15

        row = 1
        ws_why_ship[f"A{row}"] = "WHY SHIP NOW - REASONING"
        ws_why_ship[f"A{row}"].font = title_font
        row += 2

        # Headers
        headers = ["SKU", "Why This Product", "Why This Quantity", "Dominant Factor"]
        for i, header in enumerate(headers):
            col = chr(65 + i)
            ws_why_ship[f"{col}{row}"] = header
            ws_why_ship[f"{col}{row}"].font = bold_font
            ws_why_ship[f"{col}{row}"].border = thin_border
            ws_why_ship[f"{col}{row}"].fill = header_fill
        row += 1

        # Products with reasoning
        for p in selected_warehouse_products:
            ws_why_ship[f"A{row}"] = p.sku

            if p.reasoning_display:
                ws_why_ship[f"B{row}"] = p.reasoning_display.why_product_sentence
                ws_why_ship[f"C{row}"] = p.reasoning_display.why_quantity_sentence
                ws_why_ship[f"D{row}"] = p.reasoning_display.dominant_factor.upper()
            else:
                # Fallback reasoning
                days = p.days_of_stock or 0
                if days <= 0:
                    ws_why_ship[f"B{row}"] = f"OUT OF STOCK - immediate restock needed"
                elif days <= 14:
                    ws_why_ship[f"B{row}"] = f"LOW STOCK - only {days} days remaining"
                else:
                    ws_why_ship[f"B{row}"] = f"Stock replenishment - {days} days coverage"

                gap_m2 = p.coverage_gap_m2 or 0
                ws_why_ship[f"C{row}"] = f"Gap of {gap_m2:,.0f} m² to cover until next boat"
                ws_why_ship[f"D{row}"] = "STOCKOUT" if days <= 14 else "COVERAGE"

            # Color by urgency
            if p.urgency == "critical":
                for col in ["A", "B", "C", "D"]:
                    ws_why_ship[f"{col}{row}"].fill = critical_fill
            elif p.urgency == "urgent":
                for col in ["A", "B", "C", "D"]:
                    ws_why_ship[f"{col}{row}"].fill = urgent_fill

            row += 1

        # ===== SHEET 6: WHY ADD / REQUEST =====
        ws_why_other = wb.create_sheet(title="WHY ADD-REQUEST")
        ws_why_other.column_dimensions["A"].width = 25
        ws_why_other.column_dimensions["B"].width = 15
        ws_why_other.column_dimensions["C"].width = 50
        ws_why_other.column_dimensions["D"].width = 12

        row = 1
        ws_why_other[f"A{row}"] = "WHY ADD TO PRODUCTION / FACTORY REQUEST"
        ws_why_other[f"A{row}"].font = title_font
        row += 2

        # Headers
        headers = ["SKU", "Type", "Reason", "Score"]
        for i, header in enumerate(headers):
            col = chr(65 + i)
            ws_why_other[f"{col}{row}"] = header
            ws_why_other[f"{col}{row}"].font = bold_font
            ws_why_other[f"{col}{row}"].border = thin_border
            ws_why_other[f"{col}{row}"].fill = header_fill
        row += 1

        # Add to Production items
        for item in selected_add_items:
            ws_why_other[f"A{row}"] = item.sku
            ws_why_other[f"B{row}"] = "ADD"
            ws_why_other[f"B{row}"].fill = urgent_fill

            reason = (
                f"Production already scheduled ({item.current_requested_m2:,.0f} m²). "
                f"Adding {item.suggested_additional_m2:,.0f} m² more to meet demand. "
            )
            if item.target_boat:
                reason += f"Ready for {item.target_boat}."

            ws_why_other[f"C{row}"] = reason
            ws_why_other[f"D{row}"] = item.score

            if item.is_critical:
                ws_why_other[f"A{row}"].fill = critical_fill
                ws_why_other[f"D{row}"].fill = critical_fill

            row += 1

        # Factory Request items
        for item in selected_factory_items:
            ws_why_other[f"A{row}"] = item.sku
            ws_why_other[f"B{row}"] = "REQUEST"

            # Build reason
            sources = []
            if item.factory_available_m2 > 0:
                sources.append(f"SIESA: {item.factory_available_m2:,.0f}")
            if item.in_transit_m2 > 0:
                sources.append(f"In-transit: {item.in_transit_m2:,.0f}")
            if item.in_production_m2 > 0:
                sources.append(f"In-production: {item.in_production_m2:,.0f}")

            reason = f"Not in production schedule. Gap: {item.gap_m2:,.0f} m². "
            if sources:
                reason += f"Available: {', '.join(sources)}. "
            reason += f"Urgency: {item.urgency.upper()}"

            ws_why_other[f"C{row}"] = reason
            ws_why_other[f"D{row}"] = item.score

            if item.urgency == "critical":
                ws_why_other[f"A{row}"].fill = critical_fill
                ws_why_other[f"B{row}"].fill = critical_fill

            row += 1

        logger.info(
            "order_report_generated",
            warehouse_count=len(selected_warehouse_products),
            add_count=len(selected_add_items),
            factory_count=len(selected_factory_items),
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
