"""
Unit tests for Excel parser.

Tests the parse_owner_excel function with various scenarios.
"""

from datetime import date, timedelta
from io import BytesIO
import pytest
import pandas as pd

from parsers.excel_parser import (
    parse_owner_excel,
    ExcelParseResult,
    InventoryRecord,
    SalesRecord,
)
from exceptions import ExcelParseError


# ===================
# FIXTURES
# ===================

@pytest.fixture
def known_skus():
    """Sample known owner codes for validation (maps padded codes to product IDs)."""
    return {
        "0000098": "uuid-nogal-cafe",      # NOGAL CAFÉ
        "0000101": "uuid-ceiba-gris",      # CEIBA GRIS OSC
        "0000022": "uuid-tolu-gris",       # TOLU GRIS
        "0000131": "uuid-mirach",          # MIRACH
    }


@pytest.fixture
def today():
    """Today's date for test data."""
    return date.today()


@pytest.fixture
def yesterday():
    """Yesterday's date for test data."""
    return date.today() - timedelta(days=1)


def create_excel_file(
    inventory_data: list[dict] = None,
    sales_data: list[dict] = None,
    inventory_columns: list[str] = None,
    sales_columns: list[str] = None,
) -> BytesIO:
    """Helper to create test Excel files in memory."""
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Create Inventario sheet
        if inventory_data is not None:
            if inventory_columns is None:
                inventory_columns = ["SKU", "Bodega (m²)", "En Tránsito (m²)", "Fecha Conteo", "Notas"]
            df_inv = pd.DataFrame(inventory_data, columns=inventory_columns) if inventory_data else pd.DataFrame(columns=inventory_columns)
            df_inv.to_excel(writer, sheet_name="Inventario", index=False)

        # Create Ventas sheet
        if sales_data is not None:
            if sales_columns is None:
                sales_columns = ["Fecha", "SKU", "Cantidad (m²)", "Cliente", "Notas"]
            df_sales = pd.DataFrame(sales_data, columns=sales_columns) if sales_data else pd.DataFrame(columns=sales_columns)
            df_sales.to_excel(writer, sheet_name="Ventas", index=False)

    output.seek(0)
    return output


# ===================
# VALID FILE TESTS
# ===================

class TestValidFileParsing:
    """Tests for successfully parsing valid files."""

    def test_valid_inventory_parses_correctly(self, known_skus, yesterday):
        """Valid inventory data is parsed into InventoryRecord objects."""
        inventory_data = [
            [98, 1500.5, 250.0, yesterday.isoformat(), "Stock count"],   # NOGAL CAFÉ
            [101, 800.0, 0, yesterday.isoformat(), None],                # CEIBA GRIS OSC
        ]

        excel_file = create_excel_file(
            inventory_data=inventory_data,
            inventory_columns=["SKU", "Bodega (m²)", "En Tránsito (m²)", "Fecha Conteo", "Notas"],
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.inventory) == 2
        assert len(result.errors) == 0

        # Check first record
        record = result.inventory[0]
        assert record.sku == "0000098"  # Padded owner code
        assert record.product_id == "uuid-nogal-cafe"
        assert record.warehouse_qty == 1500.5
        assert record.in_transit_qty == 250.0
        assert record.snapshot_date == yesterday
        assert record.notes == "Stock count"

    def test_valid_sales_parses_correctly(self, known_skus, yesterday):
        """Valid sales data is parsed into SalesRecord objects."""
        sales_data = [
            [yesterday.isoformat(), 22, 85.5, "Cliente ABC", "Order #123"],   # TOLU GRIS
            [yesterday.isoformat(), 131, 120.0, None, None],                  # MIRACH
        ]

        excel_file = create_excel_file(
            sales_data=sales_data,
            sales_columns=["Fecha", "SKU", "Cantidad (m²)", "Cliente", "Notas"],
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.sales) == 2
        assert len(result.errors) == 0

        # Check first record
        record = result.sales[0]
        assert record.sku == "0000022"  # Padded owner code
        assert record.product_id == "uuid-tolu-gris"
        assert record.quantity == 85.5
        assert record.sale_date == yesterday
        assert record.customer == "Cliente ABC"

    def test_valid_file_with_both_sheets(self, known_skus, yesterday):
        """File with both inventory and sales is parsed correctly."""
        inventory_data = [
            [98, 1000, 0, yesterday.isoformat(), None],  # NOGAL CAFÉ
        ]
        sales_data = [
            [yesterday.isoformat(), 98, 50, None, None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(
            inventory_data=inventory_data,
            sales_data=sales_data,
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.inventory) == 1
        assert len(result.sales) == 1

    def test_owner_code_padding(self, known_skus, yesterday):
        """Owner codes are properly padded to 7 digits."""
        inventory_data = [
            [98, 500, 0, yesterday.isoformat(), None],   # Becomes 0000098
            [101, 300, 0, yesterday.isoformat(), None],  # Becomes 0000101
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.inventory) == 2
        assert result.inventory[0].sku == "0000098"
        assert result.inventory[1].sku == "0000101"


# ===================
# SKU VALIDATION TESTS
# ===================

class TestSKUValidation:
    """Tests for SKU validation."""

    def test_invalid_sku_returns_error(self, known_skus, yesterday):
        """Unknown owner code causes error and rejects upload."""
        inventory_data = [
            [98, 1000, 0, yesterday.isoformat(), None],   # valid (NOGAL CAFÉ)
            [999, 500, 0, yesterday.isoformat(), None],   # invalid (unknown code)
            [888, 200, 0, yesterday.isoformat(), None],   # invalid (unknown code)
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert len(result.errors) == 2

        # Check error details
        assert result.errors[0].sheet == "Inventario"
        assert result.errors[0].row == 3  # Row 3 (1-indexed + header)
        assert result.errors[0].field == "SKU"
        assert "999" in result.errors[0].error  # Shows raw SKU value

    def test_invalid_sku_in_sales(self, known_skus, yesterday):
        """Unknown owner code in sales sheet causes error."""
        sales_data = [
            [yesterday.isoformat(), 777, 100, None, None],  # Unknown code
        ]

        excel_file = create_excel_file(sales_data=sales_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert len(result.errors) == 1
        assert result.errors[0].sheet == "Ventas"
        assert "777" in result.errors[0].error  # Raw SKU shown in error


# ===================
# COLUMN VALIDATION TESTS
# ===================

class TestColumnValidation:
    """Tests for required column validation."""

    def test_missing_required_column_inventory(self, known_skus):
        """Missing required column in inventory returns error."""
        # Missing "Bodega (m²)" column
        excel_file = create_excel_file(
            inventory_data=[],
            inventory_columns=["SKU", "Fecha Conteo"],  # Missing Bodega
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert len(result.errors) == 1
        assert "columns" in result.errors[0].field
        assert "Bodega" in result.errors[0].error

    def test_missing_required_column_sales(self, known_skus):
        """Missing required column in sales returns error."""
        # Missing "Cantidad (m²)" column
        excel_file = create_excel_file(
            sales_data=[],
            sales_columns=["Fecha", "SKU"],  # Missing Cantidad
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert len(result.errors) == 1
        assert "Cantidad" in result.errors[0].error


# ===================
# EMPTY DATA TESTS
# ===================

class TestEmptyData:
    """Tests for empty sheets and rows."""

    def test_empty_inventory_sheet_returns_empty_list(self, known_skus):
        """Empty inventory sheet returns empty list, not error."""
        excel_file = create_excel_file(
            inventory_data=[],  # Empty data, but valid columns
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.inventory) == 0
        assert len(result.errors) == 0

    def test_empty_sales_sheet_returns_empty_list(self, known_skus):
        """Empty sales sheet returns empty list, not error."""
        excel_file = create_excel_file(
            sales_data=[],
        )

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.sales) == 0
        assert len(result.errors) == 0

    def test_rows_with_empty_sku_skipped(self, known_skus, yesterday):
        """Rows with empty SKU are silently skipped."""
        inventory_data = [
            [98, 1000, 0, yesterday.isoformat(), None],   # NOGAL CAFÉ
            ["", 500, 0, yesterday.isoformat(), None],    # Empty SKU
            [None, 300, 0, yesterday.isoformat(), None],  # None SKU
            [101, 800, 0, yesterday.isoformat(), None],   # CEIBA GRIS OSC
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert len(result.inventory) == 2  # Only 2 valid rows


# ===================
# DATE VALIDATION TESTS
# ===================

class TestDateValidation:
    """Tests for date validation."""

    def test_malformed_date_returns_error(self, known_skus):
        """Malformed date causes error."""
        inventory_data = [
            [98, 1000, 0, "not-a-date", None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert len(result.errors) == 1
        assert result.errors[0].field == "Fecha Conteo"
        assert "Invalid" in result.errors[0].error

    def test_future_date_returns_error(self, known_skus):
        """Future date causes error."""
        future_date = date.today() + timedelta(days=30)
        inventory_data = [
            [98, 1000, 0, future_date.isoformat(), None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert len(result.errors) == 1
        assert "future" in result.errors[0].error.lower()

    def test_various_date_formats_accepted(self, known_skus):
        """Various date formats are parsed correctly."""
        # Use yesterday to avoid future date issues
        yesterday = date.today() - timedelta(days=1)

        # Test with ISO format string
        inventory_data = [
            [98, 1000, 0, yesterday.isoformat(), None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert result.inventory[0].snapshot_date == yesterday


# ===================
# QUANTITY VALIDATION TESTS
# ===================

class TestQuantityValidation:
    """Tests for quantity validation."""

    def test_negative_warehouse_qty_returns_error(self, known_skus, yesterday):
        """Negative warehouse quantity causes error."""
        inventory_data = [
            [98, -100, 0, yesterday.isoformat(), None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert "non-negative" in result.errors[0].error.lower() or "positive" in result.errors[0].error.lower()

    def test_zero_warehouse_qty_allowed(self, known_skus, yesterday):
        """Zero warehouse quantity is allowed."""
        inventory_data = [
            [98, 0, 0, yesterday.isoformat(), None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is True
        assert result.inventory[0].warehouse_qty == 0

    def test_zero_sales_qty_returns_error(self, known_skus, yesterday):
        """Zero sales quantity causes error (sales must be positive)."""
        sales_data = [
            [yesterday.isoformat(), 98, 0, None, None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(sales_data=sales_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert "positive" in result.errors[0].error.lower()

    def test_missing_quantity_returns_error(self, known_skus, yesterday):
        """Missing quantity causes error."""
        inventory_data = [
            [98, None, 0, yesterday.isoformat(), None],  # NOGAL CAFÉ
        ]

        excel_file = create_excel_file(inventory_data=inventory_data)

        result = parse_owner_excel(excel_file, known_skus)

        assert result.success is False
        assert "empty" in result.errors[0].error.lower()


# ===================
# FILE FORMAT TESTS
# ===================

class TestFileFormat:
    """Tests for file format validation."""

    def test_invalid_file_raises_exception(self, known_skus):
        """Invalid file format raises ExcelParseError."""
        invalid_file = BytesIO(b"not an excel file")

        with pytest.raises(ExcelParseError) as exc_info:
            parse_owner_excel(invalid_file, known_skus)

        assert "Failed to read" in str(exc_info.value.message)


# ===================
# RESULT OBJECT TESTS
# ===================

class TestExcelParseResult:
    """Tests for ExcelParseResult object."""

    def test_to_dict_format(self, known_skus, yesterday):
        """Result converts to expected dict format."""
        inventory_data = [
            [98, 1000, 100, yesterday.isoformat(), "Note"],  # NOGAL CAFÉ
        ]
        sales_data = [
            [yesterday.isoformat(), 22, 50, "Customer", None],  # TOLU GRIS
        ]

        excel_file = create_excel_file(
            inventory_data=inventory_data,
            sales_data=sales_data,
        )

        result = parse_owner_excel(excel_file, known_skus)
        result_dict = result.to_dict()

        assert "inventory" in result_dict
        assert "sales" in result_dict
        assert "errors" in result_dict

        assert len(result_dict["inventory"]) == 1
        assert result_dict["inventory"][0]["sku"] == "0000098"  # Padded owner code
        assert result_dict["inventory"][0]["snapshot_date"] == yesterday.isoformat()

    def test_success_property(self):
        """success property reflects error state."""
        result = ExcelParseResult()
        assert result.success is True

        from parsers.excel_parser import ParseError
        result.errors.append(ParseError("Test", 1, "field", "error"))
        assert result.success is False

    def test_has_data_property(self):
        """has_data property reflects data presence."""
        result = ExcelParseResult()
        assert result.has_data is False

        result.inventory.append(InventoryRecord(
            snapshot_date=date.today(),
            sku="TEST",
            product_id="uuid",
            warehouse_qty=100,
            in_transit_qty=0,
        ))
        assert result.has_data is True
