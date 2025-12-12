"""
Unit tests for TIBA boat schedule Excel parser.

Tests parsing of Tabla de Booking Excel files.
"""

from datetime import date, timedelta
from io import BytesIO
import pytest

from parsers.tiba_parser import (
    parse_tiba_excel,
    BoatScheduleRecord,
    TibaParseResult,
    _parse_date,
    _parse_transit_days,
    _parse_route_type,
    _extract_origin_port,
    _find_header_row,
)
from exceptions import ExcelParseError


# ===================
# DATE PARSING TESTS
# ===================

class TestDateParsing:
    """Tests for date parsing helper."""

    def test_parse_datetime_object(self):
        """Parses datetime objects."""
        from datetime import datetime
        dt = datetime(2026, 1, 15, 10, 30)
        result = _parse_date(dt)
        assert result == date(2026, 1, 15)

    def test_parse_date_object(self):
        """Parses date objects."""
        d = date(2026, 1, 15)
        result = _parse_date(d)
        assert result == date(2026, 1, 15)

    def test_parse_iso_string(self):
        """Parses ISO format strings."""
        result = _parse_date("2026-01-15")
        assert result == date(2026, 1, 15)

    def test_parse_slash_format(self):
        """Parses DD/MM/YYYY format."""
        result = _parse_date("15/01/2026")
        assert result == date(2026, 1, 15)

    def test_parse_none_returns_none(self):
        """Returns None for None input."""
        import pandas as pd
        assert _parse_date(None) is None
        assert _parse_date(pd.NA) is None

    def test_parse_invalid_returns_none(self):
        """Returns None for invalid input."""
        assert _parse_date("not a date") is None
        assert _parse_date("") is None


# ===================
# TRANSIT DAYS PARSING TESTS
# ===================

class TestTransitDaysParsing:
    """Tests for transit days parsing helper."""

    def test_parse_integer(self):
        """Parses integer values."""
        assert _parse_transit_days(9) == 9

    def test_parse_float(self):
        """Parses float values."""
        assert _parse_transit_days(9.0) == 9

    def test_parse_dias_string(self):
        """Parses '9 DIAS' format."""
        assert _parse_transit_days("9 DIAS") == 9
        assert _parse_transit_days("9 dias") == 9

    def test_parse_days_string(self):
        """Parses '9 DAYS' format."""
        assert _parse_transit_days("9 DAYS") == 9

    def test_parse_none_returns_none(self):
        """Returns None for None input."""
        import pandas as pd
        assert _parse_transit_days(None) is None
        assert _parse_transit_days(pd.NA) is None


# ===================
# ROUTE TYPE PARSING TESTS
# ===================

class TestRouteTypeParsing:
    """Tests for route type parsing helper."""

    def test_parse_directo(self):
        """Parses 'DIRECTO' as direct."""
        assert _parse_route_type("DIRECTO") == "direct"
        assert _parse_route_type("Directo") == "direct"

    def test_parse_direct(self):
        """Parses 'DIRECT' as direct."""
        assert _parse_route_type("DIRECT") == "direct"

    def test_parse_escala(self):
        """Parses 'CON ESCALA' as with_stops."""
        assert _parse_route_type("CON ESCALA") == "with_stops"

    def test_parse_with_stops(self):
        """Parses 'WITH STOPS' as with_stops."""
        assert _parse_route_type("WITH STOPS") == "with_stops"

    def test_parse_none_returns_none(self):
        """Returns None for None input."""
        assert _parse_route_type(None) is None

    def test_parse_unknown_returns_none(self):
        """Returns None for unknown values."""
        assert _parse_route_type("UNKNOWN") is None


# ===================
# BOAT SCHEDULE RECORD TESTS
# ===================

class TestBoatScheduleRecord:
    """Tests for BoatScheduleRecord dataclass."""

    def test_create_record(self):
        """Creates record with required fields."""
        record = BoatScheduleRecord(
            departure_date=date(2026, 1, 15),
            arrival_date=date(2026, 1, 24),
            transit_days=9,
            booking_deadline=date(2026, 1, 12),
        )

        assert record.departure_date == date(2026, 1, 15)
        assert record.arrival_date == date(2026, 1, 24)
        assert record.transit_days == 9
        assert record.booking_deadline == date(2026, 1, 12)
        assert record.origin_port == "Castellon"  # Default
        assert record.destination_port == "Puerto Quetzal"  # Default

    def test_create_record_with_all_fields(self):
        """Creates record with all fields."""
        record = BoatScheduleRecord(
            departure_date=date(2026, 1, 15),
            arrival_date=date(2026, 1, 24),
            transit_days=9,
            booking_deadline=date(2026, 1, 12),
            vessel_name="CMA CGM FORT ST LOUIS",
            shipping_line="CMA CGM",
            origin_port="Cartagena",
            destination_port="Puerto Quetzal",
            route_type="direct",
        )

        assert record.vessel_name == "CMA CGM FORT ST LOUIS"
        assert record.shipping_line == "CMA CGM"
        assert record.origin_port == "Cartagena"
        assert record.route_type == "direct"


# ===================
# PARSE RESULT TESTS
# ===================

class TestTibaParseResult:
    """Tests for TibaParseResult dataclass."""

    def test_success_when_no_errors(self):
        """success is True when no errors."""
        result = TibaParseResult()
        assert result.success is True

    def test_success_false_when_errors(self):
        """success is False when errors exist."""
        from parsers.tiba_parser import ParseError
        result = TibaParseResult()
        result.errors.append(ParseError(
            sheet="test",
            row=1,
            field="test",
            error="test error"
        ))
        assert result.success is False

    def test_has_data_when_schedules(self):
        """has_data is True when schedules exist."""
        result = TibaParseResult()
        result.schedules.append(BoatScheduleRecord(
            departure_date=date(2026, 1, 15),
            arrival_date=date(2026, 1, 24),
            transit_days=9,
            booking_deadline=date(2026, 1, 12),
        ))
        assert result.has_data is True

    def test_has_data_false_when_empty(self):
        """has_data is False when no schedules."""
        result = TibaParseResult()
        assert result.has_data is False

    def test_to_dict_format(self):
        """to_dict returns correct format."""
        result = TibaParseResult()
        result.schedules.append(BoatScheduleRecord(
            departure_date=date(2026, 1, 15),
            arrival_date=date(2026, 1, 24),
            transit_days=9,
            booking_deadline=date(2026, 1, 12),
            vessel_name="TEST VESSEL",
        ))
        result.origin_port = "Cartagena"

        data = result.to_dict()

        assert len(data["schedules"]) == 1
        assert data["schedules"][0]["departure_date"] == "2026-01-15"
        assert data["schedules"][0]["vessel_name"] == "TEST VESSEL"
        assert data["origin_port"] == "Cartagena"
        assert data["errors"] == []


# ===================
# INTEGRATION-STYLE TESTS
# ===================
# These would require actual Excel files - marked as skip for unit tests

class TestParseExcelIntegration:
    """Integration tests requiring actual Excel files."""

    @pytest.mark.skip(reason="Requires actual Excel file")
    def test_parse_real_tiba_file(self):
        """Parse actual TIBA Excel file."""
        # This would be run manually with the real sample file
        pass

    def test_invalid_file_raises_error(self):
        """Invalid file raises ExcelParseError."""
        invalid_content = BytesIO(b"not an excel file")

        with pytest.raises(ExcelParseError):
            parse_tiba_excel(invalid_content)
