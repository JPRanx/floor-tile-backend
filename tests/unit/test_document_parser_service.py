"""
Unit tests for DocumentParserService.

Run: pytest tests/unit/test_document_parser_service.py -v
"""

import pytest
from unittest.mock import patch, MagicMock

from services.document_parser_service import DocumentParserService, get_parser_service
from models.ingest import ParsedFieldConfidence


class TestDocumentParserServiceDocumentType:
    """Tests for DocumentParserService.detect_document_type()"""

    def test_detect_booking_confirmation(self):
        """Should detect booking confirmation document."""
        service = DocumentParserService()
        text = "BOOKING CONFIRMATION\nBooking Number: BGA0505879"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "booking"
        assert confidence >= 0.9

    def test_detect_departure_confirmation(self):
        """Should detect departure confirmation document."""
        service = DocumentParserService()
        text = "DEPARTURE CONFIRMATION\nVessel: PERITO MORENO"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "departure"
        assert confidence >= 0.9

    def test_detect_departure_spanish(self):
        """Should detect Spanish departure confirmation."""
        service = DocumentParserService()
        text = "CONFIRMO ZARPE del embarque SHP0065011"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "departure"
        assert confidence >= 0.9

    def test_detect_hbl_document(self):
        """Should detect House Bill of Lading."""
        service = DocumentParserService()
        text = "HOUSE BILL OF LADING\nHBL No: 123456"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "hbl"
        assert confidence >= 0.8

    def test_detect_mbl_document(self):
        """Should detect Master Bill of Lading."""
        service = DocumentParserService()
        text = "MASTER BILL OF LADING\nMBL No: 789012"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "mbl"
        assert confidence >= 0.8

    def test_detect_arrival_notice(self):
        """Should detect arrival notice document."""
        service = DocumentParserService()
        text = "ARRIVAL NOTICE\nVessel arrived at port"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "arrival"
        assert confidence >= 0.8

    def test_detect_unknown_document(self):
        """Should return unknown for unrecognized document."""
        service = DocumentParserService()
        text = "Random text without any shipping keywords"

        doc_type, confidence = service.detect_document_type(text)

        assert doc_type == "unknown"
        assert confidence <= 0.3


class TestDocumentParserServiceSHPNumber:
    """Tests for SHP number extraction."""

    def test_parse_shp_number_standard_format(self):
        """Should extract SHP number in standard format."""
        service = DocumentParserService()
        text = "Shipment SHP0065011 has been confirmed"

        result = service.parse_field(text, service.SHP_PATTERNS, "shp_number")

        assert result is not None
        assert result.value == "0065011"
        assert result.confidence >= 0.8

    def test_parse_shp_number_with_colon(self):
        """Should extract SHP number with colon separator."""
        service = DocumentParserService()
        text = "SHP: 0065011"

        result = service.parse_field(text, service.SHP_PATTERNS, "shp_number")

        assert result is not None
        assert result.value == "0065011"

    def test_parse_shp_number_spanish(self):
        """Should extract SHP from Spanish format."""
        service = DocumentParserService()
        text = "embarque SHP0065011 confirmado"

        result = service.parse_field(text, service.SHP_PATTERNS, "shp_number")

        assert result is not None
        assert result.value == "0065011"

    def test_parse_shp_number_not_found(self):
        """Should return None when SHP not found."""
        service = DocumentParserService()
        text = "No SHP number in this document"

        result = service.parse_field(text, service.SHP_PATTERNS, "shp_number")

        assert result is None


class TestDocumentParserServiceBookingNumber:
    """Tests for booking number extraction."""

    def test_parse_booking_number_standard(self):
        """Should extract booking number like BGA0505879."""
        service = DocumentParserService()
        text = "BOOKING: BGA0505879"

        result = service.parse_field(text, service.BOOKING_PATTERNS, "booking_number")

        assert result is not None
        assert result.value == "BGA0505879"

    def test_parse_booking_number_spanish(self):
        """Should extract booking number from Spanish format."""
        service = DocumentParserService()
        text = "Número de Booking: ABC1234567"

        result = service.parse_field(text, service.BOOKING_PATTERNS, "booking_number")

        assert result is not None
        assert result.value == "ABC1234567"

    def test_parse_booking_number_with_hash(self):
        """Should extract booking number with # separator."""
        service = DocumentParserService()
        text = "BOOKING #BGA0505879"

        result = service.parse_field(text, service.BOOKING_PATTERNS, "booking_number")

        assert result is not None
        assert result.value == "BGA0505879"


class TestDocumentParserServiceContainers:
    """Tests for container number extraction."""

    def test_parse_single_container(self):
        """Should extract single container number."""
        service = DocumentParserService()
        text = "Container: OOLU0352586"

        containers, confidence = service.parse_containers(text)

        assert len(containers) == 1
        assert "OOLU0352586" in containers
        assert confidence >= 0.9

    def test_parse_multiple_containers(self):
        """Should extract multiple container numbers."""
        service = DocumentParserService()
        text = """
        Container 1: OOLU0352586
        Container 2: MSCU1234567
        Container 3: TRIU9876543
        """

        containers, confidence = service.parse_containers(text)

        assert len(containers) == 3
        assert "OOLU0352586" in containers
        assert "MSCU1234567" in containers
        assert "TRIU9876543" in containers

    def test_parse_container_with_space(self):
        """Should handle container number with space."""
        service = DocumentParserService()
        text = "Container: OOLU 0352586"

        containers, confidence = service.parse_containers(text)

        assert len(containers) == 1
        assert "OOLU0352586" in containers

    def test_parse_containers_deduplicates(self):
        """Should not return duplicate container numbers."""
        service = DocumentParserService()
        text = """
        OOLU0352586 - First mention
        OOLU0352586 - Second mention
        """

        containers, confidence = service.parse_containers(text)

        assert len(containers) == 1

    def test_parse_containers_none_found(self):
        """Should return empty list when no containers found."""
        service = DocumentParserService()
        text = "No containers in this document"

        containers, confidence = service.parse_containers(text)

        assert containers == []
        assert confidence == 0.0

    def test_parse_containers_rejects_invalid(self):
        """Should reject invalid container formats."""
        service = DocumentParserService()
        text = "Invalid: ABC123 or 12345678901"

        containers, confidence = service.parse_containers(text)

        assert containers == []


class TestDocumentParserServiceDates:
    """Tests for date extraction."""

    def test_parse_etd_date(self):
        """Should extract ETD date."""
        service = DocumentParserService()
        text = "ETD: 22-JAN-2026"

        dates = service.parse_dates(text)

        assert dates["etd"] is not None
        assert dates["etd"].value == "22-JAN-2026"

    def test_parse_eta_date(self):
        """Should extract ETA date."""
        service = DocumentParserService()
        text = "ETA: 15-FEB-2026"

        dates = service.parse_dates(text)

        assert dates["eta"] is not None
        assert dates["eta"].value == "15-FEB-2026"

    def test_parse_atd_date(self):
        """Should extract ATD (actual departure) date."""
        service = DocumentParserService()
        text = "ATD: 06-Dec-25"

        dates = service.parse_dates(text)

        assert dates["atd"] is not None
        assert dates["atd"].value == "06-Dec-25"

    def test_parse_ata_date(self):
        """Should extract ATA (actual arrival) date."""
        service = DocumentParserService()
        text = "ATA: 27-Nov-25"

        dates = service.parse_dates(text)

        assert dates["ata"] is not None
        assert dates["ata"].value == "27-Nov-25"

    def test_parse_multiple_dates(self):
        """Should extract all dates from document."""
        service = DocumentParserService()
        text = """
        ETD: 01-JAN-2026
        ETA: 15-JAN-2026
        ATD: 02-JAN-2026
        """

        dates = service.parse_dates(text)

        assert dates["etd"] is not None
        assert dates["eta"] is not None
        assert dates["atd"] is not None


class TestDocumentParserServicePorts:
    """Tests for port extraction."""

    def test_parse_pol(self):
        """Should extract Port of Loading."""
        service = DocumentParserService()
        text = "POL: Cartagena, Colombia\nPOD: Santo Tomas"

        pol, pod = service.parse_ports(text)

        assert pol is not None
        assert "Cartagena" in pol.value

    def test_parse_pod(self):
        """Should extract Port of Discharge."""
        service = DocumentParserService()
        text = "POL: Cartagena\nPOD: Santo Tomas, Guatemala"

        pol, pod = service.parse_ports(text)

        assert pod is not None
        assert "Santo Tomas" in pod.value

    def test_parse_ports_both(self):
        """Should extract both POL and POD."""
        service = DocumentParserService()
        text = """
        POL: Cartagena de Indias
        POD: Puerto Barrios
        """

        pol, pod = service.parse_ports(text)

        assert pol is not None
        assert pod is not None


class TestDocumentParserServiceVesselValidation:
    """Tests for vessel name validation."""

    def test_validate_vessel_valid_name(self):
        """Should accept valid vessel name."""
        service = DocumentParserService()
        vessel = ParsedFieldConfidence(
            value="PERITO MORENO",
            confidence=0.9,
            source_text="VESSEL: PERITO MORENO"
        )

        result = service.validate_vessel_name(vessel)

        assert result is not None
        assert result.value == "PERITO MORENO"

    def test_validate_vessel_rejects_garbage_operator(self):
        """Should reject vessel name containing 'OPERATOR'."""
        service = DocumentParserService()
        vessel = ParsedFieldConfidence(
            value="OPERATOR FOR THE",
            confidence=0.9,
            source_text="VESSEL: OPERATOR FOR THE"
        )

        result = service.validate_vessel_name(vessel)

        assert result is None

    def test_validate_vessel_rejects_garbage_carrier(self):
        """Should reject vessel name containing 'CARRIER'."""
        service = DocumentParserService()
        vessel = ParsedFieldConfidence(
            value="OCEAN CARRIER SHIPPING",
            confidence=0.9,
            source_text="VESSEL: OCEAN CARRIER SHIPPING"
        )

        result = service.validate_vessel_name(vessel)

        assert result is None

    def test_validate_vessel_rejects_too_short(self):
        """Should reject vessel name shorter than 4 characters."""
        service = DocumentParserService()
        vessel = ParsedFieldConfidence(
            value="ABC",
            confidence=0.9,
            source_text="VESSEL: ABC"
        )

        result = service.validate_vessel_name(vessel)

        assert result is None

    def test_validate_vessel_accepts_short_valid(self):
        """Should accept valid 4+ character vessel name."""
        service = DocumentParserService()
        vessel = ParsedFieldConfidence(
            value="EVER",
            confidence=0.9,
            source_text="VESSEL: EVER"
        )

        result = service.validate_vessel_name(vessel)

        assert result is not None

    def test_validate_vessel_none_input(self):
        """Should return None for None input."""
        service = DocumentParserService()

        result = service.validate_vessel_name(None)

        assert result is None


class TestDocumentParserServiceConfidence:
    """Tests for confidence calculation."""

    def test_calculate_confidence_with_all_fields(self):
        """Should calculate high confidence when many fields found."""
        service = DocumentParserService()

        # Mock a ParsedDocumentData with good fields
        from models.ingest import ParsedDocumentData
        parsed = ParsedDocumentData(
            document_type="booking",
            document_type_confidence=0.95,
            shp_number=ParsedFieldConfidence(value="0065011", confidence=0.9, source_text=""),
            booking_number=ParsedFieldConfidence(value="BGA0505879", confidence=0.9, source_text=""),
            pv_number=None,
            containers=["OOLU0352586"],
            containers_confidence=0.95,
            etd=ParsedFieldConfidence(value="22-JAN-2026", confidence=0.9, source_text=""),
            eta=ParsedFieldConfidence(value="15-FEB-2026", confidence=0.9, source_text=""),
            atd=None,
            ata=None,
            pol=None,
            pod=None,
            vessel=None,
            raw_text="test",
            overall_confidence=0.0
        )

        confidence = service.calculate_overall_confidence(parsed)

        assert confidence >= 0.9

    def test_calculate_confidence_no_fields(self):
        """Should return low confidence when no fields found."""
        service = DocumentParserService()

        from models.ingest import ParsedDocumentData
        parsed = ParsedDocumentData(
            document_type="unknown",
            document_type_confidence=0.0,
            shp_number=None,
            booking_number=None,
            pv_number=None,
            containers=[],
            containers_confidence=0.0,
            etd=None,
            eta=None,
            atd=None,
            ata=None,
            pol=None,
            pod=None,
            vessel=None,
            raw_text="test",
            overall_confidence=0.0
        )

        confidence = service.calculate_overall_confidence(parsed)

        assert confidence == 0.0


class TestGetParserService:
    """Tests for get_parser_service() singleton."""

    def test_get_parser_service_returns_instance(self):
        """Should return DocumentParserService instance."""
        service = get_parser_service()

        assert isinstance(service, DocumentParserService)