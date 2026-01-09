"""
Document parser service for extracting shipment data from PDFs.

Uses pattern matching and text extraction to parse shipping documents.
"""

import re
from datetime import datetime
from typing import Optional, Tuple
import pdfplumber
import structlog
from io import BytesIO

from models.ingest import (
    ParsedDocumentData,
    ParsedFieldConfidence,
)
from exceptions.errors import PDFParseError

logger = structlog.get_logger(__name__)


class DocumentParserService:
    """
    Parse shipping documents (PDFs) to extract structured data.

    Handles multiple document types: booking confirmations, departure notices,
    bills of lading, etc.
    """

    # Regex patterns for key fields
    SHP_PATTERNS = [
        r'SHP[#:\s]*(\d{7})',  # SHP0065011
        r'SHP[#:\s]*(\d{4,7})',  # SHP followed by 4-7 digits
        r'embarque\s+SHP(\d{7})',  # Spanish: embarque SHP0065011
    ]

    BOOKING_PATTERNS = [
        r'BOOKING[#:\s]*([A-Z]{3}\d{7})',  # BGA0505879
        r'BOOKING\s*(?:NUMBER|NO|#|:)\s*[:\s]*([A-Z0-9]{5,})',  # Generic booking number
        r'N[úu]mero de Booking[:\s]*([A-Z0-9]{5,})',  # Spanish (min 5 chars)
        r'Booking\s+No[.:]?\s*([A-Z]{2,}\d{5,})',  # At least 2 letters + 5 digits
        # NOTE: B/L patterns removed - those are Bill of Lading numbers, not booking numbers
    ]

    PV_PATTERNS = [
        r'PV[#:\s-]*(\d{5})',  # PV-17759 or PV 17759
        r'PEDIDO[:\s]+PV-(\d{5})',  # Spanish: PEDIDO: PV-17759
        r'300-PV-(\d{8})',  # 300-PV-00017759
    ]

    CONTAINER_PATTERNS = [
        r'\b([A-Z]{4}\s?\d{7})\b',  # OOLU0352586 or OOLU 0352586
    ]

    # Date patterns - multiple formats
    DATE_PATTERNS = [
        (r'(\d{1,2}[-/]\w{3}[-/]\d{2,4})', '%d-%b-%Y'),  # 22-JAN-2026
        (r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})', '%d/%m/%Y'),  # 06/01/2026
        (r'ETD[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', '%d-%b-%Y'),
        (r'ETA[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', '%d-%b-%Y'),
        (r'ATD[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', '%d-%b-%Y'),
    ]

    PORT_PATTERNS = [
        (r'POL[:\s]+([A-Za-z\s,]+?)(?=\n|POD|MODE)', 'pol'),
        (r'POD[:\s]+([A-Za-z\s,]+?)(?=\n|MODE|ATD|ETA)', 'pod'),
    ]

    VESSEL_PATTERNS = [
        # M/V or MV prefix: M/V PERITO MORENO
        r'(?:M/?V|MV)\s+([A-Z][A-Z0-9\s]{2,25}?)(?=\s*(?:\n|$|VOY|/))',
        # VESSEL: followed by vessel name (1-3 words, 3+ chars each)
        # Excludes generic text by requiring compact vessel-like names
        r'VESSEL(?:\s+NAME)?[:\s]+([A-Z]{3,15}(?:\s+[A-Z]{3,15}){0,2})(?=\s*(?:\n|$|VOY))',
    ]

    # Words that should NOT appear in vessel names (indicates garbage extraction)
    VESSEL_EXCLUDE_WORDS = {'OPERATOR', 'FOR', 'THE', 'PARTICULAR', 'OCEAN', 'CARRIER', 'SHIPPING', 'LINE'}

    def extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """
        Extract all text from PDF.

        Args:
            pdf_bytes: PDF file content as bytes

        Returns:
            Extracted text as single string

        Raises:
            PDFParseError: If PDF cannot be read
        """
        try:
            all_text = ""
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        all_text += page_text + "\n"

            if not all_text.strip():
                raise PDFParseError(
                    message="No text could be extracted from PDF (may be scanned image)",
                    details={"pdf_size_bytes": len(pdf_bytes)}
                )

            logger.info("pdf_text_extracted", text_length=len(all_text))
            return all_text

        except PDFParseError:
            raise  # Re-raise our custom exception
        except Exception as e:
            logger.error("pdf_extraction_failed", error=str(e))
            raise PDFParseError(
                message=f"Failed to extract text from PDF: {str(e)}",
                details={"original_error": str(e)}
            )

    def detect_document_type(
        self,
        text: str
    ) -> Tuple[str, float]:
        """
        Detect document type from text content.

        Args:
            text: Extracted PDF text

        Returns:
            Tuple of (document_type, confidence)
        """
        text_upper = text.upper()

        # High confidence matches
        if "BOOKING CONFIRMATION" in text_upper:
            return ("booking", 0.95)
        if "DEPARTURE CONFIRMATION" in text_upper or "CONFIRMO ZARPE" in text_upper:
            return ("departure", 0.95)
        if "HOUSE BILL OF LADING" in text_upper and "HBL" in text_upper:
            return ("hbl", 0.9)
        if "MASTER BILL OF LADING" in text_upper and "MBL" in text_upper:
            return ("mbl", 0.9)
        if "ARRIVAL" in text_upper and ("NOTICE" in text_upper or "CONFIRMATION" in text_upper):
            return ("arrival", 0.85)

        # Medium confidence based on keywords
        if "BOOKING" in text_upper and "CONFIRMATION" in text_upper:
            return ("booking", 0.7)
        if "BILL OF LADING" in text_upper:
            return ("hbl", 0.6)  # Could be HBL or MBL

        # Low confidence fallback
        if "SHP" in text and "CONTAINER" in text_upper:
            return ("unknown", 0.3)

        return ("unknown", 0.0)

    def parse_field(
        self,
        text: str,
        patterns: list[str],
        field_name: str
    ) -> Optional[ParsedFieldConfidence]:
        """
        Parse a field using multiple regex patterns.

        Args:
            text: Text to search
            patterns: List of regex patterns to try
            field_name: Name of field (for logging)

        Returns:
            ParsedFieldConfidence if found, None otherwise
        """
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                source_text = match.group(0)[:100]  # First 100 chars of match

                logger.debug(
                    "field_parsed",
                    field=field_name,
                    value=value,
                    pattern=pattern
                )

                return ParsedFieldConfidence(
                    value=value,
                    confidence=0.9,  # High confidence for regex match
                    source_text=source_text
                )

        logger.debug("field_not_found", field=field_name)
        return None

    def parse_containers(self, text: str) -> Tuple[list[str], float]:
        """
        Parse container numbers from text.

        Args:
            text: Text to search

        Returns:
            Tuple of (container_list, confidence)
        """
        containers = set()

        for pattern in self.CONTAINER_PATTERNS:
            matches = re.findall(pattern, text)
            for match in matches:
                # Clean and validate
                container = match.replace(" ", "").upper()
                # Valid container: 4 letters + 7 digits
                if len(container) == 11 and container[:4].isalpha() and container[4:].isdigit():
                    containers.add(container)

        if containers:
            logger.info("containers_parsed", count=len(containers), containers=list(containers))
            return (list(containers), 0.95)

        return ([], 0.0)

    def parse_dates(self, text: str) -> dict[str, Optional[ParsedFieldConfidence]]:
        """
        Parse dates from text (ETD, ETA, ATD, ATA).

        Args:
            text: Text to search

        Returns:
            Dict with date fields
        """
        dates = {
            "etd": None,
            "eta": None,
            "atd": None,
            "ata": None,
        }

        # Look for labeled dates first
        etd_match = re.search(r'ETD[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', text, re.IGNORECASE)
        if etd_match:
            dates["etd"] = ParsedFieldConfidence(
                value=etd_match.group(1),
                confidence=0.9,
                source_text=etd_match.group(0)
            )

        eta_match = re.search(r'ETA[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', text, re.IGNORECASE)
        if eta_match:
            dates["eta"] = ParsedFieldConfidence(
                value=eta_match.group(1),
                confidence=0.9,
                source_text=eta_match.group(0)
            )

        atd_match = re.search(r'ATD[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', text, re.IGNORECASE)
        if atd_match:
            dates["atd"] = ParsedFieldConfidence(
                value=atd_match.group(1),
                confidence=0.95,
                source_text=atd_match.group(0)
            )

        ata_match = re.search(r'ATA[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})', text, re.IGNORECASE)
        if ata_match:
            dates["ata"] = ParsedFieldConfidence(
                value=ata_match.group(1),
                confidence=0.95,
                source_text=ata_match.group(0)
            )

        return dates

    def parse_ports(self, text: str) -> Tuple[
        Optional[ParsedFieldConfidence],
        Optional[ParsedFieldConfidence]
    ]:
        """
        Parse POL (Port of Loading) and POD (Port of Discharge).

        Args:
            text: Text to search

        Returns:
            Tuple of (pol, pod) as ParsedFieldConfidence
        """
        pol = None
        pod = None

        # POL
        pol_match = re.search(r'POL[:\s]+([A-Za-z\s,]+?)(?=\n|POD|MODE|$)', text, re.IGNORECASE)
        if pol_match:
            pol = ParsedFieldConfidence(
                value=pol_match.group(1).strip(),
                confidence=0.85,
                source_text=pol_match.group(0)[:100]
            )

        # POD
        pod_match = re.search(r'POD[:\s]+([A-Za-z\s,]+?)(?=\n|MODE|ATD|ETA|$)', text, re.IGNORECASE)
        if pod_match:
            pod = ParsedFieldConfidence(
                value=pod_match.group(1).strip(),
                confidence=0.85,
                source_text=pod_match.group(0)[:100]
            )

        return (pol, pod)

    def calculate_overall_confidence(self, parsed_data: ParsedDocumentData) -> float:
        """
        Calculate overall parsing confidence.

        Args:
            parsed_data: Parsed document data

        Returns:
            Overall confidence score (0-1)
        """
        scores = []

        # Document type confidence
        scores.append(parsed_data.document_type_confidence)

        # Field confidences
        if parsed_data.shp_number:
            scores.append(parsed_data.shp_number.confidence)
        if parsed_data.booking_number:
            scores.append(parsed_data.booking_number.confidence)
        if parsed_data.containers:
            scores.append(parsed_data.containers_confidence)
        if parsed_data.etd:
            scores.append(parsed_data.etd.confidence)
        if parsed_data.eta:
            scores.append(parsed_data.eta.confidence)
        if parsed_data.atd:
            scores.append(parsed_data.atd.confidence)
        if parsed_data.pol:
            scores.append(parsed_data.pol.confidence)
        if parsed_data.pod:
            scores.append(parsed_data.pod.confidence)
        if parsed_data.vessel:
            scores.append(parsed_data.vessel.confidence)

        if not scores:
            return 0.0

        return sum(scores) / len(scores)

    def validate_vessel_name(self, vessel: Optional[ParsedFieldConfidence]) -> Optional[ParsedFieldConfidence]:
        """
        Validate extracted vessel name and reject garbage text.

        Args:
            vessel: Extracted vessel field

        Returns:
            Vessel if valid, None if garbage text detected
        """
        if not vessel:
            return None

        # Check if vessel name contains excluded words (garbage text)
        vessel_words = set(vessel.value.upper().split())
        if vessel_words & self.VESSEL_EXCLUDE_WORDS:
            logger.info(
                "vessel_rejected_garbage",
                value=vessel.value,
                matched_excluded=list(vessel_words & self.VESSEL_EXCLUDE_WORDS)
            )
            return None

        # Reject very short names (likely partial matches)
        if len(vessel.value) < 4:
            logger.info("vessel_rejected_too_short", value=vessel.value)
            return None

        return vessel

    def parse_pdf(self, pdf_bytes: bytes) -> ParsedDocumentData:
        """
        Parse PDF and extract all shipment data.

        Args:
            pdf_bytes: PDF file content

        Returns:
            ParsedDocumentData with all extracted fields

        Raises:
            PDFParseError: If PDF cannot be processed
        """
        logger.info("parsing_pdf_started")

        # Extract text
        text = self.extract_text_from_pdf(pdf_bytes)

        # Detect document type
        doc_type, doc_type_conf = self.detect_document_type(text)

        # Parse fields
        shp_number = self.parse_field(text, self.SHP_PATTERNS, "shp_number")
        booking_number = self.parse_field(text, self.BOOKING_PATTERNS, "booking_number")
        pv_number = self.parse_field(text, self.PV_PATTERNS, "pv_number")

        # Parse containers
        containers, containers_conf = self.parse_containers(text)

        # Parse dates
        dates = self.parse_dates(text)

        # Parse ports
        pol, pod = self.parse_ports(text)

        # Parse vessel and validate (reject garbage text)
        vessel_raw = self.parse_field(text, self.VESSEL_PATTERNS, "vessel")
        vessel = self.validate_vessel_name(vessel_raw)

        # Build parsed data
        parsed_data = ParsedDocumentData(
            document_type=doc_type,
            document_type_confidence=doc_type_conf,
            shp_number=shp_number,
            booking_number=booking_number,
            pv_number=pv_number,
            containers=containers,
            containers_confidence=containers_conf,
            etd=dates["etd"],
            eta=dates["eta"],
            atd=dates["atd"],
            ata=dates["ata"],
            pol=pol,
            pod=pod,
            vessel=vessel,
            raw_text=text[:5000],  # First 5000 chars
            overall_confidence=0.0  # Calculate below
        )

        # Calculate overall confidence
        parsed_data.overall_confidence = self.calculate_overall_confidence(parsed_data)

        logger.info(
            "parsing_pdf_completed",
            document_type=doc_type,
            overall_confidence=parsed_data.overall_confidence,
            fields_found={
                "shp": bool(shp_number),
                "booking": bool(booking_number),
                "containers": len(containers),
                "etd": bool(dates["etd"]),
                "eta": bool(dates["eta"]),
            }
        )

        return parsed_data


# Singleton instance
_parser_service: Optional[DocumentParserService] = None


def get_parser_service() -> DocumentParserService:
    """Get or create DocumentParserService instance."""
    global _parser_service
    if _parser_service is None:
        _parser_service = DocumentParserService()
    return _parser_service