"""
Document parser service for extracting shipment data from PDFs.

Uses pattern matching and text extraction to parse shipping documents.
Includes OCR fallback for scanned PDFs.
"""

import re
import os
import tempfile
from datetime import datetime
from typing import Optional, Tuple
import pdfplumber
import structlog
from io import BytesIO

from models.ingest import (
    ParsedDocumentData,
    ParsedFieldConfidence,
    ParsedContainerDetails,
)
from exceptions.errors import PDFParseError

# OCR imports - optional, controlled by ENABLE_OCR env var
# Disabled by default in production to avoid memory issues on Render free tier
OCR_ENABLED_IN_ENV = os.getenv("ENABLE_OCR", "false").lower() == "true"

OCR_AVAILABLE = False
if OCR_ENABLED_IN_ENV:
    try:
        import pytesseract
        from pdf2image import convert_from_bytes
        OCR_AVAILABLE = True
    except ImportError:
        OCR_AVAILABLE = False

print(f"OCR_AVAILABLE: {OCR_AVAILABLE} (ENABLE_OCR={OCR_ENABLED_IN_ENV})")

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
        r'B/L\s*No[.:]?\s*(SHP\d{7})',  # B/L No.: SHP0045642 (HBL format)
        r'B/L\s*No[.:]?\s*(\d{7})',  # B/L No.: 0045642 (numeric only)
    ]

    BOOKING_PATTERNS = [
        r'BOOKING[#:\s]*([A-Z]{3}\d{7})',  # BGA0505879
        r'BOOKING\s*(?:NUMBER|NO|#|:)\s*[:\s]*([A-Z0-9]{5,})',  # Generic booking number
        r'N[úu]mero de Booking[:\s]*([A-Z0-9]{5,})',  # Spanish (min 5 chars)
        r'Booking\s+No[.:]?\s*([A-Z]{2,}\d{5,})',  # At least 2 letters + 5 digits
        r'REFERENCIA\s+DESTINATARIO\s+([A-Z]{2,3}\d{5,})',  # Spanish arrival: REFERENCIA DESTINATARIO BGA0496181
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
        # Table format: "Vessel" header with vessel name on same line
        # Matches: Vessel    HANSA SIEGBURG    Voyage No.
        r'Vessel\s+([A-Z][A-Z\s]{3,25}?)(?=\s+(?:Voyage|0\d|$|\n))',
        # Table format: "Vessel" on one line, vessel name on NEXT line
        # Matches: Vessel  Voyage No.\nHANSA SIEGBURG   0YKATN1MA
        # Vessel name = uppercase words with NO DIGITS, stop before voyage (0Y... or OY...)
        r'Vessel\s*(?:Voyage\s*(?:No\.?)?)?\s*\n\s*([A-Z]+(?:\s+[A-Z]+)?)\s*(?=[0O]Y|\s{2,}|\n|$)',
        # BUQUE/BARCO (Spanish) followed by vessel name
        r'(?:BUQUE|BARCO)[:\s]+([A-Z][A-Z0-9\s]{2,25}?)(?=\s*(?:\n|$|VIAJE|/))',
    ]

    # Words that should NOT appear in vessel names (indicates garbage extraction)
    VESSEL_EXCLUDE_WORDS = {'OPERATOR', 'FOR', 'THE', 'PARTICULAR', 'OCEAN', 'CARRIER', 'SHIPPING', 'LINE'}

    VOYAGE_PATTERNS = [
        # Voyage format: 0Y... or OY... (like 0YKATN1MA, OYKATNIMA)
        # This is the most specific pattern for HBL voyage numbers
        r'([0O]Y[A-Z0-9]{5,10})',
        # Voyage No. or Voyage: followed by voyage number
        r'Voyage\s*(?:No\.?|Number)?[:\s]+(\d[A-Z0-9]{5,})',
        # VIAJE (Spanish) followed by voyage number
        r'VIAJE[:\s]+([A-Z0-9]{5,})',
        # VOY / VOY. followed by voyage number
        r'VOY\.?\s*[:\s]*([A-Z0-9]{5,})',
    ]

    # Words that should NOT be extracted as voyage (watermarks, etc.)
    VOYAGE_EXCLUDE_WORDS = {'ORIGINAL', 'COPY', 'DRAFT', 'DUPLICATE'}

    # Words that should NOT be extracted as ports (watermarks, labels, etc.)
    PORT_EXCLUDE_WORDS = {'FINAL', 'ORIGINAL', 'COPY', 'DRAFT', 'VOID', 'DESTINATION', 'DELIVERY'}

    # Major ocean carriers (for MBL detection)
    OCEAN_CARRIERS = [
        'CMA CGM', 'MAERSK', 'MSC', 'HAPAG', 'EVERGREEN', 'COSCO', 'ONE',
        'YANG MING', 'HMM', 'ZIM', 'PIL', 'WAN HAI', 'OOCL', 'APL'
    ]

    # Container detail patterns for table extraction
    # Matches: CMAU0630730 20GP 26963 KG 18.9 M3 14 PLT
    CONTAINER_DETAIL_PATTERNS = [
        # Full table row pattern
        r'([A-Z]{4}\d{7})\s+(\d{2}[A-Z]{2})\s+(\d+(?:,\d+)?(?:\.\d+)?)\s*KG?\s+(\d+(?:\.\d+)?)\s*M[³3]?\s+(\d+)\s*(?:PLT|PALLETS?|PKG|PACKAGES?)?',
        # Partial patterns for fallback
        r'([A-Z]{4}\d{7})\s+(\d{2}[A-Z]{2})',  # Container + Type only
    ]

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> str:
        """
        Extract text using pdfplumber (for native PDFs).

        Args:
            pdf_bytes: PDF file content as bytes

        Returns:
            Extracted text as string
        """
        all_text = ""
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    all_text += page_text + "\n"
        return all_text

    def _extract_with_ocr(self, pdf_bytes: bytes) -> str:
        """
        Extract text using OCR (for scanned PDFs).

        Memory-optimized: processes one page at a time to stay under 512MB.
        Uses 200 DPI grayscale (sufficient for printed shipping documents).

        Args:
            pdf_bytes: PDF file content as bytes

        Returns:
            Extracted text as string
        """
        if not OCR_AVAILABLE:
            logger.warning("ocr_not_available", reason="pytesseract or pdf2image not installed")
            return ""

        try:
            import gc
            from pdf2image import pdfinfo_from_bytes

            print("Starting OCR extraction (memory-optimized)...")
            logger.info("ocr_extraction_started", pdf_size=len(pdf_bytes))

            # Get page count first (minimal memory)
            try:
                info = pdfinfo_from_bytes(pdf_bytes)
                total_pages = info.get('Pages', 1)
            except Exception:
                total_pages = 5  # Assume max if can't read info

            max_pages = 5  # Safety limit for memory
            pages_to_process = min(total_pages, max_pages)

            all_text = []

            # Process ONE page at a time to minimize memory
            for page_num in range(1, pages_to_process + 1):
                try:
                    # Convert single page with memory-optimized settings
                    images = convert_from_bytes(
                        pdf_bytes,
                        dpi=200,  # Reduced from 300 - still good for printed text
                        first_page=page_num,
                        last_page=page_num,
                        grayscale=True,  # 1/3 memory of RGB
                        thread_count=1,  # Single thread = less memory overhead
                    )

                    if images:
                        # OCR this single page
                        text = pytesseract.image_to_string(images[0], lang='spa+eng')
                        all_text.append(text)
                        logger.debug("ocr_page_processed", page=page_num, text_length=len(text))

                        # Explicitly release memory
                        del images

                    # Force garbage collection after each page
                    gc.collect()

                except Exception as page_err:
                    logger.warning("ocr_page_failed", page=page_num, error=str(page_err))
                    continue

            if total_pages > max_pages:
                logger.warning("ocr_page_limit_reached", total_pages=total_pages, processed=max_pages)

            result = "\n".join(all_text)
            print(f"OCR extracted: {len(result)} chars from {len(all_text)} pages")
            logger.info("ocr_extraction_completed", text_length=len(result), pages=len(all_text))
            return result

        except Exception as e:
            logger.error("ocr_extraction_failed", error=str(e))
            return ""

    def extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """
        Extract all text from PDF, with OCR fallback for scanned documents.

        Args:
            pdf_bytes: PDF file content as bytes

        Returns:
            Extracted text as single string

        Raises:
            PDFParseError: If PDF cannot be read
        """
        try:
            # Try pdfplumber first (faster, works for native PDFs)
            all_text = self._extract_with_pdfplumber(pdf_bytes)
            print(f"pdfplumber extracted: {len(all_text.strip())} chars")

            # If too little text, try OCR fallback
            if len(all_text.strip()) < 50:
                print("Triggering OCR fallback...")
                logger.info(
                    "pdfplumber_insufficient_text",
                    text_length=len(all_text.strip()),
                    trying_ocr=OCR_AVAILABLE
                )
                if OCR_AVAILABLE:
                    ocr_text = self._extract_with_ocr(pdf_bytes)
                    if len(ocr_text.strip()) > len(all_text.strip()):
                        all_text = ocr_text
                        logger.info("using_ocr_text", text_length=len(all_text))

            if not all_text.strip():
                raise PDFParseError(
                    message="No text could be extracted from PDF (may be scanned image without OCR)",
                    details={"pdf_size_bytes": len(pdf_bytes), "ocr_available": OCR_AVAILABLE}
                )

            logger.info(
                "pdf_text_extracted",
                text_length=len(all_text),
                preview=all_text[:500].replace('\n', ' ')[:200]  # First 200 chars for debugging
            )
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

        # High confidence matches - explicit document type declarations
        if "BOOKING CONFIRMATION" in text_upper:
            return ("booking", 0.95)
        if "RESERVA DE ESPACIO" in text_upper:  # Spanish booking
            return ("booking", 0.9)
        if "DEPARTURE CONFIRMATION" in text_upper or "CONFIRMO ZARPE" in text_upper:
            return ("departure", 0.95)
        if "HOUSE BILL OF LADING" in text_upper:
            return ("hbl", 0.95)

        # MBL detection - explicit patterns
        if "MASTER BILL OF LADING" in text_upper:
            return ("mbl", 0.95)
        if "MASTER B/L" in text_upper:
            return ("mbl", 0.9)
        # "MBL" alone is less reliable but useful
        if re.search(r'\bMBL\b', text_upper):
            # Confirm with ocean carrier presence
            carrier_count = sum(1 for carrier in self.OCEAN_CARRIERS if carrier in text_upper)
            if carrier_count > 0:
                return ("mbl", 0.85)

        if "NOTIFICACIÓN DE ARRIBO" in text_upper or "NOTIFICACION DE ARRIBO" in text_upper:
            return ("arrival", 0.9)
        if "ARRIVAL" in text_upper and ("NOTICE" in text_upper or "NOTIFICATION" in text_upper):
            return ("arrival", 0.85)

        # Medium confidence based on keywords
        if "BOOKING" in text_upper and "CONFIRMATION" in text_upper:
            return ("booking", 0.7)

        # Bill of Lading detection with context clues
        if "BILL OF LADING" in text_upper or "B/L" in text_upper:
            # Check for HBL indicators (freight forwarders, house-specific terms)
            hbl_indicators = ["TIBA", "WORLDWIDE CONTAINER", "FREIGHT FORWARDER", "HOUSE B/L", "SHP", "NVOCC"]
            # Check for MBL indicators (ocean carriers, master-specific terms)
            mbl_indicators = self.OCEAN_CARRIERS + ["MASTER B/L", "OCEAN BILL", "CARRIER"]

            hbl_score = sum(1 for ind in hbl_indicators if ind in text_upper)
            mbl_score = sum(1 for ind in mbl_indicators if ind in text_upper)

            # Additional MBL signal: no SHP number pattern (HBLs usually have SHP)
            has_shp = bool(re.search(r'SHP\d{7}', text_upper) or re.search(r'B/L\s*No[.:]?\s*SHP', text_upper))
            if not has_shp:
                mbl_score += 1  # Slight boost for not having SHP

            logger.debug(
                "bill_of_lading_detection",
                hbl_score=hbl_score,
                mbl_score=mbl_score,
                has_shp=has_shp
            )

            if mbl_score > hbl_score:
                return ("mbl", 0.80)
            elif hbl_score > 0:
                return ("hbl", 0.80)
            else:
                # Default to HBL if no clear indicators
                return ("hbl", 0.6)

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

        # ATD patterns - multiple formats
        atd_patterns = [
            r'ATD[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})',
            r'On\s+Board[:\s]+(\d{1,2}[-/]\w{3}[-/]\d{2,4})',  # "On Board: 19-Nov-25"
            r'Shipped\s+on\s+Board[^\n]*?(\d{1,2}[-/]\w{3}[-/]\d{4})',  # "Shipped on Board ... 20-NOV-2025"
        ]
        for pattern in atd_patterns:
            atd_match = re.search(pattern, text, re.IGNORECASE)
            if atd_match:
                dates["atd"] = ParsedFieldConfidence(
                    value=atd_match.group(1),
                    confidence=0.95,
                    source_text=atd_match.group(0)[:100]
                )
                break

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

        # POL patterns (try multiple formats)
        pol_patterns = [
            r'POL[:\s]+([A-Za-z\s,]+?)(?=\n|POD|MODE|$)',
            r'Port\s+of\s+Loading[:\s]*\n?\s*([A-Za-z\s,]+?)(?=\n|Port|Vessel|$)',
            r'Puerto\s+de\s+Carga[:\s]*\n?\s*([A-Za-z\s,]+?)(?=\n|Puerto|Terminal|$)',
        ]
        for pattern in pol_patterns:
            pol_match = re.search(pattern, text, re.IGNORECASE)
            if pol_match:
                pol = ParsedFieldConfidence(
                    value=pol_match.group(1).strip(),
                    confidence=0.85,
                    source_text=pol_match.group(0)[:100]
                )
                break

        # POD patterns (try multiple formats)
        pod_patterns = [
            r'POD[:\s]+([A-Za-z\s,]+?)(?=\n|MODE|ATD|ETA|$)',
            r'Port\s+of\s+Discharge[:\s]*\n?\s*([A-Za-z\s,]+?)(?=\n|Place|Final|$)',
            r'Puerto\s+de\s+Descarga[:\s]*\n?\s*([A-Za-z\s,]+?)(?=\n|Lugar|Destino|$)',
        ]
        for pattern in pod_patterns:
            pod_match = re.search(pattern, text, re.IGNORECASE)
            if pod_match:
                pod = ParsedFieldConfidence(
                    value=pod_match.group(1).strip(),
                    confidence=0.85,
                    source_text=pod_match.group(0)[:100]
                )
                break

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

    def validate_voyage(self, voyage: Optional[ParsedFieldConfidence]) -> Optional[ParsedFieldConfidence]:
        """
        Validate extracted voyage and reject watermarks/garbage.

        Args:
            voyage: Extracted voyage field

        Returns:
            Voyage if valid, None if garbage detected
        """
        if not voyage:
            return None

        # Check if voyage is an excluded word (watermark)
        if voyage.value.upper() in self.VOYAGE_EXCLUDE_WORDS:
            logger.info("voyage_rejected_watermark", value=voyage.value)
            return None

        # Voyage numbers should have digits in them
        if not any(c.isdigit() for c in voyage.value):
            # Exception: OCR might read all as letters, but should still match 0Y pattern
            if not voyage.value.upper().startswith('OY'):
                logger.info("voyage_rejected_no_digits", value=voyage.value)
                return None

        return voyage

    def validate_port(self, port: Optional[ParsedFieldConfidence]) -> Optional[ParsedFieldConfidence]:
        """
        Validate extracted port and reject watermarks/garbage.

        Args:
            port: Extracted port field

        Returns:
            Port if valid, None if garbage detected
        """
        if not port:
            return None

        # Clean and check the port value
        port_value = port.value.strip().upper()

        # Check if port is an excluded word (watermark or label)
        if port_value in self.PORT_EXCLUDE_WORDS:
            logger.info("port_rejected_excluded", value=port.value)
            return None

        # Check if port contains only excluded words
        port_words = set(port_value.split())
        if port_words and port_words.issubset(self.PORT_EXCLUDE_WORDS):
            logger.info("port_rejected_all_excluded_words", value=port.value)
            return None

        # Reject very short values (likely partial matches)
        if len(port_value) < 3:
            logger.info("port_rejected_too_short", value=port.value)
            return None

        return port

    def parse_container_details(self, text: str, containers: list[str]) -> list[ParsedContainerDetails]:
        """
        Parse detailed container information from HBL/MBL tables.

        Extracts: type (20GP, 40HC), weight (kg), volume (m³), pallets

        Handles split OCR format:
        1. Header table: containers listed first, types listed after "Type|"
           Example: CMAU0630730 000100|DFSU1916028 000123|Type|20GP|20GP|
        2. Cargo blocks: each container has separate block with details
           Example: DFSU1916028|14 PALLETS|NET WEIGHT 27062

        Args:
            text: Document text to search
            containers: List of container numbers already found

        Returns:
            List of ParsedContainerDetails with extracted info
        """
        details_map: dict[str, ParsedContainerDetails] = {}
        text_upper = text.upper()

        # Initialize details for all containers
        for c in containers:
            details_map[c] = ParsedContainerDetails(container_number=c)

        # =====================================================================
        # STEP 1: Parse header table for container types
        # Format: {container} {seal}|{container} {seal}|...|Type|{type}|{type}|
        # =====================================================================
        self._parse_header_table_types(text_upper, containers, details_map)

        # =====================================================================
        # STEP 2: Parse cargo blocks for weight/pallets/volume per container
        # Each container has its own cargo block with details
        # =====================================================================
        self._parse_cargo_blocks(text_upper, containers, details_map)

        # =====================================================================
        # STEP 3: Fallback - try inline parsing for any container missing data
        # =====================================================================
        for container_num in containers:
            detail = details_map[container_num]
            # Only try fallback if we're missing key data
            if not detail.container_type or not detail.weight_kg:
                self._parse_inline_details(text_upper, container_num, detail)

        # =====================================================================
        # STEP 4: Propagate defaults for missing values
        # If any container has volume/type, apply as default to those missing it
        # (Common case: all containers in same shipment have same specs)
        # =====================================================================
        default_volume = next(
            (d.volume_m3 for d in details_map.values() if d.volume_m3),
            None
        )
        default_type = next(
            (d.container_type for d in details_map.values() if d.container_type),
            None
        )

        if default_volume or default_type:
            for container_num in containers:
                detail = details_map[container_num]
                if not detail.volume_m3 and default_volume:
                    detail.volume_m3 = default_volume
                    detail.confidence = min(detail.confidence, 0.7)  # Lower confidence for inferred
                    logger.debug("volume_defaulted", container=container_num, volume_m3=default_volume)
                if not detail.container_type and default_type:
                    detail.container_type = default_type
                    detail.confidence = min(detail.confidence, 0.7)
                    logger.debug("type_defaulted", container=container_num, type=default_type)

        # Build final list - only include containers with some data
        details = []
        for c in containers:
            d = details_map[c]
            if d.container_type or d.weight_kg or d.volume_m3 or d.pallets:
                details.append(d)

        if details:
            logger.info("container_details_extracted", count=len(details))

        return details

    def _parse_header_table_types(
        self,
        text: str,
        containers: list[str],
        details_map: dict[str, ParsedContainerDetails]
    ) -> None:
        """
        Parse header table to extract container types by position.

        Handles format: CMAU0630730 000100|DFSU1916028 000123|...|Type|20GP|20GP|
        Maps each type to corresponding container by order.
        """
        # Find "Type|" followed by container types
        type_header_match = re.search(
            r'TYPE[\s|]+(\d{2}[A-Z]{2}(?:[\s|]+\d{2}[A-Z]{2})*)',
            text,
            re.IGNORECASE
        )

        if type_header_match:
            # Extract all types after "Type|"
            types_text = type_header_match.group(1)
            types = re.findall(r'(\d{2}[A-Z]{2})', types_text)

            logger.debug("header_types_found", types=types, container_count=len(containers))

            # Find container order in the header (before "Type|")
            header_start = 0
            header_end = type_header_match.start()
            header_text = text[header_start:header_end]

            # Find containers in order they appear in header
            container_positions = []
            for c in containers:
                pos = header_text.find(c)
                if pos >= 0:
                    container_positions.append((pos, c))

            # Sort by position to get order
            container_positions.sort(key=lambda x: x[0])
            ordered_containers = [c for _, c in container_positions]

            # Map types to containers by position
            for i, container_num in enumerate(ordered_containers):
                if i < len(types):
                    details_map[container_num].container_type = types[i]
                    details_map[container_num].confidence = 0.9
                    logger.debug(
                        "container_type_from_header",
                        container=container_num,
                        type=types[i],
                        position=i
                    )

    def _parse_cargo_blocks(
        self,
        text: str,
        containers: list[str],
        details_map: dict[str, ParsedContainerDetails]
    ) -> None:
        """
        Parse cargo blocks to extract weight/pallets/volume per container.

        Each container has a cargo description block:
        DFSU1916028|14 PALLETS|...|NET WEIGHT 27062|18.9 M3

        Also handles inline table format:
        CMAU0630730 000100 20GP 26963 KG 18.9 M3 14 PLT
        """
        for container_num in containers:
            # Simpler approach: find container and extract next ~300 chars as cargo block
            idx = text.find(container_num)
            block_count = 0
            while idx >= 0 and block_count < 5:  # Check up to 5 occurrences
                block_count += 1
                block_end = min(len(text), idx + 300)

                # Find where next container starts (to limit block)
                next_container_pos = block_end
                for other_c in containers:
                    if other_c != container_num:
                        other_pos = text.find(other_c, idx + len(container_num))
                        if other_pos > idx and other_pos < next_container_pos:
                            next_container_pos = other_pos

                cargo_block = text[idx:next_container_pos]

                # Check if this block has cargo details (not just header mention)
                has_cargo_details = bool(
                    re.search(r'PALLET|WEIGHT|NET\s*W|KGS?|M[³3]|CBM|\d+\.\d+', cargo_block, re.IGNORECASE)
                )

                if has_cargo_details:
                    # Extract pallets - but NOT from "STC X Pallet" (shipment total)
                    # Valid: "14 PALLETS", "14 PLT"
                    # Invalid: "STC 70 Pallet(s)" - this is total
                    pallets_match = re.search(
                        r'(?<!STC\s)(?<!STC\s\s)(\d{1,3})\s*(?:PALLETS?|PLT)\b',
                        cargo_block,
                        re.IGNORECASE
                    )
                    if pallets_match and not details_map[container_num].pallets:
                        try:
                            pallets = int(pallets_match.group(1))
                            # Sanity check: per-container pallets should be < 50
                            if pallets < 50:
                                details_map[container_num].pallets = pallets
                                logger.debug("cargo_pallets_parsed", container=container_num, pallets=pallets)
                        except ValueError:
                            pass

                    # Extract weight - "NET WEIGHT 27062" or "27062 KG"
                    weight_patterns = [
                        r'NET\s*(?:WEIGHT|WT?)[\s:]*(\d+(?:,\d{3})*(?:\.\d+)?)',
                        r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:KGS?|KG)\b',
                        r'WEIGHT[\s:]*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:KGS?)?',
                    ]
                    for wp in weight_patterns:
                        weight_match = re.search(wp, cargo_block, re.IGNORECASE)
                        if weight_match and not details_map[container_num].weight_kg:
                            try:
                                weight_str = weight_match.group(1).replace(',', '')
                                weight = float(weight_str)
                                # Sanity: container weight should be 1000-50000 kg typically
                                if 500 < weight < 50000:
                                    details_map[container_num].weight_kg = weight
                                    logger.debug("cargo_weight_parsed", container=container_num, weight_kg=weight)
                                    break
                            except ValueError:
                                pass

                    # Extract volume - multiple patterns for different formats
                    # "18.9 M3", "18.9 CBM", "18.9M3", "18,9 M3"
                    volume_patterns = [
                        r'(\d+[.,]\d+)\s*(?:M[³3]|CBM)\b',  # 18.9 M3 or 18,9 M3
                        r'(\d+)\s*(?:M[³3]|CBM)\b',  # 18 M3 (integer)
                        r'(?:VOL(?:UME)?|CBM)[\s:]*(\d+[.,]?\d*)',  # VOLUME: 18.9
                    ]
                    if not details_map[container_num].volume_m3:
                        for vp in volume_patterns:
                            volume_match = re.search(vp, cargo_block, re.IGNORECASE)
                            if volume_match:
                                try:
                                    vol_str = volume_match.group(1).replace(',', '.')
                                    volume = float(vol_str)
                                    # Sanity: container volume should be 1-100 m³
                                    if 1 < volume < 100:
                                        details_map[container_num].volume_m3 = volume
                                        logger.debug("cargo_volume_parsed", container=container_num, volume_m3=volume)
                                        break
                                except ValueError:
                                    pass

                # Look for next occurrence of this container
                idx = text.find(container_num, idx + len(container_num))

    def _parse_inline_details(
        self,
        text: str,
        container_num: str,
        detail: ParsedContainerDetails
    ) -> None:
        """
        Fallback: Parse inline details near container number.

        For formats where type/weight are right next to container:
        CMAU0630730 20GP 26963 KG 18.9 M3 14 PLT
        """
        container_idx = text.find(container_num)
        if container_idx < 0:
            return

        # Extract context around container
        context_start = max(0, container_idx - 20)
        context_end = min(len(text), container_idx + 200)
        context = text[context_start:context_end]

        # Type: Allow seal/other text between container and type
        if not detail.container_type:
            type_patterns = [
                rf'{container_num}[\s|]+(\d{{2}}[A-Z]{{2}})\b',
                rf'{container_num}[\s|]+\d{{5,8}}[\s|]+(\d{{2}}[A-Z]{{2}})\b',
                rf'{container_num}[\s\S]{{0,30}}?(\d{{2}}[A-Z]{{2}})\b',
            ]
            for tp in type_patterns:
                type_match = re.search(tp, context, re.IGNORECASE)
                if type_match:
                    detail.container_type = type_match.group(1)
                    detail.confidence = 0.7
                    break

        # Weight (if not already found)
        if not detail.weight_kg:
            weight_match = re.search(
                r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:KGS?|KG)\b',
                context,
                re.IGNORECASE
            )
            if weight_match:
                try:
                    weight_str = weight_match.group(1).replace(',', '')
                    weight = float(weight_str)
                    if 500 < weight < 50000:
                        detail.weight_kg = weight
                except ValueError:
                    pass

        # Volume (if not already found) - multiple patterns
        if not detail.volume_m3:
            volume_patterns = [
                r'(\d+[.,]\d+)\s*(?:M[³3]|CBM)\b',  # 18.9 M3 or 18,9 M3
                r'(\d+)\s*(?:M[³3]|CBM)\b',  # 18 M3 (integer)
                r'(?:VOL(?:UME)?|CBM)[\s:]*(\d+[.,]?\d*)',  # VOLUME: 18.9
            ]
            for vp in volume_patterns:
                volume_match = re.search(vp, context, re.IGNORECASE)
                if volume_match:
                    try:
                        vol_str = volume_match.group(1).replace(',', '.')
                        volume = float(vol_str)
                        if 1 < volume < 100:
                            detail.volume_m3 = volume
                            break
                    except ValueError:
                        pass

        # Pallets (if not already found) - exclude STC totals
        if not detail.pallets:
            pallets_match = re.search(
                r'(?<!STC\s)(\d{1,3})\s*(?:PALLETS?|PLT|PKG)\b',
                context,
                re.IGNORECASE
            )
            if pallets_match:
                try:
                    pallets = int(pallets_match.group(1))
                    if pallets < 50:
                        detail.pallets = pallets
                except ValueError:
                    pass

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

        # Parse container details (for HBL/MBL documents with tables)
        container_details = []
        if containers and doc_type in ("hbl", "mbl"):
            container_details = self.parse_container_details(text, containers)

        # DEBUG: Container details extraction
        print(f"=== CONTAINER DETAILS DEBUG ===")
        print(f"Document type: {doc_type}")
        print(f"Containers found: {len(containers)}")
        print(f"Container details parsed: {len(container_details)}")

        # Build lookup for parsed details
        details_lookup = {cd.container_number: cd for cd in container_details}

        # Show context for EACH container (whether parsed or not)
        for c in containers:
            cd = details_lookup.get(c)
            if cd:
                print(f"  ✓ {c}: type={cd.container_type}, weight={cd.weight_kg}kg, vol={cd.volume_m3}m³, pallets={cd.pallets}")
            else:
                print(f"  ✗ {c}: NO DETAILS EXTRACTED")

            # Always show context to debug pattern matching
            idx = text.upper().find(c)
            if idx >= 0:
                start = max(0, idx - 10)
                end = min(len(text), idx + 120)
                context = text[start:end].replace('\n', '|')
                print(f"    Context: {repr(context)}")
        print(f"================================")

        # Parse dates
        dates = self.parse_dates(text)

        # Parse ports and validate (reject watermarks like "FINAL")
        pol_raw, pod_raw = self.parse_ports(text)
        pol = self.validate_port(pol_raw)
        pod = self.validate_port(pod_raw)

        # Parse vessel and validate (reject garbage text)
        vessel_raw = self.parse_field(text, self.VESSEL_PATTERNS, "vessel")
        vessel = self.validate_vessel_name(vessel_raw)

        # Parse voyage and validate (reject watermarks)
        voyage_raw = self.parse_field(text, self.VOYAGE_PATTERNS, "voyage")
        voyage = self.validate_voyage(voyage_raw)

        # Build parsed data
        parsed_data = ParsedDocumentData(
            document_type=doc_type,
            document_type_confidence=doc_type_conf,
            shp_number=shp_number,
            booking_number=booking_number,
            pv_number=pv_number,
            containers=containers,
            containers_confidence=containers_conf,
            container_details=container_details,
            etd=dates["etd"],
            eta=dates["eta"],
            atd=dates["atd"],
            ata=dates["ata"],
            pol=pol,
            pod=pod,
            vessel=vessel,
            voyage=voyage,
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