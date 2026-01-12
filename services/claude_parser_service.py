"""
Claude Vision parser service for extracting shipment data from PDFs.

Uses Claude's vision capabilities to intelligently parse shipping documents,
including scanned PDFs that pdfplumber cannot extract text from.
"""

import os
import base64
import json
import re
from typing import Optional
import structlog

# Load .env file for ANTHROPIC_API_KEY
from dotenv import load_dotenv
load_dotenv()

from models.ingest import (
    ParsedDocumentData,
    ParsedFieldConfidence,
    ParsedContainerDetails,
)

logger = structlog.get_logger(__name__)

# Check if Anthropic API key is available
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_AVAILABLE = bool(ANTHROPIC_API_KEY)

if CLAUDE_AVAILABLE:
    try:
        import anthropic
    except ImportError:
        CLAUDE_AVAILABLE = False
        logger.warning("anthropic_package_not_installed")

print(f"CLAUDE_AVAILABLE: {CLAUDE_AVAILABLE}")


class ClaudeParserService:
    """
    Parse shipping documents using Claude Vision API.

    Handles scanned PDFs and complex document formats that regex-based
    parsing cannot handle reliably.
    """

    # Claude model to use for vision tasks
    MODEL = "claude-sonnet-4-20250514"

    # Maximum tokens for response
    MAX_TOKENS = 4096

    # System prompt for document parsing
    SYSTEM_PROMPT = """You are a shipping document parser. Extract structured data from shipping documents.

IMPORTANT: Return ONLY valid JSON, no markdown, no explanation, no code blocks.

Extract these fields (use null if not found):
- document_type: one of "booking", "hbl", "mbl", "departure", "arrival", "unknown"
- document_type_confidence: 0.0-1.0 confidence score
- shp_number: SHP number (format: SHP followed by 7 digits, e.g., SHP0065011)
- booking_number: Booking number (format: 3 letters + 7 digits, e.g., BGA0505879)
- vessel_name: Name of the vessel/ship
- voyage_number: Voyage number
- pol: Port of Loading
- pod: Port of Discharge
- etd: Estimated Time of Departure (format: YYYY-MM-DD)
- eta: Estimated Time of Arrival (format: YYYY-MM-DD)
- atd: Actual Time of Departure (format: YYYY-MM-DD) - IMPORTANT for MBL documents!
  Look for: "Sailed Date", "On Board Date", "Shipped on Board", "Date of Shipment", "Departure Date", "Fecha de Zarpe"
- ata: Actual Time of Arrival (format: YYYY-MM-DD)
  Look for: "Arrival Date", "Discharge Date", "Fecha de Llegada"
- overall_confidence: 0.0-1.0 overall parsing confidence
- notes: Any discrepancies, issues, or important observations

CONTAINER EXTRACTION - CRITICAL:
Extract EVERY container listed in the document. This is very important.
- Container numbers follow ISO 6346 format: 4 LETTERS + 7 DIGITS (e.g., CMAU0630730, DFSU1916028)
- The 4 letters are the owner code, the 7 digits include a check digit
- Count all containers and verify your count matches any totals mentioned in the document
- DOUBLE-CHECK ambiguous characters that scanners often misread:
  * 1 vs I vs 7 (one, letter I, seven)
  * 0 vs O vs D (zero, letter O, letter D)
  * 8 vs B (eight, letter B)
  * 5 vs S (five, letter S)
  * 6 vs G (six, letter G)
- If a container appears multiple times, include it only once
- Look in tables, lists, and cargo description sections

For each container extract:
- container_number: The full 11-character number (4 letters + 7 digits)
- container_type: Size/type code (20GP, 40HC, 20RF, etc.)
- weight_kg: Gross weight in kilograms (number only)
- volume_m3: Volume in cubic meters (number only)
- pallets: Number of pallets/packages (integer)

Document type hints:
- "booking": Contains "Booking Confirmation", "Reserva de Espacio"
- "hbl": House Bill of Lading, contains "House B/L", freight forwarder info, SHP numbers
- "mbl": Master Bill of Lading, ocean carrier document, no SHP numbers
- "departure": Contains "Departure Confirmation", "Confirmo Zarpe"
- "arrival": Contains "Arrival Notice", "Notificación de Arribo"

Return JSON in this exact structure:
{
  "document_type": "booking",
  "document_type_confidence": 0.95,
  "shp_number": "SHP0065011",
  "booking_number": "BGA0505879",
  "vessel_name": "PERITO MORENO",
  "voyage_number": "0YKATN1MA",
  "pol": "Cartagena, Colombia",
  "pod": "Puerto Quetzal, Guatemala",
  "etd": "2025-01-15",
  "eta": "2025-01-22",
  "atd": null,
  "ata": null,
  "container_count": 5,
  "containers": [
    {
      "container_number": "CMAU0630730",
      "container_type": "20GP",
      "weight_kg": 26963,
      "volume_m3": 18.9,
      "pallets": 14
    }
  ],
  "overall_confidence": 0.85,
  "notes": "Document is clear and all fields extracted successfully. Found 5 containers matching the stated total."
}"""

    def __init__(self):
        """Initialize Claude parser service."""
        if CLAUDE_AVAILABLE:
            self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        else:
            self.client = None

    def _pdf_to_base64_images(self, pdf_bytes: bytes) -> list[str]:
        """
        Convert PDF pages to base64-encoded images for Claude Vision.

        Uses pdf2image if available, otherwise returns empty list.
        Claude can process PDFs directly as of late 2024.

        Args:
            pdf_bytes: PDF file content

        Returns:
            List of base64-encoded page images, or empty list if conversion fails
        """
        # Try to convert PDF to images for better OCR-like results
        try:
            from pdf2image import convert_from_bytes

            images = convert_from_bytes(
                pdf_bytes,
                dpi=150,  # Good balance of quality vs size
                first_page=1,
                last_page=3,  # Limit to first 3 pages
            )

            base64_images = []
            for img in images:
                import io
                buffer = io.BytesIO()
                img.save(buffer, format="PNG")
                base64_images.append(base64.b64encode(buffer.getvalue()).decode("utf-8"))

            logger.info("pdf_converted_to_images", page_count=len(base64_images))
            return base64_images

        except ImportError:
            logger.info("pdf2image_not_available_using_pdf_directly")
            return []
        except Exception as e:
            logger.warning("pdf_to_image_conversion_failed", error=str(e))
            return []

    async def parse_pdf(
        self,
        pdf_bytes: bytes,
        email_context: Optional[str] = None
    ) -> ParsedDocumentData:
        """
        Send PDF to Claude Vision for intelligent parsing.

        Args:
            pdf_bytes: PDF file content as bytes
            email_context: Optional email body text for additional context (Phase 2)

        Returns:
            ParsedDocumentData matching existing model structure

        Raises:
            ValueError: If Claude API is not available or parsing fails
        """
        if not CLAUDE_AVAILABLE:
            raise ValueError("Claude API not available. Set ANTHROPIC_API_KEY environment variable.")

        logger.info("claude_parsing_started", pdf_size=len(pdf_bytes))

        try:
            # Build message content
            content = []

            # Try to convert PDF to images first (better for scanned docs)
            base64_images = self._pdf_to_base64_images(pdf_bytes)

            if base64_images:
                # Use images for better scanned document handling
                for i, img_b64 in enumerate(base64_images):
                    content.append({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": img_b64
                        }
                    })
            else:
                # Use PDF directly (Claude supports PDF as of late 2024)
                pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                content.append({
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64
                    }
                })

            # Add context if provided
            prompt = "Parse this shipping document and extract all relevant data."
            if email_context:
                prompt += f"\n\nAdditional context from email:\n{email_context[:1000]}"

            content.append({
                "type": "text",
                "text": prompt
            })

            # Call Claude API
            response = self.client.messages.create(
                model=self.MODEL,
                max_tokens=self.MAX_TOKENS,
                system=self.SYSTEM_PROMPT,
                messages=[{
                    "role": "user",
                    "content": content
                }]
            )

            # Extract response text
            response_text = response.content[0].text
            logger.debug("claude_response_received", response_length=len(response_text))

            # Parse JSON response
            parsed_data = self._parse_claude_response(response_text)

            logger.info(
                "claude_parsing_completed",
                document_type=parsed_data.document_type,
                overall_confidence=parsed_data.overall_confidence,
                containers_count=len(parsed_data.containers),
                etd=parsed_data.etd.value if parsed_data.etd else None,
                eta=parsed_data.eta.value if parsed_data.eta else None,
                atd=parsed_data.atd.value if parsed_data.atd else None,
                ata=parsed_data.ata.value if parsed_data.ata else None
            )

            return parsed_data

        except anthropic.APIError as e:
            logger.error("claude_api_error", error=str(e))
            raise ValueError(f"Claude API error: {str(e)}")
        except Exception as e:
            logger.error("claude_parsing_failed", error=str(e))
            raise ValueError(f"Claude parsing failed: {str(e)}")

    def _parse_claude_response(self, response_text: str) -> ParsedDocumentData:
        """
        Parse Claude's JSON response into ParsedDocumentData.

        Args:
            response_text: Raw response from Claude

        Returns:
            ParsedDocumentData model
        """
        # Clean response - remove markdown code blocks if present
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            # Remove ```json and ``` markers
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error("json_parse_failed", response_preview=response_text[:500], error=str(e))
            # Return minimal parsed data on JSON error
            return ParsedDocumentData(
                document_type="unknown",
                document_type_confidence=0.0,
                raw_text=response_text[:5000],
                overall_confidence=0.0
            )

        # Build ParsedDocumentData from Claude's response

        # Helper to create ParsedFieldConfidence
        def make_field(value: Optional[str], confidence: float = 0.85) -> Optional[ParsedFieldConfidence]:
            if value:
                return ParsedFieldConfidence(
                    value=str(value),
                    confidence=confidence,
                    source_text=f"Extracted by Claude Vision"
                )
            return None

        # Parse containers
        containers_list = []
        container_details = []

        raw_containers = data.get("containers", [])
        stated_count = data.get("container_count")  # Claude's stated count for verification

        if isinstance(raw_containers, list):
            for c in raw_containers:
                if isinstance(c, dict):
                    container_num = c.get("container_number")
                    if container_num:
                        # Clean container number
                        container_num = container_num.replace(" ", "").upper()
                        containers_list.append(container_num)

                        # Build container details
                        detail = ParsedContainerDetails(
                            container_number=container_num,
                            container_type=c.get("container_type"),
                            weight_kg=float(c["weight_kg"]) if c.get("weight_kg") else None,
                            volume_m3=float(c["volume_m3"]) if c.get("volume_m3") else None,
                            pallets=int(c["pallets"]) if c.get("pallets") else None,
                            confidence=0.85
                        )
                        container_details.append(detail)
                elif isinstance(c, str):
                    # Just container number string
                    containers_list.append(c.replace(" ", "").upper())

        # Log container count verification
        actual_count = len(containers_list)
        if stated_count is not None and stated_count != actual_count:
            logger.warning(
                "container_count_mismatch",
                stated_count=stated_count,
                actual_count=actual_count,
                containers=containers_list
            )
        else:
            logger.info(
                "containers_extracted",
                count=actual_count,
                stated_count=stated_count,
                containers=containers_list
            )

        # Parse dates - handle various formats
        def parse_date_field(value: Optional[str]) -> Optional[ParsedFieldConfidence]:
            if not value:
                return None
            # Claude returns YYYY-MM-DD format, convert to display format
            return ParsedFieldConfidence(
                value=value,
                confidence=0.85,
                source_text="Extracted by Claude Vision"
            )

        # Build the response
        doc_type = data.get("document_type", "unknown")
        if doc_type not in ["booking", "departure", "arrival", "hbl", "mbl", "unknown"]:
            doc_type = "unknown"

        parsed_data = ParsedDocumentData(
            document_type=doc_type,
            document_type_confidence=float(data.get("document_type_confidence", 0.5)),
            shp_number=make_field(data.get("shp_number")),
            booking_number=make_field(data.get("booking_number")),
            pv_number=make_field(data.get("pv_number")),
            containers=containers_list,
            containers_confidence=0.85 if containers_list else 0.0,
            container_details=container_details,
            etd=parse_date_field(data.get("etd")),
            eta=parse_date_field(data.get("eta")),
            atd=parse_date_field(data.get("atd")),
            ata=parse_date_field(data.get("ata")),
            pol=make_field(data.get("pol")),
            pod=make_field(data.get("pod")),
            vessel=make_field(data.get("vessel_name")),
            voyage=make_field(data.get("voyage_number")),
            raw_text=f"Parsed by Claude Vision. Notes: {data.get('notes', 'None')}",
            overall_confidence=float(data.get("overall_confidence", 0.5))
        )

        return parsed_data


# Singleton instance
_claude_parser: Optional[ClaudeParserService] = None


def get_claude_parser_service() -> ClaudeParserService:
    """Get or create ClaudeParserService instance."""
    global _claude_parser
    if _claude_parser is None:
        _claude_parser = ClaudeParserService()
    return _claude_parser