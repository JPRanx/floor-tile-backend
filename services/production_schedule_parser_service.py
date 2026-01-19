"""
Production Schedule PDF parser using Claude Vision.

Parses factory production schedule PDFs to extract:
- Schedule date and version
- Production line items with dates, factory codes, and m² quantities

See STANDARDS_LOGGING.md for logging patterns.
"""

import os
import base64
import json
import re
from typing import Optional
from datetime import date
from decimal import Decimal
import structlog

from dotenv import load_dotenv
load_dotenv()

from models.production_schedule import (
    ParsedProductionSchedule,
    ProductionScheduleLineItem,
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


class ProductionScheduleParserService:
    """
    Parse production schedule PDFs using Claude Vision API.

    Handles complex tabular layouts with two plants side by side.
    """

    MODEL = "claude-sonnet-4-20250514"
    MAX_TOKENS = 16384  # Large response needed for ~50 line items

    SYSTEM_PROMPT = """You are a production schedule parser for a ceramic tile factory. Extract structured data from production schedule PDFs.

IMPORTANT: Return ONLY valid JSON, no markdown, no explanation, no code blocks.

The document is a production schedule ("PROGRAMA DE PRODUCCION") with these characteristics:
- Two production plants (Planta 1 and Planta 2) shown side by side
- Spanish dates in format: "jueves, 4 de diciembre de 2025" (day of week, day de month de year)
- Factory product codes in "ITEMS" column (4-digit numbers like 5495, 5492)
- Product names in "Referencia" column (like "CEIBA GRIS CLARO BTE", "NOGAL CAFÉ BTE")
- Key quantity is "m² Primera exportacion" (export quality m²)

EXTRACT THESE FIELDS:

1. schedule_date: The reference date shown in the header (usually highlighted in green)
   Format: YYYY-MM-DD
   Look for "sábado, 6 de diciembre de 2025" style text in header

2. schedule_version: Version from title (e.g., "ACTUALIZACION 1")
   Look in title like "PROGRAMA DE PRODUCCION DICIEMBRE (ACTUALIZACION 1)"

3. schedule_month: Month name from title (e.g., "DICIEMBRE")

4. line_items: Array of production items, ONE ENTRY PER ROW IN THE TABLE
   Each item needs:
   - production_date: YYYY-MM-DD format (parse from Spanish date)
   - factory_code: The ITEMS number (e.g., "5495")
   - product_name: The Referencia text (e.g., "CEIBA GRIS CLARO BTE")
   - plant: 1 or 2
   - format: Tile format (e.g., "51X51")
   - design: Design type (MADERA, MARMOLIZADO, PIEDRA, CEMENTO)
   - finish: Finish type (BRILLANTE, SATINADO, GRANILLA, RUSTICO, etc.)
   - shifts: Number of shifts (Nro de Turnos)
   - quality_target_pct: Calidad PROMEDIO percentage (just the number, e.g., 76 for 76%)
   - quality_actual_pct: Calidad Real percentage if available
   - m2_total_net: m² Totales Netos value
   - m2_export_first: m² Primera exportacion value (THIS IS THE KEY QUANTITY!)
   - pct_showroom: "Cant sug. salas" percentage
   - pct_distribution: "Cant sug. Distribución" percentage

SPANISH DATE PARSING:
- lunes = Monday, martes = Tuesday, miércoles = Wednesday
- jueves = Thursday, viernes = Friday, sábado = Saturday, domingo = Sunday
- enero = January, febrero = February, marzo = March, abril = April
- mayo = May, junio = June, julio = July, agosto = August
- septiembre = September, octubre = October, noviembre = November, diciembre = December
- "jueves, 4 de diciembre de 2025" → "2025-12-04"

IMPORTANT NOTES:
- Process BOTH Planta 1 AND Planta 2 columns
- Skip rows with "MANTENIMIENTO DE PLANTA" (maintenance)
- The m² Primera exportacion column may be empty for some rows
- Yellow highlighted rows indicate completed production
- Look for totals at bottom: "Total planta 1" and "Total planta 2"

Return JSON in this exact structure:
{
  "schedule_date": "2025-12-06",
  "schedule_version": "ACTUALIZACION 1",
  "schedule_month": "DICIEMBRE",
  "total_m2_plant1": 440597,
  "total_m2_plant2": 454659,
  "line_items": [
    {
      "production_date": "2025-12-04",
      "factory_code": "5495",
      "product_name": "CEIBA GRIS CLARO BTE",
      "plant": 1,
      "format": "51X51",
      "design": "MADERA",
      "finish": "BRILLANTE",
      "shifts": 3.0,
      "quality_target_pct": 76,
      "quality_actual_pct": 81,
      "m2_total_net": 14062,
      "m2_export_first": 18094,
      "pct_showroom": 50,
      "pct_distribution": 50
    },
    {
      "production_date": "2025-12-04",
      "factory_code": "5552",
      "product_name": "SAMAN CAFÉ BTE",
      "plant": 2,
      "format": "51X51",
      "design": "MADERA",
      "finish": "BRILLANTE",
      "shifts": 3.0,
      "quality_target_pct": 78,
      "quality_actual_pct": 77,
      "m2_total_net": 14062,
      "m2_export_first": 3763,
      "pct_showroom": 50,
      "pct_distribution": 50
    }
  ],
  "parsing_confidence": 0.85,
  "parsing_notes": "Successfully parsed 52 line items from 2 plants. Found 3 maintenance entries that were skipped."
}"""

    def __init__(self):
        """Initialize parser service."""
        if CLAUDE_AVAILABLE:
            self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        else:
            self.client = None

    def _pdf_to_base64_images(self, pdf_bytes: bytes) -> list[str]:
        """
        Convert PDF pages to base64-encoded images for Claude Vision.

        Uses PyMuPDF (fitz) for better quality rendering.

        Args:
            pdf_bytes: PDF file content

        Returns:
            List of base64-encoded page images
        """
        try:
            import fitz  # PyMuPDF

            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            base64_images = []

            for page_num in range(min(len(doc), 3)):  # Limit to first 3 pages
                page = doc[page_num]
                # Render at 150 DPI for good quality
                mat = fitz.Matrix(150 / 72, 150 / 72)
                pix = page.get_pixmap(matrix=mat)
                img_bytes = pix.tobytes("png")
                base64_images.append(base64.b64encode(img_bytes).decode("utf-8"))

            doc.close()
            logger.info("pdf_converted_to_images", page_count=len(base64_images))
            return base64_images

        except ImportError:
            logger.warning("pymupdf_not_installed_trying_pdf2image")
            # Fallback to pdf2image
            try:
                from pdf2image import convert_from_bytes
                import io

                images = convert_from_bytes(
                    pdf_bytes,
                    dpi=150,
                    first_page=1,
                    last_page=3,
                )

                base64_images = []
                for img in images:
                    buffer = io.BytesIO()
                    img.save(buffer, format="PNG")
                    base64_images.append(base64.b64encode(buffer.getvalue()).decode("utf-8"))

                logger.info("pdf_converted_to_images_via_pdf2image", page_count=len(base64_images))
                return base64_images

            except ImportError:
                logger.warning("pdf2image_not_available")
                return []

        except Exception as e:
            logger.error("pdf_to_image_conversion_failed", error=str(e))
            return []

    async def parse_pdf(self, pdf_bytes: bytes, filename: Optional[str] = None) -> ParsedProductionSchedule:
        """
        Parse production schedule PDF using Claude Vision.

        Args:
            pdf_bytes: PDF file content as bytes
            filename: Optional original filename

        Returns:
            ParsedProductionSchedule with extracted data

        Raises:
            ValueError: If Claude API is not available or parsing fails
        """
        if not CLAUDE_AVAILABLE:
            raise ValueError("Claude API not available. Set ANTHROPIC_API_KEY environment variable.")

        logger.info("production_schedule_parsing_started", pdf_size=len(pdf_bytes), filename=filename)

        try:
            # Build message content
            content = []

            # Convert PDF to images
            base64_images = self._pdf_to_base64_images(pdf_bytes)

            if base64_images:
                for img_b64 in base64_images:
                    content.append({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": img_b64
                        }
                    })
            else:
                # Use PDF directly if image conversion failed
                pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                content.append({
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64
                    }
                })

            content.append({
                "type": "text",
                "text": "Parse this production schedule and extract all production line items with their dates, factory codes, and m² quantities."
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

            response_text = response.content[0].text
            logger.debug("claude_response_received", response_length=len(response_text))

            # Parse JSON response
            parsed_data = self._parse_claude_response(response_text)

            logger.info(
                "production_schedule_parsing_completed",
                schedule_date=str(parsed_data.schedule_date),
                line_items_count=len(parsed_data.line_items),
                confidence=parsed_data.parsing_confidence
            )

            return parsed_data

        except anthropic.APIError as e:
            logger.error("claude_api_error", error=str(e))
            raise ValueError(f"Claude API error: {str(e)}")
        except Exception as e:
            logger.error("production_schedule_parsing_failed", error=str(e))
            raise ValueError(f"Production schedule parsing failed: {str(e)}")

    def _parse_claude_response(self, response_text: str) -> ParsedProductionSchedule:
        """
        Parse Claude's JSON response into ParsedProductionSchedule.

        Args:
            response_text: Raw response from Claude

        Returns:
            ParsedProductionSchedule model
        """
        # Clean response - remove markdown code blocks if present
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)

        logger.debug("claude_raw_response_preview", response_preview=response_text[:1000])

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error("json_parse_failed", response_preview=response_text[:500], error=str(e))
            # Return minimal parsed data on JSON error
            return ParsedProductionSchedule(
                schedule_date=date.today(),
                parsing_confidence=0.0,
                parsing_notes=f"JSON parsing failed: {str(e)}"
            )

        # Parse schedule date
        schedule_date_str = data.get("schedule_date")
        try:
            schedule_date = date.fromisoformat(schedule_date_str) if schedule_date_str else date.today()
        except (ValueError, TypeError):
            schedule_date = date.today()
            logger.warning("invalid_schedule_date", raw_value=schedule_date_str)

        # Parse line items
        line_items = []
        raw_items = data.get("line_items", [])

        for item in raw_items:
            if not isinstance(item, dict):
                continue

            # Skip maintenance entries
            product_name = item.get("product_name", "")
            if "MANTENIMIENTO" in str(product_name).upper():
                continue

            # Parse production date
            prod_date_str = item.get("production_date")
            try:
                prod_date = date.fromisoformat(prod_date_str) if prod_date_str else None
            except (ValueError, TypeError):
                prod_date = None

            if not prod_date:
                continue

            # Get factory code
            factory_code = item.get("factory_code")
            if not factory_code:
                continue

            # Build line item
            line_item = ProductionScheduleLineItem(
                production_date=prod_date,
                factory_code=str(factory_code),
                product_name=item.get("product_name"),
                plant=int(item.get("plant", 1)),
                format=item.get("format"),
                design=item.get("design"),
                finish=item.get("finish"),
                shifts=Decimal(str(item.get("shifts"))) if item.get("shifts") else None,
                quality_target_pct=Decimal(str(item.get("quality_target_pct"))) if item.get("quality_target_pct") else None,
                quality_actual_pct=Decimal(str(item.get("quality_actual_pct"))) if item.get("quality_actual_pct") else None,
                m2_total_net=Decimal(str(item.get("m2_total_net"))) if item.get("m2_total_net") else None,
                m2_export_first=Decimal(str(item.get("m2_export_first"))) if item.get("m2_export_first") else None,
                pct_showroom=int(item.get("pct_showroom")) if item.get("pct_showroom") is not None else None,
                pct_distribution=int(item.get("pct_distribution")) if item.get("pct_distribution") is not None else None,
            )
            line_items.append(line_item)

        # Build response
        return ParsedProductionSchedule(
            schedule_date=schedule_date,
            schedule_version=data.get("schedule_version"),
            schedule_month=data.get("schedule_month"),
            line_items=line_items,
            total_m2_plant1=Decimal(str(data.get("total_m2_plant1"))) if data.get("total_m2_plant1") else None,
            total_m2_plant2=Decimal(str(data.get("total_m2_plant2"))) if data.get("total_m2_plant2") else None,
            parsing_confidence=float(data.get("parsing_confidence", 0.5)),
            parsing_notes=data.get("parsing_notes")
        )


# Singleton instance
_parser_service: Optional[ProductionScheduleParserService] = None


def get_production_schedule_parser_service() -> ProductionScheduleParserService:
    """Get or create ProductionScheduleParserService instance."""
    global _parser_service
    if _parser_service is None:
        _parser_service = ProductionScheduleParserService()
    return _parser_service
