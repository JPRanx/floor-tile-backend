"""
Port service for port lookup and management.

Handles finding ports by name with fuzzy matching and auto-creation.
"""

from typing import Optional
import re
import unicodedata
import structlog

from config import get_supabase_client
from models.port import PortCreate, PortResponse
from exceptions import DatabaseError
from exceptions.errors import ValidationError

logger = structlog.get_logger(__name__)


class PortService:
    """
    Port management service.

    Handles port lookup with fuzzy matching and auto-creation.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "ports"

    def normalize_port_name(self, name: str) -> str:
        """
        Normalize port name for matching.

        - Removes accents (Tomás → Tomas)
        - Converts to lowercase
        - Removes extra whitespace
        - Removes common prefixes (Puerto, Port, Santo)

        Args:
            name: Port name to normalize

        Returns:
            Normalized port name
        """
        if not name:
            return ""

        # Remove accents
        normalized = unicodedata.normalize('NFD', name)
        without_accents = ''.join(
            char for char in normalized
            if unicodedata.category(char) != 'Mn'
        )

        # Convert to lowercase
        lower = without_accents.lower()

        # Remove common prefixes
        lower = re.sub(r'^(puerto|port|porto)\s+', '', lower)
        lower = re.sub(r'^santo\s+', '', lower)

        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', lower).strip()

        return normalized

    def find_by_name(
        self,
        name: str,
        port_type: Optional[str] = None
    ) -> Optional[PortResponse]:
        """
        Find port by name with fuzzy matching.

        Tries exact match first, then normalized match.

        Args:
            name: Port name to search for
            port_type: Optional port type filter (ORIGIN or DESTINATION)

        Returns:
            PortResponse if found, None otherwise
        """
        if not name or not name.strip():
            return None

        logger.debug("finding_port_by_name", name=name, port_type=port_type)

        try:
            # Build query
            query = self.db.table(self.table).select("*")

            if port_type:
                query = query.eq("type", port_type)

            # Get all ports (small table, can fetch all)
            result = query.execute()

            if not result.data:
                logger.debug("no_ports_in_database")
                return None

            # Try exact match first
            for row in result.data:
                if row["name"].lower() == name.lower():
                    logger.debug("port_found_exact", port_id=row["id"], name=row["name"])
                    return self._row_to_response(row)

            # Try normalized match
            normalized_search = self.normalize_port_name(name)

            for row in result.data:
                normalized_port = self.normalize_port_name(row["name"])
                if normalized_port == normalized_search:
                    logger.info(
                        "port_found_fuzzy",
                        port_id=row["id"],
                        search_name=name,
                        matched_name=row["name"]
                    )
                    return self._row_to_response(row)

            # Try partial match (contains)
            for row in result.data:
                normalized_port = self.normalize_port_name(row["name"])
                if normalized_search in normalized_port or normalized_port in normalized_search:
                    logger.info(
                        "port_found_partial",
                        port_id=row["id"],
                        search_name=name,
                        matched_name=row["name"]
                    )
                    return self._row_to_response(row)

            logger.debug("port_not_found", name=name)
            return None

        except Exception as e:
            logger.error("find_port_failed", name=name, error=str(e))
            raise DatabaseError("select", str(e))

    def create(self, data: PortCreate) -> PortResponse:
        """
        Create a new port.

        Args:
            data: Port creation data

        Returns:
            Created PortResponse

        Raises:
            DatabaseError: If creation fails
        """
        logger.info("creating_port", name=data.name, type=data.type)

        try:
            port_data = {
                "name": data.name,
                "country": data.country,
                "type": data.type,
                "unlocode": data.unlocode,
                "avg_processing_days": data.avg_processing_days,
            }

            result = (
                self.db.table(self.table)
                .insert(port_data)
                .execute()
            )

            port_id = result.data[0]["id"]

            logger.info("port_created", port_id=port_id, name=data.name)

            return self._row_to_response(result.data[0])

        except Exception as e:
            logger.error("create_port_failed", name=data.name, error=str(e))
            raise DatabaseError("insert", str(e))

    def find_or_create(
        self,
        name: str,
        port_type: str,
        country: str = "Unknown"
    ) -> PortResponse:
        """
        Find port by name or create if not found.

        Args:
            name: Port name to find or create
            port_type: Port type (ORIGIN or DESTINATION)
            country: Country name (used if creating)

        Returns:
            PortResponse (found or created)

        Raises:
            DatabaseError: If database operation fails
        """
        if not name or not name.strip():
            raise ValidationError(
                code="PORT_NAME_REQUIRED",
                message="Port name cannot be empty"
            )

        # Try to find existing
        existing = self.find_by_name(name, port_type=port_type)

        if existing:
            return existing

        # Create new port
        logger.info("auto_creating_port", name=name, type=port_type)

        port_data = PortCreate(
            name=name.strip(),
            country=country,
            type=port_type,
        )

        return self.create(port_data)

    def _row_to_response(self, row: dict) -> PortResponse:
        """Convert database row to PortResponse."""
        return PortResponse(
            id=row["id"],
            name=row["name"],
            country=row["country"],
            type=row["type"],
            unlocode=row.get("unlocode"),
            avg_processing_days=row.get("avg_processing_days"),
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
        )


# Singleton instance
_port_service: Optional[PortService] = None


def get_port_service() -> PortService:
    """Get or create PortService instance."""
    global _port_service
    if _port_service is None:
        _port_service = PortService()
    return _port_service