"""
Factory service for read-only operations on the factories table.

Factories are seeded via migrations — no CRUD operations needed here.
"""

from typing import Optional
import structlog

from config import get_supabase_client
from exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class FactoryService:
    """
    Factory business logic.

    Handles read operations for factories.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "factories"

    def get_all(self) -> list[dict]:
        """
        Get all factories ordered by sort_order.

        Returns:
            List of factory dicts
        """
        logger.info("getting_all_factories")

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .order("sort_order")
                .execute()
            )

            logger.info("factories_retrieved", count=len(result.data))
            return result.data

        except Exception as e:
            logger.error("get_all_factories_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_active(self) -> list[dict]:
        """
        Get only active factories ordered by sort_order.

        Returns:
            List of active factory dicts
        """
        logger.info("getting_active_factories")

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("active", True)
                .order("sort_order")
                .execute()
            )

            logger.info("active_factories_retrieved", count=len(result.data))
            return result.data

        except Exception as e:
            logger.error("get_active_factories_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_id(self, factory_id: str) -> Optional[dict]:
        """
        Get a single factory by ID.

        Args:
            factory_id: Factory UUID

        Returns:
            Factory dict or None if not found
        """
        logger.debug("getting_factory", factory_id=factory_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", factory_id)
                .execute()
            )

            if not result.data:
                return None

            return result.data[0]

        except Exception as e:
            logger.error(
                "get_factory_failed",
                factory_id=factory_id,
                error=str(e)
            )
            raise DatabaseError("select", str(e))


# Singleton instance
_service: Optional[FactoryService] = None


def get_factory_service() -> FactoryService:
    """Get or create FactoryService instance."""
    global _service
    if _service is None:
        _service = FactoryService()
    return _service
