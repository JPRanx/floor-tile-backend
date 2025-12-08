"""
Settings service for business logic operations.

Settings are pre-seeded key-value pairs. Only updates are allowed.
"""

from typing import Optional
import structlog

from config import get_supabase_client
from models.settings import (
    SettingUpdate,
    SettingResponse,
)
from exceptions import (
    DatabaseError,
)
from exceptions.errors import SettingNotFoundError

logger = structlog.get_logger(__name__)


class SettingsService:
    """
    Settings business logic.

    Handles read and update operations for settings.
    Settings are pre-seeded - no create/delete allowed.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "settings"

    # ===================
    # READ OPERATIONS
    # ===================

    def get_all(
        self,
        category: Optional[str] = None
    ) -> list[SettingResponse]:
        """
        Get all settings with optional category filter.

        Args:
            category: Filter by category

        Returns:
            List of settings
        """
        logger.info("getting_settings", category=category)

        try:
            query = self.db.table(self.table).select("*")

            if category:
                query = query.eq("category", category)

            # Order by key for consistent display
            query = query.order("key")

            response = query.execute()

            settings = [
                SettingResponse(**row)
                for row in response.data
            ]

            logger.info("settings_retrieved", count=len(settings))
            return settings

        except Exception as e:
            logger.error("settings_get_all_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_by_key(self, key: str) -> SettingResponse:
        """
        Get setting by key.

        Args:
            key: Setting key

        Returns:
            Setting

        Raises:
            SettingNotFoundError: If setting doesn't exist
        """
        logger.debug("getting_setting", key=key)

        try:
            response = (
                self.db.table(self.table)
                .select("*")
                .eq("key", key)
                .execute()
            )

            if not response.data:
                raise SettingNotFoundError(key)

            return SettingResponse(**response.data[0])

        except SettingNotFoundError:
            raise
        except Exception as e:
            logger.error("setting_get_failed", key=key, error=str(e))
            raise DatabaseError("select", str(e))

    def get_value(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get setting value by key.

        Convenience method that returns just the value string.

        Args:
            key: Setting key
            default: Default value if not found

        Returns:
            Setting value or default
        """
        try:
            setting = self.get_by_key(key)
            return setting.value
        except SettingNotFoundError:
            return default

    def get_int(self, key: str, default: int = 0) -> int:
        """
        Get setting value as integer.

        Args:
            key: Setting key
            default: Default value if not found or invalid

        Returns:
            Integer value
        """
        value = self.get_value(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """
        Get setting value as float.

        Args:
            key: Setting key
            default: Default value if not found or invalid

        Returns:
            Float value
        """
        value = self.get_value(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default

    # ===================
    # UPDATE OPERATIONS
    # ===================

    def update(self, key: str, data: SettingUpdate) -> SettingResponse:
        """
        Update setting value.

        Args:
            key: Setting key
            data: Update data

        Returns:
            Updated setting

        Raises:
            SettingNotFoundError: If setting doesn't exist
        """
        logger.info("updating_setting", key=key)

        # Verify setting exists
        self.get_by_key(key)

        try:
            response = (
                self.db.table(self.table)
                .update({"value": data.value})
                .eq("key", key)
                .execute()
            )

            if not response.data:
                raise SettingNotFoundError(key)

            logger.info("setting_updated", key=key)
            return SettingResponse(**response.data[0])

        except SettingNotFoundError:
            raise
        except Exception as e:
            logger.error("setting_update_failed", key=key, error=str(e))
            raise DatabaseError("update", str(e))

    # ===================
    # BULK OPERATIONS
    # ===================

    def get_by_keys(self, keys: list[str]) -> dict[str, str]:
        """
        Get multiple settings by keys.

        Args:
            keys: List of setting keys

        Returns:
            Dictionary of key -> value
        """
        logger.debug("getting_settings_bulk", keys=keys)

        try:
            response = (
                self.db.table(self.table)
                .select("key, value")
                .in_("key", keys)
                .execute()
            )

            return {row["key"]: row["value"] for row in response.data}

        except Exception as e:
            logger.error("settings_bulk_get_failed", error=str(e))
            raise DatabaseError("select", str(e))


# Singleton instance
_settings_service: Optional[SettingsService] = None


def get_settings_service() -> SettingsService:
    """Get or create SettingsService instance."""
    global _settings_service
    if _settings_service is None:
        _settings_service = SettingsService()
    return _settings_service
