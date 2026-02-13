"""
Config service — single source of truth for all system configuration.
Reads from `settings` and `product_type_configs` tables.
Caches in memory, refreshable via POST /api/config/reload.
"""
from typing import Optional
from decimal import Decimal
import structlog
from config.database import get_supabase_client

logger = structlog.get_logger(__name__)


class ConfigService:
    def __init__(self):
        self.db = get_supabase_client()
        self._global_cache: dict[str, str] = {}
        self._product_types_cache: dict[str, dict] = {}
        self._loaded = False

    def _ensure_loaded(self):
        if not self._loaded:
            self.reload()

    def reload(self):
        """Reload all config from database."""
        # Load global settings
        result = self.db.table("settings").select("key, value, category").execute()
        self._global_cache = {row["key"]: row["value"] for row in (result.data or [])}

        # Load product type configs
        result = self.db.table("product_type_configs").select("*").execute()
        self._product_types_cache = {
            row["category_group"]: row for row in (result.data or [])
        }
        self._loaded = True
        logger.info("config_loaded", global_keys=len(self._global_cache), product_types=len(self._product_types_cache))

    def get(self, key: str, default: str = None) -> Optional[str]:
        """Get a global setting value."""
        self._ensure_loaded()
        return self._global_cache.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        val = self.get(key)
        return int(val) if val is not None else default

    def get_float(self, key: str, default: float = 0.0) -> float:
        val = self.get(key)
        return float(val) if val is not None else default

    def get_decimal(self, key: str, default: Decimal = Decimal("0")) -> Decimal:
        val = self.get(key)
        return Decimal(val) if val is not None else default

    def get_product_type(self, category_group: str) -> Optional[dict]:
        """Get product type config by category group."""
        self._ensure_loaded()
        return self._product_types_cache.get(category_group)

    def get_all_global(self) -> dict[str, str]:
        """Get all global settings."""
        self._ensure_loaded()
        return dict(self._global_cache)

    def get_all_product_types(self) -> dict[str, dict]:
        """Get all product type configs."""
        self._ensure_loaded()
        return dict(self._product_types_cache)


# Singleton
_config_service: Optional[ConfigService] = None

def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service
