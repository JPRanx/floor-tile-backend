"""
Application settings loaded from environment variables.

Uses pydantic-settings for validation and type safety.
See STANDARDS_SECURITY.md for environment variable management.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings.
    
    All values loaded from .env file or environment variables.
    Validation happens automatically on startup.
    """
    
    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"),  # Check current dir, then parent
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # Ignore extra env vars
    )
    
    # ===================
    # SUPABASE
    # ===================
    supabase_url: str = Field(
        ...,
        description="Supabase project URL"
    )
    supabase_key: str = Field(
        ...,
        description="Supabase anon/public key"
    )
    supabase_service_key: Optional[str] = Field(
        None,
        description="Supabase service role key (for admin operations)"
    )
    
    # ===================
    # API SECURITY
    # ===================
    api_key: Optional[str] = Field(
        None,
        description="API key for authentication"
    )
    
    # ===================
    # TELEGRAM
    # ===================
    telegram_bot_token: Optional[str] = Field(
        None,
        description="Telegram bot token from @BotFather"
    )
    telegram_chat_id: Optional[str] = Field(
        None,
        description="Telegram chat ID for alerts"
    )
    
    # ===================
    # BUSINESS SETTINGS
    # ===================
    lead_time_days: int = Field(
        default=45,
        ge=1,
        le=120,
        description="Days from factory order to warehouse arrival"
    )
    safety_stock_z_score: float = Field(
        default=1.645,
        ge=0,
        le=3,
        description="Z-score for safety stock (1.645 = 95% service level)"
    )
    
    # ===================
    # CONTAINER CONSTRAINTS
    # ===================
    container_max_pallets: int = Field(
        default=14,
        ge=1,
        le=20,
        description="Maximum pallets per container"
    )
    container_max_weight_kg: int = Field(
        default=28000,
        ge=1000,
        le=50000,
        description="Maximum weight per container in kg"
    )
    container_max_m2: int = Field(
        default=1881,
        ge=100,
        le=3000,
        description="Maximum m² per container"
    )
    m2_per_pallet: int = Field(
        default=135,
        ge=50,
        le=200,
        description="Average m² per pallet"
    )
    
    # ===================
    # BOAT CONSTRAINTS
    # ===================
    boat_min_containers: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Minimum containers per boat shipment"
    )
    boat_max_containers: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum containers per boat shipment"
    )
    
    # ===================
    # WAREHOUSE CONSTRAINTS
    # ===================
    warehouse_max_pallets: int = Field(
        default=740,
        ge=100,
        le=2000,
        description="Maximum pallet capacity in warehouse"
    )
    warehouse_max_m2: int = Field(
        default=100000,
        ge=10000,
        le=500000,
        description="Maximum m² capacity in warehouse"
    )
    
    # ===================
    # ALERT THRESHOLDS
    # ===================
    stockout_critical_days: int = Field(
        default=14,
        ge=1,
        le=30,
        description="Days until stockout to trigger CRITICAL alert"
    )
    stockout_warning_days: int = Field(
        default=30,
        ge=7,
        le=60,
        description="Days until stockout to trigger WARNING alert"
    )
    free_days_critical: int = Field(
        default=2,
        ge=1,
        le=5,
        description="Remaining free days to trigger CRITICAL alert"
    )
    free_days_warning: int = Field(
        default=5,
        ge=2,
        le=10,
        description="Remaining free days to trigger WARNING alert"
    )
    
    # ===================
    # APP SETTINGS
    # ===================
    environment: str = Field(
        default="development",
        pattern="^(development|staging|production)$",
        description="Application environment"
    )
    debug: bool = Field(
        default=True,
        description="Enable debug mode"
    )
    log_level: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Logging level"
    )
    api_host: str = Field(
        default="0.0.0.0",
        description="API host"
    )
    api_port: int = Field(
        default=8000,
        ge=1000,
        le=65535,
        description="API port"
    )
    
    # ===================
    # COMPUTED PROPERTIES
    # ===================
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"
    
    @property
    def telegram_configured(self) -> bool:
        """Check if Telegram is properly configured."""
        return bool(self.telegram_bot_token and self.telegram_chat_id)


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Uses lru_cache to ensure settings are only loaded once.
    Call get_settings.cache_clear() to reload.
    
    Returns:
        Settings: Application settings
        
    Raises:
        ValidationError: If required env vars are missing or invalid
    """
    return Settings()


# For convenient imports: from config.settings import settings
settings = get_settings()