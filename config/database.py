"""
Database connection management.

Provides Supabase client singleton for database operations.
See STANDARDS_LOGGING.md for query logging patterns.
"""

from supabase import create_client, Client
from functools import lru_cache
from typing import Optional
import structlog

from config.settings import settings

logger = structlog.get_logger(__name__)


class DatabaseError(Exception):
    """Base exception for database errors."""
    pass


class ConnectionError(DatabaseError):
    """Failed to connect to database."""
    pass


@lru_cache()
def get_supabase_client() -> Client:
    """
    Get cached Supabase client instance.
    
    Uses lru_cache to ensure only one client is created.
    Call get_supabase_client.cache_clear() to reconnect.
    
    Returns:
        Client: Supabase client
        
    Raises:
        ConnectionError: If connection fails
    """
    try:
        logger.info(
            "connecting_to_supabase",
            url=settings.supabase_url[:30] + "..."  # Log partial URL only
        )
        
        client = create_client(
            settings.supabase_url,
            settings.supabase_key
        )
        
        # Test connection with simple query
        result = client.table("settings").select("key").limit(1).execute()
        
        logger.info(
            "supabase_connected",
            status="success"
        )
        
        return client
        
    except Exception as e:
        logger.error(
            "supabase_connection_failed",
            error=str(e),
            error_type=type(e).__name__
        )
        raise ConnectionError(f"Failed to connect to Supabase: {e}") from e


def get_admin_client() -> Optional[Client]:
    """
    Get Supabase client with service role key (admin access).
    
    Only available if SUPABASE_SERVICE_KEY is configured.
    Use sparingly - only for operations requiring admin access.
    
    Returns:
        Client: Admin Supabase client, or None if not configured
    """
    if not settings.supabase_service_key:
        logger.warning("admin_client_not_configured")
        return None
    
    try:
        return create_client(
            settings.supabase_url,
            settings.supabase_service_key
        )
    except Exception as e:
        logger.error(
            "admin_client_failed",
            error=str(e)
        )
        return None


# Convenience alias
db = get_supabase_client


class DatabaseSession:
    """
    Context manager for database operations with logging.
    
    Usage:
        async with DatabaseSession("get_products") as db:
            result = db.table("products").select("*").execute()
    """
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.client: Optional[Client] = None
    
    def __enter__(self) -> Client:
        logger.debug(
            "db_operation_start",
            operation=self.operation_name
        )
        self.client = get_supabase_client()
        return self.client
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error(
                "db_operation_failed",
                operation=self.operation_name,
                error=str(exc_val),
                error_type=exc_type.__name__
            )
        else:
            logger.debug(
                "db_operation_complete",
                operation=self.operation_name
            )
        return False  # Don't suppress exceptions


# ===================
# HELPER FUNCTIONS
# ===================

def check_connection() -> dict:
    """
    Check database connection health.
    
    Returns:
        dict: Connection status with details
    """
    try:
        client = get_supabase_client()
        
        # Count tables we expect
        products = client.table("products").select("id", count="exact").execute()
        settings_count = client.table("settings").select("key", count="exact").execute()
        
        return {
            "status": "healthy",
            "products_count": products.count,
            "settings_count": settings_count.count
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }


def reset_connection():
    """
    Reset the cached database connection.
    
    Call this if connection becomes stale or after config changes.
    """
    get_supabase_client.cache_clear()
    logger.info("database_connection_reset")
