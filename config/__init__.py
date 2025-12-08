"""
Configuration module.

Exports:
    settings: Application settings instance
    get_settings: Function to get settings (for dependency injection)
    db: Function to get Supabase client
    get_supabase_client: Same as db
    check_connection: Health check function
"""

from config.settings import settings, get_settings, Settings
from config.database import (
    db,
    get_supabase_client,
    get_admin_client,
    check_connection,
    reset_connection,
    DatabaseSession,
    DatabaseError,
    ConnectionError
)

__all__ = [
    # Settings
    "settings",
    "get_settings",
    "Settings",
    
    # Database
    "db",
    "get_supabase_client",
    "get_admin_client",
    "check_connection",
    "reset_connection",
    "DatabaseSession",
    "DatabaseError",
    "ConnectionError",
]
