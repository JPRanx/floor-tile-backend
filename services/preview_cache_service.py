"""
Temporary storage for upload previews.
Stores parsed data in memory with TTL expiration.
Single-server only (Ashley is the only user).
"""
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

_cache: dict[str, tuple[datetime, Any]] = {}
DEFAULT_TTL_MINUTES = 30


def store_preview(data: Any, ttl_minutes: int = DEFAULT_TTL_MINUTES) -> str:
    """Store parsed data, return preview_id."""
    preview_id = str(uuid.uuid4())
    expires_at = datetime.now() + timedelta(minutes=ttl_minutes)
    _cache[preview_id] = (expires_at, data)
    _cleanup_expired()
    return preview_id


def retrieve_preview(preview_id: str) -> Optional[Any]:
    """Retrieve parsed data by preview_id. Returns None if expired/not found."""
    entry = _cache.get(preview_id)
    if entry is None:
        return None
    expires_at, data = entry
    if datetime.now() > expires_at:
        del _cache[preview_id]
        return None
    return data


def delete_preview(preview_id: str) -> None:
    """Remove preview after confirm or cancel."""
    _cache.pop(preview_id, None)


def _cleanup_expired() -> None:
    """Remove all expired entries."""
    now = datetime.now()
    expired = [k for k, (exp, _) in _cache.items() if now > exp]
    for k in expired:
        del _cache[k]
