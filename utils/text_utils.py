"""
Text utilities for handling Spanish text with accents.

Used for customer name normalization and comparison.
"""

import unicodedata
from typing import Optional


def normalize_customer_name(name: Optional[str]) -> Optional[str]:
    """
    Normalize customer name for comparison/grouping.

    Handles Spanish accents and special characters:
    - "Decoración García" → "DECORACION GARCIA"
    - "José's Tiles" → "JOSE'S TILES"
    - "  PISOS S.A.  " → "PISOS S.A."

    Args:
        name: Original customer name (may have accents, mixed case)

    Returns:
        Normalized uppercase ASCII string, or None if input is empty
    """
    if not name:
        return None

    # Strip whitespace
    name = name.strip()

    if not name:
        return None

    # Normalize unicode (NFD decomposition separates base chars from accents)
    normalized = unicodedata.normalize('NFD', name)

    # Remove accent marks (combining characters in Unicode category 'Mn')
    ascii_name = ''.join(
        c for c in normalized
        if unicodedata.category(c) != 'Mn'
    )

    # Uppercase for consistent comparison
    return ascii_name.upper()


def clean_customer_name(name: Optional[str], max_length: int = 255) -> Optional[str]:
    """
    Clean customer name for storage (preserves accents).

    - Strips whitespace
    - Truncates to max length
    - Returns None for empty/whitespace-only strings

    Args:
        name: Raw customer name from Excel
        max_length: Maximum characters to store

    Returns:
        Cleaned name or None
    """
    if not name:
        return None

    # Strip whitespace
    name = name.strip()

    if not name:
        return None

    # Truncate if too long
    if len(name) > max_length:
        name = name[:max_length]

    return name
