"""
Text utilities for handling Spanish text with accents.

Used for customer name normalization, product matching, and comparison.
"""

import re
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


# Common prefixes to strip from SAC product descriptions
_SAC_PREFIXES = [
    r"^BALDOSAS\s+CERAMICAS\s*/\s*",  # "BALDOSAS CERAMICAS / "
    r"^PISO\s+\d+X\d+\s+",             # "PISO 45X45 "
    r"^PISO\s+",                        # "PISO "
    r"^CERAMICA\s+\d+X\d+\s+",         # "CERAMICA 45X45 "
    r"^CERAMICA\s+",                    # "CERAMICA "
]

# Common suffixes to strip from SAC product descriptions
_SAC_SUFFIXES = [
    r"\s*\(T\)\s*\d+X\d+-?\d*$",  # " (T) 51X51-1", " (T) 45X45"
    r"\s*\d+X\d+\s*-?\s*\d*$",    # " 51X51-1", " 45X45"
]


def normalize_product_name(name: Optional[str]) -> Optional[str]:
    """
    Normalize product name for matching across ERP systems.

    Handles SAC product descriptions like:
    - "BALDOSAS CERAMICAS / NOGAL CAFE BTE (T) 51X51-1" → "NOGAL CAFE BTE"
    - "PISO 45X45 NOGAL CAFÉ" → "NOGAL CAFE"
    - "CEIBA GRIS OSCURO" → "CEIBA GRIS OSCURO"
    - "  saman beige  " → "SAMAN BEIGE"

    Steps:
    1. Strip whitespace
    2. Remove common prefixes (BALDOSAS CERAMICAS /, PISO 45X45, etc.)
    3. Remove common suffixes ((T) 51X51-1, etc.)
    4. Remove accents (é → e, ñ → n)
    5. Uppercase for consistent comparison

    Args:
        name: Original product name (may have prefixes, suffixes, accents)

    Returns:
        Normalized uppercase ASCII string, or None if input is empty
    """
    if not name:
        return None

    # Strip whitespace
    name = name.strip()

    if not name:
        return None

    # Remove common prefixes (case-insensitive)
    for prefix_pattern in _SAC_PREFIXES:
        name = re.sub(prefix_pattern, "", name, flags=re.IGNORECASE)

    # Remove common suffixes (case-insensitive)
    for suffix_pattern in _SAC_SUFFIXES:
        name = re.sub(suffix_pattern, "", name, flags=re.IGNORECASE)

    # Strip again after prefix/suffix removal
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
