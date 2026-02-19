"""Unit configuration service for factory-aware calculations.

Provides factory-specific unit configuration to support both
tile-based (m2) and unit-based (furniture) product types.

Each factory has a unit_type column ('m2' or 'units') that determines
how inventory, velocity, and pallet conversions are interpreted.
"""

import structlog

logger = structlog.get_logger(__name__)

# Cache for unit configs by factory_id
_config_cache: dict[str, dict] = {}


def get_unit_config(db, factory_id: str) -> dict:
    """
    Get unit configuration for a factory.

    Args:
        db: Supabase client instance
        factory_id: Factory UUID

    Returns:
        Dict with:
            is_m2_based: bool
            unit_label: str (e.g., "m2" or "unidades")
            m2_per_pallet: Decimal (for tiles) or None (for unit-based)
            weight_per_m2_kg: Decimal (for tiles) or None (for unit-based)
    """
    if factory_id in _config_cache:
        return _config_cache[factory_id]

    # Get factory unit_type
    factory_result = (
        db.table("factories")
        .select("unit_type")
        .eq("id", factory_id)
        .single()
        .execute()
    )
    unit_type = factory_result.data.get("unit_type", "m2") if factory_result.data else "m2"

    if unit_type == "m2":
        # Standard tile config
        from config.shipping import M2_PER_PALLET, DEFAULT_WEIGHT_PER_M2_KG

        config = {
            "is_m2_based": True,
            "unit_label": "m\u00b2",
            "m2_per_pallet": M2_PER_PALLET,
            "weight_per_m2_kg": DEFAULT_WEIGHT_PER_M2_KG,
        }
    else:
        # Unit-based (furniture)
        config = {
            "is_m2_based": False,
            "unit_label": "unidades",
            "m2_per_pallet": None,
            "weight_per_m2_kg": None,
        }

    _config_cache[factory_id] = config
    logger.debug(
        "unit_config_loaded",
        factory_id=factory_id,
        unit_type=unit_type,
        is_m2_based=config["is_m2_based"],
    )
    return config


def clear_cache():
    """Clear unit config cache (call when settings change)."""
    _config_cache.clear()
    logger.debug("unit_config_cache_cleared")
