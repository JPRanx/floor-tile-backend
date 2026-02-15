"""Manual mapping model for resolving unmatched items during upload preview."""

from pydantic import BaseModel


class ManualMapping(BaseModel):
    """User-provided mapping for an unmatched item to an existing product."""
    original_key: str       # The unmatched identifier (e.g. SIESA description, referencia)
    mapped_product_id: str  # The product UUID the user selected
