"""
Config API routes.

Exposes system configuration for frontend and debugging.
"""
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.config_service import get_config_service

router = APIRouter()


# ===================
# REQUEST MODELS
# ===================

class SettingUpdate(BaseModel):
    value: str


class ProductTypeUpdate(BaseModel):
    display_name: Optional[str] = None
    m2_per_pallet: Optional[float] = None
    weight_per_m2_kg: Optional[float] = None
    is_m2_based: Optional[bool] = None
    unit_label: Optional[str] = None
    notes: Optional[str] = None


class ProductTypeCreate(BaseModel):
    category_group: str
    display_name: str
    m2_per_pallet: float = 0
    weight_per_m2_kg: float = 0
    is_m2_based: bool = True
    unit_label: str = "m²"
    notes: Optional[str] = None


# ===================
# READ
# ===================

@router.get("")
async def get_config():
    """Get all system configuration."""
    service = get_config_service()
    return {
        "global": service.get_all_global(),
        "product_types": service.get_all_product_types(),
    }


# ===================
# WRITE — Global Settings
# ===================

@router.put("/settings/{key}")
async def update_setting(key: str, data: SettingUpdate):
    """Update a single global setting value."""
    service = get_config_service()
    current = service.get(key)
    if current is None:
        raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")
    service.db.table("settings").update({"value": data.value}).eq("key", key).execute()
    service.reload()
    return {"key": key, "value": data.value}


# ===================
# WRITE — Product Types
# ===================

@router.put("/product-types/{category_group}")
async def update_product_type(category_group: str, data: ProductTypeUpdate):
    """Update an existing product type config."""
    service = get_config_service()
    existing = service.get_product_type(category_group)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Product type '{category_group}' not found")
    updates = data.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    service.db.table("product_type_configs").update(updates).eq("category_group", category_group).execute()
    service.reload()
    return service.get_product_type(category_group)


@router.post("/product-types", status_code=201)
async def create_product_type(data: ProductTypeCreate):
    """Create a new product type config."""
    service = get_config_service()
    existing = service.get_product_type(data.category_group)
    if existing is not None:
        raise HTTPException(status_code=409, detail=f"Product type '{data.category_group}' already exists")
    row = data.model_dump()
    service.db.table("product_type_configs").insert(row).execute()
    service.reload()
    return service.get_product_type(data.category_group)


# ===================
# CACHE
# ===================

@router.post("/reload")
async def reload_config():
    """Force reload config from database."""
    service = get_config_service()
    service.reload()
    return {"status": "reloaded"}
