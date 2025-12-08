"""
Product API routes.

See BUILDER_BLUEPRINT.md for endpoint specifications.
See STANDARDS_ERRORS.md for error response format.
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.product import (
    ProductCreate,
    ProductUpdate,
    ProductResponse,
    ProductListResponse,
    Category,
    Rotation
)
from services.product_service import get_product_service
from exceptions import (
    AppError,
    ProductNotFoundError,
    ProductSKUExistsError
)

logger = structlog.get_logger(__name__)

router = APIRouter()


# ===================
# EXCEPTION HANDLER
# ===================

def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
    # Unexpected error
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


# ===================
# ROUTES
# ===================

@router.get("", response_model=ProductListResponse)
async def list_products(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    category: Optional[Category] = Query(None, description="Filter by category"),
    rotation: Optional[Rotation] = Query(None, description="Filter by rotation"),
    include_inactive: bool = Query(False, description="Include inactive products")
):
    """
    List all products with optional filters.
    
    Returns paginated list of products.
    """
    try:
        service = get_product_service()
        
        products, total = service.get_all(
            page=page,
            page_size=page_size,
            category=category,
            rotation=rotation,
            active_only=not include_inactive
        )
        
        total_pages = (total + page_size - 1) // page_size
        
        return ProductListResponse(
            data=products,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
    except Exception as e:
        return handle_error(e)


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(product_id: str):
    """
    Get a single product by ID.
    
    Raises:
        404: Product not found
    """
    try:
        service = get_product_service()
        return service.get_by_id(product_id)
        
    except ProductNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.post("", response_model=ProductResponse, status_code=201)
async def create_product(data: ProductCreate):
    """
    Create a new product.
    
    Raises:
        409: SKU already exists
        422: Validation error
    """
    try:
        service = get_product_service()
        return service.create(data)
        
    except ProductSKUExistsError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.patch("/{product_id}", response_model=ProductResponse)
async def update_product(product_id: str, data: ProductUpdate):
    """
    Update an existing product.
    
    Only provided fields are updated.
    
    Raises:
        404: Product not found
        409: New SKU already exists
        422: Validation error
    """
    try:
        service = get_product_service()
        return service.update(product_id, data)
        
    except (ProductNotFoundError, ProductSKUExistsError) as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.delete("/{product_id}", status_code=204)
async def delete_product(product_id: str):
    """
    Delete a product (soft delete).
    
    Sets active=False rather than removing from database.
    
    Raises:
        404: Product not found
    """
    try:
        service = get_product_service()
        service.delete(product_id)
        return None  # 204 No Content
        
    except ProductNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


# ===================
# UTILITY ROUTES
# ===================

@router.get("/sku/{sku}", response_model=ProductResponse)
async def get_product_by_sku(sku: str):
    """
    Get a product by SKU.
    
    Raises:
        404: Product not found
    """
    try:
        service = get_product_service()
        product = service.get_by_sku(sku)
        
        if not product:
            raise ProductNotFoundError(sku)
        
        return product
        
    except ProductNotFoundError as e:
        return handle_error(e)
    except Exception as e:
        return handle_error(e)


@router.get("/count/total")
async def count_products(
    include_inactive: bool = Query(False, description="Include inactive")
):
    """Get total product count."""
    try:
        service = get_product_service()
        count = service.count(active_only=not include_inactive)
        return {"count": count}
        
    except Exception as e:
        return handle_error(e)
