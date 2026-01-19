"""
Product service for business logic operations.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
import structlog

from config import get_supabase_client
from models.product import (
    ProductCreate,
    ProductUpdate,
    ProductResponse,
    Category,
    Rotation
)
from exceptions import (
    ProductNotFoundError,
    ProductSKUExistsError,
    DatabaseError
)

logger = structlog.get_logger(__name__)


class ProductService:
    """
    Product business logic.
    
    Handles CRUD operations for products.
    """
    
    def __init__(self):
        self.db = get_supabase_client()
        self.table = "products"
    
    # ===================
    # READ OPERATIONS
    # ===================
    
    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        category: Optional[Category] = None,
        rotation: Optional[Rotation] = None,
        active_only: bool = True
    ) -> tuple[list[ProductResponse], int]:
        """
        Get all products with optional filters.
        
        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            category: Filter by category
            rotation: Filter by rotation
            active_only: Only return active products
            
        Returns:
            Tuple of (products list, total count)
        """
        logger.info(
            "getting_products",
            page=page,
            page_size=page_size,
            category=category,
            rotation=rotation
        )
        
        try:
            # Build query
            query = self.db.table(self.table).select("*", count="exact")
            
            # Apply filters
            if active_only:
                query = query.eq("active", True)
            if category:
                query = query.eq("category", category.value)
            if rotation:
                query = query.eq("rotation", rotation.value)
            
            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)
            
            # Order by SKU
            query = query.order("sku")
            
            # Execute
            result = query.execute()
            
            products = [ProductResponse(**row) for row in result.data]
            total = result.count or 0
            
            logger.info(
                "products_retrieved",
                count=len(products),
                total=total
            )
            
            return products, total
            
        except Exception as e:
            logger.error(
                "get_products_failed",
                error=str(e)
            )
            raise DatabaseError("select", str(e))
    
    def get_by_id(self, product_id: str) -> ProductResponse:
        """
        Get a single product by ID.
        
        Args:
            product_id: Product UUID
            
        Returns:
            ProductResponse
            
        Raises:
            ProductNotFoundError: If product doesn't exist
        """
        logger.debug("getting_product", product_id=product_id)
        
        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("id", product_id)
                .single()
                .execute()
            )
            
            if not result.data:
                raise ProductNotFoundError(product_id)
            
            return ProductResponse(**result.data)
            
        except ProductNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "get_product_failed",
                product_id=product_id,
                error=str(e)
            )
            # Check if it's a "not found" from Supabase
            if "0 rows" in str(e) or "no rows" in str(e).lower():
                raise ProductNotFoundError(product_id)
            raise DatabaseError("select", str(e))
    
    def get_by_sku(self, sku: str) -> Optional[ProductResponse]:
        """
        Get a product by SKU.
        
        Args:
            sku: Product SKU
            
        Returns:
            ProductResponse or None if not found
        """
        logger.debug("getting_product_by_sku", sku=sku)
        
        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("sku", sku.upper())
                .execute()
            )
            
            if not result.data:
                return None
            
            return ProductResponse(**result.data[0])
            
        except Exception as e:
            logger.error(
                "get_product_by_sku_failed",
                sku=sku,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_by_factory_code(self, factory_code: str) -> Optional[ProductResponse]:
        """
        Get a product by factory code.

        Args:
            factory_code: Factory internal product code (e.g., '5495')

        Returns:
            ProductResponse or None if not found
        """
        logger.debug("getting_product_by_factory_code", factory_code=factory_code)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("factory_code", factory_code)
                .eq("active", True)
                .execute()
            )

            if not result.data:
                return None

            return ProductResponse(**result.data[0])

        except Exception as e:
            logger.error(
                "get_product_by_factory_code_failed",
                factory_code=factory_code,
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    def get_by_factory_codes(self, factory_codes: list[str]) -> list[ProductResponse]:
        """
        Get multiple products by their factory codes.

        Args:
            factory_codes: List of factory codes (e.g., ['5495', '5498'])

        Returns:
            List of ProductResponse objects (may be fewer than input if some not found)
        """
        if not factory_codes:
            return []

        logger.debug("getting_products_by_factory_codes", count=len(factory_codes))

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .in_("factory_code", factory_codes)
                .eq("active", True)
                .execute()
            )

            return [ProductResponse(**row) for row in result.data]

        except Exception as e:
            logger.error(
                "get_products_by_factory_codes_failed",
                count=len(factory_codes),
                error=str(e)
            )
            raise DatabaseError("select", str(e))

    # ===================
    # WRITE OPERATIONS
    # ===================
    
    def create(self, data: ProductCreate) -> ProductResponse:
        """
        Create a new product.
        
        Args:
            data: Product creation data
            
        Returns:
            Created ProductResponse
            
        Raises:
            ProductSKUExistsError: If SKU already exists
        """
        logger.info("creating_product", sku=data.sku)
        
        # Check for duplicate SKU
        existing = self.get_by_sku(data.sku)
        if existing:
            raise ProductSKUExistsError(data.sku)
        
        try:
            # Prepare data for insert
            insert_data = {
                "sku": data.sku,
                "category": data.category.value,
                "rotation": data.rotation.value if data.rotation else None,
                "active": True
            }
            
            result = (
                self.db.table(self.table)
                .insert(insert_data)
                .execute()
            )
            
            product = ProductResponse(**result.data[0])
            
            logger.info(
                "product_created",
                product_id=product.id,
                sku=product.sku
            )
            
            return product
            
        except Exception as e:
            logger.error(
                "create_product_failed",
                sku=data.sku,
                error=str(e)
            )
            raise DatabaseError("insert", str(e))
    
    def update(self, product_id: str, data: ProductUpdate) -> ProductResponse:
        """
        Update an existing product.
        
        Args:
            product_id: Product UUID
            data: Fields to update
            
        Returns:
            Updated ProductResponse
            
        Raises:
            ProductNotFoundError: If product doesn't exist
            ProductSKUExistsError: If new SKU already exists
        """
        logger.info("updating_product", product_id=product_id)
        
        # Check product exists
        existing = self.get_by_id(product_id)
        
        # If changing SKU, check for duplicates
        if data.sku and data.sku.upper() != existing.sku:
            sku_check = self.get_by_sku(data.sku)
            if sku_check:
                raise ProductSKUExistsError(data.sku)
        
        try:
            # Build update dict with only provided fields
            update_data = {}
            if data.sku is not None:
                update_data["sku"] = data.sku
            if data.category is not None:
                update_data["category"] = data.category.value
            if data.rotation is not None:
                update_data["rotation"] = data.rotation.value
            if data.active is not None:
                update_data["active"] = data.active
            if data.fob_cost_usd is not None:
                update_data["fob_cost_usd"] = float(data.fob_cost_usd)
            if data.factory_code is not None:
                update_data["factory_code"] = data.factory_code

            if not update_data:
                # Nothing to update, return existing
                return existing
            
            result = (
                self.db.table(self.table)
                .update(update_data)
                .eq("id", product_id)
                .execute()
            )
            
            product = ProductResponse(**result.data[0])
            
            logger.info(
                "product_updated",
                product_id=product_id,
                fields=list(update_data.keys())
            )
            
            return product
            
        except Exception as e:
            logger.error(
                "update_product_failed",
                product_id=product_id,
                error=str(e)
            )
            raise DatabaseError("update", str(e))
    
    def delete(self, product_id: str) -> bool:
        """
        Soft delete a product (set active=False).
        
        Args:
            product_id: Product UUID
            
        Returns:
            True if deleted
            
        Raises:
            ProductNotFoundError: If product doesn't exist
        """
        logger.info("deleting_product", product_id=product_id)
        
        # Check product exists
        self.get_by_id(product_id)
        
        try:
            self.db.table(self.table).update(
                {"active": False}
            ).eq("id", product_id).execute()
            
            logger.info("product_deleted", product_id=product_id)
            
            return True
            
        except Exception as e:
            logger.error(
                "delete_product_failed",
                product_id=product_id,
                error=str(e)
            )
            raise DatabaseError("update", str(e))
    
    # ===================
    # BULK OPERATIONS
    # ===================

    def bulk_upsert(self, products: list[ProductCreate]) -> tuple[int, int]:
        """
        Bulk upsert products (create if not exists, update if exists).

        Args:
            products: List of ProductCreate objects

        Returns:
            Tuple of (created_count, updated_count)
        """
        logger.info("bulk_upsert_products", count=len(products))

        created = 0
        updated = 0

        for data in products:
            try:
                existing = self.get_by_sku(data.sku)

                if existing:
                    # Update existing product
                    update_data = {
                        "category": data.category.value,
                        "active": True  # Re-activate if was inactive
                    }
                    if data.rotation:
                        update_data["rotation"] = data.rotation.value

                    self.db.table(self.table).update(update_data).eq("id", existing.id).execute()
                    updated += 1
                else:
                    # Create new product
                    insert_data = {
                        "sku": data.sku,
                        "category": data.category.value,
                        "rotation": data.rotation.value if data.rotation else None,
                        "active": True
                    }
                    self.db.table(self.table).insert(insert_data).execute()
                    created += 1

            except Exception as e:
                logger.error("bulk_upsert_product_failed", sku=data.sku, error=str(e))
                # Continue with next product
                continue

        logger.info("bulk_upsert_complete", created=created, updated=updated)
        return created, updated

    # ===================
    # UTILITY METHODS
    # ===================

    def sku_exists(self, sku: str) -> bool:
        """Check if a SKU already exists."""
        return self.get_by_sku(sku) is not None

    def count(self, active_only: bool = True) -> int:
        """Count total products."""
        try:
            query = self.db.table(self.table).select("id", count="exact")
            if active_only:
                query = query.eq("active", True)
            result = query.execute()
            return result.count or 0
        except Exception as e:
            logger.error("count_products_failed", error=str(e))
            raise DatabaseError("count", str(e))


# Singleton instance for convenience
_product_service: Optional[ProductService] = None

def get_product_service() -> ProductService:
    """Get or create ProductService instance."""
    global _product_service
    if _product_service is None:
        _product_service = ProductService()
    return _product_service
