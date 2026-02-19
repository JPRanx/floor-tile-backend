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
    LiquidationProductResponse,
    Category,
    Rotation,
    InactiveReason,
    TILE_CATEGORIES,
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

    def get_all_active_tiles(self) -> list[ProductResponse]:
        """
        Get all active tile products (excludes FURNITURE, SINK, SURCHARGE).

        Used by services that should only process tile products.

        Returns:
            List of active tile ProductResponse objects
        """
        logger.debug("getting_all_active_tiles")

        try:
            tile_categories = [cat.value for cat in TILE_CATEGORIES]

            result = (
                self.db.table(self.table)
                .select("*")
                .eq("active", True)
                .in_("category", tile_categories)
                .order("sku")
                .execute()
            )

            products = [ProductResponse(**row) for row in result.data]

            logger.debug("active_tiles_retrieved", count=len(products))
            return products

        except Exception as e:
            logger.error("get_all_active_tiles_failed", error=str(e))
            raise DatabaseError("select", str(e))

    def get_active_products_for_factory(self, factory_id: str) -> list[ProductResponse]:
        """
        Get all active products for a specific factory.

        Unlike get_all_active_tiles() which filters by tile categories,
        this returns ALL active products for the factory regardless of category.
        Used for factory-aware order building (supports both tiles and furniture).

        Args:
            factory_id: Factory UUID

        Returns:
            List of active ProductResponse objects for the factory
        """
        logger.debug("getting_active_products_for_factory", factory_id=factory_id)

        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("factory_id", factory_id)
                .eq("active", True)
                .order("sku")
                .execute()
            )

            products = [ProductResponse(**row) for row in result.data]

            logger.debug(
                "active_products_for_factory_retrieved",
                factory_id=factory_id,
                count=len(products),
            )
            return products

        except Exception as e:
            logger.error(
                "get_active_products_for_factory_failed",
                factory_id=factory_id,
                error=str(e),
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
                # If reactivating, clear inactive fields
                if data.active is True:
                    update_data["inactive_reason"] = None
                    update_data["inactive_date"] = None
            if data.fob_cost_usd is not None:
                update_data["fob_cost_usd"] = float(data.fob_cost_usd)
            if data.factory_code is not None:
                update_data["factory_code"] = data.factory_code
            if data.inactive_reason is not None:
                update_data["inactive_reason"] = data.inactive_reason.value
            if data.inactive_date is not None:
                update_data["inactive_date"] = data.inactive_date.isoformat()

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

    def bulk_update_status(
        self,
        product_ids: list[str],
        active: bool,
        inactive_reason: InactiveReason | None = None,
        inactive_date: str | None = None
    ) -> tuple[int, list[str]]:
        """
        Bulk update product active status.

        Args:
            product_ids: List of product UUIDs to update
            active: Whether to activate (True) or deactivate (False)
            inactive_reason: Reason for deactivation (required if active=False)
            inactive_date: Date of deactivation (YYYY-MM-DD format)

        Returns:
            Tuple of (updated_count, failed_ids)
        """
        logger.info(
            "bulk_update_status",
            count=len(product_ids),
            active=active,
            reason=inactive_reason.value if inactive_reason else None
        )

        updated = 0
        failed_ids = []

        for product_id in product_ids:
            try:
                if active:
                    # Reactivate: clear inactive fields
                    update_data = {
                        "active": True,
                        "inactive_reason": None,
                        "inactive_date": None
                    }
                else:
                    # Deactivate: set reason and date
                    update_data = {
                        "active": False,
                        "inactive_reason": inactive_reason.value if inactive_reason else None,
                        "inactive_date": inactive_date
                    }

                self.db.table(self.table).update(update_data).eq("id", product_id).execute()
                updated += 1

            except Exception as e:
                logger.error(
                    "bulk_update_status_failed",
                    product_id=product_id,
                    error=str(e)
                )
                failed_ids.append(product_id)

        logger.info(
            "bulk_update_status_complete",
            updated=updated,
            failed=len(failed_ids)
        )
        return updated, failed_ids

    # ===================
    # UTILITY METHODS
    # ===================

    def search(self, query: str, limit: int = 10) -> list[ProductResponse]:
        """Search products by SKU or referencia substring match."""
        try:
            result = (
                self.db.table(self.table)
                .select("*")
                .eq("active", True)
                .ilike("sku", f"%{query}%")
                .order("sku")
                .limit(limit)
                .execute()
            )
            return [ProductResponse(**row) for row in result.data]
        except Exception as e:
            logger.error("search_products_failed", error=str(e), query=query)
            raise DatabaseError("search", str(e))

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

    def get_liquidation_products(self) -> list[LiquidationProductResponse]:
        """Get deactivated products that still have warehouse inventory."""
        logger.info("getting_liquidation_products")

        try:
            # 1. Query products where active=false
            products_result = (
                self.db.table(self.table)
                .select("*")
                .eq("active", False)
                .execute()
            )
            if not products_result.data:
                logger.info("liquidation_products_retrieved", count=0)
                return []

            # 2. Get latest inventory from inventory_current view (no dedup needed)
            inventory_result = (
                self.db.table("inventory_current")
                .select("product_id, warehouse_qty, factory_available_m2, snapshot_date")
                .execute()
            )

            latest_inventory = {
                row["product_id"]: {
                    "warehouse_m2": float(row.get("warehouse_qty") or 0),
                    "factory_m2": float(row.get("factory_available_m2") or 0),
                }
                for row in (inventory_result.data or [])
            }

            # 3. Filter inactive products to those with warehouse_qty > 0 OR factory_m2 > 0
            candidates = []
            for product in products_result.data:
                pid = product["id"]
                inv = latest_inventory.get(pid, {})
                warehouse_m2 = inv.get("warehouse_m2", 0.0)
                factory_m2 = inv.get("factory_m2", 0.0)
                if warehouse_m2 > 0 or factory_m2 > 0:
                    candidates.append((product, warehouse_m2, factory_m2))

            if not candidates:
                logger.info("liquidation_products_retrieved", count=0)
                return []

            # 4. For each candidate, query max(sales.week_start) to get days_since_last_sale
            from datetime import date as date_type
            candidate_ids = [c[0]["id"] for c in candidates]

            sales_result = (
                self.db.table("sales")
                .select("product_id, week_start")
                .in_("product_id", candidate_ids)
                .order("week_start", desc=True)
                .execute()
            )

            # Build dict of latest sale date per product
            latest_sale = {}
            for row in (sales_result.data or []):
                pid = row["product_id"]
                if pid not in latest_sale:
                    latest_sale[pid] = row["week_start"]

            # 5. Build response list, ordered by warehouse_m2 DESC
            today = date_type.today()
            results = []
            for product, warehouse_m2, factory_m2 in candidates:
                pid = product["id"]
                days_since = None
                if pid in latest_sale:
                    last_sale_date = date_type.fromisoformat(latest_sale[pid])
                    days_since = (today - last_sale_date).days

                results.append(LiquidationProductResponse(
                    id=pid,
                    sku=product["sku"],
                    category=product["category"],
                    rotation=product.get("rotation"),
                    inactive_reason=product.get("inactive_reason"),
                    inactive_date=product.get("inactive_date"),
                    warehouse_m2=warehouse_m2,
                    factory_m2=factory_m2,
                    days_since_last_sale=days_since,
                ))

            # Sort by warehouse_m2 descending
            results.sort(key=lambda x: x.warehouse_m2, reverse=True)

            logger.info("liquidation_products_retrieved", count=len(results))
            return results

        except Exception as e:
            logger.error("get_liquidation_products_failed", error=str(e))
            raise DatabaseError("select", str(e))


# Singleton instance for convenience
_product_service: Optional[ProductService] = None

def get_product_service() -> ProductService:
    """Get or create ProductService instance."""
    global _product_service
    if _product_service is None:
        _product_service = ProductService()
    return _product_service
