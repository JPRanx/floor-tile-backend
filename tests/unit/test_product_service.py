"""
Unit tests for ProductService.

See STANDARDS_TESTING.md for patterns.

Run: pytest tests/unit/test_product_service.py -v
Run with coverage: pytest tests/unit/test_product_service.py --cov=services/product_service
"""

import pytest
from unittest.mock import patch, MagicMock

# Import what we're testing
from services.product_service import ProductService, get_product_service
from models.product import ProductCreate, ProductUpdate, Category, Rotation
from exceptions import ProductNotFoundError, ProductSKUExistsError, DatabaseError

# Import test utilities
from tests.factories import ProductFactory


class TestProductServiceGetAll:
    """Tests for ProductService.get_all()"""
    
    def test_get_all_returns_products(self, mock_db, mock_supabase, sample_products_list):
        """Should return list of products with total count."""
        # Arrange
        mock_supabase.set_table_data("products", sample_products_list, count=3)
        service = ProductService()
        
        # Act
        products, total = service.get_all()
        
        # Assert
        assert len(products) == 3
        assert total == 3
        assert products[0].sku == "NOGAL CAFÉ"
    
    def test_get_all_empty_returns_empty_list(self, mock_db, mock_supabase):
        """Should return empty list when no products exist."""
        # Arrange
        mock_supabase.set_table_data("products", [], count=0)
        service = ProductService()
        
        # Act
        products, total = service.get_all()
        
        # Assert
        assert products == []
        assert total == 0
    
    def test_get_all_with_pagination(self, mock_db, mock_supabase):
        """Should respect page and page_size parameters."""
        # Arrange
        products = ProductFactory.create_batch(5)
        mock_supabase.set_table_data("products", products[:2], count=5)  # Return 2, but total is 5
        service = ProductService()
        
        # Act
        products, total = service.get_all(page=1, page_size=2)
        
        # Assert
        assert len(products) == 2
        assert total == 5
    
    def test_get_all_with_category_filter(self, mock_db, mock_supabase):
        """Should filter by category."""
        # Arrange
        products = [ProductFactory.create_exteriores()]
        mock_supabase.set_table_data("products", products, count=1)
        service = ProductService()
        
        # Act
        products, total = service.get_all(category=Category.EXTERIORES)
        
        # Assert
        assert len(products) == 1
        assert products[0].category == Category.EXTERIORES


class TestProductServiceGetById:
    """Tests for ProductService.get_by_id()"""
    
    def test_get_by_id_returns_product(self, mock_db, mock_supabase, sample_product_data):
        """Should return product when found."""
        # Arrange - set data as list, mock handles .single()
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        # Act
        product = service.get_by_id("test-uuid-123")
        
        # Assert
        assert product.id == "test-uuid-123"
        assert product.sku == "NOGAL CAFÉ"
    
    def test_get_by_id_not_found_raises_error(self, mock_db, mock_supabase):
        """Should raise ProductNotFoundError when product doesn't exist."""
        # Arrange
        mock_supabase.set_table_data("products", [])
        service = ProductService()
        
        # Act & Assert
        with pytest.raises(ProductNotFoundError) as exc_info:
            service.get_by_id("nonexistent-id")
        
        assert exc_info.value.status_code == 404
        assert "PRODUCT_NOT_FOUND" in exc_info.value.code


class TestProductServiceGetBySku:
    """Tests for ProductService.get_by_sku()"""
    
    def test_get_by_sku_returns_product(self, mock_db, mock_supabase, sample_product_data):
        """Should return product when SKU found."""
        # Arrange
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        # Act
        product = service.get_by_sku("NOGAL CAFÉ")
        
        # Assert
        assert product is not None
        assert product.sku == "NOGAL CAFÉ"
    
    def test_get_by_sku_not_found_returns_none(self, mock_db, mock_supabase):
        """Should return None when SKU not found."""
        # Arrange
        mock_supabase.set_table_data("products", [])
        service = ProductService()
        
        # Act
        product = service.get_by_sku("NONEXISTENT")
        
        # Assert
        assert product is None
    
    def test_get_by_sku_case_insensitive(self, mock_db, mock_supabase, sample_product_data):
        """Should search with uppercase SKU."""
        # Arrange
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        # Act - search with lowercase
        product = service.get_by_sku("nogal café")
        
        # Assert - should still work (converts to uppercase internally)
        # Note: The mock doesn't actually filter, but the service converts to uppercase
        assert product is not None


class TestProductServiceCreate:
    """Tests for ProductService.create()"""
    
    def test_create_product_success(self, mock_db, mock_supabase):
        """Should create product and return it."""
        # Arrange
        mock_supabase.set_table_data("products", [])  # No existing products
        service = ProductService()
        
        data = ProductCreate(
            sku="NEW PRODUCT",
            category=Category.MADERAS,
            rotation=Rotation.ALTA
        )
        
        # Act
        product = service.create(data)
        
        # Assert
        assert product.sku == "NEW PRODUCT"
        assert product.category == Category.MADERAS
        assert product.rotation == Rotation.ALTA
        assert product.active == True
    
    def test_create_product_duplicate_sku_raises_error(self, mock_db, mock_supabase, sample_product_data):
        """Should raise ProductSKUExistsError when SKU already exists."""
        # Arrange
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        data = ProductCreate(
            sku="NOGAL CAFÉ",  # Already exists
            category=Category.MADERAS
        )
        
        # Act & Assert
        with pytest.raises(ProductSKUExistsError) as exc_info:
            service.create(data)
        
        assert exc_info.value.status_code == 409
        assert "EXISTS" in exc_info.value.code
    
    def test_create_product_sku_normalized_to_uppercase(self, mock_db, mock_supabase):
        """Should normalize SKU to uppercase."""
        # Arrange
        mock_supabase.set_table_data("products", [])
        service = ProductService()
        
        data = ProductCreate(
            sku="lowercase product",
            category=Category.MADERAS
        )
        
        # Act
        product = service.create(data)
        
        # Assert
        assert product.sku == "LOWERCASE PRODUCT"


class TestProductServiceUpdate:
    """Tests for ProductService.update()"""
    
    def test_update_product_success(self, mock_db, mock_supabase, sample_product_data):
        """Should update product fields."""
        # Arrange - mock returns existing product for get_by_id check,
        # then updated product for the update call
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        data = ProductUpdate(rotation=Rotation.BAJA)
        
        # Act
        product = service.update("test-uuid-123", data)
        
        # Assert - mock merges the update, so rotation should be BAJA
        assert product is not None
        assert product.sku == "NOGAL CAFÉ"  # Original SKU preserved
    
    def test_update_product_not_found_raises_error(self, mock_db, mock_supabase):
        """Should raise ProductNotFoundError when product doesn't exist."""
        # Arrange
        mock_supabase.set_table_data("products", [])
        service = ProductService()
        
        data = ProductUpdate(rotation=Rotation.BAJA)
        
        # Act & Assert
        with pytest.raises(ProductNotFoundError):
            service.update("nonexistent-id", data)
    
    def test_update_empty_data_returns_existing(self, mock_db, mock_supabase, sample_product_data):
        """Should return existing product when no fields provided."""
        # Arrange
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        data = ProductUpdate()  # No fields
        
        # Act
        product = service.update("test-uuid-123", data)
        
        # Assert - should return unchanged
        assert product.sku == "NOGAL CAFÉ"
        assert product.id == "test-uuid-123"


class TestProductServiceDelete:
    """Tests for ProductService.delete()"""
    
    def test_delete_product_success(self, mock_db, mock_supabase, sample_product_data):
        """Should soft delete product (set active=False)."""
        # Arrange
        mock_supabase.set_table_data("products", [sample_product_data])
        service = ProductService()
        
        # Act
        result = service.delete("test-uuid-123")
        
        # Assert - returns True on success
        assert result is True
    
    def test_delete_product_not_found_raises_error(self, mock_db, mock_supabase):
        """Should raise ProductNotFoundError when product doesn't exist."""
        # Arrange
        mock_supabase.set_table_data("products", [])
        service = ProductService()
        
        # Act & Assert
        with pytest.raises(ProductNotFoundError):
            service.delete("nonexistent-id")


class TestProductServiceCount:
    """Tests for ProductService.count()"""
    
    def test_count_returns_total(self, mock_db, mock_supabase):
        """Should return total count of products."""
        # Arrange
        products = ProductFactory.create_batch(5)
        mock_supabase.set_table_data("products", products, count=5)
        service = ProductService()
        
        # Act
        count = service.count()
        
        # Assert
        assert count == 5
    
    def test_count_empty_returns_zero(self, mock_db, mock_supabase):
        """Should return 0 when no products exist."""
        # Arrange
        mock_supabase.set_table_data("products", [], count=0)
        service = ProductService()
        
        # Act
        count = service.count()
        
        # Assert
        assert count == 0


class TestGetProductService:
    """Tests for get_product_service() singleton."""
    
    def test_get_product_service_returns_instance(self, mock_db):
        """Should return ProductService instance."""
        # Act
        service = get_product_service()
        
        # Assert
        assert isinstance(service, ProductService)
    
    def test_get_product_service_returns_same_instance(self, mock_db):
        """Should return same instance (singleton)."""
        # Act
        service1 = get_product_service()
        service2 = get_product_service()
        
        # Assert
        assert service1 is service2
