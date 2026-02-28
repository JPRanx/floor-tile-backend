"""
Test data factories.

See STANDARDS_TESTING.md for patterns.
Uses factory pattern to generate consistent test data.
"""

from datetime import date, datetime, timedelta
from typing import Optional
from uuid import uuid4

from models.product import Category, Rotation


class ProductFactory:
    """
    Factory for creating test Product data.
    
    Usage:
        # Create with defaults
        product = ProductFactory.create()
        
        # Create with overrides
        product = ProductFactory.create(sku="CUSTOM SKU", category="EXTERIORES")
        
        # Create multiple
        products = ProductFactory.create_batch(5)
    """
    
    _counter = 0
    
    @classmethod
    def _next_counter(cls) -> int:
        cls._counter += 1
        return cls._counter
    
    @classmethod
    def create(
        cls,
        id: Optional[str] = None,
        sku: Optional[str] = None,
        category: Optional[str] = None,
        rotation: Optional[str] = None,
        active: bool = True,
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None
    ) -> dict:
        """
        Create a single product dict.
        
        Args:
            id: Product UUID (auto-generated if not provided)
            sku: Product SKU (auto-generated if not provided)
            category: MADERAS, EXTERIORES, or MARMOLIZADOS
            rotation: ALTA, MEDIA-ALTA, MEDIA, or BAJA
            active: Whether product is active
            created_at: Timestamp (auto-generated if not provided)
            updated_at: Timestamp (auto-generated if not provided)
            
        Returns:
            Product dict matching database schema
        """
        counter = cls._next_counter()
        now = datetime.utcnow().isoformat() + "Z"
        
        return {
            "id": id or str(uuid4()),
            "sku": sku or f"TEST PRODUCT {counter}",
            "category": category or "MADERAS",
            "rotation": rotation,
            "active": active,
            "created_at": created_at or now,
            "updated_at": updated_at or now
        }
    
    @classmethod
    def create_batch(cls, count: int, **overrides) -> list:
        """
        Create multiple products.
        
        Args:
            count: Number of products to create
            **overrides: Fields to override on all products
            
        Returns:
            List of product dicts
        """
        return [cls.create(**overrides) for _ in range(count)]
    
    @classmethod
    def create_maderas(cls, **overrides) -> dict:
        """Create a MADERAS category product."""
        return cls.create(category="MADERAS", **overrides)
    
    @classmethod
    def create_exteriores(cls, **overrides) -> dict:
        """Create an EXTERIORES category product."""
        return cls.create(category="EXTERIORES", **overrides)
    
    @classmethod
    def create_marmolizados(cls, **overrides) -> dict:
        """Create a MARMOLIZADOS category product."""
        return cls.create(category="MARMOLIZADOS", **overrides)
    
    @classmethod
    def create_alta_rotation(cls, **overrides) -> dict:
        """Create a high rotation product."""
        return cls.create(rotation="ALTA", **overrides)
    
    @classmethod
    def create_inactive(cls, **overrides) -> dict:
        """Create an inactive product."""
        return cls.create(active=False, **overrides)
    
    @classmethod
    def reset_counter(cls):
        """Reset the counter (call in test setup if needed)."""
        cls._counter = 0


class InventoryFactory:
    """
    Factory for creating test Inventory data.
    
    Builder can copy this pattern for inventory tests.
    """
    
    _counter = 0
    
    @classmethod
    def _next_counter(cls) -> int:
        cls._counter += 1
        return cls._counter
    
    @classmethod
    def create(
        cls,
        id: Optional[str] = None,
        product_id: Optional[str] = None,
        warehouse_qty: float = 1000.0,
        factory_qty: float = 500.0,
        snapshot_date: Optional[str] = None,
        created_at: Optional[str] = None
    ) -> dict:
        """Create a single inventory snapshot dict."""
        now = datetime.utcnow().isoformat() + "Z"
        today = datetime.utcnow().strftime("%Y-%m-%d")
        
        return {
            "id": id or str(uuid4()),
            "product_id": product_id or str(uuid4()),
            "warehouse_qty": warehouse_qty,
            "factory_qty": factory_qty,
            "snapshot_date": snapshot_date or today,
            "created_at": created_at or now
        }
    
    @classmethod
    def create_batch(cls, count: int, **overrides) -> list:
        """Create multiple inventory snapshots."""
        return [cls.create(**overrides) for _ in range(count)]
    
    @classmethod
    def reset_counter(cls):
        """Reset the counter."""
        cls._counter = 0


class SalesFactory:
    """
    Factory for creating test Sales data.
    
    Builder can copy this pattern for sales tests.
    """
    
    @classmethod
    def create(
        cls,
        id: Optional[str] = None,
        product_id: Optional[str] = None,
        quantity: float = 50.0,
        sale_date: Optional[str] = None,
        created_at: Optional[str] = None
    ) -> dict:
        """Create a single sale dict."""
        now = datetime.utcnow().isoformat() + "Z"
        today = datetime.utcnow().strftime("%Y-%m-%d")
        
        return {
            "id": id or str(uuid4()),
            "product_id": product_id or str(uuid4()),
            "quantity": quantity,
            "sale_date": sale_date or today,
            "created_at": created_at or now
        }
    
    @classmethod
    def create_batch(cls, count: int, **overrides) -> list:
        """Create multiple sales."""
        return [cls.create(**overrides) for _ in range(count)]


class BoatFactory:
    """Factory for creating test boat schedule data."""

    _counter = 0

    @classmethod
    def _next_counter(cls) -> int:
        cls._counter += 1
        return cls._counter

    @classmethod
    def create(
        cls,
        id: Optional[str] = None,
        vessel_name: Optional[str] = None,
        departure_date: Optional[str] = None,
        arrival_date: Optional[str] = None,
        origin_port: str = "Barranquilla",
        carrier: str = "TIBA",
        status: str = "available",
        max_containers: int = 5,
        shipping_line: Optional[str] = None,
    ) -> dict:
        """Create a single boat schedule dict."""
        counter = cls._next_counter()
        dep = departure_date or (date.today() + timedelta(days=30)).isoformat()
        arr = arrival_date or (date.today() + timedelta(days=45)).isoformat()

        return {
            "id": id or str(uuid4()),
            "vessel_name": vessel_name or f"Test Vessel {counter}",
            "departure_date": dep,
            "arrival_date": arr,
            "origin_port": origin_port,
            "carrier": carrier,
            "status": status,
            "max_containers": max_containers,
            "shipping_line": shipping_line or carrier,
        }

    @classmethod
    def reset_counter(cls):
        cls._counter = 0


class ProductionScheduleFactory:
    """Factory for creating test production schedule data."""

    @classmethod
    def create(
        cls,
        id: Optional[str] = None,
        product_id: Optional[str] = None,
        status: str = "in_progress",
        requested_m2: float = 2000.0,
        completed_m2: float = 0.0,
        estimated_delivery_date: Optional[str] = None,
    ) -> dict:
        """Create a single production schedule row."""
        delivery = estimated_delivery_date or (date.today() + timedelta(days=30)).isoformat()

        return {
            "id": id or str(uuid4()),
            "product_id": product_id or str(uuid4()),
            "status": status,
            "requested_m2": requested_m2,
            "completed_m2": completed_m2,
            "estimated_delivery_date": delivery,
        }
