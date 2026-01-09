"""
Shared test fixtures.

See STANDARDS_TESTING.md for patterns.
"""

import sys
from pathlib import Path

# Add backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from typing import Generator

# ===================
# MOCK SUPABASE CLIENT
# ===================

class MockSupabaseResponse:
    """Mock Supabase query response."""
    
    def __init__(self, data: list = None, count: int = None):
        self.data = data or []
        self.count = count if count is not None else len(self.data)


class MockSupabaseQuery:
    """Mock Supabase query builder with chainable methods."""
    
    def __init__(self, data: list = None, count: int = None):
        self._data = data or []
        self._count = count
        self._is_single = False
    
    def select(self, *args, **kwargs):
        return self
    
    def insert(self, data):
        # Simulate insert - add id and timestamps
        if isinstance(data, dict):
            data = [data]
        for item in data:
            item["id"] = "test-uuid-123"
            item["created_at"] = datetime.utcnow().isoformat() + "Z"
            item["updated_at"] = datetime.utcnow().isoformat() + "Z"
            item.setdefault("active", True)
        self._data = data
        return self
    
    def update(self, data):
        # Simulate update - merge with existing data
        updated_data = []
        for item in self._data:
            merged = {**item, **data}
            merged["updated_at"] = datetime.utcnow().isoformat() + "Z"
            updated_data.append(merged)
        self._data = updated_data if updated_data else [data]
        return self
    
    def delete(self):
        return self
    
    def eq(self, column, value):
        return self
    
    def neq(self, column, value):
        return self
    
    def single(self):
        self._is_single = True
        return self
    
    def order(self, column, **kwargs):
        return self
    
    def range(self, start, end):
        return self
    
    def limit(self, count):
        return self
    
    def execute(self) -> MockSupabaseResponse:
        if self._is_single:
            # Return first item or empty for single()
            data = self._data[0] if self._data else None
            return MockSupabaseResponse(
                data=data,
                count=1 if data else 0
            )
        return MockSupabaseResponse(
            data=self._data,
            count=self._count if self._count is not None else len(self._data)
        )


class MockSupabaseTable:
    """Mock Supabase table with configurable responses."""
    
    def __init__(self, data: list = None, count: int = None):
        self._data = data or []
        self._count = count
    
    def select(self, *args, **kwargs):
        return MockSupabaseQuery(self._data.copy(), self._count)
    
    def insert(self, data):
        query = MockSupabaseQuery(self._data.copy(), self._count)
        return query.insert(data)
    
    def update(self, data):
        # For update, pass the existing data so it can be merged
        query = MockSupabaseQuery(self._data.copy(), self._count)
        return query.update(data)
    
    def delete(self):
        return MockSupabaseQuery(self._data.copy(), self._count)


class MockSupabaseClient:
    """Mock Supabase client."""
    
    def __init__(self):
        self._tables = {}
    
    def set_table_data(self, table_name: str, data: list, count: int = None):
        """Configure mock data for a table."""
        self._tables[table_name] = {"data": data, "count": count}
    
    def table(self, name: str) -> MockSupabaseTable:
        """Get mock table."""
        config = self._tables.get(name, {"data": [], "count": None})
        return MockSupabaseTable(config["data"], config["count"])


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_supabase() -> MockSupabaseClient:
    """
    Create a mock Supabase client.
    
    Usage:
        def test_something(mock_supabase):
            mock_supabase.set_table_data("products", [
                {"id": "1", "sku": "TEST", ...}
            ])
    """
    return MockSupabaseClient()


@pytest.fixture
def mock_db(mock_supabase) -> Generator:
    """
    Patch the database client with mock.

    Usage:
        def test_something(mock_db, mock_supabase):
            mock_supabase.set_table_data("products", [...])
            # Now any code using get_supabase_client() gets the mock
    """
    with patch("config.database.get_supabase_client", return_value=mock_supabase):
        with patch("services.product_service.get_supabase_client", return_value=mock_supabase):
            with patch("services.port_service.get_supabase_client", return_value=mock_supabase):
                with patch("services.shipment_service.get_supabase_client", return_value=mock_supabase):
                    yield mock_supabase


@pytest.fixture
def sample_product_data() -> dict:
    """Sample product data for testing."""
    return {
        "id": "test-uuid-123",
        "sku": "NOGAL CAFÉ",
        "category": "MADERAS",
        "rotation": "ALTA",
        "active": True,
        "created_at": "2025-12-05T10:00:00Z",
        "updated_at": "2025-12-05T10:00:00Z"
    }


@pytest.fixture
def sample_products_list() -> list:
    """Sample list of products for testing."""
    return [
        {
            "id": "uuid-1",
            "sku": "NOGAL CAFÉ",
            "category": "MADERAS",
            "rotation": "ALTA",
            "active": True,
            "created_at": "2025-12-05T10:00:00Z",
            "updated_at": "2025-12-05T10:00:00Z"
        },
        {
            "id": "uuid-2",
            "sku": "CEIBA GRIS OSC",
            "category": "MADERAS",
            "rotation": "MEDIA-ALTA",
            "active": True,
            "created_at": "2025-12-05T10:00:00Z",
            "updated_at": "2025-12-05T10:00:00Z"
        },
        {
            "id": "uuid-3",
            "sku": "TOLU GRIS",
            "category": "EXTERIORES",
            "rotation": None,
            "active": True,
            "created_at": "2025-12-05T10:00:00Z",
            "updated_at": "2025-12-05T10:00:00Z"
        }
    ]


# ===================
# API TEST CLIENT
# ===================

@pytest.fixture
def test_client():
    """
    Create FastAPI test client.
    
    Usage:
        def test_endpoint(test_client):
            response = test_client.get("/api/products")
            assert response.status_code == 200
    """
    from fastapi.testclient import TestClient
    from main import app
    
    return TestClient(app)


@pytest.fixture
def test_client_with_mock_db(mock_supabase):
    """
    Create FastAPI test client with mocked database.
    
    Usage:
        def test_endpoint(test_client_with_mock_db, mock_supabase):
            mock_supabase.set_table_data("products", [...])
            response = test_client_with_mock_db.get("/api/products")
    """
    from fastapi.testclient import TestClient
    from main import app
    
    with patch("config.database.get_supabase_client", return_value=mock_supabase):
        with patch("services.product_service.get_supabase_client", return_value=mock_supabase):
            yield TestClient(app)
