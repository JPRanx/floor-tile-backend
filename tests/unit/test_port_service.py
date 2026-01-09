"""
Unit tests for PortService.

Run: pytest tests/unit/test_port_service.py -v
"""

import pytest
from unittest.mock import patch, MagicMock

from services.port_service import PortService, get_port_service
from models.port import PortCreate
from exceptions import DatabaseError
from exceptions.errors import ValidationError


class TestPortServiceNormalizePortName:
    """Tests for PortService.normalize_port_name()"""

    def test_normalize_removes_accents(self):
        """Should remove accents: Tomás → Tomas."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("Tomás")

        assert result == "tomas"

    def test_normalize_removes_accents_complex(self):
        """Should handle complex accent combinations."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("São Tomé")

        assert result == "sao tome"

    def test_normalize_converts_to_lowercase(self):
        """Should convert to lowercase."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("CARTAGENA")

        assert result == "cartagena"

    def test_normalize_removes_puerto_prefix(self):
        """Should remove 'Puerto' prefix."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("Puerto Barrios")

        assert result == "barrios"

    def test_normalize_removes_port_prefix(self):
        """Should remove 'Port' prefix."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("Port of Spain")

        assert result == "of spain"

    def test_normalize_removes_santo_prefix(self):
        """Should remove 'Santo' prefix."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("Santo Domingo")

        assert result == "domingo"

    def test_normalize_handles_extra_whitespace(self):
        """Should normalize multiple spaces to single space."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("Cartagena   de   Indias")

        assert result == "cartagena de indias"

    def test_normalize_empty_string_returns_empty(self):
        """Should return empty string for empty input."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name("")

        assert result == ""

    def test_normalize_none_returns_empty(self):
        """Should return empty string for None input."""
        service = PortService.__new__(PortService)

        result = service.normalize_port_name(None)

        assert result == ""


class TestPortServiceFindByName:
    """Tests for PortService.find_by_name()"""

    def test_find_by_name_exact_match(self, mock_db, mock_supabase):
        """Should find port with exact name match."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Cartagena", "country": "Colombia", "type": "ORIGIN", "created_at": "2025-01-01T00:00:00Z"}
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act
        result = service.find_by_name("Cartagena")

        # Assert
        assert result is not None
        assert result.id == "port-1"
        assert result.name == "Cartagena"

    def test_find_by_name_case_insensitive(self, mock_db, mock_supabase):
        """Should find port regardless of case."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Cartagena", "country": "Colombia", "type": "ORIGIN", "created_at": "2025-01-01T00:00:00Z"}
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act
        result = service.find_by_name("CARTAGENA")

        # Assert
        assert result is not None
        assert result.name == "Cartagena"

    def test_find_by_name_fuzzy_match_with_accents(self, mock_db, mock_supabase):
        """Should find port using normalized matching (accent removal)."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Santo Tomás", "country": "Guatemala", "type": "DESTINATION", "created_at": "2025-01-01T00:00:00Z"}
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act - search without accent
        result = service.find_by_name("Santo Tomas")

        # Assert
        assert result is not None
        assert result.name == "Santo Tomás"

    def test_find_by_name_partial_match(self, mock_db, mock_supabase):
        """Should find port with partial match."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Puerto Barrios", "country": "Guatemala", "type": "DESTINATION", "created_at": "2025-01-01T00:00:00Z"}
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act - search with just "Barrios"
        result = service.find_by_name("Barrios")

        # Assert
        assert result is not None
        assert result.name == "Puerto Barrios"

    def test_find_by_name_not_found_returns_none(self, mock_db, mock_supabase):
        """Should return None when port not found."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Cartagena", "country": "Colombia", "type": "ORIGIN", "created_at": "2025-01-01T00:00:00Z"}
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act
        result = service.find_by_name("Shanghai")

        # Assert
        assert result is None

    def test_find_by_name_empty_returns_none(self, mock_db, mock_supabase):
        """Should return None for empty search string."""
        # Arrange
        mock_supabase.set_table_data("ports", [])
        service = PortService()

        # Act
        result = service.find_by_name("")

        # Assert
        assert result is None

    def test_find_by_name_whitespace_returns_none(self, mock_db, mock_supabase):
        """Should return None for whitespace-only search string."""
        # Arrange
        mock_supabase.set_table_data("ports", [])
        service = PortService()

        # Act
        result = service.find_by_name("   ")

        # Assert
        assert result is None

    def test_find_by_name_with_type_filter(self, mock_db, mock_supabase):
        """Should filter by port type when specified."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Cartagena", "country": "Colombia", "type": "ORIGIN", "created_at": "2025-01-01T00:00:00Z"},
            {"id": "port-2", "name": "Cartagena", "country": "Spain", "type": "DESTINATION", "created_at": "2025-01-01T00:00:00Z"},
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act
        result = service.find_by_name("Cartagena", port_type="ORIGIN")

        # Assert
        assert result is not None
        assert result.country == "Colombia"


class TestPortServiceCreate:
    """Tests for PortService.create()"""

    def test_create_port_success(self, mock_db, mock_supabase):
        """Should create port and return it."""
        # Arrange
        mock_supabase.set_table_data("ports", [])
        service = PortService()

        data = PortCreate(
            name="New Port",
            country="Test Country",
            type="ORIGIN"
        )

        # Act
        result = service.create(data)

        # Assert
        assert result.name == "New Port"
        assert result.country == "Test Country"
        assert result.type == "ORIGIN"


class TestPortServiceFindOrCreate:
    """Tests for PortService.find_or_create()"""

    def test_find_or_create_finds_existing(self, mock_db, mock_supabase):
        """Should return existing port when found."""
        # Arrange
        ports_data = [
            {"id": "port-1", "name": "Cartagena", "country": "Colombia", "type": "ORIGIN", "created_at": "2025-01-01T00:00:00Z"}
        ]
        mock_supabase.set_table_data("ports", ports_data)
        service = PortService()

        # Act
        result = service.find_or_create("Cartagena", "ORIGIN")

        # Assert
        assert result.id == "port-1"
        assert result.name == "Cartagena"

    def test_find_or_create_creates_when_not_found(self, mock_db, mock_supabase):
        """Should create new port when not found."""
        # Arrange
        mock_supabase.set_table_data("ports", [])
        service = PortService()

        # Act
        result = service.find_or_create("New Port", "DESTINATION", country="New Country")

        # Assert
        assert result.name == "New Port"
        assert result.type == "DESTINATION"

    def test_find_or_create_empty_name_raises_error(self, mock_db, mock_supabase):
        """Should raise ValidationError for empty name."""
        # Arrange
        mock_supabase.set_table_data("ports", [])
        service = PortService()

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.find_or_create("", "ORIGIN")

        assert "PORT_NAME_REQUIRED" in exc_info.value.code

    def test_find_or_create_whitespace_name_raises_error(self, mock_db, mock_supabase):
        """Should raise ValidationError for whitespace-only name."""
        # Arrange
        mock_supabase.set_table_data("ports", [])
        service = PortService()

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.find_or_create("   ", "DESTINATION")

        assert "PORT_NAME_REQUIRED" in exc_info.value.code


class TestGetPortService:
    """Tests for get_port_service() singleton."""

    def test_get_port_service_returns_instance(self, mock_db):
        """Should return PortService instance."""
        # Act
        service = get_port_service()

        # Assert
        assert isinstance(service, PortService)