"""
Base schemas and mixins for all models.

See STANDARDS_VALIDATION.md for patterns.
"""

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional


class BaseSchema(BaseModel):
    """
    Base for all schemas.
    
    Features:
        - Auto-trim whitespace from strings
        - Validate on attribute assignment
        - Allow ORM objects (from_attributes)
    """
    model_config = ConfigDict(
        from_attributes=True,
        str_strip_whitespace=True,
        validate_assignment=True
    )


class TimestampMixin(BaseModel):
    """Add timestamps to response models."""
    created_at: datetime
    updated_at: Optional[datetime] = None


class PaginationParams(BaseModel):
    """Standard pagination parameters."""
    page: int = 1
    page_size: int = 20
    
    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size
    
    @property
    def limit(self) -> int:
        return self.page_size


class PaginatedResponse(BaseModel):
    """Standard paginated response wrapper."""
    data: list
    total: int
    page: int
    page_size: int
    total_pages: int
    
    @classmethod
    def create(cls, data: list, total: int, page: int, page_size: int):
        """Create paginated response from data."""
        total_pages = (total + page_size - 1) // page_size  # Ceiling division
        return cls(
            data=data,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
