"""
Port API routes.

Simple read-only endpoints for port lookup.
"""

from fastapi import APIRouter, HTTPException
import structlog

from models.port import PortResponse
from services.port_service import get_port_service
from exceptions import DatabaseError

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/ports", tags=["Ports"])


@router.get("", response_model=list[PortResponse])
async def list_ports():
    """
    Get all ports.

    Returns list of all ports (small table, no pagination needed).
    """
    try:
        service = get_port_service()
        result = service.db.table(service.table).select("*").execute()
        return [service._row_to_response(row) for row in result.data]
    except Exception as e:
        logger.error("list_ports_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch ports")


@router.get("/{port_id}", response_model=PortResponse)
async def get_port(port_id: str):
    """
    Get a single port by ID.

    Args:
        port_id: Port UUID

    Returns:
        Port details

    Raises:
        404: Port not found
    """
    try:
        service = get_port_service()
        result = (
            service.db.table(service.table)
            .select("*")
            .eq("id", port_id)
            .execute()
        )

        if not result.data:
            raise HTTPException(status_code=404, detail="Port not found")

        return service._row_to_response(result.data[0])
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_port_failed", port_id=port_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch port")