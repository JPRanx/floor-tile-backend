"""
Factory Request Horizon routes.
"""

from fastapi import APIRouter, HTTPException

from models.factory_request import FactoryRequestHorizonResponse
from services.factory_request_service import get_factory_request_service

router = APIRouter(prefix="/api/factory-requests", tags=["Factory Requests"])


@router.get("/horizon/{factory_id}", response_model=FactoryRequestHorizonResponse)
async def get_factory_request_horizon(factory_id: str):
    try:
        service = get_factory_request_service()
        return service.get_horizon(factory_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
