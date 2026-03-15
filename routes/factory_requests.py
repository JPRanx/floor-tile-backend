"""
Factory Request Horizon routes.
"""

from fastapi import APIRouter, HTTPException

from models.factory_request import FactoryRequestHorizonResponse
from models.factory_request_submission import (
    FactoryRequestSubmissionCreate,
    FactoryRequestSubmissionResponse,
    FactoryRequestLastSubmission,
)
from services.factory_request_service import get_factory_request_service
from services.factory_request_submission_service import get_factory_request_submission_service

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


@router.post("/submissions", response_model=FactoryRequestSubmissionResponse)
async def record_factory_request_submission(body: FactoryRequestSubmissionCreate):
    """Record a factory request export (Excel) for tracking."""
    try:
        service = get_factory_request_submission_service()
        return service.record_submission(body.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/submissions/last/{factory_id}", response_model=FactoryRequestLastSubmission)
async def get_last_factory_request_submission(factory_id: str):
    """Get the most recent factory request submission for a factory."""
    service = get_factory_request_submission_service()
    result = service.get_last_submission(factory_id)
    if not result:
        raise HTTPException(status_code=404, detail="No submissions found")
    return result
