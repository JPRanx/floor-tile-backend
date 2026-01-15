"""
Pending documents API routes.

Endpoints for managing unmatched documents that need manual resolution.
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import structlog

from models.pending_document import (
    PendingDocumentResponse,
    PendingDocumentListResponse,
    ResolvePendingRequest,
    PendingStatus,
)
from models.shipment import ShipmentResponse
from services.pending_document_service import get_pending_document_service
from services.shipment_service import get_shipment_service
from exceptions import AppError, NotFoundError

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/pending-documents", tags=["Pending Documents"])


def handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


@router.get("", response_model=PendingDocumentListResponse)
async def list_pending_documents(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: Optional[PendingStatus] = Query(None, description="Filter by status"),
    document_type: Optional[str] = Query(None, description="Filter by document type")
):
    """
    List pending documents awaiting resolution.

    Returns paginated list of unmatched documents.
    Default filters to pending status only.
    """
    try:
        service = get_pending_document_service()

        # Default to pending if no status specified
        filter_status = status if status else PendingStatus.PENDING

        documents, total = service.get_all(
            page=page,
            page_size=page_size,
            status=filter_status,
            document_type=document_type
        )

        total_pages = (total + page_size - 1) // page_size

        return PendingDocumentListResponse(
            data=documents,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        return handle_error(e)


@router.get("/count")
async def get_pending_count():
    """
    Get count of pending documents.

    Useful for dashboard badges/notifications.
    """
    try:
        service = get_pending_document_service()

        _, total = service.get_all(
            page=1,
            page_size=1,
            status=PendingStatus.PENDING
        )

        return {"pending_count": total}

    except Exception as e:
        return handle_error(e)


@router.get("/{document_id}", response_model=PendingDocumentResponse)
async def get_pending_document(document_id: str):
    """
    Get a specific pending document by ID.

    Includes full parsed data and PDF storage path.
    """
    try:
        service = get_pending_document_service()
        return service.get_by_id(document_id)

    except NotFoundError as e:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "NOT_FOUND",
                    "message": str(e)
                }
            }
        )
    except Exception as e:
        return handle_error(e)


@router.get("/{document_id}/pdf-url")
async def get_pdf_url(document_id: str):
    """
    Get signed URL for PDF download.

    URL expires after 1 hour.
    """
    try:
        service = get_pending_document_service()
        doc = service.get_by_id(document_id)

        signed_url = service._get_pdf_signed_url(doc.pdf_storage_path)

        if not signed_url:
            return JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "code": "STORAGE_ERROR",
                        "message": "Failed to generate PDF URL"
                    }
                }
            )

        return {
            "url": signed_url,
            "expires_in": 3600
        }

    except NotFoundError as e:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "NOT_FOUND",
                    "message": str(e)
                }
            }
        )
    except Exception as e:
        return handle_error(e)


@router.get("/{document_id}/candidates", response_model=list[ShipmentResponse])
async def get_candidates(
    document_id: str,
    limit: int = Query(10, ge=1, le=50, description="Maximum candidates to return")
):
    """
    Get candidate shipments for assigning this pending document.

    Returns all active shipments (matching manual upload flow behavior).
    User selects the appropriate target shipment from the list.
    """
    try:
        pending_service = get_pending_document_service()
        shipment_service = get_shipment_service()

        # Verify the pending document exists
        pending_service.get_by_id(document_id)

        # Match manual flow - show all active shipments, let user decide
        shipments, _ = shipment_service.get_all(page=1, page_size=limit)
        return shipments

    except NotFoundError as e:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "NOT_FOUND",
                    "message": str(e)
                }
            }
        )
    except Exception as e:
        return handle_error(e)


@router.post("/{document_id}/resolve", response_model=PendingDocumentResponse)
async def resolve_pending_document(document_id: str, request: ResolvePendingRequest):
    """
    Resolve a pending document.

    Actions:
    - **assign**: Assign to existing shipment (requires target_shipment_id)
    - **create**: Create new shipment from this document
    - **discard**: Mark as discarded (document not needed)

    Returns the updated document with resolution details.
    """
    try:
        service = get_pending_document_service()
        return await service.resolve(document_id, request)

    except NotFoundError as e:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "NOT_FOUND",
                    "message": str(e)
                }
            }
        )
    except Exception as e:
        return handle_error(e)


@router.post("/expire")
async def expire_old_documents():
    """
    Expire old pending documents.

    Marks documents past their expires_at date as expired.
    This is typically called by a scheduled job.

    Returns count of expired documents.
    """
    try:
        service = get_pending_document_service()
        count = service.expire_old_documents()

        return {
            "expired_count": count,
            "message": f"Expired {count} pending document(s)"
        }

    except Exception as e:
        return handle_error(e)
