"""
Draft API routes.

CRUD endpoints for order drafts (per boat + factory).
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field
import structlog

from config import get_supabase_client
from exceptions import DatabaseError
from models.draft import DraftResponse, DraftSave, DraftStatusUpdate
from services.draft_service import get_draft_service


class DraftItemAuditUpdate(BaseModel):
    """Post-order audit fields. Brain ignores these — pure observation."""
    actual_loaded_pallets: Optional[float] = Field(
        None, ge=0, description="Pallets actually loaded by carrier"
    )
    cut_reason: Optional[str] = Field(
        None, description="weight | lot_mix | deferred | other"
    )

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/boat/{boat_id}", response_model=list[DraftResponse])
async def list_drafts_for_boat(boat_id: str):
    """
    List all drafts for a boat across factories.

    Args:
        boat_id: Boat UUID

    Returns:
        List of drafts for the given boat
    """
    try:
        service = get_draft_service()
        return service.list_drafts_for_boat(boat_id)
    except Exception as e:
        logger.error("list_drafts_for_boat_failed", boat_id=boat_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch drafts for boat")


@router.get("/{boat_id}/{factory_id}", response_model=DraftResponse)
async def get_draft(boat_id: str, factory_id: str):
    """
    Get draft for a specific boat + factory.

    Args:
        boat_id: Boat UUID
        factory_id: Factory UUID

    Returns:
        Draft details

    Raises:
        404: Draft not found
    """
    try:
        service = get_draft_service()
        draft = service.get_draft(boat_id, factory_id)

        if not draft:
            raise HTTPException(status_code=404, detail="Draft not found")

        return draft
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_draft_failed", boat_id=boat_id, factory_id=factory_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch draft")


@router.post("", response_model=DraftResponse)
async def save_draft(body: DraftSave):
    """
    Create or update a draft (upsert by boat + factory).

    Args:
        body: Draft data including boat_id, factory_id, notes, and items

    Returns:
        Created or updated draft
    """
    try:
        service = get_draft_service()
        return service.save_draft(
            body.boat_id,
            body.factory_id,
            body.notes,
            [item.model_dump() for item in body.items],
        )
    except Exception as e:
        logger.error("save_draft_failed", boat_id=body.boat_id, factory_id=body.factory_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to save draft")


@router.patch("/{draft_id}/status")
async def update_draft_status(draft_id: str, body: DraftStatusUpdate):
    """
    Update draft status.

    Args:
        draft_id: Draft UUID
        body: New status value

    Returns:
        Updated draft

    Raises:
        404: Draft not found
    """
    try:
        service = get_draft_service()
        return service.update_status(draft_id, body.status.value)
    except HTTPException:
        raise
    except DatabaseError as e:
        if "not found" in str(e):
            raise HTTPException(status_code=404, detail="Draft not found")
        raise HTTPException(status_code=500, detail="Failed to update draft status")
    except Exception as e:
        logger.error("update_draft_status_failed", draft_id=draft_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to update draft status")


@router.patch("/items/{draft_item_id}/audit")
async def update_draft_item_audit(draft_item_id: str, body: DraftItemAuditUpdate):
    """
    Update post-order audit fields on a draft item: actual loaded pallets and
    the reason any cut was made (typically: weight, lot mix, deferred). Brain
    ignores these — they exist so we can observe how often Ashley's actual
    orders diverge from suggestions and why.
    """
    db = get_supabase_client()
    update: dict = {}
    if body.actual_loaded_pallets is not None:
        update["actual_loaded_pallets"] = body.actual_loaded_pallets
    if body.cut_reason is not None:
        update["cut_reason"] = body.cut_reason
    if not update:
        raise HTTPException(status_code=400, detail="No audit fields provided")
    try:
        result = db.table("draft_items").update(update).eq("id", draft_item_id).execute()
        if not result.data:
            raise HTTPException(status_code=404, detail="Draft item not found")
        return result.data[0]
    except HTTPException:
        raise
    except Exception as e:
        logger.error("draft_item_audit_update_failed", draft_item_id=draft_item_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to update audit fields")


@router.delete("/{draft_id}", status_code=204, response_class=Response)
async def delete_draft(draft_id: str):
    """
    Delete a draft.

    Args:
        draft_id: Draft UUID

    Raises:
        404: Draft not found
    """
    try:
        service = get_draft_service()
        deleted = service.delete_draft(draft_id)

        if not deleted:
            raise HTTPException(status_code=404, detail="Draft not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_draft_failed", draft_id=draft_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to delete draft")
