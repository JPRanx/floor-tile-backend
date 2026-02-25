"""
Ledger Routes - Read-only API endpoints for the inventory event ledger.

Provides access to ledger events, projected inventory state,
and reconciliation reports.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

import structlog

from models.inventory_ledger import (
    LedgerEventResponse,
    LedgerEventListResponse,
    ProjectedStateResponse,
    ProjectedStateListResponse,
    ReconciliationReportResponse,
    ReconciliationReportListResponse,
)
from services.inventory_ledger_service import get_ledger_service

logger = structlog.get_logger(__name__)

router = APIRouter(
    prefix="/api/ledger",
    tags=["ledger"],
)


# ===================
# LEDGER EVENT ROUTES
# ===================

@router.get("/events", response_model=LedgerEventListResponse)
async def get_events(
    product_id: Optional[str] = Query(None, description="Filter by product UUID"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    since: Optional[str] = Query(None, description="Filter events after this ISO timestamp"),
    limit: int = Query(100, le=500, description="Maximum events to return"),
):
    """Get ledger events with optional filters."""
    try:
        service = get_ledger_service()
        events = service.get_events(
            product_id=product_id,
            event_type=event_type,
            since=since,
            limit=limit,
        )
        return LedgerEventListResponse(
            data=[LedgerEventResponse(**e) for e in events],
            total=len(events),
        )
    except Exception as e:
        logger.error("get_ledger_events_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


# ===================
# PROJECTED STATE ROUTES
# ===================

@router.get("/projected", response_model=ProjectedStateListResponse)
async def get_projected():
    """Get all projected inventory states."""
    try:
        service = get_ledger_service()
        states = service.get_all_projected()
        return ProjectedStateListResponse(
            data=[ProjectedStateResponse(**s) for s in states],
            total=len(states),
        )
    except Exception as e:
        logger.error("get_projected_states_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/projected/{product_id}", response_model=ProjectedStateResponse)
async def get_projected_product(product_id: str):
    """Get projected state for a single product."""
    try:
        service = get_ledger_service()
        state = service.get_projected_state(product_id)
        if not state:
            raise HTTPException(
                status_code=404,
                detail="Product not found in projected state",
            )
        return ProjectedStateResponse(**state)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "get_projected_product_failed",
            product_id=product_id,
            error=str(e),
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# ===================
# RECONCILIATION REPORT ROUTES
# ===================

@router.get("/reconciliation-reports", response_model=ReconciliationReportListResponse)
async def get_reconciliation_reports(
    recon_type: Optional[str] = Query(None, description="Filter by reconciliation type"),
    limit: int = Query(20, le=100, description="Maximum reports to return"),
):
    """Get reconciliation reports."""
    try:
        service = get_ledger_service()
        reports = service.get_reconciliation_reports(
            recon_type=recon_type,
            limit=limit,
        )
        return ReconciliationReportListResponse(
            data=[ReconciliationReportResponse(**r) for r in reports],
            total=len(reports),
        )
    except Exception as e:
        logger.error("get_reconciliation_reports_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")
