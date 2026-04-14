"""Order Plan API — velocity-optimized report builder.

Three endpoints:
- GET  /api/order-plan/available-boats → upcoming boats with commitment flags
- POST /api/order-plan/generate        → structured proposal + AI narrative
- POST /api/order-plan/export-pdf      → PDF blob reflecting Ashley's adjustments
"""

from __future__ import annotations

from datetime import date
from io import BytesIO
from typing import Literal, Optional

import structlog
from fastapi import APIRouter, HTTPException

from config import get_supabase_client
from fastapi.responses import Response
from pydantic import BaseModel, Field

from services.order_plan_service import (
    PlanBoat,
    PlanProductLine,
    PlanResult,
    VelocityRankingRow,
    SkippedProduct,
    WarehouseCapacity,
    compute_plan,
    M2_PER_PALLET,
    PALLETS_PER_CONTAINER,
)
from services.plan_narrative_service import generate_narrative
from services.plan_pdf_service import render_plan_pdf


logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/order-plan", tags=["Order Plan"])


# ─── Request schemas ────────────────────────────────────────────


class GenerateRequest(BaseModel):
    boat_ids: list[str] = Field(..., min_length=1)
    max_containers: int = Field(default=10, ge=1, le=50)
    warehouse_buffer_pct: int = Field(default=15, ge=0, le=50)
    include_production: bool = Field(default=True)
    factory_id: Optional[str] = None


class AdjustedLine(BaseModel):
    """Ashley's adjustment for one product on one boat."""
    product_id: str
    sku: str
    pallets: int  # her final number


class AdjustedBoat(BaseModel):
    boat_id: str
    vessel_name: str
    departure_date: str
    arrival_date: str
    max_containers: int
    lines: list[AdjustedLine]


class ExportPdfRequest(BaseModel):
    boats: list[AdjustedBoat]
    narrative: str  # frozen narrative from /generate
    original_plan: "GenerateResponse"  # to compute the edit delta


# ─── Response schemas ───────────────────────────────────────────


class LineResponse(BaseModel):
    product_id: str
    sku: str
    pallets: int
    m2: float
    velocity_m2_wk: float
    siesa_m2: float
    coverage_weeks: float
    is_urgent: bool
    note_es: str


class BoatResponse(BaseModel):
    boat_id: str
    vessel_name: str
    departure_date: str
    arrival_date: str
    max_containers: int
    max_pallets: int
    total_pallets: int
    total_m2: float
    containers_used: float
    lines: list[LineResponse]


class RankingRow(BaseModel):
    sku: str
    velocity_m2_wk: float
    siesa_pallets: float
    siesa_m2: float
    coverage_weeks: float
    is_urgent: bool


class SkippedRow(BaseModel):
    sku: str
    siesa_pallets: float
    siesa_m2: float
    reason_es: str


class CapacityResponse(BaseModel):
    current_pallets: int
    incoming_pallets: int
    plan_pallets: int
    outflow_pallets: int
    peak_pallets: int
    max_pallets: int
    utilization_pct: float
    is_safe: bool


class GenerateResponse(BaseModel):
    boats: list[BoatResponse]
    velocity_ranking: list[RankingRow]
    skipped: list[SkippedRow]
    warehouse_capacity: CapacityResponse
    total_siesa_pallets: int
    plan_total_pallets: int
    narrative: str
    generated_at: str


# ─── Serialization helpers ──────────────────────────────────────


def _line_to_response(line: PlanProductLine) -> LineResponse:
    return LineResponse(
        product_id=line.product_id,
        sku=line.sku,
        pallets=line.pallets,
        m2=line.m2,
        velocity_m2_wk=line.velocity_m2_wk,
        siesa_m2=line.siesa_m2,
        coverage_weeks=line.coverage_weeks,
        is_urgent=line.is_urgent,
        note_es=line.note_es,
    )


def _boat_to_response(boat: PlanBoat) -> BoatResponse:
    return BoatResponse(
        boat_id=boat.boat_id,
        vessel_name=boat.vessel_name,
        departure_date=boat.departure_date,
        arrival_date=boat.arrival_date,
        max_containers=boat.max_containers,
        max_pallets=boat.max_pallets,
        total_pallets=boat.total_pallets,
        total_m2=boat.total_m2,
        containers_used=boat.containers_used,
        lines=[_line_to_response(l) for l in boat.lines],
    )


def _ranking_to_response(row: VelocityRankingRow) -> RankingRow:
    return RankingRow(**row.__dict__)


def _skipped_to_response(row: SkippedProduct) -> SkippedRow:
    return SkippedRow(**row.__dict__)


def _capacity_to_response(cap: WarehouseCapacity) -> CapacityResponse:
    return CapacityResponse(**cap.__dict__)


def _result_to_response(result: PlanResult, narrative: str) -> GenerateResponse:
    return GenerateResponse(
        boats=[_boat_to_response(b) for b in result.boats],
        velocity_ranking=[_ranking_to_response(r) for r in result.velocity_ranking],
        skipped=[_skipped_to_response(s) for s in result.skipped],
        warehouse_capacity=_capacity_to_response(result.warehouse_capacity),
        total_siesa_pallets=result.total_siesa_pallets,
        plan_total_pallets=result.plan_total_pallets,
        narrative=narrative,
        generated_at=date.today().isoformat(),
    )


# ─── Available boats (with commitment flags) ────────────────────


class AvailableBoat(BaseModel):
    boat_id: str
    vessel_name: str
    departure_date: str
    arrival_date: str
    committed_pallets: int
    status: Literal["available", "committed", "before_committed"]
    # Human-readable reason shown in the UI when disabled
    reason: Optional[str] = None


@router.get("/available-boats", response_model=list[AvailableBoat])
async def list_available_boats():
    """List upcoming boats with planning eligibility flags.

    - `available`:        no committed draft, plan can use this boat
    - `committed`:        has an ordered/confirmed draft — already locked
    - `before_committed`: earlier than a committed boat, planning it is illogical
    """
    db = get_supabase_client()
    today = date.today()

    # Boats that haven't departed and aren't ignored
    boats = (
        db.table("boat_schedules")
        .select("id, vessel_name, departure_date, arrival_date, status")
        .gte("departure_date", today.isoformat())
        .neq("status", "ignored")
        .order("departure_date")
        .execute()
        .data or []
    )

    # Committed drafts (ordered or confirmed) with their total pallets
    drafts = (
        db.table("boat_factory_drafts")
        .select("boat_id, status")
        .in_("status", ["ordered", "confirmed"])
        .execute()
        .data or []
    )
    committed_boat_ids = {d["boat_id"] for d in drafts}

    # Pallet counts per committed boat
    pallets_by_boat: dict[str, int] = {}
    if committed_boat_ids:
        draft_rows = (
            db.table("boat_factory_drafts")
            .select("id, boat_id")
            .in_("boat_id", list(committed_boat_ids))
            .execute()
            .data or []
        )
        draft_id_to_boat = {d["id"]: d["boat_id"] for d in draft_rows}
        if draft_id_to_boat:
            items = (
                db.table("draft_items")
                .select("draft_id, selected_pallets")
                .in_("draft_id", list(draft_id_to_boat.keys()))
                .execute()
                .data or []
            )
            for it in items:
                bid = draft_id_to_boat.get(it["draft_id"])
                if bid:
                    pallets_by_boat[bid] = pallets_by_boat.get(bid, 0) + int(
                        it.get("selected_pallets") or 0
                    )

    # Find latest committed departure date — any boat before this is "before_committed"
    committed_latest_date: Optional[date] = None
    for b in boats:
        if b["id"] in committed_boat_ids:
            dep = date.fromisoformat(b["departure_date"])
            if committed_latest_date is None or dep > committed_latest_date:
                committed_latest_date = dep

    result: list[AvailableBoat] = []
    for b in boats:
        dep = date.fromisoformat(b["departure_date"])
        if b["id"] in committed_boat_ids:
            status = "committed"
            reason = f"Ya ordenado ({pallets_by_boat.get(b['id'], 0)} pallets)"
        elif committed_latest_date and dep < committed_latest_date:
            status = "before_committed"
            reason = f"Sale antes de un buque ya comprometido"
        else:
            status = "available"
            reason = None
        result.append(AvailableBoat(
            boat_id=b["id"],
            vessel_name=b["vessel_name"],
            departure_date=b["departure_date"],
            arrival_date=b["arrival_date"],
            committed_pallets=pallets_by_boat.get(b["id"], 0),
            status=status,
            reason=reason,
        ))

    return result


# ─── Routes ─────────────────────────────────────────────────────


@router.post("/generate", response_model=GenerateResponse)
async def generate_plan(body: GenerateRequest):
    """Compute a velocity-optimized order plan + AI narrative."""
    try:
        result = compute_plan(
            boat_ids=body.boat_ids,
            max_containers=body.max_containers,
            warehouse_buffer_pct=body.warehouse_buffer_pct,
            include_production=body.include_production,
            factory_id=body.factory_id,
        )
    except Exception as exc:
        logger.error("plan_compute_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to compute plan: {exc}") from exc

    narrative = generate_narrative(result)
    return _result_to_response(result, narrative)


@router.post("/export-pdf")
async def export_pdf(body: ExportPdfRequest):
    """Generate a PDF of the (potentially edited) plan.

    Layout matches the reference PDF. Narrative is frozen from /generate;
    a deterministic 'Ajustes manuales' block lists deltas vs the original.
    """
    try:
        pdf_bytes = render_plan_pdf(
            adjusted=body.boats,
            original=body.original_plan,
            narrative=body.narrative,
        )
    except Exception as exc:
        logger.error("plan_pdf_render_failed", error=str(exc))
        raise HTTPException(
            status_code=500, detail=f"Failed to render PDF: {exc}"
        ) from exc

    filename = f"Plan_Pedidos_{date.today().isoformat()}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# Resolve the forward reference so Pydantic can use ExportPdfRequest
ExportPdfRequest.model_rebuild()
