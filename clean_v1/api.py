"""
I6A — local, NON-PERSISTENT demo server for the Ashley Order Planning
Workspace. In-memory engine + synthetic fixtures only.

Explicitly NOT here (gates CLOSED): no database, no persistence shim, no
real auth, no real feeds, no API integration, no deployment surface. The
process's memory is the only state; restarting the server resets the demo.

Identity: server-side session tokens (synthetic). The §4.6a boundary is
enforced by the CommandBus — Elicio-only commands are denied by construction
to Ashley's token before any event append, not merely hidden in the UI.

Dev-only scenario controls live under /api/dev/* and the unlinked /dev.html
page — they are QA tooling, not product navigation.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .commands import AuthorizationError, CommandError
from .domain import DomainError
from .fixtures import build_demo_engine
from .read_model import compose_workspace

# synthetic demo tokens — resolved SERVER-side (see reconciliation.TOKENS)
DEMO_TOKENS = {"ashley": "tok-ashley", "elicio": "tok-elicio"}

app = FastAPI(title="Clean V1 — Ashley Order Planning Workspace (DEMO)",
              docs_url=None, redoc_url=None)

ENGINE = build_demo_engine()


class CommandRequest(BaseModel):
    command: str
    params: dict = {}


def _token_from(authorization: str | None) -> str | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    return authorization.removeprefix("Bearer ")


@app.post("/api/session")
def open_session(body: dict):
    """Demo role selection. Returns a server-known token; the token→identity
    mapping lives server-side and request payloads can never carry identity."""
    role = body.get("role")
    if role not in DEMO_TOKENS:
        raise HTTPException(status_code=400, detail="unknown demo role")
    return {"token": DEMO_TOKENS[role], "role": role,
            "demo": "synthetic session — no real authentication (gate closed)"}


@app.get("/api/workspace")
def workspace(authorization: str | None = Header(default=None)):
    token = _token_from(authorization)
    try:
        ENGINE.bus.identity.resolve(token)
    except AuthorizationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    ENGINE.run_checks()
    return compose_workspace(ENGINE)


@app.post("/api/command")
def command(body: CommandRequest,
            authorization: str | None = Header(default=None)):
    token = _token_from(authorization)
    try:
        result = ENGINE.bus.execute(body.command, body.params, token=token,
                                    on=ENGINE.today)
        ENGINE._post_command(body.command, body.params)
    except AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (CommandError, DomainError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "result": result, "workspace": compose_workspace(ENGINE)}


# ── dev-only QA scenario driver (unlinked; not product navigation) ─────────

@app.post("/api/dev/reset")
def dev_reset():
    global ENGINE
    ENGINE = build_demo_engine()
    return {"ok": True}


@app.post("/api/dev/scenario/{name}")
def dev_scenario(name: str):
    """Inject synthetic follow-on evidence for QA walkthroughs."""
    from datetime import date, timedelta
    from decimal import Decimal as D
    if name == "factory-confirms-august":
        order = max(ENGINE.state.orders.values(), key=lambda o: o.cycle_month)
        if order.state != "submitted":
            raise HTTPException(status_code=400,
                                detail="submit the August order first")
        for line in list(ENGINE.state.lines.values()):
            if line.order_id == order.order_id and line.state == "awaiting_factory":
                ENGINE.elicio("RecordFactoryOutcome",
                              {"line_id": line.line_id,
                               "outcome": "factory_confirmed",
                               "confirmed_m2": str(line.selected_m2)})
        return {"ok": True}
    if name == "production-shortfall-natura":
        exp = next((e for e in ENGINE.state.expectations.values()
                    if e.product_id == "natura-60" and e.open), None)
        if exp is None:
            raise HTTPException(status_code=400,
                                detail="confirm a natura-60 line first "
                                       "(run factory-confirms-august)")
        ENGINE.today = ENGINE.today + timedelta(days=10)
        ENGINE.load_produced_evidence(as_of=ENGINE.today, rows=[
            {"product_id": "natura-60", "m2": exp.effective_m2 / 2,
             "produced_on": ENGINE.today}])
        return {"ok": True}
    if name == "v6-variance-natura":
        # Ashley accepts natura and submits; the factory confirms materially
        # less (V6): the dedicated decision-consequence implication appears
        order = max(ENGINE.state.orders.values(), key=lambda o: o.cycle_month)
        if order.state == "draft":
            ENGINE.ashley("AcceptSuggestion",
                          {"order_id": order.order_id, "product_id": "natura-60"})
            ENGINE.ashley("SubmitMonthlyOrder", {"order_id": order.order_id})
        line = next(l for l in ENGINE.state.lines.values()
                    if l.order_id == order.order_id
                    and l.product_id == "natura-60"
                    and l.state == "awaiting_factory")
        ENGINE.elicio("RecordFactoryOutcome",
                      {"line_id": line.line_id, "outcome": "factory_confirmed",
                       "confirmed_m2": str(line.selected_m2 / 2)})
        return {"ok": True}
    if name == "production-delay-natura":
        # the natura production row slips past the protective window (§10.3).
        # FV4: a soft planning row governs an expectation ONLY through an
        # accepted unique reference — Ashley confirms an amendment intent on
        # OP-2026-127 and the factory accepts it, so the row acquires an
        # accepted governing reference before it slips (the old scenario
        # relied on 1×1 cardinality, which is not a reference).
        if not any(a.production_ref == "OP-2026-127"
                   for a in ENGINE.state.amendments.values()):
            ENGINE.ashley("ConfirmAmendmentIntent",
                          {"product_id": "natura-60",
                           "additional_m2": "268.80",
                           "production_ref": "OP-2026-127",
                           "case_ref": None})
            aid = next(a for a, obj in ENGINE.state.amendments.items()
                       if obj.production_ref == "OP-2026-127")
            ENGINE.elicio("RecordAmendmentOutcome",
                          {"amendment_id": aid, "outcome": "accepted",
                           "accepted_m2": "268.80"})
        rows = [dict(r) for r in ENGINE.production_planning]
        for r in rows:
            if r.get("orden_produccion") == "OP-2026-127":
                r["status"] = "in_progress"
                r["estimated_delivery_date"] = date(2026, 10, 25)
        ENGINE.load_production_planning(rows=rows)
        ENGINE.run_checks()
        return {"ok": True}
    if name == "transit-missing-natura":
        # produced in full, then the next in-transit upload after the
        # estimated dispatch arrives WITHOUT the product (§10.3)
        exp = next((e for e in ENGINE.state.expectations.values()
                    if e.product_id == "natura-60" and e.open), None)
        if exp is None:
            raise HTTPException(status_code=400,
                                detail="run v6-variance-natura first")
        produced_on = ENGINE.today + timedelta(days=10)
        ENGINE.today = produced_on
        ENGINE.load_produced_evidence(as_of=produced_on, rows=[
            {"product_id": "natura-60", "m2": exp.effective_m2,
             "produced_on": produced_on}])
        ENGINE.today = produced_on + timedelta(days=11)
        ENGINE.load_transit_snapshot(as_of=ENGINE.today, rows=[])
        ENGINE.run_checks()
        return {"ok": True}
    if name == "evidence-goes-stale":
        # time passes with no fresh uploads: critical feeds cross the
        # staleness threshold → one MissingEvidence case per feed (C6/S9)
        ENGINE.today = ENGINE.today + timedelta(days=25)
        ENGINE.run_checks()
        return {"ok": True}
    if name == "roble-arrives-fully":
        wh = dict(ENGINE.feeds["warehouse"]["values"])
        exp = next((e for e in ENGINE.state.expectations.values()
                    if e.product_id == "roble-20" and e.open), None)
        if exp is None:
            raise HTTPException(status_code=400, detail="no open roble expectation")
        wh["roble-20"] = wh["roble-20"] + exp.remaining_m2
        ENGINE.today = ENGINE.today + timedelta(days=12)
        ENGINE.load_warehouse_snapshot(as_of=ENGINE.today, values=wh)
        return {"ok": True}
    raise HTTPException(status_code=404, detail="unknown scenario")


# ── static frontend (built bundle) ──────────────────────────────────────────

DIST = Path(__file__).resolve().parents[2] / "frontend" / "dist"
if DIST.exists():
    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

    @app.get("/")
    def index():
        return FileResponse(DIST / "index.html")

    @app.get("/dev.html")
    def dev_page():
        return FileResponse(DIST / "dev.html")
