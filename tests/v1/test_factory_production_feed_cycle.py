"""Factory production feed and gross/qualified/net cycle contracts."""
import base64
from datetime import date, timedelta
from decimal import Decimal as D

from fastapi.testclient import TestClient

from clean_v1 import sailing_api
from clean_v1.c19_inputs import InputPreviewService
from clean_v1.persistence import RequestPrincipal
from clean_v1.sailing_domain import snapshot
from clean_v1.sailing_read_model import compose_workspace
from tests.v1.test_s3_sailing_commands import make_engine, open_s1_plan, seed_basic


def principal():
    return RequestPrincipal("local-ashley", "ashley", "local-company", "ashley")


def auth():
    return {"Authorization": "Bearer tok-ashley"}


def engine_with_factory_need(*, accept_suggestion=True):
    eng = seed_basic(make_engine())
    eng.ashley("LoadSiesaSnapshot", {
        "as_of": date(2026, 8, 3),
        "rows": [
            {"product_ref": "TILE-A", "available_m2": D("67.20"), "committed_m2": D("0")},
            {"product_ref": "TILE-B", "available_m2": D("1500"), "committed_m2": D("0")},
        ],
    })
    _, plan_id = open_s1_plan(eng)
    if accept_suggestion:
        eng.ashley("AcceptSuggestion", {"plan_id": plan_id, "product_id": "TILE-A"})
    return eng, plan_id


def row_by_product(workspace, product_id="TILE-A"):
    return next(row for row in workspace["progression"]["production_need"]
                if row["product_id"] == product_id)


def add_boundary_sailing(eng):
    anchor_id = next(s.sailing_id for s in eng.state.sailings.values()
                     if s.name == "S2")
    result = eng.ashley("RecordSailing", {
        "carrier": "CMA-SYN", "name": "S3",
        "departure": date(2026, 9, 27), "voyage_days": 15,
    })
    return anchor_id, result["sailing_id"]


def test_three_anchor_factory_projection_uses_derived_next_boat_boundary_and_destination_supply():
    eng, plan_id = engine_with_factory_need()
    focus_id = eng.state.plans[plan_id].sailing_id
    anchor_id, boundary_id = add_boundary_sailing(eng)
    workspace = compose_workspace(
        eng, plan_id=plan_id, principal=principal(),
        production_anchor_sailing_id=anchor_id,
        factory_order_date=date(2026, 8, 10),
    )
    planning = workspace["factory_planning"]
    need = row_by_product(workspace)

    assert planning["focus_sailing_id"] == focus_id
    assert planning["production_anchor_sailing_id"] == anchor_id
    assert planning["coverage_boundary_sailing_id"] == boundary_id
    assert planning["factory_order_date"] == "2026-08-10"
    assert planning["coverage_through"] == eng.sailing_timing(boundary_id).warehouse_arrival.isoformat()
    assert D(need["future_demand_m2"]) > D("0")
    assert D(need["buffer_m2"]) == eng.buffer_m2("TILE-A")
    assert D(need["projected_warehouse_m2"]) == eng.warehouse_value("TILE-A")
    assert D(need["focus_shipment_m2"]) == D(
        eng.state.plans[plan_id].lines["TILE-A"]["selected_m2"])
    assert D(need["projected_boundary_balance_m2"]) == (
        D(need["projected_warehouse_m2"]) + D(need["qualified_incoming_m2"])
        + D(need["focus_shipment_m2"]) - D(need["future_demand_m2"]))
    raw = (D(need["future_demand_m2"]) + D(need["buffer_m2"])
           - D(need["projected_warehouse_m2"])
           - D(need["qualified_incoming_m2"])
           - D(need["focus_shipment_m2"]))
    from clean_v1.planning_math import round_up_to_half_pallet
    assert D(need["gross_future_factory_need_m2"]) == round_up_to_half_pallet(max(D("0"), raw))
    assert need["gross_need_formula"] == (
        "max(0, future_demand_m2 + buffer_m2 - projected_warehouse_m2 - "
        "qualified_incoming_m2 - focus_shipment_m2) rounded up to half-pallet"
    )


def test_three_anchor_factory_projection_uses_no_next_sailing_fallback_without_inventing_boundary():
    eng, plan_id = engine_with_factory_need()
    anchor_id = next(s.sailing_id for s in eng.state.sailings.values()
                     if s.name == "S2")

    workspace = compose_workspace(
        eng, plan_id=plan_id, principal=principal(),
        production_anchor_sailing_id=anchor_id,
        factory_order_date=date(2026, 8, 10),
    )

    planning = workspace["factory_planning"]
    anchor_arrival = eng.sailing_timing(anchor_id).warehouse_arrival
    assert planning["coverage_boundary_sailing_id"] is None
    assert planning["coverage_boundary_basis"] == "no_next_sailing_30_day_fallback"
    assert planning["fallback_flagged"] is True
    assert planning["coverage_through"] == (anchor_arrival + timedelta(days=30)).isoformat()
    assert workspace["production_orders"]["candidate"] is not None
    assert workspace["production_orders"]["candidate"]["coverage_boundary_sailing_id"] is None


def test_three_anchor_factory_projection_ignores_skipped_later_sailing():
    eng, plan_id = engine_with_factory_need()
    anchor_id, skipped_boundary_id = add_boundary_sailing(eng)
    eng.ashley("SetSailingDecision", {
        "sailing_id": skipped_boundary_id,
        "decision": "skip",
    })

    workspace = compose_workspace(
        eng, plan_id=plan_id, principal=principal(),
        production_anchor_sailing_id=anchor_id,
        factory_order_date=date(2026, 8, 10),
    )

    planning = workspace["factory_planning"]
    anchor_arrival = eng.sailing_timing(anchor_id).warehouse_arrival
    assert planning["coverage_boundary_sailing_id"] is None
    assert planning["coverage_boundary_basis"] == "no_next_sailing_30_day_fallback"
    assert planning["fallback_flagged"] is True
    assert planning["coverage_through"] == (anchor_arrival + timedelta(days=30)).isoformat()


def test_factory_netting_requires_active_production_ready_by_anchor_cutoff():
    eng, plan_id = engine_with_factory_need()
    anchor_id, _ = add_boundary_sailing(eng)
    eng.ashley("LoadProductionPlanning", {
        "as_of": date(2026, 8, 3),
        "rows": [
            {"product_ref": "TILE-A", "m2": "67.20", "status": "scheduled",
             "production_ref": "ON-TIME", "estimated_ready_date": "2026-08-18",
             "evidence_as_of": "2026-08-03", "can_add_more": False,
             "completion_confirmed": False},
            {"product_ref": "TILE-A", "m2": "134.40", "status": "in_progress",
             "production_ref": "TOO-LATE", "estimated_ready_date": "2026-08-20",
             "evidence_as_of": "2026-08-03", "can_add_more": False,
             "completion_confirmed": False},
        ],
    })
    workspace = compose_workspace(
        eng, plan_id=plan_id, principal=principal(),
        production_anchor_sailing_id=anchor_id,
        factory_order_date=date(2026, 8, 10),
    )
    need = row_by_product(workspace)
    assert {row["production_ref"] for row in need["qualified_production_rows"]} == {"ON-TIME"}
    excluded = {row["production_ref"]: row["excluded_reason"]
                for row in need["excluded_production_rows"]}
    assert excluded["TOO-LATE"] == "ready_after_anchor_cutoff"
    assert need["timing_context"]["anchor_production_readiness"] == "2026-08-19"


def test_factory_need_nets_only_qualified_active_production_and_never_changes_shipment_supply():
    eng, plan_id = engine_with_factory_need()
    before = compose_workspace(eng, plan_id=plan_id, principal=principal())
    before_plan_row = next(row for row in before["plan"]["rows"]
                           if row["product"]["id"] == "TILE-A")
    gross = D(row_by_product(before)["gross_future_factory_need_m2"])
    assert gross > D("67.20")

    eng.ashley("LoadProductionPlanning", {
        "as_of": date(2026, 8, 3),
        "rows": [
            {"product_ref": "TILE-A", "m2": "67.20", "status": "scheduled",
             "production_ref": "P1-ACTIVE", "estimated_ready_date": "2026-08-25",
             "evidence_as_of": "2026-08-03", "can_add_more": False,
             "completion_confirmed": False},
            {"product_ref": "TILE-A", "m2": "134.40", "status": "completed",
             "production_ref": "P1-DONE", "estimated_ready_date": "2026-08-20",
             "evidence_as_of": "2026-08-03", "can_add_more": False,
             "completion_confirmed": True},
            {"product_ref": "TILE-A", "m2": "201.60", "status": "in_progress",
             "production_ref": "P1-NO-DATE", "evidence_as_of": "2026-08-03",
             "can_add_more": False, "completion_confirmed": False},
        ],
    })

    after = compose_workspace(eng, plan_id=plan_id, principal=principal())
    need = row_by_product(after)
    after_plan_row = next(row for row in after["plan"]["rows"]
                          if row["product"]["id"] == "TILE-A")

    assert D(need["gross_future_factory_need_m2"]) == gross
    assert D(need["qualified_approved_production_m2"]) == D("67.20")
    assert D(need["net_new_production_required_m2"]) == gross - D("67.20")
    assert need["netting_formula"] == "max(0, gross_future_factory_need_m2 - qualified_approved_production_m2)"
    assert {row["production_ref"] for row in need["qualified_production_rows"]} == {"P1-ACTIVE"}
    excluded = {row["production_ref"]: row["excluded_reason"]
                for row in need["excluded_production_rows"]}
    assert excluded["P1-DONE"] == "completed_waiting_for_siesa_visibility"
    assert excluded["P1-NO-DATE"] == "missing_estimated_readiness"
    assert after_plan_row["suggested_m2"] == before_plan_row["suggested_m2"]
    assert after_plan_row["siesa_available_effective_m2"] == before_plan_row["siesa_available_effective_m2"]
    assert eng.buckets("TILE-A")["siesa_available_effective"] == D("67.20")


def test_production_need_uses_projected_tier_not_frozen_catalog_tier():
    eng, plan_id = engine_with_factory_need()
    workspace = compose_workspace(eng, plan_id=plan_id, principal=principal())
    need = row_by_product(workspace)
    assert need["product"]["tier"] == eng.tier_for("TILE-A")
    assert workspace["production_orders"]["recommendation_rows"][0]["product"]["tier"] == eng.tier_for("TILE-A")


def test_production_preview_diffs_existing_rows_by_product_and_factory_reference():
    eng, _ = engine_with_factory_need()
    existing = {
        "product_ref": "TILE-A", "m2": "67.20", "status": "scheduled",
        "production_ref": "P1-ACTIVE", "estimated_ready_date": "2026-08-25",
        "evidence_as_of": "2026-08-03", "can_add_more": False,
        "completion_confirmed": False,
    }
    eng.ashley("LoadProductionPlanning", {
        "as_of": date(2026, 8, 3), "rows": [existing],
    })
    preview = InputPreviewService().preview(
        eng, actor="ashley", feed="production_planning", input_mode="direct",
        as_of="2026-08-03", rows=[existing, {
            **existing, "production_ref": "P1-NEW", "m2": "134.40",
        }], raw_source_ref="manual-production",
    )

    assert preview["replacement_effect"]["semantics"] == "full_snapshot"
    assert preview["replacement_effect"]["counts"] == {
        "added": 1, "changed": 0, "omitted": 0,
    }
    assert preview["replacement_effect"]["diffs"][0]["row_identity"] == "TILE-A|P1-NEW"


def test_plan_de_produccion_pdf_upload_previews_then_applies_normalized_rows(monkeypatch):
    eng, _ = engine_with_factory_need()
    service = InputPreviewService()
    monkeypatch.setattr(sailing_api, "_ENGINE", eng)
    monkeypatch.setattr(sailing_api, "_INPUT_PREVIEWS", service)
    observed = {}

    def parse_pdf(pdf_bytes, *, catalog, evidence_as_of):
        observed.update(bytes=pdf_bytes, evidence_as_of=evidence_as_of,
                        catalog_count=len(catalog))
        return {"rows": [{
            "product_ref": "TILE-A", "m2": "67.20", "status": "in_progress",
            "production_ref": "P1-PDF", "estimated_ready_date": date(2026, 8, 25),
            "evidence_as_of": evidence_as_of, "completion_confirmed": False,
            "can_add_more": False,
        }], "unmatched_products": [],
                "authority": "production_context_not_siesa_availability"}

    monkeypatch.setattr(sailing_api, "parse_production_schedule_pdf", parse_pdf)
    before = snapshot(eng.state)
    payload = base64.b64encode(b"%PDF-real-plan").decode("ascii")
    with TestClient(sailing_api.app) as client:
        preview_response = client.post("/api/input/preview", headers=auth(), json={
            "feed": "production_planning", "input_mode": "upload",
            "as_of": "2026-08-03", "file_name": "PLAN_DE_PRODUCCION.pdf",
            "file_content_b64": payload,
        })
        assert preview_response.status_code == 200
        preview = preview_response.json()
        assert preview["can_apply"] is True
        assert preview["apply_token"]
        assert preview["authority_state"] == "production_context_not_siesa_availability"
        assert preview["unmatched_products"] == []
        assert snapshot(eng.state) == before
        apply_response = client.post("/api/input/apply", headers=auth(), json={
            "apply_token": preview["apply_token"],
        })

    assert apply_response.status_code == 200
    assert observed == {"bytes": b"%PDF-real-plan", "evidence_as_of": date(2026, 8, 3),
                        "catalog_count": 2}
    applied = eng._rows(eng.state, "production_planning")
    assert applied[0]["production_ref"] == "P1-PDF"
    assert eng.buckets("TILE-A")["siesa_available_effective"] == D("67.20")
