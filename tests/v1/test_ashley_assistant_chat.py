from __future__ import annotations

import json

from fastapi.testclient import TestClient

from clean_v1 import sailing_api
from clean_v1.assistant_chat import grounded_context
from clean_v1.sailing_domain import snapshot
from clean_v1.sailing_fixtures import SCENARIOS


class CapturingAssistant:
    def __init__(self):
        self.calls = []

    async def answer(self, *, question, history, context):
        self.calls.append({"question": question, "history": history, "context": context})
        return "La recomendación considera ventas históricas, velocidad, inventario y colchón."


def test_authenticated_chat_is_grounded_in_current_workspace_and_does_not_mutate(monkeypatch):
    eng = SCENARIOS["a1"]()
    plan = next(p for p in eng.state.plans.values() if p.lifecycle == "draft")
    provider = CapturingAssistant()
    monkeypatch.setattr(sailing_api, "_ENGINE", eng)
    monkeypatch.setattr(sailing_api.app.state, "assistant_provider", provider, raising=False)
    before = snapshot(eng.state)

    with TestClient(sailing_api.app) as client:
        response = client.post(
            "/api/assistant/chat",
            headers={"Authorization": "Bearer tok-ashley"},
            json={"question": "¿Por qué recomienda estas cantidades?", "plan_id": plan.plan_id,
                  "history": [{"role": "assistant", "content": "Puedes preguntarme por el ciclo."}]},
        )

    assert response.status_code == 200
    assert response.json() == {
        "answer": "La recomendación considera ventas históricas, velocidad, inventario y colchón.",
        "grounded_head_seq": len(eng.state.events),
        "grounding_as_of": "2026-08-03",
        "mutated": False,
    }
    assert snapshot(eng.state) == before
    call = provider.calls[0]
    assert call["question"] == "¿Por qué recomienda estas cantidades?"
    assert call["history"] == [{"role": "assistant", "content": "Puedes preguntarme por el ciclo."}]
    assert call["context"]["factors"]["historical_velocity_days"] == 90
    assert call["context"]["factors"]["edit_increment_m2"] == "67.20"
    assert call["context"]["grounding"]["head_seq"] == len(eng.state.events)
    assert call["context"]["shipment_recommendation"]["plan_id"] == plan.plan_id


def test_grounding_projection_excludes_notes_actions_and_raw_source_references():
    context = grounded_context({
        "head_seq": 7,
        "review_provenance": {"as_of": "2026-09-15", "mode": "current"},
        "evidence_readiness": [{"source": "sales", "status": "ready", "as_of": "2026-09-08"}],
        "focus_sailing": {"sailing_id": "S1", "name": "LITTLE SYMPHONY", "departure": "2026-09-30"},
        "sailing_rail": [{"sailing_id": "S1", "name": "LITTLE SYMPHONY", "departure": "2026-09-30"}],
        "plan": {"plan_id": "P1", "totals": {"total_m2": "17136.00"}, "rows": [{"product_id": "P1", "suggested_m2": "67.20"}], "legal_actions": ["SECRET_ACTION"]},
        "factory_planning": {"factory_order_date": "2026-09-25", "coverage_through": "2026-11-21"},
        "production_orders": {"recommendation_rows": [{"product_id": "P1", "recommended_m2": "134.40"}], "open_actions": ["SECRET_ACTION"]},
        "attention": [{"recommendation": "revisar inventario", "notes": "PRIVATE_NOTE"}],
        "source_refs": ["PRIVATE_SOURCE_REF"],
        "notes": "PRIVATE_NOTE",
    })

    encoded = json.dumps(context)
    assert context["grounding"] == {"head_seq": 7, "review_provenance": {"as_of": "2026-09-15", "mode": "current"}}
    assert "PRIVATE_NOTE" not in encoded
    assert "PRIVATE_SOURCE_REF" not in encoded
    assert "SECRET_ACTION" not in encoded


def test_chat_is_available_only_to_ashley(monkeypatch):
    eng = SCENARIOS["a1"]()
    provider = CapturingAssistant()
    monkeypatch.setattr(sailing_api, "_ENGINE", eng)
    monkeypatch.setattr(sailing_api.app.state, "assistant_provider", provider, raising=False)

    with TestClient(sailing_api.app) as client:
        response = client.post(
            "/api/assistant/chat",
            headers={"Authorization": "Bearer tok-elicio"},
            json={"question": "Resume la recomendación."},
        )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "assistant_access_forbidden"
    assert provider.calls == []


def test_chat_rejects_oversized_question_before_provider_call(monkeypatch):
    eng = SCENARIOS["a1"]()
    provider = CapturingAssistant()
    monkeypatch.setattr(sailing_api, "_ENGINE", eng)
    monkeypatch.setattr(sailing_api.app.state, "assistant_provider", provider, raising=False)

    with TestClient(sailing_api.app) as client:
        response = client.post(
            "/api/assistant/chat",
            headers={"Authorization": "Bearer tok-ashley"},
            json={"question": "x" * 2001},
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "assistant_invalid_request"
    assert provider.calls == []
