"""Reconciled V1 sailing API.

Local mode retains the deterministic in-memory review harness. Hosted mode is
explicit and fail-closed: it binds project-pinned Supabase Auth to the durable
PostgreSQL adapter and disables synthetic session/scenario mutation routes.
"""
from __future__ import annotations

import os
import base64
import binascii
import copy
import hashlib
import json
import re
import secrets
from datetime import date
from decimal import Decimal
from threading import RLock
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .sailing_commands import AuthorizationError, CommandError
from .sailing_fixtures import SCENARIOS, base_engine, scenario_default
from .sailing_engine import SailingEngine
from .config import PlanningConfig
from .historical_review import build_historical_review_engine
from .ashley_current_review import build_ashley_current_review_engine
from .sailing_read_model import compose_workspace
from .command_schema import COMMAND_SCHEMAS, validate_command
from .persistence import RequestPrincipal
from .persistence.identity_supabase import (
    AuthForbidden, AuthUnauthorized, PostgresBindingRepository,
    SupabaseIdentityResolver, SupabaseJwksLoader,
)
from .persistence.postgres_adapter import IdempotencyConflict, PostgresAdapter
from .security import SecurityMiddleware
from .settings_v1 import SettingsV1, load_settings
from .assistant_chat import configured_assistant, grounded_context, validate_chat_body
from .c19_inputs import (InputPreviewService, PreviewDenied,
                         compose_source_hub)
from .ashley_file_adapters import (parse_production_schedule_pdf,
                                   parse_sales_xlsx, parse_scheduled_dispatch_xlsx,
                                   parse_siesa_xls, parse_siesa_xlsx,
                                   parse_warehouse_xlsx)
from .production_catalog import production_catalog

_MAX_PREVIEW_REQUEST_BYTES = 15 * 1024 * 1024


class _PreviewRequestBodyLimit:
    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        if (scope.get("type") != "http"
                or scope.get("path") not in {
                    "/api/input/preview", "/api/catalog/bootstrap/preview"}):
            await self.application(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        try:
            declared = int(headers.get(b"content-length", b"0"))
        except ValueError:
            declared = 0
        if declared > _MAX_PREVIEW_REQUEST_BYTES:
            await self._reject(send)
            return
        received = 0
        overflow = False

        async def bounded_receive():
            nonlocal received, overflow
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > _MAX_PREVIEW_REQUEST_BYTES:
                    overflow = True
                    return {"type": "http.request", "body": b"", "more_body": False}
            return message

        async def bounded_send(message):
            if not overflow:
                await send(message)

        await self.application(scope, bounded_receive, bounded_send)
        if overflow:
            await self._reject(send)

    @staticmethod
    async def _reject(send):
        body = json.dumps({"detail": {
            "code": "input_too_large",
            "operator_message": "El archivo es demasiado grande para procesarlo.",
        }}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        await send({"type": "http.response.start", "status": 413, "headers": [
            (b"content-type", b"application/json; charset=utf-8"),
            (b"content-length", str(len(body)).encode("ascii")),
            (b"cache-control", b"no-store"),
            (b"x-content-type-options", b"nosniff"),
        ]})
        await send({"type": "http.response.body", "body": body})


class _HostedAuthBoundary:
    """Authenticate hosted operational routes before size or schema parsing."""
    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        if (scope.get("type") == "http"
                and scope.get("method") != "OPTIONS"
                and scope.get("path") in _HOSTED_AUTH_PATHS
                and getattr(app.state, "store", None) is not None):
            headers = dict(scope.get("headers", []))
            authorization = headers.get(b"authorization", b"").decode("latin-1") or None
            try:
                _resolve_hosted_principal(authorization)
            except HTTPException as exc:
                body = json.dumps(
                    {"detail": exc.detail}, ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                response_headers = [
                    (b"content-type", b"application/json; charset=utf-8"),
                    (b"content-length", str(len(body)).encode("ascii")),
                    (b"cache-control", b"no-store"),
                    (b"content-security-policy", b"default-src 'none'; frame-ancestors 'none'; base-uri 'none'"),
                    (b"strict-transport-security", b"max-age=31536000; includeSubDomains"),
                    (b"x-content-type-options", b"nosniff"),
                    (b"x-frame-options", b"DENY"),
                    (b"referrer-policy", b"no-referrer"),
                    (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
                ]
                origin = headers.get(b"origin", b"").decode("latin-1")
                settings = getattr(app.state, "runtime_settings", None)
                if origin and settings and origin in settings.cors_allowed_origins:
                    response_headers.extend([
                        (b"access-control-allow-origin", origin.encode("latin-1")),
                        (b"access-control-allow-credentials", b"true"),
                        (b"vary", b"Origin"),
                    ])
                await send({"type": "http.response.start", "status": exc.status_code,
                            "headers": response_headers})
                await send({"type": "http.response.body", "body": body})
                return
        await self.application(scope, receive, send)


app = FastAPI(title="Floor Tile Reconciled V1 — historical local review")
app.state.store = None
app.state.principal_resolver = None
app.state.runtime_settings = load_settings()
app.state.assistant_provider = None
app.state.readiness_check = lambda: True
_HOSTED_AUTH_PATHS = frozenset({
    "/api/workspace", "/api/sources", "/api/input/preview",
    "/api/input/apply", "/api/command", "/api/assistant/chat",
    "/api/catalog/bootstrap/preview", "/api/catalog/bootstrap/apply",
})


@app.middleware("http")
async def json_security_headers(request, call_next):
    response = await call_next(request)
    if (response.headers.get("content-type") or "").startswith("application/json"):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        if request.url.path == "/api/command":
            response.headers["Vary"] = "Authorization, Idempotency-Key"
            if "Idempotency-Replayed" not in response.headers:
                response.headers["Idempotency-Replayed"] = "false"
    return response


# See the original ASGI receive stream before request materialization.
app.add_middleware(_PreviewRequestBodyLimit)
app.add_middleware(
    SecurityMiddleware,
    settings_getter=lambda: app.state.runtime_settings,
)
app.add_middleware(_HostedAuthBoundary)

_ENGINE = None
_INPUT_PREVIEWS = InputPreviewService()


class CatalogBootstrapPreviewService:
    """Tenant-bound, two-source capability for an explicit initial catalog."""

    def __init__(self):
        self._candidates = {}

    @staticmethod
    def _source(body, name, extension):
        source = body.get(name)
        if not isinstance(source, dict):
            raise PreviewDenied(f"{name} source is required")
        file_name = str(source.get("file_name") or "")
        if not file_name.lower().endswith(extension):
            raise PreviewDenied(f"{name} source must be {extension}")
        encoded = source.get("file_content_b64")
        claimed = source.get("sha256")
        if not isinstance(encoded, str) or len(encoded) > _MAX_UPLOAD_B64_CHARS:
            raise PreviewDenied(f"{name} source is invalid")
        if not isinstance(claimed, str) or not re.fullmatch(r"[0-9a-f]{64}", claimed):
            raise PreviewDenied(f"{name} sha256 is invalid")
        try:
            payload = base64.b64decode(encoded, validate=True)
        except binascii.Error as exc:
            raise PreviewDenied(f"{name} source must be valid base64") from exc
        if not payload or len(payload) > _MAX_UPLOAD_BYTES:
            raise PreviewDenied(f"{name} source is invalid")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != claimed:
            raise PreviewDenied(f"{name} sha256 mismatch")
        return payload, actual

    def preview(self, principal, engine, body):
        if principal.role != "administrator":
            raise PreviewDenied("catalog bootstrap requires administrator role")
        if engine.catalog(engine.state):
            raise PreviewDenied("catalog bootstrap requires an empty company catalog")
        warehouse, warehouse_sha = self._source(body, "warehouse", ".xlsx")
        siesa, siesa_sha = self._source(body, "siesa", ".xls")
        products = production_catalog(warehouse, siesa)
        source_digest = _parameter_sha256({
            "authority": "two-inventory-initial-catalog-v1",
            "warehouse_sha256": warehouse_sha,
            "siesa_sha256": siesa_sha,
        })
        token = secrets.token_urlsafe(32)
        self._candidates[token] = {
            "company_id": principal.company_id,
            "warehouse_sha256": warehouse_sha,
            "siesa_sha256": siesa_sha,
            "source_digest": source_digest,
            "products": products,
        }
        return {
            "can_apply": True, "apply_token": token,
            "product_count": len(products),
            "warehouse_sha256": warehouse_sha,
            "siesa_sha256": siesa_sha,
            "source_digest": source_digest,
        }

    def apply(self, principal, store, body):
        token = body.get("apply_token")
        candidate = self._candidates.get(token)
        if candidate is None:
            raise PreviewDenied("unknown or already-used bootstrap token")
        if candidate["company_id"] != principal.company_id:
            raise PreviewDenied("bootstrap token tenant mismatch")
        for field in ("warehouse_sha256", "siesa_sha256"):
            if body.get(field) != candidate[field]:
                raise PreviewDenied("bootstrap source hash mismatch")
        result = store.bootstrap_catalog(
            principal, products=copy.deepcopy(candidate["products"]),
            warehouse_sha256=candidate["warehouse_sha256"],
            siesa_sha256=candidate["siesa_sha256"],
            source_digest=candidate["source_digest"])
        self._candidates.pop(token, None)
        return result


_CATALOG_BOOTSTRAPS = CatalogBootstrapPreviewService()
_COMMAND_LOCK = RLock()
_COMMAND_RECEIPTS = {}
_COMMAND_RECEIPT_LIMIT = 2048
_MAX_UPLOAD_BYTES = 10 * 1024 * 1024
_MAX_UPLOAD_B64_CHARS = 4 * ((_MAX_UPLOAD_BYTES + 2) // 3)
_NATIVE_XLSX_PARSERS = {
    "warehouse": parse_warehouse_xlsx,
    "sales": parse_sales_xlsx,
    "siesa_availability": parse_siesa_xlsx,
}
_UPLOAD_EXTENSIONS = {
    "warehouse": {".xlsx", ".csv", ".txt"},
    "sales": {".xlsx", ".csv", ".txt"},
    "siesa_availability": {".xls", ".xlsx", ".csv", ".txt"},
    "in_transit": {".xlsx", ".csv", ".txt"},
    "production_planning": {".pdf", ".csv", ".txt"},
    "sailing_calendar": {".csv", ".txt"},
    "committed_orders": {".csv", ".txt"},
}
_UUID4 = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")


def _reset_command_receipts():
    with _COMMAND_LOCK:
        _COMMAND_RECEIPTS.clear()


def _canonical_value(value):
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    return value


def _parameter_sha256(params):
    canonical = json.dumps(_canonical_value(params), ensure_ascii=False,
                           sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_COMMAND_ERRORS = {
    "invalid_request": (400, "Revisa el formato de la solicitud."),
    "session_required": (401, "La sesión no está disponible. Vuelve a ingresar."),
    "invalid_command": (400, "La acción solicitada no está disponible."),
    "action_unavailable": (403, "Esta acción no está disponible para tu perfil."),
    "invalid_idempotency_key": (400, "Actualiza la vista antes de volver a intentar esta acción."),
    "invalid_expected_head": (400, "Actualiza la vista antes de continuar."),
    "invalid_input": (400, "Revisa los datos requeridos para esta acción."),
    "idempotency_conflict": (409, "Esta operación no coincide con el intento original. Actualiza la vista."),
    "idempotency_unavailable": (503, "No se puede confirmar esta operación de forma segura ahora. No la repitas; solicita soporte."),
    "stale_head": (409, "El estado cambió. Actualiza la vista antes de continuar."),
    "candidate_changed": (409, "La recomendación cambió. Revisa los valores actualizados antes de continuar."),
    "command_rejected": (400, "La acción no puede aplicarse en el estado actual."),
}


def _command_error(code, *, replayed=False):
    status, message = _COMMAND_ERRORS[code]
    return JSONResponse(
        {"detail": {"code": code, "operator_message": message}},
        status_code=status,
        headers={"Idempotency-Replayed": "true" if replayed else "false"})


def _receipt_response(receipt, *, replayed):
    return JSONResponse(
        receipt["body"], status_code=receipt["status"],
        headers={"Idempotency-Replayed": "true" if replayed else "false"})


def build_engine_for_mode(mode: str | None = None):
    selected = mode or os.getenv("FLOOR_TILE_LOCAL_MODE", "historical")
    if selected == "ashley_current":
        return build_ashley_current_review_engine()
    if selected == "historical":
        return build_historical_review_engine()
    if selected == "production":
        production = SailingEngine(
            config=PlanningConfig(default_voyage_days=15),
            today=date.today(), products=[], sessions={},
        )
        production.review_provenance = {
            "mode": "production_company",
            "source": "durable_company_state",
            "current_truth": True,
            "as_of": production.today.isoformat(),
        }
        production.review_banner = (
            "Entorno de producción de la compañía — verifica las fechas y la "
            "completitud de las fuentes antes de decidir.")
        return production
    if selected == "synthetic_release":
        return base_engine(today=date(2026, 9, 1))
    raise ValueError(f"unknown FLOOR_TILE_LOCAL_MODE: {selected}")


def engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = build_engine_for_mode()
    return _ENGINE


def configure_hosted(*, store, principal_resolver,
                     settings: SettingsV1 | None = None,
                     readiness_check=None):
    """Bind hosted authority to app state, never a process-global engine."""
    app.state.store = store
    app.state.principal_resolver = principal_resolver
    if settings is not None:
        if not settings.hosted:
            raise RuntimeError("hosted composition requires hosted settings")
        app.state.runtime_settings = settings
    app.state.readiness_check = readiness_check or (lambda: bool(
        app.state.store is not None and app.state.principal_resolver is not None))


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


@app.get("/ready", include_in_schema=False)
def ready():
    try:
        healthy = app.state.readiness_check()
    except Exception:
        healthy = False
    if not healthy:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    return {"status": "ready"}


def _resolve_hosted_principal(authorization):
    resolver = getattr(app.state, "principal_resolver", None)
    if resolver is None:
        raise _operator_error(401, "session_required",
                              "La sesión no está disponible. Vuelve a ingresar.")
    token = _token_from(authorization)
    try:
        resolve = getattr(resolver, "resolve_principal", resolver)
        return resolve(token)
    except AuthUnauthorized as exc:
        raise _operator_error(
            401, "session_required",
            "La sesión no está disponible. Vuelve a ingresar.") from exc
    except AuthForbidden as exc:
        raise _operator_error(
            403, "access_forbidden",
            "Esta sesión no tiene acceso a este espacio.") from exc


def _request_context(authorization):
    token = _token_from(authorization)
    store = getattr(app.state, "store", None)
    if store is not None:
        try:
            principal = _resolve_hosted_principal(authorization)
            return principal, store.load_engine(principal)
        except AuthForbidden:
            raise
        except PermissionError as exc:
            raise _operator_error(
                403, "access_forbidden",
                "Esta sesión no tiene acceso a este espacio.") from exc
    local_engine = engine()
    try:
        actor = local_engine.identity.resolve(token)
    except AuthorizationError as exc:
        raise _operator_error(
            401, "session_required",
            "La sesión no está disponible. Vuelve a ingresar.") from exc
    return (RequestPrincipal(f"local-{actor}", actor, "local-company", actor),
            local_engine)


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: str
    params: dict = Field(default_factory=dict)


def _token_from(authorization: Optional[str]) -> Optional[str]:
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:]
    return None


def _decode_params(params: dict) -> dict:
    """ISO date strings for *date/as_of fields → date objects."""
    out = {}
    for k, v in params.items():
        if isinstance(v, str) and (k.endswith("date") or k == "as_of"
                                   or k == "departure" or k.endswith("_at")):
            try:
                out[k] = date.fromisoformat(v)
                continue
            except ValueError:
                pass
        if isinstance(v, list):
            out[k] = [_decode_params(x) if isinstance(x, dict) else x
                      for x in v]
            continue
        if isinstance(v, dict):
            out[k] = _decode_params(v)
            continue
        out[k] = v
    return out


def _operator_error(status_code: int, code: str, operator_message: str):
    return HTTPException(
        status_code=status_code,
        detail={"code": code, "operator_message": operator_message},
    )


def _upload_name(value) -> tuple[str, str]:
    name = re.split(r"[/\\\\]", str(value or "uploaded-text"))[-1]
    if not name or name in {".", ".."}:
        raise PreviewDenied("uploaded file requires a filename")
    return name, os.path.splitext(name)[1].lower()


def _rows_with_adapter_errors(feed: str, parsed: dict) -> list[dict]:
    rows = list(parsed["rows"])
    error_field = {"warehouse": "m2", "sales": "daily_velocity",
                   "siesa_availability": "available_m2"}[feed]
    for diagnostic in parsed.get("diagnostics", []):
        if diagnostic.get("severity") == "error":
            rows.append({"product_ref": diagnostic["source_row_ref"],
                         error_field: diagnostic["message"]})
    return rows


@app.post("/api/session")
async def open_session(request: Request):
    if getattr(app.state, "store", None) is not None:
        raise _operator_error(
            404, "resource_not_found",
            "No encontramos el recurso solicitado.")
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("session body must be an object")
    except Exception as exc:
        raise _operator_error(
            400, "invalid_request", "Revisa el formato de la solicitud.") from exc
    role = body.get("role", "ashley")
    token = engine().identity.token_for(role)
    if token is None:
        raise _operator_error(
            403, "role_unavailable", "El perfil solicitado no está disponible.")
    return {"token": token, "role": role}


@app.get("/api/workspace")
def workspace(plan_id: Optional[str] = None,
              focus_sailing_id: Optional[str] = None,
              production_anchor_sailing_id: Optional[str] = None,
              factory_order_date: Optional[date] = None,
              authorization: Optional[str] = Header(default=None)):
    principal, request_engine = _request_context(authorization)
    try:
        composed = compose_workspace(request_engine, plan_id=plan_id,
                                     focus_sailing_id=focus_sailing_id,
                                     production_anchor_sailing_id=production_anchor_sailing_id,
                                     factory_order_date=factory_order_date,
                                     principal=principal)
    except ValueError as exc:
        raise _operator_error(
            404, "workspace_not_found",
            "No encontramos el contexto solicitado.") from exc
    return JSONResponse(composed)


@app.post("/api/assistant/chat")
async def assistant_chat(request: Request,
                         authorization: Optional[str] = Header(default=None)):
    principal, request_engine = _request_context(authorization)
    if principal.actor != "ashley":
        raise _operator_error(
            403, "assistant_access_forbidden",
            "Este asistente está disponible únicamente para Ashley.")
    try:
        body = validate_chat_body(await request.json())
        factory_order_date = body.get("factory_order_date")
        if factory_order_date is not None:
            factory_order_date = date.fromisoformat(factory_order_date)
        composed = compose_workspace(
            request_engine,
            plan_id=body.get("plan_id"),
            focus_sailing_id=body.get("focus_sailing_id"),
            production_anchor_sailing_id=body.get("production_anchor_sailing_id"),
            factory_order_date=factory_order_date,
            principal=principal,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        raise _operator_error(
            400, "assistant_invalid_request",
            "Revisa la pregunta y el contexto de planeación.") from exc

    provider = getattr(app.state, "assistant_provider", None) or configured_assistant()
    if provider is None:
        raise _operator_error(
            503, "assistant_unavailable",
            "El asistente no está disponible en este momento.")
    head_before = len(request_engine.state.events)
    context = grounded_context(composed)
    try:
        answer = await provider.answer(
            question=body["question"], history=body["history"],
            context=context)
    except Exception as exc:
        raise _operator_error(
            503, "assistant_unavailable",
            "El asistente no está disponible en este momento.") from exc
    if len(request_engine.state.events) != head_before:
        raise RuntimeError("read-only assistant changed domain state")
    return JSONResponse({
        "answer": answer,
        "grounded_head_seq": head_before,
        "grounding_as_of": (context.get("grounding", {}).get("review_provenance") or {}).get("as_of"),
        "mutated": False,
    })


def _request_actor(authorization):
    principal, request_engine = _request_context(authorization)
    return principal.effective_actor, principal, request_engine


@app.get("/api/sources")
def sources(authorization: Optional[str] = Header(default=None)):
    _, principal, request_engine = _request_actor(authorization)
    return JSONResponse(compose_source_hub(request_engine, principal))


@app.post("/api/catalog/bootstrap/preview")
def catalog_bootstrap_preview(body: dict,
                              authorization: Optional[str] = Header(default=None)):
    _, principal, request_engine = _request_actor(authorization)
    try:
        preview = _CATALOG_BOOTSTRAPS.preview(principal, request_engine, body)
    except (PreviewDenied, ValueError) as exc:
        raise _operator_error(
            400, "catalog_bootstrap_invalid",
            "No pudimos preparar el catálogo inicial. Revisa ambos inventarios.") from exc
    return JSONResponse(preview)


@app.post("/api/catalog/bootstrap/apply")
def catalog_bootstrap_apply(body: dict,
                            authorization: Optional[str] = Header(default=None)):
    _, principal, _ = _request_actor(authorization)
    store = getattr(app.state, "store", None)
    if store is None:
        raise _operator_error(
            400, "catalog_bootstrap_unavailable",
            "El catálogo inicial sólo puede persistirse en el entorno alojado.")
    try:
        result = _CATALOG_BOOTSTRAPS.apply(principal, store, body)
    except (PreviewDenied, ValueError) as exc:
        raise _operator_error(
            400, "catalog_bootstrap_invalid",
            "No pudimos aplicar el catálogo inicial. Vuelve a previsualizarlo.") from exc
    return JSONResponse(result)


@app.post("/api/input/preview")
def input_preview(body: dict, authorization: Optional[str] = Header(default=None)):
    actor, _, request_engine = _request_actor(authorization)
    try:
        if body.get("preview_id") and body.get("corrections"):
            preview = _INPUT_PREVIEWS.revise(
                request_engine, actor=actor, preview_id=body["preview_id"],
                corrections=body["corrections"])
        else:
            text = body.get("text")
            raw_source_ref = body.get("raw_source_ref")
            if body.get("input_mode") == "upload":
                encoded = body.get("file_content_b64")
                if not isinstance(encoded, str):
                    raise PreviewDenied("upload requires file_content_b64")
                if len(encoded) > _MAX_UPLOAD_B64_CHARS:
                    raise PreviewDenied("uploaded file exceeds 10 MiB")
                try:
                    uploaded = base64.b64decode(encoded, validate=True)
                except binascii.Error as exc:
                    raise PreviewDenied("uploaded file must be valid base64") from exc
                if len(uploaded) > _MAX_UPLOAD_BYTES:
                    raise PreviewDenied("uploaded file exceeds 10 MiB")
                raw_source_ref, extension = _upload_name(body.get("file_name"))
                feed = body.get("feed")
                if extension not in _UPLOAD_EXTENSIONS.get(feed, set()):
                    raise PreviewDenied("uploaded extension is not supported for this source")
                if feed in _NATIVE_XLSX_PARSERS and extension == ".xlsx":
                    parsed = _NATIVE_XLSX_PARSERS[feed](
                        uploaded, catalog=list(request_engine.catalog(request_engine.state).values()))
                    adapter_as_of = parsed.get("as_of") if feed == "sales" else body.get("as_of")
                    preview = _INPUT_PREVIEWS.preview(
                        request_engine, actor=actor, feed=feed, input_mode="upload",
                        as_of=adapter_as_of,
                        rows=_rows_with_adapter_errors(feed, parsed),
                        raw_source_ref=raw_source_ref, _server_parsed=True)
                    preview["adapter_diagnostics"] = parsed.get("diagnostics", [])
                    return JSONResponse(jsonable_encoder(preview))
                if feed == "siesa_availability" and extension == ".xls":
                    parsed = parse_siesa_xls(
                        uploaded, catalog=list(request_engine.catalog(request_engine.state).values()))
                    preview = _INPUT_PREVIEWS.preview(
                        request_engine, actor=actor, feed=feed, input_mode="upload",
                        as_of=body.get("as_of"),
                        rows=_rows_with_adapter_errors(feed, parsed),
                        raw_source_ref=raw_source_ref, _server_parsed=True)
                    preview["adapter_diagnostics"] = parsed.get("diagnostics", [])
                    return JSONResponse(jsonable_encoder(preview))
                if feed == "in_transit" and extension == ".xlsx":
                    scheduled = parse_scheduled_dispatch_xlsx(
                        uploaded, catalog=list(request_engine.catalog(request_engine.state).values()))
                    orders = scheduled["orders"]
                    received = sum(len(order["lines"]) for order in orders)
                    return JSONResponse(jsonable_encoder({
                        "preview_id": None, "feed": "in_transit",
                        "input_mode": "upload", "entered_via": "parsed",
                        "raw_source_ref": raw_source_ref,
                        "authority_state": "scheduled_tentative_not_in_transit",
                        "scheduled_orders": orders,
                        "unmatched_products": scheduled["unmatched_products"],
                        "summary": {"received": received, "valid": received,
                                    "held": len(scheduled["unmatched_products"]),
                                    "invalid": 0},
                        "replacement_effect": {"semantics": "preview_only"},
                        "rows": [], "can_apply": False,
                        "legal_apply_action": None, "apply_token": None,
                    }))
                if feed == "production_planning" and extension == ".pdf":
                    as_of = InputPreviewService._parse_as_of(body.get("as_of"))
                    parsed = parse_production_schedule_pdf(
                        uploaded,
                        catalog=list(request_engine.catalog(request_engine.state).values()),
                        evidence_as_of=as_of)
                    preview = _INPUT_PREVIEWS.preview(
                        request_engine, actor=actor, feed="production_planning",
                        input_mode="upload", as_of=as_of,
                        rows=parsed["rows"], raw_source_ref=raw_source_ref,
                        _server_parsed=True)
                    preview["authority_state"] = parsed["authority"]
                    preview["unmatched_products"] = parsed["unmatched_products"]
                    return JSONResponse(jsonable_encoder(preview))
                try:
                    text = uploaded.decode("utf-8-sig")
                except UnicodeDecodeError as exc:
                    raise PreviewDenied(
                        "uploaded file must be UTF-8 text or a supported native file") from exc
            preview = _INPUT_PREVIEWS.preview(
                request_engine, actor=actor, feed=body.get("feed"),
                input_mode=body.get("input_mode"), as_of=body.get("as_of"),
                rows=body.get("rows"), text=text,
                raw_source_ref=raw_source_ref)
    except (PreviewDenied, ValueError) as exc:
        raise _operator_error(
            400, "input_invalid",
            "Revisa los datos ingresados antes de continuar.") from exc
    return JSONResponse(preview)


@app.post("/api/input/apply")
def input_apply(body: dict, authorization: Optional[str] = Header(default=None)):
    actor, principal, request_engine = _request_actor(authorization)
    try:
        store = getattr(app.state, "store", None)
        if store is None:
            response = _INPUT_PREVIEWS.apply(
                request_engine, actor=actor, apply_token=body.get("apply_token"))
        else:
            apply_token = body.get("apply_token")
            candidate = _INPUT_PREVIEWS._candidates.get(apply_token)
            if candidate is None:
                raise PreviewDenied("unknown or already-used apply token")
            with store.unit_of_work(principal) as uow:
                if candidate["digest"] != _INPUT_PREVIEWS._digest(
                        {k: v for k, v in candidate.items() if k != "digest"}):
                    raise PreviewDenied("apply token candidate digest mismatch")
                rebound = dict(candidate)
                rebound["engine_ref"] = id(uow.engine)
                rebound["digest"] = _INPUT_PREVIEWS._digest(
                    {k: v for k, v in rebound.items() if k != "digest"})
                _INPUT_PREVIEWS._candidates[apply_token] = rebound
                response = _INPUT_PREVIEWS.apply(
                    uow.engine, actor=actor, apply_token=apply_token)
                uow._command = rebound["command"]
                uow.commit(idempotency_key=f"input:{apply_token}",
                           parameter_hash=_parameter_sha256(rebound["params"]),
                           body=response)
    except PreviewDenied as exc:
        status = 409 if "stale" in str(exc) else 400
        raise _operator_error(
            status,
            "input_stale" if status == 409 else "input_invalid",
            ("La previsualización cambió. Vuelve a revisarla antes de aplicar."
             if status == 409 else
             "No pudimos aplicar la información. Revisa la previsualización."),
        ) from exc
    except (AuthorizationError, CommandError, ValueError) as exc:
        raise _operator_error(
            400, "input_rejected",
            "La información no puede aplicarse en el estado actual.") from exc
    return JSONResponse(response)


@app.post("/api/command")
async def command(request: Request,
                  authorization: Optional[str] = Header(default=None),
                  idempotency_key: Optional[str] = Header(
                      default=None, alias="Idempotency-Key"),
                  x_expected_head: Optional[str] = Header(default=None)):
    store = getattr(app.state, "store", None)
    principal = (_resolve_hosted_principal(authorization)
                 if store is not None else None)
    # Parse the exact request carrier ourselves so this route never leaks a
    # framework-owned 422 body.
    try:
        raw = await request.json()
        body = CommandRequest(**raw)
    except Exception:
        return _command_error("invalid_request")

    token = _token_from(authorization)
    actor = None
    if store is None:
        try:
            actor = engine().identity.resolve(token)
        except AuthorizationError:
            return _command_error("session_required")
    if body.command not in COMMAND_SCHEMAS:
        return _command_error("invalid_command")
    if store is None and body.command not in engine().bus.commands_available_to(actor):
        return _command_error("action_unavailable")
    if store is None and body.command == "ResolveImplication":
        try:
            engine().bus.authorize_implication_resolution(
                actor, body.params.get("implication_id"))
        except AuthorizationError:
            return _command_error("action_unavailable")
    if not isinstance(idempotency_key, str) or _UUID4.fullmatch(idempotency_key) is None:
        return _command_error("invalid_idempotency_key")
    if (not isinstance(x_expected_head, str)
            or re.fullmatch(r"0|[1-9][0-9]*", x_expected_head) is None):
        return _command_error("invalid_expected_head")
    expected_head = int(x_expected_head)

    if store is not None:
        decoded = _decode_params(body.params)
        parameter_hash = _parameter_sha256(decoded)

        def prepare(locked_engine, incoming):
            actor = principal.effective_actor
            if body.command not in locked_engine.bus.commands_available_to(actor):
                raise AuthorizationError("action unavailable")
            if body.command == "ResolveImplication":
                locked_engine.bus.authorize_implication_resolution(
                    actor, incoming.get("implication_id"))
            return validate_command(
                body.command, incoming, catalog_ids=set(locked_engine.catalog()),
                engine=locked_engine)

        def success(result, locked_engine, head):
            return 200, {
                "ok": True, "result": result, "workspace_schema_version": 2,
                "head_seq": head,
                "workspace": compose_workspace(locked_engine, principal=principal),
            }

        def stale(current_head):
            return 409, {"detail": {
                "code": "stale_head",
                "operator_message": _COMMAND_ERRORS["stale_head"][1]}}

        def rejected(exc):
            if isinstance(exc, AuthorizationError):
                code = "action_unavailable"
            else:
                code = ("candidate_changed" if str(exc) == "candidate_changed"
                        else "command_rejected")
            return _COMMAND_ERRORS[code][0], {"detail": {
                "code": code, "operator_message": _COMMAND_ERRORS[code][1]}}

        try:
            receipt, replayed = store.execute_idempotent(
                principal, command=body.command, params=decoded,
                expected_head=expected_head, idempotency_key=idempotency_key,
                parameter_hash=parameter_hash, prepare=prepare,
                response_factory=success, stale_factory=stale,
                error_factory=rejected)
        except IdempotencyConflict:
            return _command_error("idempotency_conflict")
        except PermissionError:
            raise _operator_error(
                403, "access_forbidden",
                "Esta sesión no tiene acceso a este espacio.")
        except AuthorizationError:
            return _command_error("action_unavailable")
        except (CommandError, ValueError):
            return _command_error("invalid_input")
        return JSONResponse(
            receipt.body, status_code=receipt.status,
            headers={"Idempotency-Replayed": "true" if replayed else "false"})

    try:
        params = validate_command(body.command, _decode_params(body.params),
                                  catalog_ids=set(engine().catalog()), engine=engine())
    except ValueError:
        return _command_error("invalid_input")

    parameter_hash = _parameter_sha256(params)
    receipt_key = (f"local-{actor}", actor, idempotency_key)
    with _COMMAND_LOCK:
        existing = _COMMAND_RECEIPTS.get(receipt_key)
        if existing is not None:
            if (existing.get("command") != body.command
                    or existing.get("parameter_hash") != parameter_hash):
                return _command_error("idempotency_conflict")
            return _receipt_response(existing, replayed=True)
        if len(_COMMAND_RECEIPTS) >= _COMMAND_RECEIPT_LIMIT:
            return _command_error("idempotency_unavailable")

        current_head = len(engine().state.events)
        if expected_head != current_head:
            response = _command_error("stale_head")
            response_body = {"detail": {
                "code": "stale_head",
                "operator_message": _COMMAND_ERRORS["stale_head"][1]}}
            _COMMAND_RECEIPTS[receipt_key] = {
                "command": body.command, "parameter_hash": parameter_hash,
                "status": 409, "body": response_body, "head": current_head}
            return response

        try:
            # The bus re-authorizes the resolved token and the domain
            # revalidates against this current head; actions remain hints.
            result = engine().execute(body.command, params, token=token)
            principal = RequestPrincipal(
                f"local-{actor}", actor, "local-company", actor)
            head = len(engine().state.events)
            response_body = {
                "ok": True, "result": result, "workspace_schema_version": 2,
                "head_seq": head,
                "workspace": compose_workspace(engine(), principal=principal)}
            status = 200
        except AuthorizationError:
            response_body = {"detail": {
                "code": "action_unavailable",
                "operator_message": _COMMAND_ERRORS["action_unavailable"][1]}}
            status = 403
        except CommandError as exc:
            code = "candidate_changed" if str(exc) == "candidate_changed" else "command_rejected"
            response_body = {"detail": {
                "code": code, "operator_message": _COMMAND_ERRORS[code][1]}}
            status = _COMMAND_ERRORS[code][0]

        receipt = {"command": body.command, "parameter_hash": parameter_hash,
                   "status": status, "body": response_body,
                   "head": len(engine().state.events)}
        _COMMAND_RECEIPTS[receipt_key] = receipt
        return _receipt_response(receipt, replayed=False)


# ── dev-only scenario driver (unlinked; never in the product UI) ───────────

@app.post("/api/dev/scenario/{name}")
def dev_scenario(name: str):
    global _ENGINE
    if getattr(app.state, "store", None) is not None:
        raise _operator_error(
            404, "resource_not_found",
            "No encontramos el recurso solicitado.")
    fn = SCENARIOS.get(name)
    if fn is None:
        raise HTTPException(status_code=404,
                            detail=f"unknown scenario {name}; "
                                   f"known: {sorted(SCENARIOS)}")
    _ENGINE = fn()
    _reset_command_receipts()
    return {"ok": True, "scenario": name}


@app.post("/api/dev/reset")
def dev_reset():
    global _ENGINE
    if getattr(app.state, "store", None) is not None:
        raise _operator_error(
            404, "resource_not_found",
            "No encontramos el recurso solicitado.")
    _ENGINE = build_engine_for_mode()
    _reset_command_receipts()
    return {"ok": True}


# ── static frontend (built dist) ───────────────────────────────────────────

_DIST = os.path.join(os.path.dirname(__file__), "..", "..", "frontend",
                     "dist")

if os.path.isdir(_DIST):
    from fastapi.staticfiles import StaticFiles

    app.mount("/assets", StaticFiles(directory=os.path.join(_DIST, "assets")),
              name="assets")

    @app.get("/")
    def index():
        return FileResponse(os.path.join(_DIST, "index.html"))

    if os.getenv("FLOOR_TILE_RUNTIME") != "hosted":
        @app.get("/dev.html")
        def dev_page():
            return FileResponse(os.path.join(_DIST, "dev.html"))


def configure_hosted_from_environment():
    """Construct the T5 hosted Auth/data boundary from server-only settings."""
    settings = load_settings()
    if not settings.hosted:
        raise RuntimeError("FLOOR_TILE_RUNTIME=hosted is required")
    dsn = settings.database_url
    project_ref = settings.supabase_project_ref
    assert dsn is not None and project_ref is not None
    audience = os.getenv("SUPABASE_JWT_AUDIENCE", "authenticated")
    jwks = SupabaseJwksLoader(project_ref=project_ref)
    bindings = PostgresBindingRepository(dsn)
    resolver = SupabaseIdentityResolver(
        project_ref=project_ref,
        audience=audience,
        jwks=jwks,
        bindings=bindings,
    )
    seed_mode = os.getenv("FLOOR_TILE_SEED_MODE", "production")
    if seed_mode not in {"production", "synthetic_release"}:
        raise RuntimeError("FLOOR_TILE_SEED_MODE must be production or synthetic_release")
    store = PostgresAdapter(
        dsn,
        lambda: build_engine_for_mode(seed_mode),
        seed_mode=seed_mode,
        seed_version=int(os.getenv("FLOOR_TILE_SEED_VERSION", "1")),
        allow_production=True,
        build_version=settings.build_version,
    )

    def readiness_check():
        if (app.state.store is not store
                or app.state.principal_resolver is not resolver):
            return False
        connection = store._connect()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT to_regclass('floor_tile.domain_events') IS NOT NULL "
                    "AND to_regclass('floor_tile.app_users') IS NOT NULL "
                    "AND to_regclass('floor_tile.config_versions') IS NOT NULL")
                row = cursor.fetchone()
                return bool(next(iter(row.values())) if isinstance(row, dict)
                            else row and row[0])
            finally:
                cursor.close()
        finally:
            connection.close()

    configure_hosted(
        store=store,
        principal_resolver=resolver,
        settings=settings,
        readiness_check=readiness_check,
    )
    return app


if os.getenv("FLOOR_TILE_RUNTIME") == "hosted":
    configure_hosted_from_environment()
