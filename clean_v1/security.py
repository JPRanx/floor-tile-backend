"""ASGI security boundary for the hosted Clean V1 API."""
from __future__ import annotations

import json
from collections.abc import Callable

from .settings_v1 import SettingsV1

_SECURITY_HEADERS = (
    (b"cache-control", b"no-store"),
    (b"content-security-policy", b"default-src 'none'; frame-ancestors 'none'; base-uri 'none'"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
)
_HSTS = (b"strict-transport-security", b"max-age=31536000; includeSubDomains")
_ALLOWED_CORS_HEADERS = "authorization, content-type, idempotency-key, x-expected-head"


def _json_response(status: int, body: dict):
    encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
    return status, encoded, [(b"content-type", b"application/json"),
                             (b"content-length", str(len(encoded)).encode("ascii"))]


class SecurityMiddleware:
    """Apply fail-closed transport, CORS, body, and response policy."""

    def __init__(self, app, settings_getter: Callable[[], SettingsV1]):
        self.app = app
        self.settings_getter = settings_getter

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        settings = self.settings_getter()
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        origin = headers.get(b"origin", b"").decode("latin-1")
        client = scope.get("client") or (None, None)
        trusted_proxy = settings.trusts_proxy(client[0])
        forwarded_proto = (headers.get(b"x-forwarded-proto", b"")
                           .decode("latin-1").split(",", 1)[0].strip().lower())
        secure = scope.get("scheme") == "https" or (trusted_proxy and forwarded_proto == "https")

        cors_headers: list[tuple[bytes, bytes]] = []
        if settings.hosted and origin:
            if origin not in settings.cors_allowed_origins:
                await self._send_direct(send, settings, *_json_response(
                    403, {"detail": {"code": "origin_forbidden"}}))
                return
            cors_headers = [
                (b"access-control-allow-origin", origin.encode("latin-1")),
                (b"access-control-allow-credentials", b"true"),
                (b"vary", b"Origin"),
            ]

        if settings.hosted and not secure:
            await self._send_direct(send, settings, *_json_response(
                400, {"detail": {"code": "https_required"}}), extra=cors_headers)
            return

        path = scope.get("path", "")
        if settings.hosted and (path == "/api/session" or path == "/dev.html"
                                or path.startswith("/api/dev/")):
            await self._send_direct(send, settings, *_json_response(
                404, {"detail": {"code": "resource_not_found"}}), extra=cors_headers)
            return

        if (settings.hosted and scope.get("method") == "OPTIONS"
                and headers.get(b"access-control-request-method")):
            requested_headers = headers.get(b"access-control-request-headers", b"").decode("latin-1")
            requested = {item.strip().lower() for item in requested_headers.split(",") if item.strip()}
            allowed = {item.strip() for item in _ALLOWED_CORS_HEADERS.split(",")}
            if not requested.issubset(allowed):
                await self._send_direct(send, settings, *_json_response(
                    403, {"detail": {"code": "cors_headers_forbidden"}}), extra=cors_headers)
                return
            extra = cors_headers + [
                (b"access-control-allow-methods", b"GET, POST, OPTIONS"),
                (b"access-control-allow-headers", _ALLOWED_CORS_HEADERS.encode("ascii")),
                (b"access-control-max-age", b"600"),
            ]
            await self._send_direct(send, settings, 204, b"", [], extra=extra)
            return

        try:
            declared = int(headers.get(b"content-length", b"0"))
        except ValueError:
            declared = settings.max_request_bytes + 1
        if declared > settings.max_request_bytes:
            await self._send_direct(send, settings, *_json_response(
                413, {"detail": {"code": "request_too_large"}}), extra=cors_headers)
            return

        bounded_receive = receive
        if scope.get("method") in {"POST", "PUT", "PATCH"}:
            messages = []
            received = 0
            while True:
                message = await receive()
                messages.append(message)
                if message.get("type") == "http.request":
                    received += len(message.get("body", b""))
                    if received > settings.max_request_bytes:
                        await self._send_direct(send, settings, *_json_response(
                            413, {"detail": {"code": "request_too_large"}}),
                            extra=cors_headers)
                        return
                    if not message.get("more_body", False):
                        break
                elif message.get("type") == "http.disconnect":
                    break
            position = 0

            async def replay_receive():
                nonlocal position
                if position < len(messages):
                    message = messages[position]
                    position += 1
                    return message
                return {"type": "http.disconnect"}

            bounded_receive = replay_receive

        async def secure_send(message):
            if message.get("type") == "http.response.start":
                current = {key.lower(): (key, value) for key, value in message.get("headers", [])}
                for key, value in _SECURITY_HEADERS + ((_HSTS,) if settings.hosted else ()):
                    current[key] = (key, value)
                for key, value in cors_headers:
                    current[key] = (key, value)
                message["headers"] = list(current.values())
            await send(message)

        try:
            await self.app(scope, bounded_receive, secure_send)
        except _BodyTooLarge:
            await self._send_direct(send, settings, *_json_response(
                413, {"detail": {"code": "request_too_large"}}), extra=cors_headers)

    @staticmethod
    async def _send_direct(send, settings, status, body, headers, *, extra=()):
        merged = {key.lower(): (key, value) for key, value in headers}
        for key, value in _SECURITY_HEADERS + ((_HSTS,) if settings.hosted else ()):
            merged[key] = (key, value)
        for key, value in extra:
            merged[key] = (key, value)
        await send({"type": "http.response.start", "status": status,
                    "headers": list(merged.values())})
        await send({"type": "http.response.body", "body": body})


class _BodyTooLarge(Exception):
    pass
