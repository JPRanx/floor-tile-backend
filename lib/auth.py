"""Supabase JWT authentication for API routes.

Verifies the Authorization: Bearer <token> header against the Supabase JWT secret.
Applied as middleware in main.py for all /api/* paths.
"""

import jwt
from fastapi import Request
from fastapi.responses import JSONResponse
from config import settings


PUBLIC_PATHS = {"/", "/health", "/docs", "/openapi.json", "/redoc"}


def _unauthorized(message: str) -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"error": {"code": "unauthorized", "message": message}},
    )


async def auth_middleware(request: Request, call_next):
    path = request.url.path

    # Public paths and non-API paths pass through
    if path in PUBLIC_PATHS or not path.startswith("/api/"):
        return await call_next(request)

    # OPTIONS (CORS preflight) passes through
    if request.method == "OPTIONS":
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return _unauthorized("Missing or malformed Authorization header")

    token = auth_header.removeprefix("Bearer ").strip()
    secret = settings.supabase_jwt_secret
    if not secret:
        return _unauthorized("Server auth misconfigured: SUPABASE_JWT_SECRET not set")

    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience="authenticated",
        )
    except jwt.ExpiredSignatureError:
        return _unauthorized("Token expired")
    except jwt.InvalidTokenError as exc:
        return _unauthorized(f"Invalid token: {exc}")

    # Attach user info to request state for downstream handlers
    request.state.user_id = payload.get("sub")
    request.state.user_email = payload.get("email")
    return await call_next(request)
