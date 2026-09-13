"""Project-pinned Supabase JWT verification and invite-only identity resolution."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from threading import RLock
import time
from typing import Callable, Mapping, Protocol, Sequence
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, UUID, uuid5

import jwt
from jwt import InvalidTokenError

from .ports import RequestPrincipal

_PROJECT_REF = re.compile(r"^[a-z0-9]{20}$")
_ASYMMETRIC_ALGORITHMS = frozenset({"RS256", "ES256"})


class AuthUnauthorized(Exception):
    """The bearer token is absent or cryptographically invalid."""


class AuthForbidden(Exception):
    """The verified subject has no active invite-only application binding."""


class AuthConfigurationError(ValueError):
    """Identity verification is not pinned to an acceptable Supabase project."""


class AuthAuditConflict(RuntimeError):
    """An authoritative event id was replayed with different verified content."""


@dataclass(frozen=True)
class IdentityBinding:
    auth_user_id: str
    actor: str
    company_id: str
    role: str
    active: bool


class BindingRepository(Protocol):
    def find_active_by_auth_user_id(self, auth_user_id: str) -> IdentityBinding | None: ...


def _row_value(row, name: str, position: int):
    if isinstance(row, Mapping):
        return row[name]
    return row[position]


class PostgresBindingRepository:
    """Resolve only the invite binding under the subject bootstrap RLS policy."""

    def __init__(self, dsn: str | None = None, *, connection_factory=None) -> None:
        if connection_factory is None and not dsn:
            raise AuthConfigurationError("a Postgres connection source is required")
        self._dsn = dsn
        self._connection_factory = connection_factory

    def _connect(self):
        if self._connection_factory is not None:
            return self._connection_factory()
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise AuthConfigurationError("psycopg is required") from exc
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def find_active_by_auth_user_id(self, auth_user_id: str) -> IdentityBinding | None:
        try:
            subject = str(UUID(str(auth_user_id)))
        except (TypeError, ValueError) as exc:
            raise AuthUnauthorized("unauthorized") from exc
        connection = self._connect()
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT set_config('floor_tile.auth_user_id', %s, true)",
                (subject,),
            )
            cursor.execute(
                "SELECT auth_user_id,actor,company_id,role,active "
                "FROM floor_tile.app_users "
                "WHERE auth_user_id=%s AND active",
                (subject,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return IdentityBinding(
                auth_user_id=str(_row_value(row, "auth_user_id", 0)),
                actor=_row_value(row, "actor", 1),
                company_id=str(_row_value(row, "company_id", 2)),
                role=_row_value(row, "role", 3),
                active=bool(_row_value(row, "active", 4)),
            )
        finally:
            if cursor is not None:
                cursor.close()
            connection.rollback()
            connection.close()


class SupabaseJwksLoader:
    """Fetch the one pinned Supabase JWKS with a bounded in-process cache."""

    def __init__(self, *, project_ref: str, ttl_seconds: int = 600,
                 timeout_seconds: int = 5) -> None:
        if not _PROJECT_REF.fullmatch(project_ref):
            raise AuthConfigurationError("invalid Supabase project ref")
        if ttl_seconds < 30 or ttl_seconds > 1200 or timeout_seconds < 1 or timeout_seconds > 15:
            raise AuthConfigurationError("invalid JWKS cache or timeout")
        self.url = f"https://{project_ref}.supabase.co/auth/v1/.well-known/jwks.json"
        self.ttl_seconds = ttl_seconds
        self.timeout_seconds = timeout_seconds
        self._lock = RLock()
        self._document = None
        self._expires_at = 0.0

    def _fetch(self) -> Mapping[str, object]:
        request = Request(self.url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                if response.status != 200:
                    raise AuthConfigurationError("JWKS endpoint unavailable")
                document = json.loads(response.read(131073))
        except AuthConfigurationError:
            raise
        except Exception as exc:
            raise AuthConfigurationError("JWKS endpoint unavailable") from exc
        keys = document.get("keys") if isinstance(document, Mapping) else None
        if not isinstance(keys, list) or not keys:
            raise AuthConfigurationError("Supabase asymmetric JWKS is not configured")
        return document

    def __call__(self) -> Mapping[str, object]:
        now = time.monotonic()
        with self._lock:
            if self._document is None or now >= self._expires_at:
                self._document = self._fetch()
                self._expires_at = now + self.ttl_seconds
            return self._document

    def force_refresh(self) -> Mapping[str, object]:
        with self._lock:
            self._document = self._fetch()
            self._expires_at = time.monotonic() + self.ttl_seconds
            return self._document


class _BoundBusIdentity:
    def __init__(self, principal: RequestPrincipal) -> None:
        self._principal = principal

    def resolve(self, _token: str) -> str:
        """Expose only the already verified effective actor to the domain bus."""
        return self._principal.actor


class SupabaseIdentityResolver:
    """Verify a project JWT, then derive authority only from the database binding.

    ``jwks`` may be a fetched JWKS mapping or a zero-argument loader. Production
    callers must fetch only ``jwks_url`` over TLS and apply their bounded cache.
    No company/role/actor claim from the token participates in authorization.
    """

    def __init__(
        self,
        *,
        project_ref: str,
        audience: str,
        jwks: Mapping[str, object] | Callable[[], Mapping[str, object]],
        bindings: BindingRepository,
        allowed_algorithms: Sequence[str] = ("RS256", "ES256"),
        clock_skew_seconds: int = 30,
    ) -> None:
        if not _PROJECT_REF.fullmatch(project_ref):
            raise AuthConfigurationError("invalid Supabase project ref")
        algorithms = tuple(allowed_algorithms)
        if not algorithms or not set(algorithms) <= _ASYMMETRIC_ALGORITHMS:
            raise AuthConfigurationError("only approved asymmetric JWT algorithms are allowed")
        if not audience or clock_skew_seconds < 0 or clock_skew_seconds > 60:
            raise AuthConfigurationError("invalid audience or clock skew")
        self.project_ref = project_ref
        self.issuer = f"https://{project_ref}.supabase.co/auth/v1"
        self.jwks_url = f"{self.issuer}/.well-known/jwks.json"
        self.audience = audience
        self._jwks = jwks
        self._bindings = bindings
        self._algorithms = algorithms
        self._leeway = clock_skew_seconds

    def _key_for(self, token: str):
        try:
            header = jwt.get_unverified_header(token)
        except (InvalidTokenError, TypeError) as exc:
            raise AuthUnauthorized("unauthorized") from exc
        kid, algorithm = header.get("kid"), header.get("alg")
        if not isinstance(kid, str) or algorithm not in self._algorithms:
            raise AuthUnauthorized("unauthorized")
        document = self._jwks() if callable(self._jwks) else self._jwks
        keys = document.get("keys") if isinstance(document, Mapping) else None
        if not isinstance(keys, list):
            raise AuthConfigurationError("invalid JWKS document")
        matches = [key for key in keys if isinstance(key, Mapping) and key.get("kid") == kid]
        if not matches and callable(self._jwks) and hasattr(self._jwks, "force_refresh"):
            document = self._jwks.force_refresh()
            keys = document.get("keys") if isinstance(document, Mapping) else None
            if not isinstance(keys, list):
                raise AuthConfigurationError("invalid JWKS document")
            matches = [key for key in keys if isinstance(key, Mapping) and key.get("kid") == kid]
        if len(matches) != 1:
            raise AuthUnauthorized("unauthorized")
        key = matches[0]
        expected_kty = "RSA" if algorithm.startswith("RS") else "EC"
        if key.get("kty") != expected_kty or key.get("alg") not in (None, algorithm) or key.get("use") not in (None, "sig"):
            raise AuthUnauthorized("unauthorized")
        try:
            return jwt.PyJWK.from_dict(dict(key), algorithm=algorithm).key
        except (InvalidTokenError, ValueError, KeyError) as exc:
            raise AuthUnauthorized("unauthorized") from exc

    def resolve_principal(self, token: str) -> RequestPrincipal:
        if not isinstance(token, str) or not token:
            raise AuthUnauthorized("unauthorized")
        key = self._key_for(token)
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=list(self._algorithms),
                audience=self.audience,
                issuer=self.issuer,
                leeway=self._leeway,
                options={"require": ["iss", "aud", "sub", "exp"]},
            )
            subject = claims["sub"]
            if not isinstance(subject, str) or not subject.strip():
                raise AuthUnauthorized("unauthorized")
            UUID(subject)
        except (InvalidTokenError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, AuthUnauthorized):
                raise
            raise AuthUnauthorized("unauthorized") from exc
        binding = self._bindings.find_active_by_auth_user_id(subject)
        if binding is None or not binding.active or binding.auth_user_id != subject:
            raise AuthForbidden("forbidden")
        if binding.actor not in {"ashley", "elicio"} or not binding.company_id or not binding.role:
            raise AuthForbidden("forbidden")
        return RequestPrincipal(
            auth_user_id=binding.auth_user_id,
            actor=binding.actor,
            company_id=binding.company_id,
            role=binding.role,
        )

    @staticmethod
    def for_principal(principal: RequestPrincipal) -> _BoundBusIdentity:
        return _BoundBusIdentity(principal)


_AUTH_LIFECYCLE_TYPES = frozenset({
    "invited", "activated", "role_changed", "recovery_requested",
    "recovery_completed", "revoked",
})


@dataclass(frozen=True)
class AuthAuditReceipt:
    event_id: str
    event_type: str
    auth_user_id: str
    company_id: str
    occurred_at: datetime


class AuthLifecycleAuditIngestor:
    """Append project-signed Auth lifecycle evidence; never trust a browser claim.

    Signature verification is an injected server-side adapter because Supabase's
    production hook/log transport is selected and wired in T8. The verifier must
    return the authoritative event mapping or raise ``AuthUnauthorized``.
    """

    def __init__(self, *, project_ref: str, verify_signed_event: Callable,
                 connection_factory) -> None:
        if not _PROJECT_REF.fullmatch(project_ref):
            raise AuthConfigurationError("invalid Supabase project ref")
        if not callable(verify_signed_event) or not callable(connection_factory):
            raise AuthConfigurationError("signed verifier and connection factory are required")
        self.project_ref = project_ref
        self._verify = verify_signed_event
        self._connect = connection_factory

    @staticmethod
    def _validated_time(value) -> datetime:
        if not isinstance(value, str):
            raise AuthUnauthorized("unauthorized")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise AuthUnauthorized("unauthorized") from exc
        if parsed.tzinfo is None:
            raise AuthUnauthorized("unauthorized")
        return parsed.astimezone(timezone.utc)

    def _validate(self, signed_event) -> tuple[dict, str, str, str, datetime, str]:
        try:
            verified = self._verify(signed_event)
        except AuthUnauthorized:
            raise
        except Exception as exc:
            raise AuthUnauthorized("unauthorized") from exc
        if not isinstance(verified, Mapping):
            raise AuthUnauthorized("unauthorized")
        project_ref = verified.get("project_ref")
        event_id = verified.get("event_id")
        event_type = verified.get("event_type")
        subject_value = verified.get("subject")
        if project_ref != self.project_ref:
            raise AuthUnauthorized("unauthorized")
        if (not isinstance(event_id, str) or not event_id.strip()
                or len(event_id.encode("utf-8")) > 500):
            raise AuthUnauthorized("unauthorized")
        if event_type not in _AUTH_LIFECYCLE_TYPES:
            raise AuthUnauthorized("unauthorized")
        try:
            subject = str(UUID(str(subject_value)))
        except (TypeError, ValueError) as exc:
            raise AuthUnauthorized("unauthorized") from exc
        occurred_at = self._validated_time(verified.get("occurred_at"))
        try:
            canonical = json.dumps(
                verified, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, default=str,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise AuthUnauthorized("unauthorized") from exc
        digest = hashlib.sha256(canonical).hexdigest()
        normalized = dict(verified)
        return normalized, event_id, event_type, subject, occurred_at, digest

    def ingest(self, signed_event) -> AuthAuditReceipt:
        _, provider_event_id, event_type, subject, occurred_at, digest = (
            self._validate(signed_event))
        audit_event_id = uuid5(
            NAMESPACE_URL,
            f"https://{self.project_ref}.supabase.co/auth/v1/events/{provider_event_id}",
        )
        connection = self._connect()
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT floor_tile.record_auth_lifecycle_event(%s,%s,%s,%s,%s,%s,%s)",
                (
                    str(audit_event_id), subject, event_type, self.project_ref,
                    provider_event_id, digest, occurred_at,
                ),
            )
            result = cursor.fetchone()
            if result is None:
                raise RuntimeError("auth audit routine returned no company")
            company_id = str(_row_value(result, "record_auth_lifecycle_event", 0))
            connection.commit()
            return AuthAuditReceipt(
                event_id=str(audit_event_id), event_type=event_type,
                auth_user_id=subject, company_id=company_id,
                occurred_at=occurred_at,
            )
        except Exception as exc:
            connection.rollback()
            message = str(exc)
            if "auth_audit_replay_conflict" in message:
                raise AuthAuditConflict("authoritative event replay conflict") from exc
            if "auth_audit_subject_unbound" in message:
                raise AuthForbidden("forbidden") from exc
            if "auth_audit_invalid" in message:
                raise AuthUnauthorized("unauthorized") from exc
            raise
        finally:
            if cursor is not None:
                cursor.close()
            connection.close()
