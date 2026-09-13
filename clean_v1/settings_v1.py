"""Validated runtime configuration for the Clean V1 API."""
from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit


class SettingsError(RuntimeError):
    """Raised when a runtime configuration is unsafe or incomplete."""


@dataclass(frozen=True)
class SettingsV1:
    runtime: str
    database_url: str | None = None
    supabase_project_ref: str | None = None
    cors_allowed_origins: tuple[str, ...] = ()
    build_version: str = "local"
    trusted_proxy_networks: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()
    max_request_bytes: int = 16 * 1024 * 1024

    @property
    def hosted(self) -> bool:
        return self.runtime == "hosted"

    def trusts_proxy(self, host: str | None) -> bool:
        if not host:
            return False
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            return False
        return any(address in network for network in self.trusted_proxy_networks)


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise SettingsError(f"{name} is required in hosted mode")
    return value


def _origins(raw: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    if not values:
        raise SettingsError("CORS_ALLOWED_ORIGINS must contain an exact HTTPS origin")
    for value in values:
        parsed = urlsplit(value)
        canonical = f"{parsed.scheme}://{parsed.netloc}"
        if (value == "*" or parsed.scheme != "https" or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path or parsed.query or parsed.fragment or canonical != value):
            raise SettingsError("CORS_ALLOWED_ORIGINS must contain exact HTTPS origins only")
    if len(set(values)) != len(values):
        raise SettingsError("CORS_ALLOWED_ORIGINS must not contain duplicates")
    return values


def _proxy_networks(raw: str):
    try:
        networks = tuple(ipaddress.ip_network(item.strip(), strict=False)
                         for item in raw.split(",") if item.strip())
    except ValueError as exc:
        raise SettingsError("TRUSTED_PROXY_IPS must contain valid IP addresses or CIDRs") from exc
    if not networks:
        raise SettingsError("TRUSTED_PROXY_IPS is required in hosted mode")
    return networks


def load_settings(env: Mapping[str, str] | None = None) -> SettingsV1:
    source = os.environ if env is None else env
    runtime = source.get("FLOOR_TILE_RUNTIME", "local").strip().lower()
    if runtime not in {"local", "hosted"}:
        raise SettingsError("FLOOR_TILE_RUNTIME must be exactly 'local' or 'hosted'")
    if runtime == "local":
        return SettingsV1(runtime="local")

    database_url = _required(source, "DATABASE_URL")
    if not re.match(r"^postgres(?:ql)?://", database_url, re.IGNORECASE):
        raise SettingsError("DATABASE_URL must be a PostgreSQL URL")
    project_ref = _required(source, "SUPABASE_PROJECT_REF")
    if re.fullmatch(r"[a-z0-9]{20}", project_ref) is None:
        raise SettingsError("SUPABASE_PROJECT_REF is invalid")
    origins = _origins(_required(source, "CORS_ALLOWED_ORIGINS"))
    build_version = source.get("FLOOR_TILE_BUILD_VERSION", "").strip()
    if not build_version:
        build_version = source.get("RENDER_GIT_COMMIT", "").strip()
    if not build_version:
        raise SettingsError(
            "FLOOR_TILE_BUILD_VERSION or RENDER_GIT_COMMIT is required in hosted mode")
    if build_version.lower() in {"local", "dev", "development", "unversioned"}:
        raise SettingsError("FLOOR_TILE_BUILD_VERSION must identify an immutable build")
    proxies = _proxy_networks(_required(source, "TRUSTED_PROXY_IPS"))
    return SettingsV1(
        runtime=runtime,
        database_url=database_url,
        supabase_project_ref=project_ref,
        cors_allowed_origins=origins,
        build_version=build_version,
        trusted_proxy_networks=proxies,
    )
