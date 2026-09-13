"""T1 persistence ports.

The unit of work owns a disposable engine.  `commit` is the only operation
that may publish it; leaving the context or raising before commit discards it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


@dataclass(frozen=True)
class RequestPrincipal:
    auth_user_id: str
    actor: str
    company_id: str
    role: str

    @property
    def effective_actor(self) -> str:
        return self.actor


class Clock(Protocol):
    def today(self) -> date: ...


class EventStore(Protocol):
    def load(self, company_id: str) -> tuple[Any, ...]: ...
    def append(self, company_id: str, expected_head: int,
               events: tuple[Any, ...]) -> None: ...


class UnitOfWork(Protocol):
    engine: Any
    principal: RequestPrincipal

    def execute(self, command: str, params: dict) -> dict: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def __enter__(self) -> "UnitOfWork": ...
    def __exit__(self, exc_type, exc, tb) -> bool: ...
