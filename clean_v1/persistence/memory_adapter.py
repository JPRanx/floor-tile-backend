"""Atomic in-memory adapter used by the local/synthetic T1 lane."""
from __future__ import annotations

import copy
from threading import RLock

from .ports import RequestPrincipal


# Synthetic local-adapter backing is module-owned, never instance-owned.  Ordinary
# callers holding adapter/UOW references cannot reach these mutable engines.
_PUBLICATIONS = {}
_WORKING_ENGINES = {}


class MemoryUnitOfWork:
    def __init__(self, adapter: "MemoryAdapter", principal: RequestPrincipal):
        self._adapter = adapter
        self.principal = principal
        working_engine, self._base_revision = adapter._checkout()
        _WORKING_ENGINES[self] = working_engine
        self._committed = False
        self._terminal = False

    @property
    def engine(self):
        return copy.deepcopy(_WORKING_ENGINES[self])

    def execute(self, command: str, params: dict) -> dict:
        if self._terminal:
            raise RuntimeError("unit of work is terminal")
        working_engine = _WORKING_ENGINES[self]
        token = working_engine.identity.token_for(
            self.principal.effective_actor)
        return working_engine.execute(command, params, token=token)

    def commit(self) -> None:
        if self._terminal and self._committed:
            return
        if self._terminal:
            raise RuntimeError("unit of work is terminal")
        with self._adapter._lock:
            if self._adapter._revision != self._base_revision:
                raise RuntimeError("stale in-memory unit of work")
            # The sole publication point.  Nothing is visible before here.
            _PUBLICATIONS[self._adapter] = copy.deepcopy(_WORKING_ENGINES[self])
            self._adapter._revision += 1
            self._committed = True
            self._terminal = True

    def rollback(self) -> None:
        if self._terminal:
            return
        self._committed = False
        self._terminal = True

    def __enter__(self) -> "MemoryUnitOfWork":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None or not self._committed:
            self.rollback()
        return False


class MemoryAdapter:
    def __init__(self, engine):
        _PUBLICATIONS[self] = copy.deepcopy(engine)
        self._lock = RLock()
        self._revision = 0

    @property
    def engine(self):
        with self._lock:
            return copy.deepcopy(_PUBLICATIONS[self])

    def _checkout(self):
        """Return a detached working copy plus its publication generation."""
        with self._lock:
            return copy.deepcopy(_PUBLICATIONS[self]), self._revision

    @property
    def head_seq(self) -> int:
        with self._lock:
            return len(_PUBLICATIONS[self].state.events)

    def unit_of_work(self, principal: RequestPrincipal) -> MemoryUnitOfWork:
        return MemoryUnitOfWork(self, principal)

    def execute(self, command: str, params: dict,
                principal: RequestPrincipal) -> dict:
        with self.unit_of_work(principal) as uow:
            result = uow.execute(command, params)
            uow.commit()
            return result
