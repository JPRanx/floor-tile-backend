"""Postgres event store and transaction-scoped sailing unit of work.

The adapter uses only the schema installed by migrations 001--007.  Domain
truth is the ordered ``domain_events`` stream; projections and command
receipts are committed in the same transaction as newly appended events.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Callable, Optional

from ..domain import Event, _ser, freeze, thaw
from .. import sailing_domain as sd
from ..production_catalog import product_from_inventory_reference
from .ports import RequestPrincipal
from .projections import rebuild_projection_tables

_EVENT_DATA = "__floor_tile_event_payload__"
_EVENT_NOTE = "__floor_tile_event_note__"
_SEED_NAMESPACE = uuid.UUID("8a8b8adc-0a52-4e30-bf67-5607fd1d57bc")


class IdempotencyConflict(RuntimeError):
    pass


class SeedMismatch(RuntimeError):
    pass


class StaleHead(RuntimeError):
    def __init__(self, *, expected_head: int, current_head: int):
        self.expected_head = expected_head
        self.current_head = current_head
        super().__init__(f"stale head: expected {expected_head}, current {current_head}")


@dataclass(frozen=True)
class DurableReceipt:
    command: str
    parameter_hash: str
    status: int
    body: dict
    head_before: int
    head_after: int


def _canonical(value):
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def canonical_parameter_sha256(params) -> str:
    encoded = json.dumps(_canonical(params), ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def encode_event(event: Event) -> dict:
    occurred = datetime.combine(date.fromisoformat(event.at), time.min,
                                tzinfo=timezone.utc)
    return {
        "seq": event.seq, "event_type": event.type, "actor": event.actor,
        "payload": {_EVENT_DATA: _ser(thaw(event.payload)),
                    _EVENT_NOTE: event.note},
        "occurred_at": occurred,
    }


def decode_event_row(row) -> Event:
    payload = row["payload"]
    if _EVENT_DATA in payload:
        note = payload.get(_EVENT_NOTE)
        payload = payload[_EVENT_DATA]
    else:  # forward-compatible with rows written before the note envelope
        note = None
    occurred = row["occurred_at"]
    at = occurred.date().isoformat() if isinstance(occurred, datetime) else str(occurred)[:10]
    return Event(seq=int(row["seq"]), type=row["event_type"], at=at,
                 actor=row["actor"], payload=freeze(payload), note=note)


def fold_through(events, head_seq: Optional[int] = None):
    selected = list(events if head_seq is None else (e for e in events if e.seq <= head_seq))
    return sd.fold(selected)


def _safe_dsn(dsn: str, allow_production: bool) -> None:
    lower = dsn.lower()
    if not allow_production and not any(marker in lower for marker in
                                        ("test", "localhost", "127.0.0.1", "::1")):
        raise ValueError("a protected test Postgres DSN is required; production is refused")


def _jsonb(value):
    try:
        from psycopg.types.json import Jsonb
    except ImportError:
        return value
    return Jsonb(_canonical(value))


class PostgresUnitOfWork:
    def __init__(self, adapter: "PostgresAdapter", principal: RequestPrincipal):
        self.adapter = adapter
        self.principal = principal
        self.connection = adapter._connect()
        self.cursor = self.connection.cursor()
        self._terminal = False
        self._committed = False
        self._command = None
        self._result = None
        self._staged_catalog_products = {}
        self._staged_catalog_mappings = {}
        try:
            self.cursor.execute(
                "SELECT set_config('floor_tile.auth_user_id', %s, true)",
                (principal.auth_user_id,))
            self.cursor.execute(
                "SELECT set_config('floor_tile.company_id', %s, true)",
                (principal.company_id,))
            self.cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (principal.company_id,))
            self.principal = adapter._authoritative_principal(self.cursor, principal)
            adapter._assert_seed(self.cursor, self.principal.company_id)
            events = adapter._load_events(self.cursor, self.principal.company_id)
            self._base_head = len(events)
            products = adapter._load_catalog(self.cursor, self.principal.company_id)
            self._working_engine = adapter._engine_from_events(events, products=products)
            self._working_engine._durable_catalog_staging = True
        except Exception:
            self.connection.rollback()
            self.connection.close()
            raise

    @property
    def engine(self):
        return self._working_engine

    @property
    def base_head(self):
        return self._base_head

    def execute(self, command: str, params: dict) -> dict:
        if self._terminal:
            raise RuntimeError("unit of work is terminal")
        if self._command is not None:
            raise RuntimeError("Postgres unit of work accepts one business command")
        staged_product_id = None
        if (command == "ResolveImplication" and params.get("action") == "create"):
            implication = self._working_engine.state.implications.get(
                params.get("implication_id"))
            if (implication is None or implication.state != "open"
                    or implication.family != "product_match"
                    or (implication.scope or {}).get("feed") not in {
                        "warehouse", "siesa_availability"}
                    or "create" not in implication.typed_actions):
                raise ValueError("create requires an open inventory product_match case")
            product = product_from_inventory_reference(
                str(implication.scope["raw_ref"]))
            product_id = product["product_id"]
            existing = self._working_engine.products.get(product_id)
            if existing is not None and existing != product:
                raise ValueError("derived product identity collides with catalog payload")
            if existing is None:
                self._working_engine.products[product_id] = copy.deepcopy(product)
                self._staged_catalog_products[product_id] = copy.deepcopy(product)
                staged_product_id = product_id
        token = self._working_engine.identity.token_for(self.principal.effective_actor)
        try:
            result = self._working_engine.execute(command, params, token=token)
            if staged_product_id is not None:
                self._staged_catalog_mappings[str(implication.scope["raw_ref"])] = {
                    "source_family": str(implication.scope["feed"]),
                    "product_id": staged_product_id,
                }
        except Exception:
            if staged_product_id is not None:
                self._working_engine.products.pop(staged_product_id, None)
                self._staged_catalog_products.pop(staged_product_id, None)
                self._staged_catalog_mappings.pop(
                    str(implication.scope["raw_ref"]), None)
            raise
        self._command, self._result = command, result
        return result

    def _persist_staged_catalog_products(self, head_after):
        if not self._staged_catalog_products:
            return
        self.cursor.execute(
            "SELECT user_id FROM floor_tile.app_users "
            "WHERE auth_user_id=%s AND company_id=%s AND active",
            (self.principal.auth_user_id, self.principal.company_id))
        row = self.cursor.fetchone()
        loaded_by = ((row.get("user_id") if isinstance(row, dict) else row[0])
                     if row else None)
        for product_id, product in sorted(self._staged_catalog_products.items()):
            source_digest = canonical_parameter_sha256({
                "authority": "inventory-reference-create-v1",
                "company_id": self.principal.company_id,
                "product": product,
            })
            load_id = uuid.uuid5(
                _SEED_NAMESPACE,
                f"{self.principal.company_id}:{source_digest}:catalog-create")
            self.cursor.execute(
                "INSERT INTO floor_tile.catalog_loads "
                "(company_id,catalog_load_id,source_digest,loaded_by) "
                "VALUES (%s,%s,%s,%s)",
                (self.principal.company_id, load_id, source_digest, loaded_by))
            self.cursor.execute(
                "INSERT INTO floor_tile.catalog_product_versions "
                "(company_id,catalog_load_id,product_id,version,sku,product_payload) "
                "VALUES (%s,%s,%s,1,%s,%s)",
                (self.principal.company_id, load_id, product_id,
                 product.get("sku", product_id), _jsonb(product)))
            self.cursor.execute(
                "INSERT INTO floor_tile.current_product_catalog "
                "(company_id,product_id,version,active,rebuilt_through_seq) "
                "VALUES (%s,%s,1,%s,%s)",
                (self.principal.company_id, product_id, True, head_after))
        for raw_ref, mapping in sorted(self._staged_catalog_mappings.items()):
            mapping_id = uuid.uuid5(
                _SEED_NAMESPACE,
                f"{self.principal.company_id}:{mapping['source_family']}:"
                f"{raw_ref}:{mapping['product_id']}:mapping")
            effective_from = datetime.combine(
                self._working_engine.today, time.min, tzinfo=timezone.utc)
            self.cursor.execute(
                "INSERT INTO floor_tile.mapping_records "
                "(company_id,mapping_id,source_family,source_product_ref,"
                "product_id,mapping_status,effective_from,collision_witness) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (self.principal.company_id, mapping_id,
                 mapping["source_family"], raw_ref, mapping["product_id"],
                 "mapped", effective_from,
                 _jsonb({"authority": "product-match-create-v1"})))

    def commit(self, *, command_id=None, idempotency_key=None,
               parameter_hash=None, status=200, body=None) -> DurableReceipt:
        if self._terminal and self._committed:
            raise RuntimeError("unit of work is terminal")
        if self._terminal:
            raise RuntimeError("unit of work is terminal")
        command = self._command or "RunChecks"
        command_id = uuid.UUID(str(command_id)) if command_id else uuid.uuid4()
        idempotency_key = idempotency_key or str(command_id)
        parameter_hash = parameter_hash or canonical_parameter_sha256({})
        head_after = len(self._working_engine.state.events)
        body = body if body is not None else {"ok": True, "result": self._result or {}}
        receipt = DurableReceipt(command, parameter_hash, status, body,
                                 self._base_head, head_after)
        try:
            self.adapter._insert_receipt(
                self.cursor, self.principal, command_id, idempotency_key, receipt)
            self.adapter._inject("after_receipt")
            self._persist_staged_catalog_products(head_after)
            self.adapter._append_events(
                self.cursor, self.principal.company_id, command_id,
                self._working_engine.state.events[self._base_head:])
            self.adapter._inject("after_events")
            if status < 400:
                rebuild_projection_tables(
                    self.cursor, self.principal.company_id,
                    self._working_engine.state,
                    after_delete=lambda: self.adapter._inject(
                        "after_projection_delete"))
            self.adapter._inject("after_projections")
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            self._terminal = True
            self.connection.close()
            raise
        self._committed = True
        self._terminal = True
        self.connection.close()
        return receipt

    def rollback(self):
        if self._terminal:
            return
        self.connection.rollback()
        self.connection.close()
        self._terminal = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None or not self._committed:
            self.rollback()
        return False


class PostgresAdapter:
    """One company stream per transaction, locked across all app instances."""
    def __init__(self, dsn: str, engine_factory: Callable, *, seed_mode="synthetic",
                 seed_version=1, allow_production=False, connection_factory=None,
                 failure_injector=None, request_version="v1", build_version="local"):
        _safe_dsn(dsn, allow_production)
        self.dsn = dsn
        self.engine_factory = engine_factory
        self.seed_mode = seed_mode
        self.seed_version = int(seed_version)
        self.connection_factory = connection_factory
        self.failure_injector = failure_injector
        self.request_version = request_version
        self.build_version = build_version
        template = engine_factory()
        self.seed_digest = self._seed_digest(template)

    def _connect(self):
        if self.connection_factory:
            return self.connection_factory()
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("psycopg is required for PostgresAdapter") from exc
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def _inject(self, stage):
        if self.failure_injector:
            self.failure_injector(stage)

    def _seed_digest(self, engine):
        payload = {
            "mode": self.seed_mode, "version": self.seed_version,
            "products": list(engine.products.values()),
            "config": vars(engine.config), "events": [encode_event(e) for e in engine.state.events],
        }
        return canonical_parameter_sha256(payload)

    def seed(self, principal: RequestPrincipal) -> None:
        """Install one explicit seed, or fail closed if another seed exists."""
        connection = self._connect()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT set_config('floor_tile.auth_user_id', %s, true)",
                (principal.auth_user_id,))
            cursor.execute(
                "SELECT set_config('floor_tile.company_id', %s, true)",
                (principal.company_id,))
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                           (principal.company_id,))
            principal = self._authoritative_principal(cursor, principal)
            cursor.execute(
                "SELECT config FROM floor_tile.config_versions WHERE company_id=%s "
                "ORDER BY version DESC LIMIT 1", (principal.company_id,))
            row = cursor.fetchone()
            if row:
                self._check_seed_metadata(row["config"])
                connection.commit()
                return
            template = self.engine_factory()
            load_id = uuid.uuid5(_SEED_NAMESPACE,
                                 f"{principal.company_id}:{self.seed_digest}:catalog")
            cursor.execute(
                "INSERT INTO floor_tile.catalog_loads "
                "(company_id,catalog_load_id,source_digest,loaded_by) VALUES (%s,%s,%s,NULL)",
                (principal.company_id, load_id, self.seed_digest))
            for product in template.products.values():
                pid = product["product_id"]
                cursor.execute(
                    "INSERT INTO floor_tile.catalog_product_versions "
                    "(company_id,catalog_load_id,product_id,version,sku,product_payload) "
                    "VALUES (%s,%s,%s,1,%s,%s)",
                    (principal.company_id, load_id, pid, product.get("sku", pid), _jsonb(product)))
                cursor.execute(
                    "INSERT INTO floor_tile.current_product_catalog "
                    "(company_id,product_id,version,rebuilt_through_seq) VALUES (%s,%s,1,%s)",
                    (principal.company_id, pid, len(template.state.events)))
            config_id = uuid.uuid5(_SEED_NAMESPACE,
                                   f"{principal.company_id}:{self.seed_digest}:config")
            metadata = {"seed_mode": self.seed_mode, "seed_version": self.seed_version,
                        "seed_digest": self.seed_digest, "planning_config": vars(template.config)}
            cursor.execute(
                "INSERT INTO floor_tile.config_versions "
                "(company_id,config_version_id,version,effective_from,config) "
                "VALUES (%s,%s,1,clock_timestamp(),%s)",
                (principal.company_id, config_id, _jsonb(metadata)))
            if template.state.events:
                command_id = uuid.uuid5(_SEED_NAMESPACE,
                                        f"{principal.company_id}:{self.seed_digest}:events")
                receipt = DurableReceipt("RunChecks", canonical_parameter_sha256(metadata), 200,
                                         {"ok": True, "seed": metadata}, 0,
                                         len(template.state.events))
                self._insert_receipt(cursor, principal, command_id,
                                     f"seed:{self.seed_mode}:{self.seed_version}", receipt)
                self._append_events(cursor, principal.company_id, command_id,
                                    template.state.events)
            rebuild_projection_tables(cursor, principal.company_id, template.state)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _check_seed_metadata(self, config):
        expected = (self.seed_mode, self.seed_version, self.seed_digest)
        actual = (config.get("seed_mode"), config.get("seed_version"),
                  config.get("seed_digest"))
        if actual != expected:
            raise SeedMismatch(f"seed metadata mismatch: expected {expected}, found {actual}")

    def _authoritative_principal(self, cursor, requested):
        cursor.execute(
            "SELECT auth_user_id,company_id,actor,role "
            "FROM floor_tile.app_users "
            "WHERE auth_user_id=%s AND company_id=%s AND active",
            (requested.auth_user_id, requested.company_id))
        row = cursor.fetchone()
        if (not row or row["actor"] != requested.effective_actor
                or row["role"] != requested.role):
            raise PermissionError(
                "request principal does not match active database binding")
        return RequestPrincipal(
            auth_user_id=str(row["auth_user_id"]),
            actor=row["actor"],
            company_id=str(row["company_id"]),
            role=row["role"],
        )

    def _assert_seed(self, cursor, company_id):
        cursor.execute(
            "SELECT config FROM floor_tile.config_versions WHERE company_id=%s "
            "ORDER BY version DESC LIMIT 1", (company_id,))
        row = cursor.fetchone()
        if not row:
            raise SeedMismatch("company has not been explicitly seeded")
        self._check_seed_metadata(row["config"])

    def _load_events(self, cursor, company_id, through=None):
        sql = ("SELECT seq,event_type,actor,payload,occurred_at "
               "FROM floor_tile.domain_events WHERE company_id=%s")
        args = [company_id]
        if through is not None:
            sql += " AND seq<=%s"
            args.append(through)
        sql += " ORDER BY seq"
        cursor.execute(sql, tuple(args))
        return [decode_event_row(row) for row in cursor.fetchall()]

    def _load_catalog(self, cursor, company_id):
        cursor.execute(
            "SELECT c.product_id,v.product_payload "
            "FROM floor_tile.current_product_catalog c "
            "JOIN floor_tile.catalog_product_versions v "
            "ON c.company_id=v.company_id AND c.product_id=v.product_id "
            "AND c.version=v.version "
            "WHERE c.company_id=%s ORDER BY c.product_id",
            (company_id,))
        products = []
        for row in cursor.fetchall():
            product_id, payload = ((row.get("product_id"), row.get("product_payload"))
                                   if isinstance(row, dict) else (row[0], row[1]))
            product = copy.deepcopy(payload)
            if not isinstance(product, dict):
                raise ValueError("catalog product payload must be an object")
            product["product_id"] = str(product_id)
            products.append(product)
        return sorted(products, key=lambda product: product["product_id"])

    def _engine_from_events(self, events, *, products=None):
        engine = copy.deepcopy(self.engine_factory())
        if products is not None:
            engine.products = {
                product["product_id"]: copy.deepcopy(product)
                for product in sorted(products, key=lambda item: item["product_id"])
            }
        engine.bus.state = sd.fold(events)
        return engine

    def bootstrap_catalog(self, principal: RequestPrincipal, *, products,
                          warehouse_sha256: str, siesa_sha256: str,
                          source_digest: str):
        """Atomically install one explicit two-inventory initial catalog."""
        expected = sorted((copy.deepcopy(product) for product in products),
                          key=lambda product: product["product_id"])
        ids = [str(product["product_id"]) for product in expected]
        if not expected or len(ids) != len(set(ids)):
            raise ValueError("bootstrap catalog must contain unique products")
        for value in (warehouse_sha256, siesa_sha256, source_digest):
            if (not isinstance(value, str) or len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)):
                raise ValueError("bootstrap source digests must be lowercase sha256")

        connection = self._connect()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT set_config('floor_tile.auth_user_id', %s, true)",
                (principal.auth_user_id,))
            cursor.execute(
                "SELECT set_config('floor_tile.company_id', %s, true)",
                (principal.company_id,))
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                           (principal.company_id,))
            principal = self._authoritative_principal(cursor, principal)
            if principal.role != "administrator":
                raise PermissionError("catalog bootstrap requires administrator role")
            self._assert_seed(cursor, principal.company_id)

            current = self._load_catalog(cursor, principal.company_id)
            if current:
                cursor.execute(
                    "SELECT 1 FROM floor_tile.catalog_loads "
                    "WHERE company_id=%s AND source_digest=%s",
                    (principal.company_id, source_digest))
                same_load = cursor.fetchone() is not None
                if (same_load and canonical_parameter_sha256(current)
                        == canonical_parameter_sha256(expected)):
                    connection.commit()
                    return {"applied": True, "replayed": True,
                            "product_count": len(expected),
                            "source_digest": source_digest}
                raise ValueError("company catalog is already initialized")

            cursor.execute(
                "SELECT user_id FROM floor_tile.app_users "
                "WHERE auth_user_id=%s AND company_id=%s AND active",
                (principal.auth_user_id, principal.company_id))
            row = cursor.fetchone()
            loaded_by = ((row.get("user_id") if isinstance(row, dict) else row[0])
                         if row else None)
            load_id = uuid.uuid5(
                _SEED_NAMESPACE,
                f"{principal.company_id}:{source_digest}:two-inventory-bootstrap")
            cursor.execute(
                "INSERT INTO floor_tile.catalog_loads "
                "(company_id,catalog_load_id,source_digest,loaded_by) "
                "VALUES (%s,%s,%s,%s)",
                (principal.company_id, load_id, source_digest, loaded_by))
            head = len(self._load_events(cursor, principal.company_id))
            for product in expected:
                product_id = str(product["product_id"])
                cursor.execute(
                    "INSERT INTO floor_tile.catalog_product_versions "
                    "(company_id,catalog_load_id,product_id,version,sku,product_payload) "
                    "VALUES (%s,%s,%s,1,%s,%s)",
                    (principal.company_id, load_id, product_id,
                     product.get("sku", product_id), _jsonb(product)))
                cursor.execute(
                    "INSERT INTO floor_tile.current_product_catalog "
                    "(company_id,product_id,version,active,rebuilt_through_seq) "
                    "VALUES (%s,%s,1,%s,%s)",
                    (principal.company_id, product_id, True, head))
            self._inject("after_catalog_bootstrap")
            connection.commit()
            return {"applied": True, "replayed": False,
                    "product_count": len(expected),
                    "source_digest": source_digest}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def load_engine(self, principal: RequestPrincipal, *, through=None):
        connection = self._connect()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT set_config('floor_tile.auth_user_id', %s, true)",
                (principal.auth_user_id,))
            cursor.execute(
                "SELECT set_config('floor_tile.company_id', %s, true)",
                (principal.company_id,))
            self._assert_seed(cursor, principal.company_id)
            events = self._load_events(cursor, principal.company_id, through)
            products = self._load_catalog(cursor, principal.company_id)
            engine = self._engine_from_events(events, products=products)
            engine._durable_catalog_staging = True
            return engine
        finally:
            connection.rollback()
            connection.close()

    def unit_of_work(self, principal):
        return PostgresUnitOfWork(self, principal)

    def lookup_receipt(self, cursor, principal, idempotency_key,
                       command, parameter_hash):
        cursor.execute(
            "SELECT command_name,canonical_parameter_sha256,principal_auth_user_id,"
            "effective_actor,role FROM floor_tile.command_receipts "
            "WHERE company_id=%s AND idempotency_key=%s",
            (principal.company_id, idempotency_key))
        row = cursor.fetchone()
        if not row:
            return None
        stored_request = (
            row["command_name"], row["canonical_parameter_sha256"],
            str(row["principal_auth_user_id"])
            if row["principal_auth_user_id"] is not None else None,
            row["effective_actor"], row["role"],
        )
        requested_request = (
            command, parameter_hash, principal.auth_user_id,
            principal.effective_actor, principal.role,
        )
        if stored_request != requested_request:
            raise IdempotencyConflict(
                "key already finalized for a different request")
        cursor.execute(
            "SELECT status_code,response_body,head_before,head_after "
            "FROM floor_tile.command_receipts "
            "WHERE company_id=%s AND idempotency_key=%s",
            (principal.company_id, idempotency_key))
        receipt_row = cursor.fetchone()
        return DurableReceipt(row["command_name"], row["canonical_parameter_sha256"],
                              receipt_row["status_code"], receipt_row["response_body"],
                              receipt_row["head_before"], receipt_row["head_after"])

    def execute_idempotent(self, principal, *, command, params, expected_head,
                           idempotency_key, parameter_hash=None,
                           response_factory=None, stale_factory=None,
                           prepare=None, error_factory=None):
        parameter_hash = parameter_hash or canonical_parameter_sha256(params)
        with self.unit_of_work(principal) as uow:
            existing = self.lookup_receipt(
                uow.cursor, uow.principal, idempotency_key,
                command, parameter_hash)
            if existing:
                uow.rollback()
                return existing, True
            if prepare is not None:
                params = prepare(uow.engine, params)
            if expected_head != uow.base_head:
                status, body = (stale_factory(uow.base_head) if stale_factory else
                                (409, {"detail": {"code": "stale_head"}}))
                uow._command = command
                receipt = uow.commit(idempotency_key=idempotency_key,
                                     parameter_hash=parameter_hash,
                                     status=status, body=body)
                return receipt, False
            try:
                result = uow.execute(command, params)
            except Exception as exc:
                if error_factory is None:
                    raise
                status, body = error_factory(exc)
                uow._command = command
                receipt = uow.commit(idempotency_key=idempotency_key,
                                     parameter_hash=parameter_hash,
                                     status=status, body=body)
                return receipt, False
            head = len(uow.engine.state.events)
            status, body = (response_factory(result, uow.engine, head)
                            if response_factory else (200, {"ok": True, "result": result,
                                                            "head_seq": head}))
            receipt = uow.commit(idempotency_key=idempotency_key,
                                 parameter_hash=parameter_hash,
                                 status=status, body=body)
            return receipt, False

    def _insert_receipt(self, cursor, principal, command_id, idempotency_key, receipt):
        cursor.execute(
            "INSERT INTO floor_tile.command_receipts "
            "(company_id,command_id,idempotency_key,command_name,canonical_parameter_sha256,"
            "principal_auth_user_id,effective_actor,infrastructure_initiator,role,status_code,"
            "response_body,head_before,head_after,request_version,build_version,schema_version) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,2)",
            (principal.company_id, command_id, idempotency_key, receipt.command,
             receipt.parameter_hash, principal.auth_user_id, principal.effective_actor,
             principal.role, receipt.status, _jsonb(receipt.body), receipt.head_before,
             receipt.head_after, self.request_version, self.build_version))

    def _append_events(self, cursor, company_id, command_id, events):
        for event in events:
            stored = encode_event(event)
            event_id = uuid.uuid5(command_id, str(event.seq))
            cursor.execute(
                "INSERT INTO floor_tile.domain_events "
                "(company_id,seq,event_id,command_id,event_type,actor,payload,occurred_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (company_id, stored["seq"], event_id, command_id,
                 stored["event_type"], stored["actor"], _jsonb(stored["payload"]),
                 stored["occurred_at"]))

    def rebuild_projections(self, principal):
        with self.unit_of_work(principal) as uow:
            rebuilt = rebuild_projection_tables(
                uow.cursor, uow.principal.company_id, uow.engine.state,
                after_delete=lambda: self._inject("after_projection_delete"))
            uow.connection.commit()
            uow._committed = uow._terminal = True
            uow.connection.close()
            return rebuilt
