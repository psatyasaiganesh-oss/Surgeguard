"""Durable source of truth with a transactional invalidation outbox."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .model import (
    AuthorizationError,
    ConflictError,
    IntegrityError,
    InvalidationEvent,
    LeaseExpiredError,
    MAX_VERSION,
    Snapshot,
    ValidationError,
    canonical_json,
    snapshot_for,
    validate_id,
    validate_json_value,
    validate_ns,
)


@dataclass(frozen=True)
class ClaimedEvent:
    event: InvalidationEvent
    token: str
    attempt: int
    lease_until_ns: int


class OriginStore:
    """SQLite origin whose data write and outbox append commit atomically."""

    def __init__(self, path: str | Path, *, origin_id: str = "origin-a") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.origin_id = validate_id(origin_id, "origin_id")
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS records (
                    tenant TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version > 0),
                    digest TEXT NOT NULL,
                    deleted INTEGER NOT NULL CHECK(deleted IN (0, 1)),
                    value_json TEXT,
                    updated_ns INTEGER NOT NULL,
                    PRIMARY KEY (tenant, cache_key),
                    CHECK ((deleted = 1 AND value_json IS NULL) OR
                           (deleted = 0 AND value_json IS NOT NULL))
                );
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN
                        ('PENDING', 'DELIVERING', 'DELIVERED', 'DEAD')),
                    lease_token_hash TEXT,
                    lease_until_ns INTEGER,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_ns INTEGER NOT NULL,
                    delivered_ns INTEGER
                );
                CREATE INDEX IF NOT EXISTS outbox_ready
                    ON outbox(state, lease_until_ns, created_ns);
                CREATE TABLE IF NOT EXISTS metrics (
                    name TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0 CHECK(value >= 0)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    occurred_ns INTEGER NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE
                );
                """
            )
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT value FROM metadata WHERE name = 'origin_id'"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO metadata(name, value) VALUES('origin_id', ?)",
                    (self.origin_id,),
                )
            elif row["value"] != self.origin_id:
                raise IntegrityError(
                    f"database belongs to origin {row['value']!r}, not {self.origin_id!r}"
                )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _metric(connection: sqlite3.Connection, name: str, delta: int = 1) -> None:
        connection.execute(
            """INSERT INTO metrics(name, value) VALUES(?, ?)
               ON CONFLICT(name) DO UPDATE SET value = value + excluded.value""",
            (name, delta),
        )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        action: str,
        subject: str,
        detail: dict[str, Any],
        occurred_ns: int,
    ) -> None:
        previous = connection.execute(
            "SELECT entry_hash FROM audit_log ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous_hash = previous["entry_hash"] if previous else "0" * 64
        detail_json = canonical_json(detail)
        material = canonical_json(
            {
                "action": action,
                "detail": detail,
                "occurred_ns": occurred_ns,
                "previous_hash": previous_hash,
                "subject": subject,
            }
        )
        entry_hash = hashlib.sha256(material.encode("utf-8")).hexdigest()
        connection.execute(
            """INSERT INTO audit_log(
                   action, subject, detail_json, occurred_ns, previous_hash, entry_hash
               ) VALUES(?, ?, ?, ?, ?, ?)""",
            (action, subject, detail_json, occurred_ns, previous_hash, entry_hash),
        )

    @staticmethod
    def _row_to_snapshot(row: sqlite3.Row) -> Snapshot:
        value = None if row["deleted"] else json.loads(row["value_json"])
        snapshot = Snapshot(
            tenant=row["tenant"],
            key=row["cache_key"],
            version=row["version"],
            digest=row["digest"],
            deleted=bool(row["deleted"]),
            value=value,
            updated_ns=row["updated_ns"],
        )
        snapshot.validate()
        return snapshot

    def write(
        self,
        tenant: str,
        key: str,
        value: Any = None,
        *,
        delete: bool = False,
        expected_version: int | None = None,
        now_ns: int | None = None,
    ) -> Snapshot:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        if not isinstance(delete, bool):
            raise ValidationError("delete must be boolean")
        if delete and value is not None:
            raise ValidationError("a delete cannot include a value")
        if not delete:
            validate_json_value(value)
        if expected_version is not None:
            if isinstance(expected_version, bool) or not isinstance(expected_version, int):
                raise ValidationError("expected_version must be an integer")
            if expected_version < 0 or expected_version > MAX_VERSION:
                raise ValidationError("expected_version is outside the supported range")
        occurred_ns = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM records WHERE tenant = ? AND cache_key = ?",
                (tenant, key),
            ).fetchone()
            current_version = row["version"] if row else 0
            if expected_version is not None and expected_version != current_version:
                raise ConflictError(
                    f"expected version {expected_version}, current version is {current_version}"
                )
            if current_version >= MAX_VERSION:
                raise IntegrityError("version counter is exhausted")
            snapshot = snapshot_for(
                tenant=tenant,
                key=key,
                version=current_version + 1,
                deleted=delete,
                value=value,
                updated_ns=occurred_ns,
            )
            value_json = None if delete else canonical_json(value)
            connection.execute(
                """INSERT INTO records(
                       tenant, cache_key, version, digest, deleted, value_json, updated_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant, cache_key) DO UPDATE SET
                       version = excluded.version,
                       digest = excluded.digest,
                       deleted = excluded.deleted,
                       value_json = excluded.value_json,
                       updated_ns = excluded.updated_ns""",
                (
                    tenant,
                    key,
                    snapshot.version,
                    snapshot.digest,
                    int(snapshot.deleted),
                    value_json,
                    occurred_ns,
                ),
            )
            event = InvalidationEvent(
                origin=self.origin_id,
                tenant=tenant,
                key=key,
                version=snapshot.version,
                digest=snapshot.digest,
                deleted=snapshot.deleted,
                created_ns=occurred_ns,
            )
            payload = event.to_json()
            connection.execute(
                """INSERT INTO outbox(
                       event_id, payload_json, payload_hash, state, created_ns
                   ) VALUES(?, ?, ?, 'PENDING', ?)""",
                (
                    event.event_id,
                    payload,
                    hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                    occurred_ns,
                ),
            )
            self._metric(connection, "writes")
            self._audit(
                connection,
                "origin.write",
                f"{tenant}/{key}",
                {"deleted": delete, "event_id": event.event_id, "version": snapshot.version},
                occurred_ns,
            )
            connection.commit()
            return snapshot
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def read_snapshot(self, tenant: str, key: str, *, count: bool = True) -> Snapshot | None:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        connection = self._connect()
        try:
            if count:
                connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM records WHERE tenant = ? AND cache_key = ?",
                (tenant, key),
            ).fetchone()
            if count:
                self._metric(connection, "reads")
                connection.commit()
            return None if row is None else self._row_to_snapshot(row)
        finally:
            connection.close()

    def peek_snapshot(self, tenant: str, key: str) -> Snapshot | None:
        return self.read_snapshot(tenant, key, count=False)

    def claim_events(
        self,
        worker: str,
        *,
        lease_ns: int,
        limit: int = 100,
        max_attempts: int = 8,
        now_ns: int | None = None,
    ) -> list[ClaimedEvent]:
        worker = validate_id(worker, "worker")
        if isinstance(lease_ns, bool) or not isinstance(lease_ns, int) or lease_ns <= 0:
            raise ValidationError("lease_ns must be a positive integer")
        if not 1 <= limit <= 1000:
            raise ValidationError("limit must be between 1 and 1000")
        if not 1 <= max_attempts <= 100:
            raise ValidationError("max_attempts must be between 1 and 100")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        if now > MAX_VERSION - lease_ns:
            raise ValidationError("lease expiry exceeds the supported range")
        lease_until = now + lease_ns
        connection = self._connect()
        claims: list[ClaimedEvent] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            exhausted = connection.execute(
                """UPDATE outbox
                      SET state = 'DEAD', lease_token_hash = NULL,
                          lease_until_ns = NULL, last_error = 'retry_budget_exhausted'
                    WHERE state = 'DELIVERING'
                      AND lease_until_ns <= ?
                      AND attempt_count >= ?""",
                (now, max_attempts),
            ).rowcount
            if exhausted:
                self._metric(connection, "delivery_failures", exhausted)
                self._audit(
                    connection,
                    "outbox.exhausted",
                    worker,
                    {"count": exhausted, "max_attempts": max_attempts},
                    now,
                )
            rows = connection.execute(
                """SELECT event_id, payload_json, attempt_count
                     FROM outbox
                    WHERE attempt_count < ?
                      AND (state = 'PENDING' OR
                           (state = 'DELIVERING' AND lease_until_ns <= ?))
                    ORDER BY created_ns, event_id
                    LIMIT ?""",
                (max_attempts, now, limit),
            ).fetchall()
            for row in rows:
                token = secrets.token_urlsafe(32)
                token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
                attempt = row["attempt_count"] + 1
                connection.execute(
                    """UPDATE outbox SET state = 'DELIVERING', lease_token_hash = ?,
                              lease_until_ns = ?, attempt_count = ?, last_error = NULL
                        WHERE event_id = ?""",
                    (token_hash, lease_until, attempt, row["event_id"]),
                )
                claims.append(
                    ClaimedEvent(
                        event=InvalidationEvent.from_json(row["payload_json"]),
                        token=token,
                        attempt=attempt,
                        lease_until_ns=lease_until,
                    )
                )
            if claims:
                self._metric(connection, "claims", len(claims))
                self._audit(
                    connection,
                    "outbox.claim",
                    worker,
                    {"count": len(claims), "lease_until_ns": lease_until},
                    now,
                )
            connection.commit()
            return claims
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _authorize(row: sqlite3.Row, token: str, now_ns: int) -> None:
        if not isinstance(token, str) or len(token) < 32:
            raise AuthorizationError("invalid lease capability")
        supplied = hashlib.sha256(token.encode("utf-8")).hexdigest()
        expected = row["lease_token_hash"] or ""
        if not hmac.compare_digest(supplied, expected):
            raise AuthorizationError("invalid lease capability")
        if row["state"] != "DELIVERING":
            raise AuthorizationError("event is not held by an active delivery lease")
        if row["lease_until_ns"] <= now_ns:
            raise LeaseExpiredError("delivery lease expired before acknowledgement")

    def ack_event(self, event_id: str, token: str, *, now_ns: int | None = None) -> None:
        event_id = self._validate_event_id(event_id)
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM outbox WHERE event_id = ?", (event_id,)).fetchone()
            if row is None:
                raise ValidationError("unknown event_id")
            self._authorize(row, token, now)
            connection.execute(
                """UPDATE outbox SET state = 'DELIVERED', lease_token_hash = NULL,
                          lease_until_ns = NULL, delivered_ns = ?, last_error = NULL
                    WHERE event_id = ?""",
                (now, event_id),
            )
            self._metric(connection, "acknowledgements")
            self._audit(connection, "outbox.ack", event_id, {"attempt": row["attempt_count"]}, now)
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def fail_event(
        self,
        event_id: str,
        token: str,
        error: str,
        *,
        max_attempts: int = 8,
        now_ns: int | None = None,
    ) -> str:
        event_id = self._validate_event_id(event_id)
        if not isinstance(error, str) or not error or len(error) > 300:
            raise ValidationError("error must be a non-empty string of at most 300 characters")
        if not 1 <= max_attempts <= 100:
            raise ValidationError("max_attempts must be between 1 and 100")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM outbox WHERE event_id = ?", (event_id,)).fetchone()
            if row is None:
                raise ValidationError("unknown event_id")
            self._authorize(row, token, now)
            state = "DEAD" if row["attempt_count"] >= max_attempts else "PENDING"
            connection.execute(
                """UPDATE outbox SET state = ?, lease_token_hash = NULL,
                          lease_until_ns = NULL, last_error = ? WHERE event_id = ?""",
                (state, error, event_id),
            )
            self._metric(connection, "delivery_failures")
            self._audit(
                connection,
                "outbox.fail",
                event_id,
                {"attempt": row["attempt_count"], "state": state},
                now,
            )
            connection.commit()
            return state
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _validate_event_id(event_id: str) -> str:
        if not isinstance(event_id, str) or len(event_id) != 64:
            raise ValidationError("event_id must be a SHA-256 digest")
        try:
            int(event_id, 16)
        except ValueError as exc:
            raise ValidationError("event_id must be a SHA-256 digest") from exc
        if event_id.lower() != event_id:
            raise ValidationError("event_id must use lowercase hexadecimal")
        return event_id

    def outbox_stats(self) -> dict[str, int]:
        connection = self._connect()
        try:
            counts = {name: 0 for name in ("PENDING", "DELIVERING", "DELIVERED", "DEAD")}
            for row in connection.execute("SELECT state, COUNT(*) AS count FROM outbox GROUP BY state"):
                counts[row["state"]] = row["count"]
            return {name.lower(): value for name, value in counts.items()}
        finally:
            connection.close()

    def metrics(self) -> dict[str, int]:
        connection = self._connect()
        try:
            return {row["name"]: row["value"] for row in connection.execute("SELECT * FROM metrics")}
        finally:
            connection.close()

    def verify(self) -> dict[str, int | bool]:
        connection = self._connect()
        try:
            record_count = 0
            for row in connection.execute("SELECT * FROM records ORDER BY tenant, cache_key"):
                self._row_to_snapshot(row)
                record_count += 1
            event_count = 0
            for row in connection.execute("SELECT * FROM outbox ORDER BY created_ns, event_id"):
                event = InvalidationEvent.from_json(row["payload_json"])
                actual_hash = hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest()
                if event.event_id != row["event_id"] or actual_hash != row["payload_hash"]:
                    raise IntegrityError(f"outbox payload integrity failure: {row['event_id']}")
                event_count += 1
            previous_hash = "0" * 64
            audit_count = 0
            for row in connection.execute("SELECT * FROM audit_log ORDER BY sequence"):
                if row["previous_hash"] != previous_hash:
                    raise IntegrityError("audit chain previous hash mismatch")
                detail = json.loads(row["detail_json"])
                material = canonical_json(
                    {
                        "action": row["action"],
                        "detail": detail,
                        "occurred_ns": row["occurred_ns"],
                        "previous_hash": row["previous_hash"],
                        "subject": row["subject"],
                    }
                )
                actual = hashlib.sha256(material.encode("utf-8")).hexdigest()
                if actual != row["entry_hash"]:
                    raise IntegrityError("audit chain entry hash mismatch")
                previous_hash = actual
                audit_count += 1
            return {
                "ok": True,
                "records": record_count,
                "outbox_events": event_count,
                "audit_entries": audit_count,
            }
        finally:
            connection.close()
