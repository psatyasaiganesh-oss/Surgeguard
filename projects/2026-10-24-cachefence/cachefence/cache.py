"""Version-fenced cache that rejects stale fills and quarantines contradictions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .model import (
    IntegrityError,
    InvalidationEvent,
    MAX_VERSION,
    Snapshot,
    ValidationError,
    canonical_json,
    content_digest,
    validate_id,
    validate_ns,
)


@dataclass(frozen=True)
class LookupResult:
    status: str
    snapshot: Snapshot | None


@dataclass(frozen=True)
class ApplyResult:
    status: str
    evicted: bool


class CacheStore:
    """Durable invalidation metadata plus evictable cached values.

    A fence is deliberately stored separately from its value. Value or TTL eviction
    therefore cannot erase knowledge that a newer version has already existed.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        cache_id: str = "cache-a",
        origin_id: str = "origin-a",
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_id = validate_id(cache_id, "cache_id")
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
                CREATE TABLE IF NOT EXISTS fences (
                    tenant TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version > 0),
                    digest TEXT NOT NULL,
                    deleted INTEGER NOT NULL CHECK(deleted IN (0, 1)),
                    event_id TEXT,
                    updated_ns INTEGER NOT NULL,
                    PRIMARY KEY (tenant, cache_key)
                );
                CREATE TABLE IF NOT EXISTS entries (
                    tenant TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version > 0),
                    digest TEXT NOT NULL,
                    deleted INTEGER NOT NULL CHECK(deleted IN (0, 1)),
                    value_json TEXT,
                    source_updated_ns INTEGER NOT NULL,
                    expires_ns INTEGER NOT NULL,
                    inserted_ns INTEGER NOT NULL,
                    PRIMARY KEY (tenant, cache_key),
                    CHECK ((deleted = 1 AND value_json IS NULL) OR
                           (deleted = 0 AND value_json IS NOT NULL))
                );
                CREATE TABLE IF NOT EXISTS applied_events (
                    event_id TEXT PRIMARY KEY,
                    payload_hash TEXT NOT NULL,
                    applied_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS quarantine (
                    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    expected_json TEXT NOT NULL,
                    observed_json TEXT NOT NULL,
                    occurred_ns INTEGER NOT NULL
                );
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
            for name, expected in (("cache_id", self.cache_id), ("origin_id", self.origin_id)):
                row = connection.execute(
                    "SELECT value FROM metadata WHERE name = ?", (name,)
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO metadata(name, value) VALUES(?, ?)", (name, expected)
                    )
                elif row["value"] != expected:
                    raise IntegrityError(
                        f"database {name} is {row['value']!r}, not {expected!r}"
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
    def _quarantine(
        connection: sqlite3.Connection,
        *,
        tenant: str,
        key: str,
        reason: str,
        expected: dict[str, Any],
        observed: dict[str, Any],
        now_ns: int,
    ) -> None:
        connection.execute(
            """INSERT INTO quarantine(
                   tenant, cache_key, reason, expected_json, observed_json, occurred_ns
               ) VALUES(?, ?, ?, ?, ?, ?)""",
            (
                tenant,
                key,
                reason,
                canonical_json(expected),
                canonical_json(observed),
                now_ns,
            ),
        )
        CacheStore._metric(connection, "integrity_rejections")
        CacheStore._audit(
            connection,
            "cache.quarantine",
            f"{tenant}/{key}",
            {"reason": reason},
            now_ns,
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
            updated_ns=row["source_updated_ns"],
        )
        snapshot.validate()
        return snapshot

    def apply_invalidation(
        self, event: InvalidationEvent, *, now_ns: int | None = None
    ) -> ApplyResult:
        event.validate()
        if event.origin != self.origin_id:
            raise IntegrityError(
                f"event belongs to origin {event.origin!r}, expected {self.origin_id!r}"
            )
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        payload_hash = hashlib.sha256(event.to_json().encode("utf-8")).hexdigest()
        connection = self._connect()
        integrity_error: str | None = None
        result: ApplyResult | None = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            applied = connection.execute(
                "SELECT payload_hash FROM applied_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if applied is not None:
                if applied["payload_hash"] != payload_hash:
                    reason = "event_identity_payload_mismatch"
                    self._quarantine(
                        connection,
                        tenant=event.tenant,
                        key=event.key,
                        reason=reason,
                        expected={"payload_hash": applied["payload_hash"]},
                        observed={"payload_hash": payload_hash},
                        now_ns=now,
                    )
                    integrity_error = reason
                else:
                    self._metric(connection, "duplicate_events")
                    result = ApplyResult(status="duplicate", evicted=False)
                connection.commit()
            else:
                fence = connection.execute(
                    "SELECT * FROM fences WHERE tenant = ? AND cache_key = ?",
                    (event.tenant, event.key),
                ).fetchone()
                if fence is not None and event.version == fence["version"] and (
                    event.digest != fence["digest"] or int(event.deleted) != fence["deleted"]
                ):
                    reason = "same_version_content_mismatch"
                    self._quarantine(
                        connection,
                        tenant=event.tenant,
                        key=event.key,
                        reason=reason,
                        expected={
                            "version": fence["version"],
                            "digest": fence["digest"],
                            "deleted": bool(fence["deleted"]),
                        },
                        observed={
                            "version": event.version,
                            "digest": event.digest,
                            "deleted": event.deleted,
                        },
                        now_ns=now,
                    )
                    integrity_error = reason
                    connection.commit()
                else:
                    outdated = fence is not None and event.version < fence["version"]
                    if not outdated:
                        connection.execute(
                            """INSERT INTO fences(
                                   tenant, cache_key, version, digest, deleted, event_id, updated_ns
                               ) VALUES(?, ?, ?, ?, ?, ?, ?)
                               ON CONFLICT(tenant, cache_key) DO UPDATE SET
                                   version = excluded.version,
                                   digest = excluded.digest,
                                   deleted = excluded.deleted,
                                   event_id = excluded.event_id,
                                   updated_ns = excluded.updated_ns""",
                            (
                                event.tenant,
                                event.key,
                                event.version,
                                event.digest,
                                int(event.deleted),
                                event.event_id,
                                now,
                            ),
                        )
                    entry = connection.execute(
                        "SELECT version FROM entries WHERE tenant = ? AND cache_key = ?",
                        (event.tenant, event.key),
                    ).fetchone()
                    evicted = bool(entry and entry["version"] <= event.version and not outdated)
                    if evicted:
                        connection.execute(
                            "DELETE FROM entries WHERE tenant = ? AND cache_key = ?",
                            (event.tenant, event.key),
                        )
                        self._metric(connection, "invalidation_evictions")
                    connection.execute(
                        "INSERT INTO applied_events(event_id, payload_hash, applied_ns) VALUES(?, ?, ?)",
                        (event.event_id, payload_hash, now),
                    )
                    self._metric(connection, "invalidations_applied")
                    if outdated:
                        self._metric(connection, "outdated_events")
                    self._audit(
                        connection,
                        "cache.invalidate",
                        f"{event.tenant}/{event.key}",
                        {
                            "event_id": event.event_id,
                            "evicted": evicted,
                            "outdated": outdated,
                            "version": event.version,
                        },
                        now,
                    )
                    result = ApplyResult(status="outdated" if outdated else "applied", evicted=evicted)
                    connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        if integrity_error is not None:
            raise IntegrityError(integrity_error)
        assert result is not None
        return result

    def install_fill(
        self,
        snapshot: Snapshot,
        *,
        ttl_ns: int,
        now_ns: int | None = None,
    ) -> ApplyResult:
        snapshot.validate()
        if isinstance(ttl_ns, bool) or not isinstance(ttl_ns, int) or ttl_ns <= 0:
            raise ValidationError("ttl_ns must be a positive integer")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        if now > MAX_VERSION - ttl_ns:
            raise ValidationError("expiry exceeds the supported range")
        connection = self._connect()
        integrity_error: str | None = None
        result: ApplyResult | None = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            fence = connection.execute(
                "SELECT * FROM fences WHERE tenant = ? AND cache_key = ?",
                (snapshot.tenant, snapshot.key),
            ).fetchone()
            if fence is not None and snapshot.version == fence["version"] and (
                snapshot.digest != fence["digest"] or int(snapshot.deleted) != fence["deleted"]
            ):
                reason = "same_version_content_mismatch"
                self._quarantine(
                    connection,
                    tenant=snapshot.tenant,
                    key=snapshot.key,
                    reason=reason,
                    expected={
                        "version": fence["version"],
                        "digest": fence["digest"],
                        "deleted": bool(fence["deleted"]),
                    },
                    observed={
                        "version": snapshot.version,
                        "digest": snapshot.digest,
                        "deleted": snapshot.deleted,
                    },
                    now_ns=now,
                )
                integrity_error = reason
                connection.commit()
            elif fence is not None and snapshot.version < fence["version"]:
                self._metric(connection, "stale_fills_rejected")
                self._audit(
                    connection,
                    "cache.reject_fill",
                    f"{snapshot.tenant}/{snapshot.key}",
                    {"fill_version": snapshot.version, "fence_version": fence["version"]},
                    now,
                )
                connection.commit()
                result = ApplyResult(status="stale", evicted=False)
            else:
                existing = connection.execute(
                    "SELECT version, digest FROM entries WHERE tenant = ? AND cache_key = ?",
                    (snapshot.tenant, snapshot.key),
                ).fetchone()
                if existing is not None and existing["version"] > snapshot.version:
                    self._metric(connection, "stale_fills_rejected")
                    connection.commit()
                    result = ApplyResult(status="stale", evicted=False)
                else:
                    if fence is None or snapshot.version > fence["version"]:
                        connection.execute(
                            """INSERT INTO fences(
                                   tenant, cache_key, version, digest, deleted, event_id, updated_ns
                               ) VALUES(?, ?, ?, ?, ?, NULL, ?)
                               ON CONFLICT(tenant, cache_key) DO UPDATE SET
                                   version = excluded.version,
                                   digest = excluded.digest,
                                   deleted = excluded.deleted,
                                   event_id = NULL,
                                   updated_ns = excluded.updated_ns""",
                            (
                                snapshot.tenant,
                                snapshot.key,
                                snapshot.version,
                                snapshot.digest,
                                int(snapshot.deleted),
                                now,
                            ),
                        )
                    value_json = None if snapshot.deleted else canonical_json(snapshot.value)
                    connection.execute(
                        """INSERT INTO entries(
                               tenant, cache_key, version, digest, deleted, value_json,
                               source_updated_ns, expires_ns, inserted_ns
                           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(tenant, cache_key) DO UPDATE SET
                               version = excluded.version,
                               digest = excluded.digest,
                               deleted = excluded.deleted,
                               value_json = excluded.value_json,
                               source_updated_ns = excluded.source_updated_ns,
                               expires_ns = excluded.expires_ns,
                               inserted_ns = excluded.inserted_ns""",
                        (
                            snapshot.tenant,
                            snapshot.key,
                            snapshot.version,
                            snapshot.digest,
                            int(snapshot.deleted),
                            value_json,
                            snapshot.updated_ns,
                            now + ttl_ns,
                            now,
                        ),
                    )
                    self._metric(connection, "fills_installed")
                    self._audit(
                        connection,
                        "cache.fill",
                        f"{snapshot.tenant}/{snapshot.key}",
                        {"deleted": snapshot.deleted, "version": snapshot.version},
                        now,
                    )
                    connection.commit()
                    result = ApplyResult(status="installed", evicted=False)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        if integrity_error is not None:
            raise IntegrityError(integrity_error)
        assert result is not None
        return result

    def lookup(
        self, tenant: str, key: str, *, now_ns: int | None = None, count: bool = True
    ) -> LookupResult:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        connection = self._connect()
        integrity_error: str | None = None
        result: LookupResult | None = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM entries WHERE tenant = ? AND cache_key = ?", (tenant, key)
            ).fetchone()
            if row is None:
                if count:
                    self._metric(connection, "misses")
                result = LookupResult(status="miss", snapshot=None)
                connection.commit()
            elif row["expires_ns"] <= now:
                connection.execute(
                    "DELETE FROM entries WHERE tenant = ? AND cache_key = ?", (tenant, key)
                )
                if count:
                    self._metric(connection, "misses")
                    self._metric(connection, "ttl_evictions")
                result = LookupResult(status="miss", snapshot=None)
                connection.commit()
            else:
                fence = connection.execute(
                    "SELECT * FROM fences WHERE tenant = ? AND cache_key = ?", (tenant, key)
                ).fetchone()
                try:
                    snapshot = self._row_to_snapshot(row)
                except (IntegrityError, ValidationError, ValueError, json.JSONDecodeError) as exc:
                    self._quarantine(
                        connection,
                        tenant=tenant,
                        key=key,
                        reason="cached_value_corruption",
                        expected={"digest": row["digest"]},
                        observed={"error": type(exc).__name__},
                        now_ns=now,
                    )
                    connection.execute(
                        "DELETE FROM entries WHERE tenant = ? AND cache_key = ?", (tenant, key)
                    )
                    integrity_error = "cached_value_corruption"
                    connection.commit()
                else:
                    fence_mismatch = fence is None or snapshot.version != fence["version"] or (
                        snapshot.digest != fence["digest"]
                        or int(snapshot.deleted) != fence["deleted"]
                    )
                    if fence_mismatch:
                        self._quarantine(
                            connection,
                            tenant=tenant,
                            key=key,
                            reason="entry_fence_mismatch",
                            expected={} if fence is None else {
                                "version": fence["version"], "digest": fence["digest"]
                            },
                            observed={"version": snapshot.version, "digest": snapshot.digest},
                            now_ns=now,
                        )
                        connection.execute(
                            "DELETE FROM entries WHERE tenant = ? AND cache_key = ?", (tenant, key)
                        )
                        integrity_error = "entry_fence_mismatch"
                        connection.commit()
                    else:
                        if count:
                            self._metric(connection, "negative_hits" if snapshot.deleted else "hits")
                        result = LookupResult(
                            status="negative" if snapshot.deleted else "hit", snapshot=snapshot
                        )
                        connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        if integrity_error is not None:
            raise IntegrityError(integrity_error)
        assert result is not None
        return result

    def evict_value(self, tenant: str, key: str, *, now_ns: int | None = None) -> bool:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "DELETE FROM entries WHERE tenant = ? AND cache_key = ?", (tenant, key)
            )
            removed = cursor.rowcount > 0
            if removed:
                self._metric(connection, "manual_evictions")
                self._audit(connection, "cache.evict", f"{tenant}/{key}", {}, now)
            connection.commit()
            return removed
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def inspect(self, tenant: str, key: str) -> dict[str, Any]:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        connection = self._connect()
        try:
            fence = connection.execute(
                "SELECT * FROM fences WHERE tenant = ? AND cache_key = ?", (tenant, key)
            ).fetchone()
            entry = connection.execute(
                "SELECT * FROM entries WHERE tenant = ? AND cache_key = ?", (tenant, key)
            ).fetchone()
            return {
                "fence": None
                if fence is None
                else {
                    "version": fence["version"],
                    "digest": fence["digest"],
                    "deleted": bool(fence["deleted"]),
                    "event_id": fence["event_id"],
                },
                "entry": None
                if entry is None
                else {
                    "version": entry["version"],
                    "digest": entry["digest"],
                    "deleted": bool(entry["deleted"]),
                    "expires_ns": entry["expires_ns"],
                },
            }
        finally:
            connection.close()

    def unsafe_inject_entry_for_test(
        self,
        snapshot: Snapshot,
        *,
        ttl_ns: int,
        now_ns: int,
        digest_override: str | None = None,
    ) -> None:
        """Explicit fault-injection hook; never use on untrusted input or production data."""
        snapshot.validate()
        validate_ns(now_ns, "now_ns")
        if ttl_ns <= 0 or now_ns > MAX_VERSION - ttl_ns:
            raise ValidationError("invalid ttl_ns")
        digest = snapshot.digest if digest_override is None else digest_override
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """INSERT INTO entries(
                       tenant, cache_key, version, digest, deleted, value_json,
                       source_updated_ns, expires_ns, inserted_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant, cache_key) DO UPDATE SET
                       version=excluded.version, digest=excluded.digest,
                       deleted=excluded.deleted, value_json=excluded.value_json,
                       source_updated_ns=excluded.source_updated_ns,
                       expires_ns=excluded.expires_ns, inserted_ns=excluded.inserted_ns""",
                (
                    snapshot.tenant,
                    snapshot.key,
                    snapshot.version,
                    digest,
                    int(snapshot.deleted),
                    None if snapshot.deleted else canonical_json(snapshot.value),
                    snapshot.updated_ns,
                    now_ns + ttl_ns,
                    now_ns,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def quarantine_count(self) -> int:
        connection = self._connect()
        try:
            return connection.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
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
            fence_count = connection.execute("SELECT COUNT(*) FROM fences").fetchone()[0]
            entry_count = 0
            for row in connection.execute("SELECT * FROM entries ORDER BY tenant, cache_key"):
                snapshot = self._row_to_snapshot(row)
                fence = connection.execute(
                    "SELECT * FROM fences WHERE tenant = ? AND cache_key = ?",
                    (snapshot.tenant, snapshot.key),
                ).fetchone()
                if fence is None or snapshot.version != fence["version"]:
                    raise IntegrityError("entry version disagrees with its durable fence")
                if (
                    snapshot.digest != fence["digest"]
                    or int(snapshot.deleted) != fence["deleted"]
                ):
                    raise IntegrityError("entry content disagrees with its durable fence")
                entry_count += 1
            previous_hash = "0" * 64
            audit_count = 0
            for row in connection.execute("SELECT * FROM audit_log ORDER BY sequence"):
                if row["previous_hash"] != previous_hash:
                    raise IntegrityError("audit chain previous hash mismatch")
                material = canonical_json(
                    {
                        "action": row["action"],
                        "detail": json.loads(row["detail_json"]),
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
                "fences": fence_count,
                "entries": entry_count,
                "quarantined": self.quarantine_count(),
                "audit_entries": audit_count,
            }
        finally:
            connection.close()
