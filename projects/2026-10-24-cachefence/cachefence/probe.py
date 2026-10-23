"""Client-observable cache consistency probes with delayed alerting."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from .cache import CacheStore
from .model import ValidationError, validate_id, validate_ns
from .origin import OriginStore


@dataclass(frozen=True)
class ProbeResult:
    status: str
    classification: str | None
    checks: int
    age_ns: int

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "status": self.status,
            "classification": self.classification,
            "checks": self.checks,
            "age_ns": self.age_ns,
        }


class ConsistencyProbe:
    """Persists mismatch samples so a transient delay is not immediately paged."""

    def __init__(self, path: str | Path, origin: OriginStore, cache: CacheStore) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.origin = origin
        self.cache = cache
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS samples (
                       tenant TEXT NOT NULL,
                       cache_key TEXT NOT NULL,
                       expected_version INTEGER NOT NULL,
                       expected_digest TEXT NOT NULL,
                       first_seen_ns INTEGER NOT NULL,
                       last_seen_ns INTEGER NOT NULL,
                       checks INTEGER NOT NULL,
                       status TEXT NOT NULL CHECK(status IN ('RETRY', 'ALERT', 'RESOLVED')),
                       classification TEXT NOT NULL,
                       PRIMARY KEY(tenant, cache_key)
                   )"""
            )
            connection.commit()
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _classify(expected: dict[str, object], observed: dict[str, object]) -> str:
        observed_version = int(observed["version"])
        expected_version = int(expected["version"])
        if expected_version == 0:
            return "orphan_cache_entry"
        if observed_version < expected_version:
            return "invalidation_lag"
        if observed_version > expected_version:
            return "future_cache_version"
        return "same_version_payload_mismatch"

    def observe(
        self,
        tenant: str,
        key: str,
        *,
        report_after_ns: int = 5_000_000_000,
        now_ns: int | None = None,
    ) -> ProbeResult:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        if (
            isinstance(report_after_ns, bool)
            or not isinstance(report_after_ns, int)
            or report_after_ns < 0
        ):
            raise ValidationError("report_after_ns must be a non-negative integer")
        now = time.time_ns() if now_ns is None else validate_ns(now_ns, "now_ns")
        origin_snapshot = self.origin.peek_snapshot(tenant, key)
        observed = self.cache.inspect(tenant, key)["entry"]
        expected = None if origin_snapshot is None else {
            "version": origin_snapshot.version,
            "digest": origin_snapshot.digest,
            "deleted": origin_snapshot.deleted,
        }
        # A cache miss is client-safe in cache-aside: the next read consults the origin.
        matches = observed is None or (
            expected is not None
            and observed["version"] == expected["version"]
            and observed["digest"] == expected["digest"]
            and observed["deleted"] == expected["deleted"]
        )
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM samples WHERE tenant = ? AND cache_key = ?", (tenant, key)
            ).fetchone()
            if matches:
                if row is not None and row["status"] != "RESOLVED":
                    connection.execute(
                        "UPDATE samples SET status = 'RESOLVED', last_seen_ns = ? WHERE tenant = ? AND cache_key = ?",
                        (now, tenant, key),
                    )
                    connection.commit()
                    return ProbeResult("resolved", row["classification"], row["checks"], now - row["first_seen_ns"])
                return ProbeResult("consistent", None, 0, 0)

            assert observed is not None
            expected_record = expected or {
                "version": 0,
                "digest": "0" * 64,
                "deleted": True,
            }
            classification = self._classify(expected_record, observed)
            if (
                row is None
                or row["status"] == "RESOLVED"
                or row["expected_version"] != expected_record["version"]
                or row["expected_digest"] != expected_record["digest"]
                or row["classification"] != classification
            ):
                connection.execute(
                    """INSERT INTO samples(
                           tenant, cache_key, expected_version, expected_digest,
                           first_seen_ns, last_seen_ns, checks, status, classification
                       ) VALUES(?, ?, ?, ?, ?, ?, 1, 'RETRY', ?)
                       ON CONFLICT(tenant, cache_key) DO UPDATE SET
                           expected_version=excluded.expected_version,
                           expected_digest=excluded.expected_digest,
                           first_seen_ns=excluded.first_seen_ns,
                           last_seen_ns=excluded.last_seen_ns,
                           checks=1, status='RETRY', classification=excluded.classification""",
                    (
                        tenant,
                        key,
                        expected_record["version"],
                        expected_record["digest"],
                        now,
                        now,
                        classification,
                    ),
                )
                connection.commit()
                return ProbeResult("retry", classification, 1, 0)
            checks = row["checks"] + 1
            age = now - row["first_seen_ns"]
            status = "ALERT" if age >= report_after_ns else "RETRY"
            connection.execute(
                """UPDATE samples SET last_seen_ns = ?, checks = ?, status = ?
                    WHERE tenant = ? AND cache_key = ?""",
                (now, checks, status, tenant, key),
            )
            connection.commit()
            return ProbeResult(status.lower(), classification, checks, age)
        finally:
            connection.close()
