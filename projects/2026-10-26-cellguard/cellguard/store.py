"""Durable shuffle-sharded work queue with fair claims and fenced leases."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from .errors import (
    CapacityError,
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    LeaseError,
    NotFoundError,
    UnavailableError,
    ValidationError,
)
from .model import (
    Assignment,
    DIGEST_RE,
    JobClaim,
    SubmitResult,
    canonical_json,
    digest_json,
    validate_id,
    validate_integer,
    validate_timestamp,
)
from .topology import ShuffleTopology


CELL_STATES = {"HEALTHY", "DRAINING", "UNHEALTHY"}
JOB_STATES = {"QUEUED", "LEASED", "SUCCEEDED", "DEAD"}


def _secret_bytes(secret: bytes | str) -> bytes:
    value = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValidationError("lease secret must contain at least 32 bytes")
    return value


class CellGuardStore:
    def __init__(
        self,
        path: str | Path,
        topology: ShuffleTopology,
        *,
        secret: bytes | str,
        default_queue_limit: int = 100,
        default_inflight_limit: int = 4,
        default_weight: int = 1,
        default_max_attempts: int = 4,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.topology = topology
        self._secret = _secret_bytes(secret)
        self._secret_fingerprint = hashlib.sha256(self._secret).hexdigest()
        self.default_queue_limit = validate_integer(
            default_queue_limit, "default_queue_limit", 1, 100_000
        )
        self.default_inflight_limit = validate_integer(
            default_inflight_limit, "default_inflight_limit", 1, 10_000
        )
        self.default_weight = validate_integer(default_weight, "default_weight", 1, 100)
        self.default_max_attempts = validate_integer(
            default_max_attempts, "default_max_attempts", 1, 100
        )
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
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
                CREATE TABLE IF NOT EXISTS cells (
                    cell_id TEXT PRIMARY KEY,
                    zone TEXT NOT NULL,
                    capacity INTEGER NOT NULL CHECK(capacity > 0),
                    state TEXT NOT NULL CHECK(state IN ('HEALTHY', 'DRAINING', 'UNHEALTHY')),
                    version INTEGER NOT NULL CHECK(version > 0),
                    updated_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tenant_policies (
                    tenant TEXT PRIMARY KEY,
                    queue_limit INTEGER NOT NULL CHECK(queue_limit > 0),
                    inflight_limit INTEGER NOT NULL CHECK(inflight_limit > 0),
                    weight INTEGER NOT NULL CHECK(weight > 0),
                    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
                    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                    version INTEGER NOT NULL CHECK(version > 0),
                    updated_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    tenant TEXT PRIMARY KEY,
                    topology_epoch TEXT NOT NULL,
                    cells_json TEXT NOT NULL,
                    assignment_digest TEXT NOT NULL,
                    created_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    tenant TEXT NOT NULL REFERENCES tenant_policies(tenant),
                    idempotency_key TEXT NOT NULL,
                    cell_id TEXT NOT NULL REFERENCES cells(cell_id),
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 9),
                    state TEXT NOT NULL CHECK(state IN ('QUEUED', 'LEASED', 'SUCCEEDED', 'DEAD')),
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK(fencing_token >= 0),
                    available_ns INTEGER NOT NULL,
                    lease_token_hash TEXT,
                    lease_until_ns INTEGER,
                    worker TEXT,
                    result_json TEXT,
                    result_digest TEXT,
                    last_error TEXT,
                    created_ns INTEGER NOT NULL,
                    updated_ns INTEGER NOT NULL,
                    UNIQUE (tenant, idempotency_key),
                    CHECK ((state = 'LEASED' AND lease_token_hash IS NOT NULL AND
                            lease_until_ns IS NOT NULL AND worker IS NOT NULL) OR
                           state != 'LEASED')
                );
                CREATE INDEX IF NOT EXISTS jobs_claim
                    ON jobs(cell_id, state, available_ns, priority DESC, created_ns);
                CREATE INDEX IF NOT EXISTS jobs_tenant_state
                    ON jobs(tenant, state);
                CREATE TABLE IF NOT EXISTS scheduler_cursor (
                    cell_id TEXT PRIMARY KEY REFERENCES cells(cell_id),
                    last_tenant TEXT
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
            now_ns = time.time_ns()
            connection.execute("BEGIN IMMEDIATE")
            expected_metadata = {
                "schema_version": "1",
                "secret_fingerprint": self._secret_fingerprint,
                "topology_epoch": self.topology.epoch,
            }
            for name, expected in expected_metadata.items():
                row = connection.execute(
                    "SELECT value FROM metadata WHERE name = ?", (name,)
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO metadata(name, value) VALUES(?, ?)", (name, expected)
                    )
                elif row["value"] != expected:
                    raise IntegrityError(f"database metadata mismatch for {name}")
            for cell in self.topology.cells:
                row = connection.execute(
                    "SELECT * FROM cells WHERE cell_id = ?", (cell.cell_id,)
                ).fetchone()
                if row is None:
                    connection.execute(
                        """INSERT INTO cells(cell_id, zone, capacity, state, version, updated_ns)
                           VALUES(?, ?, ?, 'HEALTHY', 1, ?)""",
                        (cell.cell_id, cell.zone, cell.capacity, now_ns),
                    )
                    connection.execute(
                        "INSERT INTO scheduler_cursor(cell_id, last_tenant) VALUES(?, NULL)",
                        (cell.cell_id,),
                    )
                elif row["zone"] != cell.zone or row["capacity"] != cell.capacity:
                    raise IntegrityError(f"cell metadata mismatch for {cell.cell_id}")
            database_cells = {
                row["cell_id"] for row in connection.execute("SELECT cell_id FROM cells")
            }
            configured_cells = {cell.cell_id for cell in self.topology.cells}
            if database_cells != configured_cells:
                raise IntegrityError("database cell set does not match topology")
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

    def _ensure_policy(
        self, connection: sqlite3.Connection, tenant: str, now_ns: int
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM tenant_policies WHERE tenant = ?", (tenant,)
        ).fetchone()
        if row is None:
            connection.execute(
                """INSERT INTO tenant_policies(
                       tenant, queue_limit, inflight_limit, weight, max_attempts,
                       enabled, version, updated_ns
                   ) VALUES(?, ?, ?, ?, ?, 1, 1, ?)""",
                (
                    tenant,
                    self.default_queue_limit,
                    self.default_inflight_limit,
                    self.default_weight,
                    self.default_max_attempts,
                    now_ns,
                ),
            )
            row = connection.execute(
                "SELECT * FROM tenant_policies WHERE tenant = ?", (tenant,)
            ).fetchone()
        assert row is not None
        return row

    def _ensure_assignment(
        self, connection: sqlite3.Connection, tenant: str, now_ns: int
    ) -> Assignment:
        row = connection.execute(
            "SELECT * FROM assignments WHERE tenant = ?", (tenant,)
        ).fetchone()
        expected = self.topology.assign(tenant)
        if row is None:
            connection.execute(
                """INSERT INTO assignments(
                       tenant, topology_epoch, cells_json, assignment_digest, created_ns
                   ) VALUES(?, ?, ?, ?, ?)""",
                (
                    tenant,
                    expected.epoch,
                    canonical_json(list(expected.cells)),
                    expected.digest,
                    now_ns,
                ),
            )
            return expected
        cells = tuple(json.loads(row["cells_json"]))
        restored = Assignment(
            tenant=row["tenant"],
            epoch=row["topology_epoch"],
            cells=cells,
            digest=row["assignment_digest"],
        )
        if restored != expected:
            raise IntegrityError("persisted assignment disagrees with deterministic topology")
        return restored

    def assignment(self, tenant: str) -> Assignment:
        tenant = validate_id(tenant, "tenant")
        now_ns = time.time_ns()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._ensure_policy(connection, tenant, now_ns)
            assignment = self._ensure_assignment(connection, tenant, now_ns)
            connection.commit()
            return assignment
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def set_policy(
        self,
        tenant: str,
        *,
        queue_limit: int,
        inflight_limit: int,
        weight: int = 1,
        max_attempts: int = 4,
        enabled: bool = True,
        expected_version: int | None = None,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = validate_id(tenant, "tenant")
        queue_limit = validate_integer(queue_limit, "queue_limit", 1, 100_000)
        inflight_limit = validate_integer(inflight_limit, "inflight_limit", 1, 10_000)
        weight = validate_integer(weight, "weight", 1, 100)
        max_attempts = validate_integer(max_attempts, "max_attempts", 1, 100)
        if not isinstance(enabled, bool):
            raise ValidationError("enabled must be boolean")
        if expected_version is not None:
            validate_integer(expected_version, "expected_version", 0, 1_000_000_000)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT * FROM tenant_policies WHERE tenant = ?", (tenant,)
            ).fetchone()
            current_version = current["version"] if current else 0
            if expected_version is not None and expected_version != current_version:
                raise ConflictError(
                    f"expected policy version {expected_version}, current version is {current_version}"
                )
            outstanding = connection.execute(
                """SELECT COUNT(*) FROM jobs
                   WHERE tenant = ? AND state IN ('QUEUED', 'LEASED')""",
                (tenant,),
            ).fetchone()[0]
            inflight = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE tenant = ? AND state = 'LEASED'",
                (tenant,),
            ).fetchone()[0]
            if queue_limit < outstanding or inflight_limit < inflight:
                raise ConflictError("new limits are below current outstanding work")
            version = current_version + 1
            connection.execute(
                """INSERT INTO tenant_policies(
                       tenant, queue_limit, inflight_limit, weight, max_attempts,
                       enabled, version, updated_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant) DO UPDATE SET
                       queue_limit = excluded.queue_limit,
                       inflight_limit = excluded.inflight_limit,
                       weight = excluded.weight,
                       max_attempts = excluded.max_attempts,
                       enabled = excluded.enabled,
                       version = excluded.version,
                       updated_ns = excluded.updated_ns""",
                (
                    tenant,
                    queue_limit,
                    inflight_limit,
                    weight,
                    max_attempts,
                    int(enabled),
                    version,
                    occurred_ns,
                ),
            )
            self._ensure_assignment(connection, tenant, occurred_ns)
            self._audit(
                connection,
                "SET_POLICY",
                tenant,
                {
                    "enabled": enabled,
                    "inflight_limit": inflight_limit,
                    "max_attempts": max_attempts,
                    "queue_limit": queue_limit,
                    "version": version,
                    "weight": weight,
                },
                occurred_ns,
            )
            connection.commit()
            return self.policy(tenant)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def policy(self, tenant: str) -> dict[str, Any]:
        tenant = validate_id(tenant, "tenant")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM tenant_policies WHERE tenant = ?", (tenant,)
            ).fetchone()
            if row is None:
                raise NotFoundError("tenant policy not found")
            return {
                "enabled": bool(row["enabled"]),
                "inflight_limit": row["inflight_limit"],
                "max_attempts": row["max_attempts"],
                "queue_limit": row["queue_limit"],
                "tenant": row["tenant"],
                "version": row["version"],
                "weight": row["weight"],
            }
        finally:
            connection.close()

    @staticmethod
    def _route_score(tenant: str, key: str, cell_id: str) -> int:
        return int.from_bytes(
            hashlib.sha256(f"{tenant}\0{key}\0{cell_id}".encode("utf-8")).digest(),
            "big",
        )

    def submit(
        self,
        tenant: str,
        idempotency_key: str,
        payload: Any,
        *,
        priority: int = 0,
        now_ns: int | None = None,
    ) -> SubmitResult:
        tenant = validate_id(tenant, "tenant")
        idempotency_key = validate_id(idempotency_key, "idempotency_key")
        priority = validate_integer(priority, "priority", 0, 9)
        payload_json = canonical_json(payload)
        request_digest = digest_json({"payload": payload, "priority": priority})
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            policy = self._ensure_policy(connection, tenant, occurred_ns)
            assignment = self._ensure_assignment(connection, tenant, occurred_ns)
            existing = connection.execute(
                "SELECT * FROM jobs WHERE tenant = ? AND idempotency_key = ?",
                (tenant, idempotency_key),
            ).fetchone()
            if existing is not None:
                if not hmac.compare_digest(existing["payload_digest"], request_digest):
                    raise IdempotencyConflict(
                        "idempotency key was already used with a different payload or priority"
                    )
                connection.rollback()
                return SubmitResult(
                    existing["job_id"],
                    tenant,
                    existing["cell_id"],
                    existing["state"],
                    True,
                    assignment,
                )
            if not policy["enabled"]:
                raise CapacityError("tenant admission is disabled")
            outstanding = connection.execute(
                """SELECT COUNT(*) FROM jobs
                   WHERE tenant = ? AND state IN ('QUEUED', 'LEASED')""",
                (tenant,),
            ).fetchone()[0]
            if outstanding >= policy["queue_limit"]:
                self._metric(connection, "tenant_admission_rejected")
                raise CapacityError("tenant outstanding-work limit reached")

            candidates: list[tuple[float, int, str]] = []
            assigned_healthy = 0
            for cell_id in assignment.cells:
                cell = connection.execute(
                    "SELECT * FROM cells WHERE cell_id = ?", (cell_id,)
                ).fetchone()
                if cell is None:
                    raise IntegrityError("assignment references an unknown cell")
                if cell["state"] != "HEALTHY":
                    continue
                assigned_healthy += 1
                active = connection.execute(
                    """SELECT COUNT(*) FROM jobs
                       WHERE cell_id = ? AND state IN ('QUEUED', 'LEASED')""",
                    (cell_id,),
                ).fetchone()[0]
                if active >= cell["capacity"]:
                    continue
                candidates.append(
                    (
                        active / cell["capacity"],
                        -self._route_score(tenant, idempotency_key, cell_id),
                        cell_id,
                    )
                )
            if not candidates:
                if assigned_healthy:
                    self._metric(connection, "shard_capacity_rejected")
                    raise CapacityError("all healthy cells in the tenant shard are full")
                raise UnavailableError("no healthy cell exists in the tenant shard")
            cell_id = min(candidates)[2]
            job_id = f"job-{uuid.uuid4().hex}"
            connection.execute(
                """INSERT INTO jobs(
                       job_id, tenant, idempotency_key, cell_id, payload_json,
                       payload_digest, priority, state, available_ns, created_ns, updated_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, 'QUEUED', ?, ?, ?)""",
                (
                    job_id,
                    tenant,
                    idempotency_key,
                    cell_id,
                    payload_json,
                    request_digest,
                    priority,
                    occurred_ns,
                    occurred_ns,
                    occurred_ns,
                ),
            )
            self._metric(connection, "submitted")
            self._audit(
                connection,
                "SUBMIT",
                job_id,
                {"cell_id": cell_id, "priority": priority, "tenant": tenant},
                occurred_ns,
            )
            connection.commit()
            return SubmitResult(job_id, tenant, cell_id, "QUEUED", False, assignment)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _reap_expired(
        self, connection: sqlite3.Connection, now_ns: int, cell_id: str | None = None
    ) -> dict[str, int]:
        where = "state = 'LEASED' AND lease_until_ns <= ?"
        params: list[Any] = [now_ns]
        if cell_id is not None:
            where += " AND cell_id = ?"
            params.append(cell_id)
        rows = connection.execute(
            f"""SELECT jobs.*, tenant_policies.max_attempts
                FROM jobs JOIN tenant_policies USING(tenant)
                WHERE {where} ORDER BY jobs.created_ns""",
            params,
        ).fetchall()
        result = {"dead": 0, "requeued": 0}
        for row in rows:
            if row["attempt_count"] >= row["max_attempts"]:
                state = "DEAD"
                result["dead"] += 1
                self._metric(connection, "dead_lettered")
            else:
                state = "QUEUED"
                result["requeued"] += 1
                self._metric(connection, "lease_expired_requeued")
            connection.execute(
                """UPDATE jobs SET state = ?, lease_token_hash = NULL,
                       lease_until_ns = NULL, worker = NULL, last_error = 'lease_expired',
                       available_ns = ?, updated_ns = ? WHERE job_id = ?""",
                (state, now_ns, now_ns, row["job_id"]),
            )
            self._audit(
                connection,
                "LEASE_EXPIRED",
                row["job_id"],
                {"attempt": row["attempt_count"], "next_state": state},
                now_ns,
            )
        return result

    def reap_expired(self, *, now_ns: int | None = None) -> dict[str, int]:
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = self._reap_expired(connection, occurred_ns)
            connection.commit()
            return result
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _rotated_tenants(tenants: list[str], last_tenant: str | None) -> list[str]:
        if not tenants or last_tenant is None:
            return tenants
        for index, tenant in enumerate(tenants):
            if tenant > last_tenant:
                return tenants[index:] + tenants[:index]
        return tenants

    def claim(
        self,
        cell_id: str,
        worker: str,
        *,
        limit: int = 1,
        lease_ns: int = 30_000_000_000,
        now_ns: int | None = None,
    ) -> list[JobClaim]:
        cell_id = validate_id(cell_id, "cell_id")
        worker = validate_id(worker, "worker")
        limit = validate_integer(limit, "limit", 1, 100)
        lease_ns = validate_integer(lease_ns, "lease_ns", 1_000_000, 300_000_000_000)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        if occurred_ns + lease_ns > 9_223_372_036_854_775_807:
            raise ValidationError("lease deadline overflows the supported range")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cell = connection.execute(
                "SELECT * FROM cells WHERE cell_id = ?", (cell_id,)
            ).fetchone()
            if cell is None:
                raise NotFoundError("cell does not exist")
            if cell["state"] == "UNHEALTHY":
                raise UnavailableError("unhealthy cells cannot claim work")
            self._reap_expired(connection, occurred_ns, cell_id)
            cursor = connection.execute(
                "SELECT last_tenant FROM scheduler_cursor WHERE cell_id = ?", (cell_id,)
            ).fetchone()
            last_tenant = cursor["last_tenant"] if cursor else None
            claims: list[JobClaim] = []
            last_claimed: str | None = None

            while len(claims) < limit:
                tenant_rows = connection.execute(
                    """SELECT DISTINCT jobs.tenant, tenant_policies.weight,
                              tenant_policies.inflight_limit
                       FROM jobs JOIN tenant_policies USING(tenant)
                       WHERE jobs.cell_id = ? AND jobs.state = 'QUEUED'
                         AND jobs.available_ns <= ? AND tenant_policies.enabled = 1
                       ORDER BY jobs.tenant""",
                    (cell_id, occurred_ns),
                ).fetchall()
                tenant_names = self._rotated_tenants(
                    [row["tenant"] for row in tenant_rows], last_tenant
                )
                if not tenant_names:
                    break
                policy_by_tenant = {row["tenant"]: row for row in tenant_rows}
                progress = False
                for tenant in tenant_names:
                    policy = policy_by_tenant[tenant]
                    inflight = connection.execute(
                        "SELECT COUNT(*) FROM jobs WHERE tenant = ? AND state = 'LEASED'",
                        (tenant,),
                    ).fetchone()[0]
                    allowance = min(
                        policy["weight"],
                        policy["inflight_limit"] - inflight,
                        limit - len(claims),
                    )
                    for _ in range(max(0, allowance)):
                        job = connection.execute(
                            """SELECT * FROM jobs
                               WHERE cell_id = ? AND tenant = ? AND state = 'QUEUED'
                                 AND available_ns <= ?
                               ORDER BY priority DESC, created_ns, job_id LIMIT 1""",
                            (cell_id, tenant, occurred_ns),
                        ).fetchone()
                        if job is None:
                            break
                        raw_token = secrets.token_urlsafe(32)
                        token_hash = hmac.new(
                            self._secret, raw_token.encode("utf-8"), hashlib.sha256
                        ).hexdigest()
                        lease_until = occurred_ns + lease_ns
                        attempt = job["attempt_count"] + 1
                        fencing = job["fencing_token"] + 1
                        connection.execute(
                            """UPDATE jobs SET state = 'LEASED', attempt_count = ?,
                                   fencing_token = ?, lease_token_hash = ?, lease_until_ns = ?,
                                   worker = ?, updated_ns = ? WHERE job_id = ?""",
                            (
                                attempt,
                                fencing,
                                token_hash,
                                lease_until,
                                worker,
                                occurred_ns,
                                job["job_id"],
                            ),
                        )
                        claims.append(
                            JobClaim(
                                job["job_id"],
                                tenant,
                                cell_id,
                                json.loads(job["payload_json"]),
                                job["priority"],
                                attempt,
                                fencing,
                                raw_token,
                                lease_until,
                            )
                        )
                        self._metric(connection, "claimed")
                        self._audit(
                            connection,
                            "CLAIM",
                            job["job_id"],
                            {
                                "attempt": attempt,
                                "cell_id": cell_id,
                                "fencing_token": fencing,
                                "worker": worker,
                            },
                            occurred_ns,
                        )
                        progress = True
                        last_claimed = tenant
                        if len(claims) == limit:
                            break
                    if len(claims) == limit:
                        break
                if not progress:
                    break
                last_tenant = last_claimed
            if last_claimed is not None:
                connection.execute(
                    "UPDATE scheduler_cursor SET last_tenant = ? WHERE cell_id = ?",
                    (last_claimed, cell_id),
                )
            connection.commit()
            return claims
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _verify_active_lease(
        self, row: sqlite3.Row, lease_token: str, now_ns: int
    ) -> None:
        if not isinstance(lease_token, str) or len(lease_token) < 32:
            raise LeaseError("invalid lease capability")
        token_hash = hmac.new(
            self._secret, lease_token.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if row["lease_token_hash"] is None or not hmac.compare_digest(
            token_hash, row["lease_token_hash"]
        ):
            raise LeaseError("lease capability does not match")
        if row["lease_until_ns"] is None or now_ns >= row["lease_until_ns"]:
            raise LeaseError("lease has expired")

    def complete(
        self,
        job_id: str,
        lease_token: str,
        result: Any,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        job_id = validate_id(job_id, "job_id")
        if not isinstance(lease_token, str) or len(lease_token) < 32:
            raise LeaseError("invalid lease capability")
        result_json = canonical_json(result)
        result_digest = digest_json(result)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError("job does not exist")
            if row["state"] == "SUCCEEDED":
                token_hash = hmac.new(
                    self._secret, lease_token.encode("utf-8"), hashlib.sha256
                ).hexdigest()
                if row["lease_token_hash"] is None or not hmac.compare_digest(
                    token_hash, row["lease_token_hash"]
                ):
                    raise LeaseError("completion replay capability does not match")
                if not hmac.compare_digest(row["result_digest"], result_digest):
                    raise ConflictError("completion replay has a different result")
                connection.rollback()
                return {"duplicate": True, "job_id": job_id, "state": "SUCCEEDED"}
            if row["state"] != "LEASED":
                raise LeaseError(f"job is {row['state'].lower()}, not leased")
            self._verify_active_lease(row, lease_token, occurred_ns)
            connection.execute(
                """UPDATE jobs SET state = 'SUCCEEDED', result_json = ?,
                       result_digest = ?, updated_ns = ? WHERE job_id = ?""",
                (result_json, result_digest, occurred_ns, job_id),
            )
            self._metric(connection, "succeeded")
            self._audit(
                connection,
                "COMPLETE",
                job_id,
                {"attempt": row["attempt_count"], "result_digest": result_digest},
                occurred_ns,
            )
            connection.commit()
            return {"duplicate": False, "job_id": job_id, "state": "SUCCEEDED"}
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def fail(
        self,
        job_id: str,
        lease_token: str,
        error: str,
        *,
        retry_delay_ns: int = 0,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        job_id = validate_id(job_id, "job_id")
        if not isinstance(error, str) or not error or len(error) > 500:
            raise ValidationError("error must contain 1 to 500 characters")
        retry_delay_ns = validate_integer(
            retry_delay_ns, "retry_delay_ns", 0, 3_600_000_000_000
        )
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        if occurred_ns + retry_delay_ns > 9_223_372_036_854_775_807:
            raise ValidationError("retry deadline overflows the supported range")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT jobs.*, tenant_policies.max_attempts
                   FROM jobs JOIN tenant_policies USING(tenant)
                   WHERE job_id = ?""",
                (job_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("job does not exist")
            if row["state"] != "LEASED":
                raise LeaseError(f"job is {row['state'].lower()}, not leased")
            self._verify_active_lease(row, lease_token, occurred_ns)
            if row["attempt_count"] >= row["max_attempts"]:
                next_state = "DEAD"
                self._metric(connection, "dead_lettered")
            else:
                next_state = "QUEUED"
                self._metric(connection, "retry_scheduled")
            connection.execute(
                """UPDATE jobs SET state = ?, available_ns = ?, lease_token_hash = NULL,
                       lease_until_ns = NULL, worker = NULL, last_error = ?, updated_ns = ?
                   WHERE job_id = ?""",
                (
                    next_state,
                    occurred_ns + retry_delay_ns,
                    error,
                    occurred_ns,
                    job_id,
                ),
            )
            self._audit(
                connection,
                "FAIL",
                job_id,
                {"attempt": row["attempt_count"], "next_state": next_state},
                occurred_ns,
            )
            connection.commit()
            return {"job_id": job_id, "state": next_state}
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def set_cell_state(
        self,
        cell_id: str,
        state: str,
        *,
        expected_version: int,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        cell_id = validate_id(cell_id, "cell_id")
        if state not in CELL_STATES:
            raise ValidationError(f"state must be one of {sorted(CELL_STATES)}")
        expected_version = validate_integer(
            expected_version, "expected_version", 1, 1_000_000_000
        )
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM cells WHERE cell_id = ?", (cell_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("cell does not exist")
            if row["version"] != expected_version:
                raise ConflictError(
                    f"expected cell version {expected_version}, current version is {row['version']}"
                )
            version = row["version"] + 1
            connection.execute(
                "UPDATE cells SET state = ?, version = ?, updated_ns = ? WHERE cell_id = ?",
                (state, version, occurred_ns, cell_id),
            )
            self._audit(
                connection,
                "SET_CELL_STATE",
                cell_id,
                {"from": row["state"], "to": state, "version": version},
                occurred_ns,
            )
            connection.commit()
            return self.cell(cell_id)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def cell(self, cell_id: str) -> dict[str, Any]:
        cell_id = validate_id(cell_id, "cell_id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM cells WHERE cell_id = ?", (cell_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("cell does not exist")
            return {
                "capacity": row["capacity"],
                "cell_id": row["cell_id"],
                "state": row["state"],
                "version": row["version"],
                "zone": row["zone"],
            }
        finally:
            connection.close()

    def job(self, tenant: str, job_id: str) -> dict[str, Any]:
        tenant = validate_id(tenant, "tenant")
        job_id = validate_id(job_id, "job_id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM jobs WHERE tenant = ? AND job_id = ?", (tenant, job_id)
            ).fetchone()
            if row is None:
                raise NotFoundError("job does not exist")
            return {
                "attempt_count": row["attempt_count"],
                "cell_id": row["cell_id"],
                "created_ns": row["created_ns"],
                "fencing_token": row["fencing_token"],
                "idempotency_key": row["idempotency_key"],
                "job_id": row["job_id"],
                "last_error": row["last_error"],
                "priority": row["priority"],
                "result": None if row["result_json"] is None else json.loads(row["result_json"]),
                "state": row["state"],
                "tenant": row["tenant"],
            }
        finally:
            connection.close()

    def stats(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            states = {
                row["state"].lower(): row["count"]
                for row in connection.execute(
                    "SELECT state, COUNT(*) AS count FROM jobs GROUP BY state"
                )
            }
            metrics = {
                row["name"]: row["value"]
                for row in connection.execute("SELECT name, value FROM metrics ORDER BY name")
            }
            cells = {
                row["cell_id"]: row["count"]
                for row in connection.execute(
                    """SELECT cell_id, COUNT(*) AS count FROM jobs
                       WHERE state IN ('QUEUED', 'LEASED') GROUP BY cell_id"""
                )
            }
            return {"cells": cells, "jobs": states, "metrics": metrics}
        finally:
            connection.close()

    def verify_integrity(self) -> dict[str, int]:
        connection = self._connect()
        try:
            metadata = {
                row["name"]: row["value"]
                for row in connection.execute("SELECT name, value FROM metadata")
            }
            if metadata.get("topology_epoch") != self.topology.epoch:
                raise IntegrityError("topology epoch mismatch")
            if metadata.get("secret_fingerprint") != self._secret_fingerprint:
                raise IntegrityError("secret fingerprint mismatch")
            assignments = connection.execute(
                "SELECT * FROM assignments ORDER BY tenant"
            ).fetchall()
            for row in assignments:
                expected = self.topology.assign(row["tenant"])
                cells = tuple(json.loads(row["cells_json"]))
                restored = Assignment(
                    row["tenant"], row["topology_epoch"], cells, row["assignment_digest"]
                )
                if restored != expected:
                    raise IntegrityError("assignment integrity mismatch")

            jobs = connection.execute("SELECT * FROM jobs ORDER BY created_ns").fetchall()
            assignment_map = {
                row["tenant"]: set(json.loads(row["cells_json"])) for row in assignments
            }
            for row in jobs:
                payload = json.loads(row["payload_json"])
                expected_payload = digest_json(
                    {"payload": payload, "priority": row["priority"]}
                )
                if not hmac.compare_digest(expected_payload, row["payload_digest"]):
                    raise IntegrityError("job payload digest mismatch")
                if row["cell_id"] not in assignment_map.get(row["tenant"], set()):
                    raise IntegrityError("job escaped its tenant shuffle shard")
                if row["state"] not in JOB_STATES:
                    raise IntegrityError("job has invalid state")
                if row["state"] == "LEASED" and (
                    row["lease_token_hash"] is None or row["lease_until_ns"] is None
                ):
                    raise IntegrityError("leased job lacks a capability or deadline")
                if row["state"] == "SUCCEEDED":
                    if row["result_json"] is None or row["result_digest"] is None:
                        raise IntegrityError("successful job lacks a result")
                    if not hmac.compare_digest(
                        digest_json(json.loads(row["result_json"])), row["result_digest"]
                    ):
                        raise IntegrityError("job result digest mismatch")

            previous = "0" * 64
            audits = connection.execute("SELECT * FROM audit_log ORDER BY sequence").fetchall()
            for row in audits:
                if row["previous_hash"] != previous:
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
                expected = hashlib.sha256(material.encode("utf-8")).hexdigest()
                if not hmac.compare_digest(expected, row["entry_hash"]):
                    raise IntegrityError("audit entry hash mismatch")
                previous = row["entry_hash"]
            return {
                "assignments": len(assignments),
                "audit_entries": len(audits),
                "cells": len(self.topology.cells),
                "jobs": len(jobs),
            }
        finally:
            connection.close()
