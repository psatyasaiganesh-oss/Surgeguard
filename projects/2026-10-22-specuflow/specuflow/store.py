"""Durable lease scheduler and exactly-once logical result commit."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any

from .blob import BlobStore, IntegrityError
from .model import (
    ValidationError,
    canonical,
    digest,
    identifier,
    parse_counters,
    parse_job,
    parse_output,
)


class ConflictError(RuntimeError):
    pass


class NotFoundError(LookupError):
    pass


class AuthorizationError(PermissionError):
    pass


class LeaseExpiredError(ConflictError):
    pass


class NondeterminismError(ConflictError):
    pass


class Scheduler:
    """SQLite-backed coordinator. Every public operation opens its own connection."""

    def __init__(self, database: str | Path, blobs: str | Path):
        self.database = str(database)
        self.blobs = BlobStore(blobs)
        self._initialize()

    @staticmethod
    def now_ms() -> int:
        return time.time_ns() // 1_000_000

    def _connect(self) -> sqlite3.Connection:
        existed = os.path.exists(self.database)
        connection = sqlite3.connect(self.database, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        if not existed and self.database != ":memory:" and os.name != "nt":
            os.chmod(self.database, 0o600)
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs(
                    job_id TEXT PRIMARY KEY,
                    spec TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    error_code TEXT,
                    manifest_digest TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks(
                    job_id TEXT NOT NULL REFERENCES jobs(job_id),
                    shard_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    winning_attempt TEXT,
                    output_digest TEXT,
                    counters TEXT,
                    PRIMARY KEY(job_id, shard_id),
                    UNIQUE(job_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS attempts(
                    attempt_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    shard_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    worker_id TEXT NOT NULL,
                    token_hash TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    started_at_ms INTEGER NOT NULL,
                    lease_deadline_ms INTEGER NOT NULL,
                    completed_at_ms INTEGER,
                    output_digest TEXT,
                    error_code TEXT,
                    UNIQUE(job_id, shard_id, ordinal),
                    FOREIGN KEY(job_id, shard_id) REFERENCES tasks(job_id, shard_id)
                );
                CREATE INDEX IF NOT EXISTS attempts_task_state
                    ON attempts(job_id, shard_id, state);
                CREATE TABLE IF NOT EXISTS audit(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at_ms INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    event TEXT NOT NULL,
                    job_id TEXT,
                    shard_id TEXT,
                    details TEXT NOT NULL
                );
                """
            )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        now_ms: int,
        actor: str,
        event: str,
        job_id: str | None,
        shard_id: str | None,
        details: dict[str, Any],
    ) -> None:
        connection.execute(
            """INSERT INTO audit(
                occurred_at_ms, actor, event, job_id, shard_id, details
            ) VALUES(?, ?, ?, ?, ?, ?)""",
            (now_ms, actor, event, job_id, shard_id, canonical(details)),
        )

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _time(value: int | None) -> int:
        if value is None:
            return Scheduler.now_ms()
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError("now_ms must be a non-negative integer")
        return value

    def submit(self, spec_value: Any, *, now_ms: int | None = None) -> dict[str, Any]:
        spec = parse_job(spec_value)
        now = self._time(now_ms)
        encoded = canonical(spec)
        spec_hash = digest(spec)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT spec, spec_hash FROM jobs WHERE job_id = ?", (spec["job_id"],)
            ).fetchone()
            if existing:
                if existing["spec"] != encoded:
                    raise ConflictError("job_id is already bound to a different specification")
                return self.status(spec["job_id"], connection=connection)
            connection.execute(
                """INSERT INTO jobs(
                    job_id, spec, spec_hash, state, created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, 'RUNNING', ?, ?)""",
                (spec["job_id"], encoded, spec_hash, now, now),
            )
            connection.executemany(
                """INSERT INTO tasks(
                    job_id, shard_id, ordinal, payload, state
                ) VALUES(?, ?, ?, ?, 'PENDING')""",
                [
                    (spec["job_id"], shard["shard_id"], index, canonical(shard["payload"]))
                    for index, shard in enumerate(spec["shards"])
                ],
            )
            self._audit(
                connection,
                now,
                "submitter",
                "JOB_SUBMITTED",
                spec["job_id"],
                None,
                {"spec_hash": spec_hash, "shards": len(spec["shards"])},
            )
            return self.status(spec["job_id"], connection=connection)

    def _job(self, connection: sqlite3.Connection, job_id: str) -> tuple[sqlite3.Row, dict[str, Any]]:
        row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("job not found")
        return row, json.loads(row["spec"])

    def _expire(self, connection: sqlite3.Connection, job_id: str, now: int) -> None:
        expired = connection.execute(
            """SELECT attempt_id, shard_id FROM attempts
               WHERE job_id = ? AND state = 'RUNNING' AND lease_deadline_ms <= ?""",
            (job_id, now),
        ).fetchall()
        for row in expired:
            connection.execute(
                """UPDATE attempts SET state = 'EXPIRED', completed_at_ms = ?,
                   error_code = 'LEASE_EXPIRED' WHERE attempt_id = ?""",
                (now, row["attempt_id"]),
            )
            self._audit(
                connection,
                now,
                "scheduler",
                "ATTEMPT_EXPIRED",
                job_id,
                row["shard_id"],
                {"attempt_id": row["attempt_id"]},
            )

    def _attempt_rows(
        self, connection: sqlite3.Connection, job_id: str, shard_id: str
    ) -> list[sqlite3.Row]:
        return connection.execute(
            """SELECT * FROM attempts WHERE job_id = ? AND shard_id = ?
               ORDER BY ordinal""",
            (job_id, shard_id),
        ).fetchall()

    def _issue(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        task: sqlite3.Row,
        worker_id: str,
        kind: str,
        ordinal: int,
        lease_ms: int,
        now: int,
    ) -> dict[str, Any]:
        attempt_id = secrets.token_hex(16)
        token = secrets.token_urlsafe(32)
        deadline = now + lease_ms
        connection.execute(
            """INSERT INTO attempts(
                attempt_id, job_id, shard_id, ordinal, worker_id, token_hash,
                kind, state, started_at_ms, lease_deadline_ms
            ) VALUES(?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?)""",
            (
                attempt_id,
                job_id,
                task["shard_id"],
                ordinal,
                worker_id,
                self._token_hash(token),
                kind,
                now,
                deadline,
            ),
        )
        connection.execute(
            "UPDATE tasks SET state = 'RUNNING' WHERE job_id = ? AND shard_id = ?",
            (job_id, task["shard_id"]),
        )
        self._audit(
            connection,
            now,
            worker_id,
            "ATTEMPT_CLAIMED",
            job_id,
            task["shard_id"],
            {"attempt_id": attempt_id, "kind": kind, "ordinal": ordinal},
        )
        return {
            "attempt_id": attempt_id,
            "capability_token": token,
            "job_id": job_id,
            "shard_id": task["shard_id"],
            "attempt_ordinal": ordinal,
            "kind": kind,
            "payload": json.loads(task["payload"]),
            "lease_deadline_ms": deadline,
        }

    def claim(
        self,
        job_id: str,
        worker_id: str,
        *,
        allow_speculation: bool = True,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        job_id = identifier(job_id, "job_id")
        worker_id = identifier(worker_id, "worker_id")
        if not isinstance(allow_speculation, bool):
            raise ValidationError("allow_speculation must be boolean")
        now = self._time(now_ms)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            job, spec = self._job(connection, job_id)
            if job["state"] != "RUNNING":
                return None
            self._expire(connection, job_id, now)
            tasks = connection.execute(
                "SELECT * FROM tasks WHERE job_id = ? AND state != 'COMMITTED' ORDER BY ordinal",
                (job_id,),
            ).fetchall()
            policy = spec["policy"]

            # Prefer untouched or retryable work before spending capacity on duplicates.
            for task in tasks:
                attempts = self._attempt_rows(connection, job_id, task["shard_id"])
                active = [row for row in attempts if row["state"] == "RUNNING"]
                if active:
                    continue
                if len(attempts) >= policy["max_attempts"]:
                    connection.execute(
                        "UPDATE tasks SET state = 'FAILED' WHERE job_id = ? AND shard_id = ?",
                        (job_id, task["shard_id"]),
                    )
                    connection.execute(
                        """UPDATE jobs SET state = 'FAILED', error_code = 'ATTEMPTS_EXHAUSTED',
                           updated_at_ms = ? WHERE job_id = ?""",
                        (now, job_id),
                    )
                    self._audit(
                        connection,
                        now,
                        "scheduler",
                        "JOB_FAILED",
                        job_id,
                        task["shard_id"],
                        {"code": "ATTEMPTS_EXHAUSTED"},
                    )
                    return None
                kind = "PRIMARY" if not attempts else "RETRY"
                return self._issue(
                    connection,
                    job_id,
                    task,
                    worker_id,
                    kind,
                    len(attempts) + 1,
                    policy["lease_ms"],
                    now,
                )

            if not allow_speculation or policy["max_speculative_attempts"] == 0:
                return None
            for task in tasks:
                attempts = self._attempt_rows(connection, job_id, task["shard_id"])
                active = [row for row in attempts if row["state"] == "RUNNING"]
                speculative = [row for row in attempts if row["kind"] == "SPECULATIVE"]
                if not active or len(attempts) >= policy["max_attempts"]:
                    continue
                if len(speculative) >= policy["max_speculative_attempts"]:
                    continue
                if any(row["worker_id"] == worker_id for row in active):
                    continue
                oldest = min(row["started_at_ms"] for row in active)
                if now - oldest < policy["speculate_after_ms"]:
                    continue
                return self._issue(
                    connection,
                    job_id,
                    task,
                    worker_id,
                    "SPECULATIVE",
                    len(attempts) + 1,
                    policy["lease_ms"],
                    now,
                )
            return None

    def _authorized_attempt(
        self, connection: sqlite3.Connection, attempt_id: str, token: str
    ) -> sqlite3.Row:
        if not isinstance(token, str) or not token:
            raise AuthorizationError("missing attempt capability token")
        row = connection.execute(
            "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("attempt not found")
        if not hmac.compare_digest(row["token_hash"], self._token_hash(token)):
            raise AuthorizationError("invalid attempt capability token")
        return row

    def heartbeat(
        self,
        attempt_id: str,
        token: str,
        *,
        now_ms: int | None = None,
    ) -> int:
        now = self._time(now_ms)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = self._authorized_attempt(connection, attempt_id, token)
            if attempt["state"] != "RUNNING" or attempt["lease_deadline_ms"] <= now:
                raise LeaseExpiredError("attempt lease is no longer active")
            _, spec = self._job(connection, attempt["job_id"])
            deadline = now + spec["policy"]["lease_ms"]
            connection.execute(
                "UPDATE attempts SET lease_deadline_ms = ? WHERE attempt_id = ?",
                (deadline, attempt_id),
            )
            return deadline

    def complete(
        self,
        attempt_id: str,
        token: str,
        output: bytes,
        counters: Any,
        *,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now = self._time(now_ms)
        validated_counters = parse_counters(counters)
        # Parse before storage so a worker cannot publish an identity-confused result.
        with self._connect() as lookup:
            attempt_lookup = self._authorized_attempt(lookup, attempt_id, token)
            expected_shard = attempt_lookup["shard_id"]
        parse_output(output, expected_shard)
        output_digest = self.blobs.put(output)
        nondeterministic: tuple[str, str] | None = None
        response: dict[str, Any]
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = self._authorized_attempt(connection, attempt_id, token)
            task = connection.execute(
                "SELECT * FROM tasks WHERE job_id = ? AND shard_id = ?",
                (attempt["job_id"], attempt["shard_id"]),
            ).fetchone()
            if task["state"] == "COMMITTED":
                if task["output_digest"] != output_digest:
                    connection.execute(
                        """UPDATE attempts SET state = 'NONDETERMINISTIC', completed_at_ms = ?,
                           output_digest = ?, error_code = 'OUTPUT_MISMATCH'
                           WHERE attempt_id = ?""",
                        (now, output_digest, attempt_id),
                    )
                    connection.execute(
                        """UPDATE jobs SET state = 'FAILED', error_code = 'NONDETERMINISTIC_OUTPUT',
                           updated_at_ms = ? WHERE job_id = ?""",
                        (now, attempt["job_id"]),
                    )
                    self._audit(
                        connection,
                        now,
                        attempt["worker_id"],
                        "NONDETERMINISTIC_OUTPUT",
                        attempt["job_id"],
                        attempt["shard_id"],
                        {"winning_digest": task["output_digest"], "losing_digest": output_digest},
                    )
                    nondeterministic = (task["output_digest"], output_digest)
                    response = {}
                else:
                    connection.execute(
                        """UPDATE attempts SET state = 'SUPERSEDED', completed_at_ms = ?,
                           output_digest = ?, error_code = NULL WHERE attempt_id = ?
                           AND state != 'COMMITTED'""",
                        (now, output_digest, attempt_id),
                    )
                    self._audit(
                        connection,
                        now,
                        attempt["worker_id"],
                        "DUPLICATE_RESULT_SUPPRESSED",
                        attempt["job_id"],
                        attempt["shard_id"],
                        {"attempt_id": attempt_id, "digest": output_digest},
                    )
                    response = {
                        "committed": False,
                        "duplicate": True,
                        "output_digest": output_digest,
                        "winning_attempt": task["winning_attempt"],
                    }
            else:
                if attempt["state"] != "RUNNING" or attempt["lease_deadline_ms"] <= now:
                    raise LeaseExpiredError("attempt cannot commit after its lease expires")
                connection.execute(
                    """UPDATE tasks SET state = 'COMMITTED', winning_attempt = ?,
                       output_digest = ?, counters = ? WHERE job_id = ? AND shard_id = ?""",
                    (
                        attempt_id,
                        output_digest,
                        canonical(validated_counters),
                        attempt["job_id"],
                        attempt["shard_id"],
                    ),
                )
                connection.execute(
                    """UPDATE attempts SET state = 'COMMITTED', completed_at_ms = ?,
                       output_digest = ?, error_code = NULL WHERE attempt_id = ?""",
                    (now, output_digest, attempt_id),
                )
                connection.execute(
                    """UPDATE attempts SET state = 'SUPERSEDED', completed_at_ms = ?,
                       error_code = 'LOST_COMMIT_RACE' WHERE job_id = ? AND shard_id = ?
                       AND state = 'RUNNING' AND attempt_id != ?""",
                    (now, attempt["job_id"], attempt["shard_id"], attempt_id),
                )
                remaining = connection.execute(
                    """SELECT COUNT(*) AS count FROM tasks
                       WHERE job_id = ? AND state != 'COMMITTED'""",
                    (attempt["job_id"],),
                ).fetchone()["count"]
                if remaining == 0:
                    outputs = [
                        {"shard_id": row["shard_id"], "digest": row["output_digest"]}
                        for row in connection.execute(
                            """SELECT shard_id, output_digest FROM tasks
                               WHERE job_id = ? ORDER BY ordinal""",
                            (attempt["job_id"],),
                        )
                    ]
                    manifest_digest = digest({"job_id": attempt["job_id"], "outputs": outputs})
                    connection.execute(
                        """UPDATE jobs SET state = 'SUCCEEDED', manifest_digest = ?,
                           updated_at_ms = ? WHERE job_id = ? AND state = 'RUNNING'""",
                        (manifest_digest, now, attempt["job_id"]),
                    )
                else:
                    connection.execute(
                        "UPDATE jobs SET updated_at_ms = ? WHERE job_id = ?",
                        (now, attempt["job_id"]),
                    )
                self._audit(
                    connection,
                    now,
                    attempt["worker_id"],
                    "RESULT_COMMITTED",
                    attempt["job_id"],
                    attempt["shard_id"],
                    {"attempt_id": attempt_id, "digest": output_digest},
                )
                response = {
                    "committed": True,
                    "duplicate": False,
                    "output_digest": output_digest,
                    "winning_attempt": attempt_id,
                }
        if nondeterministic:
            raise NondeterminismError(
                f"output mismatch: committed {nondeterministic[0]}, received {nondeterministic[1]}"
            )
        return response

    def fail_attempt(
        self,
        attempt_id: str,
        token: str,
        error_code: str,
        *,
        now_ms: int | None = None,
    ) -> None:
        error_code = identifier(error_code, "error_code")
        now = self._time(now_ms)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = self._authorized_attempt(connection, attempt_id, token)
            if attempt["state"] == "RUNNING":
                connection.execute(
                    """UPDATE attempts SET state = 'FAILED', completed_at_ms = ?,
                       error_code = ? WHERE attempt_id = ?""",
                    (now, error_code, attempt_id),
                )
                self._audit(
                    connection,
                    now,
                    attempt["worker_id"],
                    "ATTEMPT_FAILED",
                    attempt["job_id"],
                    attempt["shard_id"],
                    {"attempt_id": attempt_id, "error_code": error_code},
                )

    def status(
        self, job_id: str, *, connection: sqlite3.Connection | None = None
    ) -> dict[str, Any]:
        job_id = identifier(job_id, "job_id")
        owns = connection is None
        conn = connection or self._connect()
        try:
            job, spec = self._job(conn, job_id)
            states = {
                row["state"]: row["count"]
                for row in conn.execute(
                    """SELECT state, COUNT(*) AS count FROM tasks
                       WHERE job_id = ? GROUP BY state""",
                    (job_id,),
                )
            }
            attempt_states = {
                row["state"]: row["count"]
                for row in conn.execute(
                    """SELECT state, COUNT(*) AS count FROM attempts
                       WHERE job_id = ? GROUP BY state""",
                    (job_id,),
                )
            }
            counters: dict[str, int] = {}
            for row in conn.execute(
                "SELECT counters FROM tasks WHERE job_id = ? AND state = 'COMMITTED'",
                (job_id,),
            ):
                for name, value in json.loads(row["counters"]).items():
                    counters[name] = counters.get(name, 0) + value
            return {
                "job_id": job_id,
                "spec_hash": job["spec_hash"],
                "state": job["state"],
                "error_code": job["error_code"],
                "manifest_digest": job["manifest_digest"],
                "total_tasks": len(spec["shards"]),
                "task_states": states,
                "attempt_states": attempt_states,
                "counters": counters,
                "created_at_ms": job["created_at_ms"],
                "updated_at_ms": job["updated_at_ms"],
            }
        finally:
            if owns:
                conn.close()

    def attempts(self, job_id: str, shard_id: str | None = None) -> list[dict[str, Any]]:
        job_id = identifier(job_id, "job_id")
        parameters: tuple[Any, ...]
        query = """SELECT attempt_id, shard_id, ordinal, worker_id, kind, state,
                          started_at_ms, lease_deadline_ms, completed_at_ms,
                          output_digest, error_code
                   FROM attempts WHERE job_id = ?"""
        parameters = (job_id,)
        if shard_id is not None:
            shard_id = identifier(shard_id, "shard_id")
            query += " AND shard_id = ?"
            parameters = (job_id, shard_id)
        query += " ORDER BY shard_id, ordinal"
        with self._connect() as connection:
            return [dict(row) for row in connection.execute(query, parameters)]

    def output(self, job_id: str, shard_id: str) -> bytes:
        job_id = identifier(job_id, "job_id")
        shard_id = identifier(shard_id, "shard_id")
        with self._connect() as connection:
            row = connection.execute(
                """SELECT output_digest FROM tasks
                   WHERE job_id = ? AND shard_id = ? AND state = 'COMMITTED'""",
                (job_id, shard_id),
            ).fetchone()
        if row is None:
            raise NotFoundError("committed output not found")
        return self.blobs.get(row["output_digest"])

    def verify(self, job_id: str) -> dict[str, Any]:
        job_id = identifier(job_id, "job_id")
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT shard_id, output_digest FROM tasks
                   WHERE job_id = ? AND state = 'COMMITTED' ORDER BY ordinal""",
                (job_id,),
            ).fetchall()
        checked = 0
        failures: list[str] = []
        for row in rows:
            try:
                self.blobs.verify(row["output_digest"])
                checked += 1
            except IntegrityError:
                failures.append(row["shard_id"])
        return {"ok": not failures, "checked": checked, "failed_shards": failures}

    def audit_log(self, job_id: str, limit: int = 200) -> list[dict[str, Any]]:
        job_id = identifier(job_id, "job_id")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValidationError("limit must be in [1, 1000]")
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM audit WHERE job_id = ?
                   ORDER BY sequence DESC LIMIT ?""",
                (job_id, limit),
            ).fetchall()
        return [
            {
                "sequence": row["sequence"],
                "occurred_at_ms": row["occurred_at_ms"],
                "actor": row["actor"],
                "event": row["event"],
                "job_id": row["job_id"],
                "shard_id": row["shard_id"],
                "details": json.loads(row["details"]),
            }
            for row in rows
        ]
