from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from .errors import (
    ConflictError,
    IntegrityError,
    LeaseError,
    NotFoundError,
    SimulatedCrash,
    StateError,
    ValidationError,
)
from .model import (
    SliceDecision,
    canonical_json,
    digest,
    parse_json,
    require_event_kind,
    require_identifier,
    require_int,
    require_payload,
)


ZERO_HASH = "0" * 64
MAX_ATTEMPTS = 5
VALID_FAILPOINTS = {None, "after_slice_commit", "after_dispatch_commit", "after_tool_commit"}


def now_ns() -> int:
    return time.time_ns()


class SliceQueue:
    def __init__(self, path: str | Path, *, secret: bytes) -> None:
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValidationError("secret must contain at least 32 bytes")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.secret = secret
        self._closed = False
        self._schema_lock = threading.Lock()
        self._initialize()

    def __enter__(self) -> "SliceQueue":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise StateError("store is closed")

    def _connect(self) -> sqlite3.Connection:
        self._ensure_open()
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._schema_lock:
            connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
                connection.execute("PRAGMA foreign_keys = ON")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS namespaces (
                        namespace TEXT PRIMARY KEY,
                        audit_sequence INTEGER NOT NULL DEFAULT 0,
                        audit_head_hash TEXT NOT NULL,
                        created_ns INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS requests (
                        namespace TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        request_digest TEXT NOT NULL,
                        response_json TEXT NOT NULL,
                        created_ns INTEGER NOT NULL,
                        PRIMARY KEY (namespace, operation, idempotency_key),
                        FOREIGN KEY (namespace) REFERENCES namespaces(namespace)
                    );

                    CREATE TABLE IF NOT EXISTS threads (
                        namespace TEXT NOT NULL,
                        thread_id TEXT NOT NULL,
                        parent_thread_id TEXT,
                        status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'CLOSED')),
                        version INTEGER NOT NULL DEFAULT 0,
                        history_head_hash TEXT NOT NULL,
                        created_ns INTEGER NOT NULL,
                        updated_ns INTEGER NOT NULL,
                        PRIMARY KEY (namespace, thread_id),
                        FOREIGN KEY (namespace) REFERENCES namespaces(namespace),
                        FOREIGN KEY (namespace, parent_thread_id)
                            REFERENCES threads(namespace, thread_id)
                    );

                    CREATE TABLE IF NOT EXISTS events (
                        queue_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        namespace TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        thread_id TEXT NOT NULL,
                        kind TEXT NOT NULL CHECK (
                            kind IN ('USER', 'TOOL_RESULT', 'CHILD_REPORT', 'TIMER')
                        ),
                        payload_json TEXT NOT NULL,
                        event_digest TEXT NOT NULL,
                        status TEXT NOT NULL CHECK (
                            status IN ('PENDING', 'LEASED', 'PROCESSED', 'DEAD')
                        ),
                        available_ns INTEGER NOT NULL,
                        lease_worker TEXT,
                        lease_token TEXT,
                        lease_until_ns INTEGER,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        processed_version INTEGER,
                        created_ns INTEGER NOT NULL,
                        processed_ns INTEGER,
                        UNIQUE (namespace, event_id),
                        FOREIGN KEY (namespace, thread_id)
                            REFERENCES threads(namespace, thread_id)
                    );
                    CREATE INDEX IF NOT EXISTS events_ready
                        ON events(namespace, status, available_ns, queue_sequence);
                    CREATE INDEX IF NOT EXISTS events_thread
                        ON events(namespace, thread_id, status, queue_sequence);

                    CREATE TABLE IF NOT EXISTS history (
                        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        namespace TEXT NOT NULL,
                        thread_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        entry_index INTEGER NOT NULL CHECK (entry_index IN (0, 1)),
                        role TEXT NOT NULL CHECK (role IN ('EVENT', 'ASSISTANT')),
                        content_json TEXT NOT NULL,
                        source_event_id TEXT NOT NULL,
                        previous_hash TEXT NOT NULL,
                        entry_hash TEXT NOT NULL,
                        created_ns INTEGER NOT NULL,
                        UNIQUE (namespace, thread_id, version, entry_index),
                        FOREIGN KEY (namespace, thread_id)
                            REFERENCES threads(namespace, thread_id),
                        FOREIGN KEY (namespace, source_event_id)
                            REFERENCES events(namespace, event_id)
                    );

                    CREATE TABLE IF NOT EXISTS tool_calls (
                        namespace TEXT NOT NULL,
                        call_id TEXT NOT NULL,
                        thread_id TEXT NOT NULL,
                        source_event_id TEXT NOT NULL,
                        requested_version INTEGER NOT NULL,
                        tool_name TEXT NOT NULL,
                        arguments_json TEXT NOT NULL,
                        status TEXT NOT NULL CHECK (
                            status IN ('PENDING', 'DISPATCHED', 'COMPLETED')
                        ),
                        result_json TEXT,
                        result_digest TEXT,
                        created_ns INTEGER NOT NULL,
                        completed_ns INTEGER,
                        PRIMARY KEY (namespace, call_id),
                        FOREIGN KEY (namespace, thread_id)
                            REFERENCES threads(namespace, thread_id),
                        FOREIGN KEY (namespace, source_event_id)
                            REFERENCES events(namespace, event_id)
                    );

                    CREATE TABLE IF NOT EXISTS outbox (
                        outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        namespace TEXT NOT NULL,
                        dedupe_key TEXT NOT NULL,
                        kind TEXT NOT NULL CHECK (kind IN ('TOOL', 'SEND')),
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('PENDING', 'DISPATCHED')),
                        created_ns INTEGER NOT NULL,
                        dispatched_ns INTEGER,
                        UNIQUE (namespace, dedupe_key),
                        FOREIGN KEY (namespace) REFERENCES namespaces(namespace)
                    );
                    CREATE INDEX IF NOT EXISTS outbox_pending
                        ON outbox(namespace, status, outbox_id);

                    CREATE TABLE IF NOT EXISTS tool_tasks (
                        namespace TEXT NOT NULL,
                        call_id TEXT NOT NULL,
                        thread_id TEXT NOT NULL,
                        tool_name TEXT NOT NULL,
                        arguments_json TEXT NOT NULL,
                        status TEXT NOT NULL CHECK (
                            status IN ('PENDING', 'LEASED', 'COMPLETED', 'DEAD')
                        ),
                        lease_worker TEXT,
                        lease_token TEXT,
                        lease_until_ns INTEGER,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        result_json TEXT,
                        result_digest TEXT,
                        created_ns INTEGER NOT NULL,
                        completed_ns INTEGER,
                        PRIMARY KEY (namespace, call_id),
                        FOREIGN KEY (namespace, call_id)
                            REFERENCES tool_calls(namespace, call_id),
                        FOREIGN KEY (namespace, thread_id)
                            REFERENCES threads(namespace, thread_id)
                    );

                    CREATE TABLE IF NOT EXISTS audit_log (
                        namespace TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        payload_digest TEXT NOT NULL,
                        previous_hash TEXT NOT NULL,
                        entry_hash TEXT NOT NULL,
                        created_ns INTEGER NOT NULL,
                        PRIMARY KEY (namespace, sequence),
                        FOREIGN KEY (namespace) REFERENCES namespaces(namespace)
                    );
                    """
                )
            finally:
                connection.close()

    @staticmethod
    def _failpoint(value: str | None) -> str | None:
        if value not in VALID_FAILPOINTS:
            raise ValidationError("unknown failpoint")
        return value

    def _sign(self, value: Any) -> str:
        return hmac.new(self.secret, canonical_json(value).encode(), hashlib.sha256).hexdigest()

    def _ensure_namespace(
        self, connection: sqlite3.Connection, namespace: str, created_ns: int
    ) -> None:
        connection.execute(
            """INSERT OR IGNORE INTO namespaces
               (namespace, audit_sequence, audit_head_hash, created_ns)
               VALUES (?, 0, ?, ?)""",
            (namespace, ZERO_HASH, created_ns),
        )

    def _audit(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        action: str,
        entity_id: str,
        payload: Any,
        created_ns: int,
    ) -> None:
        row = connection.execute(
            "SELECT audit_sequence, audit_head_hash FROM namespaces WHERE namespace = ?",
            (namespace,),
        ).fetchone()
        assert row is not None
        sequence = row["audit_sequence"] + 1
        payload_digest = digest(payload)
        material = {
            "namespace": namespace,
            "sequence": sequence,
            "action": action,
            "entity_id": entity_id,
            "payload_digest": payload_digest,
            "previous_hash": row["audit_head_hash"],
            "created_ns": created_ns,
        }
        entry_hash = self._sign(material)
        connection.execute(
            """INSERT INTO audit_log
               (namespace, sequence, action, entity_id, payload_digest,
                previous_hash, entry_hash, created_ns)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                namespace,
                sequence,
                action,
                entity_id,
                payload_digest,
                row["audit_head_hash"],
                entry_hash,
                created_ns,
            ),
        )
        connection.execute(
            """UPDATE namespaces SET audit_sequence = ?, audit_head_hash = ?
               WHERE namespace = ?""",
            (sequence, entry_hash, namespace),
        )

    @staticmethod
    def _decode(raw: str, name: str) -> Any:
        return parse_json(raw, name)

    def launch_thread(
        self,
        namespace_value: str,
        idempotency_key_value: str,
        *,
        thread_id: str | None = None,
        parent_thread_id: str | None = None,
        created_ns: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        idempotency_key = require_identifier(idempotency_key_value, "idempotency_key")
        requested_thread_id = (
            None if thread_id is None else require_identifier(thread_id, "thread_id")
        )
        parent = (
            None
            if parent_thread_id is None
            else require_identifier(parent_thread_id, "parent_thread_id")
        )
        created = now_ns() if created_ns is None else require_int(created_ns, "created_ns", minimum=0)
        request = {"thread_id": requested_thread_id, "parent_thread_id": parent}
        request_digest = digest(request)
        with self._transaction() as connection:
            self._ensure_namespace(connection, namespace, created)
            replay = connection.execute(
                """SELECT request_digest, response_json FROM requests
                   WHERE namespace = ? AND operation = 'LAUNCH_THREAD'
                     AND idempotency_key = ?""",
                (namespace, idempotency_key),
            ).fetchone()
            if replay is not None:
                if not hmac.compare_digest(replay["request_digest"], request_digest):
                    raise ConflictError("idempotency key was reused with different input")
                return {**self._decode(replay["response_json"], "stored response"), "replayed": True}
            if parent is not None:
                parent_row = connection.execute(
                    """SELECT status FROM threads
                       WHERE namespace = ? AND thread_id = ?""",
                    (namespace, parent),
                ).fetchone()
                if parent_row is None:
                    raise NotFoundError("parent thread was not found")
            identity = requested_thread_id or uuid.uuid4().hex
            try:
                connection.execute(
                    """INSERT INTO threads
                       (namespace, thread_id, parent_thread_id, status, version,
                        history_head_hash, created_ns, updated_ns)
                       VALUES (?, ?, ?, 'ACTIVE', 0, ?, ?, ?)""",
                    (namespace, identity, parent, ZERO_HASH, created, created),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("thread identity already exists") from exc
            response = {
                "namespace": namespace,
                "thread_id": identity,
                "parent_thread_id": parent,
                "status": "ACTIVE",
                "version": 0,
                "replayed": False,
            }
            connection.execute(
                """INSERT INTO requests
                   (namespace, operation, idempotency_key, request_digest,
                    response_json, created_ns)
                   VALUES (?, 'LAUNCH_THREAD', ?, ?, ?, ?)""",
                (
                    namespace,
                    idempotency_key,
                    request_digest,
                    canonical_json({key: value for key, value in response.items() if key != "replayed"}),
                    created,
                ),
            )
            self._audit(connection, namespace, "THREAD_LAUNCHED", identity, request, created)
            return response

    def _insert_event(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        event_id: str,
        thread_id: str,
        kind: str,
        payload: dict[str, Any],
        available_ns: int,
        created_ns: int,
    ) -> tuple[dict[str, Any], bool]:
        event_digest = digest({
            "thread_id": thread_id,
            "kind": kind,
            "payload": payload,
        })
        existing = connection.execute(
            """SELECT * FROM events WHERE namespace = ? AND event_id = ?""",
            (namespace, event_id),
        ).fetchone()
        if existing is not None:
            if not hmac.compare_digest(existing["event_digest"], event_digest):
                raise ConflictError("event identity was reused with different input")
            return self._event_result(existing), True
        thread = connection.execute(
            """SELECT status FROM threads WHERE namespace = ? AND thread_id = ?""",
            (namespace, thread_id),
        ).fetchone()
        if thread is None:
            raise NotFoundError("thread was not found")
        if thread["status"] != "ACTIVE":
            raise ConflictError("thread is closed")
        cursor = connection.execute(
            """INSERT INTO events
               (namespace, event_id, thread_id, kind, payload_json, event_digest,
                status, available_ns, attempts, created_ns)
               VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, 0, ?)""",
            (
                namespace,
                event_id,
                thread_id,
                kind,
                canonical_json(payload),
                event_digest,
                available_ns,
                created_ns,
            ),
        )
        row = connection.execute(
            "SELECT * FROM events WHERE queue_sequence = ?", (cursor.lastrowid,)
        ).fetchone()
        assert row is not None
        return self._event_result(row), False

    @staticmethod
    def _event_result(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "thread_id": row["thread_id"],
            "kind": row["kind"],
            "queue_sequence": row["queue_sequence"],
            "status": row["status"],
            "available_ns": row["available_ns"],
            "attempts": row["attempts"],
        }

    def enqueue_event(
        self,
        namespace_value: str,
        event_id_value: str,
        thread_id_value: str,
        kind_value: str,
        payload_value: Any,
        *,
        available_ns: int | None = None,
        created_ns: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        event_id = require_identifier(event_id_value, "event_id")
        thread_id = require_identifier(thread_id_value, "thread_id")
        kind = require_event_kind(kind_value)
        payload = require_payload(payload_value)
        created = now_ns() if created_ns is None else require_int(created_ns, "created_ns", minimum=0)
        available = created if available_ns is None else require_int(
            available_ns, "available_ns", minimum=0
        )
        with self._transaction() as connection:
            result, replayed = self._insert_event(
                connection,
                namespace,
                event_id,
                thread_id,
                kind,
                payload,
                available,
                created,
            )
            if not replayed:
                self._audit(
                    connection,
                    namespace,
                    "EVENT_ENQUEUED",
                    event_id,
                    {"thread_id": thread_id, "kind": kind, "payload": payload},
                    created,
                )
            return {**result, "replayed": replayed}

    def _history(
        self, connection: sqlite3.Connection, namespace: str, thread_id: str
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """SELECT version, entry_index, role, content_json, source_event_id
               FROM history WHERE namespace = ? AND thread_id = ?
               ORDER BY version, entry_index""",
            (namespace, thread_id),
        ).fetchall()
        return [
            {
                "version": row["version"],
                "entry_index": row["entry_index"],
                "role": row["role"],
                "content": self._decode(row["content_json"], "history content"),
                "source_event_id": row["source_event_id"],
            }
            for row in rows
        ]

    def get_thread(self, namespace_value: str, thread_id_value: str) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        thread_id = require_identifier(thread_id_value, "thread_id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM threads WHERE namespace = ? AND thread_id = ?",
                (namespace, thread_id),
            ).fetchone()
            if row is None:
                raise NotFoundError("thread was not found")
            return {
                "namespace": namespace,
                "thread_id": thread_id,
                "parent_thread_id": row["parent_thread_id"],
                "status": row["status"],
                "version": row["version"],
                "history": self._history(connection, namespace, thread_id),
            }
        finally:
            connection.close()

    def _reap_event_leases(
        self, connection: sqlite3.Connection, namespace: str, current_ns: int
    ) -> tuple[int, int]:
        dead = connection.execute(
            """UPDATE events
               SET status = 'DEAD', lease_worker = NULL, lease_token = NULL,
                   lease_until_ns = NULL
               WHERE namespace = ? AND status = 'LEASED'
                 AND lease_until_ns < ? AND attempts >= ?""",
            (namespace, current_ns, MAX_ATTEMPTS),
        ).rowcount
        retried = connection.execute(
            """UPDATE events
               SET status = 'PENDING', lease_worker = NULL, lease_token = NULL,
                   lease_until_ns = NULL
               WHERE namespace = ? AND status = 'LEASED'
                 AND lease_until_ns < ? AND attempts < ?""",
            (namespace, current_ns, MAX_ATTEMPTS),
        ).rowcount
        return retried, dead

    def claim_event(
        self,
        namespace_value: str,
        worker_id_value: str,
        *,
        lease_ms: int = 30_000,
        current_ns: int | None = None,
    ) -> dict[str, Any] | None:
        namespace = require_identifier(namespace_value, "namespace")
        worker_id = require_identifier(worker_id_value, "worker_id")
        lease_ms = require_int(lease_ms, "lease_ms", minimum=1, maximum=600_000)
        current = now_ns() if current_ns is None else require_int(current_ns, "current_ns", minimum=0)
        lease_until = current + lease_ms * 1_000_000
        with self._transaction() as connection:
            self._reap_event_leases(connection, namespace, current)
            row = connection.execute(
                """SELECT e.*, t.version AS thread_version
                   FROM events e
                   JOIN threads t
                     ON t.namespace = e.namespace AND t.thread_id = e.thread_id
                   WHERE e.namespace = ? AND e.status = 'PENDING'
                     AND e.available_ns <= ? AND t.status = 'ACTIVE'
                     AND NOT EXISTS (
                         SELECT 1 FROM events leased
                         WHERE leased.namespace = e.namespace
                           AND leased.thread_id = e.thread_id
                           AND leased.status = 'LEASED'
                     )
                     AND NOT EXISTS (
                         SELECT 1 FROM events earlier
                         WHERE earlier.namespace = e.namespace
                           AND earlier.thread_id = e.thread_id
                           AND earlier.status = 'PENDING'
                           AND earlier.available_ns <= ?
                           AND earlier.queue_sequence < e.queue_sequence
                     )
                   ORDER BY e.available_ns, e.queue_sequence
                   LIMIT 1""",
                (namespace, current, current),
            ).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            updated = connection.execute(
                """UPDATE events
                   SET status = 'LEASED', lease_worker = ?, lease_token = ?,
                       lease_until_ns = ?, attempts = attempts + 1
                   WHERE queue_sequence = ? AND status = 'PENDING'""",
                (worker_id, token, lease_until, row["queue_sequence"]),
            ).rowcount
            if updated != 1:
                raise StateError("event claim lost a serialization race")
            return {
                "namespace": namespace,
                "event_id": row["event_id"],
                "thread_id": row["thread_id"],
                "kind": row["kind"],
                "payload": self._decode(row["payload_json"], "event payload"),
                "queue_sequence": row["queue_sequence"],
                "worker_id": worker_id,
                "lease_token": token,
                "lease_until_ns": lease_until,
                "attempt": row["attempts"] + 1,
                "expected_thread_version": row["thread_version"],
                "history": self._history(connection, namespace, row["thread_id"]),
            }

    def heartbeat_event(
        self,
        namespace_value: str,
        event_id_value: str,
        worker_id_value: str,
        lease_token_value: str,
        *,
        lease_ms: int = 30_000,
        current_ns: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        event_id = require_identifier(event_id_value, "event_id")
        worker_id = require_identifier(worker_id_value, "worker_id")
        lease_token = require_identifier(lease_token_value, "lease_token")
        lease_ms = require_int(lease_ms, "lease_ms", minimum=1, maximum=600_000)
        current = now_ns() if current_ns is None else require_int(current_ns, "current_ns", minimum=0)
        lease_until = current + lease_ms * 1_000_000
        with self._transaction() as connection:
            updated = connection.execute(
                """UPDATE events SET lease_until_ns = ?
                   WHERE namespace = ? AND event_id = ? AND status = 'LEASED'
                     AND lease_worker = ? AND lease_token = ? AND lease_until_ns >= ?""",
                (lease_until, namespace, event_id, worker_id, lease_token, current),
            ).rowcount
            if updated != 1:
                raise LeaseError("event lease is missing, stale, or expired")
        return {"event_id": event_id, "lease_until_ns": lease_until}

    def _append_history(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        thread_id: str,
        version: int,
        source_event_id: str,
        previous_hash: str,
        role: str,
        content: Any,
        entry_index: int,
        created_ns: int,
    ) -> str:
        material = {
            "namespace": namespace,
            "thread_id": thread_id,
            "version": version,
            "entry_index": entry_index,
            "role": role,
            "content": content,
            "source_event_id": source_event_id,
            "previous_hash": previous_hash,
            "created_ns": created_ns,
        }
        entry_hash = self._sign(material)
        connection.execute(
            """INSERT INTO history
               (namespace, thread_id, version, entry_index, role, content_json,
                source_event_id, previous_hash, entry_hash, created_ns)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                namespace,
                thread_id,
                version,
                entry_index,
                role,
                canonical_json(content),
                source_event_id,
                previous_hash,
                entry_hash,
                created_ns,
            ),
        )
        return entry_hash

    def commit_slice(
        self,
        namespace_value: str,
        event_id_value: str,
        worker_id_value: str,
        lease_token_value: str,
        expected_thread_version_value: int,
        decision_value: Any,
        *,
        current_ns: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        event_id = require_identifier(event_id_value, "event_id")
        worker_id = require_identifier(worker_id_value, "worker_id")
        lease_token = require_identifier(lease_token_value, "lease_token")
        expected_version = require_int(
            expected_thread_version_value, "expected_thread_version", minimum=0
        )
        decision = SliceDecision.from_value(decision_value)
        current = now_ns() if current_ns is None else require_int(current_ns, "current_ns", minimum=0)
        failpoint = self._failpoint(failpoint)
        result: dict[str, Any]
        with self._transaction() as connection:
            event = connection.execute(
                """SELECT * FROM events WHERE namespace = ? AND event_id = ?""",
                (namespace, event_id),
            ).fetchone()
            if event is None:
                raise NotFoundError("event was not found")
            if event["status"] != "LEASED":
                raise LeaseError("event is not currently leased")
            if (
                event["lease_worker"] != worker_id
                or not hmac.compare_digest(event["lease_token"], lease_token)
                or event["lease_until_ns"] < current
            ):
                raise LeaseError("event lease is stale or expired")
            thread = connection.execute(
                """SELECT * FROM threads WHERE namespace = ? AND thread_id = ?""",
                (namespace, event["thread_id"]),
            ).fetchone()
            assert thread is not None
            if thread["status"] != "ACTIVE":
                raise ConflictError("thread is closed")
            if thread["version"] != expected_version:
                raise ConflictError("thread version changed after the event was claimed")
            version = expected_version + 1
            event_content = {
                "kind": event["kind"],
                "payload": self._decode(event["payload_json"], "event payload"),
            }
            head = self._append_history(
                connection,
                namespace,
                event["thread_id"],
                version,
                event_id,
                thread["history_head_hash"],
                "EVENT",
                event_content,
                0,
                current,
            )
            assistant_content = {
                "message": decision.assistant_message,
                "command": decision.command,
            }
            head = self._append_history(
                connection,
                namespace,
                event["thread_id"],
                version,
                event_id,
                head,
                "ASSISTANT",
                assistant_content,
                1,
                current,
            )
            call_id: str | None = None
            command = decision.command
            new_status = "ACTIVE"
            if command is not None and command["type"] == "TOOL":
                call_id = f"call:{uuid.uuid4().hex}"
                connection.execute(
                    """INSERT INTO tool_calls
                       (namespace, call_id, thread_id, source_event_id,
                        requested_version, tool_name, arguments_json, status, created_ns)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
                    (
                        namespace,
                        call_id,
                        event["thread_id"],
                        event_id,
                        version,
                        command["tool_name"],
                        canonical_json(command["arguments"]),
                        current,
                    ),
                )
                outbox_payload = {
                    "call_id": call_id,
                    "thread_id": event["thread_id"],
                    "tool_name": command["tool_name"],
                    "arguments": command["arguments"],
                }
                connection.execute(
                    """INSERT INTO outbox
                       (namespace, dedupe_key, kind, payload_json, status, created_ns)
                       VALUES (?, ?, 'TOOL', ?, 'PENDING', ?)""",
                    (namespace, f"tool:{event_id}", canonical_json(outbox_payload), current),
                )
            elif command is not None and command["type"] == "SEND":
                target = connection.execute(
                    """SELECT status FROM threads
                       WHERE namespace = ? AND thread_id = ?""",
                    (namespace, command["target_thread_id"]),
                ).fetchone()
                if target is None:
                    raise NotFoundError("target thread was not found")
                if target["status"] != "ACTIVE":
                    raise ConflictError("target thread is closed")
                outbox_payload = {
                    "source_thread_id": event["thread_id"],
                    "target_thread_id": command["target_thread_id"],
                    "payload": command["payload"],
                }
                connection.execute(
                    """INSERT INTO outbox
                       (namespace, dedupe_key, kind, payload_json, status, created_ns)
                       VALUES (?, ?, 'SEND', ?, 'PENDING', ?)""",
                    (namespace, f"send:{event_id}", canonical_json(outbox_payload), current),
                )
            elif command is not None and command["type"] == "CLOSE":
                new_status = "CLOSED"
            connection.execute(
                """UPDATE threads
                   SET version = ?, history_head_hash = ?, status = ?, updated_ns = ?
                   WHERE namespace = ? AND thread_id = ?""",
                (version, head, new_status, current, namespace, event["thread_id"]),
            )
            connection.execute(
                """UPDATE events
                   SET status = 'PROCESSED', processed_version = ?, processed_ns = ?,
                       lease_worker = NULL, lease_token = NULL, lease_until_ns = NULL
                   WHERE namespace = ? AND event_id = ?""",
                (version, current, namespace, event_id),
            )
            result = {
                "event_id": event_id,
                "thread_id": event["thread_id"],
                "thread_version": version,
                "thread_status": new_status,
                "call_id": call_id,
                "command": command,
            }
            self._audit(connection, namespace, "SLICE_COMMITTED", event_id, result, current)
        if failpoint == "after_slice_commit":
            raise SimulatedCrash("simulated crash after slice commit")
        return result

    def run_one(
        self,
        namespace: str,
        worker_id: str,
        handler: Callable[[list[dict[str, Any]], dict[str, Any]], Any],
        *,
        lease_ms: int = 30_000,
        current_ns: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any] | None:
        claim = self.claim_event(
            namespace, worker_id, lease_ms=lease_ms, current_ns=current_ns
        )
        if claim is None:
            return None
        decision = handler(
            claim["history"],
            {"kind": claim["kind"], "payload": claim["payload"]},
        )
        return self.commit_slice(
            namespace,
            claim["event_id"],
            worker_id,
            claim["lease_token"],
            claim["expected_thread_version"],
            decision,
            current_ns=current_ns,
            failpoint=failpoint,
        )

    def dispatch_outbox(
        self,
        namespace_value: str,
        *,
        limit: int = 100,
        current_ns: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        limit = require_int(limit, "limit", minimum=1, maximum=1_000)
        current = now_ns() if current_ns is None else require_int(current_ns, "current_ns", minimum=0)
        failpoint = self._failpoint(failpoint)
        dispatched: list[int] = []
        with self._transaction() as connection:
            rows = connection.execute(
                """SELECT * FROM outbox
                   WHERE namespace = ? AND status = 'PENDING'
                   ORDER BY outbox_id LIMIT ?""",
                (namespace, limit),
            ).fetchall()
            for row in rows:
                payload = self._decode(row["payload_json"], "outbox payload")
                if row["kind"] == "TOOL":
                    connection.execute(
                        """INSERT OR IGNORE INTO tool_tasks
                           (namespace, call_id, thread_id, tool_name, arguments_json,
                            status, attempts, created_ns)
                           VALUES (?, ?, ?, ?, ?, 'PENDING', 0, ?)""",
                        (
                            namespace,
                            payload["call_id"],
                            payload["thread_id"],
                            payload["tool_name"],
                            canonical_json(payload["arguments"]),
                            current,
                        ),
                    )
                    connection.execute(
                        """UPDATE tool_calls SET status = 'DISPATCHED'
                           WHERE namespace = ? AND call_id = ? AND status = 'PENDING'""",
                        (namespace, payload["call_id"]),
                    )
                else:
                    message_id = f"message:{row['outbox_id']}"
                    message_payload = {
                        "source_thread_id": payload["source_thread_id"],
                        "payload": payload["payload"],
                    }
                    self._insert_event(
                        connection,
                        namespace,
                        message_id,
                        payload["target_thread_id"],
                        "CHILD_REPORT",
                        message_payload,
                        current,
                        current,
                    )
                connection.execute(
                    """UPDATE outbox SET status = 'DISPATCHED', dispatched_ns = ?
                       WHERE outbox_id = ?""",
                    (current, row["outbox_id"]),
                )
                dispatched.append(row["outbox_id"])
            if dispatched:
                self._audit(
                    connection,
                    namespace,
                    "OUTBOX_DISPATCHED",
                    str(dispatched[-1]),
                    {"outbox_ids": dispatched},
                    current,
                )
        if failpoint == "after_dispatch_commit" and dispatched:
            raise SimulatedCrash("simulated lost acknowledgement after outbox dispatch")
        return {"dispatched": len(dispatched), "outbox_ids": dispatched}

    def _reap_tool_leases(
        self, connection: sqlite3.Connection, namespace: str, current_ns: int
    ) -> tuple[int, int]:
        dead = connection.execute(
            """UPDATE tool_tasks
               SET status = 'DEAD', lease_worker = NULL, lease_token = NULL,
                   lease_until_ns = NULL
               WHERE namespace = ? AND status = 'LEASED'
                 AND lease_until_ns < ? AND attempts >= ?""",
            (namespace, current_ns, MAX_ATTEMPTS),
        ).rowcount
        retried = connection.execute(
            """UPDATE tool_tasks
               SET status = 'PENDING', lease_worker = NULL, lease_token = NULL,
                   lease_until_ns = NULL
               WHERE namespace = ? AND status = 'LEASED'
                 AND lease_until_ns < ? AND attempts < ?""",
            (namespace, current_ns, MAX_ATTEMPTS),
        ).rowcount
        return retried, dead

    def claim_tool(
        self,
        namespace_value: str,
        worker_id_value: str,
        *,
        lease_ms: int = 30_000,
        current_ns: int | None = None,
    ) -> dict[str, Any] | None:
        namespace = require_identifier(namespace_value, "namespace")
        worker_id = require_identifier(worker_id_value, "worker_id")
        lease_ms = require_int(lease_ms, "lease_ms", minimum=1, maximum=600_000)
        current = now_ns() if current_ns is None else require_int(current_ns, "current_ns", minimum=0)
        lease_until = current + lease_ms * 1_000_000
        with self._transaction() as connection:
            self._reap_tool_leases(connection, namespace, current)
            row = connection.execute(
                """SELECT * FROM tool_tasks
                   WHERE namespace = ? AND status = 'PENDING'
                   ORDER BY created_ns, call_id LIMIT 1""",
                (namespace,),
            ).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            updated = connection.execute(
                """UPDATE tool_tasks
                   SET status = 'LEASED', lease_worker = ?, lease_token = ?,
                       lease_until_ns = ?, attempts = attempts + 1
                   WHERE namespace = ? AND call_id = ? AND status = 'PENDING'""",
                (worker_id, token, lease_until, namespace, row["call_id"]),
            ).rowcount
            if updated != 1:
                raise StateError("tool claim lost a serialization race")
            return {
                "namespace": namespace,
                "call_id": row["call_id"],
                "thread_id": row["thread_id"],
                "tool_name": row["tool_name"],
                "arguments": self._decode(row["arguments_json"], "tool arguments"),
                "worker_id": worker_id,
                "lease_token": token,
                "lease_until_ns": lease_until,
                "attempt": row["attempts"] + 1,
            }

    def complete_tool(
        self,
        namespace_value: str,
        call_id_value: str,
        worker_id_value: str,
        lease_token_value: str,
        result_value: Any,
        *,
        result_available_ns: int | None = None,
        current_ns: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        call_id = require_identifier(call_id_value, "call_id")
        worker_id = require_identifier(worker_id_value, "worker_id")
        lease_token = require_identifier(lease_token_value, "lease_token")
        result_payload = require_payload(result_value, "result")
        result_digest = digest(result_payload)
        current = now_ns() if current_ns is None else require_int(current_ns, "current_ns", minimum=0)
        available = current if result_available_ns is None else require_int(
            result_available_ns, "result_available_ns", minimum=0
        )
        failpoint = self._failpoint(failpoint)
        response: dict[str, Any]
        with self._transaction() as connection:
            task = connection.execute(
                """SELECT * FROM tool_tasks WHERE namespace = ? AND call_id = ?""",
                (namespace, call_id),
            ).fetchone()
            if task is None:
                raise NotFoundError("tool task was not found")
            if task["status"] == "COMPLETED":
                if not hmac.compare_digest(task["result_digest"], result_digest):
                    raise ConflictError("completed tool was retried with a different result")
                return {
                    "call_id": call_id,
                    "result_event_id": f"tool-result:{call_id}",
                    "replayed": True,
                }
            if task["status"] != "LEASED":
                raise LeaseError("tool task is not currently leased")
            if (
                task["lease_worker"] != worker_id
                or not hmac.compare_digest(task["lease_token"], lease_token)
                or task["lease_until_ns"] < current
            ):
                raise LeaseError("tool lease is stale or expired")
            call = connection.execute(
                """SELECT * FROM tool_calls WHERE namespace = ? AND call_id = ?""",
                (namespace, call_id),
            ).fetchone()
            assert call is not None
            event_id = f"tool-result:{call_id}"
            payload = {
                "call_id": call_id,
                "tool_name": task["tool_name"],
                "result": result_payload,
            }
            self._insert_event(
                connection,
                namespace,
                event_id,
                task["thread_id"],
                "TOOL_RESULT",
                payload,
                available,
                current,
            )
            connection.execute(
                """UPDATE tool_tasks
                   SET status = 'COMPLETED', result_json = ?, result_digest = ?,
                       completed_ns = ?, lease_worker = NULL, lease_token = NULL,
                       lease_until_ns = NULL
                   WHERE namespace = ? AND call_id = ?""",
                (
                    canonical_json(result_payload),
                    result_digest,
                    current,
                    namespace,
                    call_id,
                ),
            )
            connection.execute(
                """UPDATE tool_calls
                   SET status = 'COMPLETED', result_json = ?, result_digest = ?,
                       completed_ns = ?
                   WHERE namespace = ? AND call_id = ?""",
                (
                    canonical_json(result_payload),
                    result_digest,
                    current,
                    namespace,
                    call_id,
                ),
            )
            response = {
                "call_id": call_id,
                "result_event_id": event_id,
                "replayed": False,
            }
            self._audit(connection, namespace, "TOOL_COMPLETED", call_id, payload, current)
        if failpoint == "after_tool_commit":
            raise SimulatedCrash("simulated lost acknowledgement after tool completion")
        return response

    def stats(self, namespace_value: str) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        connection = self._connect()
        try:
            exists = connection.execute(
                "SELECT 1 FROM namespaces WHERE namespace = ?", (namespace,)
            ).fetchone()
            if exists is None:
                raise NotFoundError("namespace was not found")
            counts: dict[str, int] = {}
            for label, table in (
                ("threads", "threads"),
                ("events", "events"),
                ("history_entries", "history"),
                ("tool_calls", "tool_calls"),
                ("outbox_entries", "outbox"),
                ("tool_tasks", "tool_tasks"),
                ("audit_entries", "audit_log"),
            ):
                counts[label] = connection.execute(
                    f"SELECT COUNT(*) AS count FROM {table} WHERE namespace = ?",
                    (namespace,),
                ).fetchone()["count"]
            status_counts = {
                row["status"]: row["count"]
                for row in connection.execute(
                    """SELECT status, COUNT(*) AS count FROM events
                       WHERE namespace = ? GROUP BY status""",
                    (namespace,),
                ).fetchall()
            }
            tool_status_counts = {
                row["status"]: row["count"]
                for row in connection.execute(
                    """SELECT status, COUNT(*) AS count FROM tool_tasks
                       WHERE namespace = ? GROUP BY status""",
                    (namespace,),
                ).fetchall()
            }
            return {
                "namespace": namespace,
                **counts,
                "event_status": status_counts,
                "tool_task_status": tool_status_counts,
                "database_bytes": self.path.stat().st_size,
            }
        finally:
            connection.close()

    def verify_integrity(self, namespace_value: str | None = None) -> dict[str, Any]:
        namespace = (
            None if namespace_value is None else require_identifier(namespace_value, "namespace")
        )
        connection = self._connect()
        totals = {
            "namespaces": 0,
            "threads": 0,
            "events": 0,
            "history_entries": 0,
            "tool_calls": 0,
            "tool_tasks": 0,
            "audit_entries": 0,
        }
        try:
            quick = connection.execute("PRAGMA quick_check").fetchone()[0]
            if quick != "ok":
                raise IntegrityError(f"SQLite quick_check failed: {quick}")
            query = "SELECT * FROM namespaces"
            params: tuple[Any, ...] = ()
            if namespace is not None:
                query += " WHERE namespace = ?"
                params = (namespace,)
            namespaces = connection.execute(query + " ORDER BY namespace", params).fetchall()
            if namespace is not None and not namespaces:
                raise NotFoundError("namespace was not found")
            for ns_row in namespaces:
                ns = ns_row["namespace"]
                totals["namespaces"] += 1
                previous = ZERO_HASH
                audits = connection.execute(
                    "SELECT * FROM audit_log WHERE namespace = ? ORDER BY sequence",
                    (ns,),
                ).fetchall()
                for expected_sequence, row in enumerate(audits, 1):
                    if row["sequence"] != expected_sequence or row["previous_hash"] != previous:
                        raise IntegrityError("audit sequence or previous hash is invalid")
                    material = {
                        "namespace": ns,
                        "sequence": row["sequence"],
                        "action": row["action"],
                        "entity_id": row["entity_id"],
                        "payload_digest": row["payload_digest"],
                        "previous_hash": previous,
                        "created_ns": row["created_ns"],
                    }
                    if not hmac.compare_digest(row["entry_hash"], self._sign(material)):
                        raise IntegrityError("audit entry hash is invalid")
                    previous = row["entry_hash"]
                if ns_row["audit_sequence"] != len(audits) or ns_row["audit_head_hash"] != previous:
                    raise IntegrityError("materialized audit head is invalid")
                totals["audit_entries"] += len(audits)

                threads = connection.execute(
                    "SELECT * FROM threads WHERE namespace = ? ORDER BY thread_id", (ns,)
                ).fetchall()
                for thread in threads:
                    rows = connection.execute(
                        """SELECT * FROM history
                           WHERE namespace = ? AND thread_id = ?
                           ORDER BY version, entry_index""",
                        (ns, thread["thread_id"]),
                    ).fetchall()
                    if len(rows) != thread["version"] * 2:
                        raise IntegrityError("thread history count does not match its version")
                    head = ZERO_HASH
                    for position, row in enumerate(rows):
                        expected_version = position // 2 + 1
                        expected_index = position % 2
                        if row["version"] != expected_version or row["entry_index"] != expected_index:
                            raise IntegrityError("thread history versions are not contiguous")
                        content = self._decode(row["content_json"], "history content")
                        material = {
                            "namespace": ns,
                            "thread_id": thread["thread_id"],
                            "version": row["version"],
                            "entry_index": row["entry_index"],
                            "role": row["role"],
                            "content": content,
                            "source_event_id": row["source_event_id"],
                            "previous_hash": head,
                            "created_ns": row["created_ns"],
                        }
                        if row["previous_hash"] != head or not hmac.compare_digest(
                            row["entry_hash"], self._sign(material)
                        ):
                            raise IntegrityError("thread history hash chain is invalid")
                        head = row["entry_hash"]
                    if thread["history_head_hash"] != head:
                        raise IntegrityError("materialized history head is invalid")
                    processed = connection.execute(
                        """SELECT COUNT(*) AS count FROM events
                           WHERE namespace = ? AND thread_id = ? AND status = 'PROCESSED'""",
                        (ns, thread["thread_id"]),
                    ).fetchone()["count"]
                    if processed != thread["version"]:
                        raise IntegrityError("processed event count does not match thread version")
                    totals["history_entries"] += len(rows)
                totals["threads"] += len(threads)

                events = connection.execute(
                    "SELECT * FROM events WHERE namespace = ?", (ns,)
                ).fetchall()
                for event in events:
                    expected = digest({
                        "thread_id": event["thread_id"],
                        "kind": event["kind"],
                        "payload": self._decode(event["payload_json"], "event payload"),
                    })
                    if not hmac.compare_digest(event["event_digest"], expected):
                        raise IntegrityError("event digest is invalid")
                    if event["status"] == "PROCESSED":
                        history_count = connection.execute(
                            """SELECT COUNT(*) AS count FROM history
                               WHERE namespace = ? AND source_event_id = ?""",
                            (ns, event["event_id"]),
                        ).fetchone()["count"]
                        if history_count != 2 or event["processed_version"] is None:
                            raise IntegrityError("processed event is detached from history")
                totals["events"] += len(events)

                calls = connection.execute(
                    "SELECT * FROM tool_calls WHERE namespace = ?", (ns,)
                ).fetchall()
                for call in calls:
                    outbox_count = connection.execute(
                        """SELECT COUNT(*) AS count FROM outbox
                           WHERE namespace = ? AND kind = 'TOOL'
                             AND json_extract(payload_json, '$.call_id') = ?""",
                        (ns, call["call_id"]),
                    ).fetchone()["count"]
                    if outbox_count != 1:
                        raise IntegrityError("tool call is detached from its outbox entry")
                    task = connection.execute(
                        """SELECT * FROM tool_tasks
                           WHERE namespace = ? AND call_id = ?""",
                        (ns, call["call_id"]),
                    ).fetchone()
                    if call["status"] in {"DISPATCHED", "COMPLETED"} and task is None:
                        raise IntegrityError("dispatched tool call has no task")
                    if call["status"] == "COMPLETED":
                        assert task is not None
                        if task["status"] != "COMPLETED" or task["result_digest"] != call["result_digest"]:
                            raise IntegrityError("completed tool call and task disagree")
                        result_event = connection.execute(
                            """SELECT 1 FROM events
                               WHERE namespace = ? AND event_id = ?""",
                            (ns, f"tool-result:{call['call_id']}"),
                        ).fetchone()
                        if result_event is None:
                            raise IntegrityError("completed tool has no result event")
                totals["tool_calls"] += len(calls)
                totals["tool_tasks"] += connection.execute(
                    "SELECT COUNT(*) AS count FROM tool_tasks WHERE namespace = ?", (ns,)
                ).fetchone()["count"]
            return {"valid": True, **totals}
        except (sqlite3.DatabaseError, ValidationError) as exc:
            raise IntegrityError(f"stored state is invalid: {exc}") from exc
        finally:
            connection.close()
