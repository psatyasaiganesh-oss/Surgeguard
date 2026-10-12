"""SQLite-backed transactional inbox, projection, outbox, and idempotent sink."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path

from .schema import Event


SCHEMA = """
CREATE TABLE IF NOT EXISTS webhook_events (
    event_id TEXT PRIMARY KEY,
    effect_key TEXT NOT NULL UNIQUE,
    payload_sha256 TEXT NOT NULL,
    raw_payload BLOB NOT NULL,
    event_type TEXT NOT NULL,
    payment_id TEXT NOT NULL,
    resource_version INTEGER NOT NULL,
    payment_status TEXT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT NOT NULL,
    event_created INTEGER NOT NULL,
    arrival_sequence INTEGER NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('pending','processed','stale','conflict')),
    received_at REAL NOT NULL,
    processed_at REAL,
    note TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_state_received ON webhook_events(state, received_at, event_id);

CREATE TABLE IF NOT EXISTS ingest_audit (
    receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    effect_key TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result IN ('accepted','duplicate_event','duplicate_effect','event_conflict')),
    recorded_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    resource_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    source_event_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    message TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','inflight','delivered')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    lease_token TEXT,
    lease_until REAL,
    delivered_at REAL,
    FOREIGN KEY (source_event_id) REFERENCES webhook_events(event_id)
);
CREATE INDEX IF NOT EXISTS idx_outbox_dispatch ON outbox(state, lease_until, outbox_id);

CREATE TABLE IF NOT EXISTS sink_deliveries (
    dedupe_key TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    message TEXT NOT NULL,
    first_delivered_at REAL NOT NULL,
    delivery_attempts INTEGER NOT NULL
);
"""


class SQLiteStore:
    """A durable store safe for multiple threads in one local process."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._memory_connection = None
        if self.path == ":memory:":
            # A shared in-memory URI needs one connection kept alive.
            self.path = f"file:ledgergate-{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._memory_connection = self._connect(uri=True)
            self._initialize(self._memory_connection)
        else:
            with closing(self._connect()) as connection:
                self._initialize(connection)

    def close(self) -> None:
        if self._memory_connection is not None:
            self._memory_connection.close()
            self._memory_connection = None

    def _connect(self, *, uri: bool | None = None) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=5,
            isolation_level=None,
            uri=self.path.startswith("file:") if uri is None else uri,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        if not self.path.startswith("file:"):
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _initialize(connection: sqlite3.Connection) -> None:
        connection.executescript(SCHEMA)

    def ingest(self, event: Event, raw_payload: bytes, *, now: float | None = None) -> str:
        timestamp = time.time() if now is None else now
        digest = hashlib.sha256(raw_payload).hexdigest()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT payload_sha256 FROM webhook_events WHERE event_id = ?", (event.event_id,)
            ).fetchone()
            if existing:
                result = "duplicate_event" if existing["payload_sha256"] == digest else "event_conflict"
            else:
                same_effect = connection.execute(
                    "SELECT event_id FROM webhook_events WHERE effect_key = ?", (event.effect_key,)
                ).fetchone()
                if same_effect:
                    result = "duplicate_effect"
                else:
                    connection.execute(
                        """INSERT INTO webhook_events (
                            event_id,effect_key,payload_sha256,raw_payload,event_type,payment_id,
                            resource_version,payment_status,amount,currency,event_created,arrival_sequence,state,received_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            event.event_id,
                            event.effect_key,
                            digest,
                            raw_payload,
                            event.event_type,
                            event.payment_id,
                            event.resource_version,
                            event.status,
                            event.amount,
                            event.currency,
                            event.created,
                            connection.execute("SELECT COALESCE(MAX(arrival_sequence),0)+1 FROM webhook_events").fetchone()[0],
                            "pending",
                            timestamp,
                        ),
                    )
                    result = "accepted"
            connection.execute(
                "INSERT INTO ingest_audit(event_id,effect_key,payload_sha256,result,recorded_at) VALUES (?,?,?,?,?)",
                (event.event_id, event.effect_key, digest, result, timestamp),
            )
            connection.commit()
            return result

    def process_one(self, *, now: float | None = None, crash_before_commit: bool = False) -> str | None:
        """Apply one pending event and enqueue its effect atomically."""
        timestamp = time.time() if now is None else now
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            event = connection.execute(
                "SELECT * FROM webhook_events WHERE state = 'pending' ORDER BY arrival_sequence LIMIT 1"
            ).fetchone()
            if event is None:
                connection.rollback()
                return None
            current = connection.execute(
                "SELECT * FROM payments WHERE payment_id = ?", (event["payment_id"],)
            ).fetchone()
            if current is not None and event["resource_version"] < current["resource_version"]:
                state, note = "stale", "older resource version ignored"
            elif current is not None and event["resource_version"] == current["resource_version"]:
                same = (
                    event["payment_status"] == current["status"]
                    and event["amount"] == current["amount"]
                    and event["currency"] == current["currency"]
                )
                state = "stale" if same else "conflict"
                note = "same resource version already projected" if same else "same version contains conflicting state"
            else:
                connection.execute(
                    """INSERT INTO payments(payment_id,resource_version,status,amount,currency,source_event_id,updated_at)
                       VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT(payment_id) DO UPDATE SET
                         resource_version=excluded.resource_version,status=excluded.status,
                         amount=excluded.amount,currency=excluded.currency,
                         source_event_id=excluded.source_event_id,updated_at=excluded.updated_at""",
                    (
                        event["payment_id"],
                        event["resource_version"],
                        event["payment_status"],
                        event["amount"],
                        event["currency"],
                        event["event_id"],
                        timestamp,
                    ),
                )
                state, note = "processed", "latest resource version projected"
                if event["payment_status"] == "succeeded":
                    dedupe_key = f"fulfill:{event['payment_id']}:v{event['resource_version']}"
                    message = json.dumps(
                        {
                            "payment_id": event["payment_id"],
                            "amount": event["amount"],
                            "currency": event["currency"],
                            "source_event_id": event["event_id"],
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    connection.execute(
                        """INSERT OR IGNORE INTO outbox(
                            dedupe_key,source_event_id,topic,message,state,created_at
                        ) VALUES (?,?,?,?,?,?)""",
                        (dedupe_key, event["event_id"], "payment.fulfillment", message, "pending", timestamp),
                    )
            connection.execute(
                "UPDATE webhook_events SET state=?,processed_at=?,note=? WHERE event_id=?",
                (state, timestamp, note, event["event_id"]),
            )
            if crash_before_commit:
                connection.rollback()
                raise RuntimeError("injected crash before inbox transaction commit")
            connection.commit()
            return event["event_id"]

    def process_all(self, *, now: float | None = None, limit: int = 100_000) -> int:
        if type(limit) is not int or not 1 <= limit <= 1_000_000:
            raise ValueError("limit must be an integer in [1, 1000000]")
        processed = 0
        while processed < limit and self.process_one(now=now) is not None:
            processed += 1
        return processed

    def _claim_outbox(self, *, now: float, lease_seconds: float) -> sqlite3.Row | None:
        if not 0 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 0 and 3600")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT * FROM outbox
                   WHERE state='pending' OR (state='inflight' AND lease_until<=?)
                   ORDER BY outbox_id LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                connection.rollback()
                return None
            token = uuid.uuid4().hex
            connection.execute(
                """UPDATE outbox SET state='inflight',lease_token=?,lease_until=?,attempts=attempts+1
                   WHERE outbox_id=?""",
                (token, now + lease_seconds, row["outbox_id"]),
            )
            connection.commit()
            return connection.execute(
                "SELECT * FROM outbox WHERE outbox_id=?", (row["outbox_id"],)
            ).fetchone()

    def dispatch_one(
        self,
        *,
        now: float | None = None,
        lease_seconds: float = 30,
        crash_after_sink: bool = False,
    ) -> str | None:
        """Deliver one outbox row to a receiver with its own dedupe key.

        The included receiver lives in SQLite so the demo can prove crash
        recovery. A real external receiver must enforce the same key.
        """
        timestamp = time.time() if now is None else now
        row = self._claim_outbox(now=timestamp, lease_seconds=lease_seconds)
        if row is None:
            return None
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            previous = connection.execute(
                "SELECT delivery_attempts FROM sink_deliveries WHERE dedupe_key=?", (row["dedupe_key"],)
            ).fetchone()
            if previous is None:
                connection.execute(
                    "INSERT INTO sink_deliveries VALUES (?,?,?,?,?)",
                    (row["dedupe_key"], row["topic"], row["message"], timestamp, 1),
                )
            else:
                connection.execute(
                    "UPDATE sink_deliveries SET delivery_attempts=delivery_attempts+1 WHERE dedupe_key=?",
                    (row["dedupe_key"],),
                )
            connection.commit()
        if crash_after_sink:
            raise RuntimeError("injected crash after sink accepted message")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """UPDATE outbox SET state='delivered',delivered_at=?,lease_token=NULL,lease_until=NULL
                   WHERE outbox_id=? AND state='inflight' AND lease_token=?""",
                (timestamp, row["outbox_id"], row["lease_token"]),
            ).rowcount
            if changed != 1:
                connection.rollback()
                raise RuntimeError("outbox lease was lost before acknowledgement")
            connection.commit()
        return row["dedupe_key"]

    def dispatch_all(self, *, now: float | None = None, lease_seconds: float = 0, limit: int = 100_000) -> int:
        if type(limit) is not int or not 1 <= limit <= 1_000_000:
            raise ValueError("limit must be an integer in [1, 1000000]")
        delivered = 0
        while delivered < limit and self.dispatch_one(now=now, lease_seconds=lease_seconds) is not None:
            delivered += 1
        return delivered

    def payment(self, payment_id: str) -> dict | None:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM payments WHERE payment_id=?", (payment_id,)).fetchone()
            return dict(row) if row else None

    def payments(self) -> list[dict]:
        with closing(self._connect()) as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM payments ORDER BY payment_id")]

    def audit_results(self) -> dict[str, int]:
        with closing(self._connect()) as connection:
            return {row["result"]: row["count"] for row in connection.execute(
                "SELECT result,COUNT(*) AS count FROM ingest_audit GROUP BY result"
            )}

    def snapshot(self) -> dict:
        with closing(self._connect()) as connection:
            def grouped(table: str, field: str) -> dict:
                return {row[field]: row["count"] for row in connection.execute(
                    f"SELECT {field},COUNT(*) AS count FROM {table} GROUP BY {field}"
                )}

            return {
                "ingest": grouped("ingest_audit", "result"),
                "events": grouped("webhook_events", "state"),
                "outbox": grouped("outbox", "state"),
                "payments": connection.execute("SELECT COUNT(*) FROM payments").fetchone()[0],
                "sink_unique_deliveries": connection.execute("SELECT COUNT(*) FROM sink_deliveries").fetchone()[0],
                "sink_delivery_attempts": connection.execute("SELECT COALESCE(SUM(delivery_attempts),0) FROM sink_deliveries").fetchone()[0],
            }
