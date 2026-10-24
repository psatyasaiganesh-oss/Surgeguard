"""Durable transaction coordinator with recovery and uncertainty-aware ACKs."""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Iterator

try:  # POSIX in production/demo containers; fallback keeps import portable.
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]

from .clock import BoundedClock, SystemBoundedClock
from .errors import (
    ChronoCommitError,
    ClockUnsafeError,
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    SimulatedCrash,
    TransactionAborted,
    ValidationError,
)
from .model import (
    CommitResult,
    Mutation,
    VersionedValue,
    canonical_json,
    digest_json,
    validate_id,
    validate_mutations,
    validate_timestamp,
)
from .shard import MVCCShard, _secret_bytes


VALID_FAULTS = {None, "after_decision", "after_commit_record"}


class Coordinator:
    """Coordinates transactions across local SQLite-backed participants.

    Separate participant databases make prepare/decision/finalize boundaries
    observable. A local file lock serializes the coordinator, intentionally
    avoiding any claim that this lab is a replicated production control plane.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        shard_count: int = 3,
        secret: bytes | str,
        clock: BoundedClock | None = None,
        recover_on_start: bool = True,
    ) -> None:
        if isinstance(shard_count, bool) or not isinstance(shard_count, int):
            raise ValidationError("shard_count must be an integer")
        if shard_count < 2 or shard_count > 16:
            raise ValidationError("shard_count must be between 2 and 16")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.db_path = self.directory / "coordinator.sqlite"
        self.lock_path = self.directory / "coordinator.lock"
        self.shard_count = shard_count
        self._secret = _secret_bytes(secret)
        self._secret_fingerprint = hashlib.sha256(self._secret).hexdigest()
        self.clock = clock or SystemBoundedClock()
        self._thread_lock = threading.RLock()
        self._initialize()
        self.shards = tuple(
            MVCCShard(
                self.directory / f"shard-{index:02d}.sqlite",
                f"shard-{index:02d}",
                self._secret,
            )
            for index in range(shard_count)
        )
        if recover_on_start:
            self.recover()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
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
                CREATE TABLE IF NOT EXISTS transactions (
                    txn_id TEXT PRIMARY KEY,
                    tenant TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN
                        ('PREPARING', 'COMMITTING', 'COMMITTED', 'ABORTED')),
                    commit_ts INTEGER NOT NULL CHECK(commit_ts > 0),
                    participants_json TEXT NOT NULL,
                    error_code TEXT,
                    error_message TEXT,
                    created_ns INTEGER NOT NULL,
                    decision_ns INTEGER,
                    acknowledged_ns INTEGER,
                    UNIQUE (tenant, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS transactions_recovery
                    ON transactions(state, created_ns);
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
            expected = {
                "last_clock_latest_ns": "0",
                "last_commit_ts": "0",
                "last_committed_ts": "0",
                "schema_version": "1",
                "secret_fingerprint": self._secret_fingerprint,
                "shard_count": str(self.shard_count),
            }
            for name, value in expected.items():
                row = connection.execute(
                    "SELECT value FROM metadata WHERE name = ?", (name,)
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO metadata(name, value) VALUES(?, ?)", (name, value)
                    )
                elif name in {"last_clock_latest_ns", "last_commit_ts", "last_committed_ts"}:
                    continue
                elif row["value"] != value:
                    raise IntegrityError(f"coordinator metadata mismatch for {name}")
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @contextlib.contextmanager
    def _exclusive(self) -> Iterator[None]:
        with self._thread_lock:
            descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                if fcntl is not None:
                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

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
    def _metadata_int(connection: sqlite3.Connection, name: str) -> int:
        row = connection.execute(
            "SELECT value FROM metadata WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise IntegrityError(f"missing coordinator metadata {name}")
        try:
            return int(row["value"])
        except ValueError as exc:
            raise IntegrityError(f"invalid coordinator metadata {name}") from exc

    def shard_index(self, tenant: str, key: str) -> int:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        material = f"{tenant}\0{key}".encode("utf-8")
        return int.from_bytes(hashlib.sha256(material).digest()[:8], "big") % self.shard_count

    def shard_for(self, tenant: str, key: str) -> MVCCShard:
        return self.shards[self.shard_index(tenant, key)]

    def _participants_for(
        self, tenant: str, mutations: tuple[Mutation, ...]
    ) -> dict[int, tuple[Mutation, ...]]:
        grouped: dict[int, list[Mutation]] = {}
        for mutation in mutations:
            grouped.setdefault(self.shard_index(tenant, mutation.key), []).append(mutation)
        return {index: tuple(items) for index, items in sorted(grouped.items())}

    @staticmethod
    def _parse_fault(fault: str | None) -> tuple[str | None, int | None]:
        if fault in VALID_FAULTS:
            return fault, None
        if isinstance(fault, str) and ":" in fault:
            phase, count_text = fault.split(":", 1)
            if phase in {"after_prepare", "after_commit"}:
                try:
                    count = int(count_text)
                except ValueError as exc:
                    raise ValidationError("fault count must be an integer") from exc
                if count >= 1:
                    return phase, count
        raise ValidationError("unsupported fault-injection point")

    def _assign_transaction(
        self,
        tenant: str,
        idempotency_key: str,
        payload: dict[str, Any],
        payload_digest: str,
        request_digest: str,
        participants: dict[int, tuple[Mutation, ...]],
    ) -> sqlite3.Row:
        interval = self.clock.now()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            previous_clock = self._metadata_int(connection, "last_clock_latest_ns")
            if interval.latest_ns < previous_clock:
                raise ClockUnsafeError(
                    "clock uncertainty interval regressed behind a persisted observation"
                )
            previous_commit = self._metadata_int(connection, "last_commit_ts")
            commit_ts = max(interval.latest_ns, previous_commit + 1)
            validate_timestamp(commit_ts, "commit_ts")
            txn_id = f"txn-{uuid.uuid4().hex}"
            created_ns = time.time_ns()
            participant_ids = [self.shards[index].shard_id for index in participants]
            connection.execute(
                """INSERT INTO transactions(
                       txn_id, tenant, idempotency_key, payload_json, payload_digest,
                       request_digest, state, commit_ts, participants_json, created_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, 'PREPARING', ?, ?, ?)""",
                (
                    txn_id,
                    tenant,
                    idempotency_key,
                    canonical_json(payload),
                    payload_digest,
                    request_digest,
                    commit_ts,
                    canonical_json(participant_ids),
                    created_ns,
                ),
            )
            connection.execute(
                "UPDATE metadata SET value = ? WHERE name = 'last_clock_latest_ns'",
                (str(interval.latest_ns),),
            )
            connection.execute(
                "UPDATE metadata SET value = ? WHERE name = 'last_commit_ts'",
                (str(commit_ts),),
            )
            self._audit(
                connection,
                "BEGIN",
                txn_id,
                {
                    "commit_ts": commit_ts,
                    "idempotency_key": idempotency_key,
                    "participants": participant_ids,
                    "uncertainty_ns": interval.uncertainty_ns,
                },
                created_ns,
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (txn_id,)
            ).fetchone()
            assert row is not None
            return row
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _existing(self, tenant: str, idempotency_key: str) -> sqlite3.Row | None:
        connection = self._connect()
        try:
            return connection.execute(
                "SELECT * FROM transactions WHERE tenant = ? AND idempotency_key = ?",
                (tenant, idempotency_key),
            ).fetchone()
        finally:
            connection.close()

    def _mark_aborted(self, row: sqlite3.Row, error: Exception) -> None:
        for shard_id in json.loads(row["participants_json"]):
            self._shard_by_id(shard_id).abort(row["txn_id"])
        code = error.code if isinstance(error, ChronoCommitError) else "prepare_failed"
        message = str(error)[:500] or error.__class__.__name__
        occurred_ns = time.time_ns()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT state FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            if current is None:
                raise IntegrityError("coordinator transaction disappeared")
            if current["state"] == "COMMITTING":
                raise IntegrityError("cannot abort after the durable commit decision")
            if current["state"] != "ABORTED":
                connection.execute(
                    """UPDATE transactions SET state = 'ABORTED', error_code = ?,
                           error_message = ?, decision_ns = ? WHERE txn_id = ?""",
                    (code, message, occurred_ns, row["txn_id"]),
                )
                self._audit(
                    connection,
                    "ABORT",
                    row["txn_id"],
                    {"error_code": code, "message": message},
                    occurred_ns,
                )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _record_commit_decision(self, row: sqlite3.Row) -> sqlite3.Row:
        occurred_ns = time.time_ns()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            if current is None:
                raise IntegrityError("coordinator transaction disappeared")
            if current["state"] == "PREPARING":
                connection.execute(
                    """UPDATE transactions SET state = 'COMMITTING', decision_ns = ?
                       WHERE txn_id = ?""",
                    (occurred_ns, row["txn_id"]),
                )
                self._audit(
                    connection,
                    "COMMIT_DECISION",
                    row["txn_id"],
                    {
                        "commit_ts": row["commit_ts"],
                        "participants": json.loads(row["participants_json"]),
                    },
                    occurred_ns,
                )
            elif current["state"] not in {"COMMITTING", "COMMITTED"}:
                raise IntegrityError("invalid coordinator state for commit decision")
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            assert updated is not None
            return updated
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _finalize_participants(
        self,
        row: sqlite3.Row,
        *,
        fault_phase: str | None = None,
        fault_count: int | None = None,
    ) -> None:
        for completed, shard_id in enumerate(json.loads(row["participants_json"]), start=1):
            shard = self._shard_by_id(shard_id)
            authorization = shard.sign(
                "COMMIT", row["txn_id"], row["commit_ts"], row["payload_digest"]
            )
            shard.commit(
                txn_id=row["txn_id"],
                commit_ts=row["commit_ts"],
                payload_digest=row["payload_digest"],
                authorization=authorization,
            )
            if fault_phase == "after_commit" and completed == fault_count:
                raise SimulatedCrash(f"crashed after committing {completed} participant(s)")

    def _record_committed(self, row: sqlite3.Row) -> sqlite3.Row:
        occurred_ns = time.time_ns()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            if current is None:
                raise IntegrityError("coordinator transaction disappeared")
            if current["state"] == "COMMITTING":
                connection.execute(
                    "UPDATE transactions SET state = 'COMMITTED' WHERE txn_id = ?",
                    (row["txn_id"],),
                )
                previous = self._metadata_int(connection, "last_committed_ts")
                connection.execute(
                    "UPDATE metadata SET value = ? WHERE name = 'last_committed_ts'",
                    (str(max(previous, row["commit_ts"])),),
                )
                self._audit(
                    connection,
                    "PARTICIPANTS_COMMITTED",
                    row["txn_id"],
                    {"commit_ts": row["commit_ts"]},
                    occurred_ns,
                )
            elif current["state"] != "COMMITTED":
                raise IntegrityError("invalid coordinator state after participant commit")
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            assert updated is not None
            return updated
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _acknowledge(self, row: sqlite3.Row) -> sqlite3.Row:
        if row["acknowledged_ns"] is not None:
            return row
        earliest = self.clock.wait_until_after(row["commit_ts"])
        if earliest <= row["commit_ts"]:
            raise ClockUnsafeError("clock acknowledged before commit uncertainty elapsed")
        occurred_ns = time.time_ns()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            if current is None or current["state"] != "COMMITTED":
                raise IntegrityError("only committed transactions can be acknowledged")
            if current["acknowledged_ns"] is None:
                connection.execute(
                    "UPDATE transactions SET acknowledged_ns = ? WHERE txn_id = ?",
                    (earliest, row["txn_id"]),
                )
                self._audit(
                    connection,
                    "ACKNOWLEDGE",
                    row["txn_id"],
                    {"commit_ts": row["commit_ts"], "safe_earliest_ns": earliest},
                    occurred_ns,
                )
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (row["txn_id"],)
            ).fetchone()
            assert updated is not None
            return updated
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _shard_by_id(self, shard_id: str) -> MVCCShard:
        for shard in self.shards:
            if shard.shard_id == shard_id:
                return shard
        raise IntegrityError(f"unknown participant {shard_id}")

    @staticmethod
    def _result(row: sqlite3.Row, replayed: bool) -> CommitResult:
        if row["state"] != "COMMITTED" or row["acknowledged_ns"] is None:
            raise IntegrityError("transaction is not safely acknowledged")
        return CommitResult(
            txn_id=row["txn_id"],
            tenant=row["tenant"],
            idempotency_key=row["idempotency_key"],
            commit_ts=row["commit_ts"],
            participant_count=len(json.loads(row["participants_json"])),
            replayed=replayed,
            acknowledged_ns=row["acknowledged_ns"],
        )

    def execute(
        self,
        tenant: str,
        idempotency_key: str,
        mutations: Iterable[Mutation],
        *,
        fault: str | None = None,
        request_identity: Any | None = None,
    ) -> CommitResult:
        tenant = validate_id(tenant, "tenant")
        idempotency_key = validate_id(idempotency_key, "idempotency_key")
        validated = validate_mutations(mutations)
        payload = {
            "mutations": [mutation.as_dict() for mutation in validated],
            "tenant": tenant,
        }
        payload_digest = digest_json(payload)
        request_digest = (
            payload_digest
            if request_identity is None
            else digest_json({"request_identity": request_identity, "tenant": tenant})
        )
        fault_phase, fault_count = self._parse_fault(fault)
        participants = self._participants_for(tenant, validated)

        with self._exclusive():
            self._recover_locked()
            existing = self._existing(tenant, idempotency_key)
            if existing is not None:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise IdempotencyConflict(
                        "idempotency key was already used with a different transaction"
                    )
                if existing["state"] == "ABORTED":
                    raise TransactionAborted(
                        existing["error_message"] or "transaction was previously aborted"
                    )
                existing = self._acknowledge(existing)
                return self._result(existing, replayed=True)

            row = self._assign_transaction(
                tenant,
                idempotency_key,
                payload,
                payload_digest,
                request_digest,
                participants,
            )
            prepared_count = 0
            try:
                for index, shard_mutations in participants.items():
                    shard = self.shards[index]
                    authorization = shard.sign(
                        "PREPARE", row["txn_id"], row["commit_ts"], payload_digest
                    )
                    shard.prepare(
                        txn_id=row["txn_id"],
                        tenant=tenant,
                        commit_ts=row["commit_ts"],
                        payload_digest=payload_digest,
                        mutations=shard_mutations,
                        authorization=authorization,
                    )
                    prepared_count += 1
                    if fault_phase == "after_prepare" and prepared_count == fault_count:
                        raise SimulatedCrash(
                            f"crashed after preparing {prepared_count} participant(s)"
                        )
            except SimulatedCrash:
                raise
            except Exception as exc:
                self._mark_aborted(row, exc)
                if isinstance(exc, ChronoCommitError):
                    raise
                raise IntegrityError("participant prepare failed") from exc

            row = self._record_commit_decision(row)
            if fault_phase == "after_decision":
                raise SimulatedCrash("crashed after recording the durable commit decision")
            self._finalize_participants(
                row, fault_phase=fault_phase, fault_count=fault_count
            )
            row = self._record_committed(row)
            if fault_phase == "after_commit_record":
                raise SimulatedCrash("crashed before commit wait and acknowledgement")
            row = self._acknowledge(row)
            return self._result(row, replayed=False)

    def _recover_locked(self) -> dict[str, int]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """SELECT * FROM transactions
                   WHERE state IN ('PREPARING', 'COMMITTING', 'COMMITTED')
                   ORDER BY created_ns"""
            ).fetchall()
        finally:
            connection.close()
        recovered = {"aborted": 0, "acknowledged": 0, "committed": 0}
        for row in rows:
            if row["state"] == "PREPARING":
                self._mark_aborted(
                    row,
                    TransactionAborted("recovery aborted a transaction with no commit decision"),
                )
                recovered["aborted"] += 1
            elif row["state"] == "COMMITTING":
                self._finalize_participants(row)
                row = self._record_committed(row)
                row = self._acknowledge(row)
                recovered["committed"] += 1
                recovered["acknowledged"] += 1
            elif row["acknowledged_ns"] is None:
                self._acknowledge(row)
                recovered["acknowledged"] += 1
        return recovered

    def recover(self) -> dict[str, int]:
        with self._exclusive():
            return self._recover_locked()

    def snapshot(
        self,
        tenant: str,
        keys: Iterable[str],
        *,
        read_ts: int | None = None,
    ) -> dict[str, VersionedValue]:
        tenant = validate_id(tenant, "tenant")
        requested = tuple(validate_id(key, "key") for key in keys)
        if not requested:
            raise ValidationError("snapshot requires at least one key")
        if len(requested) != len(set(requested)):
            raise ValidationError("snapshot keys must be unique")
        if len(requested) > 128:
            raise ValidationError("snapshot supports at most 128 keys")
        with self._exclusive():
            self._recover_locked()
            if read_ts is None:
                interval = self.clock.now()
                connection = self._connect()
                try:
                    last_committed = self._metadata_int(connection, "last_committed_ts")
                finally:
                    connection.close()
                chosen_ts = max(interval.latest_ns, last_committed)
            else:
                chosen_ts = validate_timestamp(read_ts, "read_ts")
            return {
                key: self.shard_for(tenant, key).read_at(tenant, key, chosen_ts)
                for key in requested
            }

    def transaction_status(self, tenant: str, idempotency_key: str) -> dict[str, Any] | None:
        tenant = validate_id(tenant, "tenant")
        idempotency_key = validate_id(idempotency_key, "idempotency_key")
        with self._exclusive():
            self._recover_locked()
            row = self._existing(tenant, idempotency_key)
            if row is None:
                return None
            return {
                "acknowledged_ns": row["acknowledged_ns"],
                "commit_ts": row["commit_ts"],
                "error_code": row["error_code"],
                "idempotency_key": row["idempotency_key"],
                "participant_count": len(json.loads(row["participants_json"])),
                "state": row["state"],
                "tenant": row["tenant"],
                "txn_id": row["txn_id"],
            }

    def replay_if_exists(
        self, tenant: str, idempotency_key: str, request_identity: Any
    ) -> CommitResult | None:
        """Return a safe replay before a caller recomputes state-dependent writes."""

        tenant = validate_id(tenant, "tenant")
        idempotency_key = validate_id(idempotency_key, "idempotency_key")
        request_digest = digest_json(
            {"request_identity": request_identity, "tenant": tenant}
        )
        with self._exclusive():
            self._recover_locked()
            row = self._existing(tenant, idempotency_key)
            if row is None:
                return None
            if not hmac.compare_digest(row["request_digest"], request_digest):
                raise IdempotencyConflict(
                    "idempotency key was already used with a different transaction"
                )
            if row["state"] == "ABORTED":
                raise TransactionAborted(
                    row["error_message"] or "transaction was previously aborted"
                )
            row = self._acknowledge(row)
            return self._result(row, replayed=True)

    def verify_integrity(self) -> dict[str, Any]:
        with self._exclusive():
            self._recover_locked()
            connection = self._connect()
            try:
                previous = "0" * 64
                audits = connection.execute(
                    "SELECT * FROM audit_log ORDER BY sequence"
                ).fetchall()
                for row in audits:
                    if row["previous_hash"] != previous:
                        raise IntegrityError("coordinator audit chain previous hash mismatch")
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
                        raise IntegrityError("coordinator audit entry hash mismatch")
                    previous = row["entry_hash"]

                transactions = connection.execute(
                    "SELECT * FROM transactions ORDER BY created_ns"
                ).fetchall()
                for transaction in transactions:
                    for shard_id in json.loads(transaction["participants_json"]):
                        state = self._shard_by_id(shard_id).transaction_state(
                            transaction["txn_id"]
                        )
                        if transaction["state"] == "COMMITTED" and state != "COMMITTED":
                            raise IntegrityError("committed coordinator has unfinished participant")
                        if transaction["state"] == "ABORTED" and state == "PREPARED":
                            raise IntegrityError("aborted coordinator left a prepared participant")
                metadata = {
                    name: self._metadata_int(connection, name)
                    for name in (
                        "last_clock_latest_ns",
                        "last_commit_ts",
                        "last_committed_ts",
                    )
                }
            finally:
                connection.close()
            shard_reports = {
                shard.shard_id: shard.verify_integrity() for shard in self.shards
            }
            return {
                "audit_entries": len(audits),
                "metadata": metadata,
                "shards": shard_reports,
                "transactions": len(transactions),
            }

