"""Crash-safe MVCC participant for ChronoCommit's two-phase commit lab."""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

from .errors import ConflictError, IntegrityError, NotFoundError, ValidationError
from .model import (
    Mutation,
    VersionedValue,
    canonical_json,
    digest_json,
    validate_id,
    validate_timestamp,
)


def _secret_bytes(secret: bytes | str) -> bytes:
    value = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValidationError("decision secret must contain at least 32 bytes")
    return value


class MVCCShard:
    """One durable participant with prepared intents and immutable versions."""

    def __init__(self, path: str | Path, shard_id: str, secret: bytes | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.shard_id = validate_id(shard_id, "shard_id")
        self._secret = _secret_bytes(secret)
        self._secret_fingerprint = hashlib.sha256(self._secret).hexdigest()
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
                CREATE TABLE IF NOT EXISTS transactions (
                    txn_id TEXT PRIMARY KEY,
                    tenant TEXT NOT NULL,
                    commit_ts INTEGER NOT NULL CHECK(commit_ts > 0),
                    payload_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('PREPARED', 'COMMITTED', 'ABORTED')),
                    decision_mac TEXT,
                    prepared_ns INTEGER NOT NULL,
                    completed_ns INTEGER
                );
                CREATE TABLE IF NOT EXISTS intents (
                    txn_id TEXT NOT NULL REFERENCES transactions(txn_id),
                    tenant TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    expected_ts INTEGER NOT NULL CHECK(expected_ts >= 0),
                    deleted INTEGER NOT NULL CHECK(deleted IN (0, 1)),
                    value_json TEXT,
                    value_digest TEXT NOT NULL,
                    PRIMARY KEY (txn_id, tenant, item_key),
                    UNIQUE (tenant, item_key),
                    CHECK ((deleted = 1 AND value_json IS NULL) OR
                           (deleted = 0 AND value_json IS NOT NULL))
                );
                CREATE TABLE IF NOT EXISTS versions (
                    tenant TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    commit_ts INTEGER NOT NULL CHECK(commit_ts >= 0),
                    txn_id TEXT NOT NULL,
                    deleted INTEGER NOT NULL CHECK(deleted IN (0, 1)),
                    value_json TEXT,
                    value_digest TEXT NOT NULL,
                    PRIMARY KEY (tenant, item_key, commit_ts),
                    CHECK ((deleted = 1 AND value_json IS NULL) OR
                           (deleted = 0 AND value_json IS NOT NULL))
                );
                CREATE INDEX IF NOT EXISTS versions_lookup
                    ON versions(tenant, item_key, commit_ts DESC);
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
                "schema_version": "1",
                "shard_id": self.shard_id,
                "secret_fingerprint": self._secret_fingerprint,
            }
            for name, value in expected.items():
                row = connection.execute(
                    "SELECT value FROM metadata WHERE name = ?", (name,)
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO metadata(name, value) VALUES(?, ?)", (name, value)
                    )
                elif row["value"] != value:
                    raise IntegrityError(f"shard metadata mismatch for {name}")
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

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

    def sign(self, phase: str, txn_id: str, commit_ts: int, payload_digest: str) -> str:
        material = f"{phase}\n{self.shard_id}\n{txn_id}\n{commit_ts}\n{payload_digest}"
        return hmac.new(self._secret, material.encode("utf-8"), hashlib.sha256).hexdigest()

    def _verify_mac(
        self,
        phase: str,
        txn_id: str,
        commit_ts: int,
        payload_digest: str,
        supplied: str,
    ) -> None:
        expected = self.sign(phase, txn_id, commit_ts, payload_digest)
        if not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
            raise IntegrityError(f"invalid {phase.lower()} authorization for {self.shard_id}")

    @staticmethod
    def _decode_version(row: sqlite3.Row) -> VersionedValue:
        value = None if row["deleted"] else json.loads(row["value_json"])
        if digest_json(value) != row["value_digest"]:
            raise IntegrityError("stored value digest mismatch")
        return VersionedValue(
            key=row["item_key"],
            value=value,
            commit_ts=row["commit_ts"],
            deleted=bool(row["deleted"]),
            txn_id=row["txn_id"],
        )

    def read_at(self, tenant: str, key: str, read_ts: int) -> VersionedValue:
        tenant = validate_id(tenant, "tenant")
        key = validate_id(key, "key")
        validate_timestamp(read_ts, "read_ts")
        connection = self._connect()
        try:
            row = connection.execute(
                """SELECT tenant, item_key, commit_ts, txn_id, deleted,
                          value_json, value_digest
                   FROM versions
                   WHERE tenant = ? AND item_key = ? AND commit_ts <= ?
                   ORDER BY commit_ts DESC LIMIT 1""",
                (tenant, key, read_ts),
            ).fetchone()
            if row is None:
                return VersionedValue(key=key, value=None, commit_ts=0, deleted=True)
            return self._decode_version(row)
        finally:
            connection.close()

    def latest(self, tenant: str, key: str) -> VersionedValue:
        return self.read_at(tenant, key, 9_223_372_036_854_775_807)

    def prepare(
        self,
        *,
        txn_id: str,
        tenant: str,
        commit_ts: int,
        payload_digest: str,
        mutations: Iterable[Mutation],
        authorization: str,
        now_ns: int | None = None,
    ) -> None:
        txn_id = validate_id(txn_id, "txn_id")
        tenant = validate_id(tenant, "tenant")
        validate_timestamp(commit_ts, "commit_ts")
        if commit_ts == 0:
            raise ValidationError("commit_ts must be positive")
        if not isinstance(payload_digest, str) or len(payload_digest) != 64:
            raise ValidationError("payload_digest must be a SHA-256 hex digest")
        prepared = tuple(m.validated() for m in mutations)
        if not prepared:
            raise ValidationError("a participant requires at least one mutation")
        if len({m.key for m in prepared}) != len(prepared):
            raise ValidationError("duplicate participant mutation")
        self._verify_mac("PREPARE", txn_id, commit_ts, payload_digest, authorization)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (txn_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["tenant"] != tenant
                    or existing["commit_ts"] != commit_ts
                    or existing["payload_digest"] != payload_digest
                ):
                    raise IntegrityError("transaction replay does not match prepared record")
                if existing["state"] in {"PREPARED", "COMMITTED"}:
                    connection.rollback()
                    return
                raise ConflictError("transaction was already aborted on this participant")

            for mutation in prepared:
                current = connection.execute(
                    """SELECT commit_ts FROM versions
                       WHERE tenant = ? AND item_key = ?
                       ORDER BY commit_ts DESC LIMIT 1""",
                    (tenant, mutation.key),
                ).fetchone()
                current_ts = current["commit_ts"] if current else 0
                if current_ts != mutation.expected_ts:
                    raise ConflictError(
                        f"{mutation.key} expected version {mutation.expected_ts}, current version is {current_ts}"
                    )
                if commit_ts <= current_ts:
                    raise IntegrityError("commit timestamp must exceed every overwritten version")

            connection.execute(
                """INSERT INTO transactions(
                       txn_id, tenant, commit_ts, payload_digest, state, prepared_ns
                   ) VALUES(?, ?, ?, ?, 'PREPARED', ?)""",
                (txn_id, tenant, commit_ts, payload_digest, occurred_ns),
            )
            for mutation in prepared:
                value_json = None if mutation.delete else canonical_json(mutation.value)
                value_digest = digest_json(None if mutation.delete else mutation.value)
                try:
                    connection.execute(
                        """INSERT INTO intents(
                               txn_id, tenant, item_key, expected_ts, deleted,
                               value_json, value_digest
                           ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
                        (
                            txn_id,
                            tenant,
                            mutation.key,
                            mutation.expected_ts,
                            int(mutation.delete),
                            value_json,
                            value_digest,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ConflictError(f"{mutation.key} is locked by another transaction") from exc
            self._audit(
                connection,
                "PREPARE",
                txn_id,
                {"commit_ts": commit_ts, "keys": sorted(m.key for m in prepared)},
                occurred_ns,
            )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def commit(
        self,
        *,
        txn_id: str,
        commit_ts: int,
        payload_digest: str,
        authorization: str,
        now_ns: int | None = None,
    ) -> None:
        txn_id = validate_id(txn_id, "txn_id")
        validate_timestamp(commit_ts, "commit_ts")
        self._verify_mac("COMMIT", txn_id, commit_ts, payload_digest, authorization)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            transaction = connection.execute(
                "SELECT * FROM transactions WHERE txn_id = ?", (txn_id,)
            ).fetchone()
            if transaction is None:
                raise NotFoundError("participant has no prepared transaction")
            if (
                transaction["commit_ts"] != commit_ts
                or transaction["payload_digest"] != payload_digest
            ):
                raise IntegrityError("commit decision does not match prepared transaction")
            if transaction["state"] == "COMMITTED":
                if not hmac.compare_digest(transaction["decision_mac"], authorization):
                    raise IntegrityError("stored decision authorization mismatch")
                connection.rollback()
                return
            if transaction["state"] != "PREPARED":
                raise ConflictError("cannot commit an aborted participant")
            intents = connection.execute(
                "SELECT * FROM intents WHERE txn_id = ? ORDER BY item_key", (txn_id,)
            ).fetchall()
            if not intents:
                raise IntegrityError("prepared transaction has no intents")
            for intent in intents:
                value = None if intent["deleted"] else json.loads(intent["value_json"])
                if digest_json(value) != intent["value_digest"]:
                    raise IntegrityError("prepared intent digest mismatch")
                connection.execute(
                    """INSERT INTO versions(
                           tenant, item_key, commit_ts, txn_id, deleted,
                           value_json, value_digest
                       ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    (
                        intent["tenant"],
                        intent["item_key"],
                        commit_ts,
                        txn_id,
                        intent["deleted"],
                        intent["value_json"],
                        intent["value_digest"],
                    ),
                )
            connection.execute("DELETE FROM intents WHERE txn_id = ?", (txn_id,))
            connection.execute(
                """UPDATE transactions SET state = 'COMMITTED', decision_mac = ?,
                       completed_ns = ? WHERE txn_id = ?""",
                (authorization, occurred_ns, txn_id),
            )
            self._audit(
                connection,
                "COMMIT",
                txn_id,
                {"commit_ts": commit_ts, "version_count": len(intents)},
                occurred_ns,
            )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def abort(self, txn_id: str, *, now_ns: int | None = None) -> None:
        txn_id = validate_id(txn_id, "txn_id")
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns, "now_ns")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            transaction = connection.execute(
                "SELECT state FROM transactions WHERE txn_id = ?", (txn_id,)
            ).fetchone()
            if transaction is None:
                connection.rollback()
                return
            if transaction["state"] == "COMMITTED":
                raise IntegrityError("a committed participant cannot be aborted")
            if transaction["state"] == "ABORTED":
                connection.rollback()
                return
            connection.execute("DELETE FROM intents WHERE txn_id = ?", (txn_id,))
            connection.execute(
                "UPDATE transactions SET state = 'ABORTED', completed_ns = ? WHERE txn_id = ?",
                (occurred_ns, txn_id),
            )
            self._audit(connection, "ABORT", txn_id, {}, occurred_ns)
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def transaction_state(self, txn_id: str) -> str | None:
        txn_id = validate_id(txn_id, "txn_id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT state FROM transactions WHERE txn_id = ?", (txn_id,)
            ).fetchone()
            return None if row is None else row["state"]
        finally:
            connection.close()

    def version_count(self) -> int:
        connection = self._connect()
        try:
            return int(connection.execute("SELECT COUNT(*) FROM versions").fetchone()[0])
        finally:
            connection.close()

    def verify_integrity(self) -> dict[str, int]:
        connection = self._connect()
        try:
            version_rows = connection.execute(
                "SELECT * FROM versions ORDER BY tenant, item_key, commit_ts"
            ).fetchall()
            for row in version_rows:
                self._decode_version(row)
                txn = connection.execute(
                    "SELECT state FROM transactions WHERE txn_id = ?", (row["txn_id"],)
                ).fetchone()
                if txn is None or txn["state"] != "COMMITTED":
                    raise IntegrityError("visible version does not belong to a committed transaction")

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
            intents = connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0]
            prepared = connection.execute(
                "SELECT COUNT(*) FROM transactions WHERE state = 'PREPARED'"
            ).fetchone()[0]
            return {
                "audit_entries": len(audits),
                "intents": int(intents),
                "prepared": int(prepared),
                "versions": len(version_rows),
            }
        finally:
            connection.close()

