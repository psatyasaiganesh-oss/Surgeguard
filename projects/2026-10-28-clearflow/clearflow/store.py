from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    NotFoundError,
    StateError,
    ValidationError,
)
from .model import (
    MoneyEvent,
    QualityPolicy,
    canonical_json,
    digest,
    normalize_references,
    require_currency,
    require_identifier,
    require_int,
    require_timestamp,
    validate_account_kind,
    validate_actor,
)


ZERO_HASH = "0" * 64
SCHEMA_VERSION = "1"


class ClearFlowStore:
    def __init__(self, path: str | Path, *, secret: bytes) -> None:
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValidationError("secret must contain at least 32 bytes")
        self.path = str(path)
        self.secret = secret
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tenant_state (
                    tenant TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
                    event_head_hash TEXT NOT NULL,
                    audit_sequence INTEGER NOT NULL DEFAULT 0 CHECK (audit_sequence >= 0),
                    audit_head_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS accounts (
                    tenant TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK (kind IN ('clearing', 'terminal')),
                    currency TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    PRIMARY KEY (tenant, account_id),
                    FOREIGN KEY (tenant) REFERENCES tenant_state(tenant)
                );
                CREATE TABLE IF NOT EXISTS events (
                    tenant TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    event_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    producer TEXT NOT NULL,
                    reference_id TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    occurred_ns INTEGER NOT NULL CHECK (occurred_ns >= 0),
                    received_ns INTEGER NOT NULL CHECK (received_ns >= 0),
                    body_json TEXT NOT NULL,
                    body_digest TEXT NOT NULL,
                    correction_of TEXT,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    PRIMARY KEY (tenant, event_id),
                    UNIQUE (tenant, sequence),
                    UNIQUE (tenant, idempotency_key),
                    UNIQUE (tenant, event_hash),
                    FOREIGN KEY (tenant) REFERENCES tenant_state(tenant),
                    FOREIGN KEY (tenant, correction_of) REFERENCES events(tenant, event_id)
                );
                CREATE INDEX IF NOT EXISTS events_reference
                    ON events(tenant, producer, reference_id, received_ns);
                CREATE TABLE IF NOT EXISTS postings (
                    tenant TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    line_no INTEGER NOT NULL CHECK (line_no >= 0),
                    account_id TEXT NOT NULL,
                    amount_minor INTEGER NOT NULL CHECK (amount_minor != 0),
                    PRIMARY KEY (tenant, event_id, line_no),
                    UNIQUE (tenant, event_id, account_id),
                    FOREIGN KEY (tenant, event_id) REFERENCES events(tenant, event_id),
                    FOREIGN KEY (tenant, account_id) REFERENCES accounts(tenant, account_id)
                );
                CREATE INDEX IF NOT EXISTS postings_account
                    ON postings(tenant, account_id);
                CREATE TABLE IF NOT EXISTS manifests (
                    tenant TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    producer TEXT NOT NULL,
                    expected_json TEXT NOT NULL,
                    manifest_digest TEXT NOT NULL,
                    deadline_ns INTEGER NOT NULL CHECK (deadline_ns >= 0),
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    PRIMARY KEY (tenant, batch_id),
                    UNIQUE (tenant, idempotency_key),
                    FOREIGN KEY (tenant) REFERENCES tenant_state(tenant)
                );
                CREATE TABLE IF NOT EXISTS quality_runs (
                    tenant TEXT NOT NULL,
                    evaluation_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    snapshot_revision INTEGER NOT NULL,
                    as_of_ns INTEGER NOT NULL CHECK (as_of_ns >= 0),
                    policy_json TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    report_digest TEXT NOT NULL,
                    attestation_json TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    PRIMARY KEY (tenant, evaluation_id),
                    UNIQUE (tenant, idempotency_key),
                    FOREIGN KEY (tenant) REFERENCES tenant_state(tenant)
                );
                CREATE TABLE IF NOT EXISTS correction_proposals (
                    tenant TEXT NOT NULL,
                    proposal_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    target_event_id TEXT NOT NULL,
                    replacement_json TEXT NOT NULL,
                    base_revision INTEGER NOT NULL,
                    preview_json TEXT NOT NULL,
                    preview_digest TEXT NOT NULL,
                    creator TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('PROPOSED', 'APPROVED', 'APPLIED')),
                    execute_key TEXT,
                    execute_digest TEXT,
                    result_json TEXT,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    applied_ns INTEGER,
                    PRIMARY KEY (tenant, proposal_id),
                    UNIQUE (tenant, idempotency_key),
                    FOREIGN KEY (tenant, target_event_id) REFERENCES events(tenant, event_id),
                    FOREIGN KEY (tenant) REFERENCES tenant_state(tenant)
                );
                CREATE TABLE IF NOT EXISTS correction_approvals (
                    tenant TEXT NOT NULL,
                    proposal_id TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    preview_digest TEXT NOT NULL,
                    approved_ns INTEGER NOT NULL CHECK (approved_ns >= 0),
                    PRIMARY KEY (tenant, proposal_id, actor),
                    FOREIGN KEY (tenant, proposal_id)
                        REFERENCES correction_proposals(tenant, proposal_id)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    tenant TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    action TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL,
                    PRIMARY KEY (tenant, sequence),
                    UNIQUE (tenant, entry_hash),
                    FOREIGN KEY (tenant) REFERENCES tenant_state(tenant)
                );
                CREATE TRIGGER IF NOT EXISTS immutable_events_update
                BEFORE UPDATE ON events BEGIN
                    SELECT RAISE(ABORT, 'events are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS immutable_events_delete
                BEFORE DELETE ON events BEGIN
                    SELECT RAISE(ABORT, 'events are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS immutable_postings_update
                BEFORE UPDATE ON postings BEGIN
                    SELECT RAISE(ABORT, 'postings are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS immutable_postings_delete
                BEFORE DELETE ON postings BEGIN
                    SELECT RAISE(ABORT, 'postings are immutable');
                END;
                """
            )
            fingerprint = hmac.new(
                key=self.secret, msg=b"clearflow-key-check", digestmod=hashlib.sha256
            ).hexdigest()
            existing = connection.execute(
                "SELECT value FROM metadata WHERE key = 'secret_fingerprint'"
            ).fetchone()
            if existing is not None and not hmac.compare_digest(existing["value"], fingerprint):
                raise IntegrityError("database was created with a different signing secret")
            connection.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
            connection.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES ('secret_fingerprint', ?)",
                (fingerprint,),
            )

    @staticmethod
    def _now(value: int | None) -> int:
        return time.time_ns() if value is None else require_timestamp(value, "now_ns")

    @staticmethod
    def _tenant(connection: sqlite3.Connection, tenant: str) -> sqlite3.Row:
        connection.execute(
            "INSERT OR IGNORE INTO tenant_state(tenant, event_head_hash, audit_head_hash) VALUES (?, ?, ?)",
            (tenant, ZERO_HASH, ZERO_HASH),
        )
        row = connection.execute(
            "SELECT * FROM tenant_state WHERE tenant = ?", (tenant,)
        ).fetchone()
        assert row is not None
        return row

    def _sign(self, payload: dict[str, Any]) -> str:
        return hmac.new(self.secret, canonical_json(payload).encode(), hashlib.sha256).hexdigest()

    def _chain_hash(self, payload: dict[str, Any]) -> str:
        return hmac.new(self.secret, canonical_json(payload).encode(), hashlib.sha256).hexdigest()

    def _audit(
        self,
        connection: sqlite3.Connection,
        tenant: str,
        action: str,
        entity_id: str,
        payload: Any,
        now_ns: int,
    ) -> None:
        state = self._tenant(connection, tenant)
        sequence = state["audit_sequence"] + 1
        payload_digest = digest(payload)
        material = {
            "tenant": tenant,
            "sequence": sequence,
            "action": action,
            "entity_id": entity_id,
            "payload_digest": payload_digest,
            "created_ns": now_ns,
            "previous_hash": state["audit_head_hash"],
        }
        entry_hash = self._chain_hash(material)
        connection.execute(
            """INSERT INTO audit_log(
                tenant, sequence, action, entity_id, payload_digest, created_ns,
                previous_hash, entry_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                tenant,
                sequence,
                action,
                entity_id,
                payload_digest,
                now_ns,
                state["audit_head_hash"],
                entry_hash,
            ),
        )
        connection.execute(
            "UPDATE tenant_state SET audit_sequence = ?, audit_head_hash = ? WHERE tenant = ?",
            (sequence, entry_hash, tenant),
        )

    def create_account(
        self,
        tenant: str,
        account_id: str,
        kind: str,
        currency: str,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        account_id = require_identifier(account_id, "account_id")
        kind = validate_account_kind(kind)
        currency = require_currency(currency)
        now = self._now(now_ns)
        with self._transaction() as connection:
            self._tenant(connection, tenant)
            existing = connection.execute(
                "SELECT * FROM accounts WHERE tenant = ? AND account_id = ?",
                (tenant, account_id),
            ).fetchone()
            if existing:
                if existing["kind"] != kind or existing["currency"] != currency:
                    raise ConflictError("account already exists with a different definition")
                return {**dict(existing), "replayed": True}
            connection.execute(
                "INSERT INTO accounts VALUES (?, ?, ?, ?, ?)",
                (tenant, account_id, kind, currency, now),
            )
            self._audit(
                connection,
                tenant,
                "ACCOUNT_CREATED",
                account_id,
                {"kind": kind, "currency": currency},
                now,
            )
            return {
                "tenant": tenant,
                "account_id": account_id,
                "kind": kind,
                "currency": currency,
                "created_ns": now,
                "replayed": False,
            }

    def _validate_event_accounts(
        self, connection: sqlite3.Connection, tenant: str, event: MoneyEvent
    ) -> None:
        placeholders = ",".join("?" for _ in event.postings)
        rows = connection.execute(
            f"SELECT account_id, currency FROM accounts WHERE tenant = ? AND account_id IN ({placeholders})",
            (tenant, *(item.account_id for item in event.postings)),
        ).fetchall()
        found = {row["account_id"]: row["currency"] for row in rows}
        missing = sorted(item.account_id for item in event.postings if item.account_id not in found)
        if missing:
            raise ValidationError(f"unknown accounts: {', '.join(missing)}")
        wrong = sorted(account for account, currency in found.items() if currency != event.currency)
        if wrong:
            raise ValidationError(f"currency does not match accounts: {', '.join(wrong)}")

    def _append_event(
        self,
        connection: sqlite3.Connection,
        tenant: str,
        idempotency_key: str,
        event: MoneyEvent,
        received_ns: int,
        correction_of: str | None,
    ) -> dict[str, Any]:
        state = self._tenant(connection, tenant)
        self._validate_event_accounts(connection, tenant, event)
        if received_ns < event.occurred_ns:
            raise ValidationError("received_ns cannot be earlier than occurred_ns")
        body = event.as_dict()
        body_json = canonical_json(body)
        body_digest = hashlib.sha256(body_json.encode()).hexdigest()
        request_digest = digest(
            {
                "event": body,
                "received_ns": received_ns,
                "correction_of": correction_of,
            }
        )
        existing_key = connection.execute(
            "SELECT * FROM events WHERE tenant = ? AND idempotency_key = ?",
            (tenant, idempotency_key),
        ).fetchone()
        if existing_key:
            stored_request = digest(
                {
                    "event": json.loads(existing_key["body_json"]),
                    "received_ns": existing_key["received_ns"],
                    "correction_of": existing_key["correction_of"],
                }
            )
            if not hmac.compare_digest(stored_request, request_digest):
                raise IdempotencyConflict("idempotency key was reused with different event data")
            return {
                "event_id": existing_key["event_id"],
                "sequence": existing_key["sequence"],
                "revision": existing_key["sequence"],
                "event_hash": existing_key["event_hash"],
                "replayed": True,
            }
        existing_event = connection.execute(
            "SELECT 1 FROM events WHERE tenant = ? AND event_id = ?",
            (tenant, event.event_id),
        ).fetchone()
        if existing_event:
            raise ConflictError("event_id already exists")
        if correction_of is not None:
            target = connection.execute(
                "SELECT 1 FROM events WHERE tenant = ? AND event_id = ?",
                (tenant, correction_of),
            ).fetchone()
            if target is None:
                raise NotFoundError("correction target event was not found")
        sequence = state["revision"] + 1
        event_hash = self._chain_hash(
            {
                "tenant": tenant,
                "sequence": sequence,
                "body_digest": body_digest,
                "received_ns": received_ns,
                "correction_of": correction_of,
                "previous_hash": state["event_head_hash"],
            }
        )
        connection.execute(
            """INSERT INTO events(
                tenant, sequence, event_id, idempotency_key, producer, reference_id,
                currency, occurred_ns, received_ns, body_json, body_digest,
                correction_of, previous_hash, event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                tenant,
                sequence,
                event.event_id,
                idempotency_key,
                event.producer,
                event.reference_id,
                event.currency,
                event.occurred_ns,
                received_ns,
                body_json,
                body_digest,
                correction_of,
                state["event_head_hash"],
                event_hash,
            ),
        )
        connection.executemany(
            "INSERT INTO postings VALUES (?, ?, ?, ?, ?)",
            [
                (tenant, event.event_id, index, item.account_id, item.amount_minor)
                for index, item in enumerate(event.postings)
            ],
        )
        connection.execute(
            "UPDATE tenant_state SET revision = ?, event_head_hash = ? WHERE tenant = ?",
            (sequence, event_hash, tenant),
        )
        return {
            "event_id": event.event_id,
            "sequence": sequence,
            "revision": sequence,
            "event_hash": event_hash,
            "replayed": False,
        }

    def ingest_event(
        self,
        tenant: str,
        idempotency_key: str,
        event_value: Any,
        *,
        received_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        key = require_identifier(idempotency_key, "idempotency_key")
        event = MoneyEvent.from_value(event_value)
        received = self._now(received_ns)
        with self._transaction() as connection:
            result = self._append_event(connection, tenant, key, event, received, None)
            if not result["replayed"]:
                self._audit(
                    connection,
                    tenant,
                    "EVENT_INGESTED",
                    event.event_id,
                    {
                        "event_hash": result["event_hash"],
                        "producer": event.producer,
                        "reference_id": event.reference_id,
                    },
                    received,
                )
            return result

    def publish_manifest(
        self,
        tenant: str,
        idempotency_key: str,
        batch_id: str,
        producer: str,
        expected_references: Any,
        deadline_ns: int,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        key = require_identifier(idempotency_key, "idempotency_key")
        batch_id = require_identifier(batch_id, "batch_id")
        producer = require_identifier(producer, "producer")
        references = normalize_references(expected_references)
        deadline = require_timestamp(deadline_ns, "deadline_ns")
        now = self._now(now_ns)
        body = {
            "batch_id": batch_id,
            "producer": producer,
            "expected_references": list(references),
            "deadline_ns": deadline,
        }
        body_digest = digest(body)
        with self._transaction() as connection:
            self._tenant(connection, tenant)
            existing = connection.execute(
                "SELECT * FROM manifests WHERE tenant = ? AND idempotency_key = ?",
                (tenant, key),
            ).fetchone()
            if existing:
                if not hmac.compare_digest(existing["manifest_digest"], body_digest):
                    raise IdempotencyConflict("manifest key was reused with different content")
                return {
                    "batch_id": existing["batch_id"],
                    "manifest_digest": existing["manifest_digest"],
                    "replayed": True,
                }
            duplicate = connection.execute(
                "SELECT 1 FROM manifests WHERE tenant = ? AND batch_id = ?",
                (tenant, batch_id),
            ).fetchone()
            if duplicate:
                raise ConflictError("batch_id already exists")
            connection.execute(
                "INSERT INTO manifests VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    tenant,
                    batch_id,
                    key,
                    producer,
                    canonical_json(list(references)),
                    body_digest,
                    deadline,
                    now,
                ),
            )
            self._audit(
                connection,
                tenant,
                "MANIFEST_PUBLISHED",
                batch_id,
                {"manifest_digest": body_digest, "expected_count": len(references)},
                now,
            )
            return {"batch_id": batch_id, "manifest_digest": body_digest, "replayed": False}

    @staticmethod
    def _balances(
        connection: sqlite3.Connection, tenant: str, as_of_ns: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """SELECT a.account_id, a.kind, a.currency,
                      COALESCE(SUM(CASE WHEN e.event_id IS NOT NULL
                                        THEN p.amount_minor ELSE 0 END), 0) AS balance_minor,
                      MIN(e.occurred_ns) AS first_activity_ns,
                      COUNT(e.event_id) AS posting_count
               FROM accounts a
               LEFT JOIN postings p ON p.tenant = a.tenant AND p.account_id = a.account_id
               LEFT JOIN events e ON e.tenant = p.tenant AND e.event_id = p.event_id
                                 AND e.received_ns <= ?
               WHERE a.tenant = ?
               GROUP BY a.account_id, a.kind, a.currency
               ORDER BY a.account_id""",
            (as_of_ns, tenant),
        ).fetchall()
        return [dict(row) for row in rows]

    def _quality_report(
        self,
        connection: sqlite3.Connection,
        tenant: str,
        as_of_ns: int,
        policy: QualityPolicy,
    ) -> dict[str, Any]:
        state = self._tenant(connection, tenant)
        balances = self._balances(connection, tenant, as_of_ns)
        clearing_accounts = [row for row in balances if row["kind"] == "clearing" and row["posting_count"]]
        clearing_issues = []
        for row in clearing_accounts:
            age = as_of_ns - row["first_activity_ns"]
            if row["balance_minor"] != 0 and age >= policy.clearing_grace_ns:
                clearing_issues.append(
                    {
                        "account_id": row["account_id"],
                        "currency": row["currency"],
                        "balance_minor": row["balance_minor"],
                        "age_ns": age,
                    }
                )
        clearing_total = len(clearing_accounts)
        clearing_score = (
            100.0
            if clearing_total == 0
            else 100.0 * (clearing_total - len(clearing_issues)) / clearing_total
        )

        event_rows = connection.execute(
            """SELECT event_id, producer, reference_id, occurred_ns, received_ns
               FROM events WHERE tenant = ? AND received_ns <= ? ORDER BY sequence""",
            (tenant, as_of_ns),
        ).fetchall()
        late = [
            {
                "event_id": row["event_id"],
                "producer": row["producer"],
                "lag_ns": row["received_ns"] - row["occurred_ns"],
            }
            for row in event_rows
            if row["received_ns"] - row["occurred_ns"] > policy.max_lag_ns
        ]
        timeliness_score = (
            100.0 if not event_rows else 100.0 * (len(event_rows) - len(late)) / len(event_rows)
        )

        manifests = connection.execute(
            """SELECT * FROM manifests
               WHERE tenant = ? AND deadline_ns <= ? AND created_ns <= ? ORDER BY batch_id""",
            (tenant, as_of_ns, as_of_ns),
        ).fetchall()
        observed = {(row["producer"], row["reference_id"]) for row in event_rows}
        missing: list[dict[str, str]] = []
        expected_count = 0
        for manifest in manifests:
            for reference in json.loads(manifest["expected_json"]):
                expected_count += 1
                if (manifest["producer"], reference) not in observed:
                    missing.append(
                        {
                            "batch_id": manifest["batch_id"],
                            "producer": manifest["producer"],
                            "reference_id": reference,
                        }
                    )
        completeness_score = (
            100.0
            if expected_count == 0
            else 100.0 * (expected_count - len(missing)) / expected_count
        )
        scores = {
            "clearing": round(clearing_score, 6),
            "timeliness": round(timeliness_score, 6),
            "completeness": round(completeness_score, 6),
        }
        overall = round(sum(scores.values()) / 3, 6)
        reasons: list[str] = []
        verdict = "PASS"
        if not event_rows or not manifests:
            verdict = "INCONCLUSIVE"
            if not event_rows:
                reasons.append("no_events")
            if not manifests:
                reasons.append("no_due_manifests")
        failures = []
        if clearing_issues:
            failures.append("uncleared_balances")
        if late:
            failures.append("late_events")
        if len(missing) > policy.max_missing_references:
            failures.append("missing_references")
        if overall < policy.minimum_score:
            failures.append("score_below_policy")
        if failures:
            verdict = "FAIL"
            reasons = failures
        return {
            "tenant": tenant,
            "snapshot_revision": state["revision"],
            "as_of_ns": as_of_ns,
            "verdict": verdict,
            "reasons": reasons,
            "score": overall,
            "scores": scores,
            "coverage": {
                "events": len(event_rows),
                "due_manifests": len(manifests),
                "expected_references": expected_count,
                "missing_references": len(missing),
                "active_clearing_accounts": clearing_total,
                "uncleared_accounts": len(clearing_issues),
                "late_events": len(late),
            },
            "issues": {
                "clearing": clearing_issues,
                "timeliness": late,
                "completeness": missing,
            },
            "policy": policy.as_dict(),
        }

    def evaluate_quality(
        self,
        tenant: str,
        idempotency_key: str,
        as_of_ns: int,
        policy_value: Any,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        key = require_identifier(idempotency_key, "idempotency_key")
        as_of = require_timestamp(as_of_ns, "as_of_ns")
        policy = QualityPolicy.from_value(policy_value)
        now = self._now(now_ns)
        request = {"as_of_ns": as_of, "policy": policy.as_dict()}
        request_digest = digest(request)
        with self._transaction() as connection:
            self._tenant(connection, tenant)
            existing = connection.execute(
                "SELECT * FROM quality_runs WHERE tenant = ? AND idempotency_key = ?",
                (tenant, key),
            ).fetchone()
            if existing:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise IdempotencyConflict("evaluation key was reused with different input")
                return {
                    "evaluation_id": existing["evaluation_id"],
                    "report": json.loads(existing["report_json"]),
                    "attestation": json.loads(existing["attestation_json"]),
                    "replayed": True,
                }
            report = self._quality_report(connection, tenant, as_of, policy)
            report_digest = digest(report)
            evaluation_id = uuid.uuid4().hex
            payload = {
                "version": 1,
                "tenant": tenant,
                "evaluation_id": evaluation_id,
                "snapshot_revision": report["snapshot_revision"],
                "as_of_ns": as_of,
                "verdict": report["verdict"],
                "score": report["score"],
                "report_digest": report_digest,
            }
            attestation = {"payload": payload, "signature": self._sign(payload)}
            connection.execute(
                """INSERT INTO quality_runs VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )""",
                (
                    tenant,
                    evaluation_id,
                    key,
                    request_digest,
                    report["snapshot_revision"],
                    as_of,
                    canonical_json(policy.as_dict()),
                    canonical_json(report),
                    report_digest,
                    canonical_json(attestation),
                    now,
                ),
            )
            self._audit(
                connection,
                tenant,
                "QUALITY_EVALUATED",
                evaluation_id,
                {"report_digest": report_digest, "verdict": report["verdict"]},
                now,
            )
            return {
                "evaluation_id": evaluation_id,
                "report": report,
                "attestation": attestation,
                "replayed": False,
            }

    def verify_attestation(
        self, attestation: Any, *, expected_tenant: str | None = None
    ) -> dict[str, Any]:
        if not isinstance(attestation, dict) or set(attestation) != {"payload", "signature"}:
            raise ValidationError("attestation must contain only payload and signature")
        payload = attestation["payload"]
        signature = attestation["signature"]
        if not isinstance(payload, dict) or not isinstance(signature, str):
            raise ValidationError("invalid attestation types")
        required = {
            "version",
            "tenant",
            "evaluation_id",
            "snapshot_revision",
            "as_of_ns",
            "verdict",
            "score",
            "report_digest",
        }
        if set(payload) != required:
            raise ValidationError("attestation payload schema is invalid")
        tenant = require_identifier(payload["tenant"], "tenant")
        if expected_tenant is not None and tenant != require_identifier(expected_tenant, "expected_tenant"):
            raise NotFoundError("attestation was not issued for this tenant")
        expected = self._sign(payload)
        if not hmac.compare_digest(signature, expected):
            raise IntegrityError("attestation signature is invalid")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT report_digest FROM quality_runs WHERE tenant = ? AND evaluation_id = ?",
                (tenant, payload["evaluation_id"]),
            ).fetchone()
        if row is None or not hmac.compare_digest(row["report_digest"], payload["report_digest"]):
            raise NotFoundError("attested report was not found")
        return {"valid": True, "payload": payload}

    def _event_postings(
        self, connection: sqlite3.Connection, tenant: str, event_id: str
    ) -> list[dict[str, Any]]:
        return [
            {"account_id": row["account_id"], "amount_minor": row["amount_minor"]}
            for row in connection.execute(
                """SELECT account_id, amount_minor FROM postings
                   WHERE tenant = ? AND event_id = ? ORDER BY line_no""",
                (tenant, event_id),
            ).fetchall()
        ]

    def propose_correction(
        self,
        tenant: str,
        idempotency_key: str,
        actor: str,
        target_event_id: str,
        replacement_value: Any,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        key = require_identifier(idempotency_key, "idempotency_key")
        actor = validate_actor(actor)
        target_event_id = require_identifier(target_event_id, "target_event_id")
        replacement = MoneyEvent.from_value(replacement_value)
        now = self._now(now_ns)
        request = {
            "actor": actor,
            "target_event_id": target_event_id,
            "replacement": replacement.as_dict(),
        }
        request_digest = digest(request)
        with self._transaction() as connection:
            state = self._tenant(connection, tenant)
            existing = connection.execute(
                "SELECT * FROM correction_proposals WHERE tenant = ? AND idempotency_key = ?",
                (tenant, key),
            ).fetchone()
            if existing:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise IdempotencyConflict("proposal key was reused with different content")
                return self._proposal_result(connection, existing, replayed=True)
            target = connection.execute(
                "SELECT * FROM events WHERE tenant = ? AND event_id = ?",
                (tenant, target_event_id),
            ).fetchone()
            if target is None:
                raise NotFoundError("target event was not found")
            if target["correction_of"] is not None:
                raise ValidationError("a correction event cannot be corrected directly")
            if target["currency"] != replacement.currency:
                raise ValidationError("replacement currency must match target currency")
            self._validate_event_accounts(connection, tenant, replacement)
            if connection.execute(
                "SELECT 1 FROM events WHERE tenant = ? AND event_id = ?",
                (tenant, replacement.event_id),
            ).fetchone():
                raise ConflictError("replacement event_id already exists")
            target_postings = self._event_postings(connection, tenant, target_event_id)
            impacted = sorted(
                {item["account_id"] for item in target_postings}
                | {item.account_id for item in replacement.postings}
            )
            balances = {
                row["account_id"]: row["balance_minor"]
                for row in self._balances(connection, tenant, now)
                if row["account_id"] in impacted
            }
            delta = {account: 0 for account in impacted}
            for item in target_postings:
                delta[item["account_id"]] -= item["amount_minor"]
            for item in replacement.postings:
                delta[item.account_id] += item.amount_minor
            preview = {
                "base_revision": state["revision"],
                "target_event_id": target_event_id,
                "replacement_event_id": replacement.event_id,
                "affected_accounts": [
                    {
                        "account_id": account,
                        "before_minor": balances.get(account, 0),
                        "delta_minor": delta[account],
                        "after_minor": balances.get(account, 0) + delta[account],
                    }
                    for account in impacted
                ],
            }
            preview_digest = digest(preview)
            proposal_id = uuid.uuid4().hex
            connection.execute(
                """INSERT INTO correction_proposals(
                    tenant, proposal_id, idempotency_key, request_digest,
                    target_event_id, replacement_json, base_revision,
                    preview_json, preview_digest, creator, state, created_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PROPOSED', ?)""",
                (
                    tenant,
                    proposal_id,
                    key,
                    request_digest,
                    target_event_id,
                    canonical_json(replacement.as_dict()),
                    state["revision"],
                    canonical_json(preview),
                    preview_digest,
                    actor,
                    now,
                ),
            )
            self._audit(
                connection,
                tenant,
                "CORRECTION_PROPOSED",
                proposal_id,
                {"preview_digest": preview_digest, "target_event_id": target_event_id},
                now,
            )
            row = connection.execute(
                "SELECT * FROM correction_proposals WHERE tenant = ? AND proposal_id = ?",
                (tenant, proposal_id),
            ).fetchone()
            assert row is not None
            return self._proposal_result(connection, row, replayed=False)

    @staticmethod
    def _proposal_result(
        connection: sqlite3.Connection, row: sqlite3.Row, *, replayed: bool
    ) -> dict[str, Any]:
        approvals = [
            item["actor"]
            for item in connection.execute(
                """SELECT actor FROM correction_approvals
                   WHERE tenant = ? AND proposal_id = ? ORDER BY actor""",
                (row["tenant"], row["proposal_id"]),
            ).fetchall()
        ]
        return {
            "proposal_id": row["proposal_id"],
            "target_event_id": row["target_event_id"],
            "base_revision": row["base_revision"],
            "preview": json.loads(row["preview_json"]),
            "preview_digest": row["preview_digest"],
            "creator": row["creator"],
            "state": row["state"],
            "approvals": approvals,
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "replayed": replayed,
        }

    def approve_correction(
        self,
        tenant: str,
        proposal_id: str,
        actor: str,
        preview_digest: str,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        proposal_id = require_identifier(proposal_id, "proposal_id")
        actor = validate_actor(actor)
        if not isinstance(preview_digest, str) or len(preview_digest) != 64:
            raise ValidationError("preview_digest must be a SHA-256 hex digest")
        now = self._now(now_ns)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM correction_proposals WHERE tenant = ? AND proposal_id = ?",
                (tenant, proposal_id),
            ).fetchone()
            if row is None:
                raise NotFoundError("proposal was not found")
            if row["state"] == "APPLIED":
                raise StateError("applied proposal cannot receive approvals")
            if actor == row["creator"]:
                raise ValidationError("proposal creator cannot approve their own correction")
            if not hmac.compare_digest(row["preview_digest"], preview_digest):
                raise ConflictError("approval does not match the proposal impact preview")
            existing = connection.execute(
                """SELECT * FROM correction_approvals
                   WHERE tenant = ? AND proposal_id = ? AND actor = ?""",
                (tenant, proposal_id, actor),
            ).fetchone()
            if existing:
                return self._proposal_result(connection, row, replayed=True)
            connection.execute(
                "INSERT INTO correction_approvals VALUES (?, ?, ?, ?, ?)",
                (tenant, proposal_id, actor, preview_digest, now),
            )
            count = connection.execute(
                """SELECT COUNT(*) AS count FROM correction_approvals
                   WHERE tenant = ? AND proposal_id = ?""",
                (tenant, proposal_id),
            ).fetchone()["count"]
            if count >= 2:
                connection.execute(
                    """UPDATE correction_proposals SET state = 'APPROVED'
                       WHERE tenant = ? AND proposal_id = ?""",
                    (tenant, proposal_id),
                )
            self._audit(
                connection,
                tenant,
                "CORRECTION_APPROVED",
                proposal_id,
                {"actor": actor, "preview_digest": preview_digest},
                now,
            )
            row = connection.execute(
                "SELECT * FROM correction_proposals WHERE tenant = ? AND proposal_id = ?",
                (tenant, proposal_id),
            ).fetchone()
            assert row is not None
            return self._proposal_result(connection, row, replayed=False)

    def execute_correction(
        self,
        tenant: str,
        proposal_id: str,
        actor: str,
        idempotency_key: str,
        expected_revision: int,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        proposal_id = require_identifier(proposal_id, "proposal_id")
        actor = validate_actor(actor)
        key = require_identifier(idempotency_key, "idempotency_key")
        expected_revision = require_int(expected_revision, "expected_revision", minimum=0)
        now = self._now(now_ns)
        execute_digest = digest(
            {"actor": actor, "idempotency_key": key, "expected_revision": expected_revision}
        )
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM correction_proposals WHERE tenant = ? AND proposal_id = ?",
                (tenant, proposal_id),
            ).fetchone()
            if row is None:
                raise NotFoundError("proposal was not found")
            if row["state"] == "APPLIED":
                if row["execute_key"] != key or not hmac.compare_digest(
                    row["execute_digest"], execute_digest
                ):
                    raise IdempotencyConflict("proposal was already applied by another execution")
                result = json.loads(row["result_json"])
                return {**result, "replayed": True}
            if row["state"] != "APPROVED":
                raise StateError("proposal requires two independent approvals")
            state = self._tenant(connection, tenant)
            if expected_revision != row["base_revision"] or state["revision"] != expected_revision:
                raise ConflictError("ledger changed after the impact preview; create a new proposal")
            target = connection.execute(
                "SELECT * FROM events WHERE tenant = ? AND event_id = ?",
                (tenant, row["target_event_id"]),
            ).fetchone()
            assert target is not None
            target_postings = self._event_postings(connection, tenant, row["target_event_id"])
            reverse_id = f"corr:{proposal_id}:reverse"
            reverse = MoneyEvent.from_value(
                {
                    "event_id": reverse_id,
                    "producer": "clearflow.correction",
                    "reference_id": f"corr:{proposal_id}:reverse",
                    "currency": target["currency"],
                    "occurred_ns": now,
                    "postings": [
                        {
                            "account_id": item["account_id"],
                            "amount_minor": -item["amount_minor"],
                        }
                        for item in target_postings
                    ],
                    "metadata": {"proposal_id": proposal_id, "operation": "reversal"},
                }
            )
            replacement = MoneyEvent.from_value(json.loads(row["replacement_json"]))
            reversed_result = self._append_event(
                connection,
                tenant,
                f"corr:{proposal_id}:reverse",
                reverse,
                now,
                row["target_event_id"],
            )
            replacement_result = self._append_event(
                connection,
                tenant,
                f"corr:{proposal_id}:replace",
                replacement,
                now,
                row["target_event_id"],
            )
            result = {
                "proposal_id": proposal_id,
                "state": "APPLIED",
                "reversal_event_id": reversed_result["event_id"],
                "replacement_event_id": replacement_result["event_id"],
                "revision": replacement_result["revision"],
                "actor": actor,
            }
            connection.execute(
                """UPDATE correction_proposals
                   SET state = 'APPLIED', execute_key = ?, execute_digest = ?,
                       result_json = ?, applied_ns = ?
                   WHERE tenant = ? AND proposal_id = ?""",
                (key, execute_digest, canonical_json(result), now, tenant, proposal_id),
            )
            self._audit(
                connection,
                tenant,
                "CORRECTION_APPLIED",
                proposal_id,
                result,
                now,
            )
            return {**result, "replayed": False}

    def account_balances(self, tenant: str, *, as_of_ns: int | None = None) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        as_of = self._now(as_of_ns)
        with self._transaction(immediate=False) as connection:
            state = connection.execute(
                "SELECT * FROM tenant_state WHERE tenant = ?", (tenant,)
            ).fetchone()
            if state is None:
                raise NotFoundError("tenant was not found")
            return {
                "tenant": tenant,
                "revision": state["revision"],
                "as_of_ns": as_of,
                "balances": self._balances(connection, tenant, as_of),
            }

    def get_event(self, tenant: str, event_id: str) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        event_id = require_identifier(event_id, "event_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM events WHERE tenant = ? AND event_id = ?",
                (tenant, event_id),
            ).fetchone()
            if row is None:
                raise NotFoundError("event was not found")
            return {
                **{key: row[key] for key in row.keys() if key != "body_json"},
                "event": json.loads(row["body_json"]),
                "postings": self._event_postings(connection, tenant, event_id),
            }

    def verify_integrity(self, tenant: str | None = None) -> dict[str, Any]:
        if tenant is not None:
            tenant = require_identifier(tenant, "tenant")
        totals = {
            "tenants": 0,
            "accounts": 0,
            "events": 0,
            "postings": 0,
            "manifests": 0,
            "quality_runs": 0,
            "proposals": 0,
            "audit_entries": 0,
        }
        with self._transaction(immediate=False) as connection:
            query = "SELECT * FROM tenant_state"
            params: tuple[Any, ...] = ()
            if tenant is not None:
                query += " WHERE tenant = ?"
                params = (tenant,)
            states = connection.execute(query + " ORDER BY tenant", params).fetchall()
            if tenant is not None and not states:
                raise NotFoundError("tenant was not found")
            for state in states:
                current_tenant = state["tenant"]
                totals["tenants"] += 1
                accounts = connection.execute(
                    "SELECT * FROM accounts WHERE tenant = ?", (current_tenant,)
                ).fetchall()
                account_currency = {row["account_id"]: row["currency"] for row in accounts}
                totals["accounts"] += len(accounts)
                previous = ZERO_HASH
                events = connection.execute(
                    "SELECT * FROM events WHERE tenant = ? ORDER BY sequence", (current_tenant,)
                ).fetchall()
                for expected_sequence, event in enumerate(events, 1):
                    if event["sequence"] != expected_sequence or event["previous_hash"] != previous:
                        raise IntegrityError("event chain sequence or previous hash is invalid")
                    body = json.loads(event["body_json"])
                    if digest(body) != event["body_digest"]:
                        raise IntegrityError("event body digest is invalid")
                    model = MoneyEvent.from_value(body)
                    if (
                        event["event_id"] != model.event_id
                        or event["producer"] != model.producer
                        or event["reference_id"] != model.reference_id
                        or event["currency"] != model.currency
                        or event["occurred_ns"] != model.occurred_ns
                    ):
                        raise IntegrityError("event index columns do not match immutable body")
                    postings = self._event_postings(connection, current_tenant, event["event_id"])
                    if postings != [item.as_dict() for item in model.postings]:
                        raise IntegrityError("event postings do not match immutable body")
                    if any(account_currency.get(item.account_id) != model.currency for item in model.postings):
                        raise IntegrityError("event account currency is invalid")
                    material = {
                        "tenant": current_tenant,
                        "sequence": event["sequence"],
                        "body_digest": event["body_digest"],
                        "received_ns": event["received_ns"],
                        "correction_of": event["correction_of"],
                        "previous_hash": previous,
                    }
                    expected_hash = self._chain_hash(material)
                    if not hmac.compare_digest(expected_hash, event["event_hash"]):
                        raise IntegrityError("event chain hash is invalid")
                    previous = event["event_hash"]
                    totals["postings"] += len(postings)
                if state["revision"] != len(events) or state["event_head_hash"] != previous:
                    raise IntegrityError("materialized event head is invalid")
                totals["events"] += len(events)

                manifests = connection.execute(
                    "SELECT * FROM manifests WHERE tenant = ?", (current_tenant,)
                ).fetchall()
                for manifest in manifests:
                    body = {
                        "batch_id": manifest["batch_id"],
                        "producer": manifest["producer"],
                        "expected_references": json.loads(manifest["expected_json"]),
                        "deadline_ns": manifest["deadline_ns"],
                    }
                    if digest(body) != manifest["manifest_digest"]:
                        raise IntegrityError("manifest digest is invalid")
                totals["manifests"] += len(manifests)

                quality = connection.execute(
                    "SELECT * FROM quality_runs WHERE tenant = ?", (current_tenant,)
                ).fetchall()
                for run in quality:
                    report = json.loads(run["report_json"])
                    policy = json.loads(run["policy_json"])
                    if digest({"as_of_ns": run["as_of_ns"], "policy": policy}) != run[
                        "request_digest"
                    ]:
                        raise IntegrityError("quality request digest is invalid")
                    if digest(report) != run["report_digest"]:
                        raise IntegrityError("quality report digest is invalid")
                    attestation = json.loads(run["attestation_json"])
                    if not hmac.compare_digest(
                        attestation.get("signature", ""), self._sign(attestation.get("payload", {}))
                    ):
                        raise IntegrityError("quality attestation is invalid")
                    if attestation["payload"].get("report_digest") != run["report_digest"]:
                        raise IntegrityError("quality attestation is detached from report")
                totals["quality_runs"] += len(quality)

                proposals = connection.execute(
                    "SELECT * FROM correction_proposals WHERE tenant = ?", (current_tenant,)
                ).fetchall()
                for proposal in proposals:
                    replacement = json.loads(proposal["replacement_json"])
                    proposal_request = {
                        "actor": proposal["creator"],
                        "target_event_id": proposal["target_event_id"],
                        "replacement": replacement,
                    }
                    if digest(proposal_request) != proposal["request_digest"]:
                        raise IntegrityError("correction request digest is invalid")
                    if digest(json.loads(proposal["preview_json"])) != proposal["preview_digest"]:
                        raise IntegrityError("correction preview digest is invalid")
                    approvals = connection.execute(
                        """SELECT * FROM correction_approvals
                           WHERE tenant = ? AND proposal_id = ?""",
                        (current_tenant, proposal["proposal_id"]),
                    ).fetchall()
                    if any(item["preview_digest"] != proposal["preview_digest"] for item in approvals):
                        raise IntegrityError("correction approval is detached from preview")
                    if proposal["state"] in {"APPROVED", "APPLIED"} and len(approvals) < 2:
                        raise IntegrityError("approved correction lacks two approvals")
                totals["proposals"] += len(proposals)

                previous = ZERO_HASH
                audits = connection.execute(
                    "SELECT * FROM audit_log WHERE tenant = ? ORDER BY sequence", (current_tenant,)
                ).fetchall()
                for expected_sequence, entry in enumerate(audits, 1):
                    if entry["sequence"] != expected_sequence or entry["previous_hash"] != previous:
                        raise IntegrityError("audit chain sequence or previous hash is invalid")
                    expected_hash = self._chain_hash(
                        {
                            "tenant": current_tenant,
                            "sequence": entry["sequence"],
                            "action": entry["action"],
                            "entity_id": entry["entity_id"],
                            "payload_digest": entry["payload_digest"],
                            "created_ns": entry["created_ns"],
                            "previous_hash": previous,
                        }
                    )
                    if not hmac.compare_digest(expected_hash, entry["entry_hash"]):
                        raise IntegrityError("audit entry hash is invalid")
                    previous = entry["entry_hash"]
                if state["audit_sequence"] != len(audits) or state["audit_head_hash"] != previous:
                    raise IntegrityError("materialized audit head is invalid")
                totals["audit_entries"] += len(audits)
        return {"valid": True, **totals}

    def stats(self, tenant: str) -> dict[str, Any]:
        tenant = require_identifier(tenant, "tenant")
        with self._connect() as connection:
            state = connection.execute(
                "SELECT * FROM tenant_state WHERE tenant = ?", (tenant,)
            ).fetchone()
            if state is None:
                raise NotFoundError("tenant was not found")
            counts = {}
            for name, table in (
                ("accounts", "accounts"),
                ("events", "events"),
                ("postings", "postings"),
                ("manifests", "manifests"),
                ("quality_runs", "quality_runs"),
                ("proposals", "correction_proposals"),
                ("audit_entries", "audit_log"),
            ):
                counts[name] = connection.execute(
                    f"SELECT COUNT(*) AS count FROM {table} WHERE tenant = ?", (tenant,)
                ).fetchone()["count"]
            return {
                "tenant": tenant,
                "revision": state["revision"],
                "event_head_hash": state["event_head_hash"],
                **counts,
            }
