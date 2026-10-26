"""Durable policies, canary evidence, judgments, receipts, and audit history."""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from .errors import (
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    NotFoundError,
    StateError,
    ValidationError,
)
from .judge import judge
from .model import (
    DIGEST_RE,
    Policy,
    canonical_json,
    digest_json,
    validate_id,
    validate_integer,
    validate_number,
    validate_series,
    validate_timestamp,
    validate_trials,
)


RUN_STATES = {"COLLECTING", "JUDGED"}
VERDICTS = {"PASS", "FAIL", "MANUAL", "INCONCLUSIVE"}


def _secret_bytes(secret: bytes | str) -> bytes:
    value = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValidationError("signing secret must contain at least 32 bytes")
    return value


class RolloutJudgeStore:
    def __init__(self, path: str | Path, *, secret: bytes | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._secret = _secret_bytes(secret)
        self._secret_fingerprint = hashlib.sha256(self._secret).hexdigest()
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
                CREATE TABLE IF NOT EXISTS policies (
                    policy_id TEXT PRIMARY KEY,
                    version INTEGER NOT NULL CHECK(version > 0),
                    config_json TEXT NOT NULL,
                    config_digest TEXT NOT NULL,
                    created_ns INTEGER NOT NULL,
                    updated_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    service TEXT NOT NULL,
                    release TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    policy_id TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    policy_json TEXT NOT NULL,
                    policy_digest TEXT NOT NULL,
                    trials_json TEXT NOT NULL,
                    trials_digest TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('COLLECTING', 'JUDGED')),
                    verdict TEXT CHECK(verdict IN ('PASS', 'FAIL', 'MANUAL', 'INCONCLUSIVE')),
                    score REAL,
                    reason TEXT,
                    report_json TEXT,
                    report_digest TEXT,
                    decision_key TEXT,
                    receipt_json TEXT,
                    version INTEGER NOT NULL CHECK(version > 0),
                    created_ns INTEGER NOT NULL,
                    updated_ns INTEGER NOT NULL,
                    UNIQUE(service, idempotency_key),
                    CHECK((state = 'JUDGED' AND verdict IS NOT NULL AND score IS NOT NULL
                           AND report_json IS NOT NULL AND report_digest IS NOT NULL
                           AND decision_key IS NOT NULL AND receipt_json IS NOT NULL)
                          OR state = 'COLLECTING')
                );
                CREATE INDEX IF NOT EXISTS runs_service_created
                    ON runs(service, created_ns DESC);
                CREATE TABLE IF NOT EXISTS evidence (
                    run_id TEXT NOT NULL REFERENCES runs(run_id),
                    trial TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    batch_key TEXT NOT NULL,
                    baseline_json TEXT NOT NULL,
                    canary_json TEXT NOT NULL,
                    evidence_digest TEXT NOT NULL,
                    created_ns INTEGER NOT NULL,
                    PRIMARY KEY(run_id, trial, metric),
                    UNIQUE(run_id, batch_key)
                );
                CREATE TABLE IF NOT EXISTS counters (
                    name TEXT PRIMARY KEY,
                    value INTEGER NOT NULL CHECK(value >= 0)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    occurred_ns INTEGER NOT NULL,
                    previous_mac TEXT NOT NULL,
                    entry_mac TEXT NOT NULL UNIQUE
                );
                """
            )
            connection.execute("BEGIN IMMEDIATE")
            expected = {
                "schema_version": "1",
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
                    raise IntegrityError(f"database metadata mismatch for {name}")
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _counter(connection: sqlite3.Connection, name: str, delta: int = 1) -> None:
        connection.execute(
            """INSERT INTO counters(name, value) VALUES(?, ?)
               ON CONFLICT(name) DO UPDATE SET value = value + excluded.value""",
            (name, delta),
        )

    def _audit(
        self,
        connection: sqlite3.Connection,
        action: str,
        subject: str,
        detail: dict[str, Any],
        occurred_ns: int,
    ) -> None:
        previous = connection.execute(
            "SELECT entry_mac FROM audit_log ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous_mac = previous["entry_mac"] if previous else "0" * 64
        material = canonical_json(
            {
                "action": action,
                "detail": detail,
                "occurred_ns": occurred_ns,
                "previous_mac": previous_mac,
                "subject": subject,
            }
        )
        entry_mac = hmac.new(
            self._secret, material.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        connection.execute(
            """INSERT INTO audit_log(
                   action, subject, detail_json, occurred_ns, previous_mac, entry_mac
               ) VALUES(?, ?, ?, ?, ?, ?)""",
            (
                action,
                subject,
                canonical_json(detail),
                occurred_ns,
                previous_mac,
                entry_mac,
            ),
        )

    def create_policy(
        self,
        policy_id: str,
        policy: Policy | dict[str, Any],
        *,
        expected_version: int = 0,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        policy_id = validate_id(policy_id, "policy_id")
        expected_version = validate_integer(
            expected_version, "expected_version", 0, 1_000_000_000
        )
        parsed = policy if isinstance(policy, Policy) else Policy.from_dict(policy)
        parsed = parsed.validated()
        config = parsed.as_dict()
        config_json = canonical_json(config)
        config_digest = digest_json(config)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT * FROM policies WHERE policy_id = ?", (policy_id,)
            ).fetchone()
            current_version = current["version"] if current else 0
            if current_version != expected_version:
                raise ConflictError(
                    f"expected policy version {expected_version}, current version is {current_version}"
                )
            version = current_version + 1
            created_ns = current["created_ns"] if current else occurred_ns
            connection.execute(
                """INSERT INTO policies(
                       policy_id, version, config_json, config_digest, created_ns, updated_ns
                   ) VALUES(?, ?, ?, ?, ?, ?)
                   ON CONFLICT(policy_id) DO UPDATE SET
                       version = excluded.version,
                       config_json = excluded.config_json,
                       config_digest = excluded.config_digest,
                       updated_ns = excluded.updated_ns""",
                (
                    policy_id,
                    version,
                    config_json,
                    config_digest,
                    created_ns,
                    occurred_ns,
                ),
            )
            self._audit(
                connection,
                "UPSERT_POLICY",
                policy_id,
                {"config_digest": config_digest, "version": version},
                occurred_ns,
            )
            self._counter(connection, "policies_written")
            connection.commit()
            return {
                "config": config,
                "digest": config_digest,
                "policy_id": policy_id,
                "version": version,
            }
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def policy(self, policy_id: str) -> dict[str, Any]:
        policy_id = validate_id(policy_id, "policy_id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM policies WHERE policy_id = ?", (policy_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("policy does not exist")
            return {
                "config": json.loads(row["config_json"]),
                "digest": row["config_digest"],
                "policy_id": row["policy_id"],
                "version": row["version"],
            }
        finally:
            connection.close()

    @staticmethod
    def _public_run(row: sqlite3.Row, *, replayed: bool = False) -> dict[str, Any]:
        return {
            "created_ns": row["created_ns"],
            "evidence": None,
            "idempotency_key": row["idempotency_key"],
            "policy": {
                "digest": row["policy_digest"],
                "policy_id": row["policy_id"],
                "version": row["policy_version"],
            },
            "reason": row["reason"],
            "receipt": None if row["receipt_json"] is None else json.loads(row["receipt_json"]),
            "release": row["release"],
            "replayed": replayed,
            "report": None if row["report_json"] is None else json.loads(row["report_json"]),
            "run_id": row["run_id"],
            "score": row["score"],
            "service": row["service"],
            "state": row["state"],
            "trials": json.loads(row["trials_json"]),
            "updated_ns": row["updated_ns"],
            "verdict": row["verdict"],
            "version": row["version"],
        }

    def start_run(
        self,
        service: str,
        release: str,
        idempotency_key: str,
        policy_id: str,
        expected_trials: list[str] | tuple[str, ...],
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        service = validate_id(service, "service")
        release = validate_id(release, "release")
        idempotency_key = validate_id(idempotency_key, "idempotency_key")
        policy_id = validate_id(policy_id, "policy_id")
        trials = validate_trials(expected_trials)
        request = {
            "expected_trials": list(trials),
            "policy_id": policy_id,
            "release": release,
        }
        request_digest = digest_json(request)
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM runs WHERE service = ? AND idempotency_key = ?",
                (service, idempotency_key),
            ).fetchone()
            if existing is not None:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise IdempotencyConflict(
                        "idempotency key was already used for a different run request"
                    )
                connection.rollback()
                return self._public_run(existing, replayed=True)
            policy_row = connection.execute(
                "SELECT * FROM policies WHERE policy_id = ?", (policy_id,)
            ).fetchone()
            if policy_row is None:
                raise NotFoundError("policy does not exist")
            run_id = f"run-{uuid.uuid4().hex}"
            trials_json = canonical_json(list(trials))
            trials_digest = digest_json(list(trials))
            connection.execute(
                """INSERT INTO runs(
                       run_id, service, release, idempotency_key, request_digest,
                       policy_id, policy_version, policy_json, policy_digest,
                       trials_json, trials_digest, state, version, created_ns, updated_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'COLLECTING', 1, ?, ?)""",
                (
                    run_id,
                    service,
                    release,
                    idempotency_key,
                    request_digest,
                    policy_id,
                    policy_row["version"],
                    policy_row["config_json"],
                    policy_row["config_digest"],
                    trials_json,
                    trials_digest,
                    occurred_ns,
                    occurred_ns,
                ),
            )
            self._audit(
                connection,
                "START_RUN",
                run_id,
                {
                    "policy_digest": policy_row["config_digest"],
                    "release": release,
                    "service": service,
                    "trials_digest": trials_digest,
                },
                occurred_ns,
            )
            self._counter(connection, "runs_started")
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            connection.commit()
            assert row is not None
            return self._public_run(row)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def ingest_evidence(
        self,
        service: str,
        run_id: str,
        batch_key: str,
        trial: str,
        metric: str,
        baseline: list[float | int | None],
        canary: list[float | int | None],
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        service = validate_id(service, "service")
        run_id = validate_id(run_id, "run_id")
        batch_key = validate_id(batch_key, "batch_key")
        trial = validate_id(trial, "trial")
        metric = validate_id(metric, "metric")
        baseline_values = validate_series(baseline, "baseline")
        canary_values = validate_series(canary, "canary")
        evidence_document = {
            "baseline": list(baseline_values),
            "canary": list(canary_values),
            "metric": metric,
            "trial": trial,
        }
        evidence_digest = digest_json(evidence_document)
        baseline_json = canonical_json(list(baseline_values))
        canary_json = canonical_json(list(canary_values))
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT * FROM runs WHERE run_id = ? AND service = ?", (run_id, service)
            ).fetchone()
            if run is None:
                raise NotFoundError("run does not exist")
            existing_batch = connection.execute(
                "SELECT * FROM evidence WHERE run_id = ? AND batch_key = ?",
                (run_id, batch_key),
            ).fetchone()
            if existing_batch is not None:
                if not hmac.compare_digest(
                    existing_batch["evidence_digest"], evidence_digest
                ):
                    raise IdempotencyConflict(
                        "batch key was already used for different evidence"
                    )
                connection.rollback()
                return {
                    "digest": evidence_digest,
                    "metric": metric,
                    "replayed": True,
                    "run_id": run_id,
                    "trial": trial,
                    "version": run["version"],
                }
            if run["state"] != "COLLECTING":
                raise StateError("evidence cannot be added after judgment")
            trials = set(json.loads(run["trials_json"]))
            policy = Policy.from_dict(json.loads(run["policy_json"]))
            metric_names = {rule.name for rule in policy.metrics}
            if trial not in trials:
                raise ValidationError("trial is not declared by this run")
            if metric not in metric_names:
                raise ValidationError("metric is not declared by the policy snapshot")
            occupied = connection.execute(
                "SELECT batch_key FROM evidence WHERE run_id = ? AND trial = ? AND metric = ?",
                (run_id, trial, metric),
            ).fetchone()
            if occupied is not None:
                raise ConflictError(
                    f"evidence slot is already owned by batch {occupied['batch_key']}"
                )
            connection.execute(
                """INSERT INTO evidence(
                       run_id, trial, metric, batch_key, baseline_json, canary_json,
                       evidence_digest, created_ns
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    trial,
                    metric,
                    batch_key,
                    baseline_json,
                    canary_json,
                    evidence_digest,
                    occurred_ns,
                ),
            )
            version = run["version"] + 1
            connection.execute(
                "UPDATE runs SET version = ?, updated_ns = ? WHERE run_id = ?",
                (version, occurred_ns, run_id),
            )
            self._audit(
                connection,
                "INGEST_EVIDENCE",
                run_id,
                {
                    "batch_key": batch_key,
                    "evidence_digest": evidence_digest,
                    "metric": metric,
                    "trial": trial,
                },
                occurred_ns,
            )
            self._counter(connection, "evidence_ingested")
            connection.commit()
            return {
                "digest": evidence_digest,
                "metric": metric,
                "replayed": False,
                "run_id": run_id,
                "trial": trial,
                "version": version,
            }
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _sign_payload(self, payload: dict[str, Any]) -> str:
        return hmac.new(
            self._secret, canonical_json(payload).encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def judge_run(
        self,
        service: str,
        run_id: str,
        decision_key: str,
        *,
        expected_version: int,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        service = validate_id(service, "service")
        run_id = validate_id(run_id, "run_id")
        decision_key = validate_id(decision_key, "decision_key")
        expected_version = validate_integer(
            expected_version, "expected_version", 1, 1_000_000_000
        )
        occurred_ns = time.time_ns() if now_ns is None else validate_timestamp(now_ns)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT * FROM runs WHERE run_id = ? AND service = ?", (run_id, service)
            ).fetchone()
            if run is None:
                raise NotFoundError("run does not exist")
            if run["state"] == "JUDGED":
                if not hmac.compare_digest(run["decision_key"], decision_key):
                    raise IdempotencyConflict(
                        "run was already judged with a different decision key"
                    )
                connection.rollback()
                return self._public_run(run, replayed=True)
            if run["version"] != expected_version:
                raise ConflictError(
                    f"expected run version {expected_version}, current version is {run['version']}"
                )
            rows = connection.execute(
                "SELECT * FROM evidence WHERE run_id = ? ORDER BY trial, metric",
                (run_id,),
            ).fetchall()
            evidence = {
                (row["trial"], row["metric"]): (
                    tuple(json.loads(row["baseline_json"])),
                    tuple(json.loads(row["canary_json"])),
                )
                for row in rows
            }
            policy = Policy.from_dict(json.loads(run["policy_json"]))
            trials = tuple(json.loads(run["trials_json"]))
            report = judge(policy, trials, evidence)
            report_digest = digest_json(report)
            version = run["version"] + 1
            receipt_payload = {
                "decided_ns": occurred_ns,
                "policy_digest": run["policy_digest"],
                "release": run["release"],
                "report_digest": report_digest,
                "run_id": run_id,
                "score": report["score"],
                "service": service,
                "trials_digest": run["trials_digest"],
                "verdict": report["verdict"],
                "version": version,
            }
            receipt = {
                "payload": receipt_payload,
                "signature": self._sign_payload(receipt_payload),
            }
            connection.execute(
                """UPDATE runs SET state = 'JUDGED', verdict = ?, score = ?, reason = ?,
                       report_json = ?, report_digest = ?, decision_key = ?, receipt_json = ?,
                       version = ?, updated_ns = ? WHERE run_id = ?""",
                (
                    report["verdict"],
                    report["score"],
                    report["reason"],
                    canonical_json(report),
                    report_digest,
                    decision_key,
                    canonical_json(receipt),
                    version,
                    occurred_ns,
                    run_id,
                ),
            )
            self._audit(
                connection,
                "JUDGE_RUN",
                run_id,
                {
                    "report_digest": report_digest,
                    "score": report["score"],
                    "verdict": report["verdict"],
                    "version": version,
                },
                occurred_ns,
            )
            self._counter(connection, f"verdict_{report['verdict'].lower()}")
            updated = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            connection.commit()
            assert updated is not None
            return self._public_run(updated)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def get_run(self, service: str, run_id: str) -> dict[str, Any]:
        service = validate_id(service, "service")
        run_id = validate_id(run_id, "run_id")
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ? AND service = ?", (run_id, service)
            ).fetchone()
            if row is None:
                raise NotFoundError("run does not exist")
            result = self._public_run(row)
            evidence_count = connection.execute(
                "SELECT COUNT(*) FROM evidence WHERE run_id = ?", (run_id,)
            ).fetchone()[0]
            policy = Policy.from_dict(json.loads(row["policy_json"]))
            trials = json.loads(row["trials_json"])
            result["evidence"] = {
                "expected": len(policy.metrics) * len(trials),
                "ingested": evidence_count,
            }
            return result
        finally:
            connection.close()

    def verify_receipt(
        self, receipt: Any, *, expected_service: str | None = None
    ) -> dict[str, Any]:
        if expected_service is not None:
            expected_service = validate_id(expected_service, "expected_service")
        if not isinstance(receipt, dict) or set(receipt) != {"payload", "signature"}:
            raise ValidationError("receipt must contain only payload and signature")
        payload = receipt["payload"]
        signature = receipt["signature"]
        if not isinstance(payload, dict) or not isinstance(signature, str):
            raise ValidationError("receipt payload or signature has invalid type")
        if not DIGEST_RE.fullmatch(signature):
            raise ValidationError("receipt signature must be a SHA-256 hex digest")
        expected = self._sign_payload(payload)
        if not hmac.compare_digest(expected, signature):
            raise IntegrityError("receipt signature does not match")
        required = {
            "decided_ns",
            "policy_digest",
            "release",
            "report_digest",
            "run_id",
            "score",
            "service",
            "trials_digest",
            "verdict",
            "version",
        }
        if set(payload) != required:
            raise ValidationError("receipt payload fields do not match the receipt schema")
        validate_timestamp(payload["decided_ns"], "decided_ns")
        validate_id(payload["release"], "release")
        validate_id(payload["run_id"], "run_id")
        validate_id(payload["service"], "service")
        validate_number(payload["score"], "score", 0.0, 100.0)
        validate_integer(payload["version"], "version", 1, 1_000_000_000)
        if payload["verdict"] not in VERDICTS:
            raise ValidationError("receipt verdict is invalid")
        for field in ("policy_digest", "report_digest", "trials_digest"):
            if not isinstance(payload[field], str) or not DIGEST_RE.fullmatch(payload[field]):
                raise ValidationError(f"{field} must be a SHA-256 hex digest")
        if expected_service is not None and payload["service"] != expected_service:
            raise NotFoundError("receipt does not belong to this service")
        return {"valid": True, "verdict": payload["verdict"]}

    def stats(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            counters = {
                row["name"]: row["value"]
                for row in connection.execute("SELECT name, value FROM counters ORDER BY name")
            }
            states = {
                row["state"].lower(): row["count"]
                for row in connection.execute(
                    "SELECT state, COUNT(*) AS count FROM runs GROUP BY state"
                )
            }
            return {"counters": counters, "runs": states}
        finally:
            connection.close()

    def verify_integrity(self) -> dict[str, int]:
        connection = self._connect()
        try:
            metadata = {
                row["name"]: row["value"]
                for row in connection.execute("SELECT name, value FROM metadata")
            }
            if metadata.get("secret_fingerprint") != self._secret_fingerprint:
                raise IntegrityError("signing secret fingerprint mismatch")
            policies = connection.execute(
                "SELECT * FROM policies ORDER BY policy_id"
            ).fetchall()
            for row in policies:
                config = json.loads(row["config_json"])
                Policy.from_dict(config)
                if not hmac.compare_digest(digest_json(config), row["config_digest"]):
                    raise IntegrityError("policy digest mismatch")

            runs = connection.execute("SELECT * FROM runs ORDER BY created_ns").fetchall()
            run_by_id = {row["run_id"]: row for row in runs}
            for row in runs:
                policy_document = json.loads(row["policy_json"])
                policy = Policy.from_dict(policy_document)
                if not hmac.compare_digest(
                    digest_json(policy_document), row["policy_digest"]
                ):
                    raise IntegrityError("run policy snapshot digest mismatch")
                trials = json.loads(row["trials_json"])
                validate_trials(trials)
                if not hmac.compare_digest(digest_json(trials), row["trials_digest"]):
                    raise IntegrityError("run trials digest mismatch")
                request = {
                    "expected_trials": trials,
                    "policy_id": row["policy_id"],
                    "release": row["release"],
                }
                if not hmac.compare_digest(
                    digest_json(request), row["request_digest"]
                ):
                    raise IntegrityError("run request digest mismatch")
                if row["state"] not in RUN_STATES:
                    raise IntegrityError("run state is invalid")
                if row["state"] == "JUDGED":
                    report = json.loads(row["report_json"])
                    if (
                        report.get("verdict") != row["verdict"]
                        or report.get("score") != row["score"]
                        or report.get("reason") != row["reason"]
                    ):
                        raise IntegrityError("run judgment columns disagree with report")
                    if not hmac.compare_digest(
                        digest_json(report), row["report_digest"]
                    ):
                        raise IntegrityError("run report digest mismatch")
                    receipt = json.loads(row["receipt_json"])
                    self.verify_receipt(receipt)
                    payload = receipt["payload"]
                    if (
                        payload["run_id"] != row["run_id"]
                        or payload["service"] != row["service"]
                        or payload["release"] != row["release"]
                        or payload["report_digest"] != row["report_digest"]
                        or payload["policy_digest"] != row["policy_digest"]
                        or payload["trials_digest"] != row["trials_digest"]
                        or payload["verdict"] != row["verdict"]
                        or payload["score"] != row["score"]
                        or payload["version"] != row["version"]
                    ):
                        raise IntegrityError("receipt disagrees with persisted run")
                expected_slots = len(policy.metrics) * len(trials)
                actual_slots = connection.execute(
                    "SELECT COUNT(*) FROM evidence WHERE run_id = ?", (row["run_id"],)
                ).fetchone()[0]
                if actual_slots > expected_slots:
                    raise IntegrityError("run has more evidence than policy slots")

            evidence_rows = connection.execute(
                "SELECT * FROM evidence ORDER BY run_id, trial, metric"
            ).fetchall()
            for row in evidence_rows:
                run = run_by_id.get(row["run_id"])
                if run is None:
                    raise IntegrityError("evidence references an unknown run")
                trials = set(json.loads(run["trials_json"]))
                policy = Policy.from_dict(json.loads(run["policy_json"]))
                metrics = {rule.name for rule in policy.metrics}
                if row["trial"] not in trials or row["metric"] not in metrics:
                    raise IntegrityError("evidence is outside its run snapshot")
                document = {
                    "baseline": json.loads(row["baseline_json"]),
                    "canary": json.loads(row["canary_json"]),
                    "metric": row["metric"],
                    "trial": row["trial"],
                }
                if not hmac.compare_digest(
                    digest_json(document), row["evidence_digest"]
                ):
                    raise IntegrityError("evidence digest mismatch")

            previous = "0" * 64
            audits = connection.execute(
                "SELECT * FROM audit_log ORDER BY sequence"
            ).fetchall()
            for row in audits:
                if row["previous_mac"] != previous:
                    raise IntegrityError("audit chain previous MAC mismatch")
                detail = json.loads(row["detail_json"])
                material = canonical_json(
                    {
                        "action": row["action"],
                        "detail": detail,
                        "occurred_ns": row["occurred_ns"],
                        "previous_mac": row["previous_mac"],
                        "subject": row["subject"],
                    }
                )
                expected = hmac.new(
                    self._secret, material.encode("utf-8"), hashlib.sha256
                ).hexdigest()
                if not hmac.compare_digest(expected, row["entry_mac"]):
                    raise IntegrityError("audit entry MAC mismatch")
                previous = row["entry_mac"]
            return {
                "audit_entries": len(audits),
                "evidence_sets": len(evidence_rows),
                "policies": len(policies),
                "runs": len(runs),
            }
        finally:
            connection.close()
