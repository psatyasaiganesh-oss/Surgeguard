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
    BaselineSnapshot,
    canonical_json,
    digest,
    normalize_inventory,
    normalize_test_ids,
    require_closed_object,
    require_digest,
    require_identifier,
    require_int,
    require_timestamp,
)


ZERO_HASH = "0" * 64
SCHEMA_VERSION = "1"
MIN_CLEAN_SHADOW_RUNS = 2
DEFAULT_MAX_BASELINE_AGE_NS = 7 * 24 * 60 * 60 * 1_000_000_000


class TestSieveStore:
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
                CREATE TABLE IF NOT EXISTS repo_state (
                    repository TEXT PRIMARY KEY,
                    mode TEXT NOT NULL CHECK (mode IN ('RECORD', 'SHADOW', 'SELECT')),
                    head_revision INTEGER NOT NULL DEFAULT 0 CHECK (head_revision >= 0),
                    shadow_passes INTEGER NOT NULL DEFAULT 0 CHECK (shadow_passes >= 0),
                    false_negatives INTEGER NOT NULL DEFAULT 0 CHECK (false_negatives >= 0),
                    audit_sequence INTEGER NOT NULL DEFAULT 0 CHECK (audit_sequence >= 0),
                    audit_head_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS baselines (
                    repository TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision > 0),
                    parent_revision INTEGER,
                    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUPERSEDED', 'QUARANTINED')),
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    snapshot_digest TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    PRIMARY KEY (repository, revision),
                    UNIQUE (repository, idempotency_key),
                    FOREIGN KEY (repository) REFERENCES repo_state(repository)
                );
                CREATE TABLE IF NOT EXISTS selections (
                    repository TEXT NOT NULL,
                    selection_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    baseline_revision INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    decision_json TEXT NOT NULL,
                    decision_digest TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    PRIMARY KEY (repository, selection_id),
                    UNIQUE (repository, idempotency_key),
                    FOREIGN KEY (repository, baseline_revision)
                        REFERENCES baselines(repository, revision)
                );
                CREATE TABLE IF NOT EXISTS shadow_results (
                    repository TEXT NOT NULL,
                    selection_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    actual_failures_json TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    PRIMARY KEY (repository, selection_id),
                    UNIQUE (repository, idempotency_key),
                    FOREIGN KEY (repository, selection_id)
                        REFERENCES selections(repository, selection_id)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    repository TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    action TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    created_ns INTEGER NOT NULL CHECK (created_ns >= 0),
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL,
                    PRIMARY KEY (repository, sequence),
                    UNIQUE (repository, entry_hash),
                    FOREIGN KEY (repository) REFERENCES repo_state(repository)
                );
                CREATE TRIGGER IF NOT EXISTS immutable_baseline_update
                BEFORE UPDATE OF snapshot_json, snapshot_digest, request_digest ON baselines BEGIN
                    SELECT RAISE(ABORT, 'baseline evidence is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS immutable_baseline_delete
                BEFORE DELETE ON baselines BEGIN
                    SELECT RAISE(ABORT, 'baselines are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS immutable_selection_update
                BEFORE UPDATE ON selections BEGIN
                    SELECT RAISE(ABORT, 'selections are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS immutable_selection_delete
                BEFORE DELETE ON selections BEGIN
                    SELECT RAISE(ABORT, 'selections are immutable');
                END;
                """
            )
            fingerprint = hmac.new(
                self.secret, b"testsieve-key-check", hashlib.sha256
            ).hexdigest()
            existing = connection.execute(
                "SELECT value FROM metadata WHERE key = 'secret_fingerprint'"
            ).fetchone()
            if existing is not None and not hmac.compare_digest(existing["value"], fingerprint):
                raise IntegrityError("database was created with a different signing secret")
            connection.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
            connection.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('secret_fingerprint', ?)",
                (fingerprint,),
            )

    @staticmethod
    def _now(value: int | None) -> int:
        return time.time_ns() if value is None else require_timestamp(value, "now_ns")

    @staticmethod
    def _repo(connection: sqlite3.Connection, repository: str) -> sqlite3.Row:
        connection.execute(
            """INSERT OR IGNORE INTO repo_state(
                   repository, mode, audit_head_hash
               ) VALUES (?, 'RECORD', ?)""",
            (repository, ZERO_HASH),
        )
        row = connection.execute(
            "SELECT * FROM repo_state WHERE repository = ?", (repository,)
        ).fetchone()
        assert row is not None
        return row

    def _sign(self, value: Any) -> str:
        return hmac.new(self.secret, canonical_json(value).encode(), hashlib.sha256).hexdigest()

    def _audit(
        self,
        connection: sqlite3.Connection,
        repository: str,
        action: str,
        entity_id: str,
        payload: Any,
        now_ns: int,
    ) -> None:
        state = self._repo(connection, repository)
        sequence = state["audit_sequence"] + 1
        payload_digest = digest(payload)
        material = {
            "repository": repository,
            "sequence": sequence,
            "action": action,
            "entity_id": entity_id,
            "payload_digest": payload_digest,
            "created_ns": now_ns,
            "previous_hash": state["audit_head_hash"],
        }
        entry_hash = self._sign(material)
        connection.execute(
            "INSERT INTO audit_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                repository,
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
            """UPDATE repo_state
               SET audit_sequence = ?, audit_head_hash = ?
               WHERE repository = ?""",
            (sequence, entry_hash, repository),
        )

    def publish_baseline(
        self,
        repository: str,
        idempotency_key: str,
        expected_head_revision: int,
        snapshot_value: Any,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        repository = require_identifier(repository, "repository")
        key = require_identifier(idempotency_key, "idempotency_key")
        expected = require_int(expected_head_revision, "expected_head_revision", minimum=0)
        snapshot = BaselineSnapshot.from_value(snapshot_value)
        now = self._now(now_ns)
        request = {
            "expected_head_revision": expected,
            "snapshot": snapshot.as_dict(),
        }
        request_digest = digest(request)
        snapshot_digest = digest(snapshot.as_dict())
        with self._transaction() as connection:
            state = self._repo(connection, repository)
            existing = connection.execute(
                "SELECT * FROM baselines WHERE repository = ? AND idempotency_key = ?",
                (repository, key),
            ).fetchone()
            if existing:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise IdempotencyConflict("baseline key was reused with different content")
                return self._baseline_result(existing, replayed=True)
            if state["head_revision"] != expected:
                raise ConflictError(
                    f"expected baseline revision {expected}, current revision is {state['head_revision']}"
                )
            revision = expected + 1
            if expected:
                connection.execute(
                    """UPDATE baselines SET status = 'SUPERSEDED'
                       WHERE repository = ? AND revision = ? AND status = 'ACTIVE'""",
                    (repository, expected),
                )
            connection.execute(
                """INSERT INTO baselines VALUES (
                    ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?
                )""",
                (
                    repository,
                    revision,
                    expected or None,
                    key,
                    request_digest,
                    canonical_json(snapshot.as_dict()),
                    snapshot_digest,
                    now,
                ),
            )
            connection.execute(
                """UPDATE repo_state
                   SET head_revision = ?, mode = 'RECORD',
                       shadow_passes = 0, false_negatives = 0
                   WHERE repository = ?""",
                (revision, repository),
            )
            self._audit(
                connection,
                repository,
                "BASELINE_PUBLISHED",
                str(revision),
                {
                    "snapshot_digest": snapshot_digest,
                    "parent_revision": expected or None,
                    "tests": len(snapshot.tests),
                    "files": len(snapshot.inventory),
                },
                now,
            )
            row = connection.execute(
                "SELECT * FROM baselines WHERE repository = ? AND revision = ?",
                (repository, revision),
            ).fetchone()
            assert row is not None
            return self._baseline_result(row, replayed=False)

    @staticmethod
    def _baseline_result(row: sqlite3.Row, *, replayed: bool) -> dict[str, Any]:
        snapshot = json.loads(row["snapshot_json"])
        return {
            "repository": row["repository"],
            "revision": row["revision"],
            "parent_revision": row["parent_revision"],
            "status": row["status"],
            "snapshot_digest": row["snapshot_digest"],
            "completed_commit": snapshot["completed_commit"],
            "test_count": len(snapshot["tests"]),
            "file_count": len(snapshot["inventory"]),
            "replayed": replayed,
        }

    def set_mode(
        self,
        repository: str,
        mode: str,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        repository = require_identifier(repository, "repository")
        if mode not in {"RECORD", "SHADOW", "SELECT"}:
            raise ValidationError("mode must be RECORD, SHADOW, or SELECT")
        now = self._now(now_ns)
        with self._transaction() as connection:
            state = self._repo(connection, repository)
            if mode != "RECORD":
                if not state["head_revision"]:
                    raise StateError("a completed baseline is required")
                baseline = connection.execute(
                    """SELECT status FROM baselines
                       WHERE repository = ? AND revision = ?""",
                    (repository, state["head_revision"]),
                ).fetchone()
                if baseline is None or baseline["status"] != "ACTIVE":
                    raise StateError("the current baseline is not active")
            if mode == "SELECT" and state["shadow_passes"] < MIN_CLEAN_SHADOW_RUNS:
                raise StateError(
                    f"SELECT requires {MIN_CLEAN_SHADOW_RUNS} clean shadow runs"
                )
            previous = state["mode"]
            connection.execute(
                "UPDATE repo_state SET mode = ? WHERE repository = ?", (mode, repository)
            )
            self._audit(
                connection,
                repository,
                "MODE_CHANGED",
                mode,
                {"previous": previous, "current": mode},
                now,
            )
            return {
                "repository": repository,
                "previous_mode": previous,
                "mode": mode,
                "shadow_passes": state["shadow_passes"],
            }

    @staticmethod
    def _worker_plan(
        tests: list[str], test_specs: dict[str, dict[str, Any]], worker_count: int
    ) -> list[dict[str, Any]]:
        workers = [
            {"worker": index + 1, "estimated_ms": 0.0, "tests": []}
            for index in range(worker_count)
        ]
        ordered = sorted(
            tests,
            key=lambda test_id: (-float(test_specs[test_id]["estimated_ms"]), test_id),
        )
        for test_id in ordered:
            target = min(workers, key=lambda item: (item["estimated_ms"], item["worker"]))
            target["tests"].append(test_id)
            target["estimated_ms"] += float(test_specs[test_id]["estimated_ms"])
        for worker in workers:
            worker["estimated_ms"] = round(worker["estimated_ms"], 6)
        return workers

    @classmethod
    def _decision(
        cls,
        snapshot: dict[str, Any],
        current_inventory: dict[str, str],
        mode: str,
        baseline_status: str,
        baseline_revision: int,
        baseline_created_ns: int,
        now_ns: int,
        max_baseline_age_ns: int,
        worker_count: int,
    ) -> dict[str, Any]:
        baseline_inventory = snapshot["inventory"]
        test_specs = {item["test_id"]: item for item in snapshot["tests"]}
        all_tests = sorted(test_specs)
        observations = {
            test_id: set(paths) for test_id, paths in snapshot["observations"].items()
        }
        inverse: dict[str, set[str]] = {}
        for test_id, paths in observations.items():
            for path in paths:
                inverse.setdefault(path, set()).add(test_id)
        changed = sorted(
            path
            for path in set(baseline_inventory) | set(current_inventory)
            if baseline_inventory.get(path) != current_inventory.get(path)
        )
        fallback_reasons: list[str] = []
        if baseline_status != "ACTIVE":
            fallback_reasons.append("baseline_not_active")
        if now_ns - baseline_created_ns > max_baseline_age_ns:
            fallback_reasons.append("baseline_stale")
        root_changes = sorted(set(changed) & set(snapshot["root_files"]))
        if root_changes:
            fallback_reasons.append("root_dependency_changed")
        unknown_changes = sorted(path for path in changed if path not in inverse and path not in root_changes)
        if unknown_changes:
            fallback_reasons.append("changed_file_has_no_observation")

        reasons: dict[str, set[str]] = {test_id: set() for test_id in all_tests}
        candidate: set[str] = set()
        for item in snapshot["tests"]:
            test_id = item["test_id"]
            if item["mandatory"]:
                candidate.add(test_id)
                reasons[test_id].add("mandatory")
            if item["last_status"] == "FAIL":
                candidate.add(test_id)
                reasons[test_id].add("previous_failure")
            if test_id not in observations or not observations[test_id]:
                candidate.add(test_id)
                reasons[test_id].add("unmapped_test")
        for path in changed:
            for test_id in inverse.get(path, set()):
                candidate.add(test_id)
                reasons[test_id].add(f"changed:{path}")
        if fallback_reasons:
            candidate = set(all_tests)
            for test_id in all_tests:
                reasons[test_id].add("safe_full_fallback")
        candidate_tests = sorted(candidate)
        if mode in {"RECORD", "SHADOW"}:
            run_tests = all_tests
            execution_reason = f"{mode.lower()}_runs_full_suite"
        else:
            run_tests = candidate_tests
            execution_reason = "select_mode_impact_subset"
        skipped = sorted(set(all_tests) - set(run_tests))
        candidate_skipped = sorted(set(all_tests) - set(candidate_tests))
        selected_reasons = {
            test_id: sorted(reasons[test_id])
            for test_id in candidate_tests
            if reasons[test_id]
        }
        plan = cls._worker_plan(run_tests, test_specs, worker_count)
        return {
            "baseline_revision": baseline_revision,
            "baseline_commit": snapshot["completed_commit"],
            "mode": mode,
            "execution_reason": execution_reason,
            "changed_files": changed,
            "root_changes": root_changes,
            "unknown_changes": unknown_changes,
            "fallback_reasons": sorted(set(fallback_reasons)),
            "all_test_count": len(all_tests),
            "candidate_tests": candidate_tests,
            "candidate_test_count": len(candidate_tests),
            "candidate_skipped_tests": candidate_skipped,
            "run_tests": run_tests,
            "run_test_count": len(run_tests),
            "skipped_tests": skipped,
            "selection_fraction": round(len(candidate_tests) / len(all_tests), 6),
            "reasons": selected_reasons,
            "worker_plan": plan,
            "worker_count": worker_count,
        }

    def select_tests(
        self,
        repository: str,
        idempotency_key: str,
        current_inventory_value: Any,
        *,
        worker_count: int = 4,
        max_baseline_age_ns: int = DEFAULT_MAX_BASELINE_AGE_NS,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        repository = require_identifier(repository, "repository")
        key = require_identifier(idempotency_key, "idempotency_key")
        inventory = normalize_inventory(current_inventory_value)
        workers = require_int(worker_count, "worker_count", minimum=1, maximum=64)
        max_age = require_int(max_baseline_age_ns, "max_baseline_age_ns", minimum=0)
        now = self._now(now_ns)
        request = {
            "current_inventory": inventory,
            "worker_count": workers,
            "max_baseline_age_ns": max_age,
        }
        request_digest = digest(request)
        with self._transaction() as connection:
            state = self._repo(connection, repository)
            existing = connection.execute(
                "SELECT * FROM selections WHERE repository = ? AND idempotency_key = ?",
                (repository, key),
            ).fetchone()
            if existing:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise IdempotencyConflict("selection key was reused with different input")
                return self._selection_result(existing, replayed=True)
            if not state["head_revision"]:
                raise StateError("no completed baseline is available")
            baseline = connection.execute(
                """SELECT * FROM baselines
                   WHERE repository = ? AND revision = ?""",
                (repository, state["head_revision"]),
            ).fetchone()
            if baseline is None:
                raise IntegrityError("head baseline is missing")
            snapshot = json.loads(baseline["snapshot_json"])
            decision = self._decision(
                snapshot,
                inventory,
                state["mode"],
                baseline["status"],
                baseline["revision"],
                baseline["created_ns"],
                now,
                max_age,
                workers,
            )
            decision_digest = digest(decision)
            selection_id = uuid.uuid4().hex
            payload = {
                "version": 1,
                "repository": repository,
                "selection_id": selection_id,
                "baseline_revision": baseline["revision"],
                "baseline_snapshot_digest": baseline["snapshot_digest"],
                "mode": state["mode"],
                "current_inventory_digest": digest(inventory),
                "decision_digest": decision_digest,
            }
            manifest = {"payload": payload, "signature": self._sign(payload)}
            connection.execute(
                """INSERT INTO selections VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )""",
                (
                    repository,
                    selection_id,
                    key,
                    canonical_json(request),
                    request_digest,
                    baseline["revision"],
                    state["mode"],
                    canonical_json(decision),
                    decision_digest,
                    canonical_json(manifest),
                    now,
                ),
            )
            self._audit(
                connection,
                repository,
                "TESTS_SELECTED",
                selection_id,
                {
                    "decision_digest": decision_digest,
                    "baseline_revision": baseline["revision"],
                    "mode": state["mode"],
                    "candidate_test_count": decision["candidate_test_count"],
                    "run_test_count": decision["run_test_count"],
                },
                now,
            )
            row = connection.execute(
                "SELECT * FROM selections WHERE repository = ? AND selection_id = ?",
                (repository, selection_id),
            ).fetchone()
            assert row is not None
            return self._selection_result(row, replayed=False)

    @staticmethod
    def _selection_result(row: sqlite3.Row, *, replayed: bool) -> dict[str, Any]:
        return {
            "selection_id": row["selection_id"],
            "baseline_revision": row["baseline_revision"],
            "mode": row["mode"],
            "decision": json.loads(row["decision_json"]),
            "manifest": json.loads(row["manifest_json"]),
            "replayed": replayed,
        }

    def record_shadow_result(
        self,
        repository: str,
        selection_id: str,
        idempotency_key: str,
        actual_failures_value: Any,
        *,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        repository = require_identifier(repository, "repository")
        selection_id = require_identifier(selection_id, "selection_id")
        key = require_identifier(idempotency_key, "idempotency_key")
        now = self._now(now_ns)
        with self._transaction() as connection:
            selection = connection.execute(
                """SELECT * FROM selections
                   WHERE repository = ? AND selection_id = ?""",
                (repository, selection_id),
            ).fetchone()
            if selection is None:
                raise NotFoundError("selection was not found")
            decision = json.loads(selection["decision_json"])
            failures = normalize_test_ids(
                actual_failures_value,
                decision["candidate_tests"] + decision["candidate_skipped_tests"],
                "actual_failures",
            )
            request_digest = digest(
                {"selection_id": selection_id, "actual_failures": list(failures)}
            )
            existing_selection = connection.execute(
                "SELECT * FROM shadow_results WHERE repository = ? AND selection_id = ?",
                (repository, selection_id),
            ).fetchone()
            if existing_selection:
                if existing_selection["idempotency_key"] != key or not hmac.compare_digest(
                    existing_selection["request_digest"], request_digest
                ):
                    raise IdempotencyConflict("selection already has a different shadow result")
                return {**json.loads(existing_selection["result_json"]), "replayed": True}
            existing_key = connection.execute(
                "SELECT * FROM shadow_results WHERE repository = ? AND idempotency_key = ?",
                (repository, key),
            ).fetchone()
            if existing_key:
                if not hmac.compare_digest(existing_key["request_digest"], request_digest):
                    raise IdempotencyConflict("shadow-result key was reused with different input")
                return {**json.loads(existing_key["result_json"]), "replayed": True}
            if selection["mode"] != "SHADOW":
                raise StateError("shadow results are accepted only for SHADOW selections")
            state = self._repo(connection, repository)
            if selection["baseline_revision"] != state["head_revision"]:
                raise ConflictError("selection baseline is no longer current")
            missed = sorted(set(failures) & set(decision["candidate_skipped_tests"]))
            safe = not missed
            if safe:
                connection.execute(
                    """UPDATE repo_state SET shadow_passes = shadow_passes + 1
                       WHERE repository = ?""",
                    (repository,),
                )
            else:
                connection.execute(
                    """UPDATE baselines SET status = 'QUARANTINED'
                       WHERE repository = ? AND revision = ?""",
                    (repository, selection["baseline_revision"]),
                )
                connection.execute(
                    """UPDATE repo_state
                       SET mode = 'RECORD', shadow_passes = 0,
                           false_negatives = false_negatives + 1
                       WHERE repository = ?""",
                    (repository,),
                )
            updated = self._repo(connection, repository)
            result = {
                "selection_id": selection_id,
                "baseline_revision": selection["baseline_revision"],
                "safe": safe,
                "actual_failures": list(failures),
                "missed_failures": missed,
                "baseline_status": "ACTIVE" if safe else "QUARANTINED",
                "mode": updated["mode"],
                "shadow_passes": updated["shadow_passes"],
            }
            connection.execute(
                "INSERT INTO shadow_results VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    repository,
                    selection_id,
                    key,
                    request_digest,
                    canonical_json(list(failures)),
                    canonical_json(result),
                    now,
                ),
            )
            self._audit(
                connection,
                repository,
                "SHADOW_VALIDATED",
                selection_id,
                result,
                now,
            )
            return {**result, "replayed": False}

    def verify_manifest(
        self, manifest: Any, *, expected_repository: str | None = None
    ) -> dict[str, Any]:
        body = require_closed_object(manifest, "manifest", {"payload", "signature"})
        payload = body["payload"]
        signature = body["signature"]
        if not isinstance(payload, dict) or not isinstance(signature, str):
            raise ValidationError("manifest payload and signature have invalid types")
        required = {
            "version",
            "repository",
            "selection_id",
            "baseline_revision",
            "baseline_snapshot_digest",
            "mode",
            "current_inventory_digest",
            "decision_digest",
        }
        if set(payload) != required:
            raise ValidationError("manifest payload schema is invalid")
        require_int(payload["version"], "manifest.version", minimum=1, maximum=1)
        repository = require_identifier(payload["repository"], "repository")
        selection_id = require_identifier(payload["selection_id"], "selection_id")
        require_int(payload["baseline_revision"], "baseline_revision", minimum=1)
        require_digest(payload["baseline_snapshot_digest"], "baseline_snapshot_digest")
        if payload["mode"] not in {"RECORD", "SHADOW", "SELECT"}:
            raise ValidationError("manifest mode is invalid")
        require_digest(payload["current_inventory_digest"], "current_inventory_digest")
        require_digest(payload["decision_digest"], "decision_digest")
        require_digest(signature, "signature")
        if expected_repository is not None and repository != require_identifier(
            expected_repository, "expected_repository"
        ):
            raise NotFoundError("manifest does not belong to this repository")
        if not hmac.compare_digest(signature, self._sign(payload)):
            raise IntegrityError("manifest signature is invalid")
        with self._connect() as connection:
            row = connection.execute(
                """SELECT decision_digest, manifest_json FROM selections
                   WHERE repository = ? AND selection_id = ?""",
                (repository, selection_id),
            ).fetchone()
        if row is None or not hmac.compare_digest(
            row["decision_digest"], payload["decision_digest"]
        ):
            raise NotFoundError("manifest selection was not found")
        if not hmac.compare_digest(row["manifest_json"], canonical_json(body)):
            raise IntegrityError("manifest does not match the archived selection")
        return {"valid": True, "payload": payload}

    def latest_baseline(self, repository: str) -> dict[str, Any]:
        repository = require_identifier(repository, "repository")
        with self._connect() as connection:
            state = connection.execute(
                "SELECT * FROM repo_state WHERE repository = ?", (repository,)
            ).fetchone()
            if state is None or not state["head_revision"]:
                raise NotFoundError("baseline was not found")
            row = connection.execute(
                "SELECT * FROM baselines WHERE repository = ? AND revision = ?",
                (repository, state["head_revision"]),
            ).fetchone()
            assert row is not None
            return {
                **self._baseline_result(row, replayed=False),
                "mode": state["mode"],
                "shadow_passes": state["shadow_passes"],
                "false_negatives": state["false_negatives"],
            }

    def stats(self, repository: str) -> dict[str, Any]:
        repository = require_identifier(repository, "repository")
        with self._connect() as connection:
            state = connection.execute(
                "SELECT * FROM repo_state WHERE repository = ?", (repository,)
            ).fetchone()
            if state is None:
                raise NotFoundError("repository was not found")
            counts = {}
            for name, table in (
                ("baselines", "baselines"),
                ("selections", "selections"),
                ("shadow_results", "shadow_results"),
                ("audit_entries", "audit_log"),
            ):
                counts[name] = connection.execute(
                    f"SELECT COUNT(*) AS count FROM {table} WHERE repository = ?",
                    (repository,),
                ).fetchone()["count"]
            return {**dict(state), **counts}

    def verify_integrity(self, repository: str | None = None) -> dict[str, Any]:
        if repository is not None:
            repository = require_identifier(repository, "repository")
        totals = {
            "repositories": 0,
            "baselines": 0,
            "selections": 0,
            "shadow_results": 0,
            "audit_entries": 0,
        }
        with self._transaction(immediate=False) as connection:
            query = "SELECT * FROM repo_state"
            params: tuple[Any, ...] = ()
            if repository is not None:
                query += " WHERE repository = ?"
                params = (repository,)
            states = connection.execute(query + " ORDER BY repository", params).fetchall()
            if repository is not None and not states:
                raise NotFoundError("repository was not found")
            for state in states:
                repo = state["repository"]
                totals["repositories"] += 1
                baselines = connection.execute(
                    "SELECT * FROM baselines WHERE repository = ? ORDER BY revision",
                    (repo,),
                ).fetchall()
                for index, row in enumerate(baselines, 1):
                    if row["revision"] != index:
                        raise IntegrityError("baseline revisions are not contiguous")
                    expected_parent = index - 1 or None
                    if row["parent_revision"] != expected_parent:
                        raise IntegrityError("baseline parent revision is invalid")
                    snapshot = json.loads(row["snapshot_json"])
                    normalized = BaselineSnapshot.from_value(snapshot).as_dict()
                    if digest(normalized) != row["snapshot_digest"]:
                        raise IntegrityError("baseline snapshot digest is invalid")
                    request = {
                        "expected_head_revision": index - 1,
                        "snapshot": normalized,
                    }
                    if digest(request) != row["request_digest"]:
                        raise IntegrityError("baseline request digest is invalid")
                if state["head_revision"] != len(baselines):
                    raise IntegrityError("materialized baseline head is invalid")
                if baselines and baselines[-1]["status"] == "SUPERSEDED":
                    raise IntegrityError("head baseline cannot be superseded")
                totals["baselines"] += len(baselines)

                selections = connection.execute(
                    "SELECT * FROM selections WHERE repository = ?", (repo,)
                ).fetchall()
                for row in selections:
                    request = json.loads(row["request_json"])
                    if digest(request) != row["request_digest"]:
                        raise IntegrityError("selection request digest is invalid")
                    decision = json.loads(row["decision_json"])
                    if digest(decision) != row["decision_digest"]:
                        raise IntegrityError("selection decision digest is invalid")
                    manifest = json.loads(row["manifest_json"])
                    if not hmac.compare_digest(
                        manifest.get("signature", ""), self._sign(manifest.get("payload", {}))
                    ):
                        raise IntegrityError("selection manifest signature is invalid")
                    if manifest["payload"].get("decision_digest") != row["decision_digest"]:
                        raise IntegrityError("manifest is detached from decision")
                    if manifest["payload"].get("selection_id") != row["selection_id"]:
                        raise IntegrityError("manifest selection identity is invalid")
                    if manifest["payload"].get("repository") != repo:
                        raise IntegrityError("manifest repository identity is invalid")
                    if manifest["payload"].get("baseline_revision") != row["baseline_revision"]:
                        raise IntegrityError("manifest baseline revision is invalid")
                    if manifest["payload"].get("version") != 1:
                        raise IntegrityError("manifest version is invalid")
                    if manifest["payload"].get("mode") != row["mode"]:
                        raise IntegrityError("manifest mode is invalid")
                    baseline_row = connection.execute(
                        """SELECT snapshot_digest FROM baselines
                           WHERE repository = ? AND revision = ?""",
                        (repo, row["baseline_revision"]),
                    ).fetchone()
                    if baseline_row is None or manifest["payload"].get(
                        "baseline_snapshot_digest"
                    ) != baseline_row["snapshot_digest"]:
                        raise IntegrityError("manifest baseline snapshot is invalid")
                    if manifest["payload"].get("current_inventory_digest") != digest(
                        request["current_inventory"]
                    ):
                        raise IntegrityError("manifest inventory digest is invalid")
                totals["selections"] += len(selections)

                shadow_rows = connection.execute(
                    "SELECT * FROM shadow_results WHERE repository = ?", (repo,)
                ).fetchall()
                for row in shadow_rows:
                    failures = json.loads(row["actual_failures_json"])
                    request = {
                        "selection_id": row["selection_id"],
                        "actual_failures": failures,
                    }
                    if digest(request) != row["request_digest"]:
                        raise IntegrityError("shadow-result request digest is invalid")
                    result = json.loads(row["result_json"])
                    if result.get("actual_failures") != failures:
                        raise IntegrityError("shadow result is detached from actual failures")
                    selection_row = connection.execute(
                        """SELECT baseline_revision, decision_json FROM selections
                           WHERE repository = ? AND selection_id = ?""",
                        (repo, row["selection_id"]),
                    ).fetchone()
                    if selection_row is None:
                        raise IntegrityError("shadow result selection is missing")
                    decision = json.loads(selection_row["decision_json"])
                    missed = sorted(
                        set(failures) & set(decision["candidate_skipped_tests"])
                    )
                    expected_result = {
                        "selection_id": row["selection_id"],
                        "baseline_revision": selection_row["baseline_revision"],
                        "safe": not missed,
                        "actual_failures": failures,
                        "missed_failures": missed,
                        "baseline_status": "ACTIVE" if not missed else "QUARANTINED",
                    }
                    if any(result.get(key) != value for key, value in expected_result.items()):
                        raise IntegrityError("shadow result is inconsistent with its decision")
                totals["shadow_results"] += len(shadow_rows)

                previous = ZERO_HASH
                audits = connection.execute(
                    "SELECT * FROM audit_log WHERE repository = ? ORDER BY sequence", (repo,)
                ).fetchall()
                for expected_sequence, row in enumerate(audits, 1):
                    if row["sequence"] != expected_sequence or row["previous_hash"] != previous:
                        raise IntegrityError("audit sequence or previous hash is invalid")
                    material = {
                        "repository": repo,
                        "sequence": row["sequence"],
                        "action": row["action"],
                        "entity_id": row["entity_id"],
                        "payload_digest": row["payload_digest"],
                        "created_ns": row["created_ns"],
                        "previous_hash": previous,
                    }
                    expected_hash = self._sign(material)
                    if not hmac.compare_digest(expected_hash, row["entry_hash"]):
                        raise IntegrityError("audit entry hash is invalid")
                    previous = row["entry_hash"]
                if state["audit_sequence"] != len(audits) or state["audit_head_hash"] != previous:
                    raise IntegrityError("materialized audit head is invalid")
                totals["audit_entries"] += len(audits)
        return {"valid": True, **totals}
