"""Durable change-control state machine with bound approvals and crash recovery."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

from .actuator import Actuator, ActuatorConflict
from .graph import analyze
from .model import ID_RE, ValidationError, canonical, digest, parse_plan


class AuthorizationError(PermissionError):
    pass


class ConflictError(RuntimeError):
    pass


class NotFoundError(LookupError):
    pass


class SimulatedCrash(RuntimeError):
    pass


ROLES = {"requester", "reviewer", "executor"}


class Controller:
    def __init__(
        self,
        database: str | Path,
        actuator: Actuator,
        *,
        analyzer: Callable[[Any, Any], dict[str, Any]] = analyze,
        required_approvals: int = 2,
    ):
        if not 1 <= required_approvals <= 5:
            raise ValueError("required_approvals must be in [1, 5]")
        self.database = str(database)
        self.actuator = actuator
        self.analyzer = analyzer
        self.required_approvals = required_approvals
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        existed = os.path.exists(self.database)
        connection = sqlite3.connect(self.database, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        if not existed and self.database != ":memory:":
            os.chmod(self.database, 0o600)
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS operators(
                    operator_id TEXT PRIMARY KEY,
                    role TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE,
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1))
                );
                CREATE TABLE IF NOT EXISTS plans(
                    plan_id TEXT PRIMARY KEY,
                    document TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    creator TEXT NOT NULL REFERENCES operators(operator_id),
                    state TEXT NOT NULL,
                    analysis TEXT,
                    analysis_hash TEXT,
                    analyzed_version INTEGER,
                    next_step INTEGER NOT NULL DEFAULT 0,
                    awaiting_observation INTEGER NOT NULL DEFAULT 0 CHECK(awaiting_observation IN (0, 1)),
                    applied TEXT NOT NULL DEFAULT '[]',
                    rollback_index INTEGER NOT NULL DEFAULT 0,
                    rollback_base_version INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS approvals(
                    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
                    operator_id TEXT NOT NULL REFERENCES operators(operator_id),
                    analysis_hash TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY(plan_id, operator_id)
                );
                CREATE TABLE IF NOT EXISTS audit(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    event TEXT NOT NULL,
                    plan_id TEXT,
                    details TEXT NOT NULL
                );
                """
            )

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def register_operator(self, operator_id: str, role: str, token: str) -> None:
        if not isinstance(operator_id, str) or not ID_RE.fullmatch(operator_id):
            raise ValidationError("invalid operator id")
        if role not in ROLES:
            raise ValidationError("invalid operator role")
        if not isinstance(token, str) or len(token) < 32:
            raise ValidationError("operator token must contain at least 32 characters")
        token_hash = self._token_hash(token)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT role, token_hash FROM operators WHERE operator_id = ?", (operator_id,)
            ).fetchone()
            if row:
                if row["role"] != role or not hmac.compare_digest(row["token_hash"], token_hash):
                    raise ConflictError("operator id is already registered differently")
                return
            connection.execute(
                "INSERT INTO operators(operator_id, role, token_hash) VALUES(?, ?, ?)",
                (operator_id, role, token_hash),
            )
            self._audit(connection, operator_id, "OPERATOR_REGISTERED", None, {"role": role})

    def revoke_operator(self, actor_token: str, operator_id: str) -> None:
        actor, role = self._authenticate(actor_token)
        if role != "executor":
            raise AuthorizationError("executor role required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                "UPDATE operators SET active = 0 WHERE operator_id = ?", (operator_id,)
            ).rowcount
            if not changed:
                raise NotFoundError("operator not found")
            self._audit(connection, actor, "OPERATOR_REVOKED", None, {"operator_id": operator_id})

    def _authenticate(self, token: str) -> tuple[str, str]:
        if not isinstance(token, str) or not token:
            raise AuthorizationError("missing bearer token")
        supplied = self._token_hash(token)
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT operator_id, role, token_hash FROM operators WHERE active = 1"
            ).fetchall()
        for row in rows:
            if hmac.compare_digest(row["token_hash"], supplied):
                return row["operator_id"], row["role"]
        raise AuthorizationError("invalid bearer token")

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        actor: str,
        event: str,
        plan_id: str | None,
        details: dict[str, Any],
    ) -> None:
        connection.execute(
            "INSERT INTO audit(occurred_at, actor, event, plan_id, details) VALUES(?, ?, ?, ?, ?)",
            (int(time.time()), actor, event, plan_id, canonical(details)),
        )

    @staticmethod
    def _plan_from_row(row: sqlite3.Row) -> dict[str, Any]:
        document = json.loads(row["document"])
        analysis = json.loads(row["analysis"]) if row["analysis"] else None
        applied = json.loads(row["applied"])
        return {
            "plan_id": row["plan_id"],
            "document": document,
            "plan_hash": row["plan_hash"],
            "creator": row["creator"],
            "state": row["state"],
            "analysis": analysis,
            "analysis_hash": row["analysis_hash"],
            "analyzed_version": row["analyzed_version"],
            "next_step": row["next_step"],
            "awaiting_observation": bool(row["awaiting_observation"]),
            "applied": applied,
            "rollback_index": row["rollback_index"],
            "rollback_base_version": row["rollback_base_version"],
            "error_code": row["error_code"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _get(self, plan_id: str, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
        owns = connection is None
        conn = connection or self._connect()
        try:
            row = conn.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
            if row is None:
                raise NotFoundError("plan not found")
            plan = self._plan_from_row(row)
            plan["approval_count"] = conn.execute(
                "SELECT COUNT(*) AS count FROM approvals WHERE plan_id = ? AND analysis_hash = ?",
                (plan_id, plan["analysis_hash"] or ""),
            ).fetchone()["count"]
            return plan
        finally:
            if owns:
                conn.close()

    def create_plan(self, token: str, plan_value: Any) -> dict[str, Any]:
        actor, role = self._authenticate(token)
        if role != "requester":
            raise AuthorizationError("requester role required")
        plan = parse_plan(plan_value)
        document = canonical(plan)
        now = int(time.time())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM plans WHERE plan_id = ?", (plan["plan_id"],)
            ).fetchone()
            if existing:
                if existing["document"] != document or existing["creator"] != actor:
                    raise ConflictError("plan id is already used")
                return self._get(plan["plan_id"], connection)
            connection.execute(
                """
                INSERT INTO plans(
                    plan_id, document, plan_hash, creator, state, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'DRAFT', ?, ?)
                """,
                (plan["plan_id"], document, digest(plan), actor, now, now),
            )
            self._audit(connection, actor, "PLAN_CREATED", plan["plan_id"], {"plan_hash": digest(plan)})
            return self._get(plan["plan_id"], connection)

    def analyze_plan(self, token: str, plan_id: str) -> dict[str, Any]:
        actor, role = self._authenticate(token)
        if role not in {"requester", "executor"}:
            raise AuthorizationError("requester or executor role required")
        current = self._get(plan_id)
        if current["state"] in {"EXECUTING", "AWAITING_OBSERVATION", "SUCCEEDED", "ROLLING_BACK", "ROLLED_BACK"}:
            raise ConflictError("plan can no longer be analyzed")
        snapshot = self.actuator.snapshot()
        try:
            result = self.analyzer(snapshot, current["document"])
            if not isinstance(result, dict) or not isinstance(result.get("safe"), bool):
                raise TypeError("analyzer returned an invalid result")
        except Exception as exc:
            result = {
                "safe": False,
                "plan_hash": current["plan_hash"],
                "snapshot_version": snapshot["version"],
                "violations": [{"code": "VALIDATOR_FAILURE", "kind": type(exc).__name__}],
                "steps": [],
            }
        analysis_hash = digest({"plan_hash": current["plan_hash"], "analysis": result})
        state = "ANALYZED" if result["safe"] else "BLOCKED"
        now = int(time.time())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
            if row is None:
                raise NotFoundError("plan not found")
            if row["state"] in {"EXECUTING", "AWAITING_OBSERVATION", "SUCCEEDED", "ROLLING_BACK", "ROLLED_BACK"}:
                raise ConflictError("plan changed while analysis was running")
            connection.execute("DELETE FROM approvals WHERE plan_id = ?", (plan_id,))
            connection.execute(
                """
                UPDATE plans
                SET state = ?, analysis = ?, analysis_hash = ?, analyzed_version = ?,
                    next_step = 0, awaiting_observation = 0, applied = '[]',
                    rollback_index = 0, rollback_base_version = NULL,
                    error_code = NULL, updated_at = ?
                WHERE plan_id = ?
                """,
                (
                    state,
                    canonical(result),
                    analysis_hash,
                    result["snapshot_version"],
                    now,
                    plan_id,
                ),
            )
            self._audit(
                connection,
                actor,
                "PLAN_ANALYZED",
                plan_id,
                {"safe": result["safe"], "analysis_hash": analysis_hash},
            )
            return self._get(plan_id, connection)

    def approve(self, token: str, plan_id: str) -> dict[str, Any]:
        actor, role = self._authenticate(token)
        if role != "reviewer":
            raise AuthorizationError("reviewer role required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            plan = self._get(plan_id, connection)
            if plan["creator"] == actor:
                raise AuthorizationError("plan creator cannot approve their own plan")
            if plan["state"] not in {"ANALYZED", "APPROVED"} or not plan["analysis"]["safe"]:
                raise ConflictError("only a safe analyzed plan can be approved")
            connection.execute(
                """
                INSERT INTO approvals(plan_id, operator_id, analysis_hash, created_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(plan_id, operator_id) DO UPDATE SET
                    analysis_hash = excluded.analysis_hash,
                    created_at = excluded.created_at
                """,
                (plan_id, actor, plan["analysis_hash"], int(time.time())),
            )
            count = connection.execute(
                "SELECT COUNT(*) AS count FROM approvals WHERE plan_id = ? AND analysis_hash = ?",
                (plan_id, plan["analysis_hash"]),
            ).fetchone()["count"]
            new_state = "APPROVED" if count >= self.required_approvals else "ANALYZED"
            connection.execute(
                "UPDATE plans SET state = ?, updated_at = ? WHERE plan_id = ?",
                (new_state, int(time.time()), plan_id),
            )
            self._audit(
                connection,
                actor,
                "PLAN_APPROVED",
                plan_id,
                {"analysis_hash": plan["analysis_hash"], "approval_count": count},
            )
            return self._get(plan_id, connection)

    def execute_next(
        self,
        token: str,
        plan_id: str,
        *,
        crash_after_actuation: bool = False,
    ) -> dict[str, Any]:
        actor, role = self._authenticate(token)
        if role != "executor":
            raise AuthorizationError("executor role required")
        plan = self._get(plan_id)
        if plan["state"] not in {"APPROVED", "EXECUTING"}:
            raise ConflictError("plan is not ready for execution")
        if plan["awaiting_observation"]:
            raise ConflictError("health observation is required before the next step")
        if plan["approval_count"] < self.required_approvals:
            raise ConflictError("approval quorum is no longer valid")
        if not plan["analysis"] or not plan["analysis"]["safe"]:
            raise ConflictError("safe analysis is required")
        index = plan["next_step"]
        steps = plan["document"]["steps"]
        if index >= len(steps):
            raise ConflictError("all plan steps are already applied")
        step = steps[index]
        operation_id = f"apply:{plan_id}:{plan['analysis_hash']}:{index}"
        expected_version = int(plan["analyzed_version"]) + index
        try:
            outcome = self.actuator.apply(
                operation_id,
                step["link_id"],
                step["up"],
                expected_version,
            )
        except ActuatorConflict as exc:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE plans SET state = 'STALE', error_code = 'TOPOLOGY_DRIFT', updated_at = ? WHERE plan_id = ?",
                    (int(time.time()), plan_id),
                )
                self._audit(connection, actor, "EXECUTION_BLOCKED", plan_id, {"code": "TOPOLOGY_DRIFT"})
            raise ConflictError(str(exc)) from exc

        if crash_after_actuation:
            raise SimulatedCrash("injected crash after device acknowledgement")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            latest = self._get(plan_id, connection)
            if latest["next_step"] != index:
                return latest
            if latest["analysis_hash"] != plan["analysis_hash"]:
                raise ConflictError("analysis changed during execution")
            applied = latest["applied"]
            applied.append({
                "index": index,
                "operation_id": operation_id,
                "link_id": step["link_id"],
                "requested_up": step["up"],
                "previous_up": bool(outcome["previous_up"]),
                "new_version": outcome["new_version"],
            })
            connection.execute(
                """
                UPDATE plans
                SET state = 'AWAITING_OBSERVATION', next_step = ?, awaiting_observation = 1,
                    applied = ?, updated_at = ?
                WHERE plan_id = ?
                """,
                (index + 1, canonical(applied), int(time.time()), plan_id),
            )
            self._audit(
                connection,
                actor,
                "STEP_APPLIED",
                plan_id,
                {"index": index, "operation_id": operation_id, "replayed": bool(outcome["replayed"])},
            )
            return self._get(plan_id, connection)

    def observe(self, token: str, plan_id: str, healthy: bool, evidence: str) -> dict[str, Any]:
        actor, role = self._authenticate(token)
        if role != "executor":
            raise AuthorizationError("executor role required")
        if not isinstance(healthy, bool):
            raise ValidationError("healthy must be boolean")
        if not isinstance(evidence, str) or not 3 <= len(evidence) <= 200:
            raise ValidationError("evidence must contain 3..200 characters")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            plan = self._get(plan_id, connection)
            if plan["state"] != "AWAITING_OBSERVATION" or not plan["awaiting_observation"]:
                raise ConflictError("plan is not awaiting an observation")
            complete = plan["next_step"] == len(plan["document"]["steps"])
            state = "SUCCEEDED" if healthy and complete else "EXECUTING" if healthy else "PAUSED"
            connection.execute(
                """
                UPDATE plans
                SET state = ?, awaiting_observation = 0, error_code = ?, updated_at = ?
                WHERE plan_id = ?
                """,
                (state, None if healthy else "UNHEALTHY_CANARY", int(time.time()), plan_id),
            )
            self._audit(
                connection,
                actor,
                "HEALTH_OBSERVED",
                plan_id,
                {"healthy": healthy, "evidence": evidence, "complete": complete},
            )
            return self._get(plan_id, connection)

    def rollback(
        self,
        token: str,
        plan_id: str,
        *,
        crash_after_actuation: bool = False,
    ) -> dict[str, Any]:
        actor, role = self._authenticate(token)
        if role != "executor":
            raise AuthorizationError("executor role required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            plan = self._get(plan_id, connection)
            if not plan["applied"]:
                raise ConflictError("plan has no applied operations")
            if plan["state"] == "ROLLED_BACK":
                return plan
            if plan["state"] not in {
                "PAUSED", "SUCCEEDED", "EXECUTING", "AWAITING_OBSERVATION",
                "ROLLING_BACK", "ROLLBACK_BLOCKED"
            }:
                raise ConflictError("plan cannot be rolled back from its current state")
            if plan["state"] != "ROLLING_BACK":
                base = self.actuator.snapshot()["version"]
                connection.execute(
                    """
                    UPDATE plans
                    SET state = 'ROLLING_BACK', rollback_base_version = ?,
                        awaiting_observation = 0, error_code = NULL, updated_at = ?
                    WHERE plan_id = ?
                    """,
                    (base, int(time.time()), plan_id),
                )

        while True:
            plan = self._get(plan_id)
            index = plan["rollback_index"]
            applied = plan["applied"]
            if index >= len(applied):
                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE plans SET state = 'ROLLED_BACK', updated_at = ? WHERE plan_id = ?",
                        (int(time.time()), plan_id),
                    )
                    self._audit(connection, actor, "PLAN_ROLLED_BACK", plan_id, {"steps": len(applied)})
                    return self._get(plan_id, connection)
            original = applied[len(applied) - 1 - index]
            operation_id = f"rollback:{plan_id}:{plan['analysis_hash']}:{original['index']}"
            expected_version = int(plan["rollback_base_version"]) + index
            try:
                outcome = self.actuator.apply(
                    operation_id,
                    original["link_id"],
                    original["previous_up"],
                    expected_version,
                )
            except ActuatorConflict as exc:
                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE plans SET state = 'ROLLBACK_BLOCKED', error_code = 'TOPOLOGY_DRIFT', updated_at = ? WHERE plan_id = ?",
                        (int(time.time()), plan_id),
                    )
                    self._audit(connection, actor, "ROLLBACK_BLOCKED", plan_id, {"code": "TOPOLOGY_DRIFT"})
                raise ConflictError(str(exc)) from exc
            if crash_after_actuation:
                raise SimulatedCrash("injected crash after rollback device acknowledgement")
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                latest = self._get(plan_id, connection)
                if latest["rollback_index"] != index:
                    continue
                connection.execute(
                    "UPDATE plans SET rollback_index = ?, updated_at = ? WHERE plan_id = ?",
                    (index + 1, int(time.time()), plan_id),
                )
                self._audit(
                    connection,
                    actor,
                    "ROLLBACK_STEP_APPLIED",
                    plan_id,
                    {"index": original["index"], "replayed": bool(outcome["replayed"])},
                )

    def status(self, token: str, plan_id: str) -> dict[str, Any]:
        self._authenticate(token)
        return self._get(plan_id)

    def audit_log(self, token: str, *, limit: int = 100) -> list[dict[str, Any]]:
        self._authenticate(token)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValidationError("limit must be in [1, 500]")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM audit ORDER BY sequence DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {
                "sequence": row["sequence"],
                "occurred_at": row["occurred_at"],
                "actor": row["actor"],
                "event": row["event"],
                "plan_id": row["plan_id"],
                "details": json.loads(row["details"]),
            }
            for row in rows
        ]

    def health(self) -> dict[str, Any]:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        snapshot = self.actuator.snapshot()
        return {"ok": True, "topology_version": snapshot["version"]}
