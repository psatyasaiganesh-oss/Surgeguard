from __future__ import annotations

import hashlib
import hmac
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    AuthorizationError,
    CapacityError,
    ConflictError,
    IntegrityError,
    NotFoundError,
    SimulatedCrash,
    ValidationError,
)
from .model import (
    LIFETIME_UPPER_S,
    VMRequest,
    canonical_json,
    digest,
    lifetime_class,
    parse_json,
    require_identifier,
    require_int,
    require_policy,
)


ZERO_HASH = "0" * 64
HOST_STATES = {"EMPTY", "OPEN", "RECYCLING", "DRAINING"}
VALID_FAILPOINTS = {None, "after_plan_commit", "after_vm_finish", "after_migration"}


def now_s() -> int:
    return int(time.time())


class ExitPack:
    def __init__(self, path: str | Path, *, secret: bytes) -> None:
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValidationError("secret must contain at least 32 bytes")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.secret = secret
        self._closed = False
        self._schema_lock = threading.Lock()
        self._initialize()

    def __enter__(self) -> "ExitPack":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._closed = True

    def _connect(self) -> sqlite3.Connection:
        if self._closed:
            raise ConflictError("store is closed")
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
                        revision INTEGER NOT NULL DEFAULT 0,
                        audit_sequence INTEGER NOT NULL DEFAULT 0,
                        audit_head_hash TEXT NOT NULL,
                        created_s INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS requests (
                        namespace TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        request_digest TEXT NOT NULL,
                        response_json TEXT NOT NULL,
                        created_s INTEGER NOT NULL,
                        PRIMARY KEY (namespace, operation, idempotency_key),
                        FOREIGN KEY (namespace) REFERENCES namespaces(namespace)
                    );

                    CREATE TABLE IF NOT EXISTS hosts (
                        namespace TEXT NOT NULL,
                        host_id TEXT NOT NULL,
                        zone TEXT NOT NULL,
                        cpu_capacity INTEGER NOT NULL CHECK (cpu_capacity > 0),
                        memory_capacity INTEGER NOT NULL CHECK (memory_capacity > 0),
                        state TEXT NOT NULL CHECK (
                            state IN ('EMPTY', 'OPEN', 'RECYCLING', 'DRAINING')
                        ),
                        lifetime_class INTEGER CHECK (
                            lifetime_class IS NULL OR lifetime_class BETWEEN 0 AND 3
                        ),
                        deadline_s INTEGER,
                        revision INTEGER NOT NULL DEFAULT 0,
                        created_s INTEGER NOT NULL,
                        updated_s INTEGER NOT NULL,
                        PRIMARY KEY (namespace, host_id),
                        FOREIGN KEY (namespace) REFERENCES namespaces(namespace)
                    );

                    CREATE TABLE IF NOT EXISTS vms (
                        namespace TEXT NOT NULL,
                        vm_id TEXT NOT NULL,
                        host_id TEXT NOT NULL,
                        cpu_millis INTEGER NOT NULL CHECK (cpu_millis > 0),
                        memory_mib INTEGER NOT NULL CHECK (memory_mib > 0),
                        predicted_lifetime_s INTEGER NOT NULL CHECK (predicted_lifetime_s > 0),
                        lifetime_class INTEGER NOT NULL CHECK (lifetime_class BETWEEN 0 AND 3),
                        start_s INTEGER NOT NULL,
                        predicted_exit_s INTEGER NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('RUNNING', 'FINISHED')),
                        residual INTEGER NOT NULL CHECK (residual IN (0, 1)),
                        migration_count INTEGER NOT NULL DEFAULT 0,
                        finished_s INTEGER,
                        created_s INTEGER NOT NULL,
                        updated_s INTEGER NOT NULL,
                        PRIMARY KEY (namespace, vm_id),
                        FOREIGN KEY (namespace, host_id)
                            REFERENCES hosts(namespace, host_id)
                    );
                    CREATE INDEX IF NOT EXISTS vms_host_status
                        ON vms(namespace, host_id, status);

                    CREATE TABLE IF NOT EXISTS plans (
                        namespace TEXT NOT NULL,
                        plan_id TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        request_digest TEXT NOT NULL,
                        policy TEXT NOT NULL CHECK (policy IN ('BEST_FIT', 'LIFETIME')),
                        vm_json TEXT NOT NULL,
                        host_id TEXT NOT NULL,
                        cluster_revision INTEGER NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('PLANNED', 'COMMITTED')),
                        created_s INTEGER NOT NULL,
                        committed_s INTEGER,
                        PRIMARY KEY (namespace, plan_id),
                        UNIQUE (namespace, idempotency_key),
                        FOREIGN KEY (namespace, host_id)
                            REFERENCES hosts(namespace, host_id)
                    );

                    CREATE TABLE IF NOT EXISTS audit_log (
                        namespace TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        payload_digest TEXT NOT NULL,
                        previous_hash TEXT NOT NULL,
                        entry_hash TEXT NOT NULL,
                        created_s INTEGER NOT NULL,
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

    @staticmethod
    def _decode(raw: str, name: str) -> Any:
        return parse_json(raw, name)

    def _ensure_namespace(
        self, connection: sqlite3.Connection, namespace: str, created_s: int
    ) -> None:
        connection.execute(
            """INSERT OR IGNORE INTO namespaces
               (namespace, revision, audit_sequence, audit_head_hash, created_s)
               VALUES (?, 0, 0, ?, ?)""",
            (namespace, ZERO_HASH, created_s),
        )

    def _namespace_row(self, connection: sqlite3.Connection, namespace: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM namespaces WHERE namespace = ?", (namespace,)
        ).fetchone()
        if row is None:
            raise NotFoundError("namespace was not found")
        return row

    def _advance_revision(self, connection: sqlite3.Connection, namespace: str) -> int:
        row = self._namespace_row(connection, namespace)
        revision = row["revision"] + 1
        connection.execute(
            "UPDATE namespaces SET revision = ? WHERE namespace = ?",
            (revision, namespace),
        )
        return revision

    def _audit(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        action: str,
        entity_id: str,
        payload: Any,
        created_s: int,
    ) -> None:
        row = self._namespace_row(connection, namespace)
        sequence = row["audit_sequence"] + 1
        payload_digest = digest(payload)
        material = {
            "namespace": namespace,
            "sequence": sequence,
            "action": action,
            "entity_id": entity_id,
            "payload_digest": payload_digest,
            "previous_hash": row["audit_head_hash"],
            "created_s": created_s,
        }
        entry_hash = self._sign(material)
        connection.execute(
            """INSERT INTO audit_log
               (namespace, sequence, action, entity_id, payload_digest,
                previous_hash, entry_hash, created_s)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                namespace,
                sequence,
                action,
                entity_id,
                payload_digest,
                row["audit_head_hash"],
                entry_hash,
                created_s,
            ),
        )
        connection.execute(
            """UPDATE namespaces SET audit_sequence = ?, audit_head_hash = ?
               WHERE namespace = ?""",
            (sequence, entry_hash, namespace),
        )

    def _request_replay(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        operation: str,
        idempotency_key: str,
        request_digest: str,
    ) -> dict[str, Any] | None:
        row = connection.execute(
            """SELECT request_digest, response_json FROM requests
               WHERE namespace = ? AND operation = ? AND idempotency_key = ?""",
            (namespace, operation, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if not hmac.compare_digest(row["request_digest"], request_digest):
            raise ConflictError("idempotency key was reused with different input")
        return {**self._decode(row["response_json"], "stored response"), "replayed": True}

    def _store_request(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        operation: str,
        idempotency_key: str,
        request_digest: str,
        response: dict[str, Any],
        created_s: int,
    ) -> None:
        stored = {key: value for key, value in response.items() if key != "replayed"}
        connection.execute(
            """INSERT INTO requests
               (namespace, operation, idempotency_key, request_digest,
                response_json, created_s)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                namespace,
                operation,
                idempotency_key,
                request_digest,
                canonical_json(stored),
                created_s,
            ),
        )

    def register_host(
        self,
        namespace_value: str,
        idempotency_key_value: str,
        host_id_value: str,
        zone_value: str,
        cpu_capacity_value: int,
        memory_capacity_value: int,
        *,
        current_s: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        idempotency_key = require_identifier(idempotency_key_value, "idempotency_key")
        host_id = require_identifier(host_id_value, "host_id")
        zone = require_identifier(zone_value, "zone")
        cpu = require_int(cpu_capacity_value, "cpu_capacity", minimum=1, maximum=10_000_000)
        memory = require_int(
            memory_capacity_value, "memory_capacity", minimum=1, maximum=10_000_000
        )
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        request = {"host_id": host_id, "zone": zone, "cpu_capacity": cpu, "memory_capacity": memory}
        request_digest = digest(request)
        with self._transaction() as connection:
            self._ensure_namespace(connection, namespace, current)
            replay = self._request_replay(
                connection, namespace, "REGISTER_HOST", idempotency_key, request_digest
            )
            if replay is not None:
                return replay
            try:
                connection.execute(
                    """INSERT INTO hosts
                       (namespace, host_id, zone, cpu_capacity, memory_capacity,
                        state, lifetime_class, deadline_s, revision, created_s, updated_s)
                       VALUES (?, ?, ?, ?, ?, 'EMPTY', NULL, NULL, 0, ?, ?)""",
                    (namespace, host_id, zone, cpu, memory, current, current),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("host identity already exists") from exc
            revision = self._advance_revision(connection, namespace)
            response = {
                "host_id": host_id,
                "zone": zone,
                "state": "EMPTY",
                "cluster_revision": revision,
                "replayed": False,
            }
            self._store_request(
                connection,
                namespace,
                "REGISTER_HOST",
                idempotency_key,
                request_digest,
                response,
                current,
            )
            self._audit(connection, namespace, "HOST_REGISTERED", host_id, request, current)
            return response

    def _host_usage(
        self, connection: sqlite3.Connection, namespace: str, host_id: str
    ) -> tuple[int, int, int]:
        row = connection.execute(
            """SELECT COALESCE(SUM(cpu_millis), 0) AS cpu,
                      COALESCE(SUM(memory_mib), 0) AS memory,
                      COUNT(*) AS count
               FROM vms WHERE namespace = ? AND host_id = ? AND status = 'RUNNING'""",
            (namespace, host_id),
        ).fetchone()
        return row["cpu"], row["memory"], row["count"]

    @staticmethod
    def _repredicted_exit(vm: sqlite3.Row, current_s: int) -> int:
        original_exit = vm["predicted_exit_s"]
        if current_s <= original_exit:
            return original_exit
        uptime = current_s - vm["start_s"]
        return current_s + max(vm["predicted_lifetime_s"], uptime)

    def _refresh_host(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        host_id: str,
        current_s: int,
    ) -> bool:
        host = connection.execute(
            "SELECT * FROM hosts WHERE namespace = ? AND host_id = ?",
            (namespace, host_id),
        ).fetchone()
        assert host is not None
        # Draining is an operator decision, not a placement-derived state.  In
        # particular, an empty draining host must not be reconciled back to
        # EMPTY and accidentally re-enter the scheduler candidate set.
        if host["state"] == "DRAINING":
            return False
        running = connection.execute(
            """SELECT * FROM vms
               WHERE namespace = ? AND host_id = ? AND status = 'RUNNING'
               ORDER BY vm_id""",
            (namespace, host_id),
        ).fetchall()
        if not running:
            changed = (
                host["state"] != "EMPTY"
                or host["lifetime_class"] is not None
                or host["deadline_s"] is not None
            )
            if changed:
                connection.execute(
                    """UPDATE hosts SET state = 'EMPTY', lifetime_class = NULL,
                       deadline_s = NULL, revision = revision + 1, updated_s = ?
                       WHERE namespace = ? AND host_id = ?""",
                    (current_s, namespace, host_id),
                )
            return changed

        state = host["state"]
        host_class = host["lifetime_class"]
        deadline = host["deadline_s"]
        changed = False
        residuals = [vm for vm in running if vm["residual"]]

        if state == "RECYCLING" and not residuals:
            host_class = max(vm["lifetime_class"] for vm in running)
            deadline = max(self._repredicted_exit(vm, current_s) for vm in running)
            connection.execute(
                """UPDATE vms SET residual = 1, updated_s = ?
                   WHERE namespace = ? AND host_id = ? AND status = 'RUNNING'""",
                (current_s, namespace, host_id),
            )
            residuals = list(running)
            changed = True

        if deadline is not None and current_s > deadline and running:
            next_class = min((host_class if host_class is not None else 0) + 1, 3)
            host_class = next_class
            deadline = current_s + LIFETIME_UPPER_S[next_class]
            state = "RECYCLING"
            connection.execute(
                """UPDATE vms SET residual = 1, updated_s = ?
                   WHERE namespace = ? AND host_id = ? AND status = 'RUNNING'""",
                (current_s, namespace, host_id),
            )
            changed = True

        if changed:
            connection.execute(
                """UPDATE hosts SET state = ?, lifetime_class = ?, deadline_s = ?,
                   revision = revision + 1, updated_s = ?
                   WHERE namespace = ? AND host_id = ?""",
                (state, host_class, deadline, current_s, namespace, host_id),
            )
        return changed

    def _reconcile_all(
        self, connection: sqlite3.Connection, namespace: str, current_s: int
    ) -> list[str]:
        host_ids = [
            row["host_id"]
            for row in connection.execute(
                "SELECT host_id FROM hosts WHERE namespace = ? ORDER BY host_id", (namespace,)
            ).fetchall()
        ]
        changed = [
            host_id
            for host_id in host_ids
            if self._refresh_host(connection, namespace, host_id, current_s)
        ]
        if changed:
            revision = self._advance_revision(connection, namespace)
            self._audit(
                connection,
                namespace,
                "LIFETIME_RECONCILED",
                str(revision),
                {"hosts": changed, "cluster_revision": revision},
                current_s,
            )
        return changed

    def reconcile(
        self, namespace_value: str, *, current_s: int | None = None
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        with self._transaction() as connection:
            self._namespace_row(connection, namespace)
            changed = self._reconcile_all(connection, namespace, current)
            revision = self._namespace_row(connection, namespace)["revision"]
            return {"changed_hosts": changed, "cluster_revision": revision}

    def _eligible_lifetime(self, host: sqlite3.Row, vm_class: int) -> bool:
        if host["state"] == "EMPTY":
            return True
        if host["state"] == "OPEN":
            return host["lifetime_class"] == vm_class
        if host["state"] == "RECYCLING":
            return vm_class < host["lifetime_class"]
        return False

    def _select_host(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        vm: VMRequest,
        policy: str,
        current_s: int,
        exclude_host_id: str | None = None,
    ) -> tuple[sqlite3.Row, dict[str, int | float]]:
        candidates: list[tuple[sqlite3.Row, int, int]] = []
        hosts = connection.execute(
            """SELECT * FROM hosts
               WHERE namespace = ? AND state != 'DRAINING'
               ORDER BY host_id""",
            (namespace,),
        ).fetchall()
        for host in hosts:
            if exclude_host_id is not None and host["host_id"] == exclude_host_id:
                continue
            used_cpu, used_memory, _ = self._host_usage(
                connection, namespace, host["host_id"]
            )
            if (
                used_cpu + vm.cpu_millis <= host["cpu_capacity"]
                and used_memory + vm.memory_mib <= host["memory_capacity"]
            ):
                candidates.append((host, used_cpu, used_memory))
        if not candidates:
            raise CapacityError("no host has enough CPU and memory")

        if policy == "LIFETIME":
            nonempty = [
                item
                for item in candidates
                if item[0]["state"] != "EMPTY"
                and self._eligible_lifetime(item[0], vm.lifetime_class)
            ]
            eligible = nonempty or [item for item in candidates if item[0]["state"] == "EMPTY"]
            if not eligible:
                raise CapacityError("capacity exists, but no host satisfies lifetime-class policy")
        else:
            eligible = candidates

        predicted_exit = current_s + vm.predicted_lifetime_s

        def score(item: tuple[sqlite3.Row, int, int]) -> tuple[float, ...]:
            host, used_cpu, used_memory = item
            cpu_left = host["cpu_capacity"] - used_cpu - vm.cpu_millis
            memory_left = host["memory_capacity"] - used_memory - vm.memory_mib
            waste = cpu_left / host["cpu_capacity"] + memory_left / host["memory_capacity"]
            if policy == "BEST_FIT":
                return (waste, host["host_id"])
            empty_penalty = 1.0 if host["state"] == "EMPTY" else 0.0
            temporal_delta = max(predicted_exit - (host["deadline_s"] or predicted_exit), 0)
            temporal_cost = next(
                (
                    index
                    for index, boundary in enumerate(
                        (0, 1_800, 3_600, 7_200, 14_400, 43_200, 86_400, 604_800)
                    )
                    if temporal_delta <= boundary
                ),
                8,
            )
            return (empty_penalty, float(temporal_cost), waste, host["host_id"])

        selected, used_cpu, used_memory = min(eligible, key=score)
        return selected, {
            "used_cpu_before": used_cpu,
            "used_memory_before": used_memory,
            "cpu_left_after": selected["cpu_capacity"] - used_cpu - vm.cpu_millis,
            "memory_left_after": selected["memory_capacity"] - used_memory - vm.memory_mib,
        }

    def _plan_material(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "namespace": row["namespace"],
            "plan_id": row["plan_id"],
            "policy": row["policy"],
            "vm": self._decode(row["vm_json"], "planned VM"),
            "host_id": row["host_id"],
            "cluster_revision": row["cluster_revision"],
            "created_s": row["created_s"],
        }

    def _plan_response(
        self, row: sqlite3.Row, placement: dict[str, Any] | None = None, *, replayed: bool
    ) -> dict[str, Any]:
        material = self._plan_material(row)
        return {
            "plan_id": row["plan_id"],
            "host_id": row["host_id"],
            "policy": row["policy"],
            "cluster_revision": row["cluster_revision"],
            "status": row["status"],
            "commit_token": self._sign(material),
            "placement": placement,
            "replayed": replayed,
        }

    def create_plan(
        self,
        namespace_value: str,
        idempotency_key_value: str,
        vm_value: Any,
        policy_value: str,
        *,
        expected_revision: int | None = None,
        exclude_host_id: str | None = None,
        current_s: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        idempotency_key = require_identifier(idempotency_key_value, "idempotency_key")
        vm = VMRequest.from_value(vm_value)
        policy = require_policy(policy_value)
        expected = (
            None
            if expected_revision is None
            else require_int(expected_revision, "expected_revision")
        )
        excluded = (
            None
            if exclude_host_id is None
            else require_identifier(exclude_host_id, "exclude_host_id")
        )
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        request = {
            "vm": vm.as_dict(),
            "policy": policy,
            "expected_revision": expected,
            "exclude_host_id": excluded,
        }
        request_digest = digest(request)
        with self._transaction() as connection:
            self._namespace_row(connection, namespace)
            existing = connection.execute(
                """SELECT * FROM plans
                   WHERE namespace = ? AND idempotency_key = ?""",
                (namespace, idempotency_key),
            ).fetchone()
            if existing is not None:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise ConflictError("idempotency key was reused with different input")
                return self._plan_response(existing, replayed=True)
            if connection.execute(
                "SELECT 1 FROM vms WHERE namespace = ? AND vm_id = ?",
                (namespace, vm.vm_id),
            ).fetchone():
                raise ConflictError("VM identity already exists")
            self._reconcile_all(connection, namespace, current)
            revision = self._namespace_row(connection, namespace)["revision"]
            if expected is not None and expected != revision:
                raise ConflictError("cluster revision changed before planning")
            host, placement = self._select_host(
                connection, namespace, vm, policy, current, excluded
            )
            plan_id = f"plan:{uuid.uuid4().hex}"
            connection.execute(
                """INSERT INTO plans
                   (namespace, plan_id, idempotency_key, request_digest, policy,
                    vm_json, host_id, cluster_revision, status, created_s)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PLANNED', ?)""",
                (
                    namespace,
                    plan_id,
                    idempotency_key,
                    request_digest,
                    policy,
                    canonical_json(vm.as_dict()),
                    host["host_id"],
                    revision,
                    current,
                ),
            )
            row = connection.execute(
                "SELECT * FROM plans WHERE namespace = ? AND plan_id = ?",
                (namespace, plan_id),
            ).fetchone()
            assert row is not None
            self._audit(
                connection,
                namespace,
                "PLACEMENT_PLANNED",
                plan_id,
                {**request, "host_id": host["host_id"], "cluster_revision": revision},
                current,
            )
            return self._plan_response(row, placement, replayed=False)

    def _apply_host_after_add(
        self,
        connection: sqlite3.Connection,
        namespace: str,
        host_id: str,
        vm_class: int,
        predicted_exit_s: int,
        current_s: int,
    ) -> None:
        host = connection.execute(
            "SELECT * FROM hosts WHERE namespace = ? AND host_id = ?",
            (namespace, host_id),
        ).fetchone()
        assert host is not None
        state = host["state"]
        host_class = host["lifetime_class"]
        deadline = host["deadline_s"]
        if state == "EMPTY":
            state = "OPEN"
            host_class = vm_class
            deadline = predicted_exit_s
        elif state == "OPEN":
            deadline = max(deadline or predicted_exit_s, predicted_exit_s)
        used_cpu, used_memory, _ = self._host_usage(connection, namespace, host_id)
        if state == "OPEN" and (
            used_cpu / host["cpu_capacity"] >= 0.9
            or used_memory / host["memory_capacity"] >= 0.9
        ):
            state = "RECYCLING"
            connection.execute(
                """UPDATE vms SET residual = 1, updated_s = ?
                   WHERE namespace = ? AND host_id = ? AND status = 'RUNNING'""",
                (current_s, namespace, host_id),
            )
        connection.execute(
            """UPDATE hosts SET state = ?, lifetime_class = ?, deadline_s = ?,
               revision = revision + 1, updated_s = ?
               WHERE namespace = ? AND host_id = ?""",
            (state, host_class, deadline, current_s, namespace, host_id),
        )

    def commit_plan(
        self,
        namespace_value: str,
        plan_id_value: str,
        commit_token_value: str,
        *,
        current_s: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        plan_id = require_identifier(plan_id_value, "plan_id")
        token = require_identifier(commit_token_value, "commit_token")
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        failpoint = self._failpoint(failpoint)
        response: dict[str, Any]
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM plans WHERE namespace = ? AND plan_id = ?",
                (namespace, plan_id),
            ).fetchone()
            if row is None:
                raise NotFoundError("placement plan was not found")
            expected_token = self._sign(self._plan_material(row))
            if not hmac.compare_digest(token, expected_token):
                raise AuthorizationError("placement commit token is invalid")
            if row["status"] == "COMMITTED":
                vm = self._decode(row["vm_json"], "planned VM")
                return {
                    "plan_id": plan_id,
                    "vm_id": vm["vm_id"],
                    "host_id": row["host_id"],
                    "cluster_revision": self._namespace_row(connection, namespace)["revision"],
                    "replayed": True,
                }
            namespace_row = self._namespace_row(connection, namespace)
            if namespace_row["revision"] != row["cluster_revision"]:
                raise ConflictError("placement plan is stale; replan against current cluster state")
            vm = VMRequest.from_value(self._decode(row["vm_json"], "planned VM"))
            host = connection.execute(
                "SELECT * FROM hosts WHERE namespace = ? AND host_id = ?",
                (namespace, row["host_id"]),
            ).fetchone()
            assert host is not None
            used_cpu, used_memory, _ = self._host_usage(
                connection, namespace, row["host_id"]
            )
            if host["state"] == "DRAINING":
                raise ConflictError("planned host is draining")
            if (
                used_cpu + vm.cpu_millis > host["cpu_capacity"]
                or used_memory + vm.memory_mib > host["memory_capacity"]
            ):
                raise ConflictError("planned host no longer has enough capacity")
            if row["policy"] == "LIFETIME" and not self._eligible_lifetime(
                host, vm.lifetime_class
            ):
                raise ConflictError("planned host no longer satisfies lifetime policy")
            predicted_exit = current + vm.predicted_lifetime_s
            connection.execute(
                """INSERT INTO vms
                   (namespace, vm_id, host_id, cpu_millis, memory_mib,
                    predicted_lifetime_s, lifetime_class, start_s, predicted_exit_s,
                    status, residual, migration_count, created_s, updated_s)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', 0, 0, ?, ?)""",
                (
                    namespace,
                    vm.vm_id,
                    row["host_id"],
                    vm.cpu_millis,
                    vm.memory_mib,
                    vm.predicted_lifetime_s,
                    vm.lifetime_class,
                    current,
                    predicted_exit,
                    current,
                    current,
                ),
            )
            self._apply_host_after_add(
                connection,
                namespace,
                row["host_id"],
                vm.lifetime_class,
                predicted_exit,
                current,
            )
            connection.execute(
                """UPDATE plans SET status = 'COMMITTED', committed_s = ?
                   WHERE namespace = ? AND plan_id = ?""",
                (current, namespace, plan_id),
            )
            revision = self._advance_revision(connection, namespace)
            response = {
                "plan_id": plan_id,
                "vm_id": vm.vm_id,
                "host_id": row["host_id"],
                "cluster_revision": revision,
                "replayed": False,
            }
            self._audit(connection, namespace, "PLACEMENT_COMMITTED", plan_id, response, current)
        if failpoint == "after_plan_commit":
            raise SimulatedCrash("simulated lost acknowledgement after placement commit")
        return response

    def schedule(
        self,
        namespace: str,
        idempotency_key: str,
        vm: Any,
        policy: str,
        *,
        current_s: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        plan = self.create_plan(
            namespace,
            idempotency_key,
            vm,
            policy,
            current_s=current_s,
        )
        return self.commit_plan(
            namespace,
            plan["plan_id"],
            plan["commit_token"],
            current_s=current_s,
            failpoint=failpoint,
        )

    def finish_vm(
        self,
        namespace_value: str,
        idempotency_key_value: str,
        vm_id_value: str,
        *,
        current_s: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        idempotency_key = require_identifier(idempotency_key_value, "idempotency_key")
        vm_id = require_identifier(vm_id_value, "vm_id")
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        failpoint = self._failpoint(failpoint)
        request_digest = digest({"vm_id": vm_id})
        with self._transaction() as connection:
            self._namespace_row(connection, namespace)
            replay = self._request_replay(
                connection, namespace, "FINISH_VM", idempotency_key, request_digest
            )
            if replay is not None:
                return replay
            vm = connection.execute(
                "SELECT * FROM vms WHERE namespace = ? AND vm_id = ?",
                (namespace, vm_id),
            ).fetchone()
            if vm is None:
                raise NotFoundError("VM was not found")
            if vm["status"] != "RUNNING":
                raise ConflictError("VM is already finished; use the original idempotency key")
            connection.execute(
                """UPDATE vms SET status = 'FINISHED', residual = 0,
                   finished_s = ?, updated_s = ?
                   WHERE namespace = ? AND vm_id = ?""",
                (current, current, namespace, vm_id),
            )
            self._refresh_host(connection, namespace, vm["host_id"], current)
            revision = self._advance_revision(connection, namespace)
            response = {
                "vm_id": vm_id,
                "host_id": vm["host_id"],
                "cluster_revision": revision,
                "replayed": False,
            }
            self._store_request(
                connection,
                namespace,
                "FINISH_VM",
                idempotency_key,
                request_digest,
                response,
                current,
            )
            self._audit(connection, namespace, "VM_FINISHED", vm_id, response, current)
        if failpoint == "after_vm_finish":
            raise SimulatedCrash("simulated lost acknowledgement after VM finish")
        return response

    def set_draining(
        self,
        namespace_value: str,
        idempotency_key_value: str,
        host_id_value: str,
        *,
        current_s: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        key = require_identifier(idempotency_key_value, "idempotency_key")
        host_id = require_identifier(host_id_value, "host_id")
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        request_digest = digest({"host_id": host_id})
        with self._transaction() as connection:
            self._namespace_row(connection, namespace)
            replay = self._request_replay(
                connection, namespace, "SET_DRAINING", key, request_digest
            )
            if replay is not None:
                return replay
            host = connection.execute(
                "SELECT * FROM hosts WHERE namespace = ? AND host_id = ?",
                (namespace, host_id),
            ).fetchone()
            if host is None:
                raise NotFoundError("host was not found")
            connection.execute(
                """UPDATE hosts SET state = 'DRAINING', revision = revision + 1,
                   updated_s = ? WHERE namespace = ? AND host_id = ?""",
                (current, namespace, host_id),
            )
            revision = self._advance_revision(connection, namespace)
            response = {
                "host_id": host_id,
                "state": "DRAINING",
                "cluster_revision": revision,
                "replayed": False,
            }
            self._store_request(
                connection,
                namespace,
                "SET_DRAINING",
                key,
                request_digest,
                response,
                current,
            )
            self._audit(connection, namespace, "HOST_DRAINING", host_id, response, current)
            return response

    def maintenance_plan(
        self,
        namespace_value: str,
        host_id_value: str,
        horizon_s_value: int,
        *,
        current_s: int | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        host_id = require_identifier(host_id_value, "host_id")
        horizon = require_int(horizon_s_value, "horizon_s", minimum=0, maximum=604_800)
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        connection = self._connect()
        try:
            host = connection.execute(
                "SELECT * FROM hosts WHERE namespace = ? AND host_id = ?",
                (namespace, host_id),
            ).fetchone()
            if host is None:
                raise NotFoundError("host was not found")
            rows = connection.execute(
                """SELECT * FROM vms
                   WHERE namespace = ? AND host_id = ? AND status = 'RUNNING'""",
                (namespace, host_id),
            ).fetchall()
            decisions = []
            for vm in rows:
                predicted_exit = self._repredicted_exit(vm, current)
                remaining = max(predicted_exit - current, 0)
                decisions.append(
                    {
                        "vm_id": vm["vm_id"],
                        "action": "WAIT" if remaining <= horizon else "MIGRATE",
                        "predicted_remaining_s": remaining,
                        "cpu_millis": vm["cpu_millis"],
                        "memory_mib": vm["memory_mib"],
                    }
                )
            decisions.sort(
                key=lambda item: (
                    0 if item["action"] == "MIGRATE" else 1,
                    -item["predicted_remaining_s"]
                    if item["action"] == "MIGRATE"
                    else item["predicted_remaining_s"],
                    item["vm_id"],
                )
            )
            return {
                "host_id": host_id,
                "horizon_s": horizon,
                "eager_migrations": len(rows),
                "lifetime_aware_migrations": sum(
                    item["action"] == "MIGRATE" for item in decisions
                ),
                "decisions": decisions,
            }
        finally:
            connection.close()

    def migrate_vm(
        self,
        namespace_value: str,
        idempotency_key_value: str,
        vm_id_value: str,
        destination_host_id_value: str,
        expected_revision_value: int,
        *,
        current_s: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        key = require_identifier(idempotency_key_value, "idempotency_key")
        vm_id = require_identifier(vm_id_value, "vm_id")
        destination = require_identifier(destination_host_id_value, "destination_host_id")
        expected = require_int(expected_revision_value, "expected_revision")
        current = now_s() if current_s is None else require_int(current_s, "current_s")
        failpoint = self._failpoint(failpoint)
        request = {
            "vm_id": vm_id,
            "destination_host_id": destination,
            "expected_revision": expected,
        }
        request_digest = digest(request)
        with self._transaction() as connection:
            self._namespace_row(connection, namespace)
            replay = self._request_replay(
                connection, namespace, "MIGRATE_VM", key, request_digest
            )
            if replay is not None:
                return replay
            namespace_row = self._namespace_row(connection, namespace)
            if namespace_row["revision"] != expected:
                raise ConflictError("cluster revision changed before migration")
            vm = connection.execute(
                "SELECT * FROM vms WHERE namespace = ? AND vm_id = ?",
                (namespace, vm_id),
            ).fetchone()
            if vm is None or vm["status"] != "RUNNING":
                raise NotFoundError("running VM was not found")
            if vm["host_id"] == destination:
                raise ConflictError("destination must differ from source host")
            host = connection.execute(
                "SELECT * FROM hosts WHERE namespace = ? AND host_id = ?",
                (namespace, destination),
            ).fetchone()
            if host is None:
                raise NotFoundError("destination host was not found")
            if host["state"] == "DRAINING":
                raise ConflictError("destination host is draining")
            used_cpu, used_memory, _ = self._host_usage(connection, namespace, destination)
            if (
                used_cpu + vm["cpu_millis"] > host["cpu_capacity"]
                or used_memory + vm["memory_mib"] > host["memory_capacity"]
            ):
                raise CapacityError("destination host lacks capacity")
            source = vm["host_id"]
            predicted_exit = self._repredicted_exit(vm, current)
            connection.execute(
                """UPDATE vms SET host_id = ?, residual = 0,
                   migration_count = migration_count + 1, predicted_exit_s = ?, updated_s = ?
                   WHERE namespace = ? AND vm_id = ?""",
                (destination, predicted_exit, current, namespace, vm_id),
            )
            self._refresh_host(connection, namespace, source, current)
            self._apply_host_after_add(
                connection,
                namespace,
                destination,
                vm["lifetime_class"],
                predicted_exit,
                current,
            )
            revision = self._advance_revision(connection, namespace)
            response = {
                "vm_id": vm_id,
                "source_host_id": source,
                "destination_host_id": destination,
                "cluster_revision": revision,
                "replayed": False,
            }
            self._store_request(
                connection,
                namespace,
                "MIGRATE_VM",
                key,
                request_digest,
                response,
                current,
            )
            self._audit(connection, namespace, "VM_MIGRATED", vm_id, response, current)
        if failpoint == "after_migration":
            raise SimulatedCrash("simulated lost acknowledgement after migration")
        return response

    def snapshot(self, namespace_value: str) -> dict[str, Any]:
        namespace = require_identifier(namespace_value, "namespace")
        connection = self._connect()
        try:
            namespace_row = self._namespace_row(connection, namespace)
            hosts = []
            for host in connection.execute(
                "SELECT * FROM hosts WHERE namespace = ? ORDER BY host_id", (namespace,)
            ).fetchall():
                used_cpu, used_memory, running = self._host_usage(
                    connection, namespace, host["host_id"]
                )
                hosts.append(
                    {
                        "host_id": host["host_id"],
                        "zone": host["zone"],
                        "state": host["state"],
                        "lifetime_class": host["lifetime_class"],
                        "deadline_s": host["deadline_s"],
                        "cpu_capacity": host["cpu_capacity"],
                        "memory_capacity": host["memory_capacity"],
                        "used_cpu": used_cpu,
                        "used_memory": used_memory,
                        "running_vms": running,
                    }
                )
            vms = [
                {
                    "vm_id": row["vm_id"],
                    "host_id": row["host_id"],
                    "cpu_millis": row["cpu_millis"],
                    "memory_mib": row["memory_mib"],
                    "predicted_lifetime_s": row["predicted_lifetime_s"],
                    "lifetime_class": row["lifetime_class"],
                    "status": row["status"],
                    "residual": bool(row["residual"]),
                    "migration_count": row["migration_count"],
                }
                for row in connection.execute(
                    "SELECT * FROM vms WHERE namespace = ? ORDER BY vm_id", (namespace,)
                ).fetchall()
            ]
            return {
                "namespace": namespace,
                "cluster_revision": namespace_row["revision"],
                "hosts": hosts,
                "vms": vms,
            }
        finally:
            connection.close()

    def metrics(self, namespace_value: str) -> dict[str, Any]:
        snapshot = self.snapshot(namespace_value)
        hosts = snapshot["hosts"]
        total_cpu = sum(host["cpu_capacity"] for host in hosts)
        total_memory = sum(host["memory_capacity"] for host in hosts)
        used_cpu = sum(host["used_cpu"] for host in hosts)
        used_memory = sum(host["used_memory"] for host in hosts)
        empty = sum(host["state"] == "EMPTY" for host in hosts)
        return {
            "namespace": snapshot["namespace"],
            "cluster_revision": snapshot["cluster_revision"],
            "hosts": len(hosts),
            "empty_hosts": empty,
            "empty_host_percent": round(100 * empty / len(hosts), 4) if hosts else 0.0,
            "hosts_fitting_full_size_vm": sum(
                host["state"] != "DRAINING"
                and host["used_cpu"] == 0
                and host["used_memory"] == 0
                for host in hosts
            ),
            "running_vms": sum(vm["status"] == "RUNNING" for vm in snapshot["vms"]),
            "finished_vms": sum(vm["status"] == "FINISHED" for vm in snapshot["vms"]),
            "cpu_utilization": round(used_cpu / total_cpu, 6) if total_cpu else 0.0,
            "memory_utilization": round(used_memory / total_memory, 6) if total_memory else 0.0,
            "database_bytes": self.path.stat().st_size,
        }

    def verify_integrity(self, namespace_value: str | None = None) -> dict[str, Any]:
        namespace = (
            None
            if namespace_value is None
            else require_identifier(namespace_value, "namespace")
        )
        connection = self._connect()
        totals = {"namespaces": 0, "hosts": 0, "vms": 0, "plans": 0, "audit_entries": 0}
        try:
            quick = connection.execute("PRAGMA quick_check").fetchone()[0]
            if quick != "ok":
                raise IntegrityError(f"SQLite quick_check failed: {quick}")
            rows = connection.execute(
                "SELECT * FROM namespaces" + (" WHERE namespace = ?" if namespace else ""),
                (() if namespace is None else (namespace,)),
            ).fetchall()
            if namespace is not None and not rows:
                raise NotFoundError("namespace was not found")
            for ns_row in rows:
                ns = ns_row["namespace"]
                totals["namespaces"] += 1
                previous = ZERO_HASH
                audits = connection.execute(
                    "SELECT * FROM audit_log WHERE namespace = ? ORDER BY sequence", (ns,)
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
                        "created_s": row["created_s"],
                    }
                    if not hmac.compare_digest(row["entry_hash"], self._sign(material)):
                        raise IntegrityError("audit entry hash is invalid")
                    previous = row["entry_hash"]
                if ns_row["audit_sequence"] != len(audits) or ns_row["audit_head_hash"] != previous:
                    raise IntegrityError("materialized audit head is invalid")
                totals["audit_entries"] += len(audits)

                hosts = connection.execute(
                    "SELECT * FROM hosts WHERE namespace = ?", (ns,)
                ).fetchall()
                for host in hosts:
                    if host["state"] not in HOST_STATES:
                        raise IntegrityError("host state is invalid")
                    cpu, memory, count = self._host_usage(connection, ns, host["host_id"])
                    if cpu > host["cpu_capacity"] or memory > host["memory_capacity"]:
                        raise IntegrityError("host is over capacity")
                    if host["state"] == "EMPTY" and (
                        count != 0
                        or host["lifetime_class"] is not None
                        or host["deadline_s"] is not None
                    ):
                        raise IntegrityError("empty host has running state")
                    if host["state"] in {"OPEN", "RECYCLING"} and (
                        count == 0 or host["lifetime_class"] is None
                    ):
                        raise IntegrityError("active host lacks running state")
                totals["hosts"] += len(hosts)

                vms = connection.execute(
                    "SELECT * FROM vms WHERE namespace = ?", (ns,)
                ).fetchall()
                for vm in vms:
                    if vm["lifetime_class"] != lifetime_class(vm["predicted_lifetime_s"]):
                        raise IntegrityError("VM lifetime class is invalid")
                    if vm["status"] == "RUNNING" and vm["finished_s"] is not None:
                        raise IntegrityError("running VM has a finish time")
                    if vm["status"] == "FINISHED" and vm["finished_s"] is None:
                        raise IntegrityError("finished VM lacks a finish time")
                totals["vms"] += len(vms)

                plans = connection.execute(
                    "SELECT * FROM plans WHERE namespace = ?", (ns,)
                ).fetchall()
                for plan in plans:
                    VMRequest.from_value(self._decode(plan["vm_json"], "planned VM"))
                    if plan["status"] == "COMMITTED":
                        vm_id = self._decode(plan["vm_json"], "planned VM")["vm_id"]
                        if connection.execute(
                            "SELECT 1 FROM vms WHERE namespace = ? AND vm_id = ?",
                            (ns, vm_id),
                        ).fetchone() is None:
                            raise IntegrityError("committed plan has no VM")
                totals["plans"] += len(plans)
            return {"valid": True, **totals}
        except (sqlite3.DatabaseError, ValidationError) as exc:
            raise IntegrityError(f"stored state is invalid: {exc}") from exc
        finally:
            connection.close()
