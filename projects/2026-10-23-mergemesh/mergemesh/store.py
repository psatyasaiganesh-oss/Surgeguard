"""Durable SQLite replica implementing field-level multi-value registers."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .model import (
    ConflictError,
    IntegrityError,
    MAX_CLOCK_ENTRIES,
    Operation,
    ValidationError,
    canonical_json,
    compare_clocks,
    join_clocks,
    normalize_clock,
    validate_field_name,
    validate_identifier,
)


class RegionStore:
    """One durable regional replica.

    Each write records its causal context. Concurrent versions of the same field
    remain visible until an explicit resolution causally supersedes them.
    """

    def __init__(self, database: str | os.PathLike[str], region: str) -> None:
        self.database = Path(database)
        self.region = validate_identifier(region, "region")
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._bootstrap()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _bootstrap(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS local_sequence (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    value INTEGER NOT NULL CHECK (value >= 0)
                );

                CREATE TABLE IF NOT EXISTS operations (
                    op_id TEXT PRIMARY KEY,
                    origin TEXT NOT NULL,
                    origin_sequence INTEGER NOT NULL CHECK (origin_sequence > 0),
                    tenant TEXT NOT NULL,
                    document_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    applied_ns INTEGER NOT NULL,
                    UNIQUE (origin, origin_sequence)
                );
                CREATE INDEX IF NOT EXISTS idx_operations_document
                    ON operations (tenant, document_id, applied_ns, op_id);

                CREATE TABLE IF NOT EXISTS document_clocks (
                    tenant TEXT NOT NULL,
                    document_id TEXT NOT NULL,
                    clock_json TEXT NOT NULL,
                    PRIMARY KEY (tenant, document_id)
                );

                CREATE TABLE IF NOT EXISTS field_versions (
                    tenant TEXT NOT NULL,
                    document_id TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    op_id TEXT NOT NULL REFERENCES operations(op_id) ON DELETE RESTRICT,
                    origin TEXT NOT NULL,
                    origin_sequence INTEGER NOT NULL,
                    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
                    value_json TEXT,
                    clock_json TEXT NOT NULL,
                    PRIMARY KEY (tenant, document_id, field_name, op_id),
                    CHECK ((deleted = 1 AND value_json IS NULL) OR
                           (deleted = 0 AND value_json IS NOT NULL))
                );
                CREATE INDEX IF NOT EXISTS idx_field_versions_lookup
                    ON field_versions (tenant, document_id, field_name, op_id);

                CREATE TABLE IF NOT EXISTS deliveries (
                    peer TEXT NOT NULL,
                    op_id TEXT NOT NULL REFERENCES operations(op_id) ON DELETE RESTRICT,
                    delivered_ns INTEGER NOT NULL,
                    PRIMARY KEY (peer, op_id)
                );

                CREATE TABLE IF NOT EXISTS audit_log (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    previous_hash TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE
                );
                """
            )
            row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'region'"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES ('region', ?)",
                    (self.region,),
                )
            elif row["value"] != self.region:
                raise IntegrityError(
                    f"database belongs to region {row['value']!r}, not {self.region!r}"
                )
            connection.execute(
                "INSERT OR IGNORE INTO local_sequence(singleton, value) VALUES (1, 0)"
            )

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

    @staticmethod
    def _document_clock_tx(
        connection: sqlite3.Connection, tenant: str, document: str
    ) -> dict[str, int]:
        row = connection.execute(
            """
            SELECT clock_json FROM document_clocks
            WHERE tenant = ? AND document_id = ?
            """,
            (tenant, document),
        ).fetchone()
        return {} if row is None else normalize_clock(json.loads(row["clock_json"]))

    @staticmethod
    def _next_sequence_tx(connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT value FROM local_sequence WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise IntegrityError("local sequence row is missing")
        sequence = int(row["value"]) + 1
        connection.execute(
            "UPDATE local_sequence SET value = ? WHERE singleton = 1", (sequence,)
        )
        return sequence

    @staticmethod
    def _append_audit_tx(connection: sqlite3.Connection, event: Mapping[str, Any]) -> str:
        event_json = canonical_json(dict(event))
        row = connection.execute(
            "SELECT event_hash FROM audit_log ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous = "0" * 64 if row is None else row["event_hash"]
        digest = hashlib.sha256((previous + "\n" + event_json).encode("utf-8")).hexdigest()
        connection.execute(
            """
            INSERT INTO audit_log(previous_hash, event_json, event_hash)
            VALUES (?, ?, ?)
            """,
            (previous, event_json, digest),
        )
        return digest

    @staticmethod
    def _candidate_rows_tx(
        connection: sqlite3.Connection,
        tenant: str,
        document: str,
        field: str | None = None,
    ) -> list[sqlite3.Row]:
        if field is None:
            return list(
                connection.execute(
                    """
                    SELECT field_name, op_id, origin, origin_sequence,
                           deleted, value_json, clock_json
                    FROM field_versions
                    WHERE tenant = ? AND document_id = ?
                    ORDER BY field_name, op_id
                    """,
                    (tenant, document),
                )
            )
        return list(
            connection.execute(
                """
                SELECT field_name, op_id, origin, origin_sequence,
                       deleted, value_json, clock_json
                FROM field_versions
                WHERE tenant = ? AND document_id = ? AND field_name = ?
                ORDER BY op_id
                """,
                (tenant, document, field),
            )
        )

    @classmethod
    def _render_document_tx(
        cls,
        connection: sqlite3.Connection,
        tenant: str,
        document: str,
    ) -> dict[str, Any]:
        clock = cls._document_clock_tx(connection, tenant, document)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in cls._candidate_rows_tx(connection, tenant, document):
            candidate: dict[str, Any] = {
                "op_id": row["op_id"],
                "origin": row["origin"],
                "sequence": row["origin_sequence"],
                "deleted": bool(row["deleted"]),
            }
            if not candidate["deleted"]:
                candidate["value"] = json.loads(row["value_json"])
            grouped.setdefault(row["field_name"], []).append(candidate)

        fields: dict[str, Any] = {}
        values: dict[str, Any] = {}
        deleted_fields: list[str] = []
        conflicts: list[str] = []
        for field, candidates in sorted(grouped.items()):
            if len(candidates) > 1:
                fields[field] = {"status": "conflict", "candidates": candidates}
                conflicts.append(field)
            elif candidates[0]["deleted"]:
                fields[field] = {"status": "deleted", "candidate": candidates[0]}
                deleted_fields.append(field)
            else:
                fields[field] = {
                    "status": "value",
                    "value": candidates[0]["value"],
                    "candidate": candidates[0],
                }
                values[field] = candidates[0]["value"]

        digest_input = {
            "tenant": tenant,
            "document": document,
            "clock": clock,
            "fields": fields,
        }
        etag = hashlib.sha256(canonical_json(digest_input).encode("utf-8")).hexdigest()
        return {
            **digest_input,
            "values": values,
            "deleted_fields": deleted_fields,
            "conflicts": conflicts,
            "conflict_count": len(conflicts),
            "etag": etag,
        }

    def get_document(self, tenant: str, document: str) -> dict[str, Any]:
        tenant = validate_identifier(tenant, "tenant")
        document = validate_identifier(document, "document")
        with self._connect() as connection:
            return self._render_document_tx(connection, tenant, document)

    def _apply_operation_tx(
        self,
        connection: sqlite3.Connection,
        operation: Operation,
        *,
        source: str,
    ) -> dict[str, Any]:
        payload = operation.to_json()
        existing = connection.execute(
            "SELECT payload_json FROM operations WHERE op_id = ?", (operation.op_id,)
        ).fetchone()
        if existing is not None:
            if existing["payload_json"] != payload:
                raise IntegrityError(f"operation ID collision for {operation.op_id}")
            return {
                "op_id": operation.op_id,
                "duplicate": True,
                "changed_fields": [],
                "obsolete_fields": [],
                "conflicts": self._render_document_tx(
                    connection, operation.tenant, operation.document
                )["conflicts"],
            }

        same_dot = connection.execute(
            """
            SELECT op_id, payload_json FROM operations
            WHERE origin = ? AND origin_sequence = ?
            """,
            (operation.origin, operation.sequence),
        ).fetchone()
        if same_dot is not None:
            raise IntegrityError(
                f"origin sequence collision: {operation.origin}:{operation.sequence}"
            )

        applied_ns = time.time_ns()
        connection.execute(
            """
            INSERT INTO operations(
                op_id, origin, origin_sequence, tenant, document_id,
                payload_json, applied_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation.op_id,
                operation.origin,
                operation.sequence,
                operation.tenant,
                operation.document,
                payload,
                applied_ns,
            ),
        )

        current_clock = self._document_clock_tx(
            connection, operation.tenant, operation.document
        )
        merged_clock = join_clocks(current_clock, operation.clock)
        if len(merged_clock) > MAX_CLOCK_ENTRIES:
            raise ValidationError(
                f"document clock exceeds the {MAX_CLOCK_ENTRIES}-region safety bound"
            )
        connection.execute(
            """
            INSERT INTO document_clocks(tenant, document_id, clock_json)
            VALUES (?, ?, ?)
            ON CONFLICT(tenant, document_id) DO UPDATE SET clock_json = excluded.clock_json
            """,
            (operation.tenant, operation.document, canonical_json(merged_clock)),
        )

        changed_fields: list[str] = []
        obsolete_fields: list[str] = []
        for field, mutation in sorted(operation.mutations.items()):
            rows = self._candidate_rows_tx(
                connection, operation.tenant, operation.document, field
            )
            comparisons = [
                compare_clocks(operation.clock, json.loads(row["clock_json"]))
                for row in rows
            ]
            if "before" in comparisons:
                obsolete_fields.append(field)
                continue
            if "equal" in comparisons:
                raise IntegrityError(
                    f"different operations produced the same causal clock for field {field}"
                )
            dominated = [
                row["op_id"]
                for row, comparison in zip(rows, comparisons, strict=True)
                if comparison == "after"
            ]
            for op_id in dominated:
                connection.execute(
                    """
                    DELETE FROM field_versions
                    WHERE tenant = ? AND document_id = ? AND field_name = ? AND op_id = ?
                    """,
                    (operation.tenant, operation.document, field, op_id),
                )
            connection.execute(
                """
                INSERT INTO field_versions(
                    tenant, document_id, field_name, op_id, origin, origin_sequence,
                    deleted, value_json, clock_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation.tenant,
                    operation.document,
                    field,
                    operation.op_id,
                    operation.origin,
                    operation.sequence,
                    int(mutation.deleted),
                    None if mutation.deleted else canonical_json(mutation.value),
                    canonical_json(operation.clock),
                ),
            )
            changed_fields.append(field)

        rendered = self._render_document_tx(
            connection, operation.tenant, operation.document
        )
        self._append_audit_tx(
            connection,
            {
                "type": "operation_applied",
                "source": source,
                "op_id": operation.op_id,
                "tenant": operation.tenant,
                "document": operation.document,
                "changed_fields": changed_fields,
                "obsolete_fields": obsolete_fields,
                "conflicts": rendered["conflicts"],
            },
        )
        return {
            "op_id": operation.op_id,
            "duplicate": False,
            "changed_fields": changed_fields,
            "obsolete_fields": obsolete_fields,
            "conflicts": rendered["conflicts"],
        }

    def apply_remote(self, operation: Operation) -> dict[str, Any]:
        """Atomically apply an already authenticated remote operation."""

        operation.validate()
        with self._transaction() as connection:
            return self._apply_operation_tx(connection, operation, source="remote")

    def patch(
        self,
        tenant: str,
        document: str,
        *,
        changes: Mapping[str, Any] | None = None,
        delete_fields: Sequence[str] | None = None,
        expected_etag: str | None = None,
    ) -> dict[str, Any]:
        """Create and commit a local field patch against the observed context."""

        tenant = validate_identifier(tenant, "tenant")
        document = validate_identifier(document, "document")
        if expected_etag is not None and (
            not isinstance(expected_etag, str)
            or len(expected_etag) != 64
            or any(char not in "0123456789abcdef" for char in expected_etag)
        ):
            raise ValidationError("expected_etag must be a lowercase SHA-256 digest")
        with self._transaction() as connection:
            before = self._render_document_tx(connection, tenant, document)
            if expected_etag is not None and before["etag"] != expected_etag:
                raise ConflictError("document etag changed")
            sequence = self._next_sequence_tx(connection)
            operation = Operation.create(
                tenant=tenant,
                document=document,
                origin=self.region,
                sequence=sequence,
                context=before["clock"],
                changes=changes,
                delete_fields=list(delete_fields or []),
            )
            result = self._apply_operation_tx(connection, operation, source="local")
            result["document"] = self._render_document_tx(connection, tenant, document)
            return result

    def resolve(
        self,
        tenant: str,
        document: str,
        field: str,
        *,
        expected_candidates: Sequence[str],
        value: Any = None,
        delete: bool = False,
    ) -> dict[str, Any]:
        """Resolve one conflict only if its exact candidate set is still current."""

        tenant = validate_identifier(tenant, "tenant")
        document = validate_identifier(document, "document")
        field = validate_field_name(field)
        if not isinstance(expected_candidates, Sequence) or isinstance(
            expected_candidates, (str, bytes)
        ):
            raise ValidationError("expected_candidates must be a sequence")
        expected = sorted(expected_candidates)
        if len(expected) < 2 or len(set(expected)) != len(expected):
            raise ValidationError("resolution requires at least two unique candidates")
        for op_id in expected:
            if not isinstance(op_id, str) or ":" not in op_id or len(op_id) > 96:
                raise ValidationError("invalid candidate operation ID")
        if not isinstance(delete, bool):
            raise ValidationError("delete must be boolean")

        with self._transaction() as connection:
            current_rows = self._candidate_rows_tx(connection, tenant, document, field)
            current = sorted(row["op_id"] for row in current_rows)
            if current != expected:
                raise ConflictError("conflict candidates changed")
            if len(current) < 2:
                raise ConflictError("field is not currently conflicted")
            context = self._document_clock_tx(connection, tenant, document)
            sequence = self._next_sequence_tx(connection)
            operation = Operation.create(
                tenant=tenant,
                document=document,
                origin=self.region,
                sequence=sequence,
                context=context,
                changes={} if delete else {field: value},
                delete_fields=[field] if delete else [],
            )
            result = self._apply_operation_tx(connection, operation, source="resolution")
            result["resolved_candidates"] = expected
            result["document"] = self._render_document_tx(connection, tenant, document)
            return result

    def export_operations(self, peer: str, *, limit: int = 100) -> list[Operation]:
        peer = validate_identifier(peer, "peer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValidationError("limit must be an integer from 1 to 1000")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT o.payload_json
                FROM operations AS o
                LEFT JOIN deliveries AS d
                  ON d.peer = ? AND d.op_id = o.op_id
                WHERE d.op_id IS NULL
                ORDER BY o.applied_ns, o.op_id
                LIMIT ?
                """,
                (peer, limit),
            ).fetchall()
        return [Operation.from_json(row["payload_json"]) for row in rows]

    def mark_delivered(self, peer: str, op_ids: Sequence[str]) -> int:
        peer = validate_identifier(peer, "peer")
        if not isinstance(op_ids, Sequence) or isinstance(op_ids, (str, bytes)):
            raise ValidationError("op_ids must be a sequence")
        if len(op_ids) > 1000:
            raise ValidationError("cannot acknowledge more than 1000 operations")
        unique = list(dict.fromkeys(op_ids))
        with self._transaction() as connection:
            for op_id in unique:
                if not isinstance(op_id, str):
                    raise ValidationError("operation IDs must be strings")
                exists = connection.execute(
                    "SELECT 1 FROM operations WHERE op_id = ?", (op_id,)
                ).fetchone()
                if exists is None:
                    raise ValidationError(f"cannot acknowledge unknown operation {op_id}")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO deliveries(peer, op_id, delivered_ns)
                    VALUES (?, ?, ?)
                    """,
                    (peer, op_id, time.time_ns()),
                )
            if unique:
                self._append_audit_tx(
                    connection,
                    {"type": "delivery_ack", "peer": peer, "op_ids": sorted(unique)},
                )
        return len(unique)

    def operation(self, op_id: str) -> Operation:
        if not isinstance(op_id, str):
            raise ValidationError("op_id must be a string")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM operations WHERE op_id = ?", (op_id,)
            ).fetchone()
        if row is None:
            raise KeyError(op_id)
        return Operation.from_json(row["payload_json"])

    def list_conflicts(self, *, tenant: str | None = None) -> list[dict[str, Any]]:
        if tenant is not None:
            tenant = validate_identifier(tenant, "tenant")
        query = """
            SELECT tenant, document_id, field_name, COUNT(*) AS candidate_count
            FROM field_versions
        """
        parameters: tuple[Any, ...] = ()
        if tenant is not None:
            query += " WHERE tenant = ?"
            parameters = (tenant,)
        query += " GROUP BY tenant, document_id, field_name HAVING COUNT(*) > 1"
        query += " ORDER BY tenant, document_id, field_name"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def stats(self) -> dict[str, Any]:
        with self._connect() as connection:
            operation_count = connection.execute(
                "SELECT COUNT(*) AS count FROM operations"
            ).fetchone()["count"]
            document_count = connection.execute(
                "SELECT COUNT(*) AS count FROM document_clocks"
            ).fetchone()["count"]
            version_count = connection.execute(
                "SELECT COUNT(*) AS count FROM field_versions"
            ).fetchone()["count"]
            audit_count = connection.execute(
                "SELECT COUNT(*) AS count FROM audit_log"
            ).fetchone()["count"]
            delivery_count = connection.execute(
                "SELECT COUNT(*) AS count FROM deliveries"
            ).fetchone()["count"]
        conflicts = self.list_conflicts()
        return {
            "region": self.region,
            "operations": operation_count,
            "documents": document_count,
            "field_versions": version_count,
            "unresolved_conflicts": len(conflicts),
            "deliveries": delivery_count,
            "audit_events": audit_count,
        }

    def verify(self) -> dict[str, Any]:
        """Verify operation canonicalization, materialized versions, and audit chain."""

        problems: list[str] = []
        operation_count = 0
        version_count = 0
        audit_count = 0
        with self._connect() as connection:
            operations: dict[str, Operation] = {}
            for row in connection.execute(
                "SELECT op_id, payload_json FROM operations ORDER BY op_id"
            ):
                operation_count += 1
                try:
                    operation = Operation.from_json(row["payload_json"])
                    if operation.op_id != row["op_id"]:
                        problems.append(f"operation key mismatch: {row['op_id']}")
                    if operation.to_json() != row["payload_json"]:
                        problems.append(f"non-canonical operation: {row['op_id']}")
                    operations[row["op_id"]] = operation
                except (ValidationError, ValueError) as exc:
                    problems.append(f"invalid operation {row['op_id']}: {exc}")

            expected_clocks: dict[tuple[str, str], dict[str, int]] = {}
            expected_candidates: dict[tuple[str, str, str], set[str]] = {}
            mutation_groups: dict[tuple[str, str, str], list[Operation]] = {}
            for operation in operations.values():
                document_key = (operation.tenant, operation.document)
                expected_clocks[document_key] = join_clocks(
                    expected_clocks.get(document_key, {}), operation.clock
                )
                for field in operation.mutations:
                    mutation_groups.setdefault((*document_key, field), []).append(operation)
            for key, group in mutation_groups.items():
                expected_candidates[key] = {
                    candidate.op_id
                    for candidate in group
                    if not any(
                        other.op_id != candidate.op_id
                        and compare_clocks(other.clock, candidate.clock) == "after"
                        for other in group
                    )
                }

            actual_clocks: dict[tuple[str, str], dict[str, int]] = {}
            for row in connection.execute(
                "SELECT tenant, document_id, clock_json FROM document_clocks"
            ):
                key = (row["tenant"], row["document_id"])
                try:
                    actual_clocks[key] = normalize_clock(json.loads(row["clock_json"]))
                except (ValidationError, ValueError, json.JSONDecodeError) as exc:
                    problems.append(f"invalid document clock {key}: {exc}")
            if actual_clocks != expected_clocks:
                problems.append("materialized document clocks do not match operation history")

            actual_candidates: dict[tuple[str, str, str], set[str]] = {}

            for row in connection.execute(
                """
                SELECT tenant, document_id, field_name, op_id, deleted,
                       value_json, clock_json
                FROM field_versions ORDER BY tenant, document_id, field_name, op_id
                """
            ):
                version_count += 1
                key = (row["tenant"], row["document_id"], row["field_name"])
                actual_candidates.setdefault(key, set()).add(row["op_id"])
                operation = operations.get(row["op_id"])
                if operation is None:
                    problems.append(f"field version references missing op {row['op_id']}")
                    continue
                mutation = operation.mutations.get(row["field_name"])
                if mutation is None:
                    problems.append(f"field version not present in op {row['op_id']}")
                    continue
                if row["tenant"] != operation.tenant or row["document_id"] != operation.document:
                    problems.append(f"field version scope mismatch for {row['op_id']}")
                if bool(row["deleted"]) != mutation.deleted:
                    problems.append(f"field deletion mismatch for {row['op_id']}")
                expected_value = None if mutation.deleted else canonical_json(mutation.value)
                if row["value_json"] != expected_value:
                    problems.append(f"field value mismatch for {row['op_id']}")
                if row["clock_json"] != canonical_json(operation.clock):
                    problems.append(f"field clock mismatch for {row['op_id']}")

            if actual_candidates != expected_candidates:
                problems.append("materialized field candidates do not match operation history")

            previous = "0" * 64
            for row in connection.execute(
                "SELECT sequence, previous_hash, event_json, event_hash FROM audit_log ORDER BY sequence"
            ):
                audit_count += 1
                if row["previous_hash"] != previous:
                    problems.append(f"audit predecessor mismatch at {row['sequence']}")
                expected = hashlib.sha256(
                    (row["previous_hash"] + "\n" + row["event_json"]).encode("utf-8")
                ).hexdigest()
                if expected != row["event_hash"]:
                    problems.append(f"audit hash mismatch at {row['sequence']}")
                previous = row["event_hash"]

            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                problems.append(f"SQLite integrity_check: {integrity}")

        return {
            "region": self.region,
            "ok": not problems,
            "operations_checked": operation_count,
            "field_versions_checked": version_count,
            "audit_events_checked": audit_count,
            "problems": problems,
        }
