from __future__ import annotations

import fcntl
import hashlib
import hmac
import os
import threading
import uuid
from pathlib import Path
from typing import Any

from .errors import ConflictError, IntegrityError, NotFoundError, SimulatedCrash, StateError, ValidationError
from .model import (
    VersionedRecord,
    canonical_json,
    digest,
    normalize_mutations,
    parse_json,
    require_closed_object,
    require_hex_digest,
    require_identifier,
    require_int,
    require_key,
)
from .segment import SegmentMetadata, SegmentReader, file_sha256, write_segment
from .wal import WriteAheadLog


MAX_IDEMPOTENCY_ENTRIES = 50_000
VALID_FAILPOINTS = {None, "after_wal_fsync", "after_segment_rename", "after_manifest_commit"}


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class TabletForge:
    def __init__(
        self,
        directory: str | Path,
        *,
        secret: bytes,
        memtable_limit: int = 1_000,
    ) -> None:
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValidationError("secret must contain at least 32 bytes")
        self.directory = Path(directory)
        self.segment_directory = self.directory / "segments"
        self.quarantine_directory = self.directory / "lost+found"
        self.manifest_path = self.directory / "MANIFEST.json"
        self.wal_path = self.directory / "wal.log"
        self.lock_path = self.directory / "LOCK"
        self.secret = secret
        self.memtable_limit = require_int(memtable_limit, "memtable_limit", minimum=1, maximum=1_000_000)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.segment_directory.mkdir(exist_ok=True)
        self.quarantine_directory.mkdir(exist_ok=True)
        self._lock_stream = self.lock_path.open("a+b")
        try:
            fcntl.flock(self._lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock_stream.close()
            raise StateError("storage directory is already open by another process") from exc
        self._mutex = threading.RLock()
        self._closed = False
        self._metrics = {
            "writes": 0,
            "replayed_writes": 0,
            "reads": 0,
            "segment_probes": 0,
            "segment_reads": 0,
            "bloom_negative_skips": 0,
            "flushes": 0,
            "compactions": 0,
            "recovered_wal_frames": 0,
            "truncated_wal_bytes": 0,
            "orphan_segments_quarantined": 0,
        }
        try:
            if not self.manifest_path.exists():
                self._write_manifest_payload(
                    {
                        "version": 1,
                        "generation": 0,
                        "segments": [],
                        "next_segment_id": 1,
                        "checkpoint_sequence": 0,
                        "next_sequence": 1,
                        "snapshots": {},
                        "idempotency": {},
                    }
                )
            self._load_manifest()
            self._quarantine_orphans()
            self._readers = self._open_readers(self._segments)
            self._memtable: dict[str, list[VersionedRecord]] = {}
            self._memtable_records = 0
            self._wal = WriteAheadLog(self.wal_path, secret)
            self._recover_wal()
        except Exception:
            self._release_lock()
            raise

    def __enter__(self) -> "TabletForge":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise StateError("store is closed")

    def _release_lock(self) -> None:
        if not self._lock_stream.closed:
            fcntl.flock(self._lock_stream.fileno(), fcntl.LOCK_UN)
            self._lock_stream.close()

    def close(self) -> None:
        with self._mutex:
            if not self._closed:
                self._closed = True
                self._release_lock()

    def _sign(self, payload: Any) -> str:
        return hmac.new(self.secret, canonical_json(payload).encode(), hashlib.sha256).hexdigest()

    def _write_manifest_payload(self, payload: dict[str, Any]) -> None:
        envelope = {"payload": payload, "signature": self._sign(payload)}
        encoded = canonical_json(envelope).encode()
        temporary = self.directory / f".MANIFEST.{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.manifest_path)
            _fsync_directory(self.directory)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _parse_manifest(self) -> dict[str, Any]:
        try:
            raw = self.manifest_path.read_text(encoding="utf-8")
            envelope = parse_json(raw, "manifest")
            envelope = require_closed_object(envelope, "manifest", {"payload", "signature"})
            if not isinstance(envelope["payload"], dict):
                raise ValidationError("manifest payload must be an object")
            require_hex_digest(envelope["signature"], "manifest signature")
            if not hmac.compare_digest(envelope["signature"], self._sign(envelope["payload"])):
                raise IntegrityError("manifest signature is invalid")
            payload = require_closed_object(
                envelope["payload"],
                "manifest payload",
                {
                    "version",
                    "generation",
                    "segments",
                    "next_segment_id",
                    "checkpoint_sequence",
                    "next_sequence",
                    "snapshots",
                    "idempotency",
                },
            )
            require_int(payload["version"], "version", minimum=1, maximum=1)
            require_int(payload["generation"], "generation", minimum=0)
            checkpoint = require_int(payload["checkpoint_sequence"], "checkpoint_sequence", minimum=0)
            require_int(payload["next_sequence"], "next_sequence", minimum=checkpoint + 1)
            require_int(payload["next_segment_id"], "next_segment_id", minimum=1)
            if not isinstance(payload["segments"], list):
                raise ValidationError("segments must be an array")
            segments = [SegmentMetadata.from_value(item) for item in payload["segments"]]
            if len({item.segment_id for item in segments}) != len(segments):
                raise ValidationError("segment identities must be unique")
            if segments and payload["next_segment_id"] <= max(item.segment_id for item in segments):
                raise ValidationError("next_segment_id is not ahead of existing segments")
            if any(item.max_sequence > checkpoint for item in segments):
                raise ValidationError("segment sequence exceeds the durable checkpoint")
            if not isinstance(payload["snapshots"], dict):
                raise ValidationError("snapshots must be an object")
            snapshots: dict[str, int] = {}
            for snapshot_id, sequence in payload["snapshots"].items():
                snapshots[require_identifier(snapshot_id, "snapshot_id")] = require_int(
                    sequence, "snapshot sequence", minimum=0, maximum=payload["next_sequence"] - 1
                )
            if not isinstance(payload["idempotency"], dict):
                raise ValidationError("idempotency must be an object")
            if len(payload["idempotency"]) > MAX_IDEMPOTENCY_ENTRIES:
                raise ValidationError("idempotency index is too large")
            idempotency: dict[str, dict[str, Any]] = {}
            for key, raw_entry in payload["idempotency"].items():
                key = require_identifier(key, "idempotency_key")
                entry = require_closed_object(
                    raw_entry,
                    "idempotency entry",
                    {"request_digest", "start_sequence", "end_sequence"},
                )
                start = require_int(entry["start_sequence"], "start_sequence", minimum=1)
                end = require_int(entry["end_sequence"], "end_sequence", minimum=start)
                if end >= payload["next_sequence"]:
                    raise ValidationError("idempotency sequence exceeds manifest head")
                idempotency[key] = {
                    "request_digest": require_hex_digest(entry["request_digest"], "request_digest"),
                    "start_sequence": start,
                    "end_sequence": end,
                }
            expected_sequence = 1
            for entry in sorted(idempotency.values(), key=lambda item: item["start_sequence"]):
                if entry["start_sequence"] != expected_sequence:
                    raise ValidationError("idempotency sequence ranges contain a gap or overlap")
                expected_sequence = entry["end_sequence"] + 1
            if expected_sequence != payload["next_sequence"]:
                raise ValidationError("idempotency sequence ranges do not cover the manifest head")
            return {
                **payload,
                "segments": segments,
                "snapshots": snapshots,
                "idempotency": idempotency,
            }
        except IntegrityError:
            raise
        except (OSError, UnicodeError, ValidationError) as exc:
            raise IntegrityError(f"manifest is invalid: {exc}") from exc

    def _load_manifest(self) -> None:
        payload = self._parse_manifest()
        self._generation = payload["generation"]
        self._segments: list[SegmentMetadata] = payload["segments"]
        self._next_segment_id = payload["next_segment_id"]
        self._checkpoint_sequence = payload["checkpoint_sequence"]
        self._next_sequence = payload["next_sequence"]
        self._snapshots: dict[str, int] = payload["snapshots"]
        self._idempotency: dict[str, dict[str, Any]] = payload["idempotency"]

    def _payload(
        self,
        *,
        segments: list[SegmentMetadata] | None = None,
        generation: int | None = None,
    ) -> dict[str, Any]:
        return {
            "version": 1,
            "generation": self._generation + 1 if generation is None else generation,
            "segments": [item.as_dict() for item in (self._segments if segments is None else segments)],
            "next_segment_id": self._next_segment_id,
            "checkpoint_sequence": self._checkpoint_sequence,
            "next_sequence": self._next_sequence,
            "snapshots": dict(sorted(self._snapshots.items())),
            "idempotency": dict(sorted(self._idempotency.items())),
        }

    def _persist_manifest(self, *, segments: list[SegmentMetadata] | None = None) -> None:
        payload = self._payload(segments=segments)
        self._write_manifest_payload(payload)
        self._generation = payload["generation"]

    def _quarantine_orphans(self) -> None:
        referenced = {item.filename for item in self._segments}
        for path in sorted(self.segment_directory.glob("segment-*.sst")):
            if path.name not in referenced:
                target = self.quarantine_directory / f"{path.name}.{uuid.uuid4().hex}.orphan"
                os.replace(path, target)
                self._metrics["orphan_segments_quarantined"] += 1
        _fsync_directory(self.segment_directory)
        _fsync_directory(self.quarantine_directory)

    def _open_readers(self, segments: list[SegmentMetadata]) -> list[SegmentReader]:
        return [SegmentReader(self.segment_directory / item.filename, item) for item in segments]

    @staticmethod
    def _validate_wal_frame(value: Any) -> tuple[str, str, int, int, list[VersionedRecord]]:
        body = require_closed_object(
            value,
            "WAL frame",
            {
                "version",
                "idempotency_key",
                "request_digest",
                "start_sequence",
                "end_sequence",
                "records",
            },
        )
        require_int(body["version"], "version", minimum=1, maximum=1)
        key = require_identifier(body["idempotency_key"], "idempotency_key")
        request_digest = require_hex_digest(body["request_digest"], "request_digest")
        start = require_int(body["start_sequence"], "start_sequence", minimum=1)
        end = require_int(body["end_sequence"], "end_sequence", minimum=start)
        if not isinstance(body["records"], list) or not body["records"]:
            raise ValidationError("WAL records must be a non-empty array")
        records = [VersionedRecord.from_value(item) for item in body["records"]]
        if len(records) != end - start + 1:
            raise ValidationError("WAL sequence range does not match record count")
        if [item.sequence for item in records] != list(range(start, end + 1)):
            raise ValidationError("WAL records are not contiguous")
        mutations = [
            {"operation": "DELETE", "key": item.key}
            if item.tombstone
            else {"operation": "PUT", "key": item.key, "value": item.value}
            for item in records
        ]
        if not hmac.compare_digest(request_digest, digest({"mutations": mutations})):
            raise ValidationError("WAL request digest does not match its records")
        return key, request_digest, start, end, records

    def _apply_records(self, records: list[VersionedRecord]) -> None:
        for record in records:
            versions = self._memtable.setdefault(record.key, [])
            versions.append(record)
            versions.sort(key=lambda item: -item.sequence)
            self._memtable_records += 1

    def _recover_wal(self) -> None:
        frames, truncated = self._wal.recover()
        self._metrics["truncated_wal_bytes"] += truncated
        manifest_next_sequence = self._next_sequence
        expected_sequence = self._checkpoint_sequence + 1
        for raw in frames:
            try:
                key, request_digest, start, end, records = self._validate_wal_frame(raw)
            except ValidationError as exc:
                raise IntegrityError(f"WAL semantic validation failed: {exc}") from exc
            if end <= self._checkpoint_sequence:
                persisted = self._idempotency.get(key)
                if persisted is None or persisted["request_digest"] != request_digest:
                    raise IntegrityError("checkpointed WAL frame is detached from idempotency state")
                continue
            if start <= self._checkpoint_sequence < end:
                raise IntegrityError("WAL frame straddles the durable checkpoint")
            if start != expected_sequence:
                raise IntegrityError("WAL sequence contains a gap or overlap")
            existing = self._idempotency.get(key)
            entry = {
                "request_digest": request_digest,
                "start_sequence": start,
                "end_sequence": end,
            }
            if existing is not None and existing != entry:
                raise IntegrityError("WAL idempotency entry conflicts with persisted state")
            self._idempotency[key] = entry
            self._apply_records(records)
            expected_sequence = end + 1
            self._metrics["recovered_wal_frames"] += 1
        if expected_sequence < manifest_next_sequence:
            raise IntegrityError("manifest sequence is ahead of recoverable WAL data")
        self._next_sequence = max(manifest_next_sequence, expected_sequence)

    @staticmethod
    def _failpoint(value: str | None) -> str | None:
        if value not in VALID_FAILPOINTS:
            raise ValidationError("unknown failpoint")
        return value

    def write_batch(
        self,
        idempotency_key: str,
        mutations_value: Any,
        *,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_open()
        key = require_identifier(idempotency_key, "idempotency_key")
        mutations = normalize_mutations(mutations_value)
        failpoint = self._failpoint(failpoint)
        request_digest = digest({"mutations": mutations})
        with self._mutex:
            existing = self._idempotency.get(key)
            if existing is not None:
                if not hmac.compare_digest(existing["request_digest"], request_digest):
                    raise ConflictError("idempotency key was reused with different mutations")
                self._metrics["replayed_writes"] += 1
                return {"idempotency_key": key, **existing, "replayed": True}
            if len(self._idempotency) >= MAX_IDEMPOTENCY_ENTRIES:
                raise StateError("idempotency index reached its configured limit")
            start = self._next_sequence
            records: list[VersionedRecord] = []
            for offset, mutation in enumerate(mutations):
                sequence = start + offset
                records.append(
                    VersionedRecord(
                        mutation["key"],
                        sequence,
                        mutation.get("value"),
                        mutation["operation"] == "DELETE",
                    )
                )
            end = records[-1].sequence
            frame = {
                "version": 1,
                "idempotency_key": key,
                "request_digest": request_digest,
                "start_sequence": start,
                "end_sequence": end,
                "records": [item.as_dict() for item in records],
            }
            self._wal.append(frame)
            if failpoint == "after_wal_fsync":
                raise SimulatedCrash("simulated crash after WAL fsync")
            entry = {
                "request_digest": request_digest,
                "start_sequence": start,
                "end_sequence": end,
            }
            self._apply_records(records)
            self._idempotency[key] = entry
            self._next_sequence = end + 1
            self._metrics["writes"] += len(records)
            result = {"idempotency_key": key, **entry, "replayed": False}
            if self._memtable_records >= self.memtable_limit:
                self._flush_locked(failpoint=failpoint)
            return result

    def put(
        self, idempotency_key: str, key: str, value: str, *, failpoint: str | None = None
    ) -> dict[str, Any]:
        return self.write_batch(
            idempotency_key,
            [{"operation": "PUT", "key": key, "value": value}],
            failpoint=failpoint,
        )

    def delete(
        self, idempotency_key: str, key: str, *, failpoint: str | None = None
    ) -> dict[str, Any]:
        return self.write_batch(
            idempotency_key,
            [{"operation": "DELETE", "key": key}],
            failpoint=failpoint,
        )

    def _snapshot_sequence(self, snapshot_id: str | None) -> int:
        if snapshot_id is None:
            return self._next_sequence - 1
        snapshot_id = require_identifier(snapshot_id, "snapshot_id")
        if snapshot_id not in self._snapshots:
            raise NotFoundError("snapshot was not found")
        return self._snapshots[snapshot_id]

    @staticmethod
    def _visible(versions: list[VersionedRecord], sequence: int) -> VersionedRecord | None:
        for record in versions:
            if record.sequence <= sequence:
                return record
        return None

    def get(self, key_value: str, *, snapshot_id: str | None = None) -> dict[str, Any]:
        self._ensure_open()
        key = require_key(key_value)
        with self._mutex:
            sequence = self._snapshot_sequence(snapshot_id)
            best = self._visible(self._memtable.get(key, []), sequence)
            for reader in self._readers:
                if not (reader.metadata.min_key <= key <= reader.metadata.max_key):
                    continue
                self._metrics["segment_probes"] += 1
                if not reader.might_contain(key):
                    self._metrics["bloom_negative_skips"] += 1
                    continue
                self._metrics["segment_reads"] += 1
                candidate = reader.get(key, sequence)
                if candidate is not None and (best is None or candidate.sequence > best.sequence):
                    best = candidate
            self._metrics["reads"] += 1
            if best is None or best.tombstone:
                raise NotFoundError("key was not found")
            return {
                "key": key,
                "value": best.value,
                "sequence": best.sequence,
                "snapshot_sequence": sequence,
            }

    def scan(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_open()
        if start is not None:
            start = require_key(start, "start")
        if end is not None:
            end = require_key(end, "end")
        if start is not None and end is not None and start >= end:
            raise ValidationError("start must sort before end")
        limit = require_int(limit, "limit", minimum=1, maximum=10_000)
        with self._mutex:
            sequence = self._snapshot_sequence(snapshot_id)
            keys = set(self._memtable)
            for reader in self._readers:
                keys.update(item.key for item in reader.records())
            selected = [
                key
                for key in sorted(keys)
                if (start is None or key >= start) and (end is None or key < end)
            ]
            items = []
            for key in selected:
                try:
                    item = self.get(key, snapshot_id=snapshot_id)
                except NotFoundError:
                    continue
                items.append(item)
                if len(items) == limit:
                    break
            return {"items": items, "snapshot_sequence": sequence, "limit": limit}

    def create_snapshot(self, snapshot_id: str | None = None) -> dict[str, Any]:
        self._ensure_open()
        snapshot_id = uuid.uuid4().hex if snapshot_id is None else require_identifier(snapshot_id, "snapshot_id")
        with self._mutex:
            if snapshot_id in self._snapshots:
                raise ConflictError("snapshot identity already exists")
            sequence = self._next_sequence - 1
            self._snapshots[snapshot_id] = sequence
            try:
                self._persist_manifest()
            except Exception:
                del self._snapshots[snapshot_id]
                raise
            return {"snapshot_id": snapshot_id, "sequence": sequence}

    def release_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        self._ensure_open()
        snapshot_id = require_identifier(snapshot_id, "snapshot_id")
        with self._mutex:
            if snapshot_id not in self._snapshots:
                raise NotFoundError("snapshot was not found")
            sequence = self._snapshots.pop(snapshot_id)
            try:
                self._persist_manifest()
            except Exception:
                self._snapshots[snapshot_id] = sequence
                raise
            return {"snapshot_id": snapshot_id, "released": True, "sequence": sequence}

    def flush(self, *, failpoint: str | None = None) -> dict[str, Any]:
        self._ensure_open()
        failpoint = self._failpoint(failpoint)
        with self._mutex:
            return self._flush_locked(failpoint=failpoint)

    def _flush_locked(self, *, failpoint: str | None = None) -> dict[str, Any]:
        if not self._memtable_records:
            return {"flushed": False, "record_count": 0, "segment": None}
        records = [record for versions in self._memtable.values() for record in versions]
        segment_id = self._next_segment_id
        metadata = write_segment(self.segment_directory, segment_id, records)
        if failpoint == "after_segment_rename":
            raise SimulatedCrash("simulated crash after segment rename")
        old_segments = self._segments
        old_checkpoint = self._checkpoint_sequence
        old_next_segment = self._next_segment_id
        new_segments = [metadata, *old_segments]
        self._checkpoint_sequence = self._next_sequence - 1
        self._next_segment_id += 1
        try:
            self._persist_manifest(segments=new_segments)
        except Exception:
            self._checkpoint_sequence = old_checkpoint
            self._next_segment_id = old_next_segment
            raise
        self._segments = new_segments
        self._readers = [SegmentReader(self.segment_directory / metadata.filename, metadata), *self._readers]
        self._memtable = {}
        self._memtable_records = 0
        self._metrics["flushes"] += 1
        if failpoint == "after_manifest_commit":
            raise SimulatedCrash("simulated crash after manifest commit")
        self._wal.reset()
        return {"flushed": True, "record_count": len(records), "segment": metadata.as_dict()}

    @staticmethod
    def _records_needed_for_views(
        versions: list[VersionedRecord], view_sequences: set[int]
    ) -> list[VersionedRecord]:
        ordered = sorted(versions, key=lambda item: -item.sequence)
        selected: dict[int, VersionedRecord] = {}
        for view in view_sequences:
            for record in ordered:
                if record.sequence <= view:
                    selected[record.sequence] = record
                    break
        result = sorted(selected.values(), key=lambda item: -item.sequence)
        if result and all(item.tombstone for item in result):
            return []
        return result

    def compact(self, *, failpoint: str | None = None) -> dict[str, Any]:
        self._ensure_open()
        failpoint = self._failpoint(failpoint)
        with self._mutex:
            if self._memtable_records:
                self._flush_locked()
            if len(self._segments) <= 1:
                return {
                    "compacted": False,
                    "input_segments": len(self._segments),
                    "output_segments": len(self._segments),
                    "records_before": sum(item.record_count for item in self._segments),
                    "records_after": sum(item.record_count for item in self._segments),
                }
            by_key: dict[str, list[VersionedRecord]] = {}
            for reader in self._readers:
                for record in reader.records():
                    by_key.setdefault(record.key, []).append(record)
            records_before = sum(len(items) for items in by_key.values())
            views = {self._next_sequence - 1, *self._snapshots.values()}
            retained = [
                item
                for key in sorted(by_key)
                for item in self._records_needed_for_views(by_key[key], views)
            ]
            old_segments = self._segments
            old_readers = self._readers
            new_segments: list[SegmentMetadata] = []
            if retained:
                metadata = write_segment(self.segment_directory, self._next_segment_id, retained)
                if failpoint == "after_segment_rename":
                    raise SimulatedCrash("simulated crash after compacted segment rename")
                new_segments = [metadata]
                self._next_segment_id += 1
            self._persist_manifest(segments=new_segments)
            self._segments = new_segments
            self._readers = self._open_readers(new_segments)
            self._metrics["compactions"] += 1
            if failpoint == "after_manifest_commit":
                raise SimulatedCrash("simulated crash after compaction manifest commit")
            referenced = {item.filename for item in new_segments}
            for item in old_segments:
                if item.filename not in referenced:
                    try:
                        (self.segment_directory / item.filename).unlink()
                    except FileNotFoundError:
                        pass
            _fsync_directory(self.segment_directory)
            del old_readers
            return {
                "compacted": True,
                "input_segments": len(old_segments),
                "output_segments": len(new_segments),
                "records_before": records_before,
                "records_after": len(retained),
                "reclaimed_records": records_before - len(retained),
            }

    def stats(self) -> dict[str, Any]:
        self._ensure_open()
        with self._mutex:
            return {
                "generation": self._generation,
                "latest_sequence": self._next_sequence - 1,
                "checkpoint_sequence": self._checkpoint_sequence,
                "memtable_records": self._memtable_records,
                "segments": len(self._segments),
                "segment_records": sum(item.record_count for item in self._segments),
                "active_snapshots": len(self._snapshots),
                "idempotency_entries": len(self._idempotency),
                "wal_bytes": self.wal_path.stat().st_size,
                **self._metrics,
            }

    def verify_integrity(self) -> dict[str, Any]:
        self._ensure_open()
        with self._mutex:
            payload = self._parse_manifest()
            if payload["generation"] != self._generation:
                raise IntegrityError("in-memory generation differs from manifest")
            readers = self._open_readers(payload["segments"])
            identities: set[tuple[str, int]] = set()
            sequences: set[int] = set()
            records = 0
            tombstones = 0
            for reader in readers:
                previous: tuple[str, int] | None = None
                count = 0
                for record in reader.records():
                    ordering = (record.key, -record.sequence)
                    if previous is not None and ordering <= previous:
                        raise IntegrityError("segment records are not strictly ordered")
                    previous = ordering
                    identity = (record.key, record.sequence)
                    if identity in identities:
                        raise IntegrityError("record exists in more than one live segment")
                    if record.sequence in sequences:
                        raise IntegrityError("sequence exists in more than one live record")
                    if record.sequence > payload["checkpoint_sequence"]:
                        raise IntegrityError("segment record exceeds the durable checkpoint")
                    identities.add(identity)
                    sequences.add(record.sequence)
                    records += 1
                    count += 1
                    tombstones += int(record.tombstone)
                if count != reader.metadata.record_count:
                    raise IntegrityError("segment record count is invalid")
            wal_frames, _ = self._wal.recover()
            expected_sequence = payload["checkpoint_sequence"] + 1
            for raw in wal_frames:
                try:
                    key, request_digest, start, end, _ = self._validate_wal_frame(raw)
                except ValidationError as exc:
                    raise IntegrityError(f"WAL semantic validation failed: {exc}") from exc
                persisted = payload["idempotency"].get(key)
                if end <= payload["checkpoint_sequence"]:
                    if persisted is None or persisted["request_digest"] != request_digest:
                        raise IntegrityError("checkpointed WAL frame is detached from idempotency state")
                    continue
                if start <= payload["checkpoint_sequence"] < end:
                    raise IntegrityError("WAL frame straddles the durable checkpoint")
                if start != expected_sequence:
                    raise IntegrityError("WAL sequence contains a gap or overlap")
                in_memory = self._idempotency.get(key)
                expected_entry = {
                    "request_digest": request_digest,
                    "start_sequence": start,
                    "end_sequence": end,
                }
                if in_memory != expected_entry:
                    raise IntegrityError("WAL frame differs from in-memory idempotency state")
                expected_sequence = end + 1
            if expected_sequence != self._next_sequence:
                raise IntegrityError("WAL does not cover the in-memory sequence head")
            referenced = {item.filename for item in payload["segments"]}
            live_files = {path.name for path in self.segment_directory.glob("segment-*.sst")}
            if live_files != referenced:
                raise IntegrityError("live segment files differ from the signed manifest")
            return {
                "valid": True,
                "manifest_generation": self._generation,
                "segments": len(readers),
                "segment_records": records,
                "tombstones": tombstones,
                "wal_frames": len(wal_frames),
                "active_snapshots": len(self._snapshots),
                "latest_sequence": self._next_sequence - 1,
            }
