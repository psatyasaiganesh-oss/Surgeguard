from __future__ import annotations

import json
import shutil
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from .errors import IntegrityError, NotFoundError, SimulatedCrash, ValidationError
from .store import TabletForge


DEMO_SECRET = b"tabletforge-reproducible-demo-secret-32-bytes"


def missing(store: TabletForge, key: str, *, snapshot_id: str | None = None) -> bool:
    try:
        store.get(key, snapshot_id=snapshot_id)
    except NotFoundError:
        return True
    return False


def run_demo(directory: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if directory is None:
        temporary = tempfile.TemporaryDirectory()
        directory = Path(temporary.name) / "tablet"
    root = Path(directory)

    store = TabletForge(root, secret=DEMO_SECRET, memtable_limit=100)
    initial = store.write_batch(
        "batch-initial",
        [
            {"operation": "PUT", "key": "account:alice", "value": "balance=100"},
            {"operation": "PUT", "key": "account:bob", "value": "balance=75"},
        ],
    )
    snapshot = store.create_snapshot("before-transfer")
    store.write_batch(
        "batch-transfer",
        [
            {"operation": "PUT", "key": "account:alice", "value": "balance=60"},
            {"operation": "DELETE", "key": "account:bob"},
        ],
    )

    crash_after_wal = False
    try:
        store.put(
            "durable-order",
            "order:1001",
            "PAID",
            failpoint="after_wal_fsync",
        )
    except SimulatedCrash:
        crash_after_wal = True
    store.close()

    store = TabletForge(root, secret=DEMO_SECRET, memtable_limit=100)
    recovered_order = store.get("order:1001")["value"]
    recovered_retry = store.put("durable-order", "order:1001", "PAID")

    orphan_failpoint = False
    try:
        store.flush(failpoint="after_segment_rename")
    except SimulatedCrash:
        orphan_failpoint = True
    store.close()

    store = TabletForge(root, secret=DEMO_SECRET, memtable_limit=100)
    orphan_count = store.stats()["orphan_segments_quarantined"]
    first_flush = store.flush()
    store.write_batch(
        "batch-later",
        [
            {"operation": "PUT", "key": "account:carol", "value": "balance=30"},
            {"operation": "PUT", "key": "order:1002", "value": "PENDING"},
        ],
    )
    store.flush()
    compact_with_snapshot = store.compact()
    old_alice = store.get("account:alice", snapshot_id="before-transfer")["value"]
    old_bob = store.get("account:bob", snapshot_id="before-transfer")["value"]
    current_alice = store.get("account:alice")["value"]
    current_bob_missing = missing(store, "account:bob")

    store.release_snapshot("before-transfer")
    store.write_batch(
        "batch-final",
        [
            {"operation": "PUT", "key": "account:alice", "value": "balance=55"},
            {"operation": "DELETE", "key": "account:carol"},
        ],
    )
    store.flush()
    compact_without_snapshot = store.compact()
    missing(store, "middle:absent")
    stats = store.stats()
    integrity = store.verify_integrity()
    store.close()
    check_store = TabletForge(root, secret=DEMO_SECRET)
    deleted_carol_missing = missing(check_store, "account:carol")
    check_store.close()

    partial_root = root.parent / "partial-tail-copy"
    if partial_root.exists():
        shutil.rmtree(partial_root)
    shutil.copytree(root, partial_root)
    partial = TabletForge(partial_root, secret=DEMO_SECRET, memtable_limit=100)
    partial.put("tail-record", "recovery:tail", "durable")
    partial.close()
    with (partial_root / "wal.log").open("ab") as stream:
        stream.write(b"\x00\x00")
        stream.flush()
    partial = TabletForge(partial_root, secret=DEMO_SECRET, memtable_limit=100)
    partial_tail_recovered = partial.get("recovery:tail")["value"] == "durable"
    truncated_bytes = partial.stats()["truncated_wal_bytes"]
    partial.close()

    tamper_root = root.parent / "tamper-copy"
    if tamper_root.exists():
        shutil.rmtree(tamper_root)
    shutil.copytree(root, tamper_root)
    manifest_path = tamper_root / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["payload"]["generation"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    tamper_rejected = False
    try:
        TabletForge(tamper_root, secret=DEMO_SECRET)
    except IntegrityError:
        tamper_rejected = True

    result = {
        "project": "TabletForge",
        "initial_batch": initial,
        "snapshot": snapshot,
        "recovery": {
            "crash_after_wal_fsync_triggered": crash_after_wal,
            "durable_order_recovered": recovered_order == "PAID",
            "retry_returned_same_sequence": recovered_retry["replayed"]
            and recovered_retry["start_sequence"] == 5,
            "crash_after_segment_rename_triggered": orphan_failpoint,
            "orphan_segment_quarantined": orphan_count == 1,
            "committed_flush_records": first_flush["record_count"],
            "partial_wal_tail_recovered": partial_tail_recovered,
            "partial_wal_bytes_truncated": truncated_bytes,
        },
        "snapshot_isolation": {
            "snapshot_alice": old_alice,
            "snapshot_bob": old_bob,
            "current_alice": current_alice,
            "current_bob_missing": current_bob_missing,
        },
        "compaction": {
            "with_snapshot": compact_with_snapshot,
            "after_snapshot_release": compact_without_snapshot,
            "deleted_carol_missing": deleted_carol_missing,
        },
        "bloom_negative_skips": stats["bloom_negative_skips"],
        "tampered_manifest_rejected": tamper_rejected,
        "integrity": integrity,
    }
    if temporary is not None:
        temporary.cleanup()
    return result


def run_benchmark(entries: int = 3_000, reads: int = 1_000) -> dict[str, Any]:
    if isinstance(entries, bool) or not isinstance(entries, int) or not 100 <= entries <= 20_000:
        raise ValidationError("entries must be between 100 and 20,000")
    if isinstance(reads, bool) or not isinstance(reads, int) or not 1 <= reads <= 20_000:
        raise ValidationError("reads must be between 1 and 20,000")
    with tempfile.TemporaryDirectory() as directory:
        store = TabletForge(
            Path(directory) / "tablet",
            secret=DEMO_SECRET,
            memtable_limit=500,
        )
        started = time.perf_counter()
        for batch_start in range(0, entries, 100):
            mutations = [
                {
                    "operation": "PUT",
                    "key": f"customer:{index:06d}",
                    "value": f"profile-{index:06d}",
                }
                for index in range(batch_start, min(entries, batch_start + 100))
            ]
            store.write_batch(f"load:{batch_start:06d}", mutations)
        store.flush()
        write_seconds = time.perf_counter() - started

        latencies_ms: list[float] = []
        for index in range(reads):
            key_index = (index * 7919) % entries
            read_started = time.perf_counter()
            value = store.get(f"customer:{key_index:06d}")
            assert value["value"] == f"profile-{key_index:06d}"
            latencies_ms.append((time.perf_counter() - read_started) * 1000)

        missing_reads = min(500, reads)
        for index in range(missing_reads):
            try:
                store.get(f"customer:{index % entries:06d}:missing")
            except NotFoundError:
                pass

        update_count = entries // 10
        for batch_start in range(0, update_count, 100):
            mutations = []
            for index in range(batch_start, min(update_count, batch_start + 100)):
                if index % 2:
                    mutations.append(
                        {"operation": "PUT", "key": f"customer:{index:06d}", "value": "updated"}
                    )
                else:
                    mutations.append({"operation": "DELETE", "key": f"customer:{index:06d}"})
            store.write_batch(f"update:{batch_start:06d}", mutations)
        store.flush()
        compaction_started = time.perf_counter()
        compaction = store.compact()
        compaction_ms = (time.perf_counter() - compaction_started) * 1000
        integrity_started = time.perf_counter()
        integrity = store.verify_integrity()
        integrity_ms = (time.perf_counter() - integrity_started) * 1000
        stats = store.stats()
        store.close()
        ordered = sorted(latencies_ms)
        p95_index = max(0, int(0.95 * len(ordered)) - 1)
        return {
            "entries": entries,
            "read_requests": reads,
            "missing_read_requests": missing_reads,
            "write_batches": (entries + 99) // 100,
            "write_throughput_entries_per_second": round(entries / write_seconds, 2),
            "point_read_latency_ms": {
                "p50": round(statistics.median(ordered), 6),
                "p95": round(ordered[p95_index], 6),
            },
            "segments_before_compaction": compaction["input_segments"],
            "records_before_compaction": compaction["records_before"],
            "records_after_compaction": compaction["records_after"],
            "reclaimed_records": compaction["reclaimed_records"],
            "compaction_ms": round(compaction_ms, 6),
            "integrity_scan_ms": round(integrity_ms, 6),
            "bloom_negative_skips": stats["bloom_negative_skips"],
            "segment_reads": stats["segment_reads"],
            "integrity": integrity,
        }
