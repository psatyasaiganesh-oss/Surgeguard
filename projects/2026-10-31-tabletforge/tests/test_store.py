from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from tabletforge.errors import (
    ConflictError,
    IntegrityError,
    NotFoundError,
    SimulatedCrash,
    StateError,
    ValidationError,
)
from tabletforge.store import TabletForge
from tabletforge.wal import WriteAheadLog


SECRET = b"store-unit-test-secret-that-is-at-least-32-bytes"
OTHER_SECRET = b"different-store-unit-test-secret-at-least-32"


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name) / "tablet"
        self.store = TabletForge(self.directory, secret=SECRET, memtable_limit=1_000)

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def reopen(self, *, secret: bytes = SECRET, memtable_limit: int = 1_000) -> None:
        self.store.close()
        self.store = TabletForge(
            self.directory,
            secret=secret,
            memtable_limit=memtable_limit,
        )

    def assert_missing(self, key: str, *, snapshot_id: str | None = None) -> None:
        with self.assertRaises(NotFoundError):
            self.store.get(key, snapshot_id=snapshot_id)

    def test_secret_must_be_at_least_32_bytes(self) -> None:
        with self.assertRaises(ValidationError):
            TabletForge(Path(self.temporary.name) / "short", secret=b"short")

    def test_only_one_writer_can_open_a_directory(self) -> None:
        with self.assertRaises(StateError):
            TabletForge(self.directory, secret=SECRET)

    def test_closed_store_rejects_operations(self) -> None:
        self.store.close()
        with self.assertRaises(StateError):
            self.store.stats()

    def test_put_get_and_delete(self) -> None:
        written = self.store.put("create-a", "a", "value")
        self.assertEqual(written["start_sequence"], 1)
        self.assertEqual(self.store.get("a")["value"], "value")
        self.store.delete("delete-a", "a")
        self.assert_missing("a")

    def test_batch_sequences_are_contiguous(self) -> None:
        result = self.store.write_batch(
            "batch-1",
            [
                {"operation": "PUT", "key": "a", "value": "1"},
                {"operation": "PUT", "key": "b", "value": "2"},
                {"operation": "DELETE", "key": "c"},
            ],
        )
        self.assertEqual((result["start_sequence"], result["end_sequence"]), (1, 3))
        self.assertEqual(self.store.get("a")["sequence"], 1)
        self.assertEqual(self.store.get("b")["sequence"], 2)

    def test_invalid_batch_does_not_advance_sequence(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.write_batch("bad", [])
        self.assertEqual(self.store.put("good", "a", "1")["start_sequence"], 1)

    def test_idempotent_retry_returns_original_result(self) -> None:
        first = self.store.put("same", "a", "one")
        second = self.store.put("same", "a", "one")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["start_sequence"], second["start_sequence"])
        self.assertEqual(self.store.stats()["latest_sequence"], 1)

    def test_idempotency_key_reuse_with_different_request_conflicts(self) -> None:
        self.store.put("same", "a", "one")
        with self.assertRaises(ConflictError):
            self.store.put("same", "a", "two")

    def test_unflushed_wal_recovers_on_restart(self) -> None:
        self.store.put("durable", "a", "from-wal")
        self.reopen()
        self.assertEqual(self.store.get("a")["value"], "from-wal")
        self.assertEqual(self.store.stats()["recovered_wal_frames"], 1)

    def test_crash_after_wal_fsync_recovers_and_deduplicates(self) -> None:
        with self.assertRaises(SimulatedCrash):
            self.store.put("crash", "a", "durable", failpoint="after_wal_fsync")
        self.reopen()
        self.assertEqual(self.store.get("a")["value"], "durable")
        replay = self.store.put("crash", "a", "durable")
        self.assertTrue(replay["replayed"])

    def test_partial_wal_tail_is_truncated(self) -> None:
        self.store.put("durable", "a", "one")
        self.store.close()
        with (self.directory / "wal.log").open("ab") as stream:
            stream.write(b"\x00\x00\x00")
        self.store = TabletForge(self.directory, secret=SECRET)
        self.assertEqual(self.store.get("a")["value"], "one")
        self.assertEqual(self.store.stats()["truncated_wal_bytes"], 3)

    def test_wal_corruption_fails_closed(self) -> None:
        self.store.put("durable", "a", "one")
        self.store.close()
        path = self.directory / "wal.log"
        with path.open("r+b") as stream:
            stream.seek(8)
            byte = stream.read(1)
            stream.seek(8)
            stream.write(bytes([byte[0] ^ 1]))
        with self.assertRaises(IntegrityError):
            TabletForge(self.directory, secret=SECRET)
        self.store = TabletForge(Path(self.temporary.name) / "replacement", secret=SECRET)

    def test_manifest_ahead_of_missing_wal_fails_closed(self) -> None:
        self.store.put("durable", "a", "one")
        self.store.create_snapshot("persists-head")
        self.store.close()
        (self.directory / "wal.log").write_bytes(b"")
        with self.assertRaises(IntegrityError):
            TabletForge(self.directory, secret=SECRET)
        self.store = TabletForge(Path(self.temporary.name) / "replacement", secret=SECRET)

    def test_authenticated_wal_with_wrong_request_digest_fails_semantics(self) -> None:
        self.store.close()
        WriteAheadLog(self.directory / "wal.log", SECRET).append(
            {
                "version": 1,
                "idempotency_key": "wrong-digest",
                "request_digest": "0" * 64,
                "start_sequence": 1,
                "end_sequence": 1,
                "records": [
                    {
                        "key": "a",
                        "sequence": 1,
                        "value": "one",
                        "tombstone": False,
                    }
                ],
            }
        )
        with self.assertRaises(IntegrityError):
            TabletForge(self.directory, secret=SECRET)
        self.store = TabletForge(Path(self.temporary.name) / "replacement", secret=SECRET)

    def test_flush_persists_records_and_resets_wal(self) -> None:
        self.store.put("one", "a", "1")
        result = self.store.flush()
        self.assertTrue(result["flushed"])
        self.assertEqual(result["record_count"], 1)
        self.assertEqual(self.store.stats()["wal_bytes"], 0)
        self.reopen()
        self.assertEqual(self.store.get("a")["value"], "1")

    def test_empty_flush_is_noop(self) -> None:
        self.assertEqual(
            self.store.flush(),
            {"flushed": False, "record_count": 0, "segment": None},
        )

    def test_memtable_limit_triggers_flush(self) -> None:
        self.reopen(memtable_limit=2)
        self.store.write_batch(
            "two",
            [
                {"operation": "PUT", "key": "a", "value": "1"},
                {"operation": "PUT", "key": "b", "value": "2"},
            ],
        )
        stats = self.store.stats()
        self.assertEqual(stats["memtable_records"], 0)
        self.assertEqual(stats["segments"], 1)

    def test_crash_after_segment_rename_quarantines_orphan(self) -> None:
        self.store.put("one", "a", "durable")
        with self.assertRaises(SimulatedCrash):
            self.store.flush(failpoint="after_segment_rename")
        self.reopen()
        self.assertEqual(self.store.stats()["orphan_segments_quarantined"], 1)
        self.assertEqual(self.store.get("a")["value"], "durable")
        self.assertTrue(any((self.directory / "lost+found").iterdir()))

    def test_crash_after_manifest_commit_does_not_duplicate_record(self) -> None:
        self.store.put("one", "a", "durable")
        with self.assertRaises(SimulatedCrash):
            self.store.flush(failpoint="after_manifest_commit")
        self.reopen()
        self.assertEqual(self.store.get("a")["value"], "durable")
        self.assertEqual(self.store.stats()["memtable_records"], 0)
        self.assertTrue(self.store.put("one", "a", "durable")["replayed"])

    def test_unknown_failpoint_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.flush(failpoint="unknown")

    def test_snapshot_reads_previous_versions_and_deletions(self) -> None:
        self.store.put("a-old", "a", "old")
        self.store.put("b-old", "b", "present")
        snapshot = self.store.create_snapshot("before")
        self.store.put("a-new", "a", "new")
        self.store.delete("b-delete", "b")
        self.assertEqual(snapshot["sequence"], 2)
        self.assertEqual(self.store.get("a", snapshot_id="before")["value"], "old")
        self.assertEqual(self.store.get("b", snapshot_id="before")["value"], "present")
        self.assertEqual(self.store.get("a")["value"], "new")
        self.assert_missing("b")

    def test_snapshot_and_unflushed_data_survive_restart(self) -> None:
        self.store.put("one", "a", "old")
        self.store.create_snapshot("stable")
        self.store.put("two", "a", "new")
        self.reopen()
        self.assertEqual(self.store.get("a", snapshot_id="stable")["value"], "old")
        self.assertEqual(self.store.get("a")["value"], "new")

    def test_snapshot_names_are_unique_and_release_is_checked(self) -> None:
        self.store.create_snapshot("stable")
        with self.assertRaises(ConflictError):
            self.store.create_snapshot("stable")
        released = self.store.release_snapshot("stable")
        self.assertTrue(released["released"])
        with self.assertRaises(NotFoundError):
            self.store.release_snapshot("stable")
        with self.assertRaises(NotFoundError):
            self.store.get("a", snapshot_id="stable")

    def test_scan_orders_filters_limits_and_hides_tombstones(self) -> None:
        for key in ["d", "a", "c", "b"]:
            self.store.put(f"put-{key}", key, key.upper())
        self.store.delete("delete-c", "c")
        result = self.store.scan(start="b", end="z", limit=2)
        self.assertEqual([item["key"] for item in result["items"]], ["b", "d"])
        with self.assertRaises(ValidationError):
            self.store.scan(start="z", end="a")

    def test_bloom_filter_skips_absent_keys_within_segment_range(self) -> None:
        self.store.write_batch(
            "bounds",
            [
                {"operation": "PUT", "key": "a", "value": "1"},
                {"operation": "PUT", "key": "z", "value": "2"},
            ],
        )
        self.store.flush()
        for index in range(100):
            self.assert_missing(f"m:{index}")
        self.assertGreater(self.store.stats()["bloom_negative_skips"], 0)

    def test_compaction_with_one_segment_is_noop(self) -> None:
        self.store.put("one", "a", "1")
        self.store.flush()
        result = self.store.compact()
        self.assertFalse(result["compacted"])
        self.assertEqual(result["input_segments"], 1)

    def test_compaction_reclaims_old_versions_and_tombstones(self) -> None:
        self.store.write_batch(
            "old",
            [
                {"operation": "PUT", "key": "a", "value": "old"},
                {"operation": "PUT", "key": "b", "value": "old"},
            ],
        )
        self.store.flush()
        self.store.write_batch(
            "new",
            [
                {"operation": "PUT", "key": "a", "value": "new"},
                {"operation": "DELETE", "key": "b"},
            ],
        )
        self.store.flush()
        result = self.store.compact()
        self.assertTrue(result["compacted"])
        self.assertEqual(result["records_before"], 4)
        self.assertEqual(result["records_after"], 1)
        self.assertEqual(self.store.get("a")["value"], "new")
        self.assert_missing("b")

    def test_compaction_can_remove_all_deleted_data(self) -> None:
        self.store.put("old", "a", "old")
        self.store.flush()
        self.store.delete("delete", "a")
        self.store.flush()
        result = self.store.compact()
        self.assertEqual(result["records_after"], 0)
        self.assertEqual(result["output_segments"], 0)
        self.assert_missing("a")

    def test_compaction_retains_versions_required_by_snapshot(self) -> None:
        self.store.write_batch(
            "old",
            [
                {"operation": "PUT", "key": "a", "value": "old"},
                {"operation": "PUT", "key": "b", "value": "old"},
            ],
        )
        self.store.flush()
        self.store.create_snapshot("before")
        self.store.write_batch(
            "new",
            [
                {"operation": "PUT", "key": "a", "value": "new"},
                {"operation": "DELETE", "key": "b"},
            ],
        )
        self.store.flush()
        compacted = self.store.compact()
        self.assertEqual(compacted["records_after"], 4)
        self.assertEqual(self.store.get("a", snapshot_id="before")["value"], "old")
        self.assertEqual(self.store.get("b", snapshot_id="before")["value"], "old")
        self.assertEqual(self.store.get("a")["value"], "new")
        self.assert_missing("b")

    def test_releasing_snapshot_allows_later_compaction_to_reclaim(self) -> None:
        self.store.put("old", "a", "old")
        self.store.flush()
        self.store.create_snapshot("before")
        self.store.put("new", "a", "new")
        self.store.flush()
        self.store.compact()
        self.store.release_snapshot("before")
        self.store.put("newer", "a", "newer")
        self.store.flush()
        result = self.store.compact()
        self.assertEqual(result["records_after"], 1)
        self.assertEqual(self.store.get("a")["value"], "newer")

    def test_stats_and_integrity_report_state(self) -> None:
        self.store.put("one", "a", "1")
        self.store.flush()
        stats = self.store.stats()
        integrity = self.store.verify_integrity()
        self.assertEqual(stats["latest_sequence"], 1)
        self.assertEqual(stats["segments"], 1)
        self.assertTrue(integrity["valid"])
        self.assertEqual(integrity["segment_records"], 1)

    def test_manifest_tampering_fails_closed(self) -> None:
        self.store.put("one", "a", "1")
        self.store.flush()
        self.store.close()
        manifest_path = self.directory / "MANIFEST.json"
        envelope = json.loads(manifest_path.read_text(encoding="utf-8"))
        envelope["payload"]["generation"] += 1
        manifest_path.write_text(json.dumps(envelope), encoding="utf-8")
        with self.assertRaises(IntegrityError):
            TabletForge(self.directory, secret=SECRET)
        self.store = TabletForge(Path(self.temporary.name) / "replacement", secret=SECRET)

    def test_wrong_secret_cannot_open_manifest(self) -> None:
        self.store.close()
        with self.assertRaises(IntegrityError):
            TabletForge(self.directory, secret=OTHER_SECRET)
        self.store = TabletForge(Path(self.temporary.name) / "replacement", secret=SECRET)

    def test_segment_tampering_fails_closed(self) -> None:
        self.store.put("one", "a", "1")
        segment = self.store.flush()["segment"]["filename"]
        self.store.close()
        path = self.directory / "segments" / segment
        with path.open("r+b") as stream:
            stream.seek(10)
            byte = stream.read(1)
            stream.seek(10)
            stream.write(bytes([byte[0] ^ 1]))
        with self.assertRaises(IntegrityError):
            TabletForge(self.directory, secret=SECRET)
        self.store = TabletForge(Path(self.temporary.name) / "replacement", secret=SECRET)

    def test_concurrent_writers_receive_unique_sequences(self) -> None:
        results: list[int] = []
        failures: list[Exception] = []
        result_lock = threading.Lock()

        def write(index: int) -> None:
            try:
                result = self.store.put(f"thread-{index}", f"k:{index}", str(index))
                with result_lock:
                    results.append(result["start_sequence"])
            except Exception as exc:  # pragma: no cover - assertion captures unexpected failures
                with result_lock:
                    failures.append(exc)

        threads = [threading.Thread(target=write, args=(index,)) for index in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        self.assertEqual(sorted(results), list(range(1, 21)))


if __name__ == "__main__":
    unittest.main()
