from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from cachefence.cache import CacheStore
from cachefence.model import InvalidationEvent, IntegrityError, content_digest, snapshot_for


def event_for(snapshot, *, origin: str = "origin-a") -> InvalidationEvent:
    return InvalidationEvent(
        origin=origin,
        tenant=snapshot.tenant,
        key=snapshot.key,
        version=snapshot.version,
        digest=snapshot.digest,
        deleted=snapshot.deleted,
        created_ns=snapshot.updated_ns,
    )


class CacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "cache.db"
        self.cache = CacheStore(self.path)
        self.v1 = snapshot_for(
            tenant="acme", key="x", version=1, deleted=False, value={"n": 1}, updated_ns=10
        )
        self.v2 = snapshot_for(
            tenant="acme", key="x", version=2, deleted=False, value={"n": 2}, updated_ns=20
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_invalidation_installs_fence(self) -> None:
        result = self.cache.apply_invalidation(event_for(self.v1), now_ns=11)
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.cache.inspect("acme", "x")["fence"]["version"], 1)

    def test_fill_and_hit(self) -> None:
        self.cache.apply_invalidation(event_for(self.v1), now_ns=11)
        self.cache.install_fill(self.v1, ttl_ns=100, now_ns=12)
        result = self.cache.lookup("acme", "x", now_ns=13)
        self.assertEqual(result.status, "hit")
        self.assertEqual(result.snapshot.value, {"n": 1})

    def test_invalidation_evicts_old_value(self) -> None:
        self.cache.install_fill(self.v1, ttl_ns=100, now_ns=11)
        result = self.cache.apply_invalidation(event_for(self.v2), now_ns=21)
        self.assertTrue(result.evicted)
        self.assertEqual(self.cache.lookup("acme", "x", now_ns=22).status, "miss")

    def test_stale_fill_is_rejected(self) -> None:
        self.cache.apply_invalidation(event_for(self.v2), now_ns=21)
        result = self.cache.install_fill(self.v1, ttl_ns=100, now_ns=22)
        self.assertEqual(result.status, "stale")
        self.assertIsNone(self.cache.inspect("acme", "x")["entry"])

    def test_manual_eviction_preserves_fence(self) -> None:
        self.cache.install_fill(self.v2, ttl_ns=100, now_ns=21)
        self.assertTrue(self.cache.evict_value("acme", "x", now_ns=22))
        inspection = self.cache.inspect("acme", "x")
        self.assertIsNone(inspection["entry"])
        self.assertEqual(inspection["fence"]["version"], 2)

    def test_ttl_eviction_preserves_fence(self) -> None:
        self.cache.install_fill(self.v2, ttl_ns=5, now_ns=21)
        self.assertEqual(self.cache.lookup("acme", "x", now_ns=26).status, "miss")
        self.assertEqual(self.cache.inspect("acme", "x")["fence"]["version"], 2)

    def test_duplicate_event_is_idempotent(self) -> None:
        event = event_for(self.v1)
        self.cache.apply_invalidation(event, now_ns=11)
        result = self.cache.apply_invalidation(event, now_ns=12)
        self.assertEqual(result.status, "duplicate")
        self.assertEqual(self.cache.metrics()["invalidations_applied"], 1)

    def test_out_of_order_event_does_not_move_fence_backward(self) -> None:
        self.cache.apply_invalidation(event_for(self.v2), now_ns=21)
        result = self.cache.apply_invalidation(event_for(self.v1), now_ns=22)
        self.assertEqual(result.status, "outdated")
        self.assertEqual(self.cache.inspect("acme", "x")["fence"]["version"], 2)

    def test_same_identity_different_payload_is_quarantined(self) -> None:
        first = event_for(self.v1)
        self.cache.apply_invalidation(first, now_ns=11)
        contradictory = InvalidationEvent(
            origin="origin-a",
            tenant="acme",
            key="x",
            version=1,
            digest=content_digest(deleted=False, value={"n": 99}),
            deleted=False,
            created_ns=10,
        )
        with self.assertRaises(IntegrityError):
            self.cache.apply_invalidation(contradictory, now_ns=12)
        self.assertEqual(self.cache.quarantine_count(), 1)

    def test_same_version_fill_mismatch_is_quarantined(self) -> None:
        self.cache.apply_invalidation(event_for(self.v1), now_ns=11)
        contradictory = snapshot_for(
            tenant="acme", key="x", version=1, deleted=False, value={"n": 99}, updated_ns=10
        )
        with self.assertRaises(IntegrityError):
            self.cache.install_fill(contradictory, ttl_ns=100, now_ns=12)
        self.assertEqual(self.cache.quarantine_count(), 1)

    def test_negative_entry_is_a_cache_hit(self) -> None:
        deleted = snapshot_for(
            tenant="acme", key="gone", version=3, deleted=True, value=None, updated_ns=30
        )
        self.cache.install_fill(deleted, ttl_ns=100, now_ns=31)
        result = self.cache.lookup("acme", "gone", now_ns=32)
        self.assertEqual(result.status, "negative")
        self.assertTrue(result.snapshot.deleted)

    def test_wrong_origin_event_is_rejected(self) -> None:
        with self.assertRaises(IntegrityError):
            self.cache.apply_invalidation(event_for(self.v1, origin="origin-b"))

    def test_database_identity_is_fenced(self) -> None:
        with self.assertRaises(IntegrityError):
            CacheStore(self.path, cache_id="other-cache")
        with self.assertRaises(IntegrityError):
            CacheStore(self.path, origin_id="other-origin")

    def test_corrupt_cached_content_fails_closed(self) -> None:
        self.cache.install_fill(self.v1, ttl_ns=100, now_ns=11)
        self.cache.unsafe_inject_entry_for_test(
            self.v1, ttl_ns=100, now_ns=12, digest_override="0" * 64
        )
        with self.assertRaises(IntegrityError):
            self.cache.lookup("acme", "x", now_ns=13)
        self.assertEqual(self.cache.lookup("acme", "x", now_ns=14).status, "miss")
        self.assertEqual(self.cache.quarantine_count(), 1)

    def test_entry_ahead_of_fence_fails_closed(self) -> None:
        self.cache.install_fill(self.v1, ttl_ns=100, now_ns=11)
        self.cache.unsafe_inject_entry_for_test(self.v2, ttl_ns=100, now_ns=12)
        with self.assertRaises(IntegrityError):
            self.cache.lookup("acme", "x", now_ns=13)
        self.assertEqual(self.cache.quarantine_count(), 1)

    def test_restart_preserves_fence_and_entry(self) -> None:
        self.cache.install_fill(self.v1, ttl_ns=100, now_ns=11)
        restarted = CacheStore(self.path)
        self.assertEqual(restarted.lookup("acme", "x", now_ns=12).snapshot, self.v1)

    def test_verify_covers_entries_and_audit(self) -> None:
        self.cache.apply_invalidation(event_for(self.v1), now_ns=11)
        self.cache.install_fill(self.v1, ttl_ns=100, now_ns=12)
        result = self.cache.verify()
        self.assertTrue(result["ok"])
        self.assertEqual(result["entries"], 1)
        self.assertEqual(result["fences"], 1)

    def test_verify_detects_audit_tampering(self) -> None:
        self.cache.apply_invalidation(event_for(self.v1), now_ns=11)
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE audit_log SET detail_json = '{}' WHERE sequence = 1")
        connection.commit()
        connection.close()
        with self.assertRaises(IntegrityError):
            self.cache.verify()


if __name__ == "__main__":
    unittest.main()
