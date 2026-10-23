from __future__ import annotations

import concurrent.futures
import json
import tempfile
import threading
import unittest
from pathlib import Path

from cachefence.cache import CacheStore
from cachefence.model import AuthorizationError, IntegrityError, ValidationError, snapshot_for
from cachefence.origin import OriginStore
from cachefence.pipeline import deliver_once, sign_event, verify_envelope
from cachefence.probe import ConsistencyProbe
from cachefence.service import CacheAsideService

SECRET = b"unit-test-secret-material-at-least-32-bytes"


class StackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.origin = OriginStore(root / "origin.db")
        self.cache = CacheStore(root / "cache.db")
        self.service = CacheAsideService(self.origin, self.cache)
        self.probe = ConsistencyProbe(root / "probe.db", self.origin, self.cache)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_signed_envelope_round_trip(self) -> None:
        snapshot = self.origin.write("acme", "x", 1)
        claim = self.origin.claim_events("worker", lease_ns=100_000_000)[0]
        envelope = sign_event(claim.event, key_id="origin-a", secret=SECRET)
        self.assertEqual(verify_envelope(envelope, keys={"origin-a": SECRET}), claim.event)
        self.assertEqual(snapshot.version, 1)

    def test_tampered_envelope_is_rejected(self) -> None:
        self.origin.write("acme", "x", 1)
        claim = self.origin.claim_events("worker", lease_ns=100_000_000)[0]
        value = json.loads(sign_event(claim.event, key_id="origin-a", secret=SECRET))
        value["event"]["digest"] = "0" * 64
        with self.assertRaises(AuthorizationError):
            verify_envelope(json.dumps(value), keys={"origin-a": SECRET})

    def test_unknown_signing_key_is_rejected(self) -> None:
        self.origin.write("acme", "x", 1)
        claim = self.origin.claim_events("worker", lease_ns=100_000_000)[0]
        envelope = sign_event(claim.event, key_id="origin-a", secret=SECRET)
        with self.assertRaises(AuthorizationError):
            verify_envelope(envelope, keys={})

    def test_short_secret_is_rejected(self) -> None:
        self.origin.write("acme", "x", 1)
        claim = self.origin.claim_events("worker", lease_ns=100_000_000)[0]
        with self.assertRaises(ValidationError):
            sign_event(claim.event, key_id="origin-a", secret=b"short")

    def test_delivery_commits_cache_before_ack(self) -> None:
        self.origin.write("acme", "x", 1)
        result = deliver_once(self.origin, self.cache, secret=SECRET)
        self.assertEqual(result.applied, 1)
        self.assertEqual(result.acknowledged, 1)
        self.assertEqual(self.origin.outbox_stats()["delivered"], 1)
        self.assertEqual(self.cache.inspect("acme", "x")["fence"]["version"], 1)

    def test_lost_ack_is_deduplicated_after_lease(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        first = deliver_once(
            self.origin, self.cache, secret=SECRET, lease_ns=5, now_ns=11, acknowledge=False
        )
        second = deliver_once(self.origin, self.cache, secret=SECRET, lease_ns=5, now_ns=17)
        self.assertEqual(first.applied, 1)
        self.assertEqual(second.duplicates, 1)
        self.assertEqual(self.origin.outbox_stats()["delivered"], 1)

    def test_cache_aside_hit_avoids_second_origin_read(self) -> None:
        self.origin.write("acme", "x", {"n": 1})
        first = self.service.get("acme", "x")
        second = self.service.get("acme", "x")
        self.assertEqual(first.source, "origin")
        self.assertEqual(second.source, "cache")
        self.assertEqual(self.origin.metrics()["reads"], 1)

    def test_deleted_snapshot_is_negative_cached(self) -> None:
        self.origin.write("acme", "x", 1)
        self.origin.write("acme", "x", delete=True)
        first = self.service.get("acme", "x")
        second = self.service.get("acme", "x")
        self.assertEqual(first.status, "not_found")
        self.assertEqual(second.source, "cache")
        self.assertEqual(self.origin.metrics()["reads"], 1)

    def test_missing_key_returns_not_found(self) -> None:
        result = self.service.get("acme", "missing")
        self.assertEqual(result.status, "not_found")
        self.assertIsNone(result.snapshot)

    def test_stale_fill_race_refetches_new_version(self) -> None:
        self.origin.write("acme", "x", {"n": 1})
        deliver_once(self.origin, self.cache, secret=SECRET)

        def race(_snapshot) -> None:
            self.origin.write("acme", "x", {"n": 2}, expected_version=1)
            deliver_once(self.origin, self.cache, secret=SECRET)

        result = self.service.get("acme", "x", after_origin_read=race)
        self.assertTrue(result.stale_fill_retried)
        self.assertEqual(result.snapshot.version, 2)
        self.assertEqual(result.snapshot.value, {"n": 2})

    def test_singleflight_coalesces_concurrent_misses(self) -> None:
        self.origin.write("acme", "hot", 7)
        callers = 12
        barrier = threading.Barrier(callers)

        def read(_: int) -> int:
            barrier.wait()
            result = self.service.get("acme", "hot", source_delay_s=0.02)
            return result.snapshot.value

        with concurrent.futures.ThreadPoolExecutor(max_workers=callers) as executor:
            values = list(executor.map(read, range(callers)))
        self.assertEqual(values, [7] * callers)
        self.assertEqual(self.origin.metrics()["reads"], 1)

    def test_probe_retries_then_alerts_then_resolves(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        deliver_once(self.origin, self.cache, secret=SECRET, now_ns=11)
        self.service.get("acme", "x", now_ns=12)
        self.origin.write("acme", "x", 2, now_ns=13)
        first = self.probe.observe("acme", "x", report_after_ns=5, now_ns=14)
        second = self.probe.observe("acme", "x", report_after_ns=5, now_ns=20)
        deliver_once(self.origin, self.cache, secret=SECRET, now_ns=21)
        third = self.probe.observe("acme", "x", report_after_ns=5, now_ns=22)
        self.assertEqual((first.status, second.status, third.status), ("retry", "alert", "resolved"))
        self.assertEqual(first.classification, "invalidation_lag")

    def test_probe_treats_cache_miss_as_client_safe(self) -> None:
        self.origin.write("acme", "x", 1)
        result = self.probe.observe("acme", "x")
        self.assertEqual(result.status, "consistent")

    def test_probe_classifies_same_version_payload_mismatch(self) -> None:
        good = self.origin.write("acme", "x", {"n": 1}, now_ns=10)
        self.cache.install_fill(good, ttl_ns=100, now_ns=11)
        bad = snapshot_for(
            tenant="acme", key="x", version=1, deleted=False, value={"n": 2}, updated_ns=10
        )
        self.cache.unsafe_inject_entry_for_test(bad, ttl_ns=100, now_ns=12)
        result = self.probe.observe("acme", "x", now_ns=13)
        self.assertEqual(result.classification, "same_version_payload_mismatch")

    def test_probe_classifies_entry_without_origin_record(self) -> None:
        ghost = snapshot_for(
            tenant="acme", key="ghost", version=1, deleted=False, value=7, updated_ns=10
        )
        self.cache.install_fill(ghost, ttl_ns=100, now_ns=11)
        result = self.probe.observe("acme", "ghost", now_ns=12)
        self.assertEqual(result.classification, "orphan_cache_entry")

    def test_end_to_end_integrity_verification(self) -> None:
        self.origin.write("acme", "x", {"n": 1})
        deliver_once(self.origin, self.cache, secret=SECRET)
        self.service.get("acme", "x")
        self.assertTrue(self.origin.verify()["ok"])
        self.assertTrue(self.cache.verify()["ok"])


if __name__ == "__main__":
    unittest.main()
