from __future__ import annotations

import concurrent.futures
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cachefence.model import (
    AuthorizationError,
    ConflictError,
    IntegrityError,
    LeaseExpiredError,
    ValidationError,
)
from cachefence.origin import OriginStore


class OriginTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "origin.db"
        self.origin = OriginStore(self.path)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_write_and_read(self) -> None:
        written = self.origin.write("acme", "x", {"count": 1}, expected_version=0, now_ns=10)
        self.assertEqual(written, self.origin.peek_snapshot("acme", "x"))
        self.assertEqual(self.origin.outbox_stats()["pending"], 1)

    def test_compare_and_set_conflict_is_atomic(self) -> None:
        self.origin.write("acme", "x", 1, expected_version=0)
        with self.assertRaises(ConflictError):
            self.origin.write("acme", "x", 2, expected_version=0)
        self.assertEqual(self.origin.peek_snapshot("acme", "x").value, 1)
        self.assertEqual(self.origin.outbox_stats()["pending"], 1)

    def test_delete_is_first_class_snapshot(self) -> None:
        self.origin.write("acme", "x", 1)
        deleted = self.origin.write("acme", "x", delete=True, expected_version=1)
        self.assertTrue(deleted.deleted)
        self.assertIsNone(deleted.value)
        self.assertEqual(deleted.version, 2)

    def test_delete_rejects_value(self) -> None:
        with self.assertRaises(ValidationError):
            self.origin.write("acme", "x", 1, delete=True)

    def test_missing_read_is_counted(self) -> None:
        self.assertIsNone(self.origin.read_snapshot("acme", "missing"))
        self.assertEqual(self.origin.metrics()["reads"], 1)

    def test_peek_does_not_increment_reads(self) -> None:
        self.assertIsNone(self.origin.peek_snapshot("acme", "missing"))
        self.assertNotIn("reads", self.origin.metrics())

    def test_claim_and_ack(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        claim = self.origin.claim_events("worker", lease_ns=100, now_ns=11)[0]
        self.origin.ack_event(claim.event.event_id, claim.token, now_ns=12)
        self.assertEqual(self.origin.outbox_stats()["delivered"], 1)

    def test_bad_capability_cannot_ack(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        claim = self.origin.claim_events("worker", lease_ns=100, now_ns=11)[0]
        with self.assertRaises(AuthorizationError):
            self.origin.ack_event(claim.event.event_id, "x" * 43, now_ns=12)
        self.assertEqual(self.origin.outbox_stats()["delivering"], 1)

    def test_expired_lease_cannot_ack(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        claim = self.origin.claim_events("worker", lease_ns=5, now_ns=11)[0]
        with self.assertRaises(LeaseExpiredError):
            self.origin.ack_event(claim.event.event_id, claim.token, now_ns=17)

    def test_lease_is_expired_at_exact_deadline(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        claim = self.origin.claim_events("worker", lease_ns=5, now_ns=11)[0]
        with self.assertRaises(LeaseExpiredError):
            self.origin.ack_event(claim.event.event_id, claim.token, now_ns=16)

    def test_failed_event_returns_to_pending(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        claim = self.origin.claim_events("worker", lease_ns=100, now_ns=11)[0]
        state = self.origin.fail_event(claim.event.event_id, claim.token, "network", now_ns=12)
        self.assertEqual(state, "PENDING")
        self.assertEqual(self.origin.outbox_stats()["pending"], 1)

    def test_retry_budget_moves_event_to_dead_letter(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        claim = self.origin.claim_events("worker", lease_ns=100, max_attempts=1, now_ns=11)[0]
        state = self.origin.fail_event(
            claim.event.event_id, claim.token, "permanent", max_attempts=1, now_ns=12
        )
        self.assertEqual(state, "DEAD")
        self.assertEqual(self.origin.outbox_stats()["dead"], 1)

    def test_repeated_worker_crash_exhausts_to_dead_letter(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        self.origin.claim_events("worker", lease_ns=5, max_attempts=1, now_ns=11)
        claims = self.origin.claim_events("worker", lease_ns=5, max_attempts=1, now_ns=16)
        self.assertEqual(claims, [])
        self.assertEqual(self.origin.outbox_stats()["dead"], 1)

    def test_expired_claim_is_reissued_with_new_capability(self) -> None:
        self.origin.write("acme", "x", 1, now_ns=10)
        first = self.origin.claim_events("worker-a", lease_ns=5, now_ns=11)[0]
        second = self.origin.claim_events("worker-b", lease_ns=5, now_ns=17)[0]
        self.assertNotEqual(first.token, second.token)
        self.assertEqual(second.attempt, 2)

    def test_database_identity_is_fenced(self) -> None:
        with self.assertRaises(IntegrityError):
            OriginStore(self.path, origin_id="different-origin")

    def test_restart_preserves_versions_and_outbox(self) -> None:
        self.origin.write("acme", "x", 1)
        restarted = OriginStore(self.path)
        self.assertEqual(restarted.peek_snapshot("acme", "x").version, 1)
        self.assertEqual(restarted.outbox_stats()["pending"], 1)

    def test_concurrent_writes_are_serialized(self) -> None:
        def write(number: int) -> int:
            return self.origin.write("acme", "hot", number).version

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            versions = list(executor.map(write, range(12)))
        self.assertEqual(sorted(versions), list(range(1, 13)))
        self.assertEqual(self.origin.peek_snapshot("acme", "hot").version, 12)

    def test_verify_covers_records_outbox_and_audit(self) -> None:
        self.origin.write("acme", "x", {"n": 1})
        result = self.origin.verify()
        self.assertTrue(result["ok"])
        self.assertEqual(result["records"], 1)
        self.assertEqual(result["outbox_events"], 1)

    def test_verify_detects_outbox_tampering(self) -> None:
        self.origin.write("acme", "x", 1)
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE outbox SET payload_json = '{}' ")
        connection.commit()
        connection.close()
        with self.assertRaises((IntegrityError, ValidationError)):
            self.origin.verify()


if __name__ == "__main__":
    unittest.main()
