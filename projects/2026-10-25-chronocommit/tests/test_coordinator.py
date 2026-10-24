from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from chronocommit.clock import ManualBoundedClock
from chronocommit.coordinator import Coordinator
from chronocommit.errors import (
    ClockUnsafeError,
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    SimulatedCrash,
    TransactionAborted,
    ValidationError,
)
from chronocommit.model import Mutation


SECRET = b"coordinator-test-decision-secret-32-bytes"


class CoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.clock = ManualBoundedClock(1_000_000, 10)
        self.coordinator = Coordinator(
            self.directory, shard_count=3, secret=SECRET, clock=self.clock
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def key_on_shard(self, index: int, prefix: str = "key") -> str:
        for number in range(10_000):
            key = f"{prefix}-{number}"
            if self.coordinator.shard_index("tenant", key) == index:
                return key
        self.fail("no key found")

    def create(self, key: str, value: object, identifier: str) -> object:
        return self.coordinator.execute(
            "tenant", identifier, [Mutation(key, value, expected_ts=0)]
        )

    def test_single_participant_commit(self) -> None:
        result = self.create("alpha", 1, "create-alpha")
        self.assertEqual(result.participant_count, 1)
        self.assertEqual(self.coordinator.snapshot("tenant", ["alpha"])["alpha"].value, 1)

    def test_cross_shard_commit_is_atomic(self) -> None:
        first = self.key_on_shard(0, "first")
        second = self.key_on_shard(1, "second")
        result = self.coordinator.execute(
            "tenant",
            "cross-shard",
            [Mutation(first, 1, 0), Mutation(second, 2, 0)],
        )
        self.assertEqual(result.participant_count, 2)
        values = self.coordinator.snapshot("tenant", [first, second])
        self.assertEqual((values[first].value, values[second].value), (1, 2))

    def test_exact_snapshot_preserves_old_version(self) -> None:
        first = self.create("alpha", 1, "create-alpha")
        second = self.coordinator.execute(
            "tenant", "update-alpha", [Mutation("alpha", 2, first.commit_ts)]
        )
        self.assertEqual(
            self.coordinator.snapshot("tenant", ["alpha"], read_ts=first.commit_ts)[
                "alpha"
            ].value,
            1,
        )
        self.assertEqual(
            self.coordinator.snapshot("tenant", ["alpha"], read_ts=second.commit_ts)[
                "alpha"
            ].value,
            2,
        )

    def test_idempotent_replay_returns_same_transaction(self) -> None:
        first = self.create("alpha", 1, "same-request")
        replay = self.create("alpha", 1, "same-request")
        self.assertEqual(first.txn_id, replay.txn_id)
        self.assertTrue(replay.replayed)
        self.assertEqual(self.coordinator.shard_for("tenant", "alpha").version_count(), 1)

    def test_idempotency_key_rejects_changed_payload(self) -> None:
        self.create("alpha", 1, "same-request")
        with self.assertRaises(IdempotencyConflict):
            self.create("beta", 2, "same-request")

    def test_request_identity_replays_state_dependent_writes(self) -> None:
        first = self.coordinator.execute(
            "tenant",
            "logical-request",
            [Mutation("alpha", 1, 0)],
            request_identity={"operation": "set-once"},
        )
        replay = self.coordinator.execute(
            "tenant",
            "logical-request",
            [Mutation("alpha", 999, first.commit_ts)],
            request_identity={"operation": "set-once"},
        )
        self.assertEqual(first.txn_id, replay.txn_id)
        self.assertEqual(self.coordinator.snapshot("tenant", ["alpha"])["alpha"].value, 1)

    def test_request_identity_detects_changed_business_request(self) -> None:
        self.coordinator.execute(
            "tenant",
            "logical-request",
            [Mutation("alpha", 1, 0)],
            request_identity={"amount": 1},
        )
        with self.assertRaises(IdempotencyConflict):
            self.coordinator.replay_if_exists(
                "tenant", "logical-request", {"amount": 2}
            )

    def test_prepare_conflict_is_terminal_for_request_key(self) -> None:
        created = self.create("alpha", 1, "create-alpha")
        stale = Mutation("alpha", 3, created.commit_ts)
        self.coordinator.execute(
            "tenant", "winner", [Mutation("alpha", 2, created.commit_ts)]
        )
        with self.assertRaises(ConflictError):
            self.coordinator.execute("tenant", "loser", [stale])
        with self.assertRaises(TransactionAborted):
            self.coordinator.execute("tenant", "loser", [stale])

    def test_crash_during_prepare_recovers_by_abort(self) -> None:
        first = self.key_on_shard(0, "a")
        second = self.key_on_shard(1, "b")
        with self.assertRaises(SimulatedCrash):
            self.coordinator.execute(
                "tenant",
                "prepare-crash",
                [Mutation(first, 1, 0), Mutation(second, 2, 0)],
                fault="after_prepare:1",
            )
        report = self.coordinator.recover()
        self.assertEqual(report["aborted"], 1)
        values = self.coordinator.snapshot("tenant", [first, second])
        self.assertTrue(values[first].deleted)
        self.assertTrue(values[second].deleted)

    def test_crash_after_decision_finishes_on_recovery(self) -> None:
        first = self.key_on_shard(0, "a")
        second = self.key_on_shard(1, "b")
        with self.assertRaises(SimulatedCrash):
            self.coordinator.execute(
                "tenant",
                "decision-crash",
                [Mutation(first, 1, 0), Mutation(second, 2, 0)],
                fault="after_decision",
            )
        report = self.coordinator.recover()
        self.assertEqual(report["committed"], 1)
        values = self.coordinator.snapshot("tenant", [first, second])
        self.assertEqual((values[first].value, values[second].value), (1, 2))

    def test_partial_participant_commit_is_idempotently_recovered(self) -> None:
        first = self.key_on_shard(0, "a")
        second = self.key_on_shard(1, "b")
        with self.assertRaises(SimulatedCrash):
            self.coordinator.execute(
                "tenant",
                "partial-commit",
                [Mutation(first, 1, 0), Mutation(second, 2, 0)],
                fault="after_commit:1",
            )
        replacement = Coordinator(
            self.directory, shard_count=3, secret=SECRET, clock=self.clock
        )
        values = replacement.snapshot("tenant", [first, second])
        self.assertEqual((values[first].value, values[second].value), (1, 2))
        self.assertEqual(sum(shard.version_count() for shard in replacement.shards), 2)

    def test_crash_before_ack_repeats_commit_wait(self) -> None:
        with self.assertRaises(SimulatedCrash):
            self.coordinator.execute(
                "tenant",
                "ack-crash",
                [Mutation("alpha", 1, 0)],
                fault="after_commit_record",
            )
        before = self.clock.wait_count
        report = self.coordinator.recover()
        self.assertEqual(report["acknowledged"], 1)
        self.assertEqual(self.clock.wait_count, before + 1)

    def test_acknowledgement_is_after_commit_timestamp(self) -> None:
        result = self.create("alpha", 1, "create-alpha")
        self.assertGreater(result.acknowledged_ns, result.commit_ts)

    def test_nonoverlapping_transactions_follow_real_time_order(self) -> None:
        first = self.create("alpha", 1, "first")
        second = self.create("beta", 2, "second")
        self.assertLess(first.commit_ts, second.commit_ts)

    def test_clock_regression_fails_closed(self) -> None:
        self.create("alpha", 1, "first")
        self.clock.set_center(100)
        with self.assertRaises(ClockUnsafeError):
            self.create("beta", 2, "second")

    def test_delete_is_visible_as_tombstone(self) -> None:
        created = self.create("alpha", 1, "create-alpha")
        self.coordinator.execute(
            "tenant", "delete-alpha", [Mutation("alpha", None, created.commit_ts, True)]
        )
        self.assertTrue(self.coordinator.snapshot("tenant", ["alpha"])["alpha"].deleted)

    def test_tenants_are_isolated_on_same_key(self) -> None:
        self.create("alpha", 1, "tenant-one")
        self.coordinator.execute(
            "other", "tenant-two", [Mutation("alpha", 2, 0)]
        )
        self.assertEqual(self.coordinator.snapshot("tenant", ["alpha"])["alpha"].value, 1)
        self.assertEqual(self.coordinator.snapshot("other", ["alpha"])["alpha"].value, 2)

    def test_concurrent_stale_writers_cannot_both_commit(self) -> None:
        created = self.create("alpha", 0, "create-alpha")
        barrier = threading.Barrier(3)
        outcomes: list[str] = []
        guard = threading.Lock()

        def writer(identifier: str, value: int) -> None:
            barrier.wait()
            try:
                self.coordinator.execute(
                    "tenant", identifier, [Mutation("alpha", value, created.commit_ts)]
                )
                outcome = "committed"
            except ConflictError:
                outcome = "conflict"
            with guard:
                outcomes.append(outcome)

        threads = [
            threading.Thread(target=writer, args=("writer-a", 1)),
            threading.Thread(target=writer, args=("writer-b", 2)),
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(sorted(outcomes), ["committed", "conflict"])

    def test_transaction_status_and_missing_status(self) -> None:
        result = self.create("alpha", 1, "create-alpha")
        status = self.coordinator.transaction_status("tenant", "create-alpha")
        assert status is not None
        self.assertEqual(status["txn_id"], result.txn_id)
        self.assertEqual(status["state"], "COMMITTED")
        self.assertIsNone(self.coordinator.transaction_status("tenant", "missing"))

    def test_integrity_report_includes_all_shards(self) -> None:
        self.create("alpha", 1, "create-alpha")
        report = self.coordinator.verify_integrity()
        self.assertEqual(len(report["shards"]), 3)
        self.assertEqual(report["transactions"], 1)

    def test_coordinator_audit_tampering_is_detected(self) -> None:
        self.create("alpha", 1, "create-alpha")
        connection = sqlite3.connect(self.coordinator.db_path)
        connection.execute("UPDATE audit_log SET detail_json = '{}' WHERE sequence = 1")
        connection.commit()
        connection.close()
        with self.assertRaises(IntegrityError):
            self.coordinator.verify_integrity()

    def test_configuration_mismatch_is_rejected(self) -> None:
        with self.assertRaises(IntegrityError):
            Coordinator(self.directory, shard_count=2, secret=SECRET, clock=self.clock)
        with self.assertRaises(IntegrityError):
            Coordinator(
                self.directory,
                shard_count=3,
                secret=b"wrong-coordinator-secret-at-least-32",
                clock=self.clock,
            )

    def test_snapshot_validation_and_fault_validation(self) -> None:
        with self.assertRaises(ValidationError):
            self.coordinator.snapshot("tenant", [])
        with self.assertRaises(ValidationError):
            self.coordinator.execute(
                "tenant", "bad-fault", [Mutation("alpha", 1, 0)], fault="unknown"
            )

    def test_shard_assignment_is_stable(self) -> None:
        first = self.coordinator.shard_index("tenant", "alpha")
        replacement = Coordinator(
            self.directory, shard_count=3, secret=SECRET, clock=self.clock
        )
        self.assertEqual(first, replacement.shard_index("tenant", "alpha"))


if __name__ == "__main__":
    unittest.main()

