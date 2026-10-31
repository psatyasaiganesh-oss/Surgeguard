from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from slicequeue.engine import deterministic_handler, execute_tool
from slicequeue.errors import (
    ConflictError,
    IntegrityError,
    LeaseError,
    NotFoundError,
    SimulatedCrash,
)
from slicequeue.store import SliceQueue


SECRET = b"unit-test-secret-with-at-least-32-bytes"


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "state.db"
        self.store = SliceQueue(self.path, secret=SECRET)
        self.store.launch_thread("test", "launch-root", thread_id="root", created_ns=1)

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def enqueue(self, event_id: str, thread_id: str = "root", *, created_ns: int = 10) -> dict:
        return self.store.enqueue_event(
            "test", event_id, thread_id, "USER", {"action": "ping"}, created_ns=created_ns
        )

    def process(self, *, current_ns: int = 100) -> dict:
        result = self.store.run_one(
            "test", "worker", deterministic_handler, current_ns=current_ns
        )
        self.assertIsNotNone(result)
        return result


class IdentityTests(StoreTestCase):
    def test_launch_is_idempotent_and_conflicting_reuse_is_rejected(self) -> None:
        replay = self.store.launch_thread(
            "test", "launch-root", thread_id="root", created_ns=2
        )
        self.assertTrue(replay["replayed"])
        with self.assertRaises(ConflictError):
            self.store.launch_thread("test", "launch-root", thread_id="other", created_ns=3)

    def test_parent_must_exist(self) -> None:
        with self.assertRaises(NotFoundError):
            self.store.launch_thread(
                "test", "missing-parent", thread_id="child", parent_thread_id="missing"
            )

    def test_event_is_idempotent_but_payload_change_conflicts(self) -> None:
        original = self.enqueue("e1")
        replay = self.enqueue("e1", created_ns=20)
        self.assertTrue(replay["replayed"])
        self.assertEqual(original["queue_sequence"], replay["queue_sequence"])
        with self.assertRaises(ConflictError):
            self.store.enqueue_event(
                "test", "e1", "root", "USER", {"action": "close"}, created_ns=30
            )

    def test_closed_thread_rejects_new_events(self) -> None:
        self.store.enqueue_event(
            "test", "close", "root", "USER", {"action": "close"}, created_ns=10
        )
        result = self.process()
        self.assertEqual(result["thread_status"], "CLOSED")
        with self.assertRaises(ConflictError):
            self.enqueue("too-late", created_ns=200)


class SchedulingTests(StoreTestCase):
    def test_fifo_within_thread_and_parallel_leases_across_threads(self) -> None:
        self.store.launch_thread("test", "launch-other", thread_id="other", created_ns=2)
        self.enqueue("root-1", created_ns=10)
        self.enqueue("root-2", created_ns=11)
        self.enqueue("other-1", "other", created_ns=12)
        first = self.store.claim_event("test", "w1", current_ns=20)
        second = self.store.claim_event("test", "w2", current_ns=20)
        self.assertEqual(first["event_id"], "root-1")
        self.assertEqual(second["event_id"], "other-1")
        self.assertIsNone(self.store.claim_event("test", "w3", current_ns=20))

    def test_unavailable_timer_does_not_consume_a_worker(self) -> None:
        self.store.enqueue_event(
            "test",
            "timer",
            "root",
            "TIMER",
            {"name": "later"},
            available_ns=100,
            created_ns=10,
        )
        self.assertIsNone(self.store.claim_event("test", "worker", current_ns=99))
        self.assertEqual(
            self.store.claim_event("test", "worker", current_ns=100)["event_id"], "timer"
        )

    def test_stale_fencing_token_cannot_commit_after_reclaim(self) -> None:
        self.enqueue("e1")
        old = self.store.claim_event("test", "old", lease_ms=1, current_ns=20)
        new = self.store.claim_event("test", "new", lease_ms=1, current_ns=1_000_021)
        self.assertEqual(new["attempt"], 2)
        with self.assertRaises(LeaseError):
            self.store.commit_slice(
                "test",
                "e1",
                "old",
                old["lease_token"],
                old["expected_thread_version"],
                {"assistant_message": "stale"},
                current_ns=1_000_022,
            )

    def test_heartbeat_extends_only_a_live_matching_lease(self) -> None:
        self.enqueue("e1")
        claim = self.store.claim_event("test", "worker", lease_ms=1, current_ns=20)
        heartbeat = self.store.heartbeat_event(
            "test", "e1", "worker", claim["lease_token"], lease_ms=5, current_ns=30
        )
        self.assertEqual(heartbeat["lease_until_ns"], 5_000_030)
        with self.assertRaises(LeaseError):
            self.store.heartbeat_event(
                "test", "e1", "attacker", claim["lease_token"], current_ns=31
            )

    def test_poison_event_becomes_dead_after_five_expired_leases(self) -> None:
        self.enqueue("poison")
        current = 100
        for attempt in range(1, 6):
            claim = self.store.claim_event("test", f"w{attempt}", lease_ms=1, current_ns=current)
            self.assertEqual(claim["attempt"], attempt)
            current += 1_000_001
        self.assertIsNone(self.store.claim_event("test", "last", current_ns=current))
        self.assertEqual(self.store.stats("test")["event_status"], {"DEAD": 1})

    def test_multiple_workers_process_distinct_threads_exactly_once(self) -> None:
        for index in range(20):
            thread_id = f"t-{index}"
            self.store.launch_thread(
                "test", f"launch-{index}", thread_id=thread_id, created_ns=10 + index
            )
            self.enqueue(f"e-{index}", thread_id, created_ns=100 + index)

        def worker(index: int) -> int:
            processed = 0
            while self.store.run_one(
                "test", f"worker-{index}", deterministic_handler, current_ns=1_000
            ):
                processed += 1
            return processed

        with ThreadPoolExecutor(max_workers=4) as pool:
            total = sum(pool.map(worker, range(4)))
        self.assertEqual(total, 20)
        self.assertEqual(self.store.stats("test")["event_status"], {"PROCESSED": 20})


class TransactionTests(StoreTestCase):
    def test_slice_lost_ack_is_durable_and_not_reapplied(self) -> None:
        self.enqueue("e1")
        claim = self.store.claim_event("test", "worker", current_ns=20)
        with self.assertRaises(SimulatedCrash):
            self.store.commit_slice(
                "test",
                "e1",
                "worker",
                claim["lease_token"],
                0,
                {"assistant_message": "committed"},
                current_ns=21,
                failpoint="after_slice_commit",
            )
        self.assertEqual(self.store.get_thread("test", "root")["version"], 1)
        self.assertIsNone(self.store.claim_event("test", "worker", current_ns=22))
        with self.assertRaises(LeaseError):
            self.store.commit_slice(
                "test", "e1", "worker", claim["lease_token"], 0, {"assistant_message": "again"}
            )

    def test_tool_outbox_and_completion_survive_lost_acks(self) -> None:
        self.store.enqueue_event(
            "test", "sum", "root", "USER", {"action": "sum", "numbers": [2, 3]}, created_ns=10
        )
        slice_result = self.process(current_ns=20)
        self.assertIsNotNone(slice_result["call_id"])
        with self.assertRaises(SimulatedCrash):
            self.store.dispatch_outbox(
                "test", current_ns=30, failpoint="after_dispatch_commit"
            )
        self.assertEqual(self.store.dispatch_outbox("test", current_ns=31)["dispatched"], 0)
        task = self.store.claim_tool("test", "tool-worker", current_ns=40)
        result = execute_tool(task["tool_name"], task["arguments"])
        with self.assertRaises(SimulatedCrash):
            self.store.complete_tool(
                "test",
                task["call_id"],
                "tool-worker",
                task["lease_token"],
                result,
                current_ns=41,
                failpoint="after_tool_commit",
            )
        replay = self.store.complete_tool(
            "test", task["call_id"], "tool-worker", task["lease_token"], result, current_ns=42
        )
        self.assertTrue(replay["replayed"])
        with self.assertRaises(ConflictError):
            self.store.complete_tool(
                "test",
                task["call_id"],
                "tool-worker",
                task["lease_token"],
                {"ok": True, "sum": 99},
                current_ns=43,
            )
        self.assertEqual(self.process(current_ns=44)["thread_version"], 2)

    def test_send_is_published_once_to_target_thread(self) -> None:
        self.store.launch_thread("test", "launch-child", thread_id="child", created_ns=2)
        self.store.enqueue_event(
            "test",
            "send",
            "root",
            "USER",
            {"action": "send", "target_thread_id": "child", "payload": {"answer": 42}},
            created_ns=10,
        )
        self.process(current_ns=20)
        self.assertEqual(self.store.dispatch_outbox("test", current_ns=21)["dispatched"], 1)
        self.assertEqual(self.store.dispatch_outbox("test", current_ns=22)["dispatched"], 0)
        result = self.process(current_ns=23)
        self.assertEqual(result["thread_id"], "child")
        self.assertEqual(self.store.get_thread("test", "child")["version"], 1)


class IntegrityTests(StoreTestCase):
    def test_valid_database_reports_linked_counts(self) -> None:
        self.enqueue("e1")
        self.process()
        result = self.store.verify_integrity("test")
        self.assertEqual(
            result,
            {
                "valid": True,
                "namespaces": 1,
                "threads": 1,
                "events": 1,
                "history_entries": 2,
                "tool_calls": 0,
                "tool_tasks": 0,
                "audit_entries": 3,
            },
        )

    def test_history_tampering_is_detected(self) -> None:
        self.enqueue("e1")
        self.process()
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE history SET content_json = ? WHERE history_id = 1", ('{"changed":true}',))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(IntegrityError, "hash chain"):
            self.store.verify_integrity("test")

    def test_audit_tampering_is_detected(self) -> None:
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE audit_log SET entity_id = 'changed' WHERE sequence = 1")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(IntegrityError, "audit entry hash"):
            self.store.verify_integrity("test")


if __name__ == "__main__":
    unittest.main()
