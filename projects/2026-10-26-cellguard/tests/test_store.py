from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from cellguard.errors import (
    CapacityError,
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    LeaseError,
    NotFoundError,
    UnavailableError,
    ValidationError,
)
from cellguard.model import Cell
from cellguard.store import CellGuardStore
from cellguard.topology import ShuffleTopology


SECRET = b"test-cellguard-secret-with-at-least-32-bytes"


def make_topology(capacity: int = 20) -> ShuffleTopology:
    return ShuffleTopology(
        [Cell(f"cell-{index}", f"zone-{index % 2}", capacity) for index in range(4)],
        shard_size=2,
    )


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "queue.sqlite"
        self.store = CellGuardStore(
            self.path,
            make_topology(),
            secret=SECRET,
            default_queue_limit=4,
            default_inflight_limit=2,
            default_max_attempts=2,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_assignment_is_persisted_and_verified(self) -> None:
        assignment = self.store.assignment("tenant-a")
        self.assertEqual(assignment, self.store.assignment("tenant-a"))
        self.assertEqual(self.store.verify_integrity()["assignments"], 1)

    def test_database_rejects_wrong_secret_or_topology(self) -> None:
        self.store.assignment("tenant-a")
        with self.assertRaises(IntegrityError):
            CellGuardStore(self.path, make_topology(), secret=b"a-different-secret-that-is-long-enough")
        different = ShuffleTopology(
            [Cell(f"other-{i}", f"zone-{i % 2}") for i in range(4)], shard_size=2
        )
        with self.assertRaises(IntegrityError):
            CellGuardStore(self.path, different, secret=SECRET)

    def test_submit_returns_job_inside_assignment(self) -> None:
        result = self.store.submit("tenant-a", "request-1", {"task": "resize"}, now_ns=1)
        self.assertIn(result.cell_id, result.assignment.cells)
        self.assertEqual(self.store.job("tenant-a", result.job_id)["state"], "QUEUED")

    def test_idempotency_replays_identical_request(self) -> None:
        first = self.store.submit("tenant-a", "request-1", {"x": 1}, now_ns=1)
        second = self.store.submit("tenant-a", "request-1", {"x": 1}, now_ns=2)
        self.assertEqual(first.job_id, second.job_id)
        self.assertTrue(second.replayed)

    def test_concurrent_idempotent_submissions_create_one_job(self) -> None:
        def submit(_: int):
            return self.store.submit("tenant-a", "same-request", {"x": 1})

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(submit, range(8)))
        self.assertEqual(len({result.job_id for result in results}), 1)
        self.assertEqual(sum(result.replayed for result in results), 7)

    def test_idempotency_rejects_changed_payload_or_priority(self) -> None:
        self.store.submit("tenant-a", "request-1", {"x": 1}, now_ns=1)
        for payload, priority in (({"x": 2}, 0), ({"x": 1}, 2)):
            with self.subTest(payload=payload, priority=priority), self.assertRaises(
                IdempotencyConflict
            ):
                self.store.submit("tenant-a", "request-1", payload, priority=priority, now_ns=2)

    def test_tenant_queue_limit_isolated_from_other_tenant(self) -> None:
        for index in range(4):
            self.store.submit("noisy", f"n-{index}", {"i": index}, now_ns=index)
        with self.assertRaises(CapacityError):
            self.store.submit("noisy", "overflow", {}, now_ns=10)
        self.assertEqual(self.store.submit("normal", "ok", {}, now_ns=11).state, "QUEUED")

    def test_disabled_tenant_cannot_submit(self) -> None:
        self.store.set_policy(
            "tenant-a", queue_limit=4, inflight_limit=2, enabled=False, expected_version=0
        )
        with self.assertRaises(CapacityError):
            self.store.submit("tenant-a", "request", {})

    def test_cell_capacity_is_enforced(self) -> None:
        tiny = CellGuardStore(
            Path(self.temporary.name) / "tiny.sqlite",
            make_topology(capacity=1),
            secret=SECRET,
            default_queue_limit=20,
        )
        tenant = "one-shard"
        first = tiny.submit(tenant, "a", {})
        second = tiny.submit(tenant, "b", {})
        self.assertNotEqual(first.cell_id, second.cell_id)
        with self.assertRaises(CapacityError):
            tiny.submit(tenant, "c", {})

    def test_unhealthy_assigned_cells_do_not_escape_shard(self) -> None:
        assigned = self.store.assignment("tenant-a")
        for index, cell_id in enumerate(assigned.cells):
            cell = self.store.cell(cell_id)
            self.store.set_cell_state(
                cell_id, "UNHEALTHY", expected_version=cell["version"], now_ns=10 + index
            )
        with self.assertRaises(UnavailableError):
            self.store.submit("tenant-a", "blocked", {}, now_ns=20)

    def test_cell_state_compare_and_set(self) -> None:
        cell = self.store.cell("cell-0")
        updated = self.store.set_cell_state(
            "cell-0", "DRAINING", expected_version=cell["version"], now_ns=1
        )
        self.assertEqual(updated["state"], "DRAINING")
        with self.assertRaises(ConflictError):
            self.store.set_cell_state("cell-0", "HEALTHY", expected_version=cell["version"])

    def test_policy_compare_and_set_and_limits(self) -> None:
        policy = self.store.set_policy(
            "tenant-a", queue_limit=5, inflight_limit=1, weight=2, expected_version=0
        )
        self.assertEqual(policy["version"], 1)
        self.assertEqual(policy["weight"], 2)
        with self.assertRaises(ConflictError):
            self.store.set_policy(
                "tenant-a", queue_limit=5, inflight_limit=1, expected_version=0
            )

    def test_policy_cannot_shrink_below_outstanding(self) -> None:
        self.store.submit("tenant-a", "one", {})
        self.store.submit("tenant-a", "two", {})
        with self.assertRaises(ConflictError):
            self.store.set_policy("tenant-a", queue_limit=1, inflight_limit=1)

    def test_claim_prefers_priority_within_tenant(self) -> None:
        low = self.store.submit("tenant-a", "low", {}, priority=1, now_ns=1)
        high = self.store.submit("tenant-a", "high", {}, priority=9, now_ns=2)
        cell = low.cell_id if low.cell_id == high.cell_id else high.cell_id
        claim = self.store.claim(cell, "worker", limit=1, now_ns=10)[0]
        expected = high.job_id if high.cell_id == cell else low.job_id
        self.assertEqual(claim.job_id, expected)

    def test_claim_round_robins_tenants(self) -> None:
        # Force two tenants sharing an assignment and put work on the same least-loaded cell.
        topology = make_topology()
        left = "tenant-left"
        target = topology.assign(left).cells
        right = next(
            f"tenant-{i}" for i in range(10_000) if topology.assign(f"tenant-{i}").cells == target
        )
        for index in range(4):
            self.store.submit(left, f"left-{index}", {}, now_ns=index)
            self.store.submit(right, f"right-{index}", {}, now_ns=index)
        cell = target[0]
        claims = self.store.claim(cell, "worker", limit=4, now_ns=20)
        self.assertEqual({claim.tenant for claim in claims}, {left, right})

    def test_concurrent_claimers_never_receive_same_job(self) -> None:
        self.store.set_policy(
            "tenant-a", queue_limit=20, inflight_limit=20, expected_version=0
        )
        submitted = [self.store.submit("tenant-a", f"job-{i}", {}) for i in range(8)]
        target = submitted[0].cell_id

        def claim(worker: str):
            return self.store.claim(target, worker, limit=10)

        with ThreadPoolExecutor(max_workers=2) as executor:
            batches = list(executor.map(claim, ["worker-a", "worker-b"]))
        claimed = [item.job_id for batch in batches for item in batch]
        expected = {item.job_id for item in submitted if item.cell_id == target}
        self.assertEqual(set(claimed), expected)
        self.assertEqual(len(claimed), len(set(claimed)))

    def test_global_tenant_inflight_limit(self) -> None:
        self.store.set_policy("tenant-a", queue_limit=4, inflight_limit=1, expected_version=0)
        results = [self.store.submit("tenant-a", str(i), {}, now_ns=i) for i in range(2)]
        first = self.store.claim(results[0].cell_id, "worker-a", now_ns=10)
        self.assertEqual(len(first), 1)
        other = next(result.cell_id for result in results if result.cell_id != results[0].cell_id)
        self.assertEqual(self.store.claim(other, "worker-b", now_ns=10), [])

    def test_unhealthy_cell_cannot_claim_but_draining_can(self) -> None:
        job = self.store.submit("tenant-a", "job", {})
        cell = self.store.cell(job.cell_id)
        self.store.set_cell_state(job.cell_id, "DRAINING", expected_version=cell["version"])
        self.assertEqual(len(self.store.claim(job.cell_id, "worker")), 1)
        current = self.store.cell(job.cell_id)
        self.store.set_cell_state(job.cell_id, "UNHEALTHY", expected_version=current["version"])
        with self.assertRaises(UnavailableError):
            self.store.claim(job.cell_id, "worker")

    def test_complete_is_idempotent_for_same_capability_and_result(self) -> None:
        job = self.store.submit("tenant-a", "job", {}, now_ns=1)
        claim = self.store.claim(job.cell_id, "worker", lease_ns=1_000_000, now_ns=2)[0]
        first = self.store.complete(job.job_id, claim.lease_token, {"ok": True}, now_ns=3)
        second = self.store.complete(job.job_id, claim.lease_token, {"ok": True}, now_ns=4)
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])

    def test_completion_rejects_bad_token_changed_result_and_expiry(self) -> None:
        job = self.store.submit("tenant-a", "job", {}, now_ns=1)
        claim = self.store.claim(job.cell_id, "worker", lease_ns=1_000_000, now_ns=2)[0]
        with self.assertRaises(LeaseError):
            self.store.complete(job.job_id, "x" * 40, {}, now_ns=3)
        self.store.complete(job.job_id, claim.lease_token, {"ok": True}, now_ns=3)
        with self.assertRaises(ConflictError):
            self.store.complete(job.job_id, claim.lease_token, {"ok": False}, now_ns=4)
        late = self.store.submit("tenant-a", "late", {}, now_ns=5)
        late_claim = self.store.claim(late.cell_id, "worker", lease_ns=1_000_000, now_ns=6)[0]
        with self.assertRaises(LeaseError):
            self.store.complete(late.job_id, late_claim.lease_token, {}, now_ns=1_000_006)

    def test_expired_lease_requeues_with_higher_fencing_token(self) -> None:
        job = self.store.submit("tenant-a", "job", {}, now_ns=1)
        old = self.store.claim(job.cell_id, "old", lease_ns=1_000_000, now_ns=2)[0]
        self.assertEqual(self.store.reap_expired(now_ns=1_000_002)["requeued"], 1)
        new = self.store.claim(job.cell_id, "new", lease_ns=1_000_000, now_ns=1_000_003)[0]
        self.assertGreater(new.fencing_token, old.fencing_token)
        with self.assertRaises(LeaseError):
            self.store.complete(job.job_id, old.lease_token, {}, now_ns=1_000_004)

    def test_fail_retries_then_dead_letters(self) -> None:
        job = self.store.submit("tenant-a", "job", {}, now_ns=1)
        first = self.store.claim(job.cell_id, "worker", lease_ns=10_000_000, now_ns=2)[0]
        self.assertEqual(
            self.store.fail(job.job_id, first.lease_token, "transient", now_ns=3)["state"],
            "QUEUED",
        )
        second = self.store.claim(job.cell_id, "worker", lease_ns=10_000_000, now_ns=4)[0]
        self.assertEqual(
            self.store.fail(job.job_id, second.lease_token, "again", now_ns=5)["state"],
            "DEAD",
        )

    def test_retry_delay_defers_claim(self) -> None:
        job = self.store.submit("tenant-a", "job", {}, now_ns=1)
        claim = self.store.claim(job.cell_id, "worker", lease_ns=10_000_000, now_ns=2)[0]
        self.store.fail(job.job_id, claim.lease_token, "later", retry_delay_ns=100, now_ns=3)
        self.assertEqual(self.store.claim(job.cell_id, "worker", now_ns=102), [])
        self.assertEqual(len(self.store.claim(job.cell_id, "worker", now_ns=103)), 1)

    def test_job_lookup_is_tenant_scoped(self) -> None:
        job = self.store.submit("tenant-a", "job", {})
        with self.assertRaises(NotFoundError):
            self.store.job("tenant-b", job.job_id)

    def test_integrity_detects_payload_tampering(self) -> None:
        job = self.store.submit("tenant-a", "job", {"safe": True})
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE jobs SET payload_json = '{}' WHERE job_id = ?", (job.job_id,))
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_integrity_detects_audit_tampering(self) -> None:
        self.store.submit("tenant-a", "job", {})
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE audit_log SET action = 'ALTERED' WHERE sequence = 1")
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_stats_include_state_and_metrics(self) -> None:
        self.store.submit("tenant-a", "job", {})
        stats = self.store.stats()
        self.assertEqual(stats["jobs"]["queued"], 1)
        self.assertEqual(stats["metrics"]["submitted"], 1)

    def test_invalid_inputs_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.submit("bad tenant", "job", {})
        with self.assertRaises(ValidationError):
            self.store.claim("cell-0", "worker", limit=0)
        with self.assertRaises(ValidationError):
            self.store.set_cell_state("cell-0", "BROKEN", expected_version=1)


if __name__ == "__main__":
    unittest.main()
