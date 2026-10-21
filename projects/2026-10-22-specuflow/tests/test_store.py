import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from specuflow.blob import IntegrityError
from specuflow.model import canonical
from specuflow.store import (
    AuthorizationError,
    ConflictError,
    LeaseExpiredError,
    NondeterminismError,
    NotFoundError,
    Scheduler,
)


def job(job_id="job", shards=1, max_attempts=3):
    return {
        "job_id": job_id,
        "shards": [
            {"shard_id": f"shard-{index}", "payload": {"number": index}}
            for index in range(shards)
        ],
        "policy": {
            "lease_ms": 100,
            "speculate_after_ms": 40,
            "max_attempts": max_attempts,
            "max_speculative_attempts": 1,
        },
    }


def output(shard_id, value=1):
    return canonical({"shard_id": shard_id, "value": value}).encode()


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.database = root / "scheduler.db"
        self.blobs = root / "blobs"
        self.scheduler = Scheduler(self.database, self.blobs)

    def tearDown(self):
        self.temp.cleanup()

    def test_submit_creates_pending_tasks(self):
        status = self.scheduler.submit(job(), now_ms=10)
        self.assertEqual(status["state"], "RUNNING")
        self.assertEqual(status["task_states"], {"PENDING": 1})

    def test_submit_retry_is_idempotent(self):
        first = self.scheduler.submit(job(), now_ms=10)
        second = self.scheduler.submit(job(), now_ms=20)
        self.assertEqual(first["spec_hash"], second["spec_hash"])
        self.assertEqual(len(self.scheduler.audit_log("job")), 1)

    def test_submit_conflict_is_rejected(self):
        self.scheduler.submit(job())
        changed = job()
        changed["shards"][0]["payload"]["number"] = 99
        with self.assertRaises(ConflictError):
            self.scheduler.submit(changed)

    def test_unknown_job_is_rejected(self):
        with self.assertRaises(NotFoundError):
            self.scheduler.status("missing")

    def test_first_claim_is_primary(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        self.assertEqual(claim["kind"], "PRIMARY")
        self.assertNotIn("token_hash", claim)

    def test_speculation_waits_for_threshold(self):
        self.scheduler.submit(job(), now_ms=0)
        self.scheduler.claim("job", "worker-a", now_ms=0)
        self.assertIsNone(self.scheduler.claim("job", "worker-b", now_ms=39))
        backup = self.scheduler.claim("job", "worker-b", now_ms=40)
        self.assertEqual(backup["kind"], "SPECULATIVE")

    def test_same_worker_cannot_claim_backup(self):
        self.scheduler.submit(job(), now_ms=0)
        self.scheduler.claim("job", "worker-a", now_ms=0)
        self.assertIsNone(self.scheduler.claim("job", "worker-a", now_ms=50))

    def test_speculation_cap_is_enforced(self):
        self.scheduler.submit(job(), now_ms=0)
        self.scheduler.claim("job", "worker-a", now_ms=0)
        self.scheduler.claim("job", "worker-b", now_ms=50)
        self.assertIsNone(self.scheduler.claim("job", "worker-c", now_ms=60))

    def test_heartbeat_extends_lease(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        deadline = self.scheduler.heartbeat(
            claim["attempt_id"], claim["capability_token"], now_ms=50
        )
        self.assertEqual(deadline, 150)

    def test_bad_capability_is_rejected(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        with self.assertRaises(AuthorizationError):
            self.scheduler.heartbeat(claim["attempt_id"], "wrong", now_ms=1)

    def test_completion_after_expiry_is_rejected(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        with self.assertRaises(LeaseExpiredError):
            self.scheduler.complete(
                claim["attempt_id"], claim["capability_token"],
                output("shard-0"), {"records": 1}, now_ms=100
            )

    def test_expired_attempt_is_retried(self):
        self.scheduler.submit(job(), now_ms=0)
        first = self.scheduler.claim("job", "worker-a", now_ms=0)
        retry = self.scheduler.claim("job", "worker-b", now_ms=101)
        self.assertEqual(retry["kind"], "RETRY")
        attempts = self.scheduler.attempts("job")
        self.assertEqual(attempts[0]["state"], "EXPIRED")
        self.assertNotEqual(first["attempt_id"], retry["attempt_id"])

    def test_first_valid_result_commits(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        result = self.scheduler.complete(
            claim["attempt_id"], claim["capability_token"],
            output("shard-0"), {"records": 1}, now_ms=10
        )
        self.assertTrue(result["committed"])
        status = self.scheduler.status("job")
        self.assertEqual(status["state"], "SUCCEEDED")
        self.assertIsNotNone(status["manifest_digest"])

    def test_equal_loser_result_is_suppressed(self):
        self.scheduler.submit(job(), now_ms=0)
        primary = self.scheduler.claim("job", "worker-a", now_ms=0)
        backup = self.scheduler.claim("job", "worker-b", now_ms=50)
        self.scheduler.complete(
            backup["attempt_id"], backup["capability_token"],
            output("shard-0", 7), {"records": 1}, now_ms=60
        )
        duplicate = self.scheduler.complete(
            primary["attempt_id"], primary["capability_token"],
            output("shard-0", 7), {"records": 999}, now_ms=70
        )
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(self.scheduler.status("job")["counters"], {"records": 1})

    def test_different_loser_result_fails_job(self):
        self.scheduler.submit(job(), now_ms=0)
        primary = self.scheduler.claim("job", "worker-a", now_ms=0)
        backup = self.scheduler.claim("job", "worker-b", now_ms=50)
        self.scheduler.complete(
            backup["attempt_id"], backup["capability_token"],
            output("shard-0", 7), {"records": 1}, now_ms=60
        )
        with self.assertRaises(NondeterminismError):
            self.scheduler.complete(
                primary["attempt_id"], primary["capability_token"],
                output("shard-0", 8), {"records": 1}, now_ms=70
            )
        status = self.scheduler.status("job")
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(status["error_code"], "NONDETERMINISTIC_OUTPUT")

    def test_output_identity_is_enforced(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.scheduler.complete(
                claim["attempt_id"], claim["capability_token"],
                output("other"), {"records": 1}, now_ms=10
            )

    def test_output_can_be_read_and_verified(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        raw = output("shard-0", {"answer": 42})
        self.scheduler.complete(
            claim["attempt_id"], claim["capability_token"], raw, {}, now_ms=10
        )
        self.assertEqual(self.scheduler.output("job", "shard-0"), raw)
        self.assertEqual(self.scheduler.verify("job"), {"ok": True, "checked": 1, "failed_shards": []})

    def test_corrupt_output_is_reported(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        result = self.scheduler.complete(
            claim["attempt_id"], claim["capability_token"],
            output("shard-0"), {}, now_ms=10
        )
        self.scheduler.blobs._path(result["output_digest"]).write_bytes(b"corrupt")
        self.assertEqual(self.scheduler.verify("job")["failed_shards"], ["shard-0"])
        with self.assertRaises(IntegrityError):
            self.scheduler.output("job", "shard-0")

    def test_failed_attempt_can_retry(self):
        self.scheduler.submit(job(), now_ms=0)
        first = self.scheduler.claim("job", "worker-a", now_ms=0)
        self.scheduler.fail_attempt(
            first["attempt_id"], first["capability_token"], "PROCESS_EXIT", now_ms=5
        )
        retry = self.scheduler.claim("job", "worker-b", now_ms=6)
        self.assertEqual(retry["kind"], "RETRY")

    def test_attempt_exhaustion_fails_job(self):
        self.scheduler.submit(job(max_attempts=1), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        self.scheduler.fail_attempt(
            claim["attempt_id"], claim["capability_token"], "PROCESS_EXIT", now_ms=1
        )
        self.assertIsNone(self.scheduler.claim("job", "worker-b", now_ms=2))
        self.assertEqual(self.scheduler.status("job")["error_code"], "ATTEMPTS_EXHAUSTED")

    def test_restart_preserves_state(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        restarted = Scheduler(self.database, self.blobs)
        restarted.complete(
            claim["attempt_id"], claim["capability_token"],
            output("shard-0"), {"records": 1}, now_ms=10
        )
        self.assertEqual(restarted.status("job")["state"], "SUCCEEDED")

    def test_concurrent_claims_have_one_winner(self):
        self.scheduler.submit(job(), now_ms=0)
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(
                lambda index: self.scheduler.claim(
                    "job", f"worker-{index}", allow_speculation=False, now_ms=0
                ),
                range(8),
            ))
        self.assertEqual(sum(item is not None for item in claims), 1)

    def test_counters_aggregate_only_committed_tasks(self):
        self.scheduler.submit(job(shards=2), now_ms=0)
        first = self.scheduler.claim("job", "worker-a", now_ms=0)
        self.scheduler.complete(
            first["attempt_id"], first["capability_token"],
            output("shard-0"), {"records": 2}, now_ms=10
        )
        second = self.scheduler.claim("job", "worker-b", now_ms=11)
        self.scheduler.complete(
            second["attempt_id"], second["capability_token"],
            output("shard-1"), {"records": 3}, now_ms=20
        )
        self.assertEqual(self.scheduler.status("job")["counters"], {"records": 5})

    def test_attempt_listing_redacts_capability(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        encoded = json.dumps(self.scheduler.attempts("job"))
        self.assertNotIn(claim["capability_token"], encoded)
        self.assertNotIn("token_hash", encoded)

    def test_audit_redacts_capability(self):
        self.scheduler.submit(job(), now_ms=0)
        claim = self.scheduler.claim("job", "worker-a", now_ms=0)
        self.assertNotIn(claim["capability_token"], json.dumps(self.scheduler.audit_log("job")))


if __name__ == "__main__":
    unittest.main()
