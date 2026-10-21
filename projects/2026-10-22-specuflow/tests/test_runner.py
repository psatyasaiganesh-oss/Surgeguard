import json
import tempfile
import unittest
from pathlib import Path

from specuflow.model import ValidationError
from specuflow.runner import make_job, run_job, synthetic_compute
from specuflow.store import Scheduler


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.scheduler = Scheduler(root / "scheduler.db", root / "blobs")

    def tearDown(self):
        self.temp.cleanup()

    def test_make_job_has_requested_shards(self):
        spec = make_job("generated", shards=7)
        self.assertEqual(len(spec["shards"]), 7)
        self.assertEqual(spec["shards"][0]["shard_id"], "shard-0000")

    def test_primary_and_backup_outputs_are_deterministic(self):
        spec = make_job(
            "deterministic", shards=1, straggler_delay_ms=0, backup_delay_ms=0
        )
        self.scheduler.submit(spec, now_ms=0)
        primary = self.scheduler.claim("deterministic", "one", now_ms=0)
        backup = self.scheduler.claim("deterministic", "two", now_ms=50)
        first_output, first_counters = synthetic_compute(primary)
        second_output, second_counters = synthetic_compute(backup)
        self.assertEqual(first_output, second_output)
        self.assertEqual(first_counters, second_counters)

    def test_runner_completes_job(self):
        spec = make_job(
            "complete", shards=4, straggler_delay_ms=0, regular_delay_ms=0
        )
        self.scheduler.submit(spec)
        result = run_job(self.scheduler, "complete", workers=2, allow_speculation=False)
        self.assertEqual(result["state"], "SUCCEEDED")
        self.assertEqual(result["counters"], {"records": 4, "words": 28})

    def test_no_speculation_has_one_attempt_per_task(self):
        spec = make_job(
            "plain", shards=5, straggler_delay_ms=5, regular_delay_ms=5
        )
        self.scheduler.submit(spec)
        result = run_job(self.scheduler, "plain", workers=3, allow_speculation=False)
        self.assertEqual(result["attempts"], 5)
        self.assertEqual(result["speculative_attempts"], 0)

    def test_speculation_reduces_measured_completion_time(self):
        baseline_spec = make_job(
            "baseline", shards=8, straggler_delay_ms=220,
            regular_delay_ms=5, backup_delay_ms=1, speculate_after_ms=20
        )
        fast_spec = make_job(
            "fast", shards=8, straggler_delay_ms=220,
            regular_delay_ms=5, backup_delay_ms=1, speculate_after_ms=20
        )
        self.scheduler.submit(baseline_spec)
        baseline = run_job(self.scheduler, "baseline", workers=3, allow_speculation=False)
        self.scheduler.submit(fast_spec)
        fast = run_job(self.scheduler, "fast", workers=3, allow_speculation=True)
        self.assertLess(fast["completion_seconds"], baseline["completion_seconds"] * 0.8)
        self.assertGreaterEqual(fast["speculative_attempts"], 1)

    def test_worker_count_is_validated(self):
        self.scheduler.submit(make_job("workers", shards=1))
        with self.assertRaises(ValidationError):
            run_job(self.scheduler, "workers", workers=0)

    def test_synthetic_payload_schema_is_closed(self):
        claim = {
            "payload": {
                "text": "hello",
                "primary_delay_ms": 0,
                "backup_delay_ms": 0,
                "command": "not allowed",
            },
            "kind": "PRIMARY",
            "shard_id": "one",
        }
        with self.assertRaisesRegex(ValidationError, "unknown"):
            synthetic_compute(claim)

    def test_compute_failures_exhaust_bounded_attempts(self):
        spec = make_job(
            "failure", shards=1, straggler_delay_ms=0, regular_delay_ms=0
        )
        self.scheduler.submit(spec)

        def broken(claim):
            raise RuntimeError("injected")

        result = run_job(
            self.scheduler, "failure", workers=1,
            allow_speculation=False, compute=broken
        )
        self.assertEqual(result["state"], "FAILED")
        self.assertEqual(result["error_code"], "ATTEMPTS_EXHAUSTED")
        self.assertEqual(len(result["completion_errors"]), 3)

    def test_committed_output_is_valid_json(self):
        self.scheduler.submit(make_job(
            "json", shards=1, straggler_delay_ms=0, regular_delay_ms=0
        ))
        run_job(self.scheduler, "json", workers=1, allow_speculation=False)
        value = json.loads(self.scheduler.output("json", "shard-0000"))
        self.assertEqual(value["shard_id"], "shard-0000")
        self.assertEqual(value["value"]["reliable"], 2)


if __name__ == "__main__":
    unittest.main()
