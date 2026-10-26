from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rolloutjudge.errors import (
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    NotFoundError,
    StateError,
    ValidationError,
)
from rolloutjudge.model import MetricRule, Policy
from rolloutjudge.store import RolloutJudgeStore


SECRET = b"rolloutjudge-test-secret-with-32-bytes-minimum"


def test_policy(*, latency_limit: float = 5.0) -> Policy:
    return Policy(
        (
            MetricRule(
                "latency",
                "lower",
                critical=True,
                min_samples=5,
                max_absolute_degradation=latency_limit,
                max_relative_degradation=0.1,
                relative_floor=1,
            ),
            MetricRule(
                "throughput",
                "higher",
                min_samples=5,
                max_absolute_degradation=10,
                max_relative_degradation=0.1,
                relative_floor=10,
            ),
        ),
        pass_score=90,
        manual_score=40,
    ).validated()


BASE = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
SAFE = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
BAD = [140, 141, 142, 143, 144, 145, 146, 147, 148, 149]
THROUGHPUT = [1000, 1002, 1004, 1006, 1008, 1010, 1012, 1014, 1016, 1018]


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "judge.sqlite"
        self.store = RolloutJudgeStore(self.path, secret=SECRET)
        self.store.create_policy("standard", test_policy(), now_ns=1)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def start(self, *, service: str = "checkout", key: str = "start-1") -> dict:
        return self.store.start_run(
            service, "release-1", key, "standard", ["zone-a"], now_ns=2
        )

    def ingest_all(self, run: dict, *, bad_latency: bool = False) -> dict:
        service = run["service"]
        latency = self.store.ingest_evidence(
            service,
            run["run_id"],
            "latency-batch",
            "zone-a",
            "latency",
            BASE,
            BAD if bad_latency else SAFE,
            now_ns=3,
        )
        self.store.ingest_evidence(
            service,
            run["run_id"],
            "throughput-batch",
            "zone-a",
            "throughput",
            THROUGHPUT,
            THROUGHPUT,
            now_ns=4,
        )
        return latency

    def judge(self, run: dict, *, key: str = "decision-1") -> dict:
        current = self.store.get_run(run["service"], run["run_id"])
        return self.store.judge_run(
            run["service"],
            run["run_id"],
            key,
            expected_version=current["version"],
            now_ns=5,
        )

    def test_secret_length_is_enforced(self) -> None:
        with self.assertRaises(ValidationError):
            RolloutJudgeStore(Path(self.temporary.name) / "short.sqlite", secret=b"short")

    def test_existing_database_rejects_different_secret(self) -> None:
        with self.assertRaises(IntegrityError):
            RolloutJudgeStore(
                self.path, secret=b"different-rolloutjudge-secret-that-is-long-enough"
            )

    def test_policy_create_read_and_compare_and_set_update(self) -> None:
        current = self.store.policy("standard")
        self.assertEqual(current["version"], 1)
        updated = self.store.create_policy(
            "standard", test_policy(latency_limit=10), expected_version=1, now_ns=2
        )
        self.assertEqual(updated["version"], 2)
        with self.assertRaises(ConflictError):
            self.store.create_policy("standard", test_policy(), expected_version=1)

    def test_unknown_policy_is_not_found(self) -> None:
        with self.assertRaises(NotFoundError):
            self.store.policy("missing")
        with self.assertRaises(NotFoundError):
            self.store.start_run("svc", "rel", "key", "missing", ["zone-a"])

    def test_run_snapshots_policy_version_and_digest(self) -> None:
        run = self.start()
        self.store.create_policy(
            "standard", test_policy(latency_limit=10), expected_version=1, now_ns=3
        )
        found = self.store.get_run("checkout", run["run_id"])
        self.assertEqual(found["policy"]["version"], 1)
        self.assertNotEqual(found["policy"]["digest"], self.store.policy("standard")["digest"])

    def test_start_run_is_idempotent(self) -> None:
        first = self.start()
        replay = self.start()
        self.assertEqual(first["run_id"], replay["run_id"])
        self.assertTrue(replay["replayed"])

    def test_start_run_rejects_idempotency_key_reuse(self) -> None:
        self.start()
        with self.assertRaises(IdempotencyConflict):
            self.store.start_run(
                "checkout", "another-release", "start-1", "standard", ["zone-a"]
            )

    def test_concurrent_run_retries_create_one_run(self) -> None:
        def create(_: int):
            return self.store.start_run(
                "catalog", "release", "same-key", "standard", ["zone-a"]
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(create, range(8)))
        self.assertEqual(len({result["run_id"] for result in results}), 1)
        self.assertEqual(sum(result["replayed"] for result in results), 7)

    def test_evidence_is_idempotent(self) -> None:
        run = self.start()
        first = self.store.ingest_evidence(
            "checkout", run["run_id"], "batch", "zone-a", "latency", BASE, SAFE
        )
        replay = self.store.ingest_evidence(
            "checkout", run["run_id"], "batch", "zone-a", "latency", BASE, SAFE
        )
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["version"], replay["version"])

    def test_changed_evidence_under_same_batch_is_conflict(self) -> None:
        run = self.start()
        self.store.ingest_evidence(
            "checkout", run["run_id"], "batch", "zone-a", "latency", BASE, SAFE
        )
        with self.assertRaises(IdempotencyConflict):
            self.store.ingest_evidence(
                "checkout", run["run_id"], "batch", "zone-a", "latency", BASE, BAD
            )

    def test_evidence_slot_cannot_be_overwritten_by_new_batch(self) -> None:
        run = self.start()
        self.store.ingest_evidence(
            "checkout", run["run_id"], "batch-a", "zone-a", "latency", BASE, SAFE
        )
        with self.assertRaises(ConflictError):
            self.store.ingest_evidence(
                "checkout", run["run_id"], "batch-b", "zone-a", "latency", BASE, SAFE
            )

    def test_evidence_must_match_snapshot_trial_and_metric(self) -> None:
        run = self.start()
        for trial, metric in (("zone-b", "latency"), ("zone-a", "unknown")):
            with self.subTest(trial=trial, metric=metric), self.assertRaises(ValidationError):
                self.store.ingest_evidence(
                    "checkout", run["run_id"], f"{trial}-{metric}", trial, metric, BASE, SAFE
                )

    def test_service_cannot_read_or_mutate_another_service_run(self) -> None:
        run = self.start()
        with self.assertRaises(NotFoundError):
            self.store.get_run("catalog", run["run_id"])
        with self.assertRaises(NotFoundError):
            self.store.ingest_evidence(
                "catalog", run["run_id"], "batch", "zone-a", "latency", BASE, SAFE
            )

    def test_judge_pass_archives_report_and_signed_receipt(self) -> None:
        run = self.start()
        self.ingest_all(run)
        result = self.judge(run)
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["state"], "JUDGED")
        self.assertTrue(self.store.verify_receipt(result["receipt"])["valid"])

    def test_critical_regression_fails(self) -> None:
        run = self.start()
        self.ingest_all(run, bad_latency=True)
        result = self.judge(run)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["reason"], "critical_regression")

    def test_missing_evidence_is_inconclusive(self) -> None:
        run = self.start()
        self.store.ingest_evidence(
            "checkout", run["run_id"], "batch", "zone-a", "latency", BASE, SAFE
        )
        result = self.judge(run)
        self.assertEqual(result["verdict"], "INCONCLUSIVE")

    def test_judge_requires_current_version(self) -> None:
        run = self.start()
        self.ingest_all(run)
        with self.assertRaises(ConflictError):
            self.store.judge_run(
                "checkout", run["run_id"], "decision", expected_version=1
            )

    def test_judgment_retry_is_idempotent_but_new_key_is_rejected(self) -> None:
        run = self.start()
        self.ingest_all(run)
        result = self.judge(run)
        replay = self.store.judge_run(
            "checkout", run["run_id"], "decision-1", expected_version=1
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(result["receipt"], replay["receipt"])
        with self.assertRaises(IdempotencyConflict):
            self.store.judge_run(
                "checkout", run["run_id"], "new-decision", expected_version=1
            )

    def test_evidence_is_immutable_after_judgment(self) -> None:
        run = self.start()
        self.ingest_all(run)
        self.judge(run)
        with self.assertRaises(StateError):
            self.store.ingest_evidence(
                "checkout", run["run_id"], "late", "zone-a", "latency", BASE, SAFE
            )

    def test_receipt_tampering_and_bad_schema_are_rejected(self) -> None:
        run = self.start()
        self.ingest_all(run)
        receipt = self.judge(run)["receipt"]
        tampered = {
            "payload": {**receipt["payload"], "verdict": "FAIL"},
            "signature": receipt["signature"],
        }
        with self.assertRaises(IntegrityError):
            self.store.verify_receipt(tampered)
        with self.assertRaises(ValidationError):
            self.store.verify_receipt({"payload": {}})

    def test_receipt_can_be_bound_to_expected_service(self) -> None:
        run = self.start()
        self.ingest_all(run)
        receipt = self.judge(run)["receipt"]
        self.assertTrue(
            self.store.verify_receipt(receipt, expected_service="checkout")["valid"]
        )
        with self.assertRaises(NotFoundError):
            self.store.verify_receipt(receipt, expected_service="catalog")

    def test_integrity_verifies_complete_database(self) -> None:
        run = self.start()
        self.ingest_all(run)
        self.judge(run)
        result = self.store.verify_integrity()
        self.assertEqual(result["runs"], 1)
        self.assertEqual(result["evidence_sets"], 2)
        self.assertGreaterEqual(result["audit_entries"], 4)

    def test_integrity_detects_policy_tampering(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "UPDATE policies SET config_digest = ? WHERE policy_id = 'standard'", ("0" * 64,)
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_integrity_detects_evidence_tampering(self) -> None:
        run = self.start()
        self.store.ingest_evidence(
            "checkout", run["run_id"], "batch", "zone-a", "latency", BASE, SAFE
        )
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "UPDATE evidence SET baseline_json = ? WHERE run_id = ?",
                (json.dumps([0] * 10), run["run_id"]),
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_integrity_detects_report_tampering(self) -> None:
        run = self.start()
        self.ingest_all(run)
        self.judge(run)
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "UPDATE runs SET report_json = ? WHERE run_id = ?",
                (json.dumps({"verdict": "PASS"}), run["run_id"]),
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_integrity_detects_materialized_score_tampering(self) -> None:
        run = self.start()
        self.ingest_all(run)
        self.judge(run)
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "UPDATE runs SET score = 1 WHERE run_id = ?", (run["run_id"],)
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_integrity_detects_audit_tampering(self) -> None:
        self.start()
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE audit_log SET action = 'ALTERED' WHERE sequence = 1")
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity()

    def test_stats_count_lifecycle_events(self) -> None:
        run = self.start()
        self.ingest_all(run)
        self.judge(run)
        stats = self.store.stats()
        self.assertEqual(stats["runs"]["judged"], 1)
        self.assertEqual(stats["counters"]["evidence_ingested"], 2)
        self.assertEqual(stats["counters"]["verdict_pass"], 1)


if __name__ == "__main__":
    unittest.main()
