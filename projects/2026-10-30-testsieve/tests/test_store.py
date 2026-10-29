from __future__ import annotations

import concurrent.futures
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from testsieve.demo import BASE_NS, SECOND, changed_inventory, file_digest, small_snapshot
from testsieve.errors import (
    ConflictError,
    IdempotencyConflict,
    IntegrityError,
    NotFoundError,
    StateError,
    ValidationError,
)
from testsieve.store import TestSieveStore


SECRET = b"unit-test-signing-key-that-is-more-than-32-bytes"
REPOSITORY = "checkout-monorepo"


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "testsieve.sqlite"
        self.store = TestSieveStore(self.database, secret=SECRET)
        self.snapshot = small_snapshot()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def publish(self, *, now_ns: int = BASE_NS) -> dict:
        return self.store.publish_baseline(
            REPOSITORY, "baseline-1", 0, self.snapshot, now_ns=now_ns
        )

    def shadow_selection(self, key: str, *, now_ns: int = BASE_NS + 2 * SECOND) -> dict:
        return self.store.select_tests(
            REPOSITORY,
            key,
            changed_inventory(self.snapshot, "lib/payments.py", "payments-v2"),
            worker_count=2,
            now_ns=now_ns,
        )

    def enable_select(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW", now_ns=BASE_NS + SECOND)
        for index in range(2):
            selected = self.shadow_selection(
                f"shadow-{index}", now_ns=BASE_NS + (2 + index) * SECOND
            )
            self.store.record_shadow_result(
                REPOSITORY,
                selected["selection_id"],
                f"result-{index}",
                [],
                now_ns=BASE_NS + (2 + index) * SECOND,
            )
        self.store.set_mode(REPOSITORY, "SELECT", now_ns=BASE_NS + 4 * SECOND)

    def test_secret_must_be_long(self) -> None:
        with self.assertRaises(ValidationError):
            TestSieveStore(Path(self.temporary.name) / "short.sqlite", secret=b"short")

    def test_database_rejects_different_secret(self) -> None:
        self.publish()
        with self.assertRaises(IntegrityError):
            TestSieveStore(self.database, secret=b"a-different-key-that-is-definitely-long-enough")

    def test_publish_baseline(self) -> None:
        result = self.publish()
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["test_count"], 5)
        self.assertEqual(result["file_count"], 10)
        self.assertFalse(result["replayed"])

    def test_publish_baseline_is_idempotent(self) -> None:
        first = self.publish()
        second = self.publish(now_ns=BASE_NS + SECOND)
        self.assertTrue(second["replayed"])
        self.assertEqual(first["snapshot_digest"], second["snapshot_digest"])

    def test_publish_key_reuse_with_changed_snapshot_conflicts(self) -> None:
        self.publish()
        changed = copy.deepcopy(self.snapshot)
        changed["completed_commit"] = "other-commit"
        with self.assertRaises(IdempotencyConflict):
            self.store.publish_baseline(REPOSITORY, "baseline-1", 0, changed)

    def test_publish_requires_current_revision(self) -> None:
        self.publish()
        with self.assertRaises(ConflictError):
            self.store.publish_baseline(REPOSITORY, "baseline-2", 0, self.snapshot)

    def test_new_baseline_supersedes_old_and_resets_mode(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        changed = copy.deepcopy(self.snapshot)
        changed["completed_commit"] = "commit-b"
        result = self.store.publish_baseline(REPOSITORY, "baseline-2", 1, changed)
        self.assertEqual(result["parent_revision"], 1)
        self.assertEqual(self.store.stats(REPOSITORY)["mode"], "RECORD")
        with sqlite3.connect(self.database) as connection:
            status = connection.execute(
                "SELECT status FROM baselines WHERE repository=? AND revision=1",
                (REPOSITORY,),
            ).fetchone()[0]
        self.assertEqual(status, "SUPERSEDED")

    def test_mode_requires_baseline(self) -> None:
        with self.assertRaises(StateError):
            self.store.set_mode(REPOSITORY, "SHADOW")

    def test_invalid_mode_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.store.set_mode(REPOSITORY, "FAST")

    def test_select_mode_requires_clean_shadow_runs(self) -> None:
        self.publish()
        with self.assertRaises(StateError):
            self.store.set_mode(REPOSITORY, "SELECT")

    def test_selection_requires_baseline(self) -> None:
        with self.assertRaises(StateError):
            self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])

    def test_record_mode_runs_full_but_calculates_candidate(self) -> None:
        self.publish()
        result = self.store.select_tests(
            REPOSITORY,
            "select-1",
            changed_inventory(self.snapshot, "lib/payments.py", "payments-v2"),
            now_ns=BASE_NS + SECOND,
        )
        decision = result["decision"]
        self.assertEqual(decision["run_test_count"], 5)
        self.assertEqual(decision["candidate_test_count"], 3)
        self.assertEqual(decision["execution_reason"], "record_runs_full_suite")

    def test_two_clean_shadow_runs_enable_select(self) -> None:
        self.enable_select()
        stats = self.store.stats(REPOSITORY)
        self.assertEqual(stats["mode"], "SELECT")
        self.assertEqual(stats["shadow_passes"], 2)

    def test_local_change_selects_impacted_mandatory_and_prior_failure(self) -> None:
        self.enable_select()
        selected = self.store.select_tests(
            REPOSITORY,
            "localized",
            changed_inventory(self.snapshot, "lib/payments.py", "payments-v2"),
            worker_count=2,
            now_ns=BASE_NS + 5 * SECOND,
        )
        self.assertEqual(
            selected["decision"]["run_tests"],
            [
                "tests/discovery_test.py",
                "tests/payments_test.py",
                "tests/sticky_failure_test.py",
            ],
        )
        self.assertEqual(selected["decision"]["selection_fraction"], 0.6)

    def test_root_dependency_change_forces_full_suite(self) -> None:
        self.enable_select()
        result = self.store.select_tests(
            REPOSITORY,
            "root-change",
            changed_inventory(self.snapshot, "boot/runtime.py", "runtime-v2"),
            now_ns=BASE_NS + 5 * SECOND,
        )
        self.assertEqual(result["decision"]["run_test_count"], 5)
        self.assertIn("root_dependency_changed", result["decision"]["fallback_reasons"])

    def test_unknown_file_change_forces_full_suite(self) -> None:
        self.enable_select()
        inventory = dict(self.snapshot["inventory"])
        inventory["generated/schema.json"] = file_digest("schema-v1")
        result = self.store.select_tests(
            REPOSITORY, "unknown-change", inventory, now_ns=BASE_NS + 5 * SECOND
        )
        self.assertEqual(result["decision"]["run_test_count"], 5)
        self.assertEqual(result["decision"]["unknown_changes"], ["generated/schema.json"])

    def test_deleted_mapped_file_selects_observer(self) -> None:
        self.enable_select()
        inventory = dict(self.snapshot["inventory"])
        del inventory["lib/refunds.py"]
        result = self.store.select_tests(
            REPOSITORY, "deleted-file", inventory, now_ns=BASE_NS + 5 * SECOND
        )
        self.assertIn("tests/refunds_test.py", result["decision"]["run_tests"])
        self.assertNotIn("changed_file_has_no_observation", result["decision"]["fallback_reasons"])

    def test_stale_baseline_forces_full_suite(self) -> None:
        self.enable_select()
        result = self.store.select_tests(
            REPOSITORY,
            "stale",
            changed_inventory(self.snapshot, "lib/payments.py", "payments-v2"),
            max_baseline_age_ns=1,
            now_ns=BASE_NS + 5 * SECOND,
        )
        self.assertIn("baseline_stale", result["decision"]["fallback_reasons"])
        self.assertEqual(result["decision"]["run_test_count"], 5)

    def test_unmapped_test_is_always_candidate(self) -> None:
        snapshot = copy.deepcopy(self.snapshot)
        del snapshot["observations"]["tests/risk_test.py"]
        self.store.publish_baseline(REPOSITORY, "baseline-1", 0, snapshot, now_ns=BASE_NS)
        result = self.store.select_tests(
            REPOSITORY, "select-1", snapshot["inventory"], now_ns=BASE_NS + SECOND
        )
        self.assertIn("tests/risk_test.py", result["decision"]["candidate_tests"])
        self.assertIn("unmapped_test", result["decision"]["reasons"]["tests/risk_test.py"])

    def test_worker_plan_contains_every_run_test_once(self) -> None:
        self.publish()
        result = self.store.select_tests(
            REPOSITORY, "workers", self.snapshot["inventory"], worker_count=3
        )
        planned = [
            test_id
            for worker in result["decision"]["worker_plan"]
            for test_id in worker["tests"]
        ]
        self.assertEqual(sorted(planned), sorted(result["decision"]["run_tests"]))
        self.assertEqual(len(planned), len(set(planned)))

    def test_worker_count_is_bounded(self) -> None:
        self.publish()
        with self.assertRaises(ValidationError):
            self.store.select_tests(
                REPOSITORY, "workers", self.snapshot["inventory"], worker_count=65
            )

    def test_selection_retry_returns_same_identity(self) -> None:
        self.publish()
        first = self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        second = self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["selection_id"], second["selection_id"])

    def test_selection_key_reuse_with_changed_input_conflicts(self) -> None:
        self.publish()
        self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        with self.assertRaises(IdempotencyConflict):
            self.store.select_tests(
                REPOSITORY,
                "select-1",
                changed_inventory(self.snapshot, "lib/payments.py", "payments-v2"),
            )

    def test_concurrent_retries_create_one_selection(self) -> None:
        self.publish()

        def select() -> str:
            return self.store.select_tests(
                REPOSITORY, "concurrent", self.snapshot["inventory"]
            )["selection_id"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            identities = list(executor.map(lambda _: select(), range(16)))
        self.assertEqual(len(set(identities)), 1)
        self.assertEqual(self.store.stats(REPOSITORY)["selections"], 1)

    def test_signed_manifest_verifies(self) -> None:
        self.publish()
        selected = self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        verified = self.store.verify_manifest(
            selected["manifest"], expected_repository=REPOSITORY
        )
        self.assertTrue(verified["valid"])

    def test_manifest_tamper_is_rejected(self) -> None:
        self.publish()
        selected = self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        manifest = copy.deepcopy(selected["manifest"])
        manifest["payload"]["mode"] = "SELECT"
        with self.assertRaises(IntegrityError):
            self.store.verify_manifest(manifest, expected_repository=REPOSITORY)

    def test_manifest_repository_binding_is_enforced(self) -> None:
        self.publish()
        selected = self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        with self.assertRaises(NotFoundError):
            self.store.verify_manifest(selected["manifest"], expected_repository="other-repo")

    def test_shadow_result_requires_shadow_selection(self) -> None:
        self.publish()
        selected = self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        with self.assertRaises(StateError):
            self.store.record_shadow_result(
                REPOSITORY, selected["selection_id"], "result-1", []
            )

    def test_shadow_result_rejects_unknown_test(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        selected = self.shadow_selection("shadow-1")
        with self.assertRaises(ValidationError):
            self.store.record_shadow_result(
                REPOSITORY, selected["selection_id"], "result-1", ["tests/unknown.py"]
            )

    def test_shadow_result_is_idempotent(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        selected = self.shadow_selection("shadow-1")
        first = self.store.record_shadow_result(
            REPOSITORY, selected["selection_id"], "result-1", []
        )
        replay = self.store.record_shadow_result(
            REPOSITORY, selected["selection_id"], "result-1", []
        )
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(self.store.stats(REPOSITORY)["shadow_passes"], 1)

    def test_selection_accepts_only_one_shadow_result(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        selected = self.shadow_selection("shadow-1")
        self.store.record_shadow_result(
            REPOSITORY, selected["selection_id"], "result-1", []
        )
        with self.assertRaises(IdempotencyConflict):
            self.store.record_shadow_result(
                REPOSITORY,
                selected["selection_id"],
                "result-2",
                ["tests/refunds_test.py"],
            )

    def test_shadow_miss_quarantines_baseline_and_resets_mode(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        selected = self.shadow_selection("shadow-1")
        result = self.store.record_shadow_result(
            REPOSITORY,
            selected["selection_id"],
            "result-1",
            ["tests/refunds_test.py"],
        )
        self.assertFalse(result["safe"])
        self.assertEqual(result["baseline_status"], "QUARANTINED")
        stats = self.store.stats(REPOSITORY)
        self.assertEqual(stats["mode"], "RECORD")
        self.assertEqual(stats["false_negatives"], 1)

    def test_quarantined_baseline_forces_later_full_suite(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        selected = self.shadow_selection("shadow-1")
        self.store.record_shadow_result(
            REPOSITORY,
            selected["selection_id"],
            "result-1",
            ["tests/refunds_test.py"],
        )
        later = self.store.select_tests(
            REPOSITORY,
            "after-miss",
            changed_inventory(self.snapshot, "lib/payments.py", "payments-v2"),
        )
        self.assertIn("baseline_not_active", later["decision"]["fallback_reasons"])
        self.assertEqual(later["decision"]["run_test_count"], 5)

    def test_old_selection_rejected_after_new_baseline(self) -> None:
        self.publish()
        self.store.set_mode(REPOSITORY, "SHADOW")
        selected = self.shadow_selection("shadow-1")
        new_snapshot = copy.deepcopy(self.snapshot)
        new_snapshot["completed_commit"] = "commit-b"
        self.store.publish_baseline(REPOSITORY, "baseline-2", 1, new_snapshot)
        with self.assertRaises(ConflictError):
            self.store.record_shadow_result(
                REPOSITORY, selected["selection_id"], "result-1", []
            )

    def test_stats_report_persisted_counts(self) -> None:
        self.publish()
        self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        stats = self.store.stats(REPOSITORY)
        self.assertEqual(stats["baselines"], 1)
        self.assertEqual(stats["selections"], 1)
        self.assertEqual(stats["audit_entries"], 2)

    def test_integrity_verifier_accepts_valid_database(self) -> None:
        self.publish()
        self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        result = self.store.verify_integrity(REPOSITORY)
        self.assertTrue(result["valid"])
        self.assertEqual(result["selections"], 1)

    def test_database_trigger_blocks_baseline_evidence_update(self) -> None:
        self.publish()
        with sqlite3.connect(self.database) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE baselines SET snapshot_digest=? WHERE repository=?",
                    ("0" * 64, REPOSITORY),
                )

    def test_database_trigger_blocks_selection_delete(self) -> None:
        self.publish()
        self.store.select_tests(REPOSITORY, "select-1", self.snapshot["inventory"])
        with sqlite3.connect(self.database) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM selections WHERE repository=?", (REPOSITORY,))

    def test_integrity_verifier_detects_audit_tamper(self) -> None:
        self.publish()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE audit_log SET action='TAMPERED' WHERE repository=? AND sequence=1",
                (REPOSITORY,),
            )
        with self.assertRaises(IntegrityError):
            self.store.verify_integrity(REPOSITORY)

    def test_latest_baseline_missing_is_not_found(self) -> None:
        with self.assertRaises(NotFoundError):
            self.store.latest_baseline(REPOSITORY)


if __name__ == "__main__":
    unittest.main()
