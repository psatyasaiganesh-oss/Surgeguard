from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from exitpack.errors import (
    AuthorizationError,
    CapacityError,
    ConflictError,
    IntegrityError,
    SimulatedCrash,
)
from exitpack.store import ExitPack


SECRET = b"exitpack-unit-test-secret-at-least-32-bytes"


def vm(vm_id: str, *, cpu: int = 2_000, memory: int = 8_000, lifetime: int = 1_000) -> dict:
    return {
        "vm_id": vm_id,
        "cpu_millis": cpu,
        "memory_mib": memory,
        "predicted_lifetime_s": lifetime,
    }


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "state.db"
        self.store = ExitPack(self.path, secret=SECRET)
        for index in range(4):
            self.store.register_host(
                "test",
                f"register-{index}",
                f"host-{index}",
                f"zone-{index % 2}",
                8_000,
                32_000,
                current_s=0,
            )

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()


class RegistrationTests(StoreTestCase):
    def test_registration_is_idempotent(self) -> None:
        replay = self.store.register_host(
            "test", "register-0", "host-0", "zone-0", 8_000, 32_000, current_s=1
        )
        self.assertTrue(replay["replayed"])
        with self.assertRaises(ConflictError):
            self.store.register_host(
                "test", "register-0", "different", "zone-0", 8_000, 32_000
            )

    def test_duplicate_host_with_new_key_conflicts(self) -> None:
        with self.assertRaises(ConflictError):
            self.store.register_host(
                "test", "new-key", "host-0", "zone-0", 8_000, 32_000
            )


class PlacementTests(StoreTestCase):
    def test_best_fit_packs_without_lifetime_separation(self) -> None:
        first = self.store.schedule("test", "a", vm("long", lifetime=100_000), "BEST_FIT", current_s=0)
        second = self.store.schedule("test", "b", vm("short", lifetime=100), "BEST_FIT", current_s=0)
        self.assertEqual(first["host_id"], second["host_id"])

    def test_lifetime_policy_separates_open_classes(self) -> None:
        first = self.store.schedule("test", "a", vm("long", lifetime=100_000), "LIFETIME", current_s=0)
        second = self.store.schedule("test", "b", vm("short", lifetime=100), "LIFETIME", current_s=0)
        self.assertNotEqual(first["host_id"], second["host_id"])

    def test_same_lifetime_class_packs_together(self) -> None:
        first = self.store.schedule("test", "a", vm("a", lifetime=100_000), "LIFETIME", current_s=0)
        second = self.store.schedule("test", "b", vm("b", lifetime=120_000), "LIFETIME", current_s=0)
        self.assertEqual(first["host_id"], second["host_id"])

    def test_recycling_host_accepts_a_shorter_class(self) -> None:
        # Four 2k VMs fill the 8k host and mark its original class residual.
        for index in range(4):
            self.store.schedule(
                "test", f"long-{index}", vm(f"long-{index}", lifetime=100_000), "LIFETIME", current_s=0
            )
        self.store.finish_vm("test", "finish-one", "long-0", current_s=1)
        filler = self.store.schedule(
            "test", "short-filler", vm("short-filler", lifetime=100), "LIFETIME", current_s=1
        )
        self.assertEqual(filler["host_id"], "host-0")

    def test_capacity_failure_is_explicit(self) -> None:
        with self.assertRaises(CapacityError):
            self.store.create_plan(
                "test", "huge", vm("huge", cpu=9_000, memory=1), "BEST_FIT", current_s=0
            )

    def test_two_plans_from_one_revision_are_fenced(self) -> None:
        revision = self.store.snapshot("test")["cluster_revision"]
        a = self.store.create_plan(
            "test", "a", vm("a"), "BEST_FIT", expected_revision=revision, current_s=0
        )
        b = self.store.create_plan(
            "test", "b", vm("b"), "BEST_FIT", expected_revision=revision, current_s=0
        )
        self.store.commit_plan("test", a["plan_id"], a["commit_token"], current_s=1)
        with self.assertRaisesRegex(ConflictError, "stale"):
            self.store.commit_plan("test", b["plan_id"], b["commit_token"], current_s=1)

    def test_invalid_commit_capability_is_rejected(self) -> None:
        plan = self.store.create_plan("test", "a", vm("a"), "BEST_FIT", current_s=0)
        with self.assertRaises(AuthorizationError):
            self.store.commit_plan("test", plan["plan_id"], "0" * 64, current_s=1)

    def test_lost_placement_ack_replays_without_second_vm(self) -> None:
        plan = self.store.create_plan("test", "a", vm("a"), "BEST_FIT", current_s=0)
        with self.assertRaises(SimulatedCrash):
            self.store.commit_plan(
                "test",
                plan["plan_id"],
                plan["commit_token"],
                current_s=1,
                failpoint="after_plan_commit",
            )
        replay = self.store.commit_plan(
            "test", plan["plan_id"], plan["commit_token"], current_s=2
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(len(self.store.snapshot("test")["vms"]), 1)


class LifecycleTests(StoreTestCase):
    def test_lost_finish_ack_replays(self) -> None:
        self.store.schedule("test", "a", vm("a"), "BEST_FIT", current_s=0)
        with self.assertRaises(SimulatedCrash):
            self.store.finish_vm(
                "test", "finish-a", "a", current_s=10, failpoint="after_vm_finish"
            )
        replay = self.store.finish_vm("test", "finish-a", "a", current_s=11)
        self.assertTrue(replay["replayed"])
        self.assertEqual(self.store.metrics("test")["finished_vms"], 1)

    def test_finish_with_a_different_key_is_not_silently_replayed(self) -> None:
        self.store.schedule("test", "a", vm("a"), "BEST_FIT", current_s=0)
        self.store.finish_vm("test", "finish-a", "a", current_s=10)
        with self.assertRaises(ConflictError):
            self.store.finish_vm("test", "other-key", "a", current_s=11)

    def test_misprediction_promotes_host_class(self) -> None:
        result = self.store.schedule(
            "test", "a", vm("a", lifetime=100), "LIFETIME", current_s=0
        )
        self.store.reconcile("test", current_s=101)
        host = next(
            item for item in self.store.snapshot("test")["hosts"] if item["host_id"] == result["host_id"]
        )
        self.assertEqual((host["state"], host["lifetime_class"]), ("RECYCLING", 1))

    def test_recycling_host_reclassifies_after_residuals_exit(self) -> None:
        for index in range(4):
            self.store.schedule(
                "test", f"long-{index}", vm(f"long-{index}", lifetime=100_000), "LIFETIME", current_s=0
            )
        self.store.finish_vm("test", "finish-long-0", "long-0", current_s=1)
        self.store.schedule(
            "test", "filler", vm("filler", lifetime=100), "LIFETIME", current_s=1
        )
        for index in range(1, 4):
            self.store.finish_vm(
                "test", f"finish-long-{index}", f"long-{index}", current_s=2
            )
        host = next(item for item in self.store.snapshot("test")["hosts"] if item["host_id"] == "host-0")
        self.assertEqual(host["lifetime_class"], 0)
        filler = next(item for item in self.store.snapshot("test")["vms"] if item["vm_id"] == "filler")
        self.assertTrue(filler["residual"])

    def test_maintenance_waits_for_short_vms_and_orders_longest_first(self) -> None:
        for key, identity, lifetime in (
            ("long", "long", 100_000),
            ("medium", "medium", 10_000),
            ("short", "short", 100),
        ):
            self.store.schedule("test", key, vm(identity, lifetime=lifetime), "BEST_FIT", current_s=0)
        plan = self.store.maintenance_plan("test", "host-0", 500, current_s=0)
        self.assertEqual(plan["lifetime_aware_migrations"], 2)
        self.assertEqual([item["vm_id"] for item in plan["decisions"]], ["long", "medium", "short"])

    def test_draining_host_is_excluded_from_new_plans(self) -> None:
        self.store.set_draining("test", "drain", "host-0", current_s=0)
        result = self.store.schedule("test", "a", vm("a"), "BEST_FIT", current_s=1)
        self.assertNotEqual(result["host_id"], "host-0")

    def test_migration_is_revision_fenced_and_retry_safe(self) -> None:
        placed = self.store.schedule("test", "a", vm("a"), "BEST_FIT", current_s=0)
        revision = self.store.snapshot("test")["cluster_revision"]
        destination = "host-1" if placed["host_id"] != "host-1" else "host-2"
        with self.assertRaises(SimulatedCrash):
            self.store.migrate_vm(
                "test",
                "move-a",
                "a",
                destination,
                revision,
                current_s=1,
                failpoint="after_migration",
            )
        replay = self.store.migrate_vm(
            "test", "move-a", "a", destination, revision, current_s=2
        )
        self.assertTrue(replay["replayed"])
        moved = next(item for item in self.store.snapshot("test")["vms"] if item["vm_id"] == "a")
        self.assertEqual((moved["host_id"], moved["migration_count"]), (destination, 1))


class IntegrityTests(StoreTestCase):
    def test_metrics_and_integrity_report_real_state(self) -> None:
        self.store.schedule("test", "a", vm("a"), "BEST_FIT", current_s=0)
        metrics = self.store.metrics("test")
        self.assertEqual((metrics["hosts"], metrics["running_vms"]), (4, 1))
        self.assertTrue(self.store.verify_integrity("test")["valid"])

    def test_audit_tampering_is_detected(self) -> None:
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE audit_log SET entity_id = 'changed' WHERE sequence = 1")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(IntegrityError, "audit entry hash"):
            self.store.verify_integrity("test")

    def test_over_capacity_corruption_is_detected(self) -> None:
        self.store.schedule("test", "a", vm("a"), "BEST_FIT", current_s=0)
        connection = sqlite3.connect(self.path)
        connection.execute(
            "UPDATE hosts SET cpu_capacity = 1 WHERE namespace = 'test' AND host_id = 'host-0'"
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(IntegrityError, "over capacity"):
            self.store.verify_integrity("test")


if __name__ == "__main__":
    unittest.main()
