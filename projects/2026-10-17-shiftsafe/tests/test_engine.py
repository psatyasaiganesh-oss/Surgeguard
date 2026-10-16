import sqlite3

from shiftsafe import Engine
from shiftsafe.errors import (Conflict, ConsistencyError, InvalidTransition, NotFound,
                             ValidationError)
from .helpers import EngineCase, item


class WriteAndReadTests(EngineCase):
    def test_legacy_write_reads_and_persists_across_restart(self):
        self.engine.put(1, item(), operation_id="a")
        other = Engine(self.path)
        self.assertEqual(other.get(1)["subscriptions"], item())
        self.assertEqual(other.get(1)["source"], "legacy")
        self.assertEqual(other.status()["projection_state"], 0)

    def test_idempotency_replay_does_not_increment_revision(self):
        first = self.engine.put(1, item(), operation_id="a", expected_revision=0)
        second = self.engine.put(1, item(), operation_id="a", expected_revision=0)
        self.assertEqual(first["revision"], second["revision"])
        self.assertTrue(second["replayed"])
        self.assertEqual(self.engine.status()["data_version"], 1)

    def test_idempotency_key_conflict_rejects_different_body(self):
        self.engine.put(1, item(), operation_id="a")
        with self.assertRaises(Conflict):
            self.engine.put(1, item(amount=101), operation_id="a")
        self.assertEqual(self.engine.get(1)["subscriptions"], item())

    def test_optimistic_revision_prevents_lost_update(self):
        self.engine.put(1, item(), operation_id="a", expected_revision=0)
        self.engine.put(1, item(amount=101), operation_id="b", expected_revision=1)
        with self.assertRaises(Conflict):
            self.engine.put(1, item(amount=102), operation_id="c", expected_revision=1)
        self.assertEqual(self.engine.get(1)["revision"], 2)

    def test_delete_and_revival_preserve_monotonic_revision(self):
        self.engine.put(1, item(), operation_id="a")
        self.engine.delete(1, operation_id="b", expected_revision=1)
        deleted = self.engine.get(1)
        self.assertTrue(deleted["deleted"])
        self.assertEqual(deleted["subscriptions"], [])
        self.engine.put(1, item(), operation_id="c", expected_revision=2)
        self.assertEqual(self.engine.get(1)["revision"], 3)
        self.assertFalse(self.engine.get(1)["deleted"])

    def test_delete_missing_customer_does_not_create_receipt(self):
        with self.assertRaises(NotFound):
            self.engine.delete(7, operation_id="missing")
        self.assertEqual(self.engine.status()["operations"], 0)

    def test_get_missing_has_explicit_error(self):
        with self.assertRaises(NotFound):
            self.engine.get(99)

    def test_empty_subscription_list_is_not_a_deleted_customer(self):
        self.engine.put(1, [], operation_id="empty")
        self.assertFalse(self.engine.get(1)["deleted"])
        self.assertEqual(self.engine.get(1)["subscriptions"], [])

    def test_dual_write_failure_rolls_back_every_table_and_receipt(self):
        self.seed(1)
        self.engine.start()
        def fail(point):
            if point == "after_legacy_write":
                raise RuntimeError("injected dual-write failure")
        faulty = Engine(self.path, fault=fail)
        with self.assertRaises(RuntimeError):
            faulty.put(1, item(amount=999), operation_id="retry", expected_revision=1)
        self.assertEqual(self.engine.get(1)["subscriptions"], item())
        self.assertEqual(self.engine.status()["operations"], 1)
        self.assertEqual(self.engine.status()["projection_state"], 0)
        receipt = self.engine.put(1, item(amount=999), operation_id="retry", expected_revision=1)
        self.assertFalse(receipt["replayed"])
        self.assertEqual(receipt["revision"], 2)
        self.assertTrue(self.engine.verify()["ok"])

    def test_failure_after_projection_also_rolls_back_dual_write(self):
        self.ready(1)
        self.engine.cutover()
        def fail(point):
            if point == "after_projection_write":
                raise RuntimeError("injected reverse dual-write failure")
        faulty = Engine(self.path, fault=fail)
        with self.assertRaises(RuntimeError):
            faulty.put(1, item(amount=99), operation_id="bad")
        self.assertTrue(self.engine.verify()["ok"])
        self.assertEqual(self.engine.get(1)["subscriptions"], item())

    def test_canonical_order_makes_reversed_input_idempotent(self):
        items = item(1) + item(2)
        self.engine.put(1, items, operation_id="order")
        self.assertTrue(self.engine.put(1, list(reversed(items)), operation_id="order")["replayed"])


class StateMachineTests(EngineCase):
    def test_complete_migration_and_cleanup_retains_business_state(self):
        self.ready()
        self.engine.cutover()
        self.assertEqual(self.engine.get(1)["source"], "new")
        self.engine.finalize()
        total = 0
        while True:
            count = self.engine.cleanup(batch_size=2)
            total += count
            if not count:
                break
        self.assertEqual(total, 5)
        self.engine.put(1, item(amount=999), operation_id="finalized")
        self.assertEqual(self.engine.get(1)["subscriptions"], item(amount=999))
        self.assertEqual(self.engine.status()["legacy_customers"], 0)

    def test_cannot_skip_required_phases(self):
        for operation in (self.engine.shadow, self.engine.cutover, self.engine.finalize,
                          self.engine.rollback, self.engine.cleanup, self.engine.verify):
            with self.subTest(operation=operation.__name__), self.assertRaises(InvalidTransition):
                operation()

    def test_shadow_requires_complete_backfill(self):
        self.seed()
        self.engine.start()
        with self.assertRaises(InvalidTransition):
            self.engine.shadow()

    def test_start_cannot_be_repeated_and_lose_checkpoint(self):
        self.seed()
        self.engine.start()
        with self.assertRaises(InvalidTransition):
            self.engine.start()

    def test_empty_database_can_migrate_without_backfill_work(self):
        self.engine.start()
        self.engine.shadow()
        self.assertTrue(self.engine.cutover()["ok"])
        self.engine.finalize()
        self.assertEqual(self.engine.cleanup(), 0)

    def test_mismatch_blocks_cutover_but_shadow_serves_legacy(self):
        self.ready()
        self.corrupt()
        read = self.engine.get(1)
        self.assertEqual(read["subscriptions"], item())
        self.assertFalse(read["shadow_match"])
        self.assertEqual(self.engine.status()["shadow_mismatches"], 1)
        with self.assertRaises(ConsistencyError) as raised:
            self.engine.cutover()
        self.assertEqual(raised.exception.report["mismatches"], 1)
        self.assertEqual(self.engine.status()["phase"], "SHADOW")

    def test_repair_restores_projection_and_matching_digests(self):
        self.ready()
        self.corrupt()
        self.engine.repair(1)
        report = self.engine.verify()
        self.assertTrue(report["ok"])
        self.assertEqual(report["source_digest"], report["target_digest"])
        self.engine.cutover()

    def test_missing_projection_is_found_and_repaired(self):
        self.ready()
        with sqlite3.connect(self.path) as con:
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("DELETE FROM projection_state WHERE customer_id=3")
        self.assertEqual(self.engine.verify()["example_customer_ids"], [3])
        self.engine.repair(3)
        self.assertTrue(self.engine.verify()["ok"])

    def test_revision_mismatch_detected_even_when_values_equal(self):
        self.ready()
        with sqlite3.connect(self.path) as con:
            con.execute("UPDATE projection_state SET revision=99 WHERE customer_id=1")
        self.assertFalse(self.engine.verify()["ok"])
        self.engine.repair(1)
        self.assertTrue(self.engine.verify()["ok"])

    def test_metadata_mismatch_cannot_be_hidden_by_equal_store_digests(self):
        self.ready()
        with sqlite3.connect(self.path) as con:
            con.execute("UPDATE customers SET revision=99 WHERE customer_id=1")
        report = self.engine.verify()
        self.assertEqual(report["source_digest"], report["target_digest"])
        self.assertFalse(report["ok"])
        with self.assertRaises(ConsistencyError):
            self.engine.cutover()

    def test_rollback_after_new_write_keeps_that_write(self):
        self.ready()
        self.engine.cutover()
        self.engine.put(1, item(amount=555), operation_id="new")
        self.engine.rollback()
        self.assertEqual(self.engine.get(1)["subscriptions"], item(amount=555))
        self.assertEqual(self.engine.get(1)["source"], "legacy")
        self.engine.shadow()
        self.engine.cutover()

    def test_rollback_can_escape_corrupt_new_store(self):
        self.ready()
        self.engine.cutover()
        self.corrupt()
        self.engine.rollback()
        self.assertEqual(self.engine.get(1)["subscriptions"], item())
        self.engine.repair(1)
        self.assertTrue(self.engine.verify()["ok"])

    def test_finalize_rechecks_current_data_instead_of_using_old_verification(self):
        self.ready()
        self.engine.cutover()
        self.assertTrue(self.engine.verify()["ok"])
        self.corrupt()
        with self.assertRaises(ConsistencyError):
            self.engine.finalize()
        self.assertEqual(self.engine.status()["phase"], "NEW")

    def test_finalize_explicitly_ends_rollback_and_legacy_repair(self):
        self.ready()
        self.engine.cutover()
        self.engine.finalize()
        for operation in (self.engine.rollback, self.engine.verify, lambda: self.engine.repair(1)):
            with self.assertRaises(InvalidTransition):
                operation()

    def test_transition_crash_rolls_back_phase_and_audit_event(self):
        self.ready()
        events_before = self.engine.events()
        def fail(point):
            if point == "before_gate_commit":
                raise RuntimeError("gate crash")
        with self.assertRaises(RuntimeError):
            Engine(self.path, fault=fail).cutover()
        self.assertEqual(self.engine.status()["phase"], "SHADOW")
        self.assertEqual(self.engine.events(), events_before)

    def test_events_are_durable_paginated_and_contain_no_subscription_payloads(self):
        self.ready()
        first = self.engine.events(limit=2)
        later = self.engine.events(after=first[-1]["seq"], limit=2)
        self.assertGreater(later[0]["seq"], first[-1]["seq"])
        self.assertNotIn("amount_cents", str(first + later))

    def test_repair_nonexistent_source_is_rejected(self):
        self.engine.start()
        with self.assertRaises(NotFound):
            self.engine.repair(999)
