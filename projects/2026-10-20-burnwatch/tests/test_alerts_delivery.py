import concurrent.futures
import subprocess
import sys
from pathlib import Path

from burnwatch.delivery import Inbox, deliver
from burnwatch.engine import Engine
from burnwatch.model import default_policy
from .helpers import EPOCH, Fixture, values


class AlertTests(Fixture):
    def test_healthy_complete_data_reports_ok(self):
        self.assertEqual("OK", self.fill()["status"])

    def test_fresh_initial_service_is_warming_not_ok(self):
        self.engine.register("new", ["a"], self.policy, EPOCH)
        self.assertEqual("WARMING", self.engine.evaluate("new")["status"])

    def test_brief_spike_does_not_page(self):
        self.fill()
        result = self.advance(bad=10)
        self.assertGreater(result["rules"][0]["short"]["error_ratio"], .001)
        self.assertEqual("CLEAR", result["severity"])

    def test_sustained_errors_page_at_five_minutes(self):
        self.fill()
        for index in range(1, 6):
            result = self.advance(bad=200)
            self.assertEqual("PAGE" if index == 5 else "CLEAR", result["severity"])

    def test_short_window_resets_without_waiting_for_long_window(self):
        self.fill(bad=200)
        self.assertEqual("PAGE", self.advance(4)["severity"])
        result = self.advance()
        self.assertEqual("OK", result["status"])
        self.assertGreater(result["rules"][0]["long"]["burn_rate"], 14.4)

    def test_stale_producer_holds_page_and_exports_unknown_quality(self):
        self.fill(bad=200)
        self.clock.now += 180
        result = self.engine.evaluate("checkout")
        self.assertEqual("PAGE_HOLD", result["status"])
        metrics = self.engine.metrics("checkout")
        self.assertIn('burnwatch_telemetry_fresh{service="checkout"} 0', metrics)
        self.assertIn('burnwatch_evidence_complete{service="checkout"} 0', metrics)

    def test_zero_traffic_does_not_resolve_active_incident(self):
        self.fill(bad=200)
        result = self.advance(5, total=0)
        self.assertEqual("PAGE_HOLD", result["status"])
        self.assertEqual("NO_TRAFFIC", result["quality"])

    def test_incomplete_rollups_cannot_resolve(self):
        self.fill(bad=200)
        with self.engine.tx(True) as conn:
            conn.execute("DELETE FROM rollups WHERE service='checkout' AND minute=?", (EPOCH - 60,))
        result = self.engine.evaluate("checkout")
        self.assertEqual("INCOMPLETE", result["quality"])
        self.assertEqual("PAGE_HOLD", result["status"])
        self.assertFalse(self.engine.verify("checkout")["passed"])

    def test_three_firing_rules_produce_one_page_transition(self):
        start = EPOCH - 4320 * 60
        self.engine.register("full", ["a"], default_policy(), start)
        for offset in range(0, 4320, 500):
            count = min(500, 4320 - offset)
            self.engine.ingest("full", "a", f"full-{offset}", values(start + offset * 60, count, 1000, 200),
                               start + (offset + count) * 60)
        result = self.engine.evaluate("full")
        self.assertEqual(3, sum(rule["firing_now"] for rule in result["rules"]))
        events = [event for event in self.engine.pending() if event["service"] == "full"]
        self.assertEqual(1, len(events))
        self.assertEqual("PAGE", events[0]["to"]["severity"])

    def test_repeated_and_parallel_evaluations_emit_no_duplicate_transition(self):
        first = self.fill(bad=200)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.engine.evaluate("checkout"), range(8)))
        self.assertEqual([first["seq"]] * 8, [result["seq"] for result in results])
        self.assertEqual(1, len(self.engine.pending()))

    def test_restart_preserves_alert_latches_and_notification_identity(self):
        first = self.fill(bad=200)
        identity = self.engine.pending()[0]["event_id"]
        self.clock.now += 180
        other = Engine(self.path, clock=self.clock)
        self.assertEqual("PAGE_HOLD", other.evaluate("checkout")["status"])
        self.assertEqual(identity, other.pending()[0]["event_id"])
        self.assertEqual(first["seq"] + 1, other.snapshot("checkout")["last_evaluation"]["seq"])

    def test_prune_preserves_rolling_window_and_rejects_resurrection(self):
        self.fill(bad=200)
        before = self.advance(10)
        pruned = self.engine.prune("checkout")
        self.assertEqual(20, pruned["raw_deleted"])
        self.assertEqual(10, pruned["rollups_deleted"])
        self.assertEqual(before["rules"], self.engine.evaluate("checkout")["rules"])
        self.assertTrue(self.engine.verify("checkout")["passed"])
        self.problem("sealed", self.ingest, "api-a", values(self.start, 1, 1000, 200))

    def test_exact_retry_receipt_remains_valid_after_pruning(self):
        self.fill()
        self.advance(10)
        self.engine.prune("checkout")
        replay = self.engine.ingest("checkout", "api-a", "batch-1", values(self.start, 60), EPOCH)
        self.assertTrue(replay["replayed"])
        self.assertTrue(self.engine.verify("checkout")["passed"])

    def test_metrics_mark_outdated_evaluation_after_new_sealed_data(self):
        self.fill()
        self.clock.now += 60
        for producer in self.producers:
            self.ingest(producer, values(EPOCH, 1), self.clock.now)
        self.assertIn('burnwatch_evaluation_stale{service="checkout"} 1', self.engine.metrics("checkout"))
        self.engine.evaluate("checkout")
        self.assertIn('burnwatch_evidence_complete{service="checkout"} 1', self.engine.metrics("checkout"))


class DeliveryTests(Fixture):
    def setUp(self):
        super().setUp()
        self.fill(bad=200)
        self.inbox_path = Path(self.temp.name) / "inbox.db"
        self.inbox = Inbox(self.inbox_path)

    def test_local_delivery_is_durable_and_idempotent(self):
        result = deliver(self.engine, self.inbox)
        self.assertEqual(1, result["new_inbox_events"])
        self.assertEqual([], self.engine.pending())
        self.assertEqual(1, len(Inbox(self.inbox_path).events()))
        self.assertEqual(0, deliver(self.engine, self.inbox)["attempted"])

    def test_crash_after_store_before_ack_redelivers_without_duplicate_effect(self):
        def crash(_):
            raise RuntimeError("injected failure")
        with self.assertRaises(RuntimeError):
            deliver(self.engine, self.inbox, _after_store=crash)
        self.assertEqual(1, len(self.inbox.events()))
        self.assertEqual(1, len(self.engine.pending()))
        result = deliver(self.engine, self.inbox)
        self.assertEqual(1, result["duplicate_deliveries"])
        self.assertEqual(1, len(self.inbox.events()))
        self.assertFalse(self.engine.pending())

    def test_conflicting_inbox_payload_is_rejected(self):
        event = self.engine.pending()[0]
        self.inbox.store(event)
        self.problem("integrity", self.inbox.store, {**event, "status": "tampered"})
        self.assertEqual(1, len(self.engine.pending()))

    def test_parallel_delivery_has_one_inbox_effect(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: deliver(self.engine, self.inbox), range(8)))
        self.assertEqual(1, len(self.inbox.events()))
        self.assertFalse(self.engine.pending())

    def test_unknown_acknowledgement_rejected(self):
        self.problem("not_found", self.engine.acknowledge, "0" * 64)

    def test_event_ids_are_namespaced_by_database(self):
        other = Engine(Path(self.temp.name) / "other.db", clock=self.clock)
        other.register("checkout", self.producers, self.policy, self.start)
        for producer in self.producers:
            other.ingest("checkout", producer, "same", values(self.start, 60, 1000, 200), EPOCH)
        other.evaluate("checkout")
        self.assertNotEqual(self.engine.pending()[0]["event_id"], other.pending()[0]["event_id"])

    def test_actual_process_exit_between_store_and_ack(self):
        code = """
import os, sys
from burnwatch.engine import Engine
from burnwatch.delivery import Inbox, deliver
deliver(Engine(sys.argv[1]), Inbox(sys.argv[2]), _after_store=lambda _: os._exit(31))
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path), str(self.inbox_path)],
                                timeout=10, capture_output=True)
        self.assertEqual(31, result.returncode)
        self.assertEqual(1, len(self.inbox.events()))
        self.assertEqual(1, deliver(self.engine, self.inbox)["duplicate_deliveries"])


class CrashTests(Fixture):
    def test_process_exit_rolls_back_partial_report_and_watermark(self):
        code = """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1], isolation_level=None)
conn.execute('BEGIN IMMEDIATE')
conn.execute('INSERT INTO samples VALUES(?,?,?,?,?)', ('checkout','api-a',int(sys.argv[2]),1000,200))
conn.execute("UPDATE producers SET watermark=watermark+60 WHERE service='checkout' AND id='api-a'")
os._exit(32)
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path), str(self.start)], timeout=10, capture_output=True)
        self.assertEqual(32, result.returncode)
        other = Engine(self.path, clock=self.clock)
        self.assertEqual(0, other.verify("checkout")["minutes"])
        self.assertEqual(self.start, other.snapshot("checkout")["end_minute"])

    def test_committed_report_and_receipt_survive_abrupt_exit(self):
        code = """
import os, sys
from burnwatch.engine import Engine
start=int(sys.argv[2]); now=int(sys.argv[3])
Engine(sys.argv[1],clock=lambda:now).ingest('checkout','api-a','durable',
 [{'minute':start,'total':1000,'bad':200}],start+60)
os._exit(33)
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path), str(self.start), str(EPOCH)],
                                timeout=10, capture_output=True)
        self.assertEqual(33, result.returncode)
        other = Engine(self.path, clock=self.clock)
        self.assertTrue(other.verify("checkout")["passed"])
        self.assertTrue(other.ingest("checkout", "api-a", "durable", values(self.start, 1, 1000, 200), self.start + 60)["replayed"])
