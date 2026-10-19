import concurrent.futures
import sqlite3

from burnwatch.engine import Engine
from .helpers import EPOCH, Fixture, fast_policy, values


class IngestTests(Fixture):
    def test_two_sources_materialize_weighted_counts(self):
        self.ingest("api-a", values(self.start, 60, 1, 1), EPOCH)
        self.ingest("api-b", values(self.start, 60, 9999, 0), EPOCH)
        result = self.engine.evaluate("checkout")
        self.assertEqual("OK", result["status"])
        self.assertEqual(.0001, result["rules"][0]["long"]["error_ratio"])
        self.assertEqual(600000, result["rules"][0]["long"]["total"])
        self.assertTrue(self.engine.verify("checkout")["passed"])

    def test_out_of_order_minutes_can_be_sealed_when_complete(self):
        self.ingest("api-a", list(reversed(values(self.start, 60))), EPOCH)
        self.assertEqual(self.start, self.engine.snapshot("checkout")["end_minute"])
        self.ingest("api-b", values(self.start, 60), EPOCH)
        self.assertEqual("OK", self.engine.evaluate("checkout")["status"])

    def test_watermark_is_minimum_of_all_expected_sources(self):
        self.ingest("api-a", values(self.start, 60), EPOCH)
        self.ingest("api-b", values(self.start, 5), self.start + 300)
        self.assertEqual(self.start + 300, self.engine.snapshot("checkout")["end_minute"])

    def test_gap_rejects_whole_batch_and_receipt(self):
        self.problem("gap", self.ingest, "api-a", values(self.start, 1), self.start + 120, "gap")
        with self.engine.tx() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM rollups").fetchone()[0])
        accepted = self.ingest("api-a", values(self.start, 2), self.start + 120, "gap")
        self.assertEqual(2, accepted["accepted"])

    def test_reports_buffer_before_watermark_without_affecting_evaluation(self):
        self.ingest("api-a", values(self.start, 60, 1000, 200))
        result = self.engine.evaluate("checkout")
        self.assertEqual(self.start, result["end_minute"])
        self.assertNotEqual("PAGE", result["status"])
        self.ingest("api-a", [], EPOCH)
        self.ingest("api-b", values(self.start, 60, 1000, 200), EPOCH)
        self.assertEqual("PAGE", self.engine.evaluate("checkout")["status"])

    def test_batch_retry_survives_restart_without_double_counting(self):
        first = self.ingest("api-a", values(self.start, 60), EPOCH, "once")
        other = Engine(self.path, clock=self.clock)
        replay = other.ingest("checkout", "api-a", "once", values(self.start, 60), EPOCH)
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["revision"], replay["revision"])
        self.assertTrue(other.verify("checkout")["passed"])

    def test_new_batch_duplicate_bucket_does_not_add_counts(self):
        self.ingest("api-a", values(self.start, 60), EPOCH)
        result = self.ingest("api-a", values(self.start, 60), EPOCH)
        self.assertEqual(0, result["accepted"])
        self.assertEqual(60, result["duplicates"])
        self.assertEqual(1, result["revision"])

    def test_conflicting_bucket_is_immutable(self):
        self.ingest("api-a", values(self.start, 1))
        self.problem("conflict", self.ingest, "api-a", values(self.start, 1, 1000, 1))

    def test_request_id_conflict(self):
        self.ingest("api-a", values(self.start, 1), batch_id="once")
        self.problem("conflict", self.ingest, "api-a", values(self.start, 2), batch_id="once")

    def test_watermark_cannot_regress(self):
        self.fill()
        self.problem("watermark", self.ingest, "api-a", [], EPOCH - 60)

    def test_open_or_future_minute_rejected(self):
        self.problem("invalid_input", self.ingest, "api-a", values(EPOCH, 1))
        self.problem("watermark", self.ingest, "api-a", [], EPOCH + 60)

    def test_before_service_start_rejected(self):
        self.problem("invalid_input", self.ingest, "api-a", values(self.start - 60, 1))

    def test_duplicate_minute_in_batch_rejected(self):
        self.problem("invalid_input", self.ingest, "api-a", values(self.start, 1) * 2)

    def test_unknown_source_and_service_rejected(self):
        self.problem("not_found", self.ingest, "unknown", values(self.start, 1))
        self.problem("not_found", self.engine.ingest, "missing", "api-a", "a", [], None)

    def test_invalid_counts_minutes_and_fields(self):
        base = values(self.start, 1)[0]
        for change in ({"total": True}, {"bad": 1001}, {"bad": -1}, {"minute": self.start + 1},
                       {"extra": 1}, {"total": 10**9 + 1}):
            self.problem("invalid_input", self.ingest, "api-a", [{**base, **change}])

    def test_batch_and_buffer_limits(self):
        self.problem("invalid_input", self.ingest, "api-a", values(self.start, 501))
        self.clock.now += 501 * 60
        self.problem("invalid_input", self.ingest, "api-a", [], self.start + 501 * 60)
        old_start = EPOCH - 8000 * 60
        self.engine.register("old", ["api-a"], fast_policy(), old_start)
        self.problem("capacity", self.engine.ingest, "old", "api-a", "too-far", values(old_start + 7200 * 60, 1))

    def test_registration_retry_is_safe_but_policy_change_conflicts(self):
        self.fill()
        replay = self.engine.register("checkout", list(reversed(self.producers)), self.policy, self.start)
        self.assertTrue(replay["replayed"])
        self.assertEqual(2, replay["revision"])
        changed = fast_policy()
        changed["target_ppm"] = 999900
        self.problem("conflict", self.engine.register, "checkout", self.producers, changed, self.start)

    def test_invalid_registration(self):
        for sources in ([], ["a"] * 2, [f"a{i}" for i in range(9)], ["bad/producer"]):
            self.problem("invalid_input", self.engine.register, "new", sources, fast_policy(), self.start)
        self.problem("invalid_input", self.engine.register, 'x"bad', ["api-a"], fast_policy(), self.start)

    def test_parallel_idempotent_retries_apply_once(self):
        def retry(_):
            return self.engine.ingest("checkout", "api-a", "retry", values(self.start, 60), EPOCH)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(retry, range(8)))
        self.assertEqual(1, sum(not result["replayed"] for result in results))
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(60000, conn.execute("SELECT SUM(total) FROM rollups").fetchone()[0])

    def test_concurrent_producers_preserve_aggregate_and_watermark(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.engine.ingest, "checkout", p, "parallel", values(self.start, 60), EPOCH)
                       for p in self.producers]
            for future in futures:
                future.result(timeout=10)
        self.assertEqual("OK", self.engine.evaluate("checkout")["status"])
        self.assertTrue(self.engine.verify("checkout")["passed"])
