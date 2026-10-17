from dataclasses import replace
import unittest

from fitfleet import Budget, Policy, Run, Trace
from fitfleet.demo import generate
from fitfleet.errors import EvidenceError
from fitfleet.sizing import compare, quantile, recommend, run_once


class SizingTests(unittest.TestCase):
    def setUp(self):
        self.baseline = Budget(8000, 8192)
        self.policy = Policy(min_training_samples=20, min_validation_samples=20, min_window_s=0, retry_memory=True)
        self.trace = generate(count=100)

    def recommend(self, trace=None, **options):
        return recommend(trace or self.trace, self.baseline, self.policy,
                         workload="encoder", revision="v1", opt_cpu=options.get("opt_cpu", True),
                         opt_memory=options.get("opt_memory", True))

    def test_nearest_rank_quantiles_preserve_tail_observations(self):
        self.assertEqual(quantile([1, 2, 9, 10], 5000), 2)
        self.assertEqual(quantile([1, 2, 9, 10], 9900), 10)
        self.assertEqual(quantile([1, 2, 9, 10], 10000), 10)

    def test_per_cycle_bounds_are_rounded_up_not_below_the_safety_floor(self):
        candidate, meta, stats = self.recommend()
        self.assertEqual(candidate, Budget(4000, 7040))
        self.assertEqual(meta["count"], 100)
        self.assertLessEqual(stats["memory_reduction_pct"], 15)
        self.assertLessEqual(stats["cpu_reduction_pct"], 50)

    def test_untuned_resource_dimensions_are_preserved(self):
        cpu_only, _, _ = self.recommend(opt_memory=False)
        memory_only, _, _ = self.recommend(opt_cpu=False)
        self.assertEqual(cpu_only.memory_mib, 8192)
        self.assertEqual(memory_only.cpu_millis, 8000)

    def test_no_opt_in_does_not_create_a_silent_change(self):
        with self.assertRaises(EvidenceError):
            self.recommend(opt_cpu=False, opt_memory=False)

    def test_censored_or_near_cpu_limit_training_is_rejected(self):
        for changes in ({"censored": True}, {"cpu_peak_millis": 7900}):
            trace = Trace(tuple(replace(r, **changes) for r in self.trace.runs))
            with self.subTest(changes=changes), self.assertRaises(EvidenceError):
                self.recommend(trace)

    def test_memory_demand_above_training_allocation_is_rejected(self):
        trace = Trace(tuple(replace(r, memory_peak_mib=9000) for r in self.trace.runs))
        with self.assertRaises(EvidenceError):
            self.recommend(trace)

    def test_insufficient_samples_and_coverage_fail_closed(self):
        tiny = generate(count=3)
        with self.assertRaises(EvidenceError):
            self.recommend(tiny)
        policy = replace(self.policy, min_window_s=100000)
        with self.assertRaises(EvidenceError):
            recommend(self.trace, self.baseline, policy, opt_cpu=True, opt_memory=True, workload="encoder", revision="v1")

    def test_runtime_model_only_benefits_from_usable_parallelism(self):
        run = Run("r", 0, "encoder", "v1", 2000, 100, 1000, 100, 10000)
        self.assertEqual(run_once(run, Budget(2000, 1024), self.policy)["duration_ms"], 600)
        self.assertEqual(run_once(run, Budget(8000, 1024), self.policy)["duration_ms"], 600)
        self.assertEqual(run_once(run, Budget(1000, 1024), self.policy)["duration_ms"], 1100)

    def test_paired_replay_reports_a_synthetic_resource_time_score(self):
        report = compare(self.trace, self.baseline, Budget(4000, 7040), self.policy)
        self.assertTrue(report["passed"])
        self.assertAlmostEqual(report["resource_score_saving_pct"], 42.8125)
        self.assertEqual(report["p95_runtime_ratio"], 1)
        self.assertIn("not measured", report["model"])

    def test_memory_guard_retry_charges_discarded_work_and_larger_allocation(self):
        run = replace(self.trace.runs[0], memory_peak_mib=7000)
        result = run_once(run, Budget(4000, 7040), self.policy)
        first = run_once(replace(run, memory_peak_mib=1000), Budget(4000, 7040), self.policy)
        self.assertTrue(result["success"])
        self.assertEqual(result["retries"], 1)
        self.assertEqual(result["attempts"][1]["memory_mib"], 10624)
        self.assertAlmostEqual(result["duration_ms"], first["duration_ms"]*1.2)
        self.assertGreater(result["score"], first["score"])

    def test_retry_exhaustion_is_a_failure_not_an_ignored_outlier(self):
        run = replace(self.trace.runs[0], memory_peak_mib=65536)
        result = run_once(run, Budget(4000, 128), self.policy)
        self.assertFalse(result["success"])
        self.assertEqual(result["retries"], 3)

    def test_disabled_retry_models_hard_memory_failure(self):
        run = replace(self.trace.runs[0], memory_peak_mib=8000)
        result = run_once(run, Budget(4000, 7040), replace(self.policy, retry_memory=False))
        self.assertFalse(result["success"])
        self.assertEqual(result["attempts"][0]["outcome"], "oom")

    def test_rare_memory_retry_is_bounded_by_rate_and_worst_runtime(self):
        runs = list(self.trace.runs)
        runs[0] = replace(runs[0], memory_peak_mib=7000)
        report = compare(Trace(tuple(runs)), self.baseline, Budget(4000, 7040), self.policy)
        self.assertTrue(report["passed"])
        self.assertEqual(report["retry_rate_pct"], 1)
        self.assertAlmostEqual(report["worst_runtime_ratio"], 1.2)

    def test_rare_cpu_regression_is_caught_even_if_p95_hides_it(self):
        runs = list(self.trace.runs)
        runs[0] = replace(runs[0], cpu_peak_millis=6000)
        report = compare(Trace(tuple(runs)), self.baseline, Budget(4000, 7040), self.policy)
        self.assertTrue(report["checks"]["p95_runtime_within_budget"])
        self.assertFalse(report["checks"]["every_runtime_within_budget"])
        self.assertFalse(report["passed"])

    def test_systematic_memory_shift_rejects_candidate(self):
        shifted = generate(count=100, profile="memory-shift")
        report = compare(shifted, self.baseline, Budget(4000, 7040), self.policy)
        self.assertFalse(report["passed"])
        self.assertIn("retry_rate_within_budget", report["failed_checks"])

    def test_deadline_failure_blocks_even_a_cheap_candidate(self):
        trace = Trace(tuple(replace(r, deadline_ms=1) for r in self.trace.runs))
        report = compare(trace, self.baseline, Budget(4000, 7040), self.policy)
        self.assertFalse(report["checks"]["baseline_healthy"])
        self.assertFalse(report["checks"]["no_deadline_misses"])

    def test_no_economic_improvement_cannot_pass_initial_verification(self):
        self.assertFalse(compare(self.trace, self.baseline, self.baseline, self.policy)["passed"])
        self.assertTrue(compare(self.trace, self.baseline, self.baseline, self.policy, require_saving=False)["passed"])
