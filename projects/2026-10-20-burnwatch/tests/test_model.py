import copy
import unittest

from burnwatch.model import Problem, assess, default_policy, parse_json, validate_policy
from .helpers import EPOCH, fast_policy


class PolicyMathTests(unittest.TestCase):
    def evaluate(self, *, long_bad=2000, short_bad=2000, total=100000, previous=None, stale=False, history=3600):
        windows = {length: {"total": total, "bad": bad, "minutes": length, "complete_minutes": length}
                   for length, bad in ((60, long_bad), (5, short_bad))}
        return assess(fast_policy(), EPOCH - history, EPOCH, windows, previous or {}, stale)

    def test_both_windows_required(self):
        self.assertEqual("PAGE", self.evaluate()["severity"])
        self.assertEqual("CLEAR", self.evaluate(short_bad=0)["severity"])
        self.assertEqual("CLEAR", self.evaluate(long_bad=0)["severity"])

    def test_exact_threshold_is_not_exceeded(self):
        self.assertEqual("CLEAR", self.evaluate(long_bad=1440, short_bad=1440)["severity"])
        self.assertEqual("PAGE", self.evaluate(long_bad=1441, short_bad=1441)["severity"])

    def test_burn_ratio_is_relative_to_error_budget(self):
        result = self.evaluate(long_bad=100, short_bad=100)
        self.assertEqual(1.0, result["rules"][0]["long"]["burn_rate"])

    def test_no_requests_is_not_healthy(self):
        result = self.evaluate(long_bad=0, short_bad=0, total=0)
        self.assertEqual("NO_TRAFFIC", result["status"])
        self.assertIsNone(result["rules"][0]["long"]["burn_rate"])

    def test_low_traffic_is_not_healthy(self):
        self.assertEqual("LOW_TRAFFIC", self.evaluate(long_bad=1, short_bad=1, total=10)["status"])

    def test_stale_data_holds_active_rule(self):
        result = self.evaluate(long_bad=0, short_bad=0, previous={"fast": True}, stale=True)
        self.assertEqual("PAGE_HOLD", result["status"])
        self.assertEqual("STALE", result["quality"])

    def test_no_traffic_does_not_resolve_active_page(self):
        result = self.evaluate(long_bad=0, short_bad=0, total=0, previous={"fast": True})
        self.assertEqual("PAGE_HOLD", result["status"])

    def test_warmup_never_reports_ok(self):
        self.assertEqual("WARMING", self.evaluate(long_bad=0, short_bad=0, history=300)["status"])

    def test_ready_healthy_evidence_resolves(self):
        result = self.evaluate(long_bad=0, short_bad=0, previous={"fast": True})
        self.assertEqual("OK", result["status"])

    def test_missing_materialized_minute_is_incomplete(self):
        windows = {n: {"total": 100000, "bad": 0, "minutes": n - 1, "complete_minutes": n - 1} for n in (60, 5)}
        result = assess(fast_policy(), EPOCH - 3600, EPOCH, windows, {"fast": True}, False)
        self.assertEqual("INCOMPLETE", result["quality"])
        self.assertEqual("PAGE_HOLD", result["status"])

    def test_default_policy_validates(self):
        self.assertEqual(default_policy(), validate_policy(default_policy()))

    def test_invalid_policy_variations(self):
        variants = []
        for field, value in (("target_ppm", 1000000), ("target_ppm", True), ("period_minutes", 0),
                             ("stale_after_seconds", 1), ("rules", [])):
            policy = default_policy()
            policy[field] = value
            variants.append(policy)
        for field, value in (("burn", "NaN"), ("burn", "1e2"), ("burn", "1000"), ("burn", "0"),
                             ("min_requests", True), ("short_minutes", 61), ("long_minutes", 4321),
                             ("severity", "EMAIL"), ("name", 'x"}bad')):
            policy = default_policy()
            policy["rules"][0][field] = value
            variants.append(policy)
        for policy in variants:
            with self.subTest(policy=policy), self.assertRaises(Problem):
                validate_policy(policy)

    def test_duplicate_rule_names_rejected(self):
        policy = default_policy()
        policy["rules"] += [copy.deepcopy(policy["rules"][0])]
        with self.assertRaises(Problem):
            validate_policy(policy)

    def test_strict_json_rejects_ambiguous_or_unbounded_data(self):
        cases = [b'{"a":1,"a":2}', b'{"x":NaN}', b'{"x":1e999}', b'\xff', b'{} garbage',
                 ('[' * 14 + '0' + ']' * 14).encode(), b'0' * 65537, '{}'.encode('utf-16')]
        for raw in cases:
            with self.subTest(length=len(raw)), self.assertRaises(Problem):
                parse_json(raw)
