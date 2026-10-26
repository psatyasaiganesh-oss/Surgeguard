from __future__ import annotations

import math
import unittest

from rolloutjudge.errors import ValidationError
from rolloutjudge.judge import benjamini_hochberg, judge, mann_whitney_two_sided
from rolloutjudge.model import (
    MetricRule,
    Policy,
    canonical_json,
    validate_id,
    validate_integer,
    validate_number,
    validate_series,
    validate_trials,
)


def rule(**changes):
    values = {
        "name": "latency",
        "direction": "lower",
        "critical": True,
        "min_samples": 5,
        "max_relative_degradation": 0.1,
        "max_absolute_degradation": 5.0,
        "relative_floor": 1.0,
    }
    values.update(changes)
    return MetricRule(**values).validated()


def policy(*rules: MetricRule, pass_score: float = 90, manual_score: float = 70) -> Policy:
    return Policy(tuple(rules), pass_score=pass_score, manual_score=manual_score).validated()


class ValidationTests(unittest.TestCase):
    def test_id_accepts_closed_safe_alphabet(self) -> None:
        self.assertEqual(validate_id("service.api_v2:west", "id"), "service.api_v2:west")

    def test_id_rejects_invalid_values(self) -> None:
        for value in ("", "has space", "slash/value", "x" * 97, 4, None):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_id(value, "id")  # type: ignore[arg-type]

    def test_integer_rejects_bool_and_bounds(self) -> None:
        for value in (True, 0, 11, 1.5):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_integer(value, "count", 1, 10)  # type: ignore[arg-type]

    def test_number_requires_finite_in_range(self) -> None:
        for value in (True, float("nan"), float("inf"), -1, 11, "2"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_number(value, "rate", 0, 10)  # type: ignore[arg-type]

    def test_canonical_json_is_stable_and_rejects_nonfinite(self) -> None:
        self.assertEqual(canonical_json({"b": 2, "a": 1}), '{"a":1,"b":2}')
        with self.assertRaises(ValidationError):
            canonical_json({"bad": math.nan})

    def test_series_accepts_null_but_rejects_bad_values(self) -> None:
        self.assertEqual(validate_series([1, None, 2.5], "series"), (1.0, None, 2.5))
        for value in ([], [True], [math.inf], "not-array"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_series(value, "series")

    def test_metric_rule_round_trip(self) -> None:
        original = rule(group="api", missing_strategy="zero")
        self.assertEqual(MetricRule.from_dict(original.as_dict()), original)

    def test_metric_rule_rejects_invalid_semantics_and_unknown_fields(self) -> None:
        with self.assertRaises(ValidationError):
            MetricRule("x", "sideways").validated()
        with self.assertRaises(ValidationError):
            MetricRule.from_dict({"name": "x", "direction": "lower", "mystery": 1})

    def test_policy_round_trip_and_unique_metrics(self) -> None:
        original = policy(rule(), rule(name="errors"))
        self.assertEqual(Policy.from_dict(original.as_dict()), original)
        with self.assertRaises(ValidationError):
            policy(rule(), rule())

    def test_policy_threshold_order_is_validated(self) -> None:
        with self.assertRaises(ValidationError):
            policy(rule(), pass_score=70, manual_score=70)

    def test_trials_are_sorted_unique_and_bounded(self) -> None:
        self.assertEqual(validate_trials(["zone-b", "zone-a"]), ("zone-a", "zone-b"))
        for values in ([], ["same", "same"], "zone-a"):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                validate_trials(values)  # type: ignore[arg-type]


class StatisticalTests(unittest.TestCase):
    def test_mann_whitney_identical_samples_are_not_significant(self) -> None:
        _, p_value = mann_whitney_two_sided([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])
        self.assertGreater(p_value, 0.9)

    def test_mann_whitney_separated_samples_are_significant(self) -> None:
        _, p_value = mann_whitney_two_sided(list(range(20)), list(range(30, 50)))
        self.assertLess(p_value, 0.001)

    def test_mann_whitney_all_ties_is_safe(self) -> None:
        _, p_value = mann_whitney_two_sided([1] * 10, [1] * 10)
        self.assertEqual(p_value, 1.0)

    def test_mann_whitney_rejects_empty_sample(self) -> None:
        with self.assertRaises(ValidationError):
            mann_whitney_two_sided([], [1])

    def test_benjamini_hochberg_preserves_order_and_monotonicity(self) -> None:
        adjusted = benjamini_hochberg([0.01, 0.04, 0.03])
        self.assertEqual([round(value, 2) for value in adjusted], [0.03, 0.04, 0.04])

    def test_lower_is_better_regression_fails_critical_metric(self) -> None:
        current = policy(rule())
        result = judge(
            current,
            ("a",),
            {("a", "latency"): (tuple(range(100, 120)), tuple(range(130, 150)))},
        )
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["comparisons"][0]["classification"], "REGRESSION")

    def test_higher_is_better_detects_drop(self) -> None:
        current = policy(
            rule(
                name="throughput",
                direction="higher",
                max_absolute_degradation=10,
                relative_floor=10,
            )
        )
        result = judge(
            current,
            ("a",),
            {("a", "throughput"): (tuple(range(100, 120)), tuple(range(50, 70)))},
        )
        self.assertEqual(result["verdict"], "FAIL")

    def test_either_direction_detects_any_large_shift(self) -> None:
        current = policy(rule(direction="either", max_absolute_degradation=2))
        result = judge(
            current,
            ("a",),
            {("a", "latency"): (tuple(range(100, 120)), tuple(range(70, 90)))},
        )
        self.assertEqual(result["verdict"], "FAIL")

    def test_effect_guardrail_prevents_tiny_significant_change(self) -> None:
        base = tuple(100 + index / 100 for index in range(100))
        canary = tuple(value + 1 for value in base)
        result = judge(policy(rule(min_samples=20)), ("a",), {("a", "latency"): (base, canary)})
        self.assertEqual(result["verdict"], "PASS")

    def test_improvement_is_pass_even_when_statistically_significant(self) -> None:
        result = judge(
            policy(rule()),
            ("a",),
            {("a", "latency"): (tuple(range(100, 120)), tuple(range(50, 70)))},
        )
        self.assertEqual(result["verdict"], "PASS")

    def test_missing_evidence_is_inconclusive(self) -> None:
        result = judge(policy(rule()), ("a",), {})
        self.assertEqual(result["verdict"], "INCONCLUSIVE")
        self.assertEqual(result["coverage"]["nodata"], 1)

    def test_excess_missing_values_are_inconclusive(self) -> None:
        values = (1.0, None, None, None, None, 2.0)
        result = judge(policy(rule()), ("a",), {("a", "latency"): (values, values)})
        self.assertEqual(result["verdict"], "INCONCLUSIVE")

    def test_zero_missing_strategy_can_make_error_counter_evaluable(self) -> None:
        current_rule = rule(
            missing_strategy="zero",
            max_missing_fraction=0.9,
            max_absolute_degradation=100,
        )
        values = (None, None, 1.0, 2.0, 3.0)
        result = judge(policy(current_rule), ("a",), {("a", "latency"): (values, values)})
        self.assertEqual(result["verdict"], "PASS")

    def test_one_of_four_noncritical_regressions_routes_to_manual(self) -> None:
        rules = tuple(
            rule(name=f"m{index}", critical=False, max_absolute_degradation=5)
            for index in range(4)
        )
        evidence = {}
        for index in range(4):
            canary = tuple(range(130, 150)) if index == 0 else tuple(range(100, 120))
            evidence[("a", f"m{index}")] = (tuple(range(100, 120)), canary)
        result = judge(policy(*rules), ("a",), evidence)
        self.assertEqual(result["score"], 75.0)
        self.assertEqual(result["verdict"], "MANUAL")

    def test_low_noncritical_score_fails(self) -> None:
        rules = tuple(
            rule(name=f"m{index}", critical=False, max_absolute_degradation=5)
            for index in range(4)
        )
        evidence = {
            ("a", f"m{index}"): (
                tuple(range(100, 120)),
                tuple(range(130, 150)) if index < 2 else tuple(range(100, 120)),
            )
            for index in range(4)
        }
        self.assertEqual(judge(policy(*rules), ("a",), evidence)["verdict"], "FAIL")


if __name__ == "__main__":
    unittest.main()
