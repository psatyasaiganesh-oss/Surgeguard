"""Deterministic statistical comparison and rollout judgment."""

from __future__ import annotations

import math
from collections import defaultdict
from statistics import median
from typing import Any, Mapping

from .errors import ValidationError
from .model import MetricRule, Policy
from .model import validate_trials


Evidence = Mapping[tuple[str, str], tuple[tuple[float | None, ...], tuple[float | None, ...]]]


def _clean(
    values: tuple[float | None, ...], strategy: str
) -> tuple[list[float], float]:
    missing = sum(value is None for value in values)
    if strategy == "zero":
        cleaned = [0.0 if value is None else value for value in values]
    else:
        cleaned = [value for value in values if value is not None]
    return cleaned, missing / len(values)


def mann_whitney_two_sided(left: list[float], right: list[float]) -> tuple[float, float]:
    """Return U for left and a tie-corrected normal-approximation p-value."""
    if not left or not right:
        raise ValidationError("Mann-Whitney samples must both be non-empty")
    n_left = len(left)
    n_right = len(right)
    combined = [(value, 0) for value in left] + [(value, 1) for value in right]
    combined.sort(key=lambda item: item[0])
    rank_sum_left = 0.0
    tie_sum = 0
    index = 0
    while index < len(combined):
        end = index + 1
        while end < len(combined) and combined[end][0] == combined[index][0]:
            end += 1
        average_rank = ((index + 1) + end) / 2
        rank_sum_left += average_rank * sum(
            combined[position][1] == 0 for position in range(index, end)
        )
        tie_size = end - index
        tie_sum += tie_size**3 - tie_size
        index = end
    u_left = rank_sum_left - n_left * (n_left + 1) / 2
    total = n_left + n_right
    mean = n_left * n_right / 2
    if total <= 1:
        return u_left, 1.0
    variance = (n_left * n_right / 12) * (
        total + 1 - tie_sum / (total * (total - 1))
    )
    if variance <= 0:
        return u_left, 1.0
    corrected_distance = max(0.0, abs(u_left - mean) - 0.5)
    z_score = corrected_distance / math.sqrt(variance)
    return u_left, min(1.0, math.erfc(z_score / math.sqrt(2)))


def benjamini_hochberg(p_values: list[float]) -> list[float]:
    if not p_values:
        return []
    count = len(p_values)
    order = sorted(range(count), key=lambda index: p_values[index])
    adjusted = [1.0] * count
    running = 1.0
    for reverse_index in range(count - 1, -1, -1):
        original_index = order[reverse_index]
        rank = reverse_index + 1
        running = min(running, p_values[original_index] * count / rank)
        adjusted[original_index] = min(1.0, running)
    return adjusted


def _comparison(
    trial: str,
    rule: MetricRule,
    baseline_raw: tuple[float | None, ...],
    canary_raw: tuple[float | None, ...],
) -> dict[str, Any]:
    baseline, baseline_missing = _clean(baseline_raw, rule.missing_strategy)
    canary, canary_missing = _clean(canary_raw, rule.missing_strategy)
    result: dict[str, Any] = {
        "alpha": rule.alpha,
        "baseline_missing_fraction": round(baseline_missing, 8),
        "baseline_samples": len(baseline),
        "canary_missing_fraction": round(canary_missing, 8),
        "canary_samples": len(canary),
        "classification": "NODATA",
        "critical": rule.critical,
        "direction": rule.direction,
        "group": rule.group,
        "metric": rule.name,
        "trial": trial,
        "weight": rule.weight,
    }
    if (
        len(baseline) < rule.min_samples
        or len(canary) < rule.min_samples
        or baseline_missing > rule.max_missing_fraction
        or canary_missing > rule.max_missing_fraction
    ):
        result["reason"] = "insufficient_or_incomplete_samples"
        return result
    baseline_median = float(median(baseline))
    canary_median = float(median(canary))
    signed_delta = canary_median - baseline_median
    if rule.direction == "lower":
        degradation = signed_delta
    elif rule.direction == "higher":
        degradation = -signed_delta
    else:
        degradation = abs(signed_delta)
    relative_degradation = degradation / max(abs(baseline_median), rule.relative_floor)
    u_statistic, p_value = mann_whitney_two_sided(baseline, canary)
    result.update(
        {
            "baseline_median": baseline_median,
            "canary_median": canary_median,
            "degradation": degradation,
            "max_absolute_degradation": rule.max_absolute_degradation,
            "max_relative_degradation": rule.max_relative_degradation,
            "p_value": p_value,
            "relative_degradation": relative_degradation,
            "signed_delta": signed_delta,
            "u_statistic": u_statistic,
        }
    )
    return result


def judge(
    policy: Policy,
    expected_trials: tuple[str, ...],
    evidence: Evidence,
) -> dict[str, Any]:
    policy = policy.validated()
    expected_trials = validate_trials(expected_trials)
    comparisons: list[dict[str, Any]] = []
    rules = {rule.name: rule for rule in policy.metrics}
    for trial in expected_trials:
        for rule in policy.metrics:
            series = evidence.get((trial, rule.name))
            if series is None:
                comparisons.append(
                    {
                        "alpha": rule.alpha,
                        "baseline_missing_fraction": 1.0,
                        "baseline_samples": 0,
                        "canary_missing_fraction": 1.0,
                        "canary_samples": 0,
                        "classification": "NODATA",
                        "critical": rule.critical,
                        "direction": rule.direction,
                        "group": rule.group,
                        "metric": rule.name,
                        "reason": "evidence_not_ingested",
                        "trial": trial,
                        "weight": rule.weight,
                    }
                )
            else:
                comparisons.append(_comparison(trial, rule, series[0], series[1]))

    statistical = [entry for entry in comparisons if "p_value" in entry]
    adjusted = benjamini_hochberg([entry["p_value"] for entry in statistical])
    for entry, adjusted_p in zip(statistical, adjusted, strict=True):
        rule = rules[entry["metric"]]
        entry["adjusted_p_value"] = round(adjusted_p, 12)
        is_regression = (
            adjusted_p <= min(rule.alpha, policy.false_discovery_rate)
            and entry["degradation"] > rule.max_absolute_degradation
            and entry["relative_degradation"] > rule.max_relative_degradation
        )
        entry["classification"] = "REGRESSION" if is_regression else "PASS"
        for field in (
            "baseline_median",
            "canary_median",
            "degradation",
            "relative_degradation",
            "signed_delta",
        ):
            entry[field] = round(entry[field], 10)
        entry["p_value"] = round(entry["p_value"], 12)
        entry["u_statistic"] = round(entry["u_statistic"], 6)

    evaluable = [entry for entry in comparisons if entry["classification"] != "NODATA"]
    pass_weight = sum(
        entry["weight"] for entry in evaluable if entry["classification"] == "PASS"
    )
    total_weight = sum(entry["weight"] for entry in evaluable)
    score = 0.0 if total_weight == 0 else 100 * pass_weight / total_weight
    critical_regressions = [
        entry
        for entry in comparisons
        if entry["critical"] and entry["classification"] == "REGRESSION"
    ]
    no_data = [entry for entry in comparisons if entry["classification"] == "NODATA"]
    if critical_regressions or (total_weight and score < policy.manual_score):
        verdict = "FAIL"
        reason = "critical_regression" if critical_regressions else "score_below_manual_threshold"
    elif no_data:
        verdict = "INCONCLUSIVE"
        reason = "required_evidence_missing_or_incomplete"
    elif score >= policy.pass_score:
        verdict = "PASS"
        reason = "score_at_or_above_pass_threshold"
    else:
        verdict = "MANUAL"
        reason = "score_requires_manual_review"

    groups: dict[str, dict[str, int]] = defaultdict(
        lambda: {"nodata": 0, "pass": 0, "regression": 0}
    )
    for entry in comparisons:
        groups[entry["group"]][entry["classification"].lower()] += 1
    return {
        "comparisons": comparisons,
        "coverage": {
            "evaluable": len(evaluable),
            "expected": len(comparisons),
            "nodata": len(no_data),
        },
        "group_counts": dict(sorted(groups.items())),
        "reason": reason,
        "score": round(score, 6),
        "thresholds": {
            "false_discovery_rate": policy.false_discovery_rate,
            "manual_score": policy.manual_score,
            "pass_score": policy.pass_score,
        },
        "verdict": verdict,
    }
