"""Deterministic release scenarios and a local synthetic benchmark."""

from __future__ import annotations

import random
import tempfile
import time
from pathlib import Path
from statistics import median
from typing import Any

from .errors import IdempotencyConflict, IntegrityError
from .model import MetricRule, Policy
from .store import RolloutJudgeStore


DEMO_SECRET = b"rolloutjudge-demo-signing-secret-2026"
TRIALS = ("zone-a", "zone-b")


def default_policy() -> Policy:
    return Policy(
        metrics=(
            MetricRule(
                "latency_ms",
                "lower",
                group="reliability",
                critical=True,
                min_samples=30,
                max_relative_degradation=0.10,
                max_absolute_degradation=5.0,
                relative_floor=1.0,
            ),
            MetricRule(
                "error_rate",
                "lower",
                group="reliability",
                critical=True,
                min_samples=30,
                max_relative_degradation=0.25,
                max_absolute_degradation=0.005,
                relative_floor=0.01,
                missing_strategy="zero",
            ),
            MetricRule(
                "cpu_ratio",
                "lower",
                group="efficiency",
                min_samples=30,
                max_relative_degradation=0.15,
                max_absolute_degradation=0.05,
                relative_floor=0.1,
            ),
            MetricRule(
                "throughput_rps",
                "higher",
                group="capacity",
                min_samples=30,
                max_relative_degradation=0.08,
                max_absolute_degradation=50.0,
                relative_floor=100.0,
            ),
        ),
        pass_score=90.0,
        manual_score=70.0,
        false_discovery_rate=0.05,
    ).validated()


def _series(center: float, noise: float, seed: int, count: int = 60) -> list[float]:
    generator = random.Random(seed)
    return [round(generator.gauss(center, noise), 8) for _ in range(count)]


def _scenario(kind: str, trial_index: int) -> dict[str, tuple[list[float], list[float]]]:
    offset = trial_index * 100
    centers = {
        "safe": (102.0, 0.011, 0.61, 995.0),
        "failed": (128.0, 0.030, 0.56, 1005.0),
        "manual": (102.0, 0.011, 0.78, 995.0),
        "inconclusive": (102.0, 0.011, 0.61, 995.0),
    }
    latency, error, cpu, throughput = centers[kind]
    return {
        "latency_ms": (
            _series(100.0, 2.0, 10 + offset),
            _series(latency, 2.0, 11 + offset),
        ),
        "error_rate": (
            _series(0.010, 0.0015, 20 + offset),
            _series(error, 0.0015, 21 + offset),
        ),
        "cpu_ratio": (
            _series(0.60, 0.025, 30 + offset),
            _series(cpu, 0.025, 31 + offset),
        ),
        "throughput_rps": (
            _series(1000.0, 20.0, 40 + offset),
            _series(throughput, 20.0, 41 + offset),
        ),
    }


def _execute_scenario(
    store: RolloutJudgeStore,
    kind: str,
    *,
    service: str = "checkout",
) -> dict[str, Any]:
    run = store.start_run(
        service,
        f"release-{kind}",
        f"start-{kind}",
        "standard",
        TRIALS,
    )
    first_batch: tuple[str, str, list[float], list[float]] | None = None
    for trial_index, trial in enumerate(TRIALS):
        for metric, (baseline, canary) in _scenario(kind, trial_index).items():
            if kind == "inconclusive" and metric == "error_rate":
                continue
            batch_key = f"{kind}-{trial}-{metric}"
            store.ingest_evidence(
                service,
                run["run_id"],
                batch_key,
                trial,
                metric,
                baseline,
                canary,
            )
            if first_batch is None:
                first_batch = (batch_key, trial, baseline, canary)
    current = store.get_run(service, run["run_id"])
    judged = store.judge_run(
        service,
        run["run_id"],
        f"judge-{kind}",
        expected_version=current["version"],
    )
    return {"judged": judged, "first_batch": first_batch}


def run_demo(directory: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if directory is None:
        temporary = tempfile.TemporaryDirectory(prefix="rolloutjudge-demo-")
        directory = temporary.name
    root = Path(directory)
    store = RolloutJudgeStore(root / "rolloutjudge.sqlite", secret=DEMO_SECRET)
    policy = store.create_policy("standard", default_policy(), expected_version=0)

    safe = _execute_scenario(store, "safe")
    failed = _execute_scenario(store, "failed")
    manual = _execute_scenario(store, "manual")
    inconclusive = _execute_scenario(store, "inconclusive")

    # Replaying the judgment returns the exact archived report and receipt.
    safe_replay = store.judge_run(
        "checkout",
        safe["judged"]["run_id"],
        "judge-safe",
        expected_version=1,
    )

    # Demonstrate evidence idempotency on a fresh collecting run.
    replay_run = store.start_run(
        "catalog", "release-replay", "start-replay", "standard", ["zone-a"]
    )
    baseline, canary = _scenario("safe", 0)["latency_ms"]
    first = store.ingest_evidence(
        "catalog",
        replay_run["run_id"],
        "stable-batch",
        "zone-a",
        "latency_ms",
        baseline,
        canary,
    )
    replay = store.ingest_evidence(
        "catalog",
        replay_run["run_id"],
        "stable-batch",
        "zone-a",
        "latency_ms",
        baseline,
        canary,
    )
    changed_batch_rejected = False
    try:
        store.ingest_evidence(
            "catalog",
            replay_run["run_id"],
            "stable-batch",
            "zone-a",
            "latency_ms",
            baseline,
            [value + 100 for value in canary],
        )
    except IdempotencyConflict:
        changed_batch_rejected = True

    receipt = safe["judged"]["receipt"]
    receipt_valid = store.verify_receipt(receipt)["valid"]
    tampered = {
        "payload": {**receipt["payload"], "verdict": "FAIL"},
        "signature": receipt["signature"],
    }
    tampered_receipt_rejected = False
    try:
        store.verify_receipt(tampered)
    except IntegrityError:
        tampered_receipt_rejected = True

    def summary(execution: dict[str, Any]) -> dict[str, Any]:
        judged = execution["judged"]
        regressions = [
            {
                "metric": item["metric"],
                "trial": item["trial"],
            }
            for item in judged["report"]["comparisons"]
            if item["classification"] == "REGRESSION"
        ]
        return {
            "coverage": judged["report"]["coverage"],
            "reason": judged["reason"],
            "regressions": regressions,
            "score": judged["score"],
            "verdict": judged["verdict"],
        }

    report = {
        "evidence_idempotency": {
            "changed_batch_rejected": changed_batch_rejected,
            "first_replayed": first["replayed"],
            "same_batch_replayed": replay["replayed"],
        },
        "integrity": {
            "receipt_valid": receipt_valid,
            "tampered_receipt_rejected": tampered_receipt_rejected,
            "verification": store.verify_integrity(),
        },
        "judgment_replay": {
            "receipt_unchanged": safe_replay["receipt"] == receipt,
            "replayed": safe_replay["replayed"],
        },
        "policy": {
            "digest": policy["digest"],
            "metric_count": len(policy["config"]["metrics"]),
            "version": policy["version"],
        },
        "scenarios": {
            "failed": summary(failed),
            "inconclusive": summary(inconclusive),
            "manual": summary(manual),
            "safe": summary(safe),
        },
        "stats": store.stats(),
    }
    if temporary is not None:
        temporary.cleanup()
    return report


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * percentile))]


def run_benchmark(analysis_count: int = 100) -> dict[str, Any]:
    if isinstance(analysis_count, bool) or not isinstance(analysis_count, int):
        raise ValueError("analysis_count must be an integer")
    if not 1 <= analysis_count <= 500:
        raise ValueError("analysis_count must be between 1 and 500")
    with tempfile.TemporaryDirectory(prefix="rolloutjudge-benchmark-") as directory:
        store = RolloutJudgeStore(
            Path(directory) / "rolloutjudge.sqlite", secret=DEMO_SECRET
        )
        store.create_policy("standard", default_policy())
        judgment_latencies: list[float] = []
        passed = 0
        started = time.perf_counter()
        for index in range(analysis_count):
            service = f"service-{index % 10}"
            run = store.start_run(
                service,
                f"release-{index}",
                f"request-{index}",
                "standard",
                ["zone-a"],
            )
            for metric, (baseline, canary) in _scenario("safe", index).items():
                store.ingest_evidence(
                    service,
                    run["run_id"],
                    f"batch-{index}-{metric}",
                    "zone-a",
                    metric,
                    baseline,
                    canary,
                )
            current = store.get_run(service, run["run_id"])
            judgment_started = time.perf_counter()
            result = store.judge_run(
                service,
                run["run_id"],
                f"decision-{index}",
                expected_version=current["version"],
            )
            judgment_latencies.append((time.perf_counter() - judgment_started) * 1000)
            passed += result["verdict"] == "PASS"
        elapsed = time.perf_counter() - started
        return {
            "analyses": analysis_count,
            "end_to_end_analyses_per_second": round(analysis_count / elapsed, 2),
            "end_to_end_seconds": round(elapsed, 6),
            "judgment_latency_ms": {
                "p50": round(median(judgment_latencies), 6),
                "p95": round(_percentile(judgment_latencies, 0.95), 6),
            },
            "passed": passed,
            "verification": store.verify_integrity(),
        }
