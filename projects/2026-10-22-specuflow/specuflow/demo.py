"""End-to-end demonstration and locally measured benchmark."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any

from .model import canonical
from .runner import make_job, run_job
from .store import NondeterminismError, Scheduler


def _output(shard_id: str, value: Any) -> bytes:
    return canonical({"shard_id": shard_id, "value": value}).encode("utf-8")


def _single_job(job_id: str, lease_ms: int = 100) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "shards": [{"shard_id": "only", "payload": {"value": 7}}],
        "policy": {
            "lease_ms": lease_ms,
            "speculate_after_ms": 40,
            "max_attempts": 3,
            "max_speculative_attempts": 1,
        },
    }


def run_demo() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="specuflow-") as directory:
        root = Path(directory)
        scheduler = Scheduler(root / "scheduler.db", root / "blobs")

        baseline_spec = make_job("baseline")
        speculative_spec = make_job("speculative")
        scheduler.submit(baseline_spec)
        baseline = run_job(scheduler, "baseline", workers=4, allow_speculation=False)
        scheduler.submit(speculative_spec)
        speculative = run_job(scheduler, "speculative", workers=4, allow_speculation=True)
        improvement = (
            (baseline["completion_seconds"] - speculative["completion_seconds"])
            / baseline["completion_seconds"]
            * 100
        )
        outputs_match = all(
            json.loads(scheduler.output("baseline", f"shard-{index:04d}"))["value"]
            == json.loads(scheduler.output("speculative", f"shard-{index:04d}"))["value"]
            for index in range(16)
        )

        scheduler.submit(_single_job("lease-recovery"), now_ms=1_000)
        abandoned = scheduler.claim(
            "lease-recovery", "crashed-worker", allow_speculation=False, now_ms=1_000
        )
        restarted = Scheduler(root / "scheduler.db", root / "blobs")
        early_claim = restarted.claim(
            "lease-recovery", "recovery-worker", allow_speculation=False, now_ms=1_099
        )
        recovered = restarted.claim(
            "lease-recovery", "recovery-worker", allow_speculation=False, now_ms=1_101
        )
        restarted.complete(
            recovered["attempt_id"],
            recovered["capability_token"],
            _output("only", {"answer": 7}),
            {"records": 1},
            now_ms=1_102,
        )

        scheduler.submit(_single_job("commit-race", lease_ms=500), now_ms=2_000)
        primary = scheduler.claim("commit-race", "slow", now_ms=2_000)
        backup = scheduler.claim("commit-race", "fast", now_ms=2_050)
        backup_result = scheduler.complete(
            backup["attempt_id"],
            backup["capability_token"],
            _output("only", {"answer": 42}),
            {"records": 1, "answers": 1},
            now_ms=2_060,
        )
        duplicate_result = scheduler.complete(
            primary["attempt_id"],
            primary["capability_token"],
            _output("only", {"answer": 42}),
            {"records": 1, "answers": 1},
            now_ms=2_070,
        )

        scheduler.submit(_single_job("nondeterministic", lease_ms=500), now_ms=3_000)
        first = scheduler.claim("nondeterministic", "slow", now_ms=3_000)
        second = scheduler.claim("nondeterministic", "fast", now_ms=3_050)
        scheduler.complete(
            second["attempt_id"],
            second["capability_token"],
            _output("only", {"answer": 1}),
            {"records": 1},
            now_ms=3_060,
        )
        nondeterminism_detected = False
        try:
            scheduler.complete(
                first["attempt_id"],
                first["capability_token"],
                _output("only", {"answer": 2}),
                {"records": 1},
                now_ms=3_070,
            )
        except NondeterminismError:
            nondeterminism_detected = True

        benchmark_spec = make_job(
            "throughput",
            shards=200,
            straggler_delay_ms=0,
            regular_delay_ms=0,
            backup_delay_ms=0,
            speculate_after_ms=100,
        )
        scheduler.submit(benchmark_spec)
        benchmark_started = time.perf_counter()
        benchmark = run_job(
            scheduler, "throughput", workers=8, allow_speculation=False
        )
        benchmark_elapsed = time.perf_counter() - benchmark_started

        return {
            "straggler_experiment": {
                "shards": 16,
                "workers": 4,
                "baseline_completion_seconds": baseline["completion_seconds"],
                "speculative_completion_seconds": speculative["completion_seconds"],
                "measured_completion_improvement_percent": round(improvement, 2),
                "speculative_attempts": speculative["speculative_attempts"],
                "outputs_match": outputs_match,
                "exactly_once_counters": speculative["counters"] == {"records": 16, "words": 112},
            },
            "lease_recovery": {
                "abandoned_attempt": abandoned["attempt_id"],
                "claim_before_expiry": early_claim is not None,
                "retry_kind": recovered["kind"],
                "state_after_restart": restarted.status("lease-recovery")["state"],
            },
            "commit_race": {
                "backup_committed": backup_result["committed"],
                "primary_duplicate_suppressed": duplicate_result["duplicate"],
                "logical_counter_records": scheduler.status("commit-race")["counters"]["records"],
                "attempts": len(scheduler.attempts("commit-race")),
            },
            "nondeterminism": {
                "detected": nondeterminism_detected,
                "job_state": scheduler.status("nondeterministic")["state"],
                "error_code": scheduler.status("nondeterministic")["error_code"],
            },
            "integrity": scheduler.verify("speculative"),
            "throughput_benchmark": {
                "tasks": benchmark["tasks"],
                "workers": 8,
                "completion_seconds": benchmark["completion_seconds"],
                "end_to_end_seconds": round(benchmark_elapsed, 6),
                "tasks_per_second": round(benchmark["tasks"] / benchmark_elapsed, 2),
            },
            "scope": "local threads, SQLite, and synthetic delays; not Google infrastructure or production capacity",
        }


def main() -> None:
    print(json.dumps(run_demo(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
