"""Local worker pool used to exercise the durable scheduler with real races."""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

from .model import ValidationError, canonical, closed, integer
from .store import NondeterminismError, Scheduler

WORD_RE = re.compile(r"[A-Za-z0-9']+")


def synthetic_compute(claim: dict[str, Any]) -> tuple[bytes, dict[str, int]]:
    """Deterministic word count with attempt-specific injected latency."""
    payload = closed(
        claim["payload"],
        {"text", "primary_delay_ms", "backup_delay_ms"},
        "synthetic payload",
    )
    if not isinstance(payload["text"], str) or len(payload["text"]) > 20_000:
        raise ValidationError("synthetic text must be a string of at most 20000 characters")
    primary_delay = integer(payload["primary_delay_ms"], "primary_delay_ms", 0, 5_000)
    backup_delay = integer(payload["backup_delay_ms"], "backup_delay_ms", 0, 5_000)
    delay = backup_delay if claim["kind"] == "SPECULATIVE" else primary_delay
    time.sleep(delay / 1_000)
    words: dict[str, int] = {}
    for word in WORD_RE.findall(payload["text"].lower()):
        words[word] = words.get(word, 0) + 1
    output = canonical({"shard_id": claim["shard_id"], "value": words}).encode("utf-8")
    return output, {"records": 1, "words": sum(words.values())}


def make_job(
    job_id: str,
    *,
    shards: int = 16,
    straggler_delay_ms: int = 320,
    regular_delay_ms: int = 12,
    backup_delay_ms: int = 5,
    lease_ms: int = 2_000,
    speculate_after_ms: int = 40,
) -> dict[str, Any]:
    data = []
    for index in range(shards):
        delay = straggler_delay_ms if index == 0 else regular_delay_ms
        data.append(
            {
                "shard_id": f"shard-{index:04d}",
                "payload": {
                    "text": f"distributed systems make shard {index} reliable reliable",
                    "primary_delay_ms": delay,
                    "backup_delay_ms": backup_delay_ms,
                },
            }
        )
    return {
        "job_id": job_id,
        "shards": data,
        "policy": {
            "lease_ms": lease_ms,
            "speculate_after_ms": speculate_after_ms,
            "max_attempts": 3,
            "max_speculative_attempts": 1,
        },
    }


def run_job(
    scheduler: Scheduler,
    job_id: str,
    *,
    workers: int = 4,
    allow_speculation: bool = True,
    compute: Callable[[dict[str, Any]], tuple[bytes, dict[str, int]]] = synthetic_compute,
) -> dict[str, Any]:
    if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 32:
        raise ValidationError("workers must be an integer in [1, 32]")
    started = time.perf_counter()
    first_terminal: float | None = None
    active: dict[Future[tuple[bytes, dict[str, int]]], tuple[dict[str, Any], str]] = {}
    available = [f"worker-{index:02d}" for index in range(workers)]
    completion_errors: list[str] = []

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="specuflow") as pool:
        while True:
            for future in list(active):
                if not future.done():
                    continue
                claim, worker_id = active.pop(future)
                available.append(worker_id)
                try:
                    output, counters = future.result()
                    scheduler.complete(
                        claim["attempt_id"], claim["capability_token"], output, counters
                    )
                except NondeterminismError as exc:
                    completion_errors.append(type(exc).__name__)
                except Exception as exc:
                    scheduler.fail_attempt(
                        claim["attempt_id"], claim["capability_token"], "WORKER_FAILURE"
                    )
                    completion_errors.append(type(exc).__name__)

            status = scheduler.status(job_id)
            if status["state"] in {"SUCCEEDED", "FAILED"} and first_terminal is None:
                first_terminal = time.perf_counter()

            if status["state"] == "RUNNING":
                for worker_id in list(available):
                    claim = scheduler.claim(
                        job_id, worker_id, allow_speculation=allow_speculation
                    )
                    if claim is None:
                        continue
                    available.remove(worker_id)
                    future = pool.submit(compute, claim)
                    active[future] = (claim, worker_id)

            if first_terminal is not None and not active:
                break
            if status["state"] == "FAILED" and not active:
                break
            time.sleep(0.001)

    finished = time.perf_counter()
    final = scheduler.status(job_id)
    attempts = scheduler.attempts(job_id)
    return {
        "state": final["state"],
        "error_code": final["error_code"],
        "tasks": final["total_tasks"],
        "attempts": len(attempts),
        "speculative_attempts": sum(item["kind"] == "SPECULATIVE" for item in attempts),
        "completion_seconds": round((first_terminal or finished) - started, 6),
        "wall_seconds_after_loser_cleanup": round(finished - started, 6),
        "counters": final["counters"],
        "manifest_digest": final["manifest_digest"],
        "completion_errors": completion_errors,
    }
