"""Bounded telemetry; successful and failed latencies are kept separate."""

from collections import Counter, deque
import math


def percentiles(values) -> dict:
    data = sorted(values)
    if not data:
        return {"samples": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    def percentile(p):
        return round(data[max(0, math.ceil(p * len(data)) - 1)] * 1000, 3)
    return {"samples": len(data), "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99)}


class Metrics:
    def __init__(self, samples: int = 8192):
        self.counts = Counter()
        self.reasons = Counter()
        self.success_latency = deque(maxlen=samples)
        self.failure_latency = deque(maxlen=samples)
        self.max_active = 0

    def record(self, outcome) -> None:
        self.counts["completed"] += 1
        self.counts["succeeded" if outcome.ok else "failed"] += 1
        self.reasons[outcome.reason] += 1
        (self.success_latency if outcome.ok else self.failure_latency).append(outcome.elapsed_s)

    def snapshot(self) -> dict:
        keys = ("requests", "completed", "succeeded", "failed", "cancelled", "attempts", "retries", "budget_rejected", "bulkhead_rejected", "circuit_rejected", "transport_errors", "deadline_exhausted")
        return {"counts": {key: self.counts[key] for key in keys}, "reasons": dict(self.reasons), "max_active": self.max_active, "latency": {"success": percentiles(self.success_latency), "failure": percentiles(self.failure_latency)}, "latency_sample_capacity": self.success_latency.maxlen}
