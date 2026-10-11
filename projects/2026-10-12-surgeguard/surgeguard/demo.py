"""Measured comparisons against an independent local HTTP fault server."""

from __future__ import annotations

import asyncio
import platform
import random
import time
from dataclasses import asdict
from datetime import datetime, timezone

from .client import Policy, ResilientClient
from .controls import CircuitBreaker, RetryBudget
from .lab import FaultServer


async def workload(client: ResilientClient, url: str, count: int, concurrency: int):
    outcomes = []
    start = time.monotonic()
    async def worker(offset):
        for index in range(offset, count, concurrency):
            separator = "&" if "?" in url else "?"
            outcome = await client.fetch(f"{url}{separator}id={index}")
            outcomes.append(outcome)
    await asyncio.gather(*(worker(offset) for offset in range(min(count, concurrency))))
    elapsed = time.monotonic() - start
    return {"logical_requests": count, "completed_successfully": sum(outcome.ok for outcome in outcomes), "elapsed_seconds": round(elapsed, 6), "successful_requests_per_second": round(sum(outcome.ok for outcome in outcomes) / elapsed, 3), "metrics": client.snapshot()}


async def demo(count: int = 96, concurrency: int = 24, seed: int = 7):
    if type(count) is not int or not 1 <= count <= 10000:
        raise ValueError("count must be in [1, 10000]")
    if type(concurrency) is not int or not 1 <= concurrency <= 1024:
        raise ValueError("concurrency must be in [1, 1024]")
    report = {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(), "platform": platform.system(), "seed": seed, "count": count, "concurrency": concurrency, "scope": "Synthetic loopback HTTP experiment, not a production or company benchmark", "sources": ["https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/", "https://aws.amazon.com/builders-library/using-load-shedding-to-avoid-overload/"]}
    policy = Policy(total_timeout=2, attempt_timeout=.5, base_delay=.002, max_delay=.008, max_inflight=concurrency)
    retry_tokens = 12
    runs = {}
    for name, guarded in (("unbounded_retry_baseline", False), ("surgeguard", True)):
        async with FaultServer("outage") as server:
            configured = policy if guarded else Policy(**{**asdict(policy), "base_delay": 0, "max_delay": 0})
            client = ResilientClient(configured, budget=RetryBudget(retry_tokens, 0) if guarded else None, unbounded_retries=not guarded, rng=random.Random(seed))
            result = await workload(client, server.url, count, concurrency)
            result.update({"backend": server.snapshot(), "policy": asdict(configured), "retry_budget": {"capacity": retry_tokens, "refill_per_second": 0} if guarded else "unbounded"})
            runs[name] = result
    baseline = runs["unbounded_retry_baseline"]["backend"]["attempts_received"]
    protected = runs["surgeguard"]["backend"]["attempts_received"]
    report["persistent_outage"] = {**runs, "backend_attempt_reduction_percent": round(100 * (baseline - protected) / baseline, 3), "interpretation": "Fewer failed backend calls; both clients still fail every logical request during a total outage. Jitter and budget are enabled together, so this is not an isolated jitter performance claim."}
    async with FaultServer("transient") as server:
        client = ResilientClient(policy, budget=RetryBudget(count, 0), rng=random.Random(seed))
        report["transient_recovery"] = {**(await workload(client, server.url, count, concurrency)), "backend": server.snapshot(), "retry_budget": {"capacity": count, "refill_per_second": 0}, "interpretation": "Each ID fails once, then succeeds; this scenario provisions one retry token per request."}
    async with FaultServer("slow", delay=.03) as server:
        limit = min(8, count)
        client = ResilientClient(Policy(max_attempts=1, max_inflight=limit, total_timeout=2, attempt_timeout=.5))
        report["burst_load_shedding"] = {**(await workload(client, server.url, count, count)), "backend": server.snapshot(), "configured_max_inflight": limit, "interpretation": "Simultaneous burst; excess logical requests are deliberately rejected before network I/O."}
    async with FaultServer("outage") as server:
        circuit = CircuitBreaker(threshold=2, cooldown=.02)
        client = ResilientClient(Policy(max_attempts=1), circuit=circuit)
        first = await client.fetch(server.url)
        second = await client.fetch(server.url)
        rejected = await client.fetch(server.url)
        server.mode = "healthy"
        await asyncio.sleep(.03)
        probes = await asyncio.gather(*(client.fetch(server.url) for _ in range(16)))
        recovered = await client.fetch(server.url)
        report["optional_circuit_recovery"] = {"initial": [first.to_dict(), second.to_dict()], "open_rejection": rejected.to_dict(), "half_open_burst": [value.to_dict() for value in probes], "recovered": recovered.to_dict(), "backend": server.snapshot(), "metrics": client.snapshot(), "interpretation": "An optional breaker permits one recovery probe; default clients use budgets without a breaker."}
    checks = {
        "outage_budget_bound": protected <= count + retry_tokens,
        "outage_baseline_four_attempts": baseline == count * policy.max_attempts,
        "outage_no_false_success": runs["surgeguard"]["completed_successfully"] == 0,
        "transient_all_recovered": report["transient_recovery"]["completed_successfully"] == count,
        "bulkhead_bound": report["burst_load_shedding"]["backend"]["max_concurrent_handlers"] <= min(8, count),
        "bulkhead_shed_expected": report["burst_load_shedding"]["metrics"]["counts"]["bulkhead_rejected"] == count - min(8, count),
        "single_half_open_probe": sum(value.ok for value in probes) == 1,
        "circuit_recovered": recovered.ok and circuit.state == "closed",
    }
    report["invariants"] = checks
    if not all(checks.values()):
        raise AssertionError(f"Demo invariant failed: {checks}")
    return report
