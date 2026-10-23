"""Reproducible scenarios and a local microbenchmark for CacheFence."""

from __future__ import annotations

import concurrent.futures
import statistics
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from .cache import CacheStore
from .model import InvalidationEvent, IntegrityError, content_digest
from .origin import OriginStore
from .pipeline import deliver_once
from .probe import ConsistencyProbe
from .service import CacheAsideService

SECRET = b"cachefence-local-demo-key-32-bytes-minimum"
TENANT = "demo"


def _stack(root: Path, name: str) -> tuple[OriginStore, CacheStore, CacheAsideService]:
    folder = root / name
    folder.mkdir(parents=True, exist_ok=True)
    origin = OriginStore(folder / "origin.db")
    cache = CacheStore(folder / "cache.db")
    return origin, cache, CacheAsideService(origin, cache)


def _naive_race() -> dict[str, Any]:
    # Intentionally unsafe baseline matching the documented fill/invalidation order.
    source = {"version": 1, "value": 42}
    cache: dict[str, dict[str, int]] = {}
    in_flight_fill = dict(source)
    source = {"version": 2, "value": 43}
    cache.pop("x", None)
    cache["x"] = in_flight_fill
    return {
        "origin_version": source["version"],
        "cache_version": cache["x"]["version"],
        "stale": cache["x"]["version"] < source["version"],
    }


def _guarded_race(root: Path) -> dict[str, Any]:
    origin, cache, service = _stack(root, "guarded-race")
    old = origin.write(TENANT, "x", {"value": 42})
    deliver_once(origin, cache, secret=SECRET)
    cache.evict_value(TENANT, "x")

    def update_during_fill(captured: Any) -> None:
        assert captured.version == old.version
        origin.write(TENANT, "x", {"value": 43}, expected_version=old.version)
        deliver_once(origin, cache, secret=SECRET)

    result = service.get(TENANT, "x", after_origin_read=update_during_fill)
    inspection = cache.inspect(TENANT, "x")
    passed = bool(
        result.snapshot
        and result.snapshot.version == 2
        and result.snapshot.value == {"value": 43}
        and result.stale_fill_retried
    )
    return {
        "returned_version": result.snapshot.version if result.snapshot else None,
        "returned_value": result.snapshot.value if result.snapshot else None,
        "stale_fill_retried": result.stale_fill_retried,
        "fence_version": inspection["fence"]["version"],
        "stale_rejections": cache.metrics().get("stale_fills_rejected", 0),
        "passed": passed,
    }


def _eviction_fence(root: Path) -> dict[str, Any]:
    origin, cache, service = _stack(root, "eviction-fence")
    first = origin.write(TENANT, "account", {"plan": "basic"})
    deliver_once(origin, cache, secret=SECRET)
    service.get(TENANT, "account")
    origin.write(TENANT, "account", {"plan": "pro"}, expected_version=1)
    deliver_once(origin, cache, secret=SECRET)
    cache.evict_value(TENANT, "account")
    result = cache.install_fill(first, ttl_ns=10_000_000_000)
    inspection = cache.inspect(TENANT, "account")
    return {
        "fill_status": result.status,
        "fence_survived": inspection["fence"]["version"] == 2,
        "entry_absent": inspection["entry"] is None,
        "passed": result.status == "stale" and inspection["fence"]["version"] == 2,
    }


def _lost_ack(root: Path) -> dict[str, Any]:
    origin, cache, _ = _stack(root, "lost-ack")
    base = time.time_ns()
    origin.write(TENANT, "payment", {"state": "captured"}, now_ns=base)
    first = deliver_once(
        origin, cache, secret=SECRET, lease_ns=100, now_ns=base + 1, acknowledge=False
    )
    second = deliver_once(
        origin, cache, secret=SECRET, lease_ns=100, now_ns=base + 102
    )
    stats = origin.outbox_stats()
    return {
        "first": first.as_dict(),
        "retry": second.as_dict(),
        "outbox": stats,
        "passed": (
            first.applied == 1
            and first.acknowledged == 0
            and second.duplicates == 1
            and stats["delivered"] == 1
        ),
    }


def _same_version_integrity(root: Path) -> dict[str, Any]:
    origin, cache, _ = _stack(root, "integrity")
    snapshot = origin.write(TENANT, "profile", {"name": "Ada"})
    deliver_once(origin, cache, secret=SECRET)
    contradictory = InvalidationEvent(
        origin=origin.origin_id,
        tenant=TENANT,
        key="profile",
        version=snapshot.version,
        digest=content_digest(deleted=False, value={"name": "Mallory"}),
        deleted=False,
        created_ns=snapshot.updated_ns,
    )
    rejected = False
    try:
        cache.apply_invalidation(contradictory)
    except IntegrityError:
        rejected = True
    return {
        "rejected": rejected,
        "quarantine_incidents": cache.quarantine_count(),
        "passed": rejected and cache.quarantine_count() == 1,
    }


def _singleflight(root: Path, callers: int = 32) -> dict[str, Any]:
    origin, _, service = _stack(root, "singleflight")
    origin.write(TENANT, "catalog", {"items": 250})
    barrier = threading.Barrier(callers)

    def read() -> int:
        barrier.wait()
        result = service.get(TENANT, "catalog", source_delay_s=0.025)
        assert result.snapshot is not None
        return result.snapshot.version

    before = origin.metrics().get("reads", 0)
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=callers) as executor:
        versions = list(executor.map(lambda _: read(), range(callers)))
    elapsed_ms = (time.perf_counter() - started) * 1000
    reads = origin.metrics().get("reads", 0) - before
    return {
        "callers": callers,
        "origin_reads": reads,
        "all_version_one": all(version == 1 for version in versions),
        "elapsed_ms": round(elapsed_ms, 3),
        "passed": reads == 1 and all(version == 1 for version in versions),
    }


def _probe(root: Path) -> dict[str, Any]:
    origin, cache, service = _stack(root, "probe")
    probe = ConsistencyProbe(root / "probe" / "probe.db", origin, cache)
    base = time.time_ns()
    origin.write(TENANT, "settings", {"mode": "old"}, now_ns=base)
    deliver_once(origin, cache, secret=SECRET, now_ns=base + 1)
    service.get(TENANT, "settings", now_ns=base + 2)
    origin.write(
        TENANT, "settings", {"mode": "new"}, expected_version=1, now_ns=base + 3
    )
    retry = probe.observe(TENANT, "settings", report_after_ns=100, now_ns=base + 4)
    alert = probe.observe(TENANT, "settings", report_after_ns=100, now_ns=base + 105)
    deliver_once(origin, cache, secret=SECRET, now_ns=base + 106)
    resolved = probe.observe(TENANT, "settings", report_after_ns=100, now_ns=base + 107)
    return {
        "first": retry.as_dict(),
        "second": alert.as_dict(),
        "after_delivery": resolved.as_dict(),
        "passed": (
            retry.status == "retry" and alert.status == "alert" and resolved.status == "resolved"
        ),
    }


def _benchmark(root: Path, *, keys: int = 80, reads: int = 2_000) -> dict[str, Any]:
    origin, cache, service = _stack(root, "benchmark")
    for number in range(keys):
        origin.write(TENANT, f"item-{number}", {"number": number, "active": True})
    deliver_once(origin, cache, secret=SECRET, limit=keys)
    for number in range(keys):
        service.get(TENANT, f"item-{number}")
    before = origin.metrics().get("reads", 0)
    latencies_us: list[float] = []
    started = time.perf_counter()
    for number in range(reads):
        tick = time.perf_counter_ns()
        result = service.get(TENANT, f"item-{number % keys}")
        assert result.source == "cache"
        latencies_us.append((time.perf_counter_ns() - tick) / 1_000)
    elapsed = time.perf_counter() - started
    extra_origin_reads = origin.metrics().get("reads", 0) - before
    sorted_latencies = sorted(latencies_us)
    p95 = sorted_latencies[max(0, int(len(sorted_latencies) * 0.95) - 1)]
    return {
        "keys": keys,
        "warm_reads": reads,
        "origin_reads_during_warm_phase": extra_origin_reads,
        "throughput_reads_per_second": round(reads / elapsed, 2),
        "latency_us_p50": round(statistics.median(latencies_us), 2),
        "latency_us_p95": round(p95, 2),
        "elapsed_ms": round(elapsed * 1000, 2),
        "note": "local SQLite microbenchmark; not a production capacity claim",
    }


def run_demo(workdir: str | Path | None = None) -> dict[str, Any]:
    if workdir is None:
        with tempfile.TemporaryDirectory(prefix="cachefence-") as temporary:
            return run_demo(Path(temporary))
    root = Path(workdir)
    root.mkdir(parents=True, exist_ok=True)
    scenarios = {
        "naive_stale_fill": _naive_race(),
        "guarded_stale_fill": _guarded_race(root),
        "durable_eviction_fence": _eviction_fence(root),
        "lost_ack_retry": _lost_ack(root),
        "same_version_integrity": _same_version_integrity(root),
        "singleflight": _singleflight(root),
        "consistency_probe": _probe(root),
    }
    passed = all(value.get("passed", value.get("stale") is True) for value in scenarios.values())
    return {
        "project": "CacheFence",
        "passed": passed,
        "scenarios": scenarios,
        "benchmark": _benchmark(root),
    }
