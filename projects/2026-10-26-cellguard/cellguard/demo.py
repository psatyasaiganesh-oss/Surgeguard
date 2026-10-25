"""Deterministic isolation scenarios and local benchmark."""

from __future__ import annotations

import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from .errors import CapacityError, LeaseError, UnavailableError
from .model import Cell
from .store import CellGuardStore
from .topology import ShuffleTopology


DEMO_SECRET = b"cellguard-demo-lease-secret-material-2026"


def default_topology(*, capacity: int = 200) -> ShuffleTopology:
    cells = [
        Cell(f"cell-{index:02d}", f"zone-{index % 4}", capacity=capacity)
        for index in range(8)
    ]
    return ShuffleTopology(cells, shard_size=2, zone_spread=True)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * percentile))]


def _find_same_shard(topology: ShuffleTopology, tenant: str) -> str:
    target = topology.assign(tenant).cells
    for index in range(1, 100_000):
        candidate = f"peer-{index:05d}"
        if topology.assign(candidate).cells == target:
            return candidate
    raise RuntimeError("could not find a tenant sharing the target shuffle shard")


def run_demo(directory: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if directory is None:
        temporary = tempfile.TemporaryDirectory(prefix="cellguard-demo-")
        directory = temporary.name
    root = Path(directory)
    topology = default_topology()
    store = CellGuardStore(
        root / "cellguard.sqlite",
        topology,
        secret=DEMO_SECRET,
        default_queue_limit=12,
        default_inflight_limit=2,
        default_max_attempts=3,
    )

    tenants = [f"tenant-{index:04d}" for index in range(2400)]
    noisy = tenants[0]
    impact = topology.impact_report(tenants, noisy)
    impact["pooled_fully_impacted_fraction"] = 1.0
    impact["traditional_four_shard_fraction"] = 0.25
    impact["improvement_vs_traditional"] = round(
        0.25 / impact["fully_impacted_fraction"], 2
    )

    peer = _find_same_shard(topology, noisy)
    noisy_results = [
        store.submit(noisy, f"flood-{index}", {"kind": "flood", "index": index}, now_ns=10 + index)
        for index in range(12)
    ]
    tenant_limit_enforced = False
    try:
        store.submit(noisy, "flood-over-limit", {"kind": "flood"}, now_ns=30)
    except CapacityError:
        tenant_limit_enforced = True
    peer_results = [
        store.submit(peer, f"normal-{index}", {"kind": "normal", "index": index}, now_ns=40 + index)
        for index in range(4)
    ]

    routes: dict[str, Counter[str]] = defaultdict(Counter)
    for result in noisy_results + peer_results:
        routes[result.tenant][result.cell_id] += 1
    shared_cell = max(
        topology.assign(noisy).cells,
        key=lambda cell_id: min(routes[noisy][cell_id], routes[peer][cell_id]),
    )
    claims = store.claim(
        shared_cell, "worker-fair", limit=4, lease_ns=1_000_000, now_ns=100
    )
    fairness_tenants = [claim.tenant for claim in claims]
    for claim in claims:
        store.complete(
            claim.job_id,
            claim.lease_token,
            {"processed_by": "worker-fair"},
            now_ns=101,
        )

    assignment = store.assignment("failover-tenant")
    first_cell = store.cell(assignment.cells[0])
    store.set_cell_state(
        first_cell["cell_id"], "UNHEALTHY", expected_version=first_cell["version"], now_ns=200
    )
    failover = store.submit(
        "failover-tenant", "during-one-cell-failure", {"ok": True}, now_ns=201
    )
    second_cell = store.cell(assignment.cells[1])
    store.set_cell_state(
        second_cell["cell_id"], "UNHEALTHY", expected_version=second_cell["version"], now_ns=202
    )
    no_escape = False
    try:
        store.submit("failover-tenant", "both-failed", {"must_not_escape": True}, now_ns=203)
    except UnavailableError:
        no_escape = True
    for cell_id in assignment.cells:
        current = store.cell(cell_id)
        store.set_cell_state(
            cell_id, "HEALTHY", expected_version=current["version"], now_ns=204
        )

    # A separate tenant/cell scenario makes lease fencing easy to observe.
    lease_tenant = "lease-tenant"
    store.set_policy(
        lease_tenant,
        queue_limit=5,
        inflight_limit=1,
        max_attempts=3,
        expected_version=0,
        now_ns=300,
    )
    leased_job = store.submit(
        lease_tenant, "lease-recovery", {"work": 1}, priority=9, now_ns=301
    )
    first_claim = store.claim(
        leased_job.cell_id,
        "worker-old",
        limit=1,
        lease_ns=1_000_000,
        now_ns=302,
    )[0]
    store.reap_expired(now_ns=1_000_303)
    second_claim = store.claim(
        leased_job.cell_id,
        "worker-new",
        limit=1,
        lease_ns=1_000_000,
        now_ns=1_000_304,
    )[0]
    stale_completion_rejected = False
    try:
        store.complete(
            first_claim.job_id,
            first_claim.lease_token,
            {"worker": "old"},
            now_ns=1_000_305,
        )
    except LeaseError:
        stale_completion_rejected = True
    store.complete(
        second_claim.job_id,
        second_claim.lease_token,
        {"worker": "new"},
        now_ns=1_000_305,
    )

    report = {
        "blast_radius": impact,
        "cell_failover": {
            "assigned_cells": list(assignment.cells),
            "failed_cell_avoided": failover.cell_id != first_cell["cell_id"],
            "no_cross_shard_escape_when_all_assigned_cells_failed": no_escape,
        },
        "fair_admission": {
            "fair_claim_tenants": fairness_tenants,
            "normal_tenant_accepted": len(peer_results),
            "noisy_tenant_accepted": len(noisy_results),
            "noisy_tenant_limit_enforced": tenant_limit_enforced,
            "shared_cell": shared_cell,
        },
        "lease_recovery": {
            "fencing_advanced": second_claim.fencing_token > first_claim.fencing_token,
            "first_fencing_token": first_claim.fencing_token,
            "second_fencing_token": second_claim.fencing_token,
            "stale_completion_rejected": stale_completion_rejected,
        },
        "stats": store.stats(),
        "topology": topology.as_dict(),
        "verification": store.verify_integrity(),
    }
    if temporary is not None:
        temporary.cleanup()
    return report


def run_benchmark(job_count: int = 500, tenant_count: int = 100) -> dict[str, Any]:
    if isinstance(job_count, bool) or not isinstance(job_count, int) or not 1 <= job_count <= 10_000:
        raise ValueError("job_count must be between 1 and 10000")
    if (
        isinstance(tenant_count, bool)
        or not isinstance(tenant_count, int)
        or not 1 <= tenant_count <= job_count
    ):
        raise ValueError("tenant_count must be between 1 and job_count")
    with tempfile.TemporaryDirectory(prefix="cellguard-benchmark-") as directory:
        topology = default_topology(capacity=job_count)
        store = CellGuardStore(
            Path(directory) / "cellguard.sqlite",
            topology,
            secret=DEMO_SECRET,
            default_queue_limit=max(10, job_count),
            default_inflight_limit=100,
        )
        latencies_ms: list[float] = []
        started = time.perf_counter()
        for index in range(job_count):
            call_started = time.perf_counter()
            store.submit(
                f"bench-{index % tenant_count:04d}",
                f"job-{index:06d}",
                {"index": index},
                priority=index % 10,
            )
            latencies_ms.append((time.perf_counter() - call_started) * 1000)
        submit_seconds = time.perf_counter() - started

        processed = 0
        process_started = time.perf_counter()
        for cell in topology.cells:
            while True:
                claims = store.claim(
                    cell.cell_id,
                    f"worker-{cell.cell_id}",
                    limit=100,
                    lease_ns=10_000_000_000,
                )
                if not claims:
                    break
                for claim in claims:
                    store.complete(
                        claim.job_id, claim.lease_token, {"index": claim.payload["index"]}
                    )
                    processed += 1
        process_seconds = time.perf_counter() - process_started
        elapsed = time.perf_counter() - started
        verification = store.verify_integrity()
        return {
            "end_to_end_jobs_per_second": round(processed / elapsed, 2),
            "job_count": job_count,
            "processed": processed,
            "processing_seconds": round(process_seconds, 6),
            "submit_latency_ms": {
                "p50": round(median(latencies_ms), 6),
                "p95": round(_percentile(latencies_ms, 0.95), 6),
            },
            "submit_seconds": round(submit_seconds, 6),
            "tenant_count": tenant_count,
            "verification": verification,
        }
