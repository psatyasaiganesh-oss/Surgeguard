from __future__ import annotations

import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from .errors import IdempotencyConflict, IntegrityError
from .store import ClearFlowStore


DEMO_SECRET = b"clearflow-reproducible-demo-secret-32-bytes"
BASE_NS = 1_800_000_000_000_000_000
SECOND = 1_000_000_000


def policy(*, max_lag_seconds: int = 10) -> dict[str, Any]:
    return {
        "max_lag_ns": max_lag_seconds * SECOND,
        "clearing_grace_ns": 10 * SECOND,
        "max_missing_references": 0,
        "minimum_score": 100.0,
    }


def event(
    event_id: str,
    reference_id: str,
    postings: list[tuple[str, int]],
    occurred_ns: int,
    *,
    producer: str = "payments",
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "producer": producer,
        "reference_id": reference_id,
        "currency": "USD",
        "occurred_ns": occurred_ns,
        "postings": [
            {"account_id": account_id, "amount_minor": amount}
            for account_id, amount in postings
        ],
        "metadata": metadata or {},
    }


def create_accounts(store: ClearFlowStore, tenant: str, *pending: str) -> None:
    for account_id, kind in (
        ("network:cash", "terminal"),
        ("merchant:available", "terminal"),
        ("platform:fees", "terminal"),
        *((account, "clearing") for account in pending),
    ):
        store.create_account(tenant, account_id, kind, "USD", now_ns=BASE_NS)


def publish_flow_manifest(
    store: ClearFlowStore, tenant: str, flow: str, *, deadline_ns: int
) -> None:
    store.publish_manifest(
        tenant,
        f"manifest:{flow}",
        f"batch:{flow}",
        "payments",
        [f"{flow}:create", f"{flow}:release"],
        deadline_ns,
        now_ns=BASE_NS,
    )


def ingest_create(
    store: ClearFlowStore,
    tenant: str,
    flow: str,
    pending: str,
    *,
    received_ns: int,
) -> dict[str, Any]:
    return store.ingest_event(
        tenant,
        f"ingest:{flow}:create",
        event(
            f"evt:{flow}:create",
            f"{flow}:create",
            [("network:cash", -10_000), (pending, 10_000)],
            BASE_NS + SECOND,
        ),
        received_ns=received_ns,
    )


def ingest_release(
    store: ClearFlowStore,
    tenant: str,
    flow: str,
    pending: str,
    *,
    received_ns: int,
) -> dict[str, Any]:
    return store.ingest_event(
        tenant,
        f"ingest:{flow}:release",
        event(
            f"evt:{flow}:release",
            f"{flow}:release",
            [(pending, -10_000), ("merchant:available", 9_700), ("platform:fees", 300)],
            BASE_NS + 2 * SECOND,
        ),
        received_ns=received_ns,
    )


def run_demo(database: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if database is None:
        temporary = tempfile.TemporaryDirectory()
        database = Path(temporary.name) / "clearflow.sqlite"
    store = ClearFlowStore(database, secret=DEMO_SECRET)

    # Healthy: release arrives before create, yet identifier-based postings still clear.
    healthy = "healthy-shop"
    create_accounts(store, healthy, "pending:healthy")
    publish_flow_manifest(store, healthy, "healthy", deadline_ns=BASE_NS + 10 * SECOND)
    ingest_release(
        store,
        healthy,
        "healthy",
        "pending:healthy",
        received_ns=BASE_NS + 3 * SECOND,
    )
    ingest_create(
        store,
        healthy,
        "healthy",
        "pending:healthy",
        received_ns=BASE_NS + 4 * SECOND,
    )
    healthy_result = store.evaluate_quality(
        healthy,
        "quality:healthy",
        BASE_NS + 30 * SECOND,
        policy(),
        now_ns=BASE_NS + 30 * SECOND,
    )

    # Broken: release uses the wrong clearing account; all expected references still exist.
    broken = "broken-shop"
    create_accounts(store, broken, "pending:correct", "pending:wrong")
    publish_flow_manifest(store, broken, "broken", deadline_ns=BASE_NS + 10 * SECOND)
    ingest_create(
        store,
        broken,
        "broken",
        "pending:correct",
        received_ns=BASE_NS + 2 * SECOND,
    )
    broken_release = ingest_release(
        store,
        broken,
        "broken",
        "pending:wrong",
        received_ns=BASE_NS + 3 * SECOND,
    )
    broken_result = store.evaluate_quality(
        broken,
        "quality:broken",
        BASE_NS + 30 * SECOND,
        policy(),
        now_ns=BASE_NS + 30 * SECOND,
    )

    replacement = event(
        "evt:broken:release:corrected",
        "broken:release",
        [
            ("pending:correct", -10_000),
            ("merchant:available", 9_700),
            ("platform:fees", 300),
        ],
        BASE_NS + 2 * SECOND,
        metadata={"reason": "correct-clearing-dimension"},
    )
    proposal = store.propose_correction(
        broken,
        "proposal:broken-release",
        "engineer-a",
        broken_release["event_id"],
        replacement,
        now_ns=BASE_NS + 4 * SECOND,
    )
    self_approval_rejected = False
    try:
        store.approve_correction(
            broken,
            proposal["proposal_id"],
            "engineer-a",
            proposal["preview_digest"],
            now_ns=BASE_NS + 4 * SECOND,
        )
    except Exception:
        self_approval_rejected = True
    store.approve_correction(
        broken,
        proposal["proposal_id"],
        "reviewer-b",
        proposal["preview_digest"],
        now_ns=BASE_NS + 4 * SECOND,
    )
    approved = store.approve_correction(
        broken,
        proposal["proposal_id"],
        "reviewer-c",
        proposal["preview_digest"],
        now_ns=BASE_NS + 4 * SECOND,
    )
    applied = store.execute_correction(
        broken,
        proposal["proposal_id"],
        "operator-d",
        "execute:broken-release",
        proposal["base_revision"],
        now_ns=BASE_NS + 5 * SECOND,
    )
    applied_replay = store.execute_correction(
        broken,
        proposal["proposal_id"],
        "operator-d",
        "execute:broken-release",
        proposal["base_revision"],
        now_ns=BASE_NS + 6 * SECOND,
    )
    corrected_result = store.evaluate_quality(
        broken,
        "quality:corrected",
        BASE_NS + 30 * SECOND,
        policy(),
        now_ns=BASE_NS + 30 * SECOND,
    )

    missing = "missing-shop"
    create_accounts(store, missing, "pending:missing")
    publish_flow_manifest(store, missing, "missing", deadline_ns=BASE_NS + 10 * SECOND)
    ingest_create(
        store,
        missing,
        "missing",
        "pending:missing",
        received_ns=BASE_NS + 2 * SECOND,
    )
    missing_result = store.evaluate_quality(
        missing,
        "quality:missing",
        BASE_NS + 30 * SECOND,
        policy(),
        now_ns=BASE_NS + 30 * SECOND,
    )

    late = "late-shop"
    create_accounts(store, late, "pending:late")
    publish_flow_manifest(store, late, "late", deadline_ns=BASE_NS + 10 * SECOND)
    ingest_create(store, late, "late", "pending:late", received_ns=BASE_NS + 2 * SECOND)
    ingest_release(store, late, "late", "pending:late", received_ns=BASE_NS + 20 * SECOND)
    late_result = store.evaluate_quality(
        late,
        "quality:late",
        BASE_NS + 30 * SECOND,
        policy(),
        now_ns=BASE_NS + 30 * SECOND,
    )

    event_retry = ingest_create(
        store,
        healthy,
        "healthy",
        "pending:healthy",
        received_ns=BASE_NS + 4 * SECOND,
    )
    changed_retry_rejected = False
    try:
        store.ingest_event(
            healthy,
            "ingest:healthy:create",
            event(
                "evt:healthy:create",
                "healthy:create",
                [("network:cash", -9_999), ("pending:healthy", 9_999)],
                BASE_NS + SECOND,
            ),
            received_ns=BASE_NS + 4 * SECOND,
        )
    except IdempotencyConflict:
        changed_retry_rejected = True

    valid_attestation = store.verify_attestation(
        healthy_result["attestation"], expected_tenant=healthy
    )["valid"]
    tampered = json.loads(json.dumps(healthy_result["attestation"]))
    tampered["payload"]["verdict"] = "FAIL"
    tampered_rejected = False
    try:
        store.verify_attestation(tampered, expected_tenant=healthy)
    except IntegrityError:
        tampered_rejected = True

    integrity = store.verify_integrity()
    result = {
        "project": "ClearFlow",
        "scenarios": {
            "out_of_order_healthy": {
                "verdict": healthy_result["report"]["verdict"],
                "score": healthy_result["report"]["score"],
            },
            "wrong_dimension": {
                "before": broken_result["report"]["verdict"],
                "uncleared_accounts": broken_result["report"]["coverage"]["uncleared_accounts"],
                "after_correction": corrected_result["report"]["verdict"],
            },
            "missing_release": {
                "verdict": missing_result["report"]["verdict"],
                "missing_references": missing_result["report"]["coverage"]["missing_references"],
            },
            "late_release": {
                "verdict": late_result["report"]["verdict"],
                "late_events": late_result["report"]["coverage"]["late_events"],
            },
        },
        "correction": {
            "self_approval_rejected": self_approval_rejected,
            "independent_approvals": len(approved["approvals"]),
            "applied_revision": applied["revision"],
            "retry_returned_same_result": applied_replay["revision"] == applied["revision"]
            and applied_replay["replayed"],
        },
        "safety": {
            "event_retry_replayed": event_retry["replayed"],
            "changed_retry_rejected": changed_retry_rejected,
            "attestation_valid": valid_attestation,
            "tampered_attestation_rejected": tampered_rejected,
        },
        "integrity": integrity,
    }
    if temporary is not None:
        temporary.cleanup()
    return result


def run_benchmark(flows: int = 200) -> dict[str, Any]:
    if isinstance(flows, bool) or not isinstance(flows, int) or not 1 <= flows <= 2_000:
        raise ValueError("flows must be between 1 and 2,000")
    with tempfile.TemporaryDirectory() as directory:
        store = ClearFlowStore(Path(directory) / "benchmark.sqlite", secret=DEMO_SECRET)
        tenant = "benchmark-shop"
        store.create_account(tenant, "network:cash", "terminal", "USD", now_ns=BASE_NS)
        store.create_account(
            tenant, "merchant:available", "terminal", "USD", now_ns=BASE_NS
        )
        store.create_account(tenant, "platform:fees", "terminal", "USD", now_ns=BASE_NS)
        expected: list[str] = []
        latencies_ms: list[float] = []
        started = time.perf_counter()
        for index in range(flows):
            flow = f"bench-{index:05d}"
            pending = f"pending:{flow}"
            store.create_account(tenant, pending, "clearing", "USD", now_ns=BASE_NS)
            expected.extend([f"{flow}:create", f"{flow}:release"])
            operation_started = time.perf_counter()
            ingest_create(
                store,
                tenant,
                flow,
                pending,
                received_ns=BASE_NS + 2 * SECOND,
            )
            ingest_release(
                store,
                tenant,
                flow,
                pending,
                received_ns=BASE_NS + 3 * SECOND,
            )
            latencies_ms.append((time.perf_counter() - operation_started) * 1000)
        store.publish_manifest(
            tenant,
            "manifest:benchmark",
            "batch:benchmark",
            "payments",
            expected,
            BASE_NS + 10 * SECOND,
            now_ns=BASE_NS,
        )
        evaluation_started = time.perf_counter()
        quality = store.evaluate_quality(
            tenant,
            "quality:benchmark",
            BASE_NS + 30 * SECOND,
            policy(),
            now_ns=BASE_NS + 30 * SECOND,
        )
        evaluation_ms = (time.perf_counter() - evaluation_started) * 1000
        elapsed = time.perf_counter() - started
        integrity_started = time.perf_counter()
        integrity = store.verify_integrity(tenant)
        integrity_ms = (time.perf_counter() - integrity_started) * 1000
        ordered = sorted(latencies_ms)
        p95_index = max(0, int(0.95 * len(ordered)) - 1)
        return {
            "flows": flows,
            "events": flows * 2,
            "postings": flows * 5,
            "verdict": quality["report"]["verdict"],
            "score": quality["report"]["score"],
            "elapsed_seconds": round(elapsed, 6),
            "flows_per_second": round(flows / elapsed, 2),
            "two_event_flow_latency_ms": {
                "p50": round(statistics.median(ordered), 6),
                "p95": round(ordered[p95_index], 6),
            },
            "quality_evaluation_ms": round(evaluation_ms, 6),
            "integrity_scan_ms": round(integrity_ms, 6),
            "integrity": integrity,
        }
