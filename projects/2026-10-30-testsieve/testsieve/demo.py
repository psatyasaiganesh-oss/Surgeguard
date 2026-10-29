from __future__ import annotations

import hashlib
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from .errors import IntegrityError, ValidationError
from .store import TestSieveStore


DEMO_SECRET = b"testsieve-reproducible-demo-secret-32-bytes"
BASE_NS = 1_800_000_000_000_000_000
SECOND = 1_000_000_000


def file_digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def small_snapshot() -> dict[str, Any]:
    inventory = {
        "boot/runtime.py": file_digest("boot-v1"),
        "config/currency.json": file_digest("currency-v1"),
        "lib/payments.py": file_digest("payments-v1"),
        "lib/refunds.py": file_digest("refunds-v1"),
        "lib/risk.py": file_digest("risk-v1"),
        "tests/discovery_test.py": file_digest("discovery-test-v1"),
        "tests/payments_test.py": file_digest("payments-test-v1"),
        "tests/refunds_test.py": file_digest("refunds-test-v1"),
        "tests/risk_test.py": file_digest("risk-test-v1"),
        "tests/sticky_failure_test.py": file_digest("sticky-test-v1"),
    }
    tests = [
        {
            "test_id": "tests/discovery_test.py",
            "estimated_ms": 20,
            "mandatory": True,
            "last_status": "PASS",
        },
        {
            "test_id": "tests/payments_test.py",
            "estimated_ms": 120,
            "mandatory": False,
            "last_status": "PASS",
        },
        {
            "test_id": "tests/refunds_test.py",
            "estimated_ms": 100,
            "mandatory": False,
            "last_status": "PASS",
        },
        {
            "test_id": "tests/risk_test.py",
            "estimated_ms": 80,
            "mandatory": False,
            "last_status": "PASS",
        },
        {
            "test_id": "tests/sticky_failure_test.py",
            "estimated_ms": 60,
            "mandatory": False,
            "last_status": "FAIL",
        },
    ]
    observations = {
        "tests/discovery_test.py": ["tests/discovery_test.py"],
        "tests/payments_test.py": [
            "config/currency.json",
            "lib/payments.py",
            "tests/payments_test.py",
        ],
        "tests/refunds_test.py": [
            "config/currency.json",
            "lib/refunds.py",
            "tests/refunds_test.py",
        ],
        "tests/risk_test.py": ["lib/risk.py", "tests/risk_test.py"],
        "tests/sticky_failure_test.py": ["lib/risk.py", "tests/sticky_failure_test.py"],
    }
    return {
        "inventory": inventory,
        "tests": tests,
        "observations": observations,
        "root_files": ["boot/runtime.py"],
        "completed_commit": "commit-a",
    }


def changed_inventory(snapshot: dict[str, Any], path: str, label: str) -> dict[str, str]:
    result = dict(snapshot["inventory"])
    result[path] = file_digest(label)
    return result


def run_demo(database: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if database is None:
        temporary = tempfile.TemporaryDirectory()
        database = Path(temporary.name) / "testsieve.sqlite"
    store = TestSieveStore(database, secret=DEMO_SECRET)
    repository = "checkout-monorepo"
    snapshot = small_snapshot()
    baseline = store.publish_baseline(
        repository,
        "baseline:1",
        0,
        snapshot,
        now_ns=BASE_NS,
    )
    store.set_mode(repository, "SHADOW", now_ns=BASE_NS + SECOND)
    localized_inventory = changed_inventory(snapshot, "lib/payments.py", "payments-v2")

    clean_shadow_results = []
    for index in range(2):
        selection = store.select_tests(
            repository,
            f"shadow-select:{index}",
            localized_inventory,
            worker_count=2,
            now_ns=BASE_NS + (2 + index) * SECOND,
        )
        clean_shadow_results.append(
            store.record_shadow_result(
                repository,
                selection["selection_id"],
                f"shadow-result:{index}",
                [],
                now_ns=BASE_NS + (2 + index) * SECOND,
            )
        )
    store.set_mode(repository, "SELECT", now_ns=BASE_NS + 4 * SECOND)
    localized = store.select_tests(
        repository,
        "select:localized",
        localized_inventory,
        worker_count=2,
        now_ns=BASE_NS + 5 * SECOND,
    )
    localized_replay = store.select_tests(
        repository,
        "select:localized",
        localized_inventory,
        worker_count=2,
        now_ns=BASE_NS + 6 * SECOND,
    )

    root_inventory = changed_inventory(snapshot, "boot/runtime.py", "boot-v2")
    root_selection = store.select_tests(
        repository,
        "select:root",
        root_inventory,
        worker_count=3,
        now_ns=BASE_NS + 6 * SECOND,
    )
    unknown_inventory = dict(snapshot["inventory"])
    unknown_inventory["generated/new_schema.json"] = file_digest("new-schema")
    unknown_selection = store.select_tests(
        repository,
        "select:unknown",
        unknown_inventory,
        worker_count=3,
        now_ns=BASE_NS + 7 * SECOND,
    )

    manifest_valid = store.verify_manifest(
        localized["manifest"], expected_repository=repository
    )["valid"]
    tampered = json.loads(json.dumps(localized["manifest"]))
    tampered["payload"]["mode"] = "RECORD"
    tampered_rejected = False
    try:
        store.verify_manifest(tampered, expected_repository=repository)
    except IntegrityError:
        tampered_rejected = True

    store.set_mode(repository, "SHADOW", now_ns=BASE_NS + 8 * SECOND)
    unsafe_selection = store.select_tests(
        repository,
        "shadow-select:unsafe",
        localized_inventory,
        worker_count=2,
        now_ns=BASE_NS + 9 * SECOND,
    )
    unsafe_result = store.record_shadow_result(
        repository,
        unsafe_selection["selection_id"],
        "shadow-result:unsafe",
        ["tests/refunds_test.py"],
        now_ns=BASE_NS + 9 * SECOND,
    )
    quarantined_selection = store.select_tests(
        repository,
        "select:after-quarantine",
        localized_inventory,
        worker_count=2,
        now_ns=BASE_NS + 10 * SECOND,
    )
    integrity = store.verify_integrity(repository)
    result = {
        "project": "TestSieve",
        "baseline": baseline,
        "clean_shadow_runs": len(clean_shadow_results),
        "localized_selection": {
            "total_tests": localized["decision"]["all_test_count"],
            "candidate_tests": localized["decision"]["candidate_tests"],
            "run_tests": localized["decision"]["run_tests"],
            "selection_fraction": localized["decision"]["selection_fraction"],
            "replay_returned_same_selection": localized_replay["replayed"]
            and localized_replay["selection_id"] == localized["selection_id"],
        },
        "guardrails": {
            "root_change_runs_full_suite": root_selection["decision"]["run_test_count"]
            == root_selection["decision"]["all_test_count"],
            "unknown_file_runs_full_suite": unknown_selection["decision"]["run_test_count"]
            == unknown_selection["decision"]["all_test_count"],
            "previous_failure_selected": "tests/sticky_failure_test.py"
            in localized["decision"]["run_tests"],
            "mandatory_test_selected": "tests/discovery_test.py"
            in localized["decision"]["run_tests"],
        },
        "shadow_miss": {
            "safe": unsafe_result["safe"],
            "missed_failures": unsafe_result["missed_failures"],
            "baseline_status": unsafe_result["baseline_status"],
            "next_run_full_suite": quarantined_selection["decision"]["run_test_count"]
            == quarantined_selection["decision"]["all_test_count"],
        },
        "security": {
            "manifest_valid": manifest_valid,
            "tampered_manifest_rejected": tampered_rejected,
        },
        "integrity": integrity,
    }
    if temporary is not None:
        temporary.cleanup()
    return result


def benchmark_snapshot(test_count: int = 1_000, file_count: int = 250) -> dict[str, Any]:
    inventory = {
        f"src/module_{index:04d}.py": file_digest(f"module-{index}-v1")
        for index in range(file_count)
    }
    inventory["boot/runtime.py"] = file_digest("runtime-v1")
    tests = []
    observations = {}
    for index in range(test_count):
        test_id = f"tests/test_{index:05d}.py"
        test_path = test_id
        inventory[test_path] = file_digest(f"test-{index}-v1")
        module = f"src/module_{index % file_count:04d}.py"
        tests.append(
            {
                "test_id": test_id,
                "estimated_ms": 5 + (index % 97),
                "mandatory": index < 5,
                "last_status": "FAIL" if 5 <= index < 10 else "PASS",
            }
        )
        observations[test_id] = [module, test_path]
    return {
        "inventory": inventory,
        "tests": tests,
        "observations": observations,
        "root_files": ["boot/runtime.py"],
        "completed_commit": "benchmark-base",
    }


def run_benchmark(selections: int = 200) -> dict[str, Any]:
    if isinstance(selections, bool) or not isinstance(selections, int) or not 1 <= selections <= 1_000:
        raise ValidationError("selections must be between 1 and 1,000")
    with tempfile.TemporaryDirectory() as directory:
        store = TestSieveStore(Path(directory) / "benchmark.sqlite", secret=DEMO_SECRET)
        repository = "benchmark-repo"
        snapshot = benchmark_snapshot()
        store.publish_baseline(repository, "baseline", 0, snapshot, now_ns=BASE_NS)
        store.set_mode(repository, "SHADOW", now_ns=BASE_NS + SECOND)
        current = dict(snapshot["inventory"])
        changed_modules = [f"src/module_{index:04d}.py" for index in range(4)]
        for path in changed_modules:
            current[path] = file_digest(f"{path}-v2")
        for index in range(2):
            shadow = store.select_tests(
                repository,
                f"shadow:{index}",
                current,
                worker_count=8,
                now_ns=BASE_NS + (2 + index) * SECOND,
            )
            store.record_shadow_result(
                repository,
                shadow["selection_id"],
                f"shadow-result:{index}",
                [],
                now_ns=BASE_NS + (2 + index) * SECOND,
            )
        store.set_mode(repository, "SELECT", now_ns=BASE_NS + 4 * SECOND)
        latencies_ms: list[float] = []
        last = None
        started = time.perf_counter()
        for index in range(selections):
            operation_started = time.perf_counter()
            last = store.select_tests(
                repository,
                f"selection:{index}",
                current,
                worker_count=8,
                now_ns=BASE_NS + (5 + index) * SECOND,
            )
            latencies_ms.append((time.perf_counter() - operation_started) * 1000)
        elapsed = time.perf_counter() - started
        assert last is not None
        integrity_started = time.perf_counter()
        integrity = store.verify_integrity(repository)
        integrity_ms = (time.perf_counter() - integrity_started) * 1000
        ordered = sorted(latencies_ms)
        p95_index = max(0, int(0.95 * len(ordered)) - 1)
        worker_loads = [item["estimated_ms"] for item in last["decision"]["worker_plan"]]
        return {
            "suite_tests": 1_000,
            "inventory_files": len(snapshot["inventory"]),
            "changed_files": len(changed_modules),
            "selection_requests": selections,
            "candidate_tests": last["decision"]["candidate_test_count"],
            "selection_fraction": last["decision"]["selection_fraction"],
            "requests_per_second": round(selections / elapsed, 2),
            "selection_latency_ms": {
                "p50": round(statistics.median(ordered), 6),
                "p95": round(ordered[p95_index], 6),
            },
            "worker_estimated_ms": worker_loads,
            "worker_load_spread_ms": round(max(worker_loads) - min(worker_loads), 6),
            "integrity_scan_ms": round(integrity_ms, 6),
            "integrity": integrity,
        }
