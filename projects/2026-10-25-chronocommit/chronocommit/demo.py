"""Reproducible failure demonstration and local benchmark."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from statistics import median
from typing import Any

from .clock import ManualBoundedClock
from .coordinator import Coordinator
from .errors import ConflictError, SimulatedCrash
from .model import Mutation
from .service import AccountService


DEMO_SECRET = b"chronocommit-demo-decision-secret-2026"


def _account_on_shard(coordinator: Coordinator, shard_index: int, prefix: str) -> str:
    for number in range(10_000):
        candidate = f"{prefix}-{number}"
        if coordinator.shard_index("demo", candidate) == shard_index:
            return candidate
    raise RuntimeError("could not find a deterministic account placement")


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


def run_demo(directory: str | Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if directory is None:
        temporary = tempfile.TemporaryDirectory(prefix="chronocommit-demo-")
        directory = temporary.name
    clock = ManualBoundedClock(center_ns=10_000_000, uncertainty_ns=50)
    coordinator = Coordinator(
        directory, shard_count=3, secret=DEMO_SECRET, clock=clock
    )
    service = AccountService(coordinator)
    source = _account_on_shard(coordinator, 0, "checking")
    destination = _account_on_shard(coordinator, 1, "savings")
    third = _account_on_shard(coordinator, 2, "reserve")

    created = [
        service.create_account("demo", source, 10_000, request_id="open-checking"),
        service.create_account("demo", destination, 5_000, request_id="open-savings"),
        service.create_account("demo", third, 2_000, request_id="open-reserve"),
    ]
    before = service.get_accounts("demo", [source, destination, third])
    before_transfer_ts = max(row["commit_ts"] for row in before.values())

    first = service.transfer(
        "demo", "rent-001", source, destination, 2_500
    )
    retry = service.transfer(
        "demo", "rent-001", source, destination, 2_500
    )
    after_retry = service.get_accounts("demo", [source, destination, third])

    crashed = False
    try:
        service.transfer(
            "demo",
            "sweep-001",
            destination,
            third,
            700,
            fault="after_commit:1",
        )
    except SimulatedCrash:
        crashed = True

    # A fresh coordinator represents process restart. Startup recovery observes
    # the durable commit decision and idempotently finishes all participants.
    restarted = Coordinator(
        directory, shard_count=3, secret=DEMO_SECRET, clock=clock
    )
    restarted_service = AccountService(restarted)
    recovered = restarted_service.transfer(
        "demo", "sweep-001", destination, third, 700
    )

    stale = restarted.snapshot("demo", [source, destination])
    winner = restarted_service.transfer(
        "demo", "purchase-001", source, destination, 100
    )
    stale_conflict = False
    try:
        restarted.execute(
            "demo",
            "stale-writer-001",
            [
                Mutation(
                    source,
                    {"balance_cents": stale[source].value["balance_cents"] - 50},
                    stale[source].commit_ts,
                ),
                Mutation(
                    destination,
                    {"balance_cents": stale[destination].value["balance_cents"] + 50},
                    stale[destination].commit_ts,
                ),
            ],
        )
    except ConflictError:
        stale_conflict = True

    historical = restarted_service.get_accounts(
        "demo", [source, destination], read_ts=before_transfer_ts
    )
    final_accounts = restarted_service.get_accounts(
        "demo", [source, destination, third]
    )
    integrity = restarted.verify_integrity()
    result = {
        "accounts": {
            "destination": destination,
            "source": source,
            "third": third,
        },
        "commit_wait": {
            "calls": clock.wait_count,
            "manual_nanoseconds_advanced": clock.waited_ns,
        },
        "crash_recovery": {
            "crash_injected": crashed,
            "participant_count": recovered["commit"]["participant_count"],
            "replayed_after_restart": recovered["commit"]["replayed"],
        },
        "external_order": {
            "first_commit_ts": first["commit"]["commit_ts"],
            "later_commit_ts": winner["commit"]["commit_ts"],
            "ordered": first["commit"]["commit_ts"] < winner["commit"]["commit_ts"],
        },
        "final_balances": {
            account: row["balance_cents"] for account, row in final_accounts.items()
        },
        "historical_balances": {
            account: row["balance_cents"] for account, row in historical.items()
        },
        "idempotent_retry": {
            "same_txn": first["commit"]["txn_id"] == retry["commit"]["txn_id"],
            "was_replayed": retry["commit"]["replayed"],
            "total_after_retry": sum(
                row["balance_cents"] for row in after_retry.values()
            ),
        },
        "integrity": integrity,
        "invariants": {
            "historical_snapshot_preserved": (
                historical[source]["balance_cents"] == 10_000
                and historical[destination]["balance_cents"] == 5_000
            ),
            "stale_writer_rejected": stale_conflict,
            "total_balance_conserved": sum(
                row["balance_cents"] for row in final_accounts.values()
            )
            == 17_000,
        },
        "initial_commit_timestamps": [item.commit_ts for item in created],
    }
    if temporary is not None:
        temporary.cleanup()
    return result


def run_benchmark(transactions: int = 200) -> dict[str, Any]:
    if isinstance(transactions, bool) or not isinstance(transactions, int):
        raise ValueError("transactions must be an integer")
    if transactions < 1 or transactions > 5_000:
        raise ValueError("transactions must be between 1 and 5000")
    with tempfile.TemporaryDirectory(prefix="chronocommit-benchmark-") as directory:
        clock = ManualBoundedClock(center_ns=20_000_000, uncertainty_ns=0)
        coordinator = Coordinator(
            directory, shard_count=3, secret=DEMO_SECRET, clock=clock
        )
        service = AccountService(coordinator)
        accounts = [
            _account_on_shard(coordinator, index, f"bench-{index}")
            for index in range(3)
        ]
        for index, account in enumerate(accounts):
            service.create_account(
                "demo", account, 100_000, request_id=f"benchmark-open-{index}"
            )
        latencies_ms: list[float] = []
        started = time.perf_counter()
        for index in range(transactions):
            source = accounts[index % len(accounts)]
            destination = accounts[(index + 1) % len(accounts)]
            call_started = time.perf_counter()
            service.transfer(
                "demo", f"benchmark-{index}", source, destination, 1
            )
            latencies_ms.append((time.perf_counter() - call_started) * 1_000)
        elapsed = time.perf_counter() - started
        final = service.get_accounts("demo", accounts)
        integrity = coordinator.verify_integrity()
        return {
            "elapsed_seconds": round(elapsed, 6),
            "integrity_transactions": integrity["transactions"],
            "latency_ms": {
                "p50": round(median(latencies_ms), 6),
                "p95": round(_percentile(latencies_ms, 0.95), 6),
            },
            "throughput_transactions_per_second": round(transactions / elapsed, 2),
            "total_balance_conserved": sum(
                row["balance_cents"] for row in final.values()
            )
            == 300_000,
            "transactions": transactions,
        }

