from __future__ import annotations

import platform
import tempfile
import time
from pathlib import Path
from typing import Any

from .errors import ConflictError, SimulatedCrash
from .store import ExitPack


DEMO_SECRET = b"exitpack-public-demo-signing-secret-v1"


def _new_database(
    path: str | Path | None, name: str
) -> tuple[Path, tempfile.TemporaryDirectory[str] | None]:
    if path is None:
        temporary = tempfile.TemporaryDirectory(prefix=f"exitpack-{name}-")
        return Path(temporary.name) / f"{name}.db", temporary
    selected = Path(path)
    if selected.exists():
        raise FileExistsError(f"refusing to overwrite existing database: {selected}")
    selected.parent.mkdir(parents=True, exist_ok=True)
    return selected, None


def _register_hosts(store: ExitPack, namespace: str, count: int, *, current_s: int = 0) -> None:
    for index in range(count):
        store.register_host(
            namespace,
            f"register-{index}",
            f"host-{index:03d}",
            f"zone-{index % 3}",
            16_000,
            64_000,
            current_s=current_s,
        )


def run_demo(path: str | Path | None = None) -> dict[str, Any]:
    database, temporary = _new_database(path, "demo")
    try:
        with ExitPack(database, secret=DEMO_SECRET) as store:
            namespace = "demo"
            _register_hosts(store, namespace, 6)

            # BEST_FIT deliberately mixes one long VM with short VMs on a host.
            workloads = [
                ("long", 500_000),
                ("short-a", 900),
                ("short-b", 900),
                ("short-c", 900),
            ]
            for vm_id, lifetime in workloads:
                store.schedule(
                    namespace,
                    f"schedule-{vm_id}",
                    {
                        "vm_id": vm_id,
                        "cpu_millis": 4_000,
                        "memory_mib": 16_000,
                        "predicted_lifetime_s": lifetime,
                    },
                    "BEST_FIT",
                    current_s=0,
                )
            maintenance = store.maintenance_plan(
                namespace, "host-000", 1_800, current_s=0
            )

            # Two schedulers plan from the same revision. Only the first may commit.
            revision = store.snapshot(namespace)["cluster_revision"]
            plan_a = store.create_plan(
                namespace,
                "concurrent-a",
                {
                    "vm_id": "race-a",
                    "cpu_millis": 2_000,
                    "memory_mib": 8_000,
                    "predicted_lifetime_s": 2_000,
                },
                "LIFETIME",
                expected_revision=revision,
                current_s=10,
            )
            plan_b = store.create_plan(
                namespace,
                "concurrent-b",
                {
                    "vm_id": "race-b",
                    "cpu_millis": 2_000,
                    "memory_mib": 8_000,
                    "predicted_lifetime_s": 2_000,
                },
                "LIFETIME",
                expected_revision=revision,
                current_s=10,
            )
            store.commit_plan(
                namespace, plan_a["plan_id"], plan_a["commit_token"], current_s=11
            )
            stale_plan_blocked = False
            try:
                store.commit_plan(
                    namespace, plan_b["plan_id"], plan_b["commit_token"], current_s=11
                )
            except ConflictError:
                stale_plan_blocked = True

            # A placement commits, but the caller loses the reply and safely retries.
            crash_plan = store.create_plan(
                namespace,
                "lost-ack-placement",
                {
                    "vm_id": "lost-ack-vm",
                    "cpu_millis": 2_000,
                    "memory_mib": 8_000,
                    "predicted_lifetime_s": 2_000,
                },
                "LIFETIME",
                current_s=20,
            )
            placement_ack_lost = False
            try:
                store.commit_plan(
                    namespace,
                    crash_plan["plan_id"],
                    crash_plan["commit_token"],
                    current_s=21,
                    failpoint="after_plan_commit",
                )
            except SimulatedCrash:
                placement_ack_lost = True
            placement_replay = store.commit_plan(
                namespace,
                crash_plan["plan_id"],
                crash_plan["commit_token"],
                current_s=22,
            )

            # Finish a short VM with another lost acknowledgement.
            finish_ack_lost = False
            try:
                store.finish_vm(
                    namespace,
                    "finish-short-a",
                    "short-a",
                    current_s=900,
                    failpoint="after_vm_finish",
                )
            except SimulatedCrash:
                finish_ack_lost = True
            finish_replay = store.finish_vm(
                namespace, "finish-short-a", "short-a", current_s=901
            )

            # A VM outlives its prediction; reconciliation promotes the host class.
            adapt_namespace = "adapt"
            _register_hosts(store, adapt_namespace, 2)
            adaptation_placement = store.schedule(
                adapt_namespace,
                "mispredicted",
                {
                    "vm_id": "mispredicted-vm",
                    "cpu_millis": 2_000,
                    "memory_mib": 8_000,
                    "predicted_lifetime_s": 100,
                },
                "LIFETIME",
                current_s=0,
            )
            before = next(
                host
                for host in store.snapshot(adapt_namespace)["hosts"]
                if host["host_id"] == adaptation_placement["host_id"]
            )
            reconciliation = store.reconcile(adapt_namespace, current_s=101)
            after = next(
                host
                for host in store.snapshot(adapt_namespace)["hosts"]
                if host["host_id"] == adaptation_placement["host_id"]
            )

            integrity = store.verify_integrity()
            metrics = store.metrics(namespace)

        return {
            "project": "ExitPack",
            "maintenance": maintenance,
            "optimistic_concurrency": {"stale_plan_blocked": stale_plan_blocked},
            "lost_acknowledgements": {
                "placement_ack_lost": placement_ack_lost,
                "placement_retry_replayed": placement_replay["replayed"],
                "finish_ack_lost": finish_ack_lost,
                "finish_retry_replayed": finish_replay["replayed"],
            },
            "misprediction_adaptation": {
                "before_class": before["lifetime_class"],
                "after_class": after["lifetime_class"],
                "after_state": after["state"],
                "changed_hosts": reconciliation["changed_hosts"],
            },
            "metrics": metrics,
            "integrity": integrity,
            "database_persisted_at": str(database) if path is not None else None,
        }
    finally:
        if temporary is not None:
            temporary.cleanup()


def _run_fragmentation_workload(
    policy: str,
    *,
    hosts: int,
    groups: int,
) -> dict[str, Any]:
    temporary = tempfile.TemporaryDirectory(prefix=f"exitpack-{policy.lower()}-")
    database = Path(temporary.name) / "simulation.db"
    try:
        with ExitPack(database, secret=DEMO_SECRET) as store:
            namespace = "benchmark"
            _register_hosts(store, namespace, hosts)
            schedule_start = time.perf_counter()
            short_ids: list[str] = []
            for group in range(groups):
                specifications = [(f"long-{group:03d}", 500_000)] + [
                    (f"short-{group:03d}-{index}", 1_800) for index in range(7)
                ]
                for vm_id, lifetime in specifications:
                    store.schedule(
                        namespace,
                        f"schedule-{vm_id}",
                        {
                            "vm_id": vm_id,
                            "cpu_millis": 2_000,
                            "memory_mib": 8_000,
                            "predicted_lifetime_s": lifetime,
                        },
                        policy,
                        current_s=0,
                    )
                    if vm_id.startswith("short-"):
                        short_ids.append(vm_id)
            schedule_seconds = time.perf_counter() - schedule_start
            initial = store.metrics(namespace)
            maintenance = store.maintenance_plan(
                namespace, "host-000", 3_600, current_s=0
            )

            finish_start = time.perf_counter()
            for vm_id in short_ids:
                store.finish_vm(
                    namespace,
                    f"finish-{vm_id}",
                    vm_id,
                    current_s=1_800,
                )
            finish_seconds = time.perf_counter() - finish_start
            after_short_exit = store.metrics(namespace)
            verify_start = time.perf_counter()
            integrity = store.verify_integrity(namespace)
            verify_seconds = time.perf_counter() - verify_start
        return {
            "policy": policy,
            "placements": groups * 8,
            "short_completions": len(short_ids),
            "initial": initial,
            "after_short_exit": after_short_exit,
            "maintenance": maintenance,
            "timing": {
                "schedule_seconds": round(schedule_seconds, 6),
                "finish_seconds": round(finish_seconds, 6),
                "verify_seconds": round(verify_seconds, 6),
            },
            "integrity": integrity,
        }
    finally:
        temporary.cleanup()


def run_benchmark(*, hosts: int = 40, groups: int = 32) -> dict[str, Any]:
    if hosts < 2 or hosts > 1_000:
        raise ValueError("hosts must be between 2 and 1000")
    if groups < 1 or groups > hosts:
        raise ValueError("groups must be between 1 and hosts")
    baseline = _run_fragmentation_workload("BEST_FIT", hosts=hosts, groups=groups)
    lifetime = _run_fragmentation_workload("LIFETIME", hosts=hosts, groups=groups)
    baseline_empty = baseline["after_short_exit"]["empty_hosts"]
    lifetime_empty = lifetime["after_short_exit"]["empty_hosts"]
    total_placements = baseline["placements"] + lifetime["placements"]
    total_schedule_seconds = (
        baseline["timing"]["schedule_seconds"] + lifetime["timing"]["schedule_seconds"]
    )
    return {
        "benchmark": "deterministic lifetime-fragmentation stress workload",
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
        },
        "workload": {
            "hosts_per_policy": hosts,
            "groups_per_policy": groups,
            "vms_per_policy": groups * 8,
            "pattern": "one long-lived VM followed by seven short-lived VMs",
        },
        "baseline": baseline,
        "lifetime_aware": lifetime,
        "comparison": {
            "baseline_empty_hosts_after_short_exit": baseline_empty,
            "lifetime_empty_hosts_after_short_exit": lifetime_empty,
            "additional_empty_hosts": lifetime_empty - baseline_empty,
            "empty_host_percentage_point_gain": round(
                100 * (lifetime_empty - baseline_empty) / hosts, 4
            ),
            "placements_per_second": round(total_placements / total_schedule_seconds, 2),
        },
        "scope": (
            "local adversarial synthetic workload; not Google production data, "
            "an ML evaluation, or a claim about LAVA performance"
        ),
    }
