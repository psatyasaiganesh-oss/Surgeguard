"""Synthetic demand traces and a complete local controller demonstration."""

from dataclasses import asdict
import platform
import random
import sqlite3
import tempfile
import time

from .controller import Controller
from .errors import Conflict, InvalidState
from .model import Budget, Policy, Run, Trace, integer, name, token


def generate(*, workload="encoder", revision="v1", count=1000, start_s=0, seed=7, profile="steady"):
    name(workload)
    token(revision, "revision")
    integer(count, "count", 1, 50000)
    integer(start_s, "start_s", 0, 2**53-1-60*count)
    integer(seed, "seed", 0, 2**32-1)
    if profile not in ("steady", "memory-shift", "cpu-shift", "rare-memory"):
        from .errors import ValidationError
        raise ValidationError("unknown synthetic profile")
    rng = random.Random(seed)
    runs = []
    for index in range(count):
        cpu = 6000 if profile == "cpu-shift" else rng.randint(1000, 2600)
        memory = 7000 if profile == "memory-shift" or profile == "rare-memory" and index == 0 else rng.randint(700, 1500)
        runs.append(Run(run_id=f"{workload}:{seed}:{start_s}:{index}", timestamp_s=start_s+index*60,
                        workload=workload, code_revision=revision, cpu_peak_millis=cpu, memory_peak_mib=memory,
                        cpu_work_ms=rng.randint(50000, 150000), fixed_ms=rng.randint(1000, 5000), deadline_ms=240000))
    return Trace(tuple(runs))


def run_demo(*, training_count=1000, holdout_count=300):
    integer(training_count, "training_count", 100, 10000)
    integer(holdout_count, "holdout_count", 100, 10000)
    started = time.perf_counter()
    baseline, policy = Budget(8000, 8192), Policy(retry_memory=True)
    with tempfile.TemporaryDirectory(prefix="fitfleet-") as directory:
        path = f"{directory}/controller.db"
        controller = Controller(path, clock=lambda: 1000000)
        controller.register("encoder", baseline, "v1", opt_cpu=True, opt_memory=True)
        training = generate(count=training_count)
        train_end = training.runs[-1].timestamp_s
        holdout = generate(count=holdout_count, start_s=train_end+60, seed=11, profile="rare-memory")
        planned_at = time.perf_counter()
        proposal = controller.plan("encoder", training, policy)
        planning_ms = (time.perf_counter()-planned_at)*1000
        rid = proposal["id"]
        assert controller.plan("encoder", training, policy)["id"] == rid
        verified_at = time.perf_counter()
        verified = controller.verify(rid, holdout)
        verification_ms = (time.perf_counter()-verified_at)*1000
        assert verified["state"] == "VERIFIED"
        controller.apply(rid)
        restarted = Controller(path, clock=lambda: 1000000)
        assert restarted.status("encoder")["version"] == 2
        assert restarted.apply(rid)["replayed"]
        stable_start = holdout.runs[-1].timestamp_s+60
        healthy = generate(count=200, start_s=stable_start, seed=13)
        observed = restarted.observe(rid, healthy)
        assert observed["state"] == "MONITORED"
        shifted = generate(count=200, start_s=healthy.runs[-1].timestamp_s+60, seed=17, profile="memory-shift")
        drift = restarted.observe(rid, shifted)
        assert drift["state"] == "ROLLED_BACK"
        assert restarted.status("encoder")["cpu_millis"] == baseline.cpu_millis
        assert restarted.status("encoder")["memory_mib"] == baseline.memory_mib
        version_after_rollback = restarted.status("encoder")["version"]
        assert restarted.observe(rid, shifted)["replayed"]
        assert restarted.status("encoder")["version"] == version_after_rollback

        # A candidate that looks efficient in training must fail on a CPU-heavy holdout.
        restarted.register("cpu-test", baseline, "v1", opt_cpu=True, opt_memory=True)
        cpu_training = generate(workload="cpu-test", count=training_count, seed=19)
        cpu_rec = restarted.plan("cpu-test", cpu_training, policy)
        heavy = generate(workload="cpu-test", count=holdout_count,
                         start_s=cpu_training.runs[-1].timestamp_s+60, seed=23, profile="cpu-shift")
        rejected = restarted.verify(cpu_rec["id"], heavy)
        assert rejected["state"] == "REJECTED"
        try:
            restarted.apply(cpu_rec["id"])
        except InvalidState:
            unsafe_activation_blocked = True
        else:
            raise AssertionError("rejected candidate was activated")

        # Operator changes are authoritative: never roll back over them.
        restarted.register("operator-test", baseline, "v1", opt_cpu=True, opt_memory=True)
        operator_training = generate(workload="operator-test", count=training_count, seed=29)
        operator_rec = restarted.plan("operator-test", operator_training, policy)
        operator_holdout = generate(workload="operator-test", count=holdout_count,
                                    start_s=operator_training.runs[-1].timestamp_s+60, seed=31)
        restarted.verify(operator_rec["id"], operator_holdout)
        restarted.apply(operator_rec["id"])
        override = Budget(6000, 7808)
        restarted.register("operator-test", override, "v2", opt_cpu=False, opt_memory=False, expected_version=2)
        skipped = restarted.rollback(operator_rec["id"])
        assert skipped["state"] == "SUPERSEDED"
        assert restarted.status("operator-test")["cpu_millis"] == override.cpu_millis
        assert restarted.status("operator-test")["code_revision"] == "v2"

        # Revision binding also prevents activation if the operator changes config before apply.
        stale_controller = Controller(f"{directory}/stale.db")
        stale_controller.register("encoder", baseline, "v1", opt_cpu=True, opt_memory=True)
        stale_rec = stale_controller.plan("encoder", training, policy)
        stale_controller.verify(stale_rec["id"], holdout)
        stale_controller.register("encoder", baseline, "v2", expected_version=1)
        try:
            stale_controller.apply(stale_rec["id"])
        except Conflict:
            stale_activation_blocked = True
        else:
            raise AssertionError("stale candidate was activated")

        return {
            "success": True,
            "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                            "platform": platform.system(), "dependencies": "Python standard library only"},
            "scope": "Independent synthetic demand replay and local SQLite configuration changes. No containers, cloud, or production traffic were used.",
            "workload": {"training_executions": training_count, "holdout_executions": holdout_count,
                         "healthy_monitor_executions": 200, "shifted_monitor_executions": 200,
                         "separate_rejected_and_operator_scenarios": True},
            "recommendation": {"baseline": asdict(baseline), "candidate": proposal["spec"]["candidate"],
                               "statistics": proposal["spec"]["statistics"], "idempotent_plan": True},
            "modeled_holdout_comparison": verified["verification"]["comparison"],
            "safety": {"restart_preserved_applied_version": True, "healthy_state": observed["state"],
                       "memory_shift_state": drift["state"], "memory_shift_failed_checks": drift["comparison"]["failed_checks"],
                       "rollback_restored_baseline": True, "rollback_retry_did_not_change_version": True,
                       "cpu_shift_rejected": rejected["verification"]["comparison"]["failed_checks"],
                       "rejected_activation_blocked": unsafe_activation_blocked,
                       "stale_activation_blocked": stale_activation_blocked,
                       "operator_override_preserved": skipped["state"] == "SUPERSEDED"},
            "measured_local_timings": {"planning_ms": round(planning_ms, 3), "verification_ms": round(verification_ms, 3),
                                       "total_demo_ms": round((time.perf_counter()-started)*1000, 3)},
            "final_local_encoder_configuration": restarted.status("encoder"),
            "limits": "Resource-time score, runtime, retries, and reservation savings are modeled. Timing of the Python tool itself is measured. No measured cloud cost savings."
        }
