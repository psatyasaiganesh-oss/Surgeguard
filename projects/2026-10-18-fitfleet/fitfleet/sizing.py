"""Deterministic recommendation and paired replay; all performance is MODELED."""

from dataclasses import asdict
import math
import statistics

from .errors import EvidenceError
from .model import Budget, validate_window


def quantile(values, basis_points):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered)*basis_points/10000)-1)]


def round_up(value, step):
    return int(math.ceil(value/step)*step)


def recommend(trace, baseline, policy, *, opt_cpu, opt_memory, workload, revision):
    meta = validate_window(trace, workload, revision, policy, training=True)
    cpu_q = quantile([r.cpu_peak_millis for r in trace.runs], policy.cpu_quantile_bp)
    mem_q = quantile([r.memory_peak_mib for r in trace.runs], policy.memory_quantile_bp)
    if max(r.memory_peak_mib for r in trace.runs) > baseline.memory_mib:
        raise EvidenceError("training memory exceeds its baseline allocation; baseline is not trustworthy")
    if max(r.cpu_peak_millis for r in trace.runs) >= baseline.cpu_millis*0.95 and opt_cpu:
        raise EvidenceError("CPU demand reaches the baseline limit; do not infer spare CPU from capped data")
    cpu = max(250, round_up(cpu_q*(1+policy.cpu_headroom_bp/10000), 250),
              round_up(baseline.cpu_millis*(1-policy.max_cpu_reduction_bp/10000), 250))
    memory = max(128, round_up(mem_q*(1+policy.memory_headroom_bp/10000), 128),
                 round_up(baseline.memory_mib*(1-policy.max_memory_reduction_bp/10000), 128))
    candidate = Budget(min(baseline.cpu_millis, cpu) if opt_cpu else baseline.cpu_millis,
                       min(baseline.memory_mib, memory) if opt_memory else baseline.memory_mib)
    if candidate == baseline:
        raise EvidenceError("no permitted downsize; dimensions are opted out or already near the target")
    return candidate, meta, {"cpu_quantile_millis": cpu_q, "memory_quantile_mib": mem_q,
                             "cpu_reduction_pct": (1-candidate.cpu_millis/baseline.cpu_millis)*100,
                             "memory_reduction_pct": (1-candidate.memory_mib/baseline.memory_mib)*100}


def run_once(run, budget, policy):
    """Replay a demand record. No containers are executed or memory limits enforced.

    duration = fixed time + CPU work / usable cores. The guard-trigger timing and
    discarded-work fraction are explicit assumptions, not inferred telemetry.
    """
    memory, elapsed, score, cpu_seconds, gib_seconds = budget.memory_mib, 0.0, 0.0, 0.0, 0.0
    attempts = []
    for index in range(policy.max_retries + 1):
        full_duration = run.fixed_ms + run.cpu_work_ms*1000/min(budget.cpu_millis, run.cpu_peak_millis)
        guard = policy.retry_memory and run.memory_peak_mib*10000 > memory*policy.memory_guard_bp
        oom = not policy.retry_memory and run.memory_peak_mib > memory
        duration = full_duration*(policy.retry_wasted_work_bp/10000) if guard or oom else full_duration
        elapsed += duration
        cpu_reservation = budget.cpu_millis*duration/1_000_000
        memory_reservation = memory*duration/(1024*1000)
        cpu_seconds += cpu_reservation
        gib_seconds += memory_reservation
        score += cpu_reservation + memory_reservation*policy.memory_score_weight_bp/10000
        attempts.append({"memory_mib": memory, "duration_ms": duration,
                         "outcome": "guard_retry" if guard else "oom" if oom else "success"})
        if not guard and not oom:
            return {"success": True, "duration_ms": elapsed, "score": score, "retries": index,
                    "deadline_missed": elapsed > run.deadline_ms, "cpu_reserved_seconds": cpu_seconds,
                    "gib_reserved_seconds": gib_seconds, "attempts": attempts}
        if oom or index == policy.max_retries or memory == 65536:
            break
        memory = min(65536, round_up(memory*1.5, 128))
    return {"success": False, "duration_ms": elapsed, "score": score, "retries": len(attempts)-1,
            "deadline_missed": elapsed > run.deadline_ms, "cpu_reserved_seconds": cpu_seconds,
            "gib_reserved_seconds": gib_seconds, "attempts": attempts}


def summarize(results):
    durations = [r["duration_ms"] for r in results]
    return {"executions": len(results), "failures": sum(not r["success"] for r in results),
            "deadline_misses": sum(r["deadline_missed"] for r in results),
            "retry_executions": sum(r["retries"] > 0 or not r["success"] and r["attempts"][0]["outcome"] == "guard_retry" for r in results),
            "retry_attempts": sum(r["retries"] for r in results),
            "oom_attempts": sum(a["outcome"] == "oom" for r in results for a in r["attempts"]),
            "p50_duration_ms": statistics.median(durations), "p95_duration_ms": quantile(durations, 9500),
            "max_duration_ms": max(durations), "resource_time_score": sum(r["score"] for r in results),
            "cpu_reserved_seconds": sum(r["cpu_reserved_seconds"] for r in results),
            "gib_reserved_seconds": sum(r["gib_reserved_seconds"] for r in results)}


def compare(trace, baseline, candidate, policy, *, require_saving=True):
    before = [run_once(r, baseline, policy) for r in trace.runs]
    after = [run_once(r, candidate, policy) for r in trace.runs]
    b, c = summarize(before), summarize(after)
    saving_bp = (1-c["resource_time_score"]/b["resource_time_score"])*10000
    p95_ratio_bp = c["p95_duration_ms"]/b["p95_duration_ms"]*10000
    worst_ratio_bp = max(a["duration_ms"]/o["duration_ms"]*10000 for o, a in zip(before, after))
    retry_rate_bp = c["retry_executions"]/len(trace.runs)*10000
    checks = {
        "baseline_healthy": b["failures"] == 0 and b["deadline_misses"] == 0,
        "no_failed_executions": c["failures"] == 0,
        "no_deadline_misses": c["deadline_misses"] == 0,
        "p95_runtime_within_budget": p95_ratio_bp <= policy.max_p95_runtime_ratio_bp + 1e-8,
        "every_runtime_within_budget": worst_ratio_bp <= policy.max_single_runtime_ratio_bp + 1e-8,
        "retry_rate_within_budget": retry_rate_bp <= policy.max_retry_rate_bp + 1e-8,
        "sufficient_resource_score_saving": not require_saving or saving_bp + 1e-8 >= policy.min_score_saving_bp,
    }
    return {"passed": all(checks.values()), "failed_checks": [k for k, v in checks.items() if not v],
            "checks": checks, "baseline": b, "candidate": c,
            "resource_score_saving_pct": saving_bp/100, "p95_runtime_ratio": p95_ratio_bp/10000,
            "worst_runtime_ratio": worst_ratio_bp/10000, "retry_rate_pct": retry_rate_bp/100,
            "model": "Paired deterministic demand replay, not measured container performance or cloud billing",
            "budgets": {"baseline": asdict(baseline), "candidate": asdict(candidate)}}
