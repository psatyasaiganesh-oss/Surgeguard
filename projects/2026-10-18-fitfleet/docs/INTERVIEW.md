# FitFleet interview walkthrough

## Honest introduction

"I independently implemented FitFleet after reading Netflix's engineering article on container right-sizing. I focused on the safety controls around a recommendation: independent evidence, version binding, durable activation, and recovery when workload demand changes. It is a working local controller and replay tool, not a production autoscaler."

## Five-minute demonstration

1. **The problem:** Show the primary source and explain that unnecessarily large CPU/memory allocations reserve capacity, but reducing them can make a workload slow or fail. State clearly that no cloud savings were measured in this project.
2. **The recommendation:** Run `python -m fitfleet demo`. Show the 1,000 training records and the bounded resource change. Explain different treatment of compressible CPU capacity and hard memory failure, with explicit per-dimension opt-in.
3. **The evidence gate:** Explain why 300 newer holdout executions are separate from training, why reused execution IDs are invalid, and why a failed verification cannot be overwritten by an easier test. Show the rejected CPU-shift scenario.
4. **The controller:** Show activation version 2, restart persistence, and idempotent retries. Demonstrate the later memory-shift replay restoring baseline version 3. Then show that an operator override yields SUPERSEDED, not an overwrite.
5. **The boundaries:** Distinguish modeled workload outcomes from measured Python tool timing. Explain that `apply` changes only SQLite, and Job export does not install a watcher or a cluster controller. Show the CLI tests and abrupt-exit tests as actual implementation evidence.

## Questions to prepare for

**Why not just use average utilization?** A mean hides burst and tail demand. The planner uses high quantiles and headroom, while verification independently checks retries, deadlines, p95 slowdown, and every execution's slowdown. Even a p95 can hide a rare serious regression; a test explicitly demonstrates that case.

**How do you avoid evaluating on your training data?** The controller persists training provenance, rejects duplicate execution IDs, requires holdout timestamps strictly after training, and binds the workload and code revision. Observation windows must also be strictly newer. This avoids evidence reuse inside the lab; it does not authenticate inputs or establish statistical independence.

**What do you mean by savings?** The result is a modeled reservation-time score: vCPU-seconds plus a configurable relative weight for GiB-seconds. It is not dollars, measured utilization, or proof of a lower cloud invoice. The candidate budget itself is an actual local configuration change.

**How accurate is the memory fallback?** It is an assumption-driven replay: an early guard consumes a configured fraction of work before retrying with more memory. No real watchdog is implemented. A genuine service would need tested detection latency, realistic OOM behavior, restart-safe workload semantics, and real application measurements.

**Why use configuration versions?** A valid recommendation refers to one baseline. A later change can invalidate both its evidence and its authority. Activation uses compare-and-swap on that version. Rollback also checks the exact version it activated, so it cannot erase an operator's newer configuration or code.

**What happens if the process dies halfway through activation?** Resource update, recommendation state, and audit event are in the same SQLite transaction. Subprocess tests call `os._exit` between updates and confirm that reopening the DB sees the previous complete state. This is local transactional recovery, not a distributed guarantee.

**Why keep rejection immutable?** Replacing negative evidence with a more favorable window can conceal a real failure. An identical verification retry is safe and idempotent; different evidence needs a distinct recommendation with distinct provenance or a new baseline version.

**What prevents flapping?** Per-cycle reduction limits, a configurable cooldown, immutable proposal identities, explicit opt-in, and a global activation pause. Monitoring can continue after a successful window because initial success does not prove future stability.

**What would production integration require?** Signed/validated telemetry, code/image identity binding, calibrated runtime/resource models, genuinely independent baseline/candidate experiments, actual cgroup/container measurements, cluster-aware capacity and price modeling, a secured deployment adapter, observation expiry, and operational ownership. These are future work, not implemented claims.

## Resume bullets

- Built an independent Python/SQLite resource-tuning controller with strict trace validation, immutable holdout evidence, version-checked activation, cooldown, and operator-safe rollback; evaluated 1,000 synthetic training executions and 300 holdout executions.
- Added 68 automated tests covering concurrency, abrupt process exits, evidence leakage, tail regressions, and configuration drift; demonstrated a 42.74% improvement in a modeled resource-time score, with Docker and multi-version Python CI configuration.

Use these numbers only while they match the committed reports. Never describe the modeled score as production cloud savings or this independent work as employment at Netflix.
