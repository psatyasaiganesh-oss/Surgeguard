# FitFleet — evidence-gated batch resource right-sizing

A working Python resource recommender, paired demand-replay engine, and durable local configuration controller. It generates bounded CPU/memory recommendations, verifies a disjoint holdout, updates a versioned configuration registry, monitors newer replay windows, and rolls back regressions without overwriting operator changes. It also exports a hardened Kubernetes Job JSON using the active configuration.

**Independent implementation inspired by a documented Netflix engineering challenge.** Not Netflix software, an affiliation, or a production deployment. Python 3.11+; standard library only; no paid cloud resources, credentials, or network required for the core demo.

## Sourced problem and intended business impact

**Documented facts:** Netflix's [Netflix Stratum Media Processing: Automated Container Right-Sizing](https://netflixtechblog.com/netflix-stratum-media-processing-automated-container-right-sizing-ddf9aa68a801), by Violetta Pidvolotska and Naveen Mareddy, is dated **September 25, 2026** on the original article. It describes over-provisioned batch containers, resource-specific opt-in, recommendation verification, post-change monitoring, and rollback after workload changes. Its failure example shows why passing an initial experiment does not guarantee future safety. Source checked **October 18, 2026**; the original publication date, not an aggregator's later indexing date, is used here.

**Our engineering inference:** A useful tuner needs more than a percentile formula: it must bind evidence to a workload/code/config version, preserve failed verification, detect later demand shifts, and honor operator overrides. FitFleet implements those controls locally. The intended benefit is reducing idle reservations and resource-tuning toil while making regressions recoverable. Actual cloud cost savings, workload performance, and business impact have **not** been measured.

See [source boundaries](docs/SOURCES.md). Resource efficiency is a different focus from this repository's reliability, tracing, payment, replication, backup, and schema migration projects.

## Quick start

```bash
cd projects/2026-10-18-fitfleet
python --version                       # Python 3.11+
python -m unittest discover -v
python -m fitfleet demo
```

Every demo uses a new temporary database; it never resets an existing registry. Synthetic data and logical outcomes are deterministic. The demo:

1. Trains on 1,000 executions, verifies 300 newer executions, and activates a passed candidate **in the local registry only**.
2. Reopens the database and checks that an activation retry does not increment its version again.
3. Replays 200 healthy executions followed by a 200-execution memory-demand shift; the latter triggers a local rollback.
4. Rejects a separate CPU-heavy holdout, blocks a stale activation, and preserves an operator's newer configuration/code revision during rollback.

Successful JSON includes `success: true`, `modeled_holdout_comparison.passed: true`, `safety.memory_shift_state: ROLLED_BACK`, and `safety.operator_override_preserved: true`.

```bash
python -m fitfleet demo --training-count 5000 --holdout-count 1000
python scripts/validate.py
```

The validator records actual test outcomes and tool timings in [reports](reports/). Demand-replay results are separately labeled as modeled.

## Architecture and lifecycle

| Component | Responsibility | Safety boundary |
| --- | --- | --- |
| `model.py` | Strict bounded JSON schemas, units, trace provenance | Rejects duplicate keys/IDs, mixed code revisions, invalid quantities, and censored evidence |
| `sizing.py` | Nearest-rank quantiles, bounded proposals, paired replay | Recommendation is not authorization to apply |
| `controller.py` | SQLite WAL registry, immutable verification, version-CAS activation, observation ledger | Configuration/state/audit updates commit together |
| `__main__.py` | JSON CLI, synthetic trace generation, Job export | No cluster client, shell execution, or cloud writes |

Each recommendation has a SHA-256 ID derived from its training evidence, baseline version, candidate, and policy. Repeating the same plan reuses it. States are:

- `CREATED`: proposed but not trusted; active resources remain unchanged.
- `VERIFIED` or `REJECTED`: a newer, disjoint holdout passed or failed all gates. Verification becomes immutable, including negative results.
- `APPLIED`: a passed candidate replaced the expected version in the local registry. Repeated activation is idempotent.
- `MONITORED`: a newer replay window passed. Further newer windows may still trigger rollback.
- `ROLLED_BACK`: regression or manual rollback restored the baseline and incremented the config version. The old recommendation cannot reactivate.
- `SUPERSEDED`: an operator already changed configuration or code. Old automation does not overwrite that change.

Replay calculations run outside the writer transaction. The controller rechecks state and configuration versions when committing a decision. Tests cover an operator change during planning and during observation, not merely before those operations start. Abrupt process-exit tests verify that a configuration change cannot commit without its associated state update.

`pause` blocks new activation but leaves verification, observation, and rollback available. A configurable cooldown prevents immediate retuning after a tune or rollback. This is a local operator CLI, not a multi-tenant service or a remote authorization system.

## Recommendation policy

All policy thresholds are explicit in `Policy`; a JSON file may override known fields. Values with a `_bp` suffix use basis points: 1,500 means 15%, and a runtime ratio of 10,500 means 1.05x baseline. The supplied [policy](examples/policy.json) enables the modeled memory-retry mechanism; it is off by default.

- Default training/holdout minimums: 100/50 executions and at least one hour of observation span. These are sample-quality floors, **not statistical confidence guarantees**.
- CPU and memory targets use p99 and p99.9 demand, with 10% and 25% headroom. Resource values round upward to 250 millicores and 128 MiB steps.
- A single cycle reduces CPU by at most 50% and memory by at most 15%. A dimension not opted in is unchanged. No-change proposals are rejected.
- Censored data, memory peaks above the training allocation, or opted-in CPU demand reaching 95% of its allocation are rejected; capped telemetry cannot establish spare capacity.
- Verification requires a healthy modeled baseline, zero failed candidate executions, zero missed deadlines, p95 runtime at most 1.05x baseline, **every** runtime at most 1.25x baseline, retry rate at most 2%, and at least 5% improvement in the resource-time score.
- Observation checks health, not initial financial justification. Failed gates restore the baseline only if the currently active version still belongs to this recommendation.

Default thresholds are lab choices and are configurable. This is not a reproduction of Netflix's production algorithm or infrastructure.

## What the replay model means

Input is a versioned JSON document with a `runs` list. Each run specifies `run_id`, `timestamp_s`, `workload`, `code_revision`, `cpu_peak_millis`, `memory_peak_mib`, `cpu_work_ms`, `fixed_ms`, `deadline_ms`, and optional `censored`. Records represent **unconstrained execution demand**, not raw samples measured under an unknown throttle. The generator produces synthetic demand; no telemetry is collected from the host or a cloud service.

For each execution and budget:

```text
modeled_duration_ms = fixed_ms + cpu_work_ms * 1000 / min(allocated_cpu_millis, cpu_peak_millis)
resource_time_score = reserved_vCPU_seconds + 0.25 * reserved_GiB_seconds
```

The score coefficient is an assumed relative weight, **not a cloud price**. CPU units represent parallel work demand; excess reserved CPU beyond that demand does not speed the execution up. The model omits shared-node scheduling, cache effects, GC, I/O coupling, network/disk limits, and real application scaling behavior.

When memory retry is explicitly enabled, a predicted peak above 90% of allocation triggers a modeled early cancellation. It charges 20% of that attempt's full work and retries with 1.5x memory, rounded up, for at most three retries and 65,536 MiB. All attempts contribute reservation-time and latency. **Timely detection before OOM is an assumption. No watcher or real OOM-prevention mechanism is implemented.** With retry disabled, demand beyond the allocation models a hard memory failure. Exhausted retries also fail verification.

Baseline and candidate receive identical records. Training timestamps must precede holdout timestamps, execution IDs cannot be reused, and later observation windows must be strictly newer. This prevents evidence leakage within the lab; it does not authenticate telemetry or prove coverage of unseen future workloads.

## Persistent local operator workflow

Use a new, operator-owned DB path. These commands generate synthetic files and modify only the local registry. They do not contact Kubernetes:

```bash
python -m fitfleet generate --count 1000 --start 0 > training.json
python -m fitfleet generate --count 300 --start 60000 --seed 11 --profile rare-memory > holdout.json
python -m fitfleet --db /tmp/fitfleet-lab.db register encoder \
  --cpu-millis 8000 --memory-mib 8192 --code-revision v1 --opt-in cpu --opt-in memory
python -m fitfleet --db /tmp/fitfleet-lab.db plan encoder \
  --trace training.json --policy examples/policy.json > plan.json
FITFLEET_REC_ID=$(python -c "import json; print(json.load(open('plan.json'))['id'])")
python -m fitfleet --db /tmp/fitfleet-lab.db verify "$FITFLEET_REC_ID" --trace holdout.json
python -m fitfleet --db /tmp/fitfleet-lab.db apply "$FITFLEET_REC_ID"
python -m fitfleet --db /tmp/fitfleet-lab.db manifest encoder --image fitfleet-local:demo > job.json
python -m fitfleet generate --count 200 --start 78000 --seed 17 --profile memory-shift > shifted.json
python -m fitfleet --db /tmp/fitfleet-lab.db observe "$FITFLEET_REC_ID" --trace shifted.json
python -m fitfleet --db /tmp/fitfleet-lab.db status encoder
python -m fitfleet --db /tmp/fitfleet-lab.db events encoder
```

Expected final state: the observation reports `ROLLED_BACK`, and the local registry again contains 8,000 millicores and 8,192 MiB at version 3. The earlier `job.json` remains an export of the candidate at version 2; re-export to see later active values.

`register --expected-version N` is the explicit operator update path; it invalidates older recommendations, including changes to opt-in or code revision. `show ID`, `pause`, `resume`, and `rollback ID` support inspection and control. Expected failures produce JSON on stderr and exit code 2. Parser usage errors also exit 2. Database contention raises `Busy`; retry the whole command. The DB and a trace are never reset implicitly.

## Kubernetes export, not a deployment controller

`manifest` exports a `batch/v1` Job using only the current local budget. CPU is expressed in millicores and memory in `Mi`; requests equal limits. The template disables service-account token mounting, privilege escalation, and extra capabilities, uses UID/GID 10001 and the runtime-default seccomp profile, and makes the root filesystem read-only. A bounded 128 MiB scratch volume mounted at `/tmp` supports the demo's temporary files.

Review the image and pin its digest for real use. The image must support those restrictions, and `code_revision` is an operator assertion, not cryptographic image attestation. Exported Jobs **do not contain the modeled memory watcher, resize retries, monitoring, or rollback machinery**. They are neither submitted nor server-side validated here. FitFleet's automatic rollback affects its SQLite registry, not an already exported file or an existing Kubernetes Job. A real deployment adapter and live experiments are future work.

## Tests, measured evidence, and Docker

The suite covers recommendation bounds, tail-sensitive runtime checks, replay accounting, immutable failed evidence, cohort/window validation, version conflicts, concurrent activation and verification, operator-change races, cooldown, pause behavior, JSON security limits, abrupt exits, and a full CLI workflow. Read [reports/tests.txt](reports/tests.txt), [reports/demo.json](reports/demo.json), and [reports/validation.json](reports/validation.json) for the actual run rather than inferred performance.

Recorded snapshot on Python 3.12.14 / SQLite 3.53.1, Linux:

| Evidence type | Metric | Recorded result |
| --- | --- | --- |
| Measured | Unit/integration/CLI tests | 68 passed |
| Measured | Planning over 1,000 training records | 30.894 ms |
| Measured | Verification over 300 holdout records | 126.748 ms |
| Local configuration | CPU / memory candidate | 8,000 to 4,000 millicores / 8,192 to 7,040 MiB |
| Modeled | Resource-time score improvement | 42.7407% |
| Modeled | Holdout p95 runtime ratio / retry rate | 1.0x / 0.3333% |
| Measured controller behavior | Rejection, rollback, stale-write block, override protection | All demonstrated successfully |
| Local environment | Docker execution | Unavailable locally; separate CI job configured |

Tool timing is one local sample and varies with system load. Configuration reductions are not measured reductions in CPU utilization; replay scores are not cloud bill savings.

```bash
docker build -t fitfleet-local:demo .
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m fitfleet-local:demo demo
```

The image runs the unit tests during build, then executes as UID 10001. The demo writes only to a temporary directory. Docker availability and local execution are recorded honestly by the validator; [GitHub CI](../../.github/workflows/fitfleet-ci.yml) separately checks Python 3.11/3.12/3.13 and a container build plus isolated demo.

## Security and known limits

- No network listener, external API credentials, telemetry uploads, subprocess shell invocation, pickle/eval, or `kubectl apply` exists in the runtime. Inputs use strict JSON with a 16 MiB cap, 32-level nesting limit, and at most 50,000 executions per trace. Numeric units, unknown fields, duplicate keys, duplicate IDs, and non-finite constants are checked.
- SQL values are bound parameters. New DB files have POSIX mode 0600; direct symlinks and unrelated database identities are rejected. Use a trusted directory: filesystem path-swap attacks, shared untrusted users, and malicious local DB edits are outside the threat model. This is not encryption or remote access control.
- SQLite WAL with `synchronous=FULL` provides the local transaction boundary, not distributed consensus. One writer serializes configuration mutations. Run IDs retained for a recommendation are capped at 200,000; audit history and recommendations require an operator-defined archival policy.
- There is no real workload/container execution, cloud billing integration, autoscaling, cluster scheduler simulation, live production experiment, statistics-based significance test, or signed telemetry. The trace model needs calibration against real, independently collected telemetry before it could justify operational changes. Evidence has ordering/version guards, but no wall-clock expiry policy.
- Default cooldown is 24 hours of the trusted controller clock. Synthetic timestamps are independent of that clock. Pausing never prevents safety rollback. An operator override supersedes automation even if its budget looks worse; FitFleet does not have authority to undo it.

The [interview guide](docs/INTERVIEW.md) includes honest resume bullets, a demonstration sequence, and the main design tradeoffs.
