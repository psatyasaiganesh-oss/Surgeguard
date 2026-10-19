# BurnWatch

A working SLO telemetry collector and multi-window alerting pipeline, with completeness watermarks, exact burn-rate comparisons, durable alert transitions, and a crash-safe local notification inbox. Python standard library + SQLite WAL; no paid cloud resources or external API credentials.

**Independent implementation inspired by Google SRE guidance.** This is not Google software, a production monitoring service, or evidence of work performed for Google.

## Sourced problem and business relevance

**Documented fact:** Google’s [SRE Workbook, chapter 5: Alerting on SLOs](https://sre.google/workbook/alerting-on-slos/), by Steven Thurgood with contributors, discusses detecting important reliability failures while limiting noisy or slow-resetting alerts. It presents multi-window, multi-burn-rate alerting and warns about low-traffic signals and duplicate notifications from overlapping rules.

**Our inference:** reducing unnecessary pages while recognizing sustained service failures can help an engineering team focus on customer-impacting incidents. This is a motivating business risk, not a measured reduction in any company’s incident count, costs, or on-call workload.

**This project’s distinct focus:** SLO signal correctness and alert delivery—not the distributed tracing, authorization, retry control, replica reconciliation, backup integrity, schema migration, or resource right-sizing covered by earlier repository projects.

## Run the complete demo

Linux/POSIX, Python 3.11+ with SQLite. No package installation is required.

```bash
cd projects/2026-10-20-burnwatch
python -m burnwatch demo
python -m unittest discover -v
python scripts/validate.py
```

The demo creates disposable synthetic telemetry and a real loopback HTTP server. It runs the actual collector, SQL rollups, policy evaluator, outbox, inbox, and benchmark. It prints no producer keys. The scenario clock advances programmatically; five simulated minutes do not mean five minutes of wall-clock waiting.

## Implemented functionality

- Closed-minute, per-producer request/error aggregates with immutable bucket identity and exact batch retry receipts.
- Atomic raw reports, weighted service rollups, producer watermarks, and ingestion revisions. A gap rejects the whole batch.
- Multi-window burn-rate rules with exact rational threshold comparisons, sample floors, startup warmup, and explicit unknown-data states.
- Durable per-rule latches, highest-severity suppression, separate telemetry-quality transitions, and restart-safe notification identity.
- At-least-once delivery to a separate SQLite inbox with idempotent effects, including a crash between storing and acknowledging a notification.
- Producer/service-bound local API keys stored only as hashes, a bounded loopback HTTP API, and Prometheus text-format metrics with freshness indicators.
- Retention pruning that preserves required windows, an independent raw-versus-materialized integrity check, Docker, CI, and 82 automated tests.

## Alert math and policy

For a configured objective, `allowed_bad_fraction = 1 - objective` and `burn = sum(bad) / sum(total) / allowed_bad_fraction`. Counts are summed before division; producer or minute error ratios are **not** averaged.

The example uses a 99.9% objective and the following starting windows/rates from the Google chapter:

| Rule | Severity | Long window | Short window | Burn threshold |
| --- | --- | --- | --- | --- |
| fast | PAGE | 1 hour | 5 minutes | 14.4 |
| slow | PAGE | 6 hours | 30 minutes | 6 |
| ticket | TICKET | 3 days | 6 hours | 1 |

Each rule fires only when **both** window burns are strictly greater than its threshold. The objective uses integer parts per million; thresholds are bounded decimal strings converted to rational numbers. Integer cross-multiplication avoids floating-point threshold errors. An unreachable threshold is rejected at registration.

Our additional choices—not Google defaults—include a 100-request minimum in each window, two-minute telemetry freshness allowance, producer completeness barriers, immutable policies, and conservative latch holding. A request floor can miss important low-volume failures; adjust it only with an explicit understanding of the product’s risk.

The 30-day period is policy context. This lab retains/evaluates up to three-day alert windows; it does **not** claim to calculate a complete monthly SLO or actual monthly error-budget spend. The input is minute-level delta counts, not Prometheus cumulative counters or counter-reset/interpolation logic.

## Completeness and time semantics

A bucket describes `[minute, minute + 60)` in aligned Unix seconds. A producer’s watermark is an **exclusive end time**: all its buckets before that end have been reported, including explicit `total=0, bad=0` heartbeats. Watermarks cannot move backwards, seal gaps, or enter the future. Buckets cannot be corrected after acceptance; inconsistent duplicates return 409.

The service watermark is the minimum across all registered producers. SQL windows are `[service_watermark - window, service_watermark)`. Reports beyond that watermark may be buffered but are excluded from decisions until every producer catches up. This avoids quietly using one healthy producer as if it represented the whole service.

| Evidence | Rule action | User-visible behavior |
| --- | --- | --- |
| Complete, fresh, sufficiently sampled | Update latch from paired-window comparison | PAGE, TICKET, or OK |
| Incomplete history | Hold any previously active latch | WARMING; never a new healthy claim |
| Missing/stale producer | Hold active latches | STALE or PAGE_HOLD/TICKET_HOLD |
| Zero/insufficient traffic | Hold active latches | NO_TRAFFIC/LOW_TRAFFIC or a held incident |
| Missing materialized minutes | Hold active latches | INCOMPLETE; integrity check identifies discrepancies |

`quality` and `severity` are separate. Unknown evidence is not permission to resolve a known incident. Ready rules can clear their own latches; unknown rules retain theirs. PAGE takes precedence over TICKET. No repeated severity notification is created simply because another overlapping rule starts firing.

This is conservative: one stopped producer can delay fresh SLO decisions. It produces an explicit telemetry-quality event rather than inventing successful observations. A trusted producer that lies about its data cannot be detected by this protocol.

## Architecture and delivery boundary

| Component | Responsibility |
| --- | --- |
| Collector | Authenticates the producer; commits reports, rollups, watermarks, revision, and retry receipt atomically |
| Evaluator | Reads one consistent database state; evaluates complete windows; commits latches and any outbox transition together |
| Delivery worker | Writes an event to the separate inbox, then acknowledges it in the collector database |
| Inbox | Stores a single effect for each database-namespaced event ID; rejects a conflicting payload for an existing ID |
| Metrics/status | Expose the last evaluation, live watermark lag, and whether the evaluation/evidence are still current |

The two databases are deliberately **not** one transaction. A crash after inbox commit but before acknowledgement causes redelivery. Stable event IDs and inbox uniqueness prevent a second inbox effect; the transport itself is at least once, not exactly once. This is one logical inbox consumer, not a multi-destination notification service.

The server collects data but does **not** run an invisible evaluation scheduler. Invoke `tick`/`POST /v1/evaluate` and `deliver` explicitly. This keeps synchronous collection, periodic computation, durable data, and asynchronous delivery separate and testable. Moving to stateless hosting requires a real persistent-store and scheduling design; copying the local database into an ephemeral runtime is insufficient.

## Manual HTTP example

Use synthetic data only. From the project directory, set a stable start minute and register the sources:

```bash
BURNWATCH_START=$(python -c 'import time; print(int(time.time()) // 60 * 60 - 60)')
python -m burnwatch init --db demo.db --service checkout --file examples/config.json --start-minute "$BURNWATCH_START"
export BURNWATCH_API_KEY=$(python -m burnwatch mint --db demo.db --service checkout --producer api-a | python -c 'import json,sys; print(json.load(sys.stdin)["api_key"])')
python -m burnwatch serve --db demo.db --port 8080
```

In another terminal, use the same `BURNWATCH_START` and locally minted key to run:

```bash
python examples/post_minute.py --minute "$BURNWATCH_START" --total 1000 --bad 20
curl --fail-with-body -s http://127.0.0.1:8080/v1/evaluate \
  -H "Authorization: Bearer ${BURNWATCH_API_KEY}" -H 'Content-Type: application/json' --data '{}'
curl --fail-with-body -s http://127.0.0.1:8080/metrics -H "Authorization: Bearer ${BURNWATCH_API_KEY}"
```

Terminal environments are not automatically shared: set the variables privately in the second terminal or use a local process manager. Never paste the key into issues, commit it, or put it in a URL. Mint a distinct `api-b` key to submit its report, too. Until that source reports, the service watermark stays behind; after reporting, the historical windows still need to warm up. Use the complete `demo` for an immediate fully seeded scenario. The example client uses a direct loopback connection, ignoring environment HTTP proxies so the local key is not forwarded to one.

Registration is immutable. A retry must use the same policy, producer set, and original start minute; reinitializing an existing name with a new start returns a conflict. Exact batch retries return their original receipt, not a second application of the counts.

| Endpoint | Body / identity |
| --- | --- |
| `GET /healthz` | Minimal public health response |
| `POST /v1/buckets` | `batch_id`, `buckets`, optional `watermark`; service/producer come only from the bearer key |
| `POST /v1/evaluate` | `{}`; evaluates only the key’s service |
| `GET /v1/status` | Last persisted evaluation and current watermark lag for that service |
| `GET /metrics` | Service-scoped Prometheus text format |

Each bucket has exactly `minute`, `total`, and `bad`, with `0 <= bad <= total`. The API rejects `service`, `producer`, and `now` body overrides. The trusted local CLI also provides `status`, `metrics`, `verify`, `prune`, and `deliver --inbox inbox.db`. It can administer all services; it is not an untrusted-user boundary.

Do not interpret severity 0 alone as healthy. Check `burnwatch_telemetry_fresh`, `burnwatch_evaluation_stale`, and `burnwatch_evidence_complete`; an old CLEAR evaluation can coexist with missing current telemetry.

## Actual validation results

Recorded with Python 3.12.14 and SQLite 3.53.1: [test log](reports/tests.txt), [command metadata](reports/validation.json), [demo and timings](reports/demo.json).

| Check | Recorded outcome |
| --- | --- |
| Automated tests | 82 passed; test command 2.652873 s; compilation passed |
| Brief 1%-error minute | Naive short-window SLO threshold would page; paired-window policy did not |
| Sustained 20% errors | PAGE first detected at simulated minute 5 |
| Overlapping rules | Three firing rules; one severity transition to PAGE |
| Producer interruption | STALE / PAGE_HOLD, not a false recovery |
| Recovery | Fast rule reset after 5 simulated healthy minutes; long-window-only comparison reset after 56 |
| Notification crash/retry | Seven unique inbox effects; one duplicate redelivery suppressed; zero pending events |
| Local ingestion microbenchmark | 10,000 minute reports / 20 batches in 0.152602 s |
| Local evaluation microbenchmark | 200 evaluations; p50 1.4789 ms, p95 1.9970 ms |
| Retention | Removed 1,360 raw rows and 680 rollups without changing the evaluated windows |

Scenario time, counts, error patterns, and producer failures are synthetic. SQLite operations, HTTP responses, process exits, inbox effects, and execution timings are real local measurements. The ingestion figure is **aggregate-report throughput**, not live application requests per second. There is no production alert precision/recall study, real pager delivery, Google-scale performance result, or measured business savings. Hardware and reruns affect timings; delivery/evaluation cadence adds delay outside the simulated rule boundaries.

## Docker and CI

```bash
docker build -t burnwatch .
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m burnwatch demo
```

The image tests during build and runs as UID/GID 10001. The demo requires only temporary storage and loopback sockets. [GitHub Actions](../../.github/workflows/burnwatch-ci.yml) checks Python 3.11/3.12/3.13 and separately builds/tests/runs the restricted container. Docker was unavailable locally; the saved validation report explicitly records that rather than claiming a local container run. The image tag pins a Python minor version, not a hermetic digest.

## Limits and security tradeoffs

- **Local design:** one SQLite writer, no HA/replicas, no distributed watermark consensus, no PromQL engine, latency SLI, live scraper, or cloud deployment. A failed producer is not automatically removed. Policies and producer membership cannot be changed in place.
- **Explicit operations:** schedule evaluations and delivery outside the server when operating it. `prune` removes data older than the longest rule at the global watermark; it is not automatic. Receipts, expired keys, delivered notifications, and inbox history still need operator retention management.
- **Trusted boundaries:** operator, parent directories, producers, clock, and disk are trusted. Private 0600 files and database identity checks reject direct symlinks and unrelated databases; there is no encryption-at-rest, external identity provider, adversarial-storage protection, or distributed secure clock. `verify` detects rollup disagreements; it does not claim automatic repair of arbitrary corruption.
- **Bounded local API:** loopback only, 16 workers, five-second socket timeout, 64 KiB bodies, 500 reports per batch, at most 500 minutes of watermark advancement per batch, eight producers/service, 128 services, 7,200 minutes of ahead-of-watermark buffering per producer, and three-day maximum alert windows. Keys default to one hour and are capped at one day. Duplicate keys/headers, ambiguous framing, unknown fields, unaligned timestamps, non-finite numbers, and invalid counts are rejected. Add reviewed transport/TLS, identity lifecycle, quotas, and availability engineering before public exposure.
- **Semantics:** no retroactive bucket correction, arbitrary historical evaluation, repeated page reminders, pager escalation policy, or fan-out destinations. Changes require an explicitly designed new protocol. A minimum-sample guard reduces signal sensitivity; it must not be mistaken for evidence that low-traffic failures are harmless.

## Code map and interview material

| File | Main responsibility |
| --- | --- |
| `burnwatch/model.py` | Policy/input validation, exact burn math, quality and latch rules |
| `burnwatch/engine.py` | Ingestion, watermarks, rollups, evaluations, outbox, retention, metrics |
| `burnwatch/delivery.py` | Durable inbox and store-before-ack delivery |
| `burnwatch/http.py` | Bounded, identity-bound collector API |
| `burnwatch/demo.py` | Reproducible scenarios and actual local microbenchmark |
| `tests/` | 82 math, ingestion, concurrency, crash, API, security, and CLI tests |

See [primary-source notes](docs/SOURCES.md) and the [interview walkthrough with two evidence-based resume bullets](docs/INTERVIEW.md).
