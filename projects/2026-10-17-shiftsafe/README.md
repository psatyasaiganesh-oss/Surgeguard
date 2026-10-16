# ShiftSafe — migrate subscription schemas while writes continue

A working Python/SQLite migration engine with atomic dual writes, resumable backfill, lease fencing, shadow reads, verified cutover, rollback, and bounded legacy cleanup. Python 3.11+; standard library only. The default demo needs no network, cloud account, or API credentials.

**Independent implementation inspired by a documented Stripe engineering challenge.** This is a local engineering lab, not Stripe software or work performed for Stripe.

## Sourced problem and business impact

**Documented facts:** Stripe's [Online migrations at scale](https://stripe.com/blog/online-migrations), published **February 2, 2017**, describes moving subscription data out of customer records while preserving API availability and accurate values. Its approach uses phased dual writes, backfill, experimental read comparisons, and eventual retirement of the old representation. Source rechecked **October 17, 2026**.

**Our inference and implementation:** A migration tool should make partial progress durable, prevent old snapshots from overwriting newer writes, and refuse a read switch when the two representations differ. ShiftSafe demonstrates those safeguards with synthetic subscription records. The potential business benefit is reducing migration-related data loss, manual recovery, and service disruption; no customer impact or production savings have been measured.

See [source and assumption notes](docs/SOURCES.md). This project addresses schema evolution, a different problem from LedgerGate's webhook delivery correctness.

## Run locally

From the repository root:

```bash
cd projects/2026-10-17-shiftsafe
python --version                     # Python 3.11 or newer
python -m unittest discover -v
python -m shiftsafe demo
```

The demo creates a fresh temporary database and removes it on exit. It seeds 1,000 customers with three subscriptions each, migrates in batches of 100, interleaves 400 writer operations, restarts a worker, injects corruption, repairs it, exercises rollback, and retires the legacy data. Output is JSON with safety outcomes and real local timings. Successful output has `success: true`, `verified_before_finalization.mismatches: 0`, and `final_status.phase: FINALIZED`.

```bash
python -m shiftsafe demo --customers 5000 --writes 2000 --batch-size 100
```

The burst schedule makes overlap reproducible: each batch is captured before a writer burst; most batch application runs concurrently with that burst, and the final checkpoint waits for its burst to finish. This is a controlled local workload, not an unconstrained traffic generator. Data and failure outcomes are reproducible; elapsed time and lock scheduling vary.

## Architecture and correctness rules

One SQLite WAL database holds both layouts. `legacy_customers` embeds a canonical JSON subscription array. `projection_state` plus `subscriptions_v2` stores the normalized layout. `customers` retains identities, revisions, and deletion tombstones. Separate tables hold idempotency receipts, migration/checkpoint/lease state, and operational events.

| Phase | Application reads | Application writes | Next safe action |
| --- | --- | --- | --- |
| LEGACY | Legacy | Legacy | `start` captures a high watermark and enables dual writes |
| DUAL | Legacy | Both, legacy first | Backfill bounded batches; then `shadow` |
| SHADOW | Legacy; compare new in the same read snapshot | Both, legacy first | `cutover` verifies all customers and switches atomically |
| NEW | New; compare legacy | Both, new first | Roll back to DUAL, or `finalize` after a fresh verification |
| FINALIZED | New | New only | Remove old rows in bounded `cleanup` batches |

1. **Atomic business writes.** Both representations, identity revision, idempotency receipt, and data version commit together. An exception or process exit before commit leaves no partial business operation. A retry with the same key/body returns the original revision; a different body conflicts. Optional expected revisions prevent lost updates.
2. **Backfill without stale overwrite.** A batch captures a read snapshot and releases its read transaction. Apply runs separately, so live writes can intervene. A projection revision equal to or newer than the captured revision is preserved. Tombstones carry revisions too, preventing resurrection. Inserts behind the cursor or above the starting watermark are dual written immediately.
3. **Crash-safe progress and fencing.** Batch projection updates and cursor advancement share a transaction. A worker lease has an owner, expiry, and increasing fence. Apply checks the current lease and cursor under the writer lock; an expired worker cannot commit after a successor takes over. Workers renew between batches. Expiry is checked when the apply transaction obtains the lock; SQLite serializes the whole apply transaction.
4. **Current evidence gates cutover.** Historical shadow counters are diagnostic. Cutover and finalization each perform a fresh full comparison and phase update inside one `BEGIN IMMEDIATE` transaction. A writer cannot slip between verification and the switch. The comparison checks values, revisions, deletion flags, missing rows, and identity metadata. Digests aid reporting; direct record equality determines the result.
5. **Rollback has an explicit boundary.** NEW still maintains legacy state, so rollback keeps writes made after cutover. FINALIZED stops legacy writes and permanently disables rollback and legacy-based repair. Cleanup is never automatic on a normal operator command.

`verify` reports at most 20 example customer IDs, aggregate counts, and digests, not subscription contents. Successful transitions and batches are durably recorded in `events`; failed gates return structured CLI diagnostics.

## Operate a persistent lab database

Use a new, operator-owned path. Reuse an operation ID only for an identical retry.

```bash
python -m shiftsafe --db /tmp/shiftsafe-lab.db init
python -m shiftsafe --db /tmp/shiftsafe-lab.db put 1 \
  --file examples/subscriptions.json --operation-id create:1 --expected-revision 0
python -m shiftsafe --db /tmp/shiftsafe-lab.db start
python -m shiftsafe --db /tmp/shiftsafe-lab.db backfill --batch-size 100
python -m shiftsafe --db /tmp/shiftsafe-lab.db shadow
python -m shiftsafe --db /tmp/shiftsafe-lab.db get 1
python -m shiftsafe --db /tmp/shiftsafe-lab.db verify
python -m shiftsafe --db /tmp/shiftsafe-lab.db cutover
python -m shiftsafe --db /tmp/shiftsafe-lab.db status
python -m shiftsafe --db /tmp/shiftsafe-lab.db rollback
python -m shiftsafe --db /tmp/shiftsafe-lab.db shadow
python -m shiftsafe --db /tmp/shiftsafe-lab.db cutover
python -m shiftsafe --db /tmp/shiftsafe-lab.db finalize
python -m shiftsafe --db /tmp/shiftsafe-lab.db cleanup --batch-size 100 --confirm DROP_LEGACY
python -m shiftsafe --db /tmp/shiftsafe-lab.db events --limit 100
```

`backfill --max-batches N` can stop after a bounded amount of work; rerunning it resumes the committed cursor. `repair CUSTOMER_ID` reconstructs the projection from the current legacy record before finalization. `delete CUSTOMER_ID --operation-id KEY [--expected-revision N]` writes a tombstone. Use `--help` for commands. Expected operational failures return JSON on stderr and exit code 2. Parser usage errors also exit 2.

The library exposes the same methods through `from shiftsafe import Engine`. `capture` and `apply` are separate for testing or controlled workers; captured `Batch` and `Lease` objects are trusted internal objects, not input formats or authorization credentials.

## Validation and measured results

The committed [demo report](reports/demo.json), [validation metadata](reports/validation.json), and [test log](reports/tests.txt) record the actual run. The suite covers abrupt process exits between dual writes and after cursor updates, stale snapshots and tombstones, lease takeover, optimistic concurrency, the cutover lock boundary, corrupted/missing projections, rollback, validation failures, and a complete CLI workflow. A seeded model checks 300 mutations across migration phases; a separate four-writer test checks 120 increments without lost updates.

Recorded local run on Python 3.12.14 / SQLite 3.53.1, Linux:

| Measurement | Actual result |
| --- | --- |
| Automated tests | 59 passed |
| Customers / interleaved writer mutations | 1,000 / 400 |
| Backfill batches / source rows scanned | 10 / 1,000 |
| Final comparison | 0 mismatches across 1,000 customers |
| Controlled concurrent load interval | 1.022958 seconds |
| Writer operations / load interval | 391.02 per second |
| Writer latency, p50 / p95 / maximum | 1.901 / 5.825 / 20.005 milliseconds |
| Local Docker execution | Not run: Docker is unavailable locally |

The rate uses the entire controlled load interval as its denominator, including backfill work and synchronization. It is not an isolated database capacity measurement. Safety outcomes are the main result; timings are one hardware-dependent sample.

Run `python scripts/validate.py` to refresh these reports. The report states whether Docker was available; a configuration file alone is not evidence of a successful container build. The repository's [ShiftSafe CI](../../.github/workflows/shiftsafe-ci.yml) runs Python 3.11/3.12/3.13 tests and a separate container job.

## Container demo

```bash
docker build -t shiftsafe .
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m shiftsafe demo
```

The image runs the tests while building and executes the demo as UID 10001. Persistent operator use requires an explicitly writable directory owned by UID 10001 and a `--db` path inside it; the default readonly demo only uses temporary storage.

## Security, tradeoffs, and limits

- There is no HTTP listener, remote authentication surface, SQL console, shell execution, external telemetry, or real payment processing. Use synthetic records. Values are bound SQL parameters; identifiers, money, currencies, lists, input size, operation IDs, and batch sizes are validated. New DB files use mode 0600 on POSIX; direct symlinks are rejected. The containing directory is trusted, so this is not protection against an attacker swapping filesystem paths.
- Both layouts share one transactional database. This deliberately avoids the partial-failure problem of writing two independent databases. A real cross-database migration needs durable change capture/outbox replication, lag bounds, ordering, and reconciliation; this implementation does not provide them.
- SQLite permits one writer. Transactions serialize and can wait up to the configured timeout, then raise `Busy`. A client retries the whole request with the same idempotency key. WAL allows read snapshots; it does not make writes parallel. Full verification is O(customers + subscriptions), uses in-memory record digests, and holds the writer lock during a gate. Larger deployments need partitioned verification plus a proven change boundary; this local lab does not demonstrate production zero downtime.
- All business writes must go through the Engine. Manual SQL can violate its invariants; only the synthetic fault harness does that intentionally. Lease coordination uses one host's trusted clock and SQLite lock, not distributed consensus. An untrusted caller must never supply serialized Batch objects.
- Subscription identity is unique within a customer. There are at most 100 subscriptions per customer. Tombstones, events, and idempotency receipts are retained indefinitely; retention and compaction policies are future work. No multi-tenant authorization, encryption at rest, replication, multi-region failover, or automated production DDL is included.
- Shadow comparison metrics are stored after the read snapshot in a separate transaction and can encounter writer contention. A missing projection is reported as a mismatch while SHADOW serves legacy. Runtime/database corruption may still produce an error. This is a correctness lab, not a general-purpose migration service.

For resume wording and a technical discussion outline, see [the interview walkthrough](docs/INTERVIEW.md).
