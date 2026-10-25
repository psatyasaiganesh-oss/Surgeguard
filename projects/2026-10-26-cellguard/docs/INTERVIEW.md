# Interview walkthrough

## 60-second opening

“AWS documented a multi-tenant reliability problem: pooling every customer across every worker is efficient, but a noisy tenant or poison request can follow failover and cascade across the whole fleet. CellGuard is my independent Python/SQLite queue lab for that problem. Deterministic, zone-aware shuffle sharding maps each tenant to two of eight cells. Admission never escapes that shard, per-tenant limits stop one tenant from consuming the entire queue, and weighted round-robin claims prevent starvation. Durable leases, retry/dead-letter transitions, and fencing reject a worker that finishes after its lease was reassigned. I validate those claims with fault scenarios, integrity checks, 54 tests, a local benchmark, Docker, and three-version CI.”

## Whiteboard flow

1. Hash `(topology salt, tenant, cell)` for every cell and rank the scores.
2. Pick two cells across different zones, sort their IDs, and persist the assignment with a topology epoch and digest.
3. On submit, atomically bind `(tenant, idempotency key)` to the payload/priority digest.
4. Enforce the tenant outstanding limit, ignore unhealthy/draining admission targets, and choose only a healthy assigned cell with capacity.
5. A cell worker claims tenants round-robin; each tenant receives up to its configured weight without exceeding its global in-flight limit.
6. A claim increments the job's fencing token, persists an HMAC of a random lease capability, and sets an expiry.
7. Completion requires the live capability. After expiry, requeue creates a higher fencing token, so the old worker cannot commit through CellGuard.
8. Verification recomputes assignments, payload/result digests, state invariants, and every audit-chain link.

## Demo story

- Start with 2,400 deterministic synthetic tenants over eight cells. The selected noisy tenant fully overlaps 120 tenants (5%), compared with the simplified fixed-shard baseline of 25% and pooled baseline of 100%.
- Fill the noisy tenant's 12-job allowance and show its 13th submission rejected while a tenant sharing the exact shard still submits four jobs.
- Claim four jobs from their shared cell and observe alternating tenant identities.
- Mark one assigned cell unhealthy and show admission selects the other; mark both unhealthy and show the request fails closed instead of leaking blast radius.
- Expire a worker lease, reassign it with fencing token 2, reject token 1's completion, and accept the current worker.

## Likely follow-ups

### Why rendezvous hashing?

It is deterministic, requires no central lookup for ordinary assignment, and gives a stable ranking of cells. The demo persists assignments and a topology epoch because changing the cell set changes rankings; production topology migration needs explicit dual-read/drain orchestration.

### Why not choose another healthy cell during a full shard outage?

That would improve one tenant's availability by expanding its failure domain and possibly exporting overload to healthy tenants. CellGuard fails closed to preserve containment. A production system could use an explicitly reserved isolation cell, not silent global spillover.

### Is the 5% result guaranteed?

No. It is the observed exact-overlap fraction for one deterministic 2,400-tenant dataset with zone-aware assignments. The theoretical topology has 24 valid cross-zone pairs, but rendezvous-hash distribution and finite sample size determine the measured fraction.

### What does fencing add beyond lease expiry?

An expired worker can still be running. The increasing number lets the downstream side-effect store reject writes from an older owner. This lab enforces it at completion; a real database or API receiving side effects must also store the highest accepted fence.

### What would you change for production?

Replicate cell-local state, put the API behind TLS/OIDC, use a secret manager, export metrics/traces, add overload-aware autoscaling, make assignment migration explicit, test partitions with a fault harness, and make every external side effect idempotent plus fence-aware. I would also measure distribution skew, recovery objectives, and noisy-neighbor workloads at realistic concurrency.

## Honest boundaries

This is not Route 53 code and does not reproduce AWS infrastructure. Eight logical cells share one SQLite file and process. The benchmark measures local synchronous Python/SQLite work only. The value is in the explicit invariants, failure behavior, validation, and ability to explain tradeoffs without overstating scale.
