# ChronoCommit interview walkthrough

## 30-second summary

“I built a local distributed-transaction laboratory inspired by Google Spanner's documented external-consistency problem. One coordinator commits account transfers across separate SQLite participants. It uses MVCC versions, expected-version validation, durable two-phase commit, a bounded-clock commit wait, idempotent business requests, and restart recovery. I tested crashes at every protocol boundary and clearly limit the claim to a single-host lab—not Spanner or production infrastructure.”

## Seven-minute demonstration

Run:

```bash
python -m chronocommit demo
```

### 1. Establish the business invariant

Three accounts begin with a total of 17,000 cents and are deliberately mapped to three shards. A transfer must change both accounts or neither, and retries must not debit twice.

### 2. Follow a normal cross-shard commit

Open `coordinator.py` and trace:

1. stable tenant/key placement;
2. logical-request digest and idempotency lookup;
3. commit timestamp chosen at or above the clock interval's latest bound;
4. expected-version validation and durable participant intents;
5. durable commit decision;
6. idempotent participant finalization;
7. wait until the clock interval's earliest bound is later than the commit timestamp;
8. acknowledgement persisted and returned.

Point out that timestamp gaps after an abort are harmless. Reusing timestamps would not be harmless.

### 3. Reproduce an ambiguous success

The demo injects a crash after the first participant materializes a transaction whose commit decision is already durable. A fresh coordinator sees `COMMITTING`, replays the signed decision to every participant, and waits before ACK. The already committed participant treats the decision idempotently; the remaining participant finishes.

Contrast this with a crash while the state is only `PREPARING`: recovery aborts all known intents because no commit decision exists.

### 4. Prove retry safety

The same transfer ID returns the original transaction ID and exact post-commit snapshot. If the amount, source, or destination changes under that ID, the request digest changes and the service rejects it.

This matters because recomputing a transfer from today's balance would produce different mutations than replaying yesterday's successful request.

### 5. Show isolation and history

Two writers use the same expected version; only one can prepare. An exact read at the timestamp before the transfer still returns the old checking and savings balances because versions are immutable.

### 6. Close with evidence and limits

- 91 automated tests cover clock bounds, MVCC, stale writers, concurrent writers, prepare/decision/commit/ACK crashes, idempotency, integrity tampering, tenant isolation, and HTTP validation.
- The controlled 200-transfer local run measured 62.33 transactions/s and 17.249 ms p95.
- This uses one machine, separate SQLite files, a local coordinator, and a configured uncertainty bound. It has no consensus, region failure tolerance, hardware time, or production benchmark claim.

## Likely questions

### Why is the commit decision written before participants expose versions?

After all participants prepare, a durable decision resolves ambiguity. If a crash occurs, recovery has one legal action: finish commit. Without that durable point, different participants could make incompatible choices.

### Why does recovery abort `PREPARING` but finish `COMMITTING`?

`PREPARING` means no global commit decision was recorded. Its intents are not visible, so abort is safe. `COMMITTING` is the durable decision; aborting it would contradict participants that may already have materialized versions.

### Why is commit wait needed?

The assigned timestamp can be as late as the clock's upper uncertainty bound. Waiting until the lower bound passes it ensures real time is later than that timestamp before the client observes success. That turns uncertainty into latency.

### Is `SystemBoundedClock` equivalent to TrueTime?

No. It trusts one host clock and a configured bound. TrueTime's guarantee comes from Google's distributed time infrastructure. The class exists to make the uncertainty contract and failure behavior testable, not to claim the same guarantee.

### Does two-phase commit guarantee availability?

No. Atomicity and availability are separate. A durable but unavailable coordinator can block resolution. A production design would replicate the coordinator log with consensus, define failure domains, and add operator-safe recovery tooling.

### How are stale writes stopped?

Every mutation identifies the exact MVCC timestamp it read. Prepare compares that expectation with the latest committed version while obtaining the unique per-key intent. A mismatch aborts the transaction.

### How are duplicate requests different from duplicate protocol messages?

The coordinator binds a client idempotency key to a logical business request. Shards separately make repeated prepare/commit messages idempotent using transaction ID, timestamp, payload digest, and decision HMAC. Both layers are needed.

### Could a reader see only half a recovered transaction?

Not through this service. Snapshot reads take the same local coordinator lock, run recovery first, and then choose one read timestamp. This is a single-host mechanism and does not generalize to an unreplicated multi-region reader fleet.

### Why keep immutable versions?

They provide exact historical snapshots without blocking current writers and make commit timestamps directly observable. The cost is storage growth; production needs safe version garbage collection.

### What would you build next?

1. replicate the coordinator decision log with Raft;
2. add a tested key-version garbage-collection watermark;
3. model network partitions and unavailable participants;
4. export OpenTelemetry traces and recovery-age SLOs;
5. rotate versioned decision keys;
6. run Jepsen-style histories and linearizability checks against a multi-process deployment.

