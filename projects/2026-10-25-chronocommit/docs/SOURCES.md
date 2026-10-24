# Sources and traceability

ChronoCommit is an independent educational project. It is not affiliated with Google and does not reproduce Google's private code or infrastructure.

## Primary sources

1. James C. Corbett et al., [“Spanner: Google's Globally-Distributed Database”](https://research.google/pubs/spanner-googles-globally-distributed-database-2/), OSDI 2012.
   - Documents a scalable, multi-version, globally distributed, synchronously replicated database.
   - States that Spanner supports externally consistent distributed transactions.
   - Describes a time API exposing clock uncertainty and connects it to external consistency, non-blocking reads in the past, and lock-free read-only transactions.

2. Google Cloud, [“Spanner: TrueTime and external consistency”](https://docs.cloud.google.com/spanner/docs/true-time-external-consistency), last updated October 22, 2026.
   - Defines external consistency and its agreement with observable real-time commit order.
   - Explains timestamped immutable versions and snapshot reads.
   - Uses a deposit/debit example to show why improper timestamp ordering can expose a state that contradicts the user's observed order.

3. Google Cloud, [“Transactions overview”](https://docs.cloud.google.com/spanner/docs/transactions), accessed October 25, 2026.
   - Documents atomic read-write transactions, consistent read-only snapshots, conflict aborts/retries, and the role of short transaction duration.
   - Uses atomic balance movement as an example of work that belongs in one read-write transaction.

4. Google Cloud, [“Isolation levels overview”](https://docs.cloud.google.com/spanner/docs/isolation-levels), accessed October 25, 2026.
   - Contrasts externally consistent serializable isolation with repeatable-read snapshots.
   - Documents write-skew risk when application constraints depend on several concurrently read values.

## Source-to-feature map

| Source concept | ChronoCommit exercise | Important distinction |
| --- | --- | --- |
| Externally consistent commit timestamps | Persisted monotonic timestamp assignment plus `earliest > commit_ts` wait | Local bounded-clock model, not TrueTime |
| Atomic distributed transaction | Durable prepare/commit decision/finalize state machine | Local 2PC, not Spanner's replicated architecture |
| MVCC snapshot read | Immutable per-key versions and exact timestamp reads | Key lookup only; no query engine or GC |
| Conflict handling | Expected-version validation and terminal abort record | Optimistic local validation, not Spanner lock management |
| Real-time banking example | Synthetic cross-shard account transfer | Demonstration chosen by this project |

## Original project decisions

The following are this project's inferences and designs, not claims about Google:

- three separate SQLite participant files;
- POSIX file locking around one coordinator;
- HMAC-authenticated participant commands;
- hash-chained local audit records;
- HTTP bearer authentication and loopback-only serving;
- terminal idempotency records for aborted requests;
- fault injection at prepare, decision, participant commit, and acknowledgement boundaries;
- the exact benchmark workload and all reported local measurements.

