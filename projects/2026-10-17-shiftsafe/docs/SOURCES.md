# Sources and boundaries

Reviewed October 17, 2026. The company challenge is historical; no claim is made about Stripe's current internal architecture.

| Primary source | What it supports | What ShiftSafe adds independently |
| --- | --- | --- |
| [Stripe: Online migrations at scale](https://stripe.com/blog/online-migrations), Jacqueline Xu, February 2, 2017 | Subscription data-model migration, staged dual writing and read switching, backfill, experimental read comparison, and old-data retirement while maintaining service | Local SQLite implementation; fenced leases, transactional checkpoints, revision guards, mutation receipts, explicit cutover verification, fault harness, CLI |
| [SQLite: Transactions](https://sqlite.org/lang_transaction.html), living documentation | Read snapshots, single-writer transactions, and the behavior of `BEGIN IMMEDIATE` | Gate correctness relies on comparing and switching within the same writer transaction |
| [SQLite: Write-Ahead Logging](https://sqlite.org/wal.html), living documentation | WAL behavior and its concurrency constraints | WAL plus `synchronous=FULL` for the local lab and abrupt-exit recovery tests |

No Stripe source code, datasets, credentials, proprietary implementation details, or performance figures are used. Synthetic customers, subscriptions, amounts, failures, and workload scheduling belong to this project. The lab does not implement Stripe's offline Hadoop processing, service estate, production scale, or subscription business logic.

The risk that stale snapshots overwrite newer updates and the choice of revision-based projection guards are our engineering reasoning, not a statement that Stripe experienced that failure. Expected business benefits are hypotheses; all reported measurements come from this repository's demo and tests.
