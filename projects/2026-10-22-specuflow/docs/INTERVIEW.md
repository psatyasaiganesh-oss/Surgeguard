# Interview walkthrough

## Two-minute framing

"Google's MapReduce paper documented that a few slow final tasks can hold a large parallel job open. Backup attempts reduce that tail, but duplicate execution creates a correctness problem. I built an independent local scheduler that races a bounded backup while ensuring exactly one logical output and one counter contribution."

Do not describe SpecuFlow as Google software or the local benchmark as cluster performance.

## Five-minute demo

```bash
python -m specuflow demo
```

1. Compare the baseline and speculative `completion_seconds`. The input, worker count, and injected straggler are identical.
2. Show `outputs_match`, `exactly_once_counters`, and one speculative attempt.
3. Explain why `wall_seconds_after_loser_cleanup` can exceed logical completion: the lab drains the loser to detect inconsistent output.
4. Show the abandoned lease surviving coordinator restart and becoming a retry only after expiry.
5. Show the backup winning a commit race while the primary's equal result is suppressed.
6. Show a different loser output changing a previously complete job to `FAILED/NONDETERMINISTIC_OUTPUT`.
7. Finish with checksum verification and the 200-task benchmark, explicitly labeling it local.

## Core design questions

### Why not launch every task twice?

That doubles resource use. SpecuFlow first schedules all untouched work, waits for the threshold, uses a different idle worker, and respects a strict per-task limit.

### Why store the blob before the database commit?

The immutable blob can safely exist without a reference. Committing metadata that points to a missing blob would be harder to repair. Rewriting identical content is idempotent.

### What is "exactly once" here?

Only the **logical result and counters** commit once. Physical execution is at least once and may be duplicated deliberately. External side effects are disallowed.

### Why compare a losing result?

If supposedly deterministic attempts disagree, accepting whichever finished first can hide corruption or buggy task logic. SpecuFlow quarantines the job instead.

### What happens if the scheduler crashes?

SQLite persists jobs, attempts, tokens hashes, and lease deadlines. A new scheduler instance refuses an early claim, then issues a retry once the abandoned lease expires.

## Production extension

- Replace SQLite with a highly available transactional metadata store.
- Put immutable results in replicated object storage.
- Authenticate workers through mTLS/workload identity instead of local capability transport.
- Derive speculation thresholds from per-stage latency distributions and progress rate.
- Add heartbeat progress, worker locality, resource accounting, cancellation, and backpressure.
- Isolate untrusted workloads in containers or microVMs.
- Add garbage collection based on durable manifest reachability.
