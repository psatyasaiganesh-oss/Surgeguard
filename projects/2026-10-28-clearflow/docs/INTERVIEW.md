# ClearFlow interview walkthrough

## Thirty-second summary

“Stripe documented that financial correctness requires more than accepting transactions: independent producer events may be missing, late, out of order, or keyed incorrectly. I built ClearFlow, an independent Python/SQLite lab that stores balanced immutable events, detects uncleared fund flows, checks source completeness and timeliness, signs quality results, and repairs mistakes with reviewed compensating entries rather than history mutation.”

## Five-minute demonstration

Run:

```bash
python -m clearflow demo
```

### 1. Establish the invariant

Show that both creation and release events independently sum to zero. Explain terminal accounts versus per-flow clearing accounts. Integer minor units prevent binary floating-point money errors.

### 2. Show order independence

The healthy scenario ingests release before creation. The first event temporarily makes the clearing balance negative; the second returns it to zero. Event order does not matter because both use the same stable account identifier.

### 3. Detect a subtle wrong-key problem

The broken scenario contains both expected references and every event is individually balanced. However, creation uses `pending:correct` and release uses `pending:wrong`. One account remains +10,000 and the other -10,000. Completeness alone would miss it; clearing catches it.

### 4. Repair without rewriting history

Create an impact preview that reverses the bad release and appends a corrected release. Show that:

- the author cannot approve;
- two other actors approve the preview digest;
- a tenant-revision compare-and-set protects the preview;
- one SQLite transaction appends reversal and replacement;
- retrying after a lost acknowledgement returns the same result;
- the original event still exists.

The quality verdict changes from `FAIL` to `PASS` after correction.

### 5. Separate clearing, completeness, and timeliness

The missing-release scenario fails completeness and clearing. The late-release scenario balances and is complete, but fails timeliness. This demonstrates why a single database constraint cannot prove end-to-end data quality.

### 6. Prove the evidence

Verify the HMAC quality attestation, reject a changed verdict, then replay event and audit hashes. State clearly that tamper evidence is not the same as preventing a privileged attacker from replacing both the database and key.

## Architecture decisions to defend

### Why double-entry?

It converts conservation of value into a local invariant: every event's signed postings sum to zero. Clearing accounts then make unfinished state-machine transitions visible.

### Why manifests?

An immutable ledger cannot detect an event it never received without an independent expectation. A producer manifest supplies that expected set and deadline.

### Why append a reversal and replacement?

Updating the bad row would destroy the evidence used by prior reports and auditors. Compensating events preserve history and make the net change explicit.

### Why two reviewers and a revision fence?

Approvals should bind to the actual projected impact. The author cannot self-approve, and a changed ledger invalidates the old projection before execution.

### Why SQLite?

It gives the demo real transactions, unique constraints, foreign keys, WAL durability, and crash-safe local behavior without credentials or paid infrastructure. It is not a claim of horizontal scale.

## Likely questions

**Does a zero clearing balance prove everything is correct?**  
No. Two errors can offset, and the source may be lying or incomplete. That is why ClearFlow combines clearing with source manifests, timestamps, event tracing, and integrity verification.

**Why not use decimals?**  
Integer minor units make addition and equality exact. A real multi-asset system still needs versioned scales and explicit rounding rules for conversions.

**What happens during a duplicate request?**  
The idempotency key is bound to canonical input. An identical retry returns the prior result; changed content under the same key fails with conflict.

**Can release arrive first?**  
Yes. The clearing account may temporarily be negative, but the paired creation later returns it to zero. The grace period prevents premature clearing alerts.

**What if an unrelated event arrives after approval?**  
Execution rejects the stale global revision. That is conservative and simple; production could use affected-account or partition revisions to reduce false conflicts.

**What is the isolation level?**  
Writes use SQLite `BEGIN IMMEDIATE`, giving one serialized writer. Evaluations read and archive the report in one transaction. A production distributed ledger needs consensus or carefully partitioned ordering.

**How would you scale it?**  
Partition by tenant/currency/flow, use a durable ordered log, maintain versioned balance projections, checkpoint externally, process manifests and DQ asynchronously, and keep correction orchestration in a durable workflow. Preserve the same balanced-event and append-only invariants.

## Honest closing

“This project proves local invariants and failure handling on synthetic data. It does not connect to Stripe, banks, or real payments; it is not an accounting certification; and its benchmark is single-process SQLite. The value is the implementation of exact ledger math, missing-data reasoning, immutable correction semantics, security boundaries, tests, and reproducible evidence.”
