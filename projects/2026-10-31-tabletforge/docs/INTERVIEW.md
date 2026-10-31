# Interview walkthrough

## 30-second summary

TabletForge is an independent single-node LSM storage-engine lab inspired by Google's published Bigtable paper. I built it to demonstrate the correctness boundary between a durable redo log, mutable versioned memory, immutable sorted files, and an atomic metadata manifest. It survives three injected crash points, deduplicates retries, serves snapshot-consistent reads, uses Bloom filters to avoid negative reads, compacts obsolete versions without reviving deletions, and can verify its entire on-disk state.

## Five-minute walkthrough

### 1. Start with the documented problem

Google's paper describes recent committed state in a memtable, older state in immutable SSTables, redo-log recovery, merged reads, compaction to bound file count, and Bloom filters to reduce unnecessary SSTable access. The business tension is durability versus performance: a write must not be lost, but unbounded immutable files make reads and storage increasingly expensive.

Be explicit: TabletForge is not Bigtable. It isolates the local storage-engine slice so the invariants are reproducible without cloud accounts or a cluster.

### 2. Draw the durable write path

For one batch:

1. strict validation and canonical request digest;
2. contiguous sequence allocation;
3. length-delimited WAL append containing the records, CRC, and HMAC;
4. WAL `fsync` before any visible memtable mutation;
5. versioned memtable application and idempotency entry; and
6. optional threshold flush.

If the process exits after step 4, recovery validates and replays the complete frame. Retrying the same idempotency key returns its original range; changing the payload returns a conflict.

### 3. Explain the file-publication invariant

A flush writes a temporary immutable SSTable, `fsync`s it, renames it, and then publishes its metadata in a signed atomic manifest. The manifest—not directory discovery—is authoritative. Therefore:

- a crash before the manifest leaves an orphan, which recovery quarantines while replaying the WAL;
- a crash after the manifest leaves a valid segment and possibly an old WAL frame, which recovery recognizes as already checkpointed; and
- the WAL is reset only after manifest publication.

That ordering prevents both lost committed writes and accidental adoption of half-published data.

### 4. Explain snapshot-safe compaction

Every record has a sequence. A read chooses the newest record at or below its view sequence. During compaction, for each key I retain the record visible to the current head plus the record visible to every active snapshot. A tombstone can disappear only if no retained view also needs an older value. The new segment is published before old segments are deleted.

This is the subtle failure to mention in an interview: dropping a tombstone too early can make an older value visible again.

### 5. Show measured evidence, honestly

The deterministic demo proves WAL recovery, orphan quarantine, partial-tail truncation, snapshot isolation, Bloom skipping, deletion safety, manifest tamper rejection, and full integrity replay. The suite has 88 tests. On one local CPython 3.12.14 run, the synthetic benchmark compacted 7 segments to 1, reduced 3,300 stored versions to 2,850 live records, and reclaimed 450 obsolete/tombstoned records. Those are machine-specific educational results, not production or Google performance.

## Core invariants

| Invariant | Mechanism | Test evidence |
| --- | --- | --- |
| A committed frame is replayable | WAL `fsync` precedes memtable mutation | crash after WAL `fsync`, close, reopen, read succeeds |
| One retry produces one sequence range | canonical digest under persistent idempotency key | same retry replays; changed payload conflicts; concurrent sequences unique |
| Only committed files become live | signed manifest is authoritative | pre-manifest segment is quarantined |
| Partial WAL tail is not treated as committed | truncate only incomplete final frame | complete record survives; exact trailing bytes removed |
| Corruption fails closed | CRC, HMAC, SHA-256, strict schemas | modified WAL, SSTable, and manifest are rejected |
| Deletions do not revive | view-aware tombstone retention | old snapshot reads value while head remains deleted; later release reclaims it |
| Compaction is atomic to readers after restart | manifest publication precedes input deletion | post-manifest recovery uses new segment |

## Likely questions

### Why both CRC and HMAC?

CRC cheaply identifies accidental bit corruption. HMAC authenticates content against an attacker who can modify files but does not know the secret. Neither encrypts values, and a privileged attacker with the secret remains outside the threat model.

### Why SHA-256 the whole SSTable on open?

It makes the local proof simple and ensures the manifest binds the exact file. The cost is startup I/O proportional to data size. A production engine would favor checksummed blocks, authenticated metadata, background scrubbing, and caching of verified state.

### Why immutable segments?

They eliminate in-place update recovery, permit readers to use stable files, make publication a metadata operation, and allow compaction to build replacement state before switching the manifest. The costs are write amplification and temporary disk space.

### What happens if compaction crashes?

Before manifest publication, its output is an orphan and is quarantined on restart. After publication, the new segment is authoritative; old input files may remain as unreferenced files until recovery quarantines them. Data correctness is preserved, though production cleanup would distinguish obsolete inputs from unknown orphans.

### Why not use SQLite or RocksDB?

Those would be appropriate product choices. The point here is to expose storage-engine invariants—WAL framing, publish order, snapshot retention, filters, and compaction—in code that can be explained and fault-injected.

### How would you scale it?

First introduce block checksums/indexes and tiered or leveled background compaction with I/O budgets. Then separate the storage process from the API, add metrics and recovery checkpoints, use asymmetric/KMS-backed provenance, and design a replicated log with consensus and fenced leadership. Partitioning and rebalancing come only after the single-shard state machine is precise.

## Honest limitations to volunteer

- single node and single writer;
- synchronous all-segment compaction;
- no encryption, replication, consensus, or rollback detection;
- process-local HTTP server with no TLS/OIDC;
- Python implementation and synthetic warm-cache benchmark; and
- a versioned string map, not Bigtable's distributed multidimensional data model.
