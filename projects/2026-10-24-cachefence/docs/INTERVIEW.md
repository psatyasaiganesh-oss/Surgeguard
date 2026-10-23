# Interview walkthrough

## 90-second version

“Meta documented a subtle cache invalidation race: a read starts fetching version 1, version 2 is written and invalidated, then the slow version-1 fill arrives last. Versions help, but if value eviction also removes the latest version, the old fill can still win. I built CacheFence as an independent Python/SQLite lab around that failure.

The key design is to store an evictable value separately from a durable version-and-content-digest fence. An invalidation advances the fence and evicts older data. A fill below the fence is rejected and the cache-aside service refetches once. Equal version with a different digest is quarantined because choosing either payload would hide corruption.

The origin commits each mutation and its outbox event in one transaction. Workers claim events with expiring capabilities, authenticate them with HMAC, commit or deduplicate at the cache, and only then acknowledge. The demo loses an acknowledgement intentionally and proves the retry creates one logical application. A persistent probe samples client-observable mismatches, retries short-lived lag, and alerts only after a threshold.

I verified it with 82 tests, a deterministic race demo, 32 concurrent cold readers that caused one origin read, restart/integrity checks, a non-root restricted container, and a local benchmark. It is a lab, not a Meta system or cloud-scale performance claim.”

## Whiteboard the stale-fill race

1. Origin has `(v1, 42)`; a cache fill captures it.
2. Origin commits `(v2, 43)` and an outbox event atomically.
3. Cache applies the invalidation and records durable fence `v2`.
4. Value eviction may remove `43`, but does not remove fence `v2`.
5. Delayed fill `v1` arrives; `v1 < fence v2`, so it cannot install.
6. Cache-aside service refetches `v2`, verifies its digest, and installs it.

The naive demo performs steps 1–3 and then accepts step 5, ending with cache version 1 while the origin is version 2. The guarded demo records one rejection and returns version 2.

## Why a digest as well as a version?

Ordering answers “which version is newer?” It cannot explain two different payloads both labelled version 4. Meta documented that class of bug. CacheFence hashes canonical `{deleted, value}` content. If equal versions have different digests or tombstone states, it records an incident and fails closed instead of treating them as equivalent.

## Why keep the fence forever?

The lab cannot prove when all old fills and invalidations have disappeared. Deleting a fence too early reopens the exact race being prevented. A real design needs a retention bound, epoch/watermark protocol, or upstream guarantee before compacting it. Unbounded fence metadata is an explicit tradeoff here.

## Why transactional outbox and commit-before-ack?

- Without one source transaction, a record can commit without its invalidation or an event can exist for a rolled-back write.
- If the sender acknowledges before the cache commit, a crash can permanently lose the invalidation.
- Committing first means an acknowledgement can be lost. Stable event IDs and canonical payload hashes make that replay safe.
- The random lease token prevents another worker from acknowledging a claim it does not own; expiry enables recovery.

## Why is a cache miss “consistent” to the probe?

The invariant is client-observable behavior, not byte-for-byte internal equality. In this cache-aside service, a miss consults the origin, so an absent entry does not make the caller observe stale data. A present stale value does. That assumption must change for systems that return a default on miss or cannot reach the origin.

## Likely follow-up questions

### Does this guarantee linearizability?

No. Source writes are serialized per SQLite database, but invalidation propagation is asynchronous. The design prevents known older fills from crossing an observed fence and eventually makes cached reads match the source. It does not turn separate origin and cache systems into one linearizable transaction.

### What happens if HMAC verification fails?

The receiver does not mutate state. The delivery attempt is failed and can retry until the configured dead-letter threshold. The API and audit log do not expose the secret or lease capability.

### Why refetch only once?

It bounds amplification. If a second fetch is already below the fence, the origin/cache relationship is not making progress and an unbounded loop could overload the source. The lab raises a clear error; production could back off, queue repair, or fail the request.

### How would you scale it?

Partition ownership by tenant/key, use an ordered durable log or transactional CDC instead of an in-process outbox worker, persist fences on cache hosts or a colocated metadata store, add per-partition watermarks for fence compaction, use KMS/asymmetric authentication, and deploy probes independently with sampling and rate-limited origin bypass.

### What would you monitor?

Outbox age by percentile, pending/dead events, lease expiry and retry rate, stale-fill rejections, equal-version quarantines, fence/value cardinality, cache hit and negative-hit rates, origin reads per cache miss, probe mismatches at each age threshold, and repair time.

### What did the benchmark prove?

Only that the complete local path runs reproducibly and reports its own latency/rate without credentials or cloud services. It is useful for regression comparison on the same environment, not for capacity planning or comparison with Meta.
