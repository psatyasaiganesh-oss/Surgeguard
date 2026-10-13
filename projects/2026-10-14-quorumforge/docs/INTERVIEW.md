# QuorumForge interview walkthrough

## 30-second introduction

QuorumForge is my independent Python implementation of a failure-aware replicated key-value lab inspired by Amazon's published Dynamo paper. I wanted to demonstrate the tradeoff between availability and consistency rather than only describe it. The project implements consistent hashing, `N/R/W` quorums, vector clocks, sloppy quorum, hinted handoff, read repair, tombstones, and Merkle-based anti-entropy, then fault-tests them with 35 automated tests and a deterministic 600-write campaign.

## Five-minute walkthrough

### 1. Begin with the business requirement

Amazon's paper explains that infrastructure components fail continuously and that services such as shopping carts need persistent state to remain readable and writable through failures. A strict primary or strict replica set can turn a temporary node outage into a customer-facing write outage.

QuorumForge models seven nodes with a replication factor of three. Each key has three intended owners selected from a consistent-hash ring.

### 2. Explain placement and quorum

SHA-256 maps both virtual nodes and keys into a 64-bit ring. Walking clockwise produces a preference list of distinct physical nodes. Virtual nodes spread ownership more evenly: in the committed 6,000-key experiment, the placement coefficient of variation fell from 0.718619 with one token to 0.109226 with 64 tokens.

The default configuration is `N=3`, `R=2`, `W=2`. `N` is the replica count; `R` and `W` are the minimum read and write responses. Requiring `R + W > N` gives a quorum-like overlap under the normal replica set, though sloppy quorum can weaken that simple argument because fallback nodes may participate.

### 3. Show failure handling

During the experiment I fail two nodes. A strict strategy that contacts only intended owners rejects 76 of 600 writes. QuorumForge walks farther around the ring, stores versions on healthy fallback nodes, and records the missing intended owner as a hint. It accepts 600/600 writes and creates 453 hints.

After the failed nodes recover, holders transfer those versions to the intended owners and delete their hints. The experiment delivered 453/453 hints, left no pending hints, and restored all keys to three intended replicas.

This is the availability gain, but it is not free: hinted data needs capacity, retention, monitoring, and backpressure in a real system.

### 4. Explain causality instead of timestamps

Every version carries a vector clock. If all counters in clock A are less than or equal to clock B, A is an ancestor and can be discarded. If neither clock dominates, the versions are concurrent siblings.

The demo partitions six nodes into two groups. Both clients start from the same cart context and add a different item. Their writes increment different clock components, so neither overwrites the other. After healing, a read returns both siblings. The client merges the cart values and writes with the merged context, creating one version that causally supersedes both branches.

Last-write-wins would be simpler but could silently lose one cart update, especially if wall clocks are skewed.

### 5. Finish with permanent divergence and evidence

Hinted handoff handles known temporary failures. Anti-entropy handles replicas that silently diverge. QuorumForge computes fixed-bucket Merkle trees for the ranges shared by a replica pair. Equal branches stop traversal; divergent leaves identify buckets that need key comparison.

I removed 12 copies from one replica. Repair found all 12, copied 12 versions, and made the roots equal while examining 42 of 157 shared keys instead of a full key scan. All 11 demo invariants passed. These are in-process synthetic correctness measurements, not production Amazon results.

## Likely interview questions

### Is `R + W > N` enough for strong consistency?

Not generally. It creates overlap for the same replica set, but sloppy quorum may contact different substitute nodes during partitions. Concurrent writes, network timing, client contexts, and reconciliation still matter. QuorumForge intentionally exposes siblings instead of claiming linearizability.

### Why not use a timestamp for last-write-wins?

Wall clocks can drift, and last-write-wins discards a branch even when updates are logically concurrent. Vector clocks identify causality directly, but grow with the number of coordinators and require application reconciliation.

### What is stored in a hint?

The fallback node stores the version together with the intended owner. When both nodes are available and connected, handoff copies every version to that owner and removes the hint only after delivery in this simulator.

### What if a client retries after a reported failed quorum?

The original attempt may have reached some replicas. A retry without the prior context can produce a concurrent sibling. A real client should use an idempotency token or read/reconcile before retrying; the system must never assume that an error means no write occurred.

### How does read repair differ from anti-entropy?

Read repair is foreground and opportunistic: a client read compares returned replica versions and updates reachable stale owners. Anti-entropy is background and proactive: Merkle hashes locate divergence even for cold keys that clients do not read.

### Can a Merkle root prove which replica is correct?

No. It proves equality or locates divergence. Causal metadata, multiple replicas, application semantics, or an authoritative source are still needed to decide the correct state. This lab repairs missing versions; equal clocks with different values are treated as corruption and quarantined as an error.

### Why fixed buckets?

They keep bucket boundaries stable when a key is added or removed, which makes the comparison deterministic. The cost is false-positive work: every key in a divergent bucket is examined. More buckets reduce that work but increase metadata.

### How would you make it production-ready?

I would separate nodes into processes, persist a write-ahead log plus sorted storage, use authenticated RPC with deadlines, implement gossip and rack-aware placement, persist bounded hints, schedule anti-entropy, add tombstone GC with repair guarantees, support membership changes and streaming, expose latency/queue/repair metrics, and test with process kills and network fault injection.

### What would you monitor?

- Read/write availability and latency by quorum configuration
- Pending hints by age, bytes, target, and delivery failures
- Sibling counts and reconciliation rate
- Under-replicated keys and Merkle root mismatches
- Read-repair and anti-entropy work rates
- Replica storage skew and virtual-node ownership
- Tombstone age and garbage-collection safety windows

## Honest framing

Say: “I independently implemented a deterministic lab inspired by mechanisms described in Amazon's Dynamo paper.”

Do not say that you built Amazon Dynamo, that DynamoDB uses this source code, or that these in-process timing results predict production performance. The cited failure model and Dynamo techniques come from Amazon's paper; QuorumForge's implementation and measurements belong to this project.
