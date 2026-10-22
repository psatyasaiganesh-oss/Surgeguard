# Interview walkthrough

## 60-second framing

"Microsoft documents that concurrent writes to one item in Azure Cosmos DB multi-write regions can conflict. LWW converges by choosing one complete winner. I built an independent local replication lab to explore application-defined semantics: merge independent fields, expose same-field conflicts, and make retries safe. It is not a Cosmos DB clone."

## Five-minute demo

```bash
python -m mergemesh demo
```

1. Point to the LWW baseline: east changes `price`, west changes `availability`, and the later whole-document revision loses the price edit.
2. Show the same disconnected writes through MergeMesh. Per-field vector clocks retain both independent changes with no conflict.
3. Make both replicas change `price`. The read returns both candidates instead of guessing. Resolution supplies the exact candidate IDs and creates a causally newer version.
4. Show the delete/update race. A tombstone is a first-class candidate, so an unseen delete cannot be silently resurrected.
5. Show commit-before-ack recovery: the receiver commits, the acknowledgement is "lost," and the retry is authenticated and deduplicated.
6. Mention the tampered-envelope rejection, persisted restart recovery, audit verification, 107 tests, and containerized CI.

## Whiteboard model

For an operation from region `r` with local sequence `n`:

```text
dot = (r, n)
version = join(observed_document_clock, dot)
```

For each mutated field:

- incoming version dominates current candidate -> replace it;
- current candidate dominates incoming -> ignore the stale field mutation;
- neither dominates -> retain both as an explicit conflict.

Fields absent from the patch are untouched. This is why disjoint edits merge without treating the entire document as conflicted.

## Questions to expect

### Why not just use timestamps?

Wall clocks give a total winner but do not tell whether two writes observed each other. Vector clocks capture causality; concurrent same-field values can therefore remain visible. The cost is metadata proportional to region membership.

### Is this a CRDT?

The field state follows multi-value-register merge rules and converges for the tested delivery permutations. The system deliberately does not auto-merge values with business-specific meaning. The research source discusses formally specified replicated data types; this project is a practical implementation, not a formal proof.

### Why commit before acknowledgement?

Acknowledging first can lose a write if the receiver crashes before commit. Committing first can cause retry, so stable operation IDs and canonical-payload deduplication make retry safe.

### Why keep delete tombstones?

Deleting a row would erase causal evidence. A late update could then resurrect it. A tombstone participates in causal comparison and delete/update conflicts remain visible.

### Why require candidate IDs during resolution?

An operator may inspect two versions while a third arrives. Compare-and-set resolution fails rather than resolving an outdated conflict set.

### What would production require?

Durable streaming or an anti-entropy protocol with membership, batch transactions, TLS and asymmetric identities, key rotation, bounded metadata, tombstone garbage collection using global progress, observability, backpressure, tenant quotas, backups, and fault-injection tests across processes and machines.

## Honest limits

The benchmark is one local process using three SQLite files and synthetic records. SQLite serializes writers per database. The sync driver is in-process, keys are pre-shared, the field schema is generic, and no formal convergence proof is included. Do not describe the measured throughput as Azure or production performance.
