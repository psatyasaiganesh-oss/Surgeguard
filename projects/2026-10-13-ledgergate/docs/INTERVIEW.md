# LedgerGate interview walkthrough

## 30-second introduction

LedgerGate is my independent implementation of a reliable payment-webhook consumer inspired by Stripe's public webhook documentation. Stripe documents that deliveries can be retried, duplicated, and arrive out of order. I built a dependency-free Python service that authenticates the raw request, durably records it in a transactional inbox, projects only newer state, and uses a transactional outbox plus receiver idempotency to make side effects crash-safe. A deterministic local experiment and 28 tests exercise the failure cases rather than only the happy path.

## Five-minute walkthrough

### 1. Start with the failure model

The key realization is that an HTTP 2xx is not a correctness boundary. Four independent failures matter:

1. The same event can be delivered more than once.
2. Different events can describe the same logical transition.
3. An older event can arrive after a newer one.
4. A process can crash before or after a downstream side effect while its local acknowledgement has a different state.

Stripe's public docs establish the first three delivery conditions and recommend quick asynchronous processing. The fourth follows from the distributed-systems failure window between two systems; it is my design inference.

### 2. Show the intake boundary

`security.py` verifies HMAC-SHA256 over the exact raw bytes and signed timestamp. It accepts rotation secrets, checks every signature candidate with `compare_digest`, and rejects stale or future replay. `schema.py` then validates an exact, bounded event contract and rejects duplicate JSON keys.

Only after both checks does `service.py` call `store.ingest`. The SQLite transaction classifies a delivery as accepted, exact duplicate, semantic duplicate, or conflicting reuse of an event ID. An accepted request returns 202 because processing is asynchronous; duplicates return 200 so the sender does not keep retrying; a conflict returns 409 so it remains visible.

### 3. Explain monotonic state

The projector selects a pending event inside `BEGIN IMMEDIATE`. It compares the synthetic resource version with the stored payment:

- newer: update the projection;
- older: mark stale;
- same version and same state: mark stale;
- same version but different state: quarantine as conflict.

The version exists only for this local model. In a real Stripe adapter I would retrieve authoritative object state when event ordering matters, because Stripe does not promise this sequence field.

### 4. Explain the outbox and the impossible gap

When a payment succeeds, the state update and outbox insert commit in one transaction. This closes the gap where state commits but work is never published.

There is still an ambiguous window: the receiver can accept a message and the dispatcher can crash before marking the outbox row delivered. LedgerGate deliberately injects that crash. On retry, the sink sees the same `fulfill:<payment>:v<version>` key. It records a second attempt but does not repeat the effect. This is at-least-once delivery with an idempotent consumer—not a false claim of exactly-once networking.

### 5. Close with measured evidence

The committed experiment shuffled 978 deliveries for 400 payments. The naive authenticated consumer ended with 167 wrong final states and 103 repeated fulfillment attempts. LedgerGate admitted 800 unique transitions, detected 76 exact plus 62 semantic duplicates, rejected 40 invalid/stale signatures, projected all 400 payments with zero state mismatches, and produced exactly 300 unique fulfillment effects. All eight experiment invariants and six crash-recovery checks passed.

These are deterministic synthetic results on one local environment, not production Stripe traffic or a throughput claim.

## Questions I would expect

### Why are event-ID checks alone insufficient?

They stop redelivery of the same event object. Stripe notes that separate Event objects may represent the same logical event. LedgerGate therefore has both an `event_id` primary key and an effect key derived from event type, object ID, and synthetic version.

### Why store the raw payload?

The raw bytes are the signed evidence. Keeping them with the digest supports audit and deterministic replay. Retention and encryption policies would be required in production because event payloads can contain sensitive business data.

### Why respond 202 rather than 200?

202 communicates that intake is durable but business processing is pending. Stripe mainly needs a successful 2xx; this distinction helps the local API communicate its own state. A production integration should confirm how its provider treats every chosen status.

### Is the implementation exactly once?

No. It provides at-most-one committed inbox transition per key, atomic local state/outbox writes, and at-least-once dispatch to an idempotent receiver. Network-level exactly-once is not claimed.

### What if two workers claim one outbox row?

Claiming runs under `BEGIN IMMEDIATE`, which serializes SQLite writers. The chosen row receives a unique lease token. Acknowledgement updates only an inflight row with that token, so a stale worker cannot acknowledge a lease it no longer owns.

### Why can the demo use a zero-second lease?

Only to make crash recovery immediate and deterministic. The long-running worker also uses zero because the included sink is local and idempotent. A real external call should use a nonzero lease longer than its expected timeout, plus jittered retries, heartbeat/lease extension where appropriate, and monitoring.

### What happens when the same event ID has different bytes?

It returns `event_conflict`, writes an audit record, and does not replace the original event. Silently accepting the second payload would hide corruption or malicious reuse.

### What is the largest scaling bottleneck?

SQLite serializes writers, and every operation opens a connection. The next design would use PostgreSQL with `SELECT ... FOR UPDATE SKIP LOCKED` or a partitioned inbox, a managed queue for wakeups, and multiple stateless workers. The uniqueness constraints and idempotency contracts remain the same.

### How would you observe it in production?

I would publish counters and age histograms for signature failures, duplicate classes, conflicts, inbox depth/oldest age, processing latency, outbox depth/oldest age, attempts, lease expirations, and dead letters. Alerts should focus on growing age and conflicts rather than raw duplicate counts, because retries can be normal.

### How would you adapt this to real Stripe events?

Use Stripe's official SDK for signature verification, preserve the raw framework body, store Stripe Event IDs, maintain an allow-list of subscribed event types, and fetch the latest PaymentIntent or relevant object before order-sensitive transitions. The business operation would run from the outbox using a stable idempotency key and an idempotent downstream contract.

### What would you build next?

1. PostgreSQL-backed concurrent workers and a migration strategy.
2. Dead-letter and exponential-backoff scheduling with bounded attempts.
3. OpenTelemetry traces and Prometheus metrics.
4. Property-based state-machine tests and process-kill fault injection.
5. Payload encryption/retention controls and authenticated admin endpoints.
6. A real Stripe adapter behind the existing domain boundary, tested with Stripe CLI fixtures.

## Honest project framing

Say: “I independently built LedgerGate after studying Stripe's public documentation about webhook delivery behavior.”

Do not say: “I built Stripe's webhook system,” “Stripe uses this architecture,” or “this achieved production throughput.” The implementation, architecture choices, and synthetic measurements belong to this project; the cited delivery facts belong to Stripe's documentation.
