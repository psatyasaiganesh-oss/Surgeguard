# Source-to-design traceability

CellGuard is an independent implementation. The AWS publications establish the problem and pattern; they do not describe this repository's code or validate its results.

## Primary sources

1. Colm MacCárthaigh, AWS Builder Center, [Workload isolation using shuffle-sharding](https://builder.aws.com/content/3F06NpJ8YeoIGP8VHTw4n81pFn8/workload-isolation-using-shuffle-sharding), published December 5, 2019.
2. Colm MacCárthaigh, AWS Architecture Blog, [Shuffle Sharding: Massive and Magical Fault Isolation](https://aws.amazon.com/blogs/architecture/shuffle-sharding-massive-and-magical-fault-isolation/), published April 14, 2014.
3. AWS Well-Architected Framework, [REL10-BP04: Use bulkhead architectures to limit scope of impact](https://docs.aws.amazon.com/wellarchitected/2022-03-31/framework/rel_fault_isolation_use_bulkhead.html), March 31, 2022 edition.

## Documented facts

- AWS describes a pooled fleet as efficient for ordinary instance failure but vulnerable when a flood or poison request follows failover and cascades through every worker.
- Four fixed two-worker shards over eight workers bound a customer's full failure domain to about 25% of customers in the simplified example.
- Selecting two-worker virtual shards from eight workers yields 28 unordered combinations. The AWS Builder Center article describes a 1/28 full-overlap scope, seven times better than four fixed shards.
- Route 53 assigns domains to four of 2,048 virtual name servers, producing roughly 730 billion potential shards; AWS says domains can be assigned so no pair shares more than two virtual servers.
- AWS identifies stateless hash assignment and stateful overlap-aware assignment, including availability-zone-aware placement. It also calls out queues, rate limiters, and locks as candidates.
- AWS Well-Architected calls service bulkheads “cells” and recommends containing a failure to a subset of clients or requests.

## Project inferences and independent decisions

| Evidence or need | CellGuard decision | Status |
| --- | --- | --- |
| A noisy customer can overwhelm shared workers. | Model a multi-tenant durable work queue with per-tenant outstanding/in-flight admission and fair claims. | Project inference |
| Stateless assignment is easy to reproduce. | Use salted rendezvous hashing, then enforce distinct-zone selection when possible. | Independent design |
| A partially failed shard should retain service. | Route new work to the least-loaded healthy assigned cell only. Never escape to an unassigned cell. | Independent design |
| Worker failure creates ambiguous ownership. | Use expiring random capabilities plus monotonic fencing tokens and idempotent completion. | Independent design |
| Durable controls need evidence. | Persist policies, assignments, job state, metrics, and a hash-chained audit log in SQLite WAL. | Independent design |

The synthetic impact calculation counts tenants with the exact same two-cell shard as “fully impacted.” It is a local model, not a measurement of Route 53, another AWS service, or a real outage.
