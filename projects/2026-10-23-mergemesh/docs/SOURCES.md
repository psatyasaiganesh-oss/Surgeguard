# Sources and claim boundary

Accessed 2026-10-23. These are Microsoft primary sources.

## Documented facts

1. [Conflict types and resolution policies when using multiple write regions — Microsoft Learn](https://learn.microsoft.com/en-us/azure/cosmos-db/conflict-resolution-policies)

   - Azure Cosmos DB multi-region writers can produce insert, replace, and delete conflicts when clients write the same item concurrently.
   - Last-write-wins (LWW) chooses the item with the highest conflict-resolution-path value; replicas converge to one winner. Delete wins against insert or replace under that policy.
   - Custom policies can invoke an application merge procedure. Unresolved or failed custom conflicts can be read from a conflict feed.

2. [Reliability in Azure Cosmos DB — Microsoft Learn](https://learn.microsoft.com/en-us/azure/reliability/reliability-cosmos-db#multiple-write-regions)

   - Multiple write regions can improve outage resilience and reduce geographically distributed write latency.
   - Changes replicate asynchronously between regions, strong consistency is unavailable for the multi-write configuration, and conflicting changes can occur.
   - The document warns that repeatedly updating the same document ID increases conflict volume and can hurt replication performance.

3. [Replicated Data Types: Specification, Verification, Optimality — Microsoft Research, POPL 2014](https://www.microsoft.com/en-us/research/publication/replicated-data-types-specification-verification-optimality/)

   - The paper describes replicated data types for eventually consistent stores and analyzes last-writer-wins registers, counters, multi-value registers, and observed-remove sets.
   - Its stated objective is formal reasoning about replicated data types and their conflict-resolution behavior.

## Our inference and design response

- **Inference:** If an application replaces a multi-field business object as one LWW item, two concurrent writers that change different fields can converge by discarding one complete revision. Microsoft documents winner selection; the "lost independent edit" consequence is our application-level example, not a quoted Microsoft incident.
- **Design response:** MergeMesh stores one causal multi-value register per field. Independent field mutations merge. Concurrent mutations to the same field, including delete/update races, remain explicit until a caller resolves the exact candidate set.
- **Operational response:** Immutable operations, authenticated envelopes, commit-before-ack delivery, deduplication, durable causal context, and a hash-chained audit log make retry and restart behavior observable in a local lab.

## Claim boundary

MergeMesh is an independent educational implementation inspired by the documented challenge. It is not Azure Cosmos DB code, a Cosmos DB emulator, a Microsoft product, or evidence of deployment at Microsoft. Results in `reports/` come from local synthetic data. They do not establish cloud-scale throughput, availability, cost savings, or production safety.
