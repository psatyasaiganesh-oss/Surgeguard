# Source notes

## Primary source

Jeffrey Dean and Sanjay Ghemawat, ["MapReduce: Simplified Data Processing on Large Clusters"](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/), OSDI 2004, pages 137–150. [Original PDF](https://research.google.com/archive/mapreduce-osdi04.pdf). Accessed October 22, 2026.

## Documented facts used

- The runtime partitions inputs, assigns map/reduce tasks, handles worker failures, and manages communication.
- Worker failure causes incomplete work—and certain completed local map work—to become eligible for re-execution.
- Deterministic operations use private temporary output and atomic publication so duplicate execution yields one logical result.
- Stragglers can result from bad disks, resource contention, and machine configuration faults.
- Near job completion, backup executions race the remaining in-progress tasks; either completion finishes the logical task.
- The paper says backup execution usually adds no more than a few percent of compute and reports its sort example was 44% slower without it.
- Successful-task counters exclude duplicate executions.

## Independent implementation choices

SpecuFlow's JSON schema, SQLite tables, lease timing, capability tokens, SHA-256 blob layout, fixed speculation threshold, retry limits, nondeterminism quarantine, Python thread runner, scenarios, tests, and benchmark are our design choices. They do not describe Google's implementation.

## Inference

We infer that tail latency can affect batch freshness and downstream deadlines and that speculative execution must be coupled to output and counter correctness. The paper documents the technical behavior; it does not quantify those business effects or prescribe SpecuFlow's architecture.
