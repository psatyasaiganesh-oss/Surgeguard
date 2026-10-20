# Interview walkthrough

## Two-minute framing

"Meta documented a 2021 outage in which a maintenance command disconnected the backbone, an audit-tool bug failed to stop it, dependent DNS routes disappeared, and normal recovery paths were impaired. I built an independent local control-plane lab that asks whether the whole plan remains serviceable and recoverable before allowing a synthetic link change."

Be precise: this is not Meta software and not a production network controller.

## Five-minute demo

```bash
python -m routeproof demo
```

Walk through the JSON in this order:

1. **Unsafe backbone plan:** show that later steps disconnect a data center and earlier steps lose one-failure tolerance. The whole plan is blocked before step one.
2. **DNS cascade:** show that the analyzer follows reachability to the minimum advertiser invariant.
3. **Validator timeout:** point out that absence of a verdict becomes `BLOCKED`, not approval.
4. **Approval binding:** one reviewer leaves the plan analyzed; two distinct reviewers approve the same analysis hash.
5. **Crash boundary:** the actuator commits, the controller "crashes," and a retry receives the existing deterministic receipt. Topology version increments once.
6. **Health gate and rollback:** the next step cannot run without evidence; rollback reverses the recorded prior state.
7. **Drift:** mutate an unrelated link out of band and show optimistic version fencing move the plan to `STALE`.
8. **HTTP and benchmark:** distinguish public health from authorized plan state, then explain that the latency is local graph-analysis time only.

## Core design discussion

### Why simulate the entire plan?

A first step can be safe while a later step removes the final path. Approving step-by-step without seeing the terminal and intermediate states can authorize a dangerous sequence.

### Why simulate another failure?

Maintenance reduces redundancy. A state that is connected now can still be unacceptable if the next ordinary circuit failure causes isolation.

### Why preserve management reachability separately?

The path used to repair an incident is a dependency. Modeling the management plane separately avoids counting the failing production path as the recovery path.

### Why bind approvals to the analysis hash?

A reviewer approved a particular plan against a particular snapshot and result. Re-analysis deletes approvals; topology drift stops execution.

### Why separate controller and actuator state?

It creates the real distributed-systems ambiguity: the device may commit while the controller crashes before journaling. A deterministic operation ID and actuator receipt make the retry idempotent.

### Why fail closed?

A timeout, exception, malformed result, stale snapshot, or unknown state provides no evidence that a risky change is safe. Blocking maintenance is costly but bounded; authorizing an unverified change is not.

## Likely follow-ups

- **How would you productionize it?** Replace SQLite with highly available transactional storage, use mTLS/workload identity, integrate inventory and telemetry, run analysis in a durable workflow, sign artifacts, require time-bounded approvals, and test the control plane independently from the production network.
- **What does the graph miss?** Capacity, route preference, convergence, asymmetric paths, control-plane CPU, and undeclared correlated risks.
- **Can rollback always help?** No. State may drift or the original path may no longer be healthy. RouteProof fences rollback and reports `ROLLBACK_BLOCKED`; production needs a separately tested recovery plan.
- **Why not auto-repair drift?** An unreviewed repair is another change. The lab reports drift instead of guessing operator intent.
- **How would you scale analysis?** Partition by failure domain, cache immutable topology components, parallelize independent contingencies, and preserve deterministic result ordering.
