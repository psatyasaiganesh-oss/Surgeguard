# Source notes

## Primary source

Meta Engineering, ["More details about the October 4 outage"](https://engineering.fb.com/2021/10/05/networking-traffic/outage-details/), posted October 5, 2021. Accessed October 21, 2026.

## Documented facts used

The article states that a command issued during routine maintenance unintentionally took down all global backbone connections and that a bug in the audit tool prevented the command from being stopped. It describes the dependent DNS/BGP withdrawal, loss of normal and out-of-band access, disruption of internal recovery tools, onsite recovery, and careful traffic restoration.

## Independent design choices

RouteProof's graph schema, one-failure simulation, exact invariants, two-reviewer rule, approval hash, topology fencing, SQLite journals, bearer-token roles, HTTP API, canary evidence, idempotent receipts, rollback algorithm, tests, and benchmark are original choices for this educational implementation. They do not describe Meta's current or historical internal design.

## Inference

We infer that a pre-change system should evaluate dependency and recoverability invariants, and that unknown validation results should block execution. Meta's article documents the failure chain; it does not prescribe RouteProof's architecture.
