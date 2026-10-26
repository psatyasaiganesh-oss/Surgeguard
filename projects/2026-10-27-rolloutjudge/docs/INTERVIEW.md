# Interview walkthrough

## 60-second opening

“Netflix documented that manual canary review took hours, did not scale, and could miss subtle differences between baseline and canary metrics. RolloutJudge is my independent Python/SQLite implementation of the judgment boundary. A service starts an idempotent run against a versioned policy, uploads immutable baseline/canary evidence for declared trials, and requests a version-fenced judgment. The engine validates completeness, runs tie-corrected Mann–Whitney comparisons, adjusts for multiple metrics, requires practical effect thresholds, and returns PASS, FAIL, MANUAL, or INCONCLUSIVE. It archives a detailed report and signs a receipt for a CI/CD controller, but remains passive and never deploys. I validated the lifecycle, concurrency, security, tamper detection, and HTTP boundary with 67 tests and reproducible scenarios.”

## Whiteboard flow

1. A deployment controller creates a run with service identity, release, policy ID, expected trials, and an idempotency key.
2. RolloutJudge snapshots the current policy digest and version so later edits cannot rewrite the evaluation contract.
3. Each `(trial, metric)` slot accepts exactly one baseline/canary dataset. Batch keys make lost acknowledgements safely replayable.
4. Judgment requires the current run version, which prevents a stale caller from deciding before newly accepted evidence is visible.
5. The engine cleans missing values by metric semantics, validates sample count/completeness, and compares distributions with Mann–Whitney U.
6. Benjamini–Hochberg adjusts p-values across comparisons. A regression also must exceed both absolute and relative median degradation limits.
7. A critical regression fails immediately. Missing evidence is INCONCLUSIVE. Otherwise weighted score thresholds select PASS, MANUAL, or FAIL.
8. The transaction archives the report and HMAC receipt; replaying the decision key returns the identical decision.
9. The external rollout system verifies a service-bound receipt and decides whether to promote, pause, or roll back.

## Demo narrative

- **Safe:** eight of eight metric/trial checks are evaluable, no guardrail is crossed, score 100, verdict PASS.
- **Failed:** latency and error rate regress in both zones; the critical checks force FAIL even though CPU improves.
- **Manual:** only noncritical CPU regresses in both zones; score 75 routes the release to human review.
- **Inconclusive:** error-rate evidence is absent in both zones; remaining metrics score 100, but missing expected evidence prevents PASS.
- Replay one evidence batch after a simulated lost acknowledgement; the same content returns the original result, while changed content under that key is rejected.
- Change a signed receipt from PASS to FAIL and show verification rejecting it.

## Likely follow-ups

### Why Mann–Whitney instead of comparing averages?

It is nonparametric and detects distribution shifts without assuming normally distributed latency. This implementation follows the documented algorithm family, but its normal approximation and independence assumptions are explicit limitations.

### Why combine p-values with effect thresholds?

Large samples can make tiny changes statistically significant. A production decision needs both evidence that a shift is unlikely to be random and evidence that its size violates an agreed service guardrail.

### Why multiple-comparison correction?

Testing many metrics at alpha 0.05 increases the chance of at least one false alarm. Benjamini–Hochberg controls the expected false-discovery proportion. Correlated telemetry still complicates interpretation, so the report exposes every adjusted value.

### Why a newly created baseline?

Netflix documents that comparing a fresh canary to long-lived production can include process-age effects. A similarly fresh baseline reduces that confound. The lab trusts the caller to supply comparable populations; it cannot prove routing quality.

### Why is missing evidence INCONCLUSIVE rather than FAIL?

It distinguishes “known regression” from “could not decide,” which improves diagnosis. Operationally both are fail-closed because the rollout controller should advance only on PASS.

### Why not deploy automatically?

Keeping analysis passive separates evidence interpretation from the authority to change production. It reduces blast radius, makes the result reusable, and mirrors Google's documented CAS boundary.

### What would you add for production?

Prometheus/Atlas adapters, window alignment, traffic-balance checks, seasonal baselines, autocorrelation-aware inference, exact or permutation tests, replicated storage, OIDC workload identity, KMS-backed versioned signing, externally anchored audit logs, OpenTelemetry, policy approval, and a durable rollout workflow.

## Honest boundaries

This is not Netflix Kayenta or Google CAS. Evidence is synthetic and cells/clusters are not provisioned. SQLite serializes local writers; the benchmark is sequential and machine-specific. A PASS says only that supplied evidence stayed within configured tests—it is not proof of production safety.
