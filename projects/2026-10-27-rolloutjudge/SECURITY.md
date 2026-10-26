# Security and statistical model

RolloutJudge is a local engineering lab, not a production release-control service. It demonstrates service-bound identity, immutable evidence slots, signed verdicts, durable idempotency, statistical guardrails, and integrity verification. It has not received an external security or statistical review.

## Implemented controls

- The HTTP server derives the service identity from a server-side bearer-token map; request bodies cannot select another service.
- Tokens must be distinct, contain at least 24 characters, are compared in constant time, and are not logged.
- Policies are snapshotted into a run. Later policy edits cannot silently change an in-progress or archived judgment.
- Run creation, evidence batches, and judgment have idempotency keys. A key reused with different content fails closed.
- Evidence slots become immutable after first write; optimistic run versions prevent judging stale/incomplete state.
- HMAC-SHA-256 receipts bind service, release, run, policy, trials, report, score, verdict, version, and judgment time.
- Audit entries form an HMAC chain. Integrity verification also recomputes policy, request, trial, evidence, and report digests.
- Strict UTF-8 JSON rejects duplicate keys, non-finite values, unknown fields, unsupported media types, unsafe IDs, oversized requests, and out-of-range values.
- SQLite uses bound parameters, foreign keys, immediate write transactions, `FULL` synchronous WAL durability, and a busy timeout.
- The service defaults to loopback and returns `no-store`, `nosniff`, frame-denial, and generic unexpected-error responses.

## Statistical boundary

- Mann–Whitney U uses a two-sided, tie-corrected normal approximation. This is not an exact small-sample test.
- Benjamini–Hochberg adjustment reduces false discoveries across one judgment but does not make correlated metrics independent.
- Median effect guardrails require both absolute and relative degradation thresholds to be exceeded. They are policy choices, not universal safety limits.
- Time-series values may be autocorrelated, seasonal, imbalanced, or affected by traffic-routing bias. This lab does not correct those effects.
- A PASS means the supplied samples did not cross configured checks. It does not establish absolute service health or prove that a full rollout is safe.
- Missing or incomplete expected evidence produces INCONCLUSIVE. Pipeline consumers should advance only on a verified PASS.

## Production gaps

- There is no TLS, OIDC/workload identity, KMS/HSM integration, secret rotation, RBAC, tenant provisioning, or external policy approval.
- One SQLite file is a single-machine boundary. There is no replication, consensus, distributed locking, regional failover, or object-store archive.
- The standard-library HTTP server lacks proxy hardening, connection budgets, distributed rate limiting, and graceful rollout draining.
- The same HMAC secret signs receipts and audit entries. A production design should use versioned, purpose-separated keys with externally anchored audit heads.
- Evidence is supplied by a trusted caller. There is no Prometheus/Atlas integration, provenance attestation, sampling-ratio validation, or clock-window verification.
- RolloutJudge is intentionally passive and never changes traffic. A production rollout controller must verify the receipt and separately own promotion/rollback authorization.

Do not expose the demo server to an untrusted network or reuse the checked-in demo secret.
