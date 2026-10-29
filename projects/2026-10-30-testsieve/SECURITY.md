# Security model

TestSieve is a local educational control plane. Its security goal is to prevent an untrusted or ambiguous selection decision from silently becoming trusted evidence.

## Assets and trust boundaries

Protected assets are baseline snapshots, repository identity, observation edges, selection decisions, rollout state, shadow results, manifests, and the audit chain. The HTTP client is untrusted. The SQLite host process, signing secret, and server-side repository/token mapping are trusted.

## Implemented controls

- Repository identity is derived from a constant-time-compared bearer token, never request JSON.
- Tokens must be distinct and at least 24 characters; the HMAC secret must contain at least 32 bytes.
- JSON schemas are closed and reject duplicate keys, non-finite numbers, non-UTF-8 bodies, unexpected media types, queries, fragments, and bodies over 4 MiB.
- Paths must be bounded normalized relative paths without traversal, backslashes, NUL bytes, or absolute roots.
- Canonical request digests bind idempotency keys to content.
- SQLite transactions and uniqueness constraints make concurrent retries converge.
- Evidence and decisions are immutable through database triggers.
- Signed manifests bind repository, selection, baseline, inventory, mode, and decision.
- HMAC-chained audits and full integrity replay detect changed or detached stored evidence.
- Unknown, stale, root-scoped, unmapped, or quarantined evidence fails closed to broader execution.
- The built-in HTTP server binds only to loopback and returns no-store, nosniff, frame-denial, and restrictive CSP headers.
- The container uses an unprivileged numeric user and CI exercises it with no network, a read-only root, dropped capabilities, no-new-privileges, and a PID limit.

## Threats not solved

- A host administrator who can replace both database and signing secret can forge a new internally consistent history. External signed checkpoints are required.
- Bearer tokens are repository-level credentials, not human or workload identities. There is no OIDC, RBAC, mTLS, rotation, revocation, or secret manager.
- HMAC uses one shared symmetric key with no key identifier or online rotation protocol.
- The Python tracer is process-global, not thread-safe, and does not observe syscalls, subprocesses, directory reads, memory maps, network calls, environment variables, or database queries.
- The server has no TLS, request-rate limiting, proxy trust policy, distributed denial-of-service protection, or production concurrency tuning.
- Caller-provided file hashes and test outcomes are trusted. A compromised runner can lie about both.
- SQLite is not a multi-region or high-availability database.

## Production hardening path

Use an OS-level or runtime-native observation agent, authenticated workload identity, signed runner attestations, KMS-backed asymmetric keys, append-only external audit checkpoints, a highly available transactional metadata store, compact partitioned indexes, per-repository policy, telemetry, rate limits, and gradual shadow analysis over a statistically meaningful window. Keep full-suite escape hatches and periodically rebuild observations from trusted runs.

Report security issues privately to the repository owner. Do not include real credentials or proprietary dependency data in an issue.
