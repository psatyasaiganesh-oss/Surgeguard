# Security model

CellGuard is a local engineering lab, not a production multi-tenant service. It demonstrates bounded input, identity-derived tenancy, capability leases, fencing, compare-and-set administration, and integrity checking. It has not received an external security review.

## Implemented controls

- Tenant identity is selected from a server-side bearer-token map; callers cannot set `tenant` in request JSON.
- Worker and tenant credentials are separate, compared in constant time, and never written to application logs.
- Raw lease capabilities are returned only to an authenticated worker. SQLite stores an HMAC-SHA-256 digest, not the capability itself.
- Lease expiry plus monotonically increasing per-job fencing tokens reject late workers.
- IDs, integer ranges, JSON size, media type, unknown fields, duplicate keys, UTF-8, and non-finite values are validated.
- SQLite uses foreign keys, `FULL` synchronous WAL durability, immediate write transactions, and bound parameters.
- A hash chain covers administrative and job lifecycle audit events; job payload/result digests and assignment integrity are independently checked.
- The service defaults to loopback, returns generic unexpected-error responses, and applies `no-store`, `nosniff`, and frame-denial headers.

## Trust boundary and production gaps

- Bearer tokens arrive through environment variables and are stored in process memory. There is no TLS termination, secret manager, rotation protocol, OAuth/OIDC, user lifecycle, or RBAC.
- A token identifies one tenant; the sample does not support multiple principals, fine-grained policy, revocation propagation, or audit export.
- The standard-library HTTP server is for local demonstrations. It lacks connection/request timeouts, distributed rate limiting, graceful deployment draining, and hardened proxy handling.
- One SQLite database is a single-machine durability boundary, not a replicated cell. Anyone able to alter the database can also recompute an unkeyed audit chain.
- HMAC lease digests rely on the lease secret remaining confidential. Fencing protects this queue's writes; an external side-effecting system must persist and enforce the fencing token too.
- The scheduler serializes writers through SQLite and has no cross-process wakeup, autoscaling, overload telemetry, or Byzantine protection.

Do not expose the demo server to an untrusted network or reuse the checked-in demo secret. Generate distinct high-entropy tenant, worker, and lease secrets for local API experiments.
