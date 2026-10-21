# Security model

SpecuFlow is a local distributed-systems lab, not a remote multi-tenant execution service.

- Payloads are data only. The reference worker never evaluates code, imports a user module, opens a user URL, or invokes a shell.
- Closed schemas, duplicate-key rejection, depth limits, finite numbers, and byte caps constrain untrusted input.
- Each attempt receives a random capability token. Only its SHA-256 hash is stored; status, attempt lists, and audit events redact both token and hash.
- A result must name the exact shard and must commit before lease expiry.
- Result storage is immutable and content addressed. Reads verify the digest.
- Retry and speculation counts are bounded.
- SQLite and blob paths use owner-only permissions on POSIX systems.

Do not expose this implementation directly to a network. Production use requires authenticated transport, workload isolation, quotas, rate limiting, replicated storage, key rotation, monitoring, and an authorization policy around job submission and result access.
