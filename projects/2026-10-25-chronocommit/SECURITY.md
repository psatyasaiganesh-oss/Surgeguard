# Security notes

ChronoCommit is an educational transaction laboratory. Do not expose it directly to an untrusted network or use it for real money.

## Implemented controls

- The HTTP adapter binds only to loopback addresses.
- Tenant identity is server-side configuration; request bodies cannot select a tenant.
- API and decision secrets require at least 32 bytes. Bearer tokens and HMAC values use constant-time comparisons.
- Participant `PREPARE` and `COMMIT` commands are authenticated and bound to the shard, transaction, timestamp, and payload digest.
- Reusing an idempotency key with a different logical request fails closed.
- JSON bodies reject invalid UTF-8, duplicate keys, unknown fields, excessive size, and non-finite values.
- Identifiers use a closed alphabet, and encoded transaction-route identifiers are rejected instead of normalized.
- Stored values carry SHA-256 digests. Coordinator and participant audit records form independently verified hash chains.
- A persisted clock observation rejects backwards clock movement across process restarts.
- Responses disable storage, MIME sniffing, and framing; server logs avoid paths and authorization headers.
- The container uses a non-root numeric UID; CI runs it read-only, without network access or Linux capabilities.

## Secrets

Set `CHRONOCOMMIT_SECRET` and `CHRONOCOMMIT_API_TOKEN` through the environment. Use distinct random values. Do not commit them, put them in URLs or command arguments, reuse the demo constants, or log them. Changing the decision secret for an existing database deliberately fails startup; production key rotation would need versioned keys and an overlap protocol.

## Deliberately absent

There is no TLS, external identity provider, secret manager, key rotation, database encryption, rate limiter, replicated coordinator, consensus protocol, remote attestation, or off-host audit sink. The local POSIX file lock and SQLite files are single-host mechanisms. Hash chains detect later mutation only when checked and are not a substitute for append-only remote audit storage.

Use authenticated TLS infrastructure, durable replicated logs, hardware-backed time, encrypted storage, access-control roles, observability, backups, and a reviewed incident procedure before adapting these ideas to production.

