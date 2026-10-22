# Security model

## Included controls

- Closed JSON schemas reject duplicate/unknown keys, non-finite numbers, excessive nesting, oversized bodies, unsafe identifiers, and oversized operations.
- Replication envelopes use per-origin HMAC-SHA-256 keys. The key ID must equal the operation origin, signatures use constant-time comparison, and secrets are neither persisted nor logged.
- Each operation ID is bound to one canonical payload. A different payload reusing an ID or origin sequence fails closed.
- The HTTP API stores only SHA-256 token digests, binds each token to one tenant, suppresses request logging, and returns `no-store`, `nosniff`, and `no-referrer` headers.
- API bearer tokens are read from `MERGEMESH_API_TOKEN`; they are not accepted as command-line arguments.
- SQLite foreign keys, full synchronous commits, WAL, transactions, integrity checks, and a hash-chained audit log protect local state and expose tampering.
- Conflict resolution requires the caller's exact candidate set, preventing a stale operator from silently overwriting a newly arrived version.

## Production gaps

This lab does not provide TLS, secret rotation, a KMS/HSM, asymmetric replica identities, authorization roles, rate limiting, process isolation, distributed membership, secure time, disk encryption, audit-log anchoring, or disaster recovery. HMAC keys are shared verifier secrets: compromise of a key can forge that origin. Put any real API behind TLS and an authenticated gateway; do not expose this demo server to an untrusted network.

Report suspected issues without including live credentials or sensitive data.
