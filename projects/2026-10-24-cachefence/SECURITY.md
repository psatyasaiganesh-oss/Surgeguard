# Security notes

CacheFence is an educational backend and should not be exposed directly to an untrusted network.

## Implemented controls

- The HTTP adapter accepts only loopback bind addresses.
- Tenant identity is server-side configuration; request bodies cannot select a tenant.
- Bearer and signing secrets must be at least 32 bytes and are compared with constant-time functions.
- Invalidation signatures cover canonical payloads and bind the signing key ID to the event origin.
- JSON rejects duplicate keys, non-finite numbers, invalid UTF-8, excessive size/depth/items, and unknown fields.
- Keys use a closed identifier alphabet; encoded and nested route keys are rejected rather than normalized.
- Writes support optimistic compare-and-set versions.
- Outbox acknowledgement requires an unguessable hashed capability and an active lease.
- Equal-version payload contradictions and persisted-content corruption fail closed and are quarantined.
- Responses disable storage and MIME sniffing; errors avoid stack traces and secrets.
- The container uses a non-root numeric UID; CI runs it read-only without network or Linux capabilities.

## Secrets

Set `CACHEFENCE_API_TOKEN` and `CACHEFENCE_SIGNING_KEY` through the environment. Use distinct random values. Do not commit them, pass them in URLs/command arguments, reuse the demo/test constants, or log authorization headers. The demo constants authenticate only disposable local state.

## Deliberately absent

There is no TLS, external secret manager, asymmetric identity, HMAC rotation overlap, revocation, multi-tenant token directory, distributed rate limiting, authorization roles, reverse-proxy allowlist, CSRF model, broker ACL, or database encryption. Put a production service behind authenticated TLS infrastructure and add those controls before any real deployment.

SQLite files contain caller values and operational metadata. Protect them with filesystem permissions and encrypted storage. Hash-chained audit records detect mutation only when verified and are not a substitute for off-host append-only audit storage.

## Reporting

Do not include live tokens, signing keys, database contents, or personal data in a security report. Reproduce findings with synthetic local data.
