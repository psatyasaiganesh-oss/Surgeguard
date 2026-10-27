# Security and correctness model

ClearFlow is an educational local system, not a payment processor or certified financial ledger. It demonstrates exact arithmetic, evidence immutability, tenant isolation, correction controls, signed reports, and corruption detection. It has not received an external security or accounting review.

## Protected invariants

- Monetary postings use bounded integers and every event sums exactly to zero.
- An event has one currency and every referenced account must use that currency.
- Stored event and posting rows cannot be updated or deleted through normal SQLite statements.
- Event and manifest idempotency keys cannot be reused with different canonical content.
- Missing or overdue evidence cannot produce a passing quality verdict.
- Correction authors cannot approve their own proposal.
- Two distinct approvals bind to the exact impact-preview digest.
- The ledger revision must still equal the preview revision at correction execution.
- A correction transaction appends both reversal and replacement or neither.
- HMAC attestations bind a tenant, evaluation, snapshot revision, verdict, score, and report digest.
- Full verification reconstructs event and audit chains and checks reports, manifests, proposals, approvals, currencies, and materialized heads.

## HTTP boundary

- The tenant is derived from a server-side bearer-token map, never from request JSON.
- Tokens must be distinct and at least 24 characters and are compared with `hmac.compare_digest`.
- The server binds only to loopback.
- JSON must be UTF-8, at most 1 MiB, finite, duplicate-free, and match a closed endpoint schema.
- Identifiers are length- and character-bounded; amounts and timestamps are range-checked.
- SQL uses bound parameters.
- Responses set `no-store`, `nosniff`, frame denial, and a restrictive content security policy.
- Unexpected exceptions return a generic message.

## Explicitly out of scope

- TLS termination, OIDC/service identity, RBAC administration, token rotation, WAF, distributed rate limiting, and denial-of-service resistance.
- PCI DSS, SOC, SOX, AML/KYC, tax, accounting standards, legal retention, and jurisdiction-specific controls.
- HSM/KMS custody, asymmetric attestations, external transparency logs, trusted timestamps, and independent database backups.
- Source authenticity, bank statement parsing, settlement confirmation, exchange-rate correctness, and payment execution.
- Authenticated human reviewer identities. Correction actor labels are accepted only by the trusted library/admin boundary and are not exposed through the tenant-token HTTP API.
- Multi-region consensus, replicated storage, high availability, sharded ordering, and disaster recovery.
- Statistical anomaly detection, materiality policy, exception expiry, ownership workflows, and period close.

## Secret handling

Set `CLEARFLOW_SIGNING_SECRET` to at least 32 random bytes and `CLEARFLOW_TENANT_TOKENS` to a JSON object with one distinct random token per tenant. Never commit either value. A database is bound to the signing secret through a keyed fingerprint and refuses to open under a different secret.

For a real system, use workload identity, versioned KMS/HSM keys, encrypted replicated storage, externally checkpointed hash roots, centrally managed authorization, structured audit export, and an independently reviewed accounting model.
