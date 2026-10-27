# Primary source and traceability

## Primary source

Ilya Ganelin, Stripe, **[Ledger: Stripe's system for tracking and validating money movement](https://stripe.com/blog/ledger-stripe-system-for-tracking-and-validating-money-movement)**, February 16, 2024.

This is an official Stripe engineering article. ClearFlow uses it as problem evidence, not as an implementation specification.

## Documented facts

The following statements come from Stripe's article:

| Documented Stripe statement | Article section |
| --- | --- |
| Stripe's financial network integrates services and external entities with different interfaces, data models, cadences, volumes, and correctness definitions. | “How Stripe processes payments” |
| Ledger is an immutable and auditable system of record used as a foundation for automated data-quality analysis. | Introduction; “Immutability” |
| Producer behavior is modeled as state machines and logical fund flows between accounts. | “How we designed Ledger” |
| Double-entry bookkeeping provides balanced debits and credits; nonzero clearing balances reveal unresolved flow stages. | “Clearing” |
| Creation and release are independent and may arrive out of order; missing or incorrectly identified events leave clearing balances nonzero. | “Detecting problems” |
| Data-quality questions cover clearing, timeliness, and completeness. | “How we designed the Data Quality Platform” |
| Completeness includes checking source IDs against matching Ledger events and modeling expected arrival. | “Completeness” |
| The immutable ledger is corrected through reversal/reprocessing rather than direct mutation. | “Data correction” |
| Proposed repairs receive out-of-band impact reports and a two-phase review and commit. | “Data correction” |

Stripe's published scale and performance figures describe Stripe only. ClearFlow does not reproduce or claim them.

## Independent project choices

The following are ClearFlow design decisions and must not be attributed to Stripe:

- Python standard-library implementation and one SQLite WAL database.
- Per-tenant event and audit HMAC-SHA-256 chains.
- Integer amount bounds, identifier grammar, 64-posting limit, and 1 MiB HTTP limit.
- Producer manifests as explicit arrays of expected references.
- Arithmetic-mean DQ score and four local policy thresholds.
- `PASS`, `FAIL`, and `INCONCLUSIVE` local verdicts.
- HMAC-SHA-256 signed quality attestations.
- Two reviewers, author exclusion, and global tenant-revision fencing for corrections.
- Bearer-token tenant mapping and loopback-only HTTP server.
- The synthetic charge/create/release model, local benchmark, and measured results.

## Inference connecting source to project

The source demonstrates that money movement correctness is broader than transactional writes: systems must expose missing, late, mis-keyed, and incorrectly repaired data. From that, this project infers that a useful fresher-scale implementation should combine accounting invariants, source-coverage evidence, delivery-time policy, immutable correction history, retry behavior, and audit verification in one locally reproducible service.

ClearFlow is not endorsed by Stripe and has not undergone financial, accounting, security, or production review.
