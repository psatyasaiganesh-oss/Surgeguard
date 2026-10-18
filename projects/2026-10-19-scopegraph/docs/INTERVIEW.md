# ScopeGraph interview walkthrough

## Two evidence-based resume bullets

- Implemented ScopeGraph, an independent Google Zanzibar-inspired authorization engine using Python and SQLite WAL, with nested-group/folder inheritance, tenant-bound signed freshness tokens, revision/expiry-aware caching, and snapshot-atomic protected document reads.
- Validated access-control and recovery invariants with 95 automated tests, including HTTP identity tampering, concurrent CAS/idempotency races, abrupt process exits, and a forced revocation/content-update race; benchmarked 2,000 synthetic local checks with 1,000 warm-cache hits.

Use these only if you can explain and reproduce the work. Do not describe it as employment at Google, Google production software, production-scale performance, or a guarantee of shortlisting.

## A five-minute narrative

1. **Problem (45 seconds).** Explain the sourced stale-permission/new-content risk: Alice loses access, the owner adds sensitive content, and an old cached allow must not expose the new body. State clearly that the source is Google’s 2019 Zanzibar paper, not a claim of a Google outage.
2. **Model (45 seconds).** Show `examples/bootstrap.json`: a document inherits a folder’s editor group; nested membership resolves to a concrete user. Owner/editor/viewer permissions are fixed, and an ancestor block overrides any allow. Explain why the lab rejects cycles and wildcard subjects.
3. **Consistency boundary (60 seconds).** In `engine.py`, show `read_content`: read the revision to anchor the SQLite transaction, check minimum freshness, evaluate permissions, and fetch the body on that exact connection. Contrast with the unsafe split “check, revoke, publish, fetch latest body” sequence. Publishing holds the write lock and checks current edit permission.
4. **Caching and identity (45 seconds).** Show why a TTL alone is insufficient. Every lookup first reads a fresh database revision; the cache key changes after ACL writes. Explain why expiring grants and expiring denies also require a clock-based validity barrier. Access tokens authenticate the caller; revision tokens only bound freshness and cannot authenticate.
5. **Evidence and tradeoffs (45 seconds).** Run `python -m scopegraph demo` and `python -m unittest discover -v`. Point to the observed old-content/new-content race boundary and HTTP 403/400 results. Finish with the single-database, trusted-clock, loopback-only limits.

## Questions to be ready for

**Why isn’t this just RBAC?** Roles are attached to individual resources, groups can include groups, and resources inherit from folders. Effective permissions depend on a relationship graph rather than a single global role on the user.

**Does a completed revoke cancel every in-flight read?** No. A read anchored before revocation can finish with the old content. The demonstrated invariant is that it cannot combine old permissions with a post-revocation content version. A read that starts afterward sees the new revision and denies. Stronger response-time revocation requires another protocol and has availability costs.

**Why sign a revision token if the server reads the latest local state anyway?** Within this single-primary lab, the token makes the caller’s lower-bound expectation explicit and safely fails if unavailable. It is an educational API contract and a boundary for a potential replica implementation—not evidence that replicas exist here. Tenant/database binding and purpose separation prevent misuse across contexts.

**Why can’t you cache forever at one revision?** Tuple expiry changes effective permissions without a mutation. An expiring deny can turn a negative answer positive, too. The conservative next-expiry barrier covers both directions. Every cached answer also has a normal TTL and a bounded LRU footprint.

**What if a nested blocked group is too expensive to evaluate?** Return `allowed=false` with `budget_exceeded`; do not accept a positive grant until all relevant deny checks have completed. This sacrifices availability to avoid an incomplete allow.

**What does idempotency guarantee?** An identical tenant/action/request ID returns its durable original receipt without reapplying changes; different input conflicts. Receipts and mutations commit together. Content retries additionally recheck current edit permission, so the receipt cannot authorize a revoked user.

**What does the benchmark establish?** That the included implementation executes the measured local workload. The recorded run used 1,001 objects/tuples and 1,000 cold plus 1,000 warm checks; it does not establish multi-tenant capacity, distributed latency, a production SLA, or Google-scale performance. Inspect `reports/demo.json` for exact timings.

**How would you extend it?** Start with identity/key lifecycle and a hardened API boundary; then add retention, realistic concurrent load tests, and explicit policy-model versioning. Moving content to another store or adding replicas requires a new consistency design and adversarial tests—not simply reusing an old boolean allow.
