# ScopeGraph

A working relationship-based authorization engine and protected-document API that prevents stale permission decisions from exposing newly published content. Python standard library + SQLite WAL; no paid services, external identity provider, or API credentials.

**Independent implementation inspired by a documented Google challenge.** This is not Google code, a Zanzibar-compatible service, or a claim of production use at Google.

## Sourced problem and business impact

**Documented fact:** Google’s *Zanzibar: Google’s Consistent, Global Authorization System*, presented at USENIX ATC on July 10–12, 2019, describes the “new enemy” problem: applying old permissions to content added after a user’s access was revoked can expose that new content. The paper describes ordering ACL/content changes and using freshness-bounding tokens. [Primary paper, §2.2](https://www.usenix.org/system/files/atc19-pang.pdf) · [Google Research](https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/) · [Conference record](https://www.usenix.org/conference/atc19/presentation/pang).

**Our inference:** shared-document products, enterprise SaaS, and internal knowledge tools face a privacy and trust risk if cached access outlives revocation. This is a motivating risk, not a claim that Google suffered a particular incident or a quantified financial loss.

**Our scope:** solve the analogous ordering problem inside one local database. Unlike previous projects in this repository, the main challenge is access-control correctness, tenant isolation, expiring permissions, and the boundary between permission checks and protected data reads—not retry control, replica reconciliation, backups, tracing, migrations, or right-sizing.

## Quick start

Linux/POSIX, Python 3.11+ with SQLite. No pip install is necessary.

```bash
cd projects/2026-10-19-scopegraph
python -m scopegraph demo
python -m unittest discover -v
python scripts/validate.py
```

The demo creates temporary synthetic data, opens a real loopback HTTP server, tests revocation and expiry, forces a concurrent read/revoke/publish race, benchmarks 2,000 real authorization calls, and removes its temporary database. It emits no access tokens or signing keys. `validate.py` regenerates the measured reports in `reports/`.

## What is implemented

- Durable, tenant-scoped relationship tuples; nested groups; inherited folder permissions; explicit deny overrides; expiring edges; bounded proof paths.
- Atomic batches, final-graph cycle rejection, one-parent enforcement, compare-and-swap revisions, durable idempotency receipts, and a per-revision mutation audit.
- Database/tenant-bound signed minimum-revision tokens. An old token never selects old permissions; an unavailable revision fails closed.
- A bounded LRU decision cache keyed by tenant, revision, resource, permission, and user. Every request still reads the database revision. A tenant-wide next-expiry barrier protects positive **and negative** entries.
- Protected content reads that evaluate authorization and retrieve the body in the **same SQLite snapshot**; protected writes evaluate edit access inside the same write transaction as the content change.
- Purpose-separated bearer access tokens, admin-only relationship writes, identity binding, strict JSON/framing validation, request limits, and bounded HTTP worker admission.
- Crash, race, randomized graph-oracle, API, CLI, validation, and security tests; Docker and GitHub Actions configuration.

## Fixed policy model

Objects are `folder:id`, `document:id`, or `group:id`. Subjects are `user:id` or `group:id#member`. Identifiers use bounded ASCII letters, digits, underscores, and hyphens. There are no wildcard subjects or executable policy expressions.

| Relation | Meaning |
| --- | --- |
| `owner` | Allows `view`, `edit`, and `share` |
| `editor` | Allows `view` and `edit` |
| `viewer` | Allows only `view` |
| `parent` | One folder parent; inherits that folder’s roles and ancestor roles |
| `blocked` | Matching user/group on the resource or any ancestor overrides every allow |
| group `member` | Direct user membership or another group’s `#member` userset |

The deny-overrides policy, inheritance of all three permissions, expiry rules, and cycle prohibition are **our design choices**, not claims about Zanzibar’s default behavior. `share` is a policy query; graph administration remains admin-only rather than a self-service sharing API.

Each tuple may have an `expires_at` Unix timestamp. It is active only while the trusted server clock is strictly less than that value. Even expired structural edges count for cycle/one-parent validation until explicitly deleted. Moving a document requires deleting its old parent and adding its new parent in one batch. Dropping an object removes outgoing tuples, incoming references, and its content, preventing grants from resurrecting on recreation.

## Architecture and consistency boundary

| Flow | Transaction and safety property |
| --- | --- |
| Relationship mutation | `BEGIN IMMEDIATE` → validate/apply batch → reject final cycles → increment tenant revision + receipt + audit → commit atomically |
| Permission check | `BEGIN` → read current revision to anchor snapshot → enforce minimum token → revision/expiry-safe cache or bounded graph evaluation |
| Content read | Same anchored transaction for permission evaluation **and** body/version retrieval; never a separate “check then fetch” connection |
| Content publish | Write lock → current edit check → content-version CAS → body + ACL revision + receipt committed together |

SQLite WAL permits an old reader to finish while a writer commits. If the reader’s snapshot preceded revocation, it can return the old content it was authorized to see; it must not fetch a newer body using that old authorization. The barrier-controlled test pauses after authorization, commits revocation and new content, then verifies exactly this boundary. Fresh requests after revocation are denied.

Per-tenant revisions advance for relationship batches; content versions advance independently. Content records retain the ACL revision observed at publication. All cache lookups first read the revision **from the current transaction**, including changes made by another engine process. The expiry barrier conservatively invalidates every tenant entry at the next tuple expiry, even if a particular decision did not depend on that tuple.

Evaluation is iterative, limited to 2,000 fetched edges and depth 32 by default. It inspects all relevant ancestor denies before accepting an allow. Exhausting a budget returns a denial with `budget_exceeded`, never an optimistic allow. Proofs explain the caller’s own decision; this lab does not offer arbitrary-user lookup through HTTP.

## Local operator and API demo

The CLI is trusted operator tooling: anyone with database-file access can administer the graph or mint credentials. The HTTP API is the untrusted boundary. Use only synthetic data in a private directory.

Terminal A, from the project directory:

```bash
python -m scopegraph init --db demo.db --tenant demo
python -m scopegraph mutate --db demo.db --tenant demo --file examples/bootstrap.json
python -m scopegraph check --db demo.db --tenant demo --resource document:roadmap --user user:alice
python -m scopegraph serve --db demo.db --port 8080
```

Terminal B, from the same directory:

```bash
SCOPEGRAPH_OWNER_TOKEN=$(python -m scopegraph mint --db demo.db --tenant demo --user user:owner | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
curl --fail-with-body -s http://127.0.0.1:8080/v1/publish \
  -H "Authorization: Bearer ${SCOPEGRAPH_OWNER_TOKEN}" -H 'Content-Type: application/json' \
  --data '{"resource":"document:roadmap","text":"Synthetic roadmap v1","request_id":"publish-1","expected_version":0}'
curl --fail-with-body -s http://127.0.0.1:8080/v1/read \
  -H "Authorization: Bearer ${SCOPEGRAPH_OWNER_TOKEN}" -H 'Content-Type: application/json' \
  --data '{"resource":"document:roadmap"}'
unset SCOPEGRAPH_OWNER_TOKEN
```

Keep tokens in your local terminal; do not paste them into issues or commit them. Minting defaults to a 15-minute TTL, capped at one hour. Restarting with the same database preserves keys and receipts. Retrying the exact bootstrap request returns its original receipt rather than duplicating the batch.

| Endpoint | Request body | Authorization |
| --- | --- | --- |
| `GET /healthz` | None | Minimal public health response |
| `POST /v1/check` | `resource`, `permission`, optional `min_token` | Signed caller’s user and tenant |
| `POST /v1/read` | `resource`, optional `min_token` | Current `view` permission |
| `POST /v1/publish` | `resource`, `text`, `request_id`, `expected_version`, optional `min_token` | Current `edit` permission |
| `POST /v1/relationships` | `operations`, `request_id`, optional `expected_revision` | Signed `admin` role |

The server rejects body overrides for `subject`, `tenant`, and `now`; bearer identity cannot be replaced by request JSON. Non-admin tokens cannot modify the graph, even if they have `share`. No HTTP token-issuance endpoint exists. Revision tokens are freshness bounds, **not** access credentials.

## Measured validation

Captured locally with Python 3.12.14 / SQLite 3.53.1; full artifacts: [tests](reports/tests.txt), [validation metadata](reports/validation.json), [demo/benchmark](reports/demo.json).

| Check | Recorded result |
| --- | --- |
| Automated tests | 95 passed; test command completed in 2.955146 s |
| Compilation | Passed (`compileall`) |
| Forced concurrent read | In-flight reader returned version 2, never the post-revocation version 3; fresh reader denied |
| Real HTTP checks | Revoked read 403; owner check 200; identity override 400; non-admin mutation 403 |
| Cold 1,000 checks | 0.323608 s total; p50 0.3020 ms; p95 0.4463 ms |
| Warm 1,000 checks | 0.235998 s total; p50 0.2128 ms; p95 0.3384 ms; 1,000 cache hits |

This is one sequential local microbenchmark over 1,001 synthetic objects and 1,001 tuples, with a fixed scenario clock and no expiries during timing. The engine performs actual SQLite reads and graph evaluation; the clock and inputs are synthetic. These numbers are not a scalability claim, SLA, load test, or Google performance result. Reruns will vary, and warm caching does not eliminate database access or guarantee lower latency on every run.

## Docker and CI

```bash
docker build -t scopegraph .
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m scopegraph demo
```

The image runs tests during build and executes as UID/GID 10001. The demo needs only temporary storage and loopback sockets, not outbound networking. The [CI workflow](../../.github/workflows/scopegraph-ci.yml) checks Python 3.11/3.12/3.13 and separately builds/runs the restricted container. Docker was unavailable in the local authoring environment; `validation.json` records that fact rather than claiming a local container execution. The Python base tag is minor-version pinned, not a hermetic image digest.

## Tradeoffs, security, and known limits

1. **One database, not a global system.** No replicas, Spanner/TrueTime, cross-region causal guarantees, watch stream, federation, or Zanzibar rewrite DSL. SQLite serializes writers; tenant graph validation scans structural edges after each batch.
2. **Availability versus correctness.** Busy writes return retryable 503; CAS conflicts return 409; traversal exhaustion denies. The cache is process-local and bounded to 2,048 entries, but receipts and audit history need operator retention management for long-running use.
3. **Trusted host and time.** Database/key files are private (0600); direct symlinks and unrelated databases are rejected. The parent directory, SQLite storage, operator, and clock are trusted. Observed clock rollback clears cached entries, but this is not a secure time source or defense against a hostile filesystem administrator. No encryption-at-rest or signing-key rotation is implemented.
4. **Local API only.** IPv4 loopback binding, 16 workers, five-second socket timeout, 64 KiB request bodies, 32 KiB UTF-8 content, 200-operation batches, 20,000 objects and 100,000 tuples per tenant. Duplicate JSON keys, non-finite numbers, ambiguous framing, transfer encodings, and unknown schema fields are rejected. Errors and logs do not include bearer tokens or protected content. This is not a hardened public HTTP server; add TLS, a production identity provider, token revocation/rotation, rate limiting, and a reviewed transport before exposure.
5. **Authorization timing.** Access is decided at the anchored snapshot and sampled server time, not at response delivery. An already-authorized in-flight read may finish with old content after revocation or expiry. A `check` result is not a durable capability; callers must use the protected read/write methods instead of independently fetching protected data.
6. **Retry semantics.** Mutation receipts return the original completed outcome even after later graph changes; they do not reapply it. Publishing retries recheck current edit authorization before returning a receipt. Request IDs are scoped by tenant and action and must not be repurposed for a new logical operation.

## Code and interview guide

| Module | Responsibility |
| --- | --- |
| `scopegraph/engine.py` | SQLite transactions, mutations, receipts, cache, protected content |
| `scopegraph/policy.py` | Iterative bounded graph evaluation and witness paths |
| `scopegraph/tokens.py` | Purpose-separated HMAC access and revision tokens |
| `scopegraph/model.py` | Closed schema, identifiers, bounded strict JSON |
| `scopegraph/http.py` | Loopback transport, identity binding, admission control |
| `scopegraph/demo.py` | Reproducible adversarial scenarios, race, HTTP checks, timings |
| `tests/` | 95 invariant, security, concurrency, crash, API, and CLI tests |

See [source notes](docs/SOURCES.md) and the [interview walkthrough and two evidence-based resume bullets](docs/INTERVIEW.md).
