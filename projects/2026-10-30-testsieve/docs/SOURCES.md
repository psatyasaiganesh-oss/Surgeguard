# Sources and design traceability

## Primary source

Aditya Anchuri, Stripe, [“Selective Test Execution at Stripe: Fast CI for a 50M-line Ruby monorepo”](https://stripe.dev/blog/selective-test-execution-at-stripe-fast-ci-for-a-50m-line-ruby-monorepo), published April 9, 2026; accessed October 30, 2026.

This is an official Stripe engineering article. It is the only company source needed for the project premise. TestSieve does not copy Stripe source code and has no access to Stripe's internal implementation.

## Documented facts

The following statements are paraphrases of the source, not TestSieve measurements:

| Stripe-documented fact | TestSieve concept it motivated |
| --- | --- |
| Stripe describes roughly 50 million Ruby lines, 100,000 test files, 1.2 million test units, and a four-month sequential run. | A selector is a business-relevant CI control, not just a toy filter. |
| Stripe runs around 50,000 builds weekly and reports about 5% average selection, 0.5% median selection, and less than 10% of always-full compute. | The benchmark reports selection fraction separately from correctness; it does not reuse Stripe's scale claims as project results. |
| Static analysis struggles with metaprogramming, runtime branches, configuration, fixtures, templates, and generated artifacts. | Runtime file access is the dependency signal. |
| Stripe associates intercepted file opens with test scopes and aggregates/inverts observations into a selection index. | `FileAccessTracer`, observation maps, and the in-memory inverse index. |
| Files opened in root scope are global dependencies and can trigger all tests. | `root_files` force a safe full-suite fallback. |
| Directory glob/discovery tests can change behavior without opening the new file, so a small set is mandatory. | `mandatory` tests are always candidates. |
| A hashed inventory detects tracked and generated-file changes relative to the selected baseline. | Every decision compares complete caller-supplied path/digest inventories. |
| Previously failing tests rerun. | `last_status == FAIL` is always selected. |
| Recent, completed, coherently ordered baseline metadata makes selection fast, reliable, reproducible, and debuggable. | Immutable parent-linked revisions and signed manifests identify the exact baseline. |

## Independent inferences and additions

These are TestSieve design choices. They are not claims about Stripe:

- Python `builtins.open`/`os.open` interception is a small local teaching adapter. It is not Stripe's documented C++ `LD_PRELOAD` syscall interceptor and does not propagate to child processes.
- SQLite is used for reproducible transactions; Stripe's article says its current completion metadata is persisted internally in MongoDB.
- Contiguous integer revisions approximate the ordering property needed by this lab. They are not Stripe's internal Monotonic Revision ID implementation.
- `RECORD -> SHADOW -> SELECT`, a two-clean-run threshold, and automatic quarantine after a skipped failure are conservative rollout controls added by this project.
- Unknown-file and stale-baseline full fallbacks are project safety policy.
- HMAC decision manifests, chained audits, strict HTTP validation, and whole-database replay are project integrity mechanisms.
- Longest-processing-time worker assignment is a project scheduling addition.
- The 1,000-test benchmark is synthetic and cannot be compared directly with Stripe's production results.

## Interpretation boundary

File-access correlation says that a test touched a file in an observed run. It does not prove the test covers every behavior of that file or every future runtime branch. TestSieve therefore treats uncertain evidence as a reason to broaden execution and documents the residual risk instead of claiming perfect selection.
