# TestSieve interview walkthrough

## 30-second framing

“I built TestSieve as an independent selective-test execution lab inspired by Stripe's public engineering article. The key objective is not simply to run fewer tests; it is to make underselection observable and fail closed. The system versions runtime file observations and complete file inventories, computes a conservative test subset, signs the decision, and uses full-suite shadow runs to quarantine unsafe baselines.”

## Five-minute walkthrough

1. **State the business problem.** Large dynamic-language suites make full execution slow and expensive, while a missed dependency can allow a regression to merge.
2. **Show the evidence model.** Each baseline stores test metadata, `test -> opened files`, root/global files, a full path/digest inventory, and the completed commit name.
3. **Change `lib/payments.py`.** The inverse index selects the payment test. The selector also includes a mandatory discovery test and a previously failing test, producing three candidates from five.
4. **Explain rollout.** `RECORD` and `SHADOW` both execute the full suite. After two clean shadow comparisons, `SELECT` may execute only candidates.
5. **Trigger uncertainty.** A root dependency or unobserved generated file causes a full-suite fallback.
6. **Trigger a false negative.** Report `tests/refunds_test.py` failing in the full shadow run even though it was outside the candidate set. The transaction quarantines the baseline and resets mode to `RECORD`; the next decision runs everything.
7. **Show integrity.** Retry the same selection key, verify the signed manifest, modify it and observe rejection, then run the integrity replay.
8. **Close honestly.** The local Python tracer proves the model but does not provide Stripe's documented OS-level or child-process coverage.

Run it:

```bash
python -m testsieve demo
python scripts/validate.py
```

## Core invariants

- One repository and idempotency key maps to one canonical request and one durable result.
- A selection names exactly one immutable completed baseline and current-inventory digest.
- Mandatory, prior-failing, and unmapped tests cannot be silently skipped.
- Unknown paths, root changes, stale evidence, or quarantine cause a full fallback.
- `SELECT` cannot be enabled without clean shadow evidence.
- A shadow false negative atomically quarantines the same current baseline.
- Persisted evidence and decision manifests can be replay-verified.

## Likely questions

### Why observe file access instead of imports?

Imports miss configuration, templates, fixtures, generated artifacts, and runtime loading. File access gives one language-agnostic boundary. It still misses directory enumeration and unexecuted branches, which is why mandatory tests, recording, shadow comparison, and fallbacks remain necessary.

### Why is an unknown changed file a full fallback?

There is no evidence proving which tests depend on it. Returning an empty impacted set would optimize precisely where the model is blind. A production system could introduce trusted-generator policy, but the default should remain safe.

### How do retries remain safe?

Every mutating request has a repository-scoped idempotency key and canonical digest. `BEGIN IMMEDIATE` plus a unique constraint serializes races. Identical retries return the stored identity; different content with the same key conflicts.

### Why store an inventory instead of using Git diff?

Generated artifacts and other build outputs can affect tests without appearing as source changes. A complete hashed inventory also detects additions and deletions relative to the exact baseline.

### How would this scale?

Move observation aggregation off the hot open path, encode file-to-test sets as compressed bitmaps, partition indexes by repository and revision, store immutable snapshots in object storage, keep transactional head metadata separately, cache recent baselines, and push signed plans to authenticated workers.

### What is the biggest correctness limitation?

Observations describe paths exercised in prior runs, not all possible paths. Conditional behavior can change. Trusted broad recording, richer signals, long shadow windows, periodic full runs, and production incident feedback are still required.

### Why HMAC rather than public-key signatures?

It keeps the local demo dependency-free and makes tampering testable. In production, asymmetric KMS-backed keys with key IDs and rotation let workers verify plans without holding signing authority.

## Honest claims

Say “inspired by Stripe's documented challenge,” “synthetic local benchmark,” and “Python-level teaching tracer.” Do not say the project is deployed, used by Stripe, equivalent to Stripe STE, production-safe, or proven at Stripe scale.
