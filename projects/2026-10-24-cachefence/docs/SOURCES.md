# Source notes and claim boundary

CacheFence is an independent implementation. It is not affiliated with Meta and does not contain Meta production code.

## Primary source 1

Meta Engineering, [Cache made consistent](https://engineering.fb.com/2022/06/08/core-infra/cache-made-consistent/), posted June 8, 2022.

Documented facts used here:

- A read fill for an older value can arrive after a newer database write and its invalidation, overwriting the cache with stale state indefinitely.
- Version ordering can prevent older state from winning, but evicting the newer cache entry can also erase the version knowledge needed to reject the old fill.
- A dynamic cache changes on both read-fill and write-invalidation paths; those two paths create many possible races.
- Meta says some cache inconsistencies can be indistinguishable from data loss from a user's perspective and gives a messaging storage-mapping example.
- Meta describes Polaris as checking client-observable invariants, retrying samples at later timescales to avoid false positives, and distinguishing lag from longer-lived inconsistency.
- The article describes a real defect in which cache and database content differed at the same version. An error path attempted to drop only lower-version entries, so the equal-version stale content remained.
- Meta describes selective tracing around invalidation race windows because tracing every high-volume fill would be impractical.

The article reports Meta production scale and consistency percentages. CacheFence neither reproduces nor compares itself with those numbers.

## Primary source 2

Meta Engineering (then Facebook), [Wormhole pub/sub system: Moving data through space and time](https://engineering.fb.com/2013/06/13/core-infra/wormhole-pub-sub-system-moving-data-through-space-and-time/), posted June 13, 2013.

Documented facts used here:

- Wormhole propagates changes from one system to systems that need to reflect them within and across data centers.
- The article lists caches as consumers because refills and invalidation messages must reach each cache to keep it aligned with its local database and other caches.

## Independent inferences and project choices

The following are CacheFence design decisions, not statements about Meta's current or historical implementation:

- Store the latest version/digest fence durably in a SQLite table separate from the evictable value.
- Hash canonical semantic content and treat equal-version/different-digest observations as integrity incidents.
- Use a transactional outbox, leased delivery, random acknowledgement capabilities, bounded retries, and stable event IDs.
- Authenticate invalidations with HMAC and bind the key ID to the declared origin.
- Recheck a rejected fill from the source exactly once.
- Coalesce concurrent cold reads with an in-process per-key lock.
- Treat an absent cache entry as safe for cache-aside clients because they consult the origin.
- Persist probe samples and label mismatches as lag, future cache state, or same-version content mismatch.
- Use the local scenarios as evidence of implementation behavior, not evidence of production impact.

Potential business effects such as stale entitlements, incorrect settings, or misrouted workflows are reasonable examples of why cache correctness matters, but this project does not claim those happened at Meta beyond the specific examples in the source.
