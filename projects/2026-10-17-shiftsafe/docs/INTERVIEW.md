# Interview walkthrough

Introduce it as: "I independently built ShiftSafe after reading Stripe's engineering article about online subscription schema migrations. I wanted to test how a migration can preserve state while application writes continue. My implementation is a local Python/SQLite correctness lab."

## Five-minute demonstration

1. **Problem, 30 seconds:** Show the source link in the README. An embedded subscription list must become normalized rows while the application continues to mutate it. The concrete risk is switching reads before the destination agrees with the source.
2. **Architecture, 60 seconds:** Explain the five phases. One transaction commits both layouts and the operation receipt. The NEW phase continues legacy writes so rollback remains available. Finalization is the irreversible migration boundary.
3. **A real race, 60 seconds:** Run `python -m shiftsafe demo`. Point to `stale_snapshot_rows_skipped: 2`: a captured batch sees older state, then a live update and delete occur. Revision checks preserve both changes. A deletion tombstone is essential; removing that revision would allow old data to return.
4. **Recovery, 60 seconds:** Show the committed cursor and increasing fence. The restarted worker resumes at the checkpoint; the expired worker is rejected. Show the two subprocess tests: they terminate without Python exception cleanup, forcing SQLite to recover uncommitted work.
5. **Evidence and tradeoff, 90 seconds:** Show the corrupt cutover refusal, repair, zero final mismatches, and rollback preserving a post-cutover write. Explain why a previous verification report cannot authorize a later switch. Compare and phase change share the writer lock. Finish with the single-database/single-writer limitation and the cross-database work still required.

## Likely questions

**Why can a stale batch not resurrect a deleted subscription?** A delete increments the identity revision and writes an empty normalized projection with a deletion flag. Backfill refuses an older or equal revision, including deletion tombstones.

**What if the process dies after copying data but before advancing the cursor?** Both changes are uncommitted in the same transaction and roll back. The next worker repeats the whole batch from the previous durable checkpoint. If the transaction committed before the crash, the new cursor prevents repeating those rows.

**Why have fencing as well as lease expiry?** An old process can wake after another worker takes ownership. The database compares owner and monotonically increasing fence under the same writer lock used for applying rows. A lease check done earlier in application memory is insufficient.

**Why not trust a zero-mismatch report from a minute ago?** That report describes an earlier data version. Cutover recalculates the comparison while holding the write lock and changes the phase before releasing it. There is no unprotected gap for a cooperating writer.

**Are historical shadow mismatches a permanent block?** No. They are useful observations. Repair can resolve a past discrepancy; current full verification determines whether cutover is safe.

**Can you roll back after the old data is removed?** No. Rollback is disabled at FINALIZED, before cleanup. Restoring that capability would require another migration or a separately designed backup/recovery process.

**Would this work between PostgreSQL and a document database?** The current atomicity argument would not apply. I would add a transactional outbox/change log at the source, ordered idempotent consumers with tombstones, replicated progress and lag measurements, and a defined cutover boundary with reconciliation. Those are future designs, not implemented claims.

**What is the performance cost?** Dual writes add work, and gates briefly serialize writers for a full scan. Refer to the actual report's sample size, timing, environment, and burst schedule. Do not extrapolate this run into Stripe-scale throughput or claim no production downtime.

## Evidence-based resume bullets

- Built an independent Python/SQLite schema migration engine with atomic dual writes, fenced resumable backfill, shadow comparisons, rollback, and verified cutover; validated 1,000 synthetic customer records with 400 interleaved mutations and zero final mismatches.
- Added 59 automated tests covering abrupt process exits, stale-update/deletion races, idempotency, lease takeover, concurrent revision updates, and cutover isolation; supplied a reproducible JSON benchmark, Docker image, and Python 3.11–3.13 CI configuration.

Use the test count and workload only while they match the committed validation reports. No employment, Stripe contribution, production deployment, business saving, or external user adoption is implied.
