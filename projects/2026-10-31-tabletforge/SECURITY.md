# Security model

TabletForge is an educational local storage-engine laboratory, not an Internet-facing database.

## In scope

- malformed and duplicate-field JSON at the HTTP boundary;
- oversized keys, values, batches, request bodies, and result limits;
- unauthorized access to non-health endpoints;
- accidental or unauthorized modification of complete WAL frames, manifests, or SSTables without the signing secret;
- incomplete WAL tails caused by a process exit during append;
- retry duplication or reuse of an idempotency key with changed input;
- two local writers accidentally opening the same directory; and
- orphan immutable files created by a crash before manifest publication.

## Controls

- HMAC-SHA256 signs each WAL frame and the manifest; comparisons use `hmac.compare_digest`.
- CRC32 detects accidental WAL-frame corruption; SHA-256 binds every referenced SSTable.
- Canonical JSON and closed schemas prevent ambiguous signed inputs.
- The parser rejects duplicate object keys, non-finite numbers, invalid UTF-8, unknown fields, and invalid lengths.
- The API requires one bearer token of at least 24 characters and binds only to loopback.
- Responses disable caching and framing and set a restrictive content security policy.
- A non-blocking POSIX advisory lock provides one writer per directory.
- Temporary-file `fsync`, atomic rename, directory `fsync`, and signed manifest publication define local commit boundaries.
- Integrity verification reconciles signed metadata, file hashes, record ordering, WAL frames, and live directory contents.
- The supplied container runs as UID 65532 and the documented invocation drops capabilities, network access, privilege escalation, and writable root filesystem access.

## Explicitly out of scope

- an attacker with the signing secret or privileged access to replace both the directory and secret;
- rollback to an older but internally valid complete directory, or external truncation of a WAL tail that is indistinguishable from a torn append;
- confidentiality at rest or in memory;
- denial of service through authorized expensive scans, compactions, or filesystem exhaustion;
- kernel, Python runtime, filesystem, container runtime, or hardware compromise;
- network exposure, TLS termination, identity federation, authorization roles, quotas, or audit delivery; and
- distributed Byzantine behavior, replica compromise, split brain, or regional failure.

## Production hardening

Use a production server behind TLS and workload identity; scoped authorization; secret rotation and KMS/HSM-backed versioned asymmetric signatures; encryption at rest; external monotonic checkpoints; block-level checksums and scrubbing; disk quotas and compaction backpressure; rate limits; structured audit/metrics export; backups and restore drills; replicated consensus; fuzzing; and filesystem/power-loss testing on the exact deployment platform.

Never reuse the demonstration secret. Supply a random secret through `TABLETFORGE_SIGNING_SECRET` and keep the tablet directory and secret under separate access controls.
