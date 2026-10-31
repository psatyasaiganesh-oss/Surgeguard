# Source traceability

TabletForge is an independent educational project. The facts below come from Google's public paper; the implementation choices in the second section are this project's own inferences and additions.

## Primary source

Fay Chang, Jeffrey Dean, Sanjay Ghemawat, Wilson C. Hsieh, Deborah A. Wallach, Mike Burrows, Tushar Chandra, Andrew Fikes, and Robert E. Gruber, **“Bigtable: A Distributed Storage System for Structured Data,”** OSDI 2006, pp. 205–218.

- [Google Research publication page](https://research.google/pubs/bigtable-a-distributed-storage-system-for-structured-data/)
- [Official Google-hosted PDF](https://storage.googleapis.com/gweb-research2023-media/pubtools/4443.pdf)

## Documented facts

| Paper location | Documented fact | Relevance to this project |
| --- | --- | --- |
| Abstract and §1, paper p. 1 | Bigtable was designed for petabytes across thousands of commodity servers and served Google workloads with varied size and latency requirements. | Establishes the company problem and why storage reliability and efficiency matter. |
| §4, paper pp. 3–4 | An SSTable is a persistent, ordered, immutable key/value map with an index used to locate data. | Motivates immutable sorted segment files and in-memory indexes. |
| §5.3, paper pp. 5–6 | Tablet persistent state consists of redo commit-log records, recent sorted memtable updates, and older SSTables; recovery replays committed updates after redo points. | Motivates WAL-before-memtable ordering and replay after a checkpoint. |
| §5.3, paper p. 6 | Valid writes enter the commit log before the memtable; reads form a merged view of the memtable and SSTables. | Motivates the write protocol and highest-visible-sequence read path. |
| §5.4, paper p. 6 | Minor compaction freezes a threshold-sized memtable and writes it to an SSTable, reducing memory and recovery work while reads/writes may continue. | Motivates explicit and threshold-triggered flushes; TabletForge does not claim concurrent background compaction. |
| §5.4, paper p. 6 | Merging compaction bounds the number of SSTables a read must merge; major compaction removes deletion entries and deleted data. | Motivates read-amplification control and safe obsolete-version/tombstone reclamation. |
| §6 “Bloom filters,” paper p. 7 | Bloom filters reduce SSTable disk accesses and let most nonexistent-row/column lookups avoid disk. | Motivates per-segment Bloom filters and observable negative-skip counters. |
| §6 “Exploiting immutability,” paper p. 8 | Immutable SSTables simplify concurrent reads, garbage collection, and tablet splitting. | Motivates manifest-referenced immutable files and publish-before-delete compaction. |
| §7, paper pp. 8–9 | Writes benefit from a shared commit log and group commit. | Explains why TabletForge's one-`fsync`-per-batch approach is deliberately simpler and slower. |

## Project inferences and additions

The following are not claims about Google's implementation:

- a custom Python SSTable binary format with whole-file SHA-256, an internal data digest, per-key offsets, and a serialized Bloom filter;
- HMAC-SHA256 signatures for WAL frames and manifest envelopes, CRC32 for accidental WAL corruption, and constant-time comparison;
- canonical request digests and a persistent idempotency index for retry convergence;
- named sequence snapshots and exact view-aware version retention during compaction;
- atomic manifest replacement as the local commit point and orphan SSTable quarantine after a pre-manifest crash;
- explicit failpoints after WAL `fsync`, SSTable rename, and manifest commit;
- a strict bearer-authenticated loopback HTTP adapter, local integrity command, synthetic benchmark, Docker image, and CI workflow; and
- all local test and benchmark numbers in this repository.

TabletForge intentionally omits Bigtable's distributed components, including GFS, Chubby, tablet assignment/splitting, replication, cluster scheduling, and the full row/column/timestamp data model.
