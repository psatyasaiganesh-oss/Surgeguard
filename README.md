# Surgeguard
Code for mtc

## Company engineering challenges

Working, independent implementations inspired by documented company engineering problems. Each project includes its primary sources, local demonstration, automated tests, measured results, and design tradeoffs.

| Date | Project | Company problem | Run |
| --- | --- | --- | --- |
| 2026-10-25 | [ChronoCommit](projects/2026-10-25-chronocommit/) | Google Spanner: keep cross-shard transactions atomic and order their observable commits despite clock uncertainty and process crashes | `cd projects/2026-10-25-chronocommit && python -m chronocommit demo` |
| 2026-10-24 | [CacheFence](projects/2026-10-24-cachefence/) | Meta: prevent a delayed cache fill from overwriting a newer invalidation, even after value eviction, and detect observable inconsistency | `cd projects/2026-10-24-cachefence && python -m cachefence demo` |
| 2026-10-23 | [MergeMesh](projects/2026-10-23-mergemesh/) | Microsoft Azure Cosmos DB: preserve independent multi-region edits and expose true write conflicts instead of silently selecting a whole-item winner | `cd projects/2026-10-23-mergemesh && python -m mergemesh demo` |
| 2026-10-22 | [SpecuFlow](projects/2026-10-22-specuflow/) | Google MapReduce: reduce straggler-driven job tail latency without double-committing outputs or counters | `cd projects/2026-10-22-specuflow && python -m specuflow demo` |
| 2026-10-21 | [RouteProof](projects/2026-10-21-routeproof/) | Meta: prevent a maintenance command from cascading through backbone, DNS, and recovery dependencies | `cd projects/2026-10-21-routeproof && python -m routeproof demo` |
| 2026-10-20 | [BurnWatch](projects/2026-10-20-burnwatch/) | Google SRE: detect sustained error-budget burn while controlling noisy pages and treating missing telemetry as unknown | `cd projects/2026-10-20-burnwatch && python -m burnwatch demo` |
| 2026-10-19 | [ScopeGraph](projects/2026-10-19-scopegraph/) | Google Zanzibar: prevent stale authorization from exposing content published after access revocation | `cd projects/2026-10-19-scopegraph && python -m scopegraph demo` |
| 2026-10-18 | [FitFleet](projects/2026-10-18-fitfleet/) | Netflix Stratum: reduce over-provisioned batch CPU/memory through verified recommendations and regression-aware rollback | `cd projects/2026-10-18-fitfleet && python -m fitfleet demo` |
| 2026-10-17 | [ShiftSafe](projects/2026-10-17-shiftsafe/) | Stripe: migrate subscription schemas while preserving live writes, verified read cutover, and rollback | `cd projects/2026-10-17-shiftsafe && python -m shiftsafe demo` |
| 2026-10-16 | [TraceLens](projects/2026-10-16-tracelens/) | Google Dapper/Cloud Trace: diagnose request latency across distributed services while controlling retained trace detail | `cd projects/2026-10-16-tracelens && python -m tracelens demo` |
| 2026-10-15 | [VaultGuard](projects/2026-10-15-vaultguard/) | Rubrik: preserve backup integrity, detect corruption, and prove recovery after ransomware or storage faults | `cd projects/2026-10-15-vaultguard && python -m vaultguard demo` |
| 2026-10-14 | [QuorumForge](projects/2026-10-14-quorumforge/) | Amazon Dynamo: keep key-value state available and repair replicas during continuous failures | `cd projects/2026-10-14-quorumforge && python -m quorumforge demo` |
| 2026-10-13 | [LedgerGate](projects/2026-10-13-ledgergate/) | Stripe: webhook delivery can be duplicated, delayed, and out of order | `cd projects/2026-10-13-ledgergate && python -m ledgergate demo` |
| 2026-10-12 | [SurgeGuard](projects/2026-10-12-surgeguard/) | Amazon: retries can amplify overload and delay recovery | `cd projects/2026-10-12-surgeguard && python -m surgeguard demo` |

These projects are independent engineering exercises. They are not company products or claims of work performed for those companies.
