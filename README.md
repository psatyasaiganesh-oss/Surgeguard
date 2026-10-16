# Surgeguard
Code for mtc

## Company engineering challenges

Working, independent implementations inspired by documented company engineering problems. Each project includes its primary sources, local demonstration, automated tests, measured results, and design tradeoffs.

| Date | Project | Company problem | Run |
| --- | --- | --- | --- |
| 2026-10-17 | [ShiftSafe](projects/2026-10-17-shiftsafe/) | Stripe: migrate subscription schemas while preserving live writes, verified read cutover, and rollback | `cd projects/2026-10-17-shiftsafe && python -m shiftsafe demo` |
| 2026-10-16 | [TraceLens](projects/2026-10-16-tracelens/) | Google Dapper/Cloud Trace: diagnose request latency across distributed services while controlling retained trace detail | `cd projects/2026-10-16-tracelens && python -m tracelens demo` |
| 2026-10-15 | [VaultGuard](projects/2026-10-15-vaultguard/) | Rubrik: preserve backup integrity, detect corruption, and prove recovery after ransomware or storage faults | `cd projects/2026-10-15-vaultguard && python -m vaultguard demo` |
| 2026-10-14 | [QuorumForge](projects/2026-10-14-quorumforge/) | Amazon Dynamo: keep key-value state available and repair replicas during continuous failures | `cd projects/2026-10-14-quorumforge && python -m quorumforge demo` |
| 2026-10-13 | [LedgerGate](projects/2026-10-13-ledgergate/) | Stripe: webhook delivery can be duplicated, delayed, and out of order | `cd projects/2026-10-13-ledgergate && python -m ledgergate demo` |
| 2026-10-12 | [SurgeGuard](projects/2026-10-12-surgeguard/) | Amazon: retries can amplify overload and delay recovery | `cd projects/2026-10-12-surgeguard && python -m surgeguard demo` |

These projects are independent engineering exercises. They are not company products or claims of work performed for those companies.
