# Primary sources and attribution boundary

## Company challenge

- **Company:** Google.
- **Paper:** *Zanzibar: Google’s Consistent, Global Authorization System*, Ruoming Pang et al., USENIX ATC ’19, pp. 33–46; conference July 10–12, 2019.
- [Google Research publication record](https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/).
- [Publisher/conference record and BibTeX](https://www.usenix.org/conference/atc19/presentation/pang).
- [Primary paper PDF](https://www.usenix.org/system/files/atc19-pang.pdf), especially §2.2 (printed pp. 34–35).
- Sources inspected for this implementation on October 19, 2026 (Asia/Calcutta).

## Facts versus implementation choices

**Source facts:** §2.2 presents stale-ACL/new-content examples and a token-based freshness protocol. §2.1 describes relationship tuples and nested usersets. These establish the engineering challenge; they are not a disclosure of a particular customer incident.

**Independent implementation:** ScopeGraph uses SQLite snapshots, per-tenant integer revisions, HMAC freshness tokens, a fixed deny-overrides policy, and an expiry-aware LRU. It deliberately co-locates synthetic documents and permissions to make the consistency boundary directly testable. It does not reproduce Google’s distributed storage, external consistency machinery, general policy language, or production architecture.

**Inference:** preventing accidental disclosure is relevant to collaborative SaaS and enterprise tools. No company revenue loss, incident count, production adoption, or interview outcome has been measured or claimed.

All performance numbers in this project come from the included local executable demo, not from these publications. ScopeGraph has no dependency on Google infrastructure or services.
