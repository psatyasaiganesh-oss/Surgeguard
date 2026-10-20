# Security model

RouteProof is a local engineering lab, not a production control plane.

- The HTTP listener is restricted to IPv4 loopback.
- Operator tokens must be at least 32 characters, are stored only as SHA-256 digests, and are never logged.
- Request, review, and execution are separate roles; two reviewers are required.
- JSON bodies use a 64 KiB cap, explicit Content-Length, no transfer encoding, duplicate-key rejection, and closed schemas.
- SQLite files are owner-only on POSIX systems.
- Analysis failure, stale topology, missing approval, unknown health, and actuator conflicts stop execution.

Do not expose the reference server to a network. Production use would require mTLS, external identity, a secret manager, highly available storage, signed inventory, trusted telemetry, rate limits, monitoring, and an independently reachable recovery control plane.
