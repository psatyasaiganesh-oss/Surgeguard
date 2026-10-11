# Source guide

| File | What to study |
| --- | --- |
| `surgeguard/controls.py` | State transitions, a token bucket, and monotonic time. Start here. |
| `surgeguard/client.py` | `async`/`await`, retry classification, deadlines, and `try`/`finally` for resource release. |
| `surgeguard/transport.py` | URL validation, TCP streams, response framing, body limits, and TLS defaults. |
| `surgeguard/lab.py` | A small asynchronous server that injects failures using synthetic data. |
| `surgeguard/demo.py` | Workload generation, controlled experiments, direct measurements, and invariants. |
| `surgeguard/metrics.py` | Counters and bounded recent latency samples. |
| `surgeguard/__main__.py` | The command-line interface and configuration validation. |
| `tests/test_controls.py` | Fake-clock tests and unsafe-input validation. |
| `tests/test_client.py` | Concurrent failures, cancellation, recovery, and response classification. |
| `tests/test_http.py` | Actual HTTP connections, invalid framing, and a complete demo check. |

Understand three terms before the interview:

- A **logical request** is what the caller wants to do once.
- An **attempt** is one invocation of the HTTP adapter; a logical request may make several attempts.
- A **retry** is an attempt after the first. Retry credit bounds these additional attempts.

The local experiment counts both client attempts and requests actually received by the HTTP server. That gives an independent check of traffic amplification.
