# Interview walkthrough

## One-minute explanation

"I built SurgeGuard to explore an Amazon-documented reliability problem: retries can overload an already failing dependency. It is an asynchronous Python HTTP client with a shared token bucket, deadlines, jitter, and a bounded bulkhead. I used a synthetic local catalog service to inject failures and count real backend calls. In a 96-request persistent outage, an immediate four-attempt baseline produced 384 calls; a 12-token retry budget produced 108, a 71.875% reduction. Both still had zero successful requests, so that result demonstrates reduced amplification, not improved availability. A separate transient-failure experiment recovered all 96 requests with sufficient credit. I also implemented an optional breaker with one recovery probe and tested cancellation, stale completions, and malformed HTTP responses."

This is an independent implementation inspired by a public engineering article. Explain the source and experiments; do not imply employment at Amazon or production adoption.

## Evidence-based resume bullets

- Implemented SurgeGuard, an asynchronous Python HTTP client with token-bucket retry budgets, capped exponential jitter, total deadlines, load shedding, and optional single-probe circuit recovery; validated behavior with 39 automated unit and real HTTP integration tests.
- Reduced failed backend calls from 384 to 108 (71.875%) in a synthetic 96-request outage experiment, and demonstrated 96/96 successful recoveries in a separate transient-failure scenario with an explicitly configured retry budget.

Use these bullets after you have run the project and can explain the implementation. The measurements are local experiments and must keep that qualifier.

## Questions to prepare

| Question | Points to explain |
| --- | --- |
| Why can retries worsen outages? | Additional attempts spend capacity on an unhealthy dependency; retries across layers can multiply. Retry at one chosen layer and bound the extra work. |
| Why not retry every error? | A repeated 401 or 404 normally does not fix the request. This project retries an explicit transient status set and transport timeouts, and rejects protocol violations. |
| Why does a retry budget help? | Original requests remain possible; only extra attempts spend shared credit. During a complete outage, a fixed budget gives a direct bound on amplification. |
| Why not enable the breaker by default? | The state transitions complicate recovery and tuning. A budget is the default; the breaker is a separately tested option. |
| What race does the generation number solve? | A success started before the breaker opened must not close the new circuit. Only completions belonging to the active generation affect it. |
| What happens when a half-open caller is cancelled? | Its probe is abandoned, the bulkhead slot is released, and another caller may probe. Cancellation propagates rather than being mistaken for success. |
| How does Retry-After interact with jitter? | Wait at least the larger delay. If that delay consumes the remaining total deadline, stop; never shorten the server's requested pause to squeeze in a retry. |
| What does the bulkhead bound? | Active logical requests, including retry sleeps. Excess requests fail before network I/O; that is different from an unbounded queue. |
| What metric could mislead you? | Mixing millisecond rejections with successful response times can make median latency look good while useful requests remain slow. This implementation separates them. |
| What is required for production use? | Mature pooled HTTP transport, fleet-aware coordination where needed, observability export, SLO-based tuning, TLS and platform testing, and deliberate write idempotency. |
| Why doesn't 71.875% prove better availability? | Every request failed in the total outage. The benefit measured there is reduced failed backend load; recovery is a different experiment. |

## Five-minute demo

1. Open the README and explain the two Amazon sources and the independent implementation.
2. Run `python -m unittest discover -s tests -v` and identify the stale completion, cancellation, and HTTP framing tests.
3. Run `python -m surgeguard demo --json reports/my-demo.json`. Show backend calls, logical successes, rejection counts, and invariant results.
4. Open `surgeguard/controls.py` and walk through token refill and the breaker generation.
5. Explain one tradeoff you would change for production: pooled connections, a bounded queue, fleet-wide retry credit, or exported metrics. Describe the validation you would run before changing it.

## Learning exercises

- Predict the outage attempt count for 50 requests with a zero-refill capacity of 5, then verify it with a local experiment.
- Set `--max-inflight 4` and increase offered concurrency. Explain why rejected requests can have extremely low latency without improving availability.
- Read the unsafe-method test and explain why adding POST needs more than changing a set of allowed method names.
- Change the transient retry budget from 96 to 12 and explain the expected loss in successful recoveries. Keep the measurements separate from the committed baseline.
