# BurnWatch interview walkthrough

## Two evidence-based resume bullets

- Implemented BurnWatch, an independent Google SRE-inspired Python/SQLite alerting pipeline with producer completeness watermarks, weighted multi-window burn-rate evaluation, exact threshold comparisons, service-bound collector credentials, and durable notification state.
- Validated the pipeline with 82 automated tests covering boundary math, telemetry gaps, duplicate reports, concurrent collectors, process crashes, and HTTP isolation; benchmarked ingestion of 10,000 synthetic minute reports and 200 local SLO evaluations, with crash-safe idempotent inbox delivery.

Only use these if you can explain and reproduce the implementation. Do not describe it as Google employment, production SRE experience, live request throughput, production-scale performance, or guaranteed screening success.

## Five-minute demonstration

1. **Motivation (45 seconds).** Explain the sourced SLO alerting problem in your own words, then state the independent scope: availability/error-count monitoring on a local database, not a replacement for a production monitoring stack.
2. **Data contract (60 seconds).** Show `examples/config.json` and a bucket with `minute`, `total`, and `bad`. Describe exclusive watermarks, explicit zero-traffic heartbeats, immutable reports, and the minimum watermark across expected sources. Demonstrate why a partially reported minute is not representative of the whole service.
3. **Decision math (45 seconds).** Open `assess` in `model.py`. Show both window comparisons and integer cross-multiplication. Explain the weighted aggregation test: one failing request out of one plus zero failures out of 9,999 is one failure in 10,000—not a 50% error rate.
4. **Adversarial demo (60 seconds).** Run `python -m burnwatch demo`. Point out the brief spike, sustained outage, three overlapping rules with one PAGE transition, producer interruption holding the incident, and the faster short-window reset. Make clear that scenario minutes are simulated.
5. **Delivery and evidence (45 seconds).** Show store-before-ack in `delivery.py`, the abrupt-exit test, and the duplicate suppression result. Explain why the two databases cannot atomically commit together and why the inbox effect can still be idempotent.
6. **Tradeoffs (45 seconds).** Finish with trusted producers/time, conservative unknown data, explicit evaluation/delivery scheduling, manual retention, a local single writer, and the absence of real pager integration. Separate measured command timings from simulated outage outcomes.

## Questions to prepare for

**Why not alert whenever the last five minutes exceed the SLO?** Use the included spike example and demonstrate the decision with the real implementation. The paired-window rule requires significant long-window evidence and continuing short-window failure.

**What if one producer disappears?** The global watermark stops. After the freshness allowance, evidence is STALE; an active incident is held, and a telemetry-quality transition is recorded. This is explicit uncertainty, not green health. It can delay SLO detection, so telemetry health is itself operationally important.

**Why not fill missing buckets with zeros?** That fabricates observations and can dilute the measured failure ratio. A real zero/zero heartbeat is an explicit declaration by a trusted producer; a missing report is not.

**Why are there per-rule latches?** Complete, sufficiently sampled evidence can update a rule. Missing data cannot clear a previously firing one. The aggregate severity is PAGE before TICKET, while quality is tracked separately. This preserves uncertainty without creating one page per overlapping rule.

**How do you avoid duplicate telemetry counts?** A minute is uniquely identified by service, producer, and timestamp. Exact duplicates do not add to the materialized totals. Conflicting duplicates fail. Raw rows, rollups, watermarks, and retry receipts commit in one transaction.

**Why not allow late corrections after sealing?** A sealed window may already have produced a notification. Retroactive correction needs versioned evidence and a deliberate retraction protocol. The lab rejects corrections rather than silently rewriting the facts behind a previous decision.

**Is notification delivery exactly once?** No. A failure after durable inbox storage but before the collector acknowledgement causes another attempt. A stable event ID and unique inbox record prevent a duplicate inbox effect. External email or pager systems would need a compatible idempotency contract; none is claimed here.

**What do the benchmark numbers mean?** The time to persist 10,000 aggregate records in 20 local transactions and evaluate 200 policy snapshots. They are not 10,000 live HTTP requests, a production load test, a business saving, or a Google performance number.

**Why not deploy the SQLite file directly to stateless compute?** The collector, evaluation state, receipts, and outbox require durable shared storage. A real deployment needs persistent storage, explicit scheduling, identity lifecycle, availability decisions, and a reviewed public transport—not just a hosting configuration file.
