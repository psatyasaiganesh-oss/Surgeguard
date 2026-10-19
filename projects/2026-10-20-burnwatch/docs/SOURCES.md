# Sources and attribution boundary

- **Company:** Google.
- **Primary chapter:** [Alerting on SLOs](https://sre.google/workbook/alerting-on-slos/), chapter 5 of *The Site Reliability Workbook*, by Steven Thurgood, with Jess Frame, Anthony Lenton, Carmela Quinito, Anton Tolchanov, and Nejc Trdin.
- **Relevant sections:** “Multiwindow, Multi-Burn-Rate Alerts,” “Low-Traffic Services and Error Budget Alerting,” and “Alerting at Scale.”
- **Book record:** the [Google-hosted colophon](https://sre.google/workbook/colophon/) identifies the 2018 copyright and publisher. The chapter itself does not display a day-level publication date; none is invented here.
- Sources verified on October 20, 2026 (Asia/Calcutta).

**Documented challenge:** useful SLO alerts must balance significance, detection, reset behavior, and overlapping signals.

**Our inference:** inaccurate or noisy alerts can distract a small engineering team from customer-impacting failures. No company loss, incident history, or production improvement is asserted.

**Independent engineering:** all implementation details—closed-minute producer watermarks, immutable count buckets, sample floors, exact rational comparisons, conservative unknown-data latches, SQLite transactions, local API keys, retention/integrity checks, and the idempotent inbox—are this project’s choices. BurnWatch does not reproduce Google’s monitoring infrastructure or claim its performance.

The README’s mathematical rule table is attributed to the chapter. All captured timings and test outcomes come from the executable local code; simulated rule timings are labeled separately from real elapsed time. No external paging or cloud service was used.
