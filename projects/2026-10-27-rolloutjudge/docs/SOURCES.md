# Source-to-design traceability

RolloutJudge is an independent implementation. The sources document real canary-analysis problems and practices; they do not describe this repository's code or validate its results.

## Primary sources

1. Michael Graff and Chris Sanden, Netflix Technology Blog, [Automated Canary Analysis at Netflix with Kayenta](https://netflixtechblog.com/automated-canary-analysis-at-netflix-with-kayenta-3260bc7acc69), April 10, 2018.
2. Alec Warner and Štěpán Davidovič with contributors, Google SRE Workbook, [Canarying Releases](https://sre.google/workbook/canarying-releases/).
3. Štěpán Davidovič with Betsy Beyer, Google/ACM Queue, [Canary Analysis Service](https://sre.google/static/pdf/canary_analysis.pdf), January–February 2018.

## Documented facts

- Netflix describes canarying as sending a small traffic portion to a new version beside the stable version and comparing key behavioral metrics.
- Netflix used production, newly created baseline, and canary clusters. A fresh baseline reduces misleading differences caused by long-running processes.
- Netflix reports that manual analysis took hours of graph/log review, limited builds to once or twice weekly, and made subtle differences hard to see.
- Kayenta separates metric retrieval from judgment. Its documented judgment stages include data validation, cleaning, metric comparison using Mann–Whitney U confidence intervals, score computation, and detailed reporting.
- Google defines canarying as a partial, time-limited deployment followed by evaluation, and reports that many incidents are triggered by binary or configuration pushes.
- Google's Canary Analysis Service is a passive observer. It asks whether the canary is meaningfully worse—not whether the service is absolutely healthy—and returns a clear PASS, FAIL, or NONE-style outcome to the rollout tool.

## Project inferences and independent decisions

| Evidence or need | RolloutJudge decision | Status |
| --- | --- | --- |
| Manual visual comparison is slow and may miss subtle differences. | Implement deterministic per-metric statistics and a machine-readable archived report. | Project inference |
| Many simultaneous metrics create false-positive risk. | Apply Benjamini–Hochberg adjustment across the judgment. | Independent design |
| Statistical significance alone can flag harmless tiny shifts. | Require configured absolute and relative median degradation guardrails as well. | Independent design |
| Missing telemetry should not be interpreted as healthy. | Return INCONCLUSIVE whenever an expected evidence slot is absent or incomplete unless known evidence already proves failure. | Independent design |
| Pipeline retries and policy changes can alter decisions accidentally. | Persist idempotent evidence, snapshot policies, fence judgment with a run version, and archive the report. | Independent design |
| A CI/CD consumer needs portable evidence. | Sign a receipt binding service, release, policy/trials/report digests, verdict, score, version, and time. | Independent design |
| Analysis should not own production mutation. | Remain a passive judge; promotion and rollback stay outside this process. | Aligned principle, independent implementation |

The synthetic demo and benchmark are local Python/SQLite measurements. They are not Kayenta, Google CAS, Netflix traffic, Google infrastructure, or production rollout results.
