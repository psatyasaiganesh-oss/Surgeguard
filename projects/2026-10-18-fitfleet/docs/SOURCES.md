# Primary sources and independent scope

Reviewed October 18, 2026.

| Source | Supported facts | Independent FitFleet choices |
| --- | --- | --- |
| [Netflix: Netflix Stratum Media Processing: Automated Container Right-Sizing](https://netflixtechblog.com/netflix-stratum-media-processing-automated-container-right-sizing-ddf9aa68a801), Violetta Pidvolotska and Naveen Mareddy, September 25, 2026 | Container over-provisioning, per-resource opt-in, verified recommendations, monitoring, and reverting changes after a workload shift | Standard-library Python CLI; deterministic demand model; immutable negative evidence; chronological holdouts; local transactional controller with version-CAS and override protection |
| [Kubernetes: Resource Management for Pods and Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/), living documentation | CPU/memory resource quantities and the distinction between requests and limits | Job JSON export with equal requests/limits; no cluster mutation or claim of native automatic resizing |

The documented Netflix failure motivates evaluating later windows, not just initial verification. FitFleet does not reproduce its production platform, orchestrator, telemetry stores, experiment fleet, scale, or results. The article's original header gives September 25, 2026; later search/indexing dates are not treated as publication dates.

The model equations, score weight, dataset, default thresholds, test results, and tool timings are this project's own implementation and evidence. A 15% memory-change cap follows the source's conservative incremental-change idea, while the full policy and replay model remain a separate lab design. Source-derived business motivation is distinct from unmeasured potential benefits.

No proprietary code, Netflix data, company credentials, real user activity, cloud prices, or company performance numbers are included.
