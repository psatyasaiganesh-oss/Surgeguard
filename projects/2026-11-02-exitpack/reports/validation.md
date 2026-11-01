# Validation record

Recorded on 2 November 2026 in the supplied Linux x86-64 / Python 3.12.14 environment.

| Command | Result |
| --- | --- |
| `python -m compileall -q exitpack tests` | passed |
| `python -m unittest discover -q` | 36 tests passed in 3.896 seconds |
| `python -m exitpack demo` | both injected lost acknowledgements recovered; stale plan blocked; misprediction adapted; integrity valid |
| `python -m exitpack benchmark --hosts 40 --groups 32` | 512 placements and 448 completions; 8 vs. 36 empty hosts; both integrity scans valid |

The benchmark is deliberately adversarial synthetic data. It is not Google production data, an ML evaluation, or a measurement of Google LAVA.
