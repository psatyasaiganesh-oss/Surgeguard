# Validation record

Recorded on 1 November 2026 in the supplied Linux x86-64 / Python 3.12.14 environment.

| Command | Result |
| --- | --- |
| `python -m compileall -q slicequeue tests` | passed |
| `python -m unittest discover -q` | 32 tests passed in 3.179 seconds |
| `python -m slicequeue demo` | 6/6 events processed; three injected lost acknowledgements recovered; integrity valid |
| `python -m slicequeue benchmark --threads 250 --events-per-thread 2` | 500/500 events in 2.173897 seconds; integrity valid |

These are local synthetic results, not production measurements or an AWS Infinity comparison.
