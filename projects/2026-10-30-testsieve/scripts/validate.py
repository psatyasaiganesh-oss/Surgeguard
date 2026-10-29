from __future__ import annotations

import compileall
import json
import platform
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    started = time.perf_counter()
    compiled = all(
        compileall.compile_dir(ROOT / path, quiet=1)
        for path in ("testsieve", "tests", "scripts")
    )
    if not compiled:
        raise SystemExit("bytecode compilation failed")

    tests_started = time.perf_counter()
    tests = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    tests_seconds = time.perf_counter() - tests_started
    if tests.returncode:
        sys.stderr.write(tests.stdout)
        sys.stderr.write(tests.stderr)
        return tests.returncode
    match = re.search(r"Ran (\d+) tests in ([0-9.]+)s", tests.stdout + tests.stderr)
    if match is None:
        raise SystemExit("could not parse unittest summary")

    from testsieve.demo import run_benchmark, run_demo

    demo = run_demo()
    benchmark = run_benchmark(200)

    expected_localized = [
        "tests/discovery_test.py",
        "tests/payments_test.py",
        "tests/sticky_failure_test.py",
    ]
    assert demo["clean_shadow_runs"] == 2
    assert demo["localized_selection"]["run_tests"] == expected_localized
    assert demo["localized_selection"]["selection_fraction"] == 0.6
    assert demo["localized_selection"]["replay_returned_same_selection"]
    assert all(demo["guardrails"].values())
    assert not demo["shadow_miss"]["safe"]
    assert demo["shadow_miss"]["baseline_status"] == "QUARANTINED"
    assert demo["shadow_miss"]["next_run_full_suite"]
    assert all(demo["security"].values())
    assert demo["integrity"]["valid"]

    assert benchmark["suite_tests"] == 1_000
    assert benchmark["inventory_files"] == 1_251
    assert benchmark["changed_files"] == 4
    assert benchmark["selection_requests"] == 200
    assert benchmark["candidate_tests"] == 22
    assert benchmark["selection_fraction"] == 0.022
    assert benchmark["integrity"] == {
        "valid": True,
        "repositories": 1,
        "baselines": 1,
        "selections": 202,
        "shadow_results": 2,
        "audit_entries": 207,
    }

    output = {
        "project": "TestSieve",
        "environment": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "checks": {
            "bytecode_compilation": "PASS",
            "automated_tests": int(match.group(1)),
            "unittest_reported_seconds": float(match.group(2)),
            "test_process_seconds": round(tests_seconds, 6),
        },
        "demo": demo,
        "benchmark": benchmark,
        "validation_seconds": round(time.perf_counter() - started, 6),
        "validation": "PASS",
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
