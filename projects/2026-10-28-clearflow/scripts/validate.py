from __future__ import annotations

import compileall
import json
import platform
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
    compiled = compileall.compile_dir(ROOT / "clearflow", quiet=1) and compileall.compile_dir(
        ROOT / "tests", quiet=1
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

    from clearflow.demo import run_benchmark, run_demo

    demo_started = time.perf_counter()
    demo = run_demo()
    demo_seconds = time.perf_counter() - demo_started
    benchmark_started = time.perf_counter()
    benchmark = run_benchmark(200)
    benchmark_seconds = time.perf_counter() - benchmark_started
    test_count = sum(
        1 for line in tests.stderr.splitlines() if line.startswith("test_") and line.endswith("... ok")
    )
    result = {
        "project": "ClearFlow",
        "environment": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
        },
        "checks": {
            "compileall": True,
            "tests_passed": test_count,
            "tests_seconds": round(tests_seconds, 6),
            "demo_seconds": round(demo_seconds, 6),
            "benchmark_wrapper_seconds": round(benchmark_seconds, 6),
            "total_seconds": round(time.perf_counter() - started, 6),
        },
        "demo": demo,
        "benchmark": benchmark,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
