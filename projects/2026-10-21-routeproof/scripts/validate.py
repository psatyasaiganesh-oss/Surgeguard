#!/usr/bin/env python3
"""Run reproducible RouteProof validation without external dependencies."""

from __future__ import annotations

import json
import platform
import re
import sqlite3
import subprocess
import sys
import time


def run(command: list[str]) -> tuple[subprocess.CompletedProcess[str], float]:
    started = time.perf_counter()
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    return result, time.perf_counter() - started


def main() -> None:
    compile_result, compile_seconds = run([
        sys.executable, "-m", "compileall", "-q", "routeproof", "tests", "scripts"
    ])
    tests, test_seconds = run([
        sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"
    ])
    demo, demo_seconds = run([sys.executable, "-m", "routeproof", "demo"])
    combined = tests.stdout + tests.stderr
    match = re.search(r"Ran (\d+) tests?", combined)
    report = {
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.system(),
        "compileall": {
            "exit_code": compile_result.returncode,
            "seconds": round(compile_seconds, 6),
        },
        "tests": {
            "exit_code": tests.returncode,
            "count": int(match.group(1)) if match else None,
            "seconds": round(test_seconds, 6),
        },
        "demo": {
            "exit_code": demo.returncode,
            "seconds": round(demo_seconds, 6),
            "result": json.loads(demo.stdout) if demo.returncode == 0 else None,
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if compile_result.returncode or tests.returncode or demo.returncode:
        if compile_result.stderr:
            print(compile_result.stderr, file=sys.stderr)
        if tests.stderr:
            print(tests.stderr, file=sys.stderr)
        if demo.stderr:
            print(demo.stderr, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
