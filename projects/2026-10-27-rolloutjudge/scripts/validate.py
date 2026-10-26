#!/usr/bin/env python3
"""Run all reproducible checks and emit a machine-readable report."""

from __future__ import annotations

import json
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import time


def run(command: list[str]) -> tuple[subprocess.CompletedProcess[str], float]:
    started = time.perf_counter()
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    return result, time.perf_counter() - started


def decode_json(result: subprocess.CompletedProcess[str]) -> object | None:
    if result.returncode:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def main() -> None:
    compile_result, compile_seconds = run(
        [sys.executable, "-m", "compileall", "-q", "rolloutjudge", "tests", "scripts"]
    )
    tests, test_seconds = run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"]
    )
    demo, demo_seconds = run([sys.executable, "-m", "rolloutjudge", "demo"])
    benchmark, benchmark_seconds = run(
        [sys.executable, "-m", "rolloutjudge", "benchmark", "--analyses", "100"]
    )
    combined = tests.stdout + tests.stderr
    match = re.search(r"Ran (\d+) tests?", combined)
    report = {
        "benchmark": {
            "exit_code": benchmark.returncode,
            "result": decode_json(benchmark),
            "seconds": round(benchmark_seconds, 6),
        },
        "compileall": {
            "exit_code": compile_result.returncode,
            "seconds": round(compile_seconds, 6),
        },
        "demo": {
            "exit_code": demo.returncode,
            "result": decode_json(demo),
            "seconds": round(demo_seconds, 6),
        },
        "docker": {
            "available_locally": shutil.which("docker") is not None,
            "executed_by_this_script": False,
        },
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
        },
        "tests": {
            "count": int(match.group(1)) if match else None,
            "exit_code": tests.returncode,
            "seconds": round(test_seconds, 6),
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    results = (compile_result, tests, demo, benchmark)
    if any(result.returncode for result in results) or any(
        decode_json(result) is None for result in (demo, benchmark)
    ):
        for result in results:
            if result.stderr:
                print(result.stderr, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
