#!/usr/bin/env python3
"""Run CacheFence checks and emit one machine-readable report."""

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


def main() -> None:
    compile_result, compile_seconds = run(
        [sys.executable, "-m", "compileall", "-q", "cachefence", "tests", "scripts"]
    )
    tests, test_seconds = run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"]
    )
    demo, demo_seconds = run([sys.executable, "-m", "cachefence", "demo"])
    combined = tests.stdout + tests.stderr
    match = re.search(r"Ran (\d+) tests?", combined)
    demo_result = None
    try:
        demo_result = json.loads(demo.stdout) if demo.returncode == 0 else None
    except json.JSONDecodeError:
        pass
    report = {
        "environment": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
        },
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
            "result": demo_result,
        },
        "docker": {
            "available_locally": shutil.which("docker") is not None,
            "executed_by_this_script": False,
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if compile_result.returncode or tests.returncode or demo.returncode or not demo_result:
        for result in (compile_result, tests, demo):
            if result.stderr:
                print(result.stderr, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
