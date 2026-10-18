"""Capture actual command results; regenerate reports, not claimed production metrics."""

from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def main():
    REPORTS.mkdir(exist_ok=True)
    checks = []

    def check(name, command, timeout=120):
        start = time.perf_counter()
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
        checks.append({"name": name, "command": command, "exit_code": result.returncode,
                       "seconds": round(time.perf_counter() - start, 6)})
        if result.returncode:
            print(result.stdout + result.stderr, file=sys.stderr)
            raise SystemExit(result.returncode)
        return result

    tests = check("unittest", [sys.executable, "-m", "unittest", "discover", "-v"])
    log = tests.stdout + tests.stderr
    (REPORTS / "tests.txt").write_text(log, encoding="utf-8")
    count = int(re.search(r"Ran (\d+) tests", log).group(1))
    check("compile", [sys.executable, "-m", "compileall", "-q", "scopegraph", "tests", "scripts"])
    demo = json.loads(check("demo", [sys.executable, "-m", "scopegraph", "demo"]).stdout)
    assert demo["concurrent_read"]["inflight_reader_never_observed_new_content"]
    assert demo["concurrent_read"]["fresh_reader_denied"]
    assert demo["scenarios"]["old_revision_token_does_not_restore_access"]
    assert demo["scenarios"]["expiry_without_revision_change_denied"]
    assert demo["benchmark"]["cache"]["hits"] == 1000
    (REPORTS / "demo.json").write_text(json.dumps(demo, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    container = {"available": shutil.which("docker") is not None, "executed": False}
    if container["available"]:
        check("docker_build", ["docker", "build", "-t", "scopegraph-validation", "."], timeout=300)
        output = check("docker_demo", ["docker", "run", "--rm", "--network", "none", "--read-only",
                                       "--tmpfs", "/tmp:rw,noexec,nosuid,size=128m", "scopegraph-validation", "demo"])
        assert json.loads(output.stdout)["concurrent_read"]["fresh_reader_denied"]
        container["executed"] = True
    else:
        container["reason"] = "Docker executable unavailable locally; container build and demo are separate CI checks."
    report = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(),
              "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
              "test_count": count, "checks": checks, "docker": container,
              "measurement_scope": "actual local Python, SQLite, socket requests, tests, and sequential microbenchmark",
              "synthetic_inputs": "all identities, documents, graph relationships, and scenario clock",
              "external_services": "none; no company system, cloud account, or production data accessed"}
    (REPORTS / "validation.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"test_count": count, "checks": checks, "docker": container,
                      "benchmark": demo["benchmark"]}, indent=2))


if __name__ == "__main__":
    main()
