"""Regenerate reports from actual commands; keep synthetic outcomes clearly labeled."""

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
        began = time.perf_counter()
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
        checks.append({"name": name, "command": command, "exit_code": result.returncode,
                       "seconds": round(time.perf_counter() - began, 6)})
        if result.returncode:
            print(result.stdout + result.stderr, file=sys.stderr)
            raise SystemExit(result.returncode)
        return result

    result = check("unittest", [sys.executable, "-m", "unittest", "discover", "-v"])
    log = result.stdout + result.stderr
    (REPORTS / "tests.txt").write_text(log, encoding="utf-8")
    count = int(re.search(r"Ran (\d+) tests", log).group(1))
    check("compile", [sys.executable, "-m", "compileall", "-q", "burnwatch", "tests", "scripts", "examples"])
    demo = json.loads(check("demo", [sys.executable, "-m", "burnwatch", "demo"]).stdout)
    assert demo["scenario"]["brief_spike_multiwindow_did_not_page"]
    assert demo["scenario"]["sustained_20_percent_error_page_detection_minutes"] == 5
    assert demo["scenario"]["missing_producer_status"] == "PAGE_HOLD"
    assert demo["delivery"]["outbox_pending"] == 0 and demo["delivery"]["duplicate_deliveries"] == 1
    assert demo["benchmark"]["ingested_minute_reports"] == 10000
    (REPORTS / "demo.json").write_text(json.dumps(demo, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    docker = {"available": shutil.which("docker") is not None, "executed": False}
    if docker["available"]:
        check("docker_build", ["docker", "build", "-t", "burnwatch-validation", "."], timeout=300)
        output = check("docker_demo", ["docker", "run", "--rm", "--network", "none", "--read-only",
                                      "--tmpfs", "/tmp:rw,noexec,nosuid,size=128m", "burnwatch-validation", "demo"])
        assert json.loads(output.stdout)["delivery"]["outbox_pending"] == 0
        docker["executed"] = True
    else:
        docker["reason"] = "Docker executable unavailable locally; container build/test/demo are separate CI checks."
    metadata = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
                "sqlite": sqlite3.sqlite_version, "test_count": count, "checks": checks, "docker": docker,
                "measured": "actual local command times, SQLite report ingestion/evaluation, HTTP responses, and inbox effects",
                "synthetic": "minute-level request counts, error patterns, producer interruptions, scenario clock",
                "not_claimed": "live user request throughput, production alert precision, Google performance, or real pager delivery"}
    (REPORTS / "validation.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"test_count": count, "checks": checks, "docker": docker,
                      "benchmark": demo["benchmark"], "delivery": demo["delivery"]}, indent=2))


if __name__ == "__main__":
    main()
