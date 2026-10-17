"""Record actual local checks separately from simulated workload outcomes."""

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
REPORTS.mkdir(exist_ok=True)
checks = []


def check(name, command, timeout=120):
    started = time.perf_counter()
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    checks.append({"name": name, "command": command, "exit_code": result.returncode,
                   "elapsed_seconds": round(time.perf_counter()-started, 6)})
    if result.returncode:
        print(result.stdout+result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)
    return result


tests = check("unittest", [sys.executable, "-m", "unittest", "discover", "-v"])
test_log = tests.stdout+tests.stderr
(REPORTS / "tests.txt").write_text(test_log, encoding="utf-8")
count = int(re.search(r"Ran (\d+) tests", test_log).group(1))
check("compile", [sys.executable, "-m", "compileall", "-q", "fitfleet", "tests", "scripts"])
demo = check("demo", [sys.executable, "-m", "fitfleet", "demo"])
report = json.loads(demo.stdout)
assert report["success"] and report["safety"]["memory_shift_state"] == "ROLLED_BACK"
assert report["modeled_holdout_comparison"]["passed"]
(REPORTS / "demo.json").write_text(json.dumps(report, indent=2, sort_keys=True)+"\n", encoding="utf-8")
container = {"available": shutil.which("docker") is not None, "executed": False}
if container["available"]:
    check("docker_build", ["docker", "build", "-t", "fitfleet-validation", "."], timeout=300)
    result = check("docker_demo", ["docker", "run", "--rm", "--network", "none", "--read-only",
                                  "--tmpfs", "/tmp:rw,noexec,nosuid,size=128m", "fitfleet-validation", "demo"])
    assert json.loads(result.stdout)["success"]
    container["executed"] = True
else:
    container["reason"] = "Docker executable is unavailable locally; the GitHub Actions container job builds and runs the demo separately."
metadata = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version, "test_count": count, "checks": checks, "docker": container,
            "measured": "Test outcomes and local Python command execution time",
            "modeled": "Workload latency, memory guard retries, resource-time score and savings",
            "external_operations": "No cloud account, cluster, production workload or live resource deployment used"}
(REPORTS / "validation.json").write_text(json.dumps(metadata, indent=2, sort_keys=True)+"\n", encoding="utf-8")
print(json.dumps({"success": True, "test_count": count, "checks": checks, "docker": container,
                  "measured_tool_timings": report["measured_local_timings"],
                  "modeled_resource_score_saving_pct": report["modeled_holdout_comparison"]["resource_score_saving_pct"]}, indent=2))
