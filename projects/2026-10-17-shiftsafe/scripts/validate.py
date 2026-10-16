"""Record actual checks and a demo run; no invented timings or cloud dependencies."""

from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)
checks = []


def check(name, args, *, timeout=120):
    start = time.perf_counter()
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    checks.append({"name": name, "command": args, "exit_code": result.returncode,
                   "elapsed_seconds": round(time.perf_counter()-start, 6)})
    if result.returncode:
        print(result.stdout + result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)
    return result


tests = check("unittest", [sys.executable, "-m", "unittest", "discover", "-v"])
(REPORTS / "tests.txt").write_text(tests.stdout + tests.stderr, encoding="utf-8")
check("compile", [sys.executable, "-m", "compileall", "-q", "shiftsafe", "tests", "scripts"])
demo = check("demo", [sys.executable, "-m", "shiftsafe", "demo"])
report = json.loads(demo.stdout)
assert report["success"] and report["verified_before_finalization"]["mismatches"] == 0
(REPORTS / "demo.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
container = {"available": shutil.which("docker") is not None, "executed": False}
if container["available"]:
    check("docker_build", ["docker", "build", "-t", "shiftsafe-validation", "."], timeout=300)
    run = check("docker_demo", ["docker", "run", "--rm", "--network", "none", "--read-only",
                               "--tmpfs", "/tmp:rw,noexec,nosuid,size=128m", "shiftsafe-validation", "demo"])
    assert json.loads(run.stdout)["success"]
    container["executed"] = True
else:
    container["reason"] = "Docker executable is not installed in this local environment; CI defines a container build/demo job."
metadata = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version, "checks": checks, "docker": container,
            "scope": "Local synthetic validation only; no production or cloud deployment."}
(REPORTS / "validation.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(json.dumps({"success": True, "checks": checks, "docker": container, "demo_timings": report["timings"]}, indent=2))
