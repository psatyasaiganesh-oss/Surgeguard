from __future__ import annotations

import compileall
import json
import platform
import re
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
        for path in ("tabletforge", "tests", "scripts")
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

    from tabletforge.demo import run_benchmark, run_demo

    demo = run_demo()
    benchmark = run_benchmark(entries=3_000, reads=1_000)

    recovery = demo["recovery"]
    assert recovery["crash_after_wal_fsync_triggered"]
    assert recovery["durable_order_recovered"]
    assert recovery["retry_returned_same_sequence"]
    assert recovery["crash_after_segment_rename_triggered"]
    assert recovery["orphan_segment_quarantined"]
    assert recovery["committed_flush_records"] == 5
    assert recovery["partial_wal_tail_recovered"]
    assert recovery["partial_wal_bytes_truncated"] == 2
    assert demo["snapshot_isolation"] == {
        "snapshot_alice": "balance=100",
        "snapshot_bob": "balance=75",
        "current_alice": "balance=60",
        "current_bob_missing": True,
    }
    assert demo["compaction"]["with_snapshot"]["records_after"] == 7
    assert demo["compaction"]["after_snapshot_release"]["reclaimed_records"] == 6
    assert demo["compaction"]["deleted_carol_missing"]
    assert demo["bloom_negative_skips"] > 0
    assert demo["tampered_manifest_rejected"]
    assert demo["integrity"] == {
        "valid": True,
        "manifest_generation": 7,
        "segments": 1,
        "segment_records": 3,
        "tombstones": 0,
        "wal_frames": 0,
        "active_snapshots": 0,
        "latest_sequence": 9,
    }

    assert benchmark["entries"] == 3_000
    assert benchmark["read_requests"] == 1_000
    assert benchmark["missing_read_requests"] == 500
    assert benchmark["write_batches"] == 30
    assert benchmark["segments_before_compaction"] == 7
    assert benchmark["records_before_compaction"] == 3_300
    assert benchmark["records_after_compaction"] == 2_850
    assert benchmark["reclaimed_records"] == 450
    assert benchmark["bloom_negative_skips"] > 0
    assert benchmark["point_read_latency_ms"]["p95"] >= benchmark["point_read_latency_ms"]["p50"]
    assert benchmark["integrity"] == {
        "valid": True,
        "manifest_generation": 8,
        "segments": 1,
        "segment_records": 2_850,
        "tombstones": 0,
        "wal_frames": 0,
        "active_snapshots": 0,
        "latest_sequence": 3_300,
    }

    output = {
        "project": "TabletForge",
        "environment": {
            "python": platform.python_version(),
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
