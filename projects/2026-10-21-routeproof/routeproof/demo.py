"""Reproducible end-to-end demonstration and local microbenchmark."""

from __future__ import annotations

import json
import secrets
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .actuator import Actuator
from .control import ConflictError, Controller, SimulatedCrash
from .graph import analyze
from .http import create_server
from .samples import safe_plan, sample_topology, unsafe_backbone_plan, unsafe_dns_plan


def _tokens() -> dict[str, str]:
    return {name: secrets.token_urlsafe(32) for name in ("alice", "bob", "carol", "dave")}


def _register(controller: Controller, tokens: dict[str, str]) -> None:
    controller.register_operator("alice", "requester", tokens["alice"])
    controller.register_operator("bob", "reviewer", tokens["bob"])
    controller.register_operator("carol", "reviewer", tokens["carol"])
    controller.register_operator("dave", "executor", tokens["dave"])


def _request(url: str, token: str | None = None) -> tuple[int, dict[str, Any]]:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers=headers)
    try:
        with opener.open(request, timeout=3) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def run_demo() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="routeproof-") as directory:
        root = Path(directory)
        actuator = Actuator(root / "actuator.db")
        actuator.initialize(sample_topology())
        controller = Controller(root / "control.db", actuator)
        tokens = _tokens()
        _register(controller, tokens)

        controller.create_plan(tokens["alice"], unsafe_backbone_plan())
        unsafe_backbone = controller.analyze_plan(tokens["alice"], "unsafe-backbone")

        controller.create_plan(tokens["alice"], unsafe_dns_plan())
        unsafe_dns = controller.analyze_plan(tokens["alice"], "unsafe-dns")

        def broken_analyzer(topology: Any, plan: Any) -> dict[str, Any]:
            raise TimeoutError("injected validator timeout")

        fail_closed = Controller(root / "control.db", actuator, analyzer=broken_analyzer)
        fail_plan = safe_plan(plan_id="validator-timeout")
        fail_closed.create_plan(tokens["alice"], fail_plan)
        failed_analysis = fail_closed.analyze_plan(tokens["alice"], "validator-timeout")

        controller.create_plan(tokens["alice"], safe_plan())
        safe = controller.analyze_plan(tokens["alice"], "safe-maintenance")
        after_one = controller.approve(tokens["bob"], "safe-maintenance")
        after_two = controller.approve(tokens["carol"], "safe-maintenance")
        crash_seen = False
        try:
            controller.execute_next(
                tokens["dave"], "safe-maintenance", crash_after_actuation=True
            )
        except SimulatedCrash:
            crash_seen = True
        version_after_crash = actuator.snapshot()["version"]
        resumed = controller.execute_next(tokens["dave"], "safe-maintenance")
        completed = controller.observe(
            tokens["dave"], "safe-maintenance", True, "synthetic probes: 100/100 healthy"
        )
        replay_events = [
            event for event in controller.audit_log(tokens["dave"])
            if event["event"] == "STEP_APPLIED"
            and event["plan_id"] == "safe-maintenance"
            and event["details"].get("replayed")
        ]
        rolled_back = controller.rollback(tokens["dave"], "safe-maintenance")

        current_version = actuator.snapshot()["version"]
        drift_plan = safe_plan(current_version, "drift-detected")
        drift_plan["steps"][0]["link_id"] = "dc-b-backbone-1"
        controller.create_plan(tokens["alice"], drift_plan)
        controller.analyze_plan(tokens["alice"], "drift-detected")
        controller.approve(tokens["bob"], "drift-detected")
        controller.approve(tokens["carol"], "drift-detected")
        actuator.external_change("dc-c-backbone-1", False)
        drift_blocked = False
        try:
            controller.execute_next(tokens["dave"], "drift-detected")
        except ConflictError:
            drift_blocked = True

        benchmark_topology = sample_topology()
        benchmark_plan = safe_plan()
        samples: list[float] = []
        started = time.perf_counter()
        for _ in range(500):
            one = time.perf_counter()
            analyze(benchmark_topology, benchmark_plan)
            samples.append((time.perf_counter() - one) * 1000)
        elapsed = time.perf_counter() - started
        samples.sort()

        server = create_server(controller, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        port = server.server_address[1]
        try:
            health_status, health_body = _request(f"http://127.0.0.1:{port}/healthz")
            unauthorized_status, _ = _request(
                f"http://127.0.0.1:{port}/v1/plans/safe-maintenance"
            )
            status_code, status_body = _request(
                f"http://127.0.0.1:{port}/v1/plans/safe-maintenance",
                tokens["dave"],
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

        return {
            "unsafe_backbone_blocked": unsafe_backbone["state"] == "BLOCKED",
            "unsafe_backbone_violation_codes": [
                item["code"] for item in unsafe_backbone["analysis"]["violations"]
            ],
            "dns_cascade_blocked": unsafe_dns["state"] == "BLOCKED",
            "dns_violation_codes": [item["code"] for item in unsafe_dns["analysis"]["violations"]],
            "validator_timeout_failed_closed": (
                failed_analysis["state"] == "BLOCKED"
                and failed_analysis["analysis"]["violations"][0]["code"] == "VALIDATOR_FAILURE"
            ),
            "approvals_after_first": after_one["approval_count"],
            "approvals_after_second": after_two["approval_count"],
            "crash_injected": crash_seen,
            "device_version_after_crash": version_after_crash,
            "resumed_state": resumed["state"],
            "idempotent_replay_recorded": len(replay_events) == 1,
            "completed_state": completed["state"],
            "rollback_state": rolled_back["state"],
            "topology_drift_blocked": drift_blocked,
            "drift_plan_state": controller.status(tokens["dave"], "drift-detected")["state"],
            "http": {
                "health_status": health_status,
                "health_ok": health_body.get("ok"),
                "unauthorized_status": unauthorized_status,
                "authenticated_status": status_code,
                "authenticated_plan_state": status_body.get("state"),
            },
            "benchmark": {
                "analyses": 500,
                "elapsed_seconds": round(elapsed, 6),
                "analyses_per_second": round(500 / elapsed, 2),
                "p50_ms": round(samples[249], 4),
                "p95_ms": round(samples[474], 4),
            },
            "scope": "synthetic topology and local SQLite stores; not production network control",
        }


def main() -> None:
    print(json.dumps(run_demo(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
