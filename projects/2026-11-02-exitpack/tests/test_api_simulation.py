from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from exitpack.api import build_server
from exitpack.simulation import run_benchmark, run_demo
from exitpack.store import ExitPack


SECRET = b"exitpack-api-test-secret-at-least-32-bytes"
TOKEN_A = "exitpack-token-a-for-tests"
TOKEN_B = "exitpack-token-b-for-tests"


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = ExitPack(Path(self.temporary.name) / "api.db", secret=SECRET)
        self.server = build_server("127.0.0.1", 0, self.store, (TOKEN_A, TOKEN_B))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.store.close()
        self.temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        value: dict | str | None = None,
        *,
        token: str | None = TOKEN_A,
        content_type: str = "application/json",
    ) -> tuple[int, dict, dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        body = json.dumps(value) if isinstance(value, dict) else value
        headers = {"Content-Type": content_type}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, payload, response_headers

    def host(self) -> dict:
        return {
            "idempotency_key": "register-host",
            "host_id": "host-0",
            "zone": "zone-a",
            "cpu_capacity": 8_000,
            "memory_capacity": 32_000,
        }

    def test_health_and_security_headers(self) -> None:
        status, body, headers = self.request("GET", "/health", token=None)
        self.assertEqual((status, body), (200, {"status": "ok"}))
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_authentication_and_namespace_isolation(self) -> None:
        self.assertEqual(self.request("GET", "/v1/metrics", token=None)[0], 401)
        self.assertEqual(self.request("POST", "/v1/hosts", self.host())[0], 201)
        self.assertEqual(self.request("GET", "/v1/metrics", token=TOKEN_B)[0], 404)

    def test_http_placement_lifecycle(self) -> None:
        self.assertEqual(self.request("POST", "/v1/hosts", self.host())[0], 201)
        plan_body = {
            "idempotency_key": "plan-1",
            "policy": "LIFETIME",
            "vm": {
                "vm_id": "vm-1",
                "cpu_millis": 2_000,
                "memory_mib": 8_000,
                "predicted_lifetime_s": 1_000,
            },
        }
        status, plan, _ = self.request("POST", "/v1/plans", plan_body)
        self.assertEqual(status, 201)
        status, committed, _ = self.request(
            "POST",
            f"/v1/plans/{plan['plan_id']}/commit",
            {"commit_token": plan["commit_token"]},
        )
        self.assertEqual((status, committed["vm_id"]), (200, "vm-1"))
        status, metrics, _ = self.request("GET", "/v1/metrics")
        self.assertEqual((status, metrics["running_vms"]), (200, 1))
        status, finished, _ = self.request(
            "POST", "/v1/vms/vm-1/finish", {"idempotency_key": "finish-1"}
        )
        self.assertEqual((status, finished["vm_id"]), (200, "vm-1"))

    def test_duplicate_and_unknown_fields_are_rejected(self) -> None:
        duplicate = '{"host_id":"a","host_id":"b"}'
        status, body, _ = self.request("POST", "/v1/hosts", duplicate)
        self.assertEqual(status, 400)
        self.assertIn("duplicate field", body["error"]["message"])
        bad = {**self.host(), "root": True}
        self.assertEqual(self.request("POST", "/v1/hosts", bad)[0], 400)

    def test_content_type_and_canonical_path_are_enforced(self) -> None:
        self.assertEqual(
            self.request("POST", "/v1/hosts", "{}", content_type="text/plain")[0], 400
        )
        self.assertEqual(self.request("GET", "/v1//metrics")[0], 400)


class SimulationTests(unittest.TestCase):
    def test_demo_exercises_failure_and_adaptation_paths(self) -> None:
        result = run_demo()
        self.assertTrue(result["integrity"]["valid"])
        self.assertTrue(result["optimistic_concurrency"]["stale_plan_blocked"])
        self.assertEqual(result["maintenance"]["lifetime_aware_migrations"], 1)
        self.assertEqual(result["misprediction_adaptation"]["after_class"], 1)
        self.assertTrue(result["lost_acknowledgements"]["placement_retry_replayed"])

    def test_small_benchmark_improves_empty_host_availability(self) -> None:
        result = run_benchmark(hosts=12, groups=8)
        self.assertEqual(result["baseline"]["placements"], 64)
        self.assertGreater(
            result["comparison"]["lifetime_empty_hosts_after_short_exit"],
            result["comparison"]["baseline_empty_hosts_after_short_exit"],
        )
        self.assertTrue(result["baseline"]["integrity"]["valid"])
        self.assertTrue(result["lifetime_aware"]["integrity"]["valid"])


if __name__ == "__main__":
    unittest.main()
