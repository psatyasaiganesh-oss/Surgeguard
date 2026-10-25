from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

from cellguard.api import create_server
from cellguard.demo import DEMO_SECRET, default_topology, run_benchmark, run_demo
from cellguard.errors import ValidationError
from cellguard.store import CellGuardStore


TENANT_TOKEN = "tenant-token-that-is-at-least-24-chars"
WORKER_TOKEN = "worker-token-that-is-at-least-24-chars"


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = CellGuardStore(
            Path(self.temporary.name) / "api.sqlite",
            default_topology(),
            secret=DEMO_SECRET,
        )
        self.server = create_server(
            self.store,
            port=0,
            tenant_tokens={"tenant-api": TENANT_TOKEN, "tenant-other": "o" * 30},
            worker_token=WORKER_TOKEN,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        token: str | None = None,
        raw: bytes | None = None,
        content_type: str = "application/json",
    ) -> tuple[int, dict[str, str], dict[str, Any]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        payload = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        if payload is not None:
            headers["Content-Type"] = content_type
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = json.loads(response.read())
        result_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, result_headers, data

    def test_health_is_public_and_hardened(self) -> None:
        status, headers, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok"})
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_assignment_comes_from_authenticated_identity(self) -> None:
        status, _, body = self.request("GET", "/v1/assignment", token=TENANT_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "tenant-api")
        self.assertEqual(len(body["cells"]), 2)

    def test_missing_and_wrong_auth_are_rejected(self) -> None:
        for token in (None, "wrong"):
            with self.subTest(token=token):
                status, headers, body = self.request("GET", "/v1/assignment", token=token)
                self.assertEqual(status, 401)
                self.assertIn("Bearer", headers["www-authenticate"])
                self.assertEqual(body["error"]["code"], "unauthorized")

    def test_submit_replay_and_tenant_scoped_status(self) -> None:
        request = {"idempotency_key": "request-1", "payload": {"task": "email"}}
        first_status, _, first = self.request("POST", "/v1/jobs", request, token=TENANT_TOKEN)
        replay_status, _, replay = self.request("POST", "/v1/jobs", request, token=TENANT_TOKEN)
        self.assertEqual(first_status, 201)
        self.assertEqual(replay_status, 200)
        self.assertEqual(first["job_id"], replay["job_id"])
        self.assertTrue(replay["replayed"])
        status, _, found = self.request(
            "GET", f"/v1/jobs/{first['job_id']}", token=TENANT_TOKEN
        )
        self.assertEqual(status, 200)
        self.assertEqual(found["tenant"], "tenant-api")
        hidden, _, _ = self.request("GET", f"/v1/jobs/{first['job_id']}", token="o" * 30)
        self.assertEqual(hidden, 404)

    def test_worker_claims_and_completes(self) -> None:
        _, _, submitted = self.request(
            "POST",
            "/v1/jobs",
            {"idempotency_key": "request-1", "payload": {"task": "work"}},
            token=TENANT_TOKEN,
        )
        status, _, response = self.request(
            "POST",
            "/internal/claims",
            {"cell_id": submitted["cell_id"], "worker": "worker-1"},
            token=WORKER_TOKEN,
        )
        self.assertEqual(status, 200)
        claim = response["claims"][0]
        self.assertGreaterEqual(len(claim["lease_token"]), 32)
        completed, _, result = self.request(
            "POST",
            f"/internal/jobs/{claim['job_id']}/complete",
            {"lease_token": claim["lease_token"], "result": {"ok": True}},
            token=WORKER_TOKEN,
        )
        self.assertEqual(completed, 200)
        self.assertEqual(result["state"], "SUCCEEDED")

    def test_worker_route_rejects_tenant_token(self) -> None:
        status, _, body = self.request(
            "POST",
            "/internal/claims",
            {"cell_id": "cell-00", "worker": "worker-1"},
            token=TENANT_TOKEN,
        )
        self.assertEqual(status, 401)
        self.assertEqual(body["error"]["code"], "unauthorized")

    def test_strict_json_rejects_duplicates_nonfinite_and_unknown_fields(self) -> None:
        cases = [
            b'{"idempotency_key":"a","idempotency_key":"b","payload":{}}',
            b'{"idempotency_key":"a","payload":NaN}',
            b'{"idempotency_key":"a","payload":{},"surprise":true}',
        ]
        for raw in cases:
            with self.subTest(raw=raw):
                status, _, body = self.request(
                    "POST", "/v1/jobs", token=TENANT_TOKEN, raw=raw
                )
                self.assertEqual(status, 400)
                self.assertEqual(body["error"]["code"], "validation_error")

    def test_wrong_content_type_is_rejected(self) -> None:
        status, _, body = self.request(
            "POST", "/v1/jobs", token=TENANT_TOKEN, raw=b"{}", content_type="text/plain"
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_query_string_is_rejected(self) -> None:
        status, _, body = self.request("GET", "/healthz?verbose=1")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_server_rejects_short_tokens_and_invalid_port(self) -> None:
        with self.assertRaises(ValidationError):
            create_server(self.store, port=0, tenant_tokens={"tenant": "short"}, worker_token="x" * 30)
        with self.assertRaises(ValidationError):
            create_server(
                self.store,
                port=0,
                tenant_tokens={"one": "x" * 30, "two": "x" * 30},
                worker_token="y" * 30,
            )
        with self.assertRaises(ValidationError):
            create_server(
                self.store,
                port=70_000,
                tenant_tokens={"tenant": "x" * 30},
                worker_token="y" * 30,
            )


class ScenarioTests(unittest.TestCase):
    def test_demo_proves_isolation_failover_fairness_and_fencing(self) -> None:
        report = run_demo()
        self.assertLess(report["blast_radius"]["fully_impacted_fraction"], 0.25)
        self.assertTrue(report["cell_failover"]["failed_cell_avoided"])
        self.assertTrue(
            report["cell_failover"]["no_cross_shard_escape_when_all_assigned_cells_failed"]
        )
        self.assertTrue(report["fair_admission"]["noisy_tenant_limit_enforced"])
        self.assertEqual(len(set(report["fair_admission"]["fair_claim_tenants"])), 2)
        self.assertTrue(report["lease_recovery"]["stale_completion_rejected"])
        self.assertEqual(report["verification"]["jobs"], 18)

    def test_benchmark_processes_every_job_and_verifies_database(self) -> None:
        report = run_benchmark(40, 8)
        self.assertEqual(report["processed"], 40)
        self.assertEqual(report["verification"]["jobs"], 40)
        self.assertGreater(report["end_to_end_jobs_per_second"], 0)

    def test_benchmark_validates_dimensions(self) -> None:
        for jobs, tenants in ((0, 1), (10, 0), (10, 11), (True, 1)):
            with self.subTest(jobs=jobs, tenants=tenants), self.assertRaises(ValueError):
                run_benchmark(jobs, tenants)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
