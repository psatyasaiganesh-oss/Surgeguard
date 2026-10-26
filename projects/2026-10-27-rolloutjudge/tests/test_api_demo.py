from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

from rolloutjudge.api import create_server
from rolloutjudge.demo import DEMO_SECRET, default_policy, run_benchmark, run_demo
from rolloutjudge.errors import ValidationError
from rolloutjudge.store import RolloutJudgeStore


CHECKOUT_TOKEN = "checkout-token-with-more-than-24-characters"
CATALOG_TOKEN = "catalog-token-with-more-than-24-characters"
BASE = list(range(100, 140))
SAFE = list(range(101, 141))
THROUGHPUT = list(range(1000, 1040))


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = RolloutJudgeStore(
            Path(self.temporary.name) / "api.sqlite", secret=DEMO_SECRET
        )
        self.store.create_policy("standard", default_policy())
        self.server = create_server(
            self.store,
            port=0,
            service_tokens={"checkout": CHECKOUT_TOKEN, "catalog": CATALOG_TOKEN},
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
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=4)
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        payload = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        if payload is not None:
            headers["Content-Type"] = content_type
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = json.loads(response.read())
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, response_headers, data

    def start_run(self) -> dict[str, Any]:
        status, _, body = self.request(
            "POST",
            "/v1/runs",
            {
                "expected_trials": ["zone-a"],
                "idempotency_key": "request-1",
                "policy_id": "standard",
                "release": "release-1",
            },
            token=CHECKOUT_TOKEN,
        )
        self.assertEqual(status, 201)
        return body

    def test_health_is_public_and_hardened(self) -> None:
        status, headers, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok"})
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_missing_and_invalid_auth_are_rejected(self) -> None:
        for token in (None, "wrong"):
            with self.subTest(token=token):
                status, headers, body = self.request("POST", "/v1/runs", {}, token=token)
                self.assertEqual(status, 401)
                self.assertIn("Bearer", headers["www-authenticate"])
                self.assertEqual(body["error"]["code"], "unauthorized")

    def test_start_replay_and_get_run(self) -> None:
        first = self.start_run()
        status, _, replay = self.request(
            "POST",
            "/v1/runs",
            {
                "expected_trials": ["zone-a"],
                "idempotency_key": "request-1",
                "policy_id": "standard",
                "release": "release-1",
            },
            token=CHECKOUT_TOKEN,
        )
        self.assertEqual(status, 200)
        self.assertEqual(first["run_id"], replay["run_id"])
        self.assertTrue(replay["replayed"])
        found_status, _, found = self.request(
            "GET", f"/v1/runs/{first['run_id']}", token=CHECKOUT_TOKEN
        )
        self.assertEqual(found_status, 200)
        self.assertEqual(found["evidence"]["expected"], 4)

    def test_other_service_cannot_observe_run(self) -> None:
        run = self.start_run()
        status, _, body = self.request(
            "GET", f"/v1/runs/{run['run_id']}", token=CATALOG_TOKEN
        )
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")

    def test_evidence_judgment_and_receipt_verification_flow(self) -> None:
        run = self.start_run()
        series = {
            "latency_ms": (BASE, SAFE),
            "error_rate": ([0.01] * 40, [0.011] * 40),
            "cpu_ratio": ([0.6] * 40, [0.61] * 40),
            "throughput_rps": (THROUGHPUT, THROUGHPUT),
        }
        for metric, (baseline, canary) in series.items():
            status, _, _ = self.request(
                "POST",
                f"/v1/runs/{run['run_id']}/evidence",
                {
                    "baseline": baseline,
                    "batch_key": f"batch-{metric}",
                    "canary": canary,
                    "metric": metric,
                    "trial": "zone-a",
                },
                token=CHECKOUT_TOKEN,
            )
            self.assertEqual(status, 201)
        _, _, current = self.request(
            "GET", f"/v1/runs/{run['run_id']}", token=CHECKOUT_TOKEN
        )
        status, _, judged = self.request(
            "POST",
            f"/v1/runs/{run['run_id']}/judge",
            {"decision_key": "decision-1", "expected_version": current["version"]},
            token=CHECKOUT_TOKEN,
        )
        self.assertEqual(status, 200)
        self.assertEqual(judged["verdict"], "PASS")
        verify_status, _, verified = self.request(
            "POST",
            "/v1/receipts/verify",
            {"receipt": judged["receipt"]},
            token=CHECKOUT_TOKEN,
        )
        self.assertEqual(verify_status, 200)
        self.assertEqual(verified, {"valid": True, "verdict": "PASS"})

    def test_evidence_replay_uses_200(self) -> None:
        run = self.start_run()
        body = {
            "baseline": BASE,
            "batch_key": "batch-1",
            "canary": SAFE,
            "metric": "latency_ms",
            "trial": "zone-a",
        }
        first, _, _ = self.request(
            "POST", f"/v1/runs/{run['run_id']}/evidence", body, token=CHECKOUT_TOKEN
        )
        second, _, replay = self.request(
            "POST", f"/v1/runs/{run['run_id']}/evidence", body, token=CHECKOUT_TOKEN
        )
        self.assertEqual(first, 201)
        self.assertEqual(second, 200)
        self.assertTrue(replay["replayed"])

    def test_strict_json_rejects_duplicates_nonfinite_and_unknown_fields(self) -> None:
        cases = [
            b'{"release":"a","release":"b"}',
            b'{"release":NaN}',
            b'{"release":"r","idempotency_key":"i","policy_id":"standard","expected_trials":["a"],"extra":1}',
        ]
        for raw in cases:
            with self.subTest(raw=raw):
                status, _, body = self.request(
                    "POST", "/v1/runs", token=CHECKOUT_TOKEN, raw=raw
                )
                self.assertEqual(status, 400)
                self.assertEqual(body["error"]["code"], "validation_error")

    def test_wrong_content_type_is_rejected(self) -> None:
        status, _, body = self.request(
            "POST", "/v1/runs", token=CHECKOUT_TOKEN, raw=b"{}", content_type="text/plain"
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_query_strings_and_unsupported_methods_are_rejected(self) -> None:
        query, _, query_body = self.request("GET", "/healthz?verbose=1")
        method, _, method_body = self.request("PUT", "/healthz")
        self.assertEqual(query, 400)
        self.assertEqual(query_body["error"]["code"], "validation_error")
        self.assertEqual(method, 405)
        self.assertEqual(method_body["error"]["code"], "method_not_allowed")

    def test_server_configuration_validates_tokens_and_port(self) -> None:
        with self.assertRaises(ValidationError):
            create_server(self.store, port=0, service_tokens={"service": "short"})
        with self.assertRaises(ValidationError):
            create_server(
                self.store,
                port=0,
                service_tokens={"one": "x" * 30, "two": "x" * 30},
            )
        with self.assertRaises(ValidationError):
            create_server(self.store, port=70_000, service_tokens={"one": "x" * 30})


class ScenarioTests(unittest.TestCase):
    def test_demo_exercises_all_four_verdicts_and_integrity_controls(self) -> None:
        report = run_demo()
        self.assertEqual(report["scenarios"]["safe"]["verdict"], "PASS")
        self.assertEqual(report["scenarios"]["failed"]["verdict"], "FAIL")
        self.assertEqual(report["scenarios"]["manual"]["verdict"], "MANUAL")
        self.assertEqual(
            report["scenarios"]["inconclusive"]["verdict"], "INCONCLUSIVE"
        )
        self.assertTrue(report["evidence_idempotency"]["changed_batch_rejected"])
        self.assertTrue(report["integrity"]["tampered_receipt_rejected"])
        self.assertEqual(report["integrity"]["verification"]["evidence_sets"], 31)

    def test_benchmark_processes_and_verifies_every_analysis(self) -> None:
        result = run_benchmark(5)
        self.assertEqual(result["analyses"], 5)
        self.assertEqual(result["passed"], 5)
        self.assertEqual(result["verification"]["runs"], 5)
        self.assertGreater(result["end_to_end_analyses_per_second"], 0)

    def test_benchmark_validates_count(self) -> None:
        for value in (0, 501, True, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                run_benchmark(value)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
