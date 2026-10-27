from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from clearflow.api import build_handler, serve
from clearflow.demo import BASE_NS, SECOND, event, policy, run_benchmark, run_demo
from clearflow.errors import ValidationError
from clearflow.store import ClearFlowStore


SECRET = b"clearflow-api-test-secret-at-least-32-bytes"
TOKEN = "shop-token-that-is-definitely-24-chars"
OTHER_TOKEN = "other-token-that-is-definitely-24-chars"


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = ClearFlowStore(Path(self.temporary.name) / "api.sqlite", secret=SECRET)
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            build_handler(self.store, {"shop": TOKEN, "other": OTHER_TOKEN}),
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
        body=None,
        *,
        token: str | None = TOKEN,
        content_type: str = "application/json",
        raw: bytes | None = None,
    ):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        headers = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None or raw is not None:
            payload = raw if raw is not None else json.dumps(body).encode()
            headers["Content-Type"] = content_type
            headers["Content-Length"] = str(len(payload))
        else:
            payload = None
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = json.loads(response.read())
        headers_out = dict(response.getheaders())
        connection.close()
        return response.status, data, headers_out

    def test_health_does_not_require_authentication(self) -> None:
        status, data, _ = self.request("GET", "/healthz", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(data, {"status": "ok"})

    def test_protected_route_requires_token(self) -> None:
        status, data, _ = self.request("GET", "/v1/stats", token=None)
        self.assertEqual(status, 401)
        self.assertEqual(data["error"]["code"], "unauthorized")

    def test_invalid_token_is_rejected(self) -> None:
        status, _, _ = self.request("GET", "/v1/stats", token="invalid")
        self.assertEqual(status, 401)

    def test_create_account_and_read_balances(self) -> None:
        status, created, headers = self.request(
            "POST",
            "/v1/accounts",
            {"account_id": "cash", "kind": "terminal", "currency": "USD"},
        )
        self.assertEqual(status, 201)
        self.assertFalse(created["replayed"])
        self.assertEqual(headers["Cache-Control"], "no-store")
        status, balances, _ = self.request("GET", "/v1/balances")
        self.assertEqual(status, 200)
        self.assertEqual(balances["balances"][0]["account_id"], "cash")

    def test_replayed_account_returns_200(self) -> None:
        body = {"account_id": "cash", "kind": "terminal", "currency": "USD"}
        self.request("POST", "/v1/accounts", body)
        status, data, _ = self.request("POST", "/v1/accounts", body)
        self.assertEqual(status, 200)
        self.assertTrue(data["replayed"])

    def test_token_selects_tenant_not_request_body(self) -> None:
        status, _, _ = self.request(
            "POST",
            "/v1/accounts",
            {"tenant": "other", "account_id": "cash", "kind": "terminal", "currency": "USD"},
        )
        self.assertEqual(status, 400)

    def test_tenants_are_isolated(self) -> None:
        self.request(
            "POST",
            "/v1/accounts",
            {"account_id": "cash", "kind": "terminal", "currency": "USD"},
        )
        status, _, _ = self.request("GET", "/v1/balances", token=OTHER_TOKEN)
        self.assertEqual(status, 404)

    def test_duplicate_json_field_is_rejected(self) -> None:
        raw = b'{"account_id":"cash","account_id":"other","kind":"terminal","currency":"USD"}'
        status, data, _ = self.request("POST", "/v1/accounts", raw=raw)
        self.assertEqual(status, 400)
        self.assertIn("duplicate", data["error"]["message"])

    def test_wrong_content_type_is_rejected(self) -> None:
        status, _, _ = self.request(
            "POST",
            "/v1/accounts",
            {"account_id": "cash", "kind": "terminal", "currency": "USD"},
            content_type="text/plain",
        )
        self.assertEqual(status, 400)

    def test_non_utf8_body_is_rejected(self) -> None:
        status, _, _ = self.request("POST", "/v1/accounts", raw=b"\xff")
        self.assertEqual(status, 400)

    def test_non_object_body_is_rejected(self) -> None:
        status, _, _ = self.request("POST", "/v1/accounts", raw=b"[]")
        self.assertEqual(status, 400)

    def test_query_string_is_rejected(self) -> None:
        status, _, _ = self.request("GET", "/v1/stats?tenant=other")
        self.assertEqual(status, 400)

    def test_unknown_route_returns_404(self) -> None:
        status, _, _ = self.request("GET", "/v1/unknown")
        self.assertEqual(status, 404)

    def test_event_ingestion_and_lookup(self) -> None:
        for account_id, kind in (("cash", "terminal"), ("pending:1", "clearing")):
            self.request(
                "POST",
                "/v1/accounts",
                {"account_id": account_id, "kind": kind, "currency": "USD"},
            )
        body = {
            "idempotency_key": "ingest:1",
            "received_ns": BASE_NS + 2 * SECOND,
            "event": event(
                "evt:1",
                "flow:create",
                [("cash", -100), ("pending:1", 100)],
                BASE_NS + SECOND,
            ),
        }
        status, created, _ = self.request("POST", "/v1/events", body)
        self.assertEqual(status, 201)
        status, found, _ = self.request("GET", f"/v1/events/{created['event_id']}")
        self.assertEqual(status, 200)
        self.assertEqual(found["event"]["reference_id"], "flow:create")

    def test_manifest_and_evaluation_routes(self) -> None:
        manifest = {
            "idempotency_key": "manifest:1",
            "batch_id": "batch:1",
            "producer": "payments",
            "expected_references": ["flow:create"],
            "deadline_ns": BASE_NS + SECOND,
        }
        status, _, _ = self.request("POST", "/v1/manifests", manifest)
        self.assertEqual(status, 201)
        evaluation = {
            "idempotency_key": "evaluation:1",
            "as_of_ns": BASE_NS + 30 * SECOND,
            "policy": policy(),
        }
        status, result, _ = self.request("POST", "/v1/quality/evaluations", evaluation)
        self.assertEqual(status, 201)
        self.assertEqual(result["report"]["verdict"], "FAIL")

    def test_verify_attestation_route_is_tenant_bound(self) -> None:
        result = self.store.evaluate_quality(
            "shop", "eval", BASE_NS, policy(), now_ns=BASE_NS
        )
        status, data, _ = self.request(
            "POST",
            "/v1/quality/attestations/verify",
            {"attestation": result["attestation"]},
        )
        self.assertEqual(status, 200)
        self.assertTrue(data["valid"])


class ConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = ClearFlowStore(Path(self.temporary.name) / "config.sqlite", secret=SECRET)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_token_must_be_long(self) -> None:
        with self.assertRaises(ValidationError):
            build_handler(self.store, {"shop": "short"})

    def test_tokens_must_be_distinct(self) -> None:
        with self.assertRaises(ValidationError):
            build_handler(self.store, {"shop": TOKEN, "other": TOKEN})

    def test_server_rejects_non_loopback_binding(self) -> None:
        with self.assertRaises(ValidationError):
            serve(self.store, {"shop": TOKEN}, host="0.0.0.0", port=8080)


class DemoTests(unittest.TestCase):
    def test_demo_exercises_all_failure_modes(self) -> None:
        result = run_demo()
        self.assertEqual(result["scenarios"]["out_of_order_healthy"]["verdict"], "PASS")
        self.assertEqual(result["scenarios"]["wrong_dimension"]["before"], "FAIL")
        self.assertEqual(result["scenarios"]["wrong_dimension"]["after_correction"], "PASS")
        self.assertTrue(result["integrity"]["valid"])

    def test_small_benchmark_is_reproducible(self) -> None:
        result = run_benchmark(5)
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["events"], 10)
        self.assertEqual(result["postings"], 25)
        self.assertTrue(result["integrity"]["valid"])

    def test_benchmark_rejects_zero_flows(self) -> None:
        with self.assertRaises(ValueError):
            run_benchmark(0)


if __name__ == "__main__":
    unittest.main()
