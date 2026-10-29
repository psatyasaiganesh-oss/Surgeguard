from __future__ import annotations

import copy
import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from testsieve.api import build_handler
from testsieve.demo import BASE_NS, run_benchmark, run_demo, small_snapshot
from testsieve.errors import ValidationError
from testsieve.store import TestSieveStore


SECRET = b"api-test-signing-key-that-is-more-than-32-bytes"
REPOSITORY = "checkout-monorepo"
TOKEN = "repository-token-that-is-long-enough"


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = TestSieveStore(
            Path(self.temporary.name) / "api.sqlite", secret=SECRET
        )
        handler = build_handler(self.store, {REPOSITORY: TOKEN})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
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
        body: object | bytes | None = None,
        *,
        token: str | None = TOKEN,
        content_type: str = "application/json",
    ) -> tuple[int, dict, dict[str, str]]:
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=3
        )
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            headers["Content-Type"] = content_type
            encoded = body if isinstance(body, bytes) else json.dumps(body).encode()
        else:
            encoded = None
        connection.request(method, path, body=encoded, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, json.loads(raw), response_headers

    def publish(self) -> dict:
        status, body, _ = self.request(
            "POST",
            "/v1/baselines",
            {
                "idempotency_key": "baseline-1",
                "expected_head_revision": 0,
                "snapshot": small_snapshot(),
            },
        )
        self.assertEqual(status, 201)
        return body

    def test_health_does_not_require_authentication(self) -> None:
        status, body, _ = self.request("GET", "/healthz", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok"})

    def test_responses_have_security_headers(self) -> None:
        status, _, headers = self.request("GET", "/healthz", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")
        self.assertEqual(headers["x-frame-options"], "DENY")
        self.assertIn("default-src 'none'", headers["content-security-policy"])

    def test_missing_token_is_unauthorized(self) -> None:
        status, body, _ = self.request("GET", "/v1/stats", token=None)
        self.assertEqual(status, 401)
        self.assertEqual(body["error"]["code"], "unauthorized")

    def test_invalid_token_is_unauthorized(self) -> None:
        status, _, _ = self.request("GET", "/v1/stats", token="x" * 32)
        self.assertEqual(status, 401)

    def test_baseline_create_and_idempotent_replay(self) -> None:
        created = self.publish()
        self.assertEqual(created["revision"], 1)
        status, replayed, _ = self.request(
            "POST",
            "/v1/baselines",
            {
                "idempotency_key": "baseline-1",
                "expected_head_revision": 0,
                "snapshot": small_snapshot(),
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(replayed["replayed"])

    def test_latest_baseline_and_stats(self) -> None:
        self.publish()
        status, baseline, _ = self.request("GET", "/v1/baselines/latest")
        self.assertEqual(status, 200)
        self.assertEqual(baseline["revision"], 1)
        status, stats, _ = self.request("GET", "/v1/stats")
        self.assertEqual(status, 200)
        self.assertEqual(stats["baselines"], 1)

    def test_selection_and_manifest_verification(self) -> None:
        self.publish()
        status, selected, _ = self.request(
            "POST",
            "/v1/selections",
            {
                "idempotency_key": "select-1",
                "current_inventory": small_snapshot()["inventory"],
                "worker_count": 2,
            },
        )
        self.assertEqual(status, 201)
        status, verified, _ = self.request(
            "POST", "/v1/manifests/verify", {"manifest": selected["manifest"]}
        )
        self.assertEqual(status, 200)
        self.assertTrue(verified["valid"])

    def test_shadow_result_route(self) -> None:
        self.publish()
        status, _, _ = self.request("POST", "/v1/mode", {"mode": "SHADOW"})
        self.assertEqual(status, 200)
        status, selected, _ = self.request(
            "POST",
            "/v1/selections",
            {
                "idempotency_key": "select-1",
                "current_inventory": small_snapshot()["inventory"],
            },
        )
        self.assertEqual(status, 201)
        status, result, _ = self.request(
            "POST",
            f"/v1/selections/{selected['selection_id']}/shadow-results",
            {"idempotency_key": "result-1", "actual_failures": []},
        )
        self.assertEqual(status, 201)
        self.assertTrue(result["safe"])

    def test_unknown_route_is_not_found(self) -> None:
        status, body, _ = self.request("GET", "/v1/nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")

    def test_query_string_is_rejected(self) -> None:
        status, body, _ = self.request("GET", "/healthz?verbose=1", token=None)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_wrong_media_type_is_rejected(self) -> None:
        status, _, _ = self.request(
            "POST", "/v1/mode", {"mode": "RECORD"}, content_type="text/plain"
        )
        self.assertEqual(status, 400)

    def test_duplicate_json_field_is_rejected(self) -> None:
        raw = b'{"mode":"RECORD","mode":"SHADOW"}'
        status, _, _ = self.request("POST", "/v1/mode", raw)
        self.assertEqual(status, 400)

    def test_non_utf8_body_is_rejected(self) -> None:
        status, _, _ = self.request("POST", "/v1/mode", b"\xff")
        self.assertEqual(status, 400)

    def test_unknown_request_field_is_rejected(self) -> None:
        status, _, _ = self.request(
            "POST", "/v1/mode", {"mode": "RECORD", "surprise": True}
        )
        self.assertEqual(status, 400)

    def test_conflict_maps_to_http_409(self) -> None:
        self.publish()
        changed = copy.deepcopy(small_snapshot())
        changed["completed_commit"] = "changed"
        status, body, _ = self.request(
            "POST",
            "/v1/baselines",
            {
                "idempotency_key": "baseline-1",
                "expected_head_revision": 0,
                "snapshot": changed,
            },
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")

    def test_tampered_manifest_maps_to_http_422(self) -> None:
        self.publish()
        _, selected, _ = self.request(
            "POST",
            "/v1/selections",
            {
                "idempotency_key": "select-1",
                "current_inventory": small_snapshot()["inventory"],
            },
        )
        selected["manifest"]["payload"]["mode"] = "SELECT"
        status, body, _ = self.request(
            "POST", "/v1/manifests/verify", {"manifest": selected["manifest"]}
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "integrity_error")

    def test_malformed_manifest_maps_to_http_400(self) -> None:
        self.publish()
        _, selected, _ = self.request(
            "POST",
            "/v1/selections",
            {
                "idempotency_key": "select-1",
                "current_inventory": small_snapshot()["inventory"],
            },
        )
        selected["manifest"]["payload"]["baseline_revision"] = "one"
        status, body, _ = self.request(
            "POST", "/v1/manifests/verify", {"manifest": selected["manifest"]}
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_missing_baseline_is_not_found(self) -> None:
        status, _, _ = self.request("GET", "/v1/baselines/latest")
        self.assertEqual(status, 404)

    def test_build_handler_requires_tokens(self) -> None:
        with self.assertRaises(ValidationError):
            build_handler(self.store, {})

    def test_build_handler_requires_distinct_long_tokens(self) -> None:
        with self.assertRaises(ValidationError):
            build_handler(self.store, {REPOSITORY: "short"})
        with self.assertRaises(ValidationError):
            build_handler(self.store, {REPOSITORY: TOKEN, "other-repo": TOKEN})


class DemoTests(unittest.TestCase):
    def test_demo_exercises_safety_lifecycle(self) -> None:
        output = run_demo()
        self.assertTrue(output["localized_selection"]["replay_returned_same_selection"])
        self.assertTrue(output["guardrails"]["root_change_runs_full_suite"])
        self.assertTrue(output["guardrails"]["unknown_file_runs_full_suite"])
        self.assertEqual(output["shadow_miss"]["baseline_status"], "QUARANTINED")
        self.assertTrue(output["shadow_miss"]["next_run_full_suite"])
        self.assertTrue(output["security"]["tampered_manifest_rejected"])
        self.assertTrue(output["integrity"]["valid"])

    def test_benchmark_has_expected_synthetic_shape(self) -> None:
        output = run_benchmark(5)
        self.assertEqual(output["suite_tests"], 1_000)
        self.assertEqual(output["candidate_tests"], 22)
        self.assertEqual(output["selection_fraction"], 0.022)
        self.assertEqual(output["integrity"]["selections"], 7)
        self.assertGreater(output["requests_per_second"], 0)

    def test_benchmark_rejects_invalid_iteration_count(self) -> None:
        with self.assertRaises(ValidationError):
            run_benchmark(0)


if __name__ == "__main__":
    unittest.main()
