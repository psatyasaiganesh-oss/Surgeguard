import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from routeproof.actuator import Actuator
from routeproof.control import Controller
from routeproof.http import create_server
from routeproof.samples import safe_plan, sample_topology


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        actuator = Actuator(root / "actuator.db")
        actuator.initialize(sample_topology())
        self.controller = Controller(root / "control.db", actuator)
        self.tokens = {
            "alice": "a" * 40,
            "bob": "b" * 40,
            "carol": "c" * 40,
            "dave": "d" * 40,
        }
        for name, role in (
            ("alice", "requester"),
            ("bob", "reviewer"),
            ("carol", "reviewer"),
            ("dave", "executor"),
        ):
            self.controller.register_operator(name, role, self.tokens[name])
        self.server = create_server(self.controller, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temp.cleanup()

    def request(self, method, path, body=None, token=None, content_type="application/json"):
        data = None if body is None else (
            body if isinstance(body, bytes) else json.dumps(body).encode()
        )
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if data is not None and content_type is not None:
            headers["Content-Type"] = content_type
        request = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with self.opener.open(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_health_is_public_and_minimal(self):
        status, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(set(body), {"ok", "topology_version"})

    def test_plan_status_requires_auth(self):
        status, body = self.request("GET", "/v1/plans/missing")
        self.assertEqual(status, 401)
        self.assertEqual(body["error"], "unauthorized")

    def test_invalid_token_is_unauthorized(self):
        status, _ = self.request("GET", "/v1/audit", token="wrong")
        self.assertEqual(status, 401)

    def test_unknown_route_is_not_found(self):
        status, _ = self.request("GET", "/v1/nope", token=self.tokens["dave"])
        self.assertEqual(status, 404)

    def test_query_strings_are_rejected(self):
        status, body = self.request("GET", "/v1/audit?limit=1", token=self.tokens["dave"])
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "invalid_request")

    def test_wrong_content_type_is_rejected(self):
        status, _ = self.request(
            "POST", "/v1/plans", safe_plan(), self.tokens["alice"], "text/plain"
        )
        self.assertEqual(status, 400)

    def test_duplicate_json_keys_are_rejected(self):
        raw = b'{"plan_id":"one","plan_id":"two"}'
        status, body = self.request(
            "POST", "/v1/plans", raw, self.tokens["alice"]
        )
        self.assertEqual(status, 400)
        self.assertIn("duplicate", body["message"])

    def test_closed_plan_schema_is_enforced(self):
        plan = safe_plan()
        plan["service"] = "override"
        status, _ = self.request("POST", "/v1/plans", plan, self.tokens["alice"])
        self.assertEqual(status, 400)

    def test_create_analyze_and_read(self):
        status, created = self.request(
            "POST", "/v1/plans", safe_plan(), self.tokens["alice"]
        )
        self.assertEqual(status, 201)
        self.assertEqual(created["state"], "DRAFT")
        status, analyzed = self.request(
            "POST", "/v1/plans/safe-maintenance/analyze", {}, self.tokens["alice"]
        )
        self.assertEqual(status, 200)
        self.assertEqual(analyzed["state"], "ANALYZED")
        status, read = self.request(
            "GET", "/v1/plans/safe-maintenance", token=self.tokens["dave"]
        )
        self.assertEqual(status, 200)
        self.assertEqual(read["analysis_hash"], analyzed["analysis_hash"])

    def test_role_failure_returns_401(self):
        status, _ = self.request(
            "POST", "/v1/plans", safe_plan(), self.tokens["bob"]
        )
        self.assertEqual(status, 401)

    def test_conflict_returns_409(self):
        self.request("POST", "/v1/plans", safe_plan(), self.tokens["alice"])
        status, _ = self.request(
            "POST", "/v1/plans/safe-maintenance/execute", {}, self.tokens["dave"]
        )
        self.assertEqual(status, 409)

    def test_observe_body_is_closed(self):
        status, _ = self.request(
            "POST",
            "/v1/plans/missing/observe",
            {"healthy": True, "evidence": "ok evidence", "extra": True},
            self.tokens["dave"],
        )
        self.assertEqual(status, 400)

    def test_put_is_not_allowed(self):
        status, body = self.request("PUT", "/v1/plans")
        self.assertEqual(status, 405)
        self.assertEqual(body["error"], "method_not_allowed")

    def test_audit_response_excludes_secrets(self):
        status, body = self.request("GET", "/v1/audit", token=self.tokens["dave"])
        self.assertEqual(status, 200)
        encoded = json.dumps(body)
        for token in self.tokens.values():
            self.assertNotIn(token, encoded)


if __name__ == "__main__":
    unittest.main()
