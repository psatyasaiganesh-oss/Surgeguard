from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from slicequeue.api import build_server
from slicequeue.demo import run_benchmark, run_demo
from slicequeue.store import SliceQueue


SECRET = b"api-test-secret-with-more-than-32-bytes"
TOKEN_A = "token-a-with-strong-test-length"
TOKEN_B = "token-b-with-strong-test-length"


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = SliceQueue(Path(self.temporary.name) / "api.db", secret=SECRET)
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
        body: str | None = None,
        *,
        token: str | None = TOKEN_A,
        content_type: str = "application/json",
    ) -> tuple[int, dict, dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        headers = {"Content-Type": content_type}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, payload, response_headers

    def test_health_and_security_headers(self) -> None:
        status, body, headers = self.request("GET", "/health", token=None)
        self.assertEqual((status, body), (200, {"status": "ok"}))
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_authentication_and_token_namespace_isolation(self) -> None:
        status, _, _ = self.request("GET", "/v1/stats", token=None)
        self.assertEqual(status, 401)
        launch = json.dumps({"idempotency_key": "launch", "thread_id": "root"})
        self.assertEqual(self.request("POST", "/v1/threads", launch)[0], 201)
        status, _, _ = self.request("GET", "/v1/threads/root", token=TOKEN_B)
        self.assertEqual(status, 404)

    def test_http_end_to_end_slice(self) -> None:
        launch = json.dumps({"idempotency_key": "launch", "thread_id": "root"})
        first = self.request("POST", "/v1/threads", launch)
        replay = self.request("POST", "/v1/threads", launch)
        self.assertEqual((first[0], replay[0]), (201, 200))
        event = json.dumps({"event_id": "e1", "kind": "USER", "payload": {"action": "ping"}})
        self.assertEqual(self.request("POST", "/v1/threads/root/events", event)[0], 202)
        status, work, _ = self.request("POST", "/v1/work/slices/worker-1", "{}")
        self.assertEqual(status, 200)
        self.assertTrue(work["processed"])
        status, thread, _ = self.request("GET", "/v1/threads/root")
        self.assertEqual(status, 200)
        self.assertEqual(thread["version"], 1)
        self.assertEqual(thread["history"][1]["content"]["message"], "pong")

    def test_duplicate_fields_and_unknown_fields_are_rejected(self) -> None:
        duplicate = '{"idempotency_key":"one","idempotency_key":"two"}'
        status, body, _ = self.request("POST", "/v1/threads", duplicate)
        self.assertEqual(status, 400)
        self.assertIn("duplicate field", body["error"]["message"])
        unknown = json.dumps({"idempotency_key": "one", "is_admin": True})
        self.assertEqual(self.request("POST", "/v1/threads", unknown)[0], 400)

    def test_json_content_type_and_canonical_path_are_required(self) -> None:
        self.assertEqual(
            self.request("POST", "/v1/threads", "{}", content_type="text/plain")[0], 400
        )
        self.assertEqual(self.request("GET", "/v1//stats")[0], 400)


class DemoTests(unittest.TestCase):
    def test_demo_proves_recovery_invariants(self) -> None:
        result = run_demo()
        self.assertTrue(result["integrity"]["valid"])
        self.assertTrue(result["idempotency"]["launch_replayed"])
        self.assertEqual(result["tool_result"]["sum"], 29.0)
        self.assertEqual(
            [item["ack_lost"] for item in result["trace"][:3]], [True, True, True]
        )
        self.assertFalse(result["trace"][2]["claimed_before_due_time"])

    def test_small_benchmark_processes_every_event(self) -> None:
        result = run_benchmark(threads=5, events_per_thread=2)
        self.assertEqual(result["result"]["processed"], 10)
        self.assertTrue(result["result"]["integrity_valid"])


if __name__ == "__main__":
    unittest.main()
