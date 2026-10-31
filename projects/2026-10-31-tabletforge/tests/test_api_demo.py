from __future__ import annotations

import http.client
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tabletforge.api import build_handler, serve
from tabletforge.demo import run_benchmark, run_demo
from tabletforge.errors import ValidationError
from tabletforge.store import TabletForge


SECRET = b"api-unit-test-secret-that-is-at-least-32-bytes"
TOKEN = "api-test-bearer-token-at-least-24-characters"


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = TabletForge(Path(self.temporary.name) / "tablet", secret=SECRET)
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            build_handler(self.store, TOKEN),
        )
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
        body: Any | None = None,
        *,
        authenticated: bool = True,
        raw: bool = False,
        content_type: str = "application/json",
    ) -> tuple[int, dict[str, str], dict[str, Any]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers = {"Content-Type": content_type}
        if authenticated:
            headers["Authorization"] = f"Bearer {TOKEN}"
        if body is None:
            payload = None
            headers.pop("Content-Type")
        elif raw:
            payload = body
        else:
            payload = json.dumps(body)
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = json.loads(response.read().decode("utf-8"))
        response_headers = {name.lower(): value for name, value in response.getheaders()}
        status = response.status
        connection.close()
        return status, response_headers, data

    def test_health_requires_no_credentials_and_sets_security_headers(self) -> None:
        status, headers, body = self.request(
            "GET", "/healthz", authenticated=False
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok"})
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")
        self.assertEqual(headers["x-frame-options"], "DENY")
        self.assertIn("default-src 'none'", headers["content-security-policy"])

    def test_protected_route_rejects_missing_and_wrong_token(self) -> None:
        status, _, body = self.request("GET", "/v1/stats", authenticated=False)
        self.assertEqual(status, 401)
        self.assertEqual(body["error"]["code"], "unauthorized")

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(
            "GET", "/v1/stats", headers={"Authorization": "Bearer definitely-wrong"}
        )
        response = connection.getresponse()
        self.assertEqual(response.status, 401)
        response.read()
        connection.close()

    def test_batch_create_replay_get_and_conflict(self) -> None:
        batch = {
            "idempotency_key": "api-write",
            "mutations": [{"operation": "PUT", "key": "a", "value": "one"}],
        }
        status, _, first = self.request("POST", "/v1/batches", batch)
        self.assertEqual(status, 201)
        self.assertFalse(first["replayed"])

        status, _, replay = self.request("POST", "/v1/batches", batch)
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["start_sequence"], first["start_sequence"])

        status, _, fetched = self.request("POST", "/v1/get", {"key": "a"})
        self.assertEqual(status, 200)
        self.assertEqual(fetched["value"], "one")

        changed = {
            "idempotency_key": "api-write",
            "mutations": [{"operation": "PUT", "key": "a", "value": "two"}],
        }
        status, _, body = self.request("POST", "/v1/batches", changed)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")

    def test_missing_key_and_unknown_route_return_not_found(self) -> None:
        status, _, body = self.request("POST", "/v1/get", {"key": "missing"})
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")
        status, _, body = self.request("GET", "/v1/unknown")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")

    def test_scan_snapshot_release_flush_compact_stats_and_verify(self) -> None:
        self.request(
            "POST",
            "/v1/batches",
            {
                "idempotency_key": "initial",
                "mutations": [
                    {"operation": "PUT", "key": "a", "value": "old"},
                    {"operation": "PUT", "key": "b", "value": "present"},
                ],
            },
        )
        status, _, snapshot = self.request(
            "POST", "/v1/snapshots", {"snapshot_id": "before"}
        )
        self.assertEqual(status, 201)
        self.assertEqual(snapshot["sequence"], 2)
        self.request(
            "POST",
            "/v1/batches",
            {
                "idempotency_key": "update",
                "mutations": [
                    {"operation": "PUT", "key": "a", "value": "new"},
                    {"operation": "DELETE", "key": "b"},
                ],
            },
        )

        status, _, old = self.request(
            "POST", "/v1/get", {"key": "a", "snapshot_id": "before"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(old["value"], "old")
        status, _, scan = self.request("POST", "/v1/scan", {"limit": 10})
        self.assertEqual(status, 200)
        self.assertEqual([item["key"] for item in scan["items"]], ["a"])
        self.assertEqual(scan["items"][0]["value"], "new")

        status, _, flushed = self.request("POST", "/v1/flush", {})
        self.assertEqual(status, 200)
        self.assertTrue(flushed["flushed"])
        status, _, compacted = self.request("POST", "/v1/compact", {})
        self.assertEqual(status, 200)
        self.assertFalse(compacted["compacted"])
        status, _, stats = self.request("GET", "/v1/stats")
        self.assertEqual(status, 200)
        self.assertEqual(stats["active_snapshots"], 1)
        status, _, integrity = self.request("GET", "/v1/verify")
        self.assertEqual(status, 200)
        self.assertTrue(integrity["valid"])
        status, _, released = self.request(
            "POST", "/v1/snapshots/release", {"snapshot_id": "before"}
        )
        self.assertEqual(status, 200)
        self.assertTrue(released["released"])

    def test_duplicate_json_field_is_rejected(self) -> None:
        status, _, body = self.request(
            "POST",
            "/v1/get",
            '{"key":"a","key":"b"}',
            raw=True,
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_invalid_media_type_is_rejected(self) -> None:
        status, _, body = self.request(
            "POST",
            "/v1/get",
            '{"key":"a"}',
            raw=True,
            content_type="text/plain",
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_query_strings_are_rejected(self) -> None:
        status, _, body = self.request("GET", "/v1/stats?debug=1")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_unknown_request_fields_are_rejected(self) -> None:
        status, _, body = self.request(
            "POST", "/v1/get", {"key": "a", "unexpected": True}
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "validation_error")

    def test_build_handler_validates_token(self) -> None:
        with self.assertRaises(ValidationError):
            build_handler(self.store, "too-short")

    def test_serve_restricts_host_and_port(self) -> None:
        with self.assertRaises(ValidationError):
            serve(self.store, TOKEN, host="0.0.0.0")
        with self.assertRaises(ValidationError):
            serve(self.store, TOKEN, port=0)


class DemoAndCliTests(unittest.TestCase):
    def test_demo_exercises_recovery_snapshot_compaction_and_tamper_checks(self) -> None:
        result = run_demo()
        recovery = result["recovery"]
        self.assertTrue(recovery["crash_after_wal_fsync_triggered"])
        self.assertTrue(recovery["durable_order_recovered"])
        self.assertTrue(recovery["retry_returned_same_sequence"])
        self.assertTrue(recovery["crash_after_segment_rename_triggered"])
        self.assertTrue(recovery["orphan_segment_quarantined"])
        self.assertTrue(recovery["partial_wal_tail_recovered"])
        self.assertEqual(recovery["partial_wal_bytes_truncated"], 2)
        self.assertEqual(result["snapshot_isolation"]["snapshot_alice"], "balance=100")
        self.assertEqual(result["snapshot_isolation"]["current_alice"], "balance=60")
        self.assertTrue(result["snapshot_isolation"]["current_bob_missing"])
        self.assertGreater(
            result["compaction"]["after_snapshot_release"]["reclaimed_records"], 0
        )
        self.assertTrue(result["compaction"]["deleted_carol_missing"])
        self.assertGreater(result["bloom_negative_skips"], 0)
        self.assertTrue(result["tampered_manifest_rejected"])
        self.assertTrue(result["integrity"]["valid"])

    def test_small_benchmark_reports_measured_invariants(self) -> None:
        result = run_benchmark(entries=500, reads=100)
        self.assertEqual(result["entries"], 500)
        self.assertGreater(result["write_throughput_entries_per_second"], 0)
        self.assertGreaterEqual(
            result["point_read_latency_ms"]["p95"],
            result["point_read_latency_ms"]["p50"],
        )
        self.assertGreater(result["reclaimed_records"], 0)
        self.assertGreater(result["bloom_negative_skips"], 0)
        self.assertTrue(result["integrity"]["valid"])

    def test_benchmark_validates_workload_size(self) -> None:
        with self.assertRaises(ValidationError):
            run_benchmark(entries=99, reads=1)

    def test_module_cli_emits_json(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "tabletforge", "benchmark", "--entries", "100", "--reads", "10"],
            check=True,
            capture_output=True,
            text=True,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(result["entries"], 100)
        self.assertTrue(result["integrity"]["valid"])


if __name__ == "__main__":
    unittest.main()
