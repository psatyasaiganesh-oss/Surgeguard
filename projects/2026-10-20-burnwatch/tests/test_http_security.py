import hashlib
import http.client
import json
import os
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

from burnwatch.delivery import Inbox
from burnwatch.engine import Engine
from burnwatch.http import Server
from .helpers import EPOCH, Fixture, values


class HttpTests(Fixture):
    def setUp(self):
        super().setUp()
        self.token = self.engine.mint("checkout", "api-a")
        self.server = Server(self.engine)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(5)
        self.server.server_close()

    def request(self, route, body=None, token=None, method="POST", media="application/json"):
        conn = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        try:
            conn.request(method, route, json.dumps(body) if body is not None else None,
                         {"Content-Type": media, "Authorization": "Bearer " + (token or self.token)})
            response = conn.getresponse()
            raw = response.read().decode()
            return response.status, raw if route == "/metrics" else json.loads(raw), dict(response.getheaders())
        finally:
            conn.close()

    def raw(self, extra, body=b"{}", route="/v1/evaluate", authorization=None):
        auth = authorization if authorization is not None else "Authorization: Bearer " + self.token + "\r\n"
        request = (f"POST {route} HTTP/1.1\r\nHost: localhost\r\n" + auth).encode() + extra + b"\r\n" + body
        with socket.create_connection(("127.0.0.1", self.server.server_port), timeout=5) as conn:
            conn.sendall(request)
            conn.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
        return b"".join(chunks).split(b"\r\n")[0]

    def test_collector_evaluator_and_metrics_end_to_end(self):
        for producer in self.producers:
            token = self.token if producer == "api-a" else self.engine.mint("checkout", producer)
            status, result, _ = self.request("/v1/buckets", {"batch_id": "api", "buckets": values(self.start, 60, 1000, 200),
                                                           "watermark": EPOCH}, token)
            self.assertEqual(200, status)
            self.assertEqual(60, result["accepted"])
        status, result, headers = self.request("/v1/evaluate", {})
        self.assertEqual(200, status)
        self.assertEqual("PAGE", result["status"])
        self.assertEqual("no-store", headers["Cache-Control"])
        status, metrics, _ = self.request("/metrics", method="GET")
        self.assertEqual(200, status)
        self.assertIn('burnwatch_alert_severity{service="checkout"} 2', metrics)

    def test_exact_api_retry_is_idempotent(self):
        body = {"batch_id": "api", "buckets": values(self.start, 1), "watermark": self.start + 60}
        self.assertEqual(200, self.request("/v1/buckets", body)[0])
        self.assertTrue(self.request("/v1/buckets", body)[1]["replayed"])

    def test_example_client_posts_without_printing_key(self):
        env = {**os.environ, "BURNWATCH_API_KEY": self.token, "HTTP_PROXY": "http://127.0.0.1:9",
               "http_proxy": "http://127.0.0.1:9", "NO_PROXY": "", "no_proxy": ""}
        result = subprocess.run([sys.executable, "examples/post_minute.py", "--minute", str(self.start),
                                 "--port", str(self.server.server_port)], env=env, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(1, json.loads(result.stdout)["accepted"])
        self.assertNotIn(self.token, result.stdout + result.stderr)

    def test_cannot_override_identity_service_or_clock(self):
        for field, value in (("service", "other"), ("producer", "api-b"), ("now", EPOCH)):
            status, _, _ = self.request("/v1/buckets", {"batch_id": "evil", "buckets": [], field: value})
            self.assertEqual(400, status)
        self.assertEqual(400, self.request("/v1/evaluate", {"now": EPOCH + 1})[0])

    def test_cross_service_key_reads_only_its_service(self):
        self.engine.register("other", ["api-a"], self.policy, self.start)
        token = self.engine.mint("other", "api-a")
        status, result, _ = self.request("/v1/status", token=token, method="GET")
        self.assertEqual(200, status)
        self.assertEqual("other", result["service"])
        _, metrics, _ = self.request("/metrics", token=token, method="GET")
        self.assertNotIn("checkout", metrics)

    def test_authentication_required_and_expired_key_rejected(self):
        framing = b"Content-Length: 2\r\nContent-Type: application/json\r\n"
        self.assertIn(b" 401 ", self.raw(framing, authorization=""))
        self.clock.now += 3600
        self.assertEqual(401, self.request("/v1/status", method="GET")[0])

    def test_duplicate_authorization_rejected(self):
        auth = ("Authorization: Bearer " + self.token + "\r\n") * 2
        self.assertIn(b" 401 ", self.raw(b"Content-Length: 2\r\nContent-Type: application/json\r\n", authorization=auth))

    def test_unknown_query_and_absolute_routes_rejected(self):
        for route in ("/v1/status?service=other", "http://localhost/v1/status", "/v1/mint", "/v1/status/../buckets"):
            self.assertEqual(404, self.request(route, {}, method="POST")[0])

    def test_ambiguous_and_unbounded_framing_rejected(self):
        for headers in (b"Content-Length: 2\r\nContent-Length: 2\r\n", b"Content-Length: -1\r\n",
                        b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n",
                        b"Content-Length: " + b"9" * 1000 + b"\r\n"):
            self.assertIn(b" 400 ", self.raw(headers))
        self.assertIn(b" 413 ", self.raw(b"Content-Length: 65537\r\n"))

    def test_duplicate_json_invalid_type_and_truncated_body_rejected(self):
        payload = b'{"batch_id":"x","batch_id":"y","buckets":[]}'
        headers = f"Content-Length: {len(payload)}\r\nContent-Type: application/json\r\n".encode()
        self.assertIn(b" 400 ", self.raw(headers, payload, route="/v1/buckets"))
        self.assertIn(b" 400 ", self.raw(b"Content-Length: 20\r\nContent-Type: application/json\r\n", b"{"))
        self.assertEqual(400, self.request("/v1/evaluate", [])[0])

    def test_wrong_media_type_rejected(self):
        self.assertEqual(415, self.request("/v1/evaluate", {}, media="text/plain")[0])

    def test_gap_is_reported_without_partial_ingestion(self):
        status, result, _ = self.request("/v1/buckets", {"batch_id": "gap", "buckets": values(self.start, 1),
                                                       "watermark": self.start + 120})
        self.assertEqual(409, status)
        self.assertEqual("gap", result["error"])
        self.assertEqual(0, self.engine.verify("checkout")["minutes"])

    def test_public_binding_refused_and_health_minimal(self):
        self.problem("invalid_input", Server, self.engine, "0.0.0.0")
        status, result, _ = self.request("/healthz", method="GET")
        self.assertEqual(200, status)
        self.assertEqual({"status": "ok", "service": "burnwatch"}, result)

    def test_worker_admission_is_bounded(self):
        for _ in range(16):
            self.assertTrue(self.server.slots.acquire(blocking=False))
        try:
            self.assertIn(b" 503 ", self.raw(b"Content-Length: 2\r\n"))
        finally:
            for _ in range(16):
                self.server.slots.release()


class SecurityTests(Fixture):
    def test_keys_are_hashed_at_rest_and_bound_to_source(self):
        token = self.engine.mint("checkout", "api-b")
        self.assertEqual({"service": "checkout", "producer": "api-b"}, self.engine.authenticate(token))
        with self.engine.tx() as conn:
            digest = conn.execute("SELECT digest FROM api_keys").fetchone()[0]
        self.assertEqual(hashlib.sha256(token.encode()).hexdigest(), digest)
        self.assertNotEqual(token, digest)

    def test_revoke_and_exact_expiry_boundary(self):
        token = self.engine.mint("checkout", "api-a", ttl=10)
        self.clock.now += 10
        self.problem("unauthorized", self.engine.authenticate, token)
        token = self.engine.mint("checkout", "api-a")
        self.engine.revoke(token)
        self.problem("unauthorized", self.engine.authenticate, token)

    def test_invalid_credentials_and_ttl(self):
        for token in (None, 1, "", "bw_" + "0" * 64, "x" * 10000):
            self.problem("unauthorized", self.engine.authenticate, token)
        for ttl in (0, True, 86401):
            self.problem("invalid_input", self.engine.mint, "checkout", "api-a", ttl)

    def test_private_database_and_inbox_files(self):
        self.assertEqual(0o600, stat.S_IMODE(os.stat(self.path).st_mode))
        inbox = Inbox(Path(self.temp.name) / "inbox.db")
        self.assertEqual(0o600, stat.S_IMODE(os.stat(inbox.path).st_mode))

    def test_symlinks_are_rejected_for_engine_and_inbox(self):
        link = Path(self.temp.name) / "symlink.db"
        link.symlink_to(self.path)
        self.problem("invalid_input", Engine, link)
        self.problem("invalid_input", Inbox, link)

    def test_unrelated_database_is_preserved(self):
        path = Path(self.temp.name) / "foreign.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE important(value TEXT)")
            conn.execute("INSERT INTO important VALUES('keep')")
        path.chmod(0o600)
        self.problem("invalid_input", Engine, path)
        self.problem("invalid_input", Inbox, path)
        with sqlite3.connect(path) as conn:
            self.assertEqual("keep", conn.execute("SELECT value FROM important").fetchone()[0])

    def test_world_readable_database_refused(self):
        self.path.chmod(0o644)
        self.problem("invalid_input", Engine, self.path)
        self.path.chmod(0o600)

    def test_service_identifiers_cannot_inject_metrics_or_sql(self):
        self.problem("invalid_input", self.engine.metrics, 'x"} 1\nmalicious')
        self.problem("invalid_input", self.engine.ingest, "checkout';DROP_TABLE", "api-a", "x", [])


class CliTests(Fixture):
    def cli(self, *args):
        return subprocess.run([sys.executable, "-m", "burnwatch", *args], text=True,
                              capture_output=True, timeout=30)

    def test_cli_init_ingest_tick_and_mint(self):
        path = str(Path(self.temp.name) / "cli.db")
        start = int(time.time()) // 60 * 60 - 3600
        result = self.cli("init", "--db", path, "--service", "checkout", "--file", "examples/config.json", "--start-minute", str(start))
        self.assertEqual(0, result.returncode, result.stderr)
        batch_path = Path(self.temp.name) / "batch.json"
        batch_path.write_text(json.dumps({"batch_id": "first", "buckets": values(start, 1), "watermark": start + 60}), encoding="utf-8")
        result = self.cli("ingest", "--db", path, "--service", "checkout", "--producer", "api-a", "--file", str(batch_path))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(1, json.loads(result.stdout)["accepted"])
        result = self.cli("tick", "--db", path, "--service", "checkout")
        self.assertEqual("STALE", json.loads(result.stdout)["status"])
        result = self.cli("mint", "--db", path, "--service", "checkout", "--producer", "api-a")
        self.assertTrue(json.loads(result.stdout)["api_key"].startswith("bw_"))

    def test_cli_errors_are_structured(self):
        result = self.cli("status", "--db", str(self.path), "--service", "missing")
        self.assertEqual(1, result.returncode)
        self.assertEqual("not_found", json.loads(result.stderr)["error"])
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_demo_runs_without_credentials_or_token_output(self):
        result = self.cli("demo")
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(0, report["delivery"]["outbox_pending"])
        self.assertEqual(10000, report["benchmark"]["ingested_minute_reports"])
        self.assertNotIn('"api_key"', result.stdout)
