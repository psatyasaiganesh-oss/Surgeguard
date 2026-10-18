import http.client
import json
import socket
import subprocess
import sys
import threading
from pathlib import Path

from scopegraph.http import Server
from .helpers import Fixture, create, delete


class HttpTests(Fixture):
    def setUp(self):
        super().setUp()
        self.publish()
        self.user_token = self.engine.mint("acme", "user:alice")
        self.owner_token = self.engine.mint("acme", "user:owner")
        self.admin_token = self.engine.mint("acme", "user:operator", "admin")
        self.server = Server(self.engine)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(5)
        self.server.server_close()

    def request(self, route, body=None, token=None, method="POST", **overrides):
        conn = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + (token or self.user_token)}
        headers.update(overrides)
        try:
            conn.request(method, route, json.dumps(body) if body is not None else None, headers)
            response = conn.getresponse()
            return response.status, json.loads(response.read()), dict(response.getheaders())
        finally:
            conn.close()

    def raw(self, data):
        with socket.create_connection(("127.0.0.1", self.server.server_port), timeout=5) as conn:
            conn.sendall(data)
            conn.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                part = conn.recv(4096)
                if not part:
                    break
                chunks.append(part)
        return b"".join(chunks)

    def test_health(self):
        status, result, _ = self.request("/healthz", method="GET")
        self.assertEqual(200, status)
        self.assertEqual({"service": "scopegraph", "status": "ok"}, result)

    def test_check_and_read_bind_signed_identity(self):
        status, result, headers = self.request("/v1/check", {"resource": "document:plan", "permission": "edit"})
        self.assertEqual(200, status)
        self.assertTrue(result["allowed"])
        self.assertEqual("no-store", headers["Cache-Control"])
        status, result, _ = self.request("/v1/read", {"resource": "document:plan"})
        self.assertEqual(200, status)
        self.assertEqual("version one", result["body"])

    def test_subject_override_rejected(self):
        status, _, _ = self.request("/v1/check", {"resource": "document:plan", "permission": "share",
                                                  "subject": "user:owner"})
        self.assertEqual(400, status)

    def test_tenant_override_rejected(self):
        status, _, _ = self.request("/v1/read", {"resource": "document:plan", "tenant": "beta"})
        self.assertEqual(400, status)

    def test_server_time_override_rejected(self):
        status, _, _ = self.request("/v1/check", {"resource": "document:plan", "permission": "view", "now": 1})
        self.assertEqual(400, status)

    def test_missing_and_expired_authentication(self):
        request = b"POST /v1/check HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n{}"
        self.assertIn(b" 401 ", self.raw(request).split(b"\r\n")[0])
        self.clock.now += 901
        status, _, _ = self.request("/v1/read", {"resource": "document:plan"})
        self.assertEqual(401, status)

    def test_non_admin_cannot_change_relationships(self):
        status, _, _ = self.request("/v1/relationships", {"operations": [create("document:evil")], "request_id": "evil"})
        self.assertEqual(403, status)
        self.assertEqual("not_found", self.check(resource="document:evil")["reason"])

    def test_admin_revoke_then_publish_then_old_token_read_denied(self):
        old_token = self.seed["revision_token"]
        status, revoked, _ = self.request("/v1/relationships", {
            "operations": [delete("group:backend", "member", "user:alice")],
            "request_id": "api-revoke", "expected_revision": 1}, self.admin_token)
        self.assertEqual(200, status)
        status, written, _ = self.request("/v1/publish", {"resource": "document:plan", "text": "new private",
            "request_id": "api-publish", "expected_version": 1, "min_token": revoked["revision_token"]}, self.owner_token)
        self.assertEqual(200, status)
        self.assertEqual(2, written["version"])
        status, result, _ = self.request("/v1/read", {"resource": "document:plan", "min_token": old_token})
        self.assertEqual(403, status)
        self.assertNotIn("body", result)
        self.assertNotIn("new private", json.dumps(result))

    def test_beta_token_does_not_read_acme_content(self):
        self.engine.create_tenant("beta")
        self.engine.mutate("beta", [create("document:plan")], "beta")
        token = self.engine.mint("beta", "user:owner")
        status, _, _ = self.request("/v1/read", {"resource": "document:plan"}, token)
        self.assertEqual(403, status)

    def test_revision_token_is_not_bearer_credential(self):
        status, _, _ = self.request("/v1/read", {"resource": "document:plan"}, self.seed["revision_token"])
        self.assertEqual(401, status)

    def test_query_and_absolute_route_not_accepted(self):
        for path in ("/v1/read?tenant=beta", "http://localhost/v1/read", "/v1/read/../relationships"):
            status, _, _ = self.request(path, {"resource": "document:plan"})
            self.assertEqual(404, status)

    def test_media_type_and_large_body_rejected(self):
        status, _, _ = self.request("/v1/read", {"resource": "document:plan"}, **{"Content-Type": "text/plain"})
        self.assertEqual(415, status)
        request = b"POST /v1/read HTTP/1.1\r\nHost: localhost\r\nContent-Length: 65537\r\n\r\n"
        self.assertIn(b" 413 ", self.raw(request).split(b"\r\n")[0])

    def test_duplicate_length_and_transfer_encoding_rejected(self):
        for framing in (b"Content-Length: 2\r\nContent-Length: 2\r\n",
                        b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n",
                        b"Content-Length: -1\r\n", b"Content-Length: " + b"9" * 1000 + b"\r\n"):
            request = b"POST /v1/read HTTP/1.1\r\nHost: localhost\r\n" + framing + b"\r\n{}"
            self.assertIn(b" 400 ", self.raw(request).split(b"\r\n")[0])

    def test_duplicate_authorization_rejected(self):
        token = self.user_token.encode()
        request = (b"POST /v1/read HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nContent-Type: application/json\r\n"
                   b"Authorization: Bearer " + token + b"\r\nAuthorization: Bearer " + token + b"\r\n\r\n{}")
        self.assertIn(b" 401 ", self.raw(request).split(b"\r\n")[0])

    def test_duplicate_json_and_truncated_body_rejected(self):
        for payload, length in ((b'{"resource":"document:plan","resource":"document:other"}', None),
                                (b'{"resource":', 100)):
            size = length or len(payload)
            request = (f"POST /v1/read HTTP/1.1\r\nHost: localhost\r\nContent-Length: {size}\r\n"
                       f"Content-Type: application/json\r\nAuthorization: Bearer {self.user_token}\r\n\r\n").encode() + payload
            self.assertIn(b" 400 ", self.raw(request).split(b"\r\n")[0])

    def test_worker_capacity_is_bounded(self):
        # Reserve all worker slots to deterministically exercise admission control.
        for _ in range(16):
            self.assertTrue(self.server.slots.acquire(blocking=False))
        try:
            response = self.raw(b"GET /healthz HTTP/1.0\r\n\r\n")
            self.assertIn(b" 503 ", response.split(b"\r\n")[0])
        finally:
            for _ in range(16):
                self.server.slots.release()

    def test_public_binding_is_refused(self):
        self.problem("invalid_input", Server, self.engine, "0.0.0.0")


class CliTests(Fixture):
    def cli(self, *args):
        return subprocess.run([sys.executable, "-m", "scopegraph", *args], text=True,
                              capture_output=True, timeout=30)

    def test_cli_init_mint_and_mutate(self):
        path = str(Path(self.temp.name) / "cli.db")
        result = self.cli("init", "--db", path, "--tenant", "demo")
        self.assertEqual(0, result.returncode)
        result = self.cli("mutate", "--db", path, "--tenant", "demo", "--file", "examples/bootstrap.json")
        self.assertEqual(0, result.returncode, result.stderr)
        result = self.cli("check", "--db", path, "--tenant", "demo", "--resource", "document:roadmap", "--user", "user:alice")
        self.assertTrue(json.loads(result.stdout)["allowed"])
        result = self.cli("mint", "--db", path, "--tenant", "demo", "--user", "user:alice")
        self.assertEqual(0, result.returncode)
        self.assertIn("access_token", json.loads(result.stdout))

    def test_cli_error_is_structured_without_traceback(self):
        result = self.cli("check", "--db", str(self.path), "--tenant", "missing", "--resource", "document:plan", "--user", "user:alice")
        self.assertEqual(1, result.returncode)
        self.assertEqual("not_found", json.loads(result.stderr)["error"])
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_demo_runs_real_scenarios(self):
        result = self.cli("demo")
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["concurrent_read"]["inflight_reader_never_observed_new_content"])
        self.assertEqual(1000, report["benchmark"]["cache"]["hits"])
        self.assertNotIn("access_token", result.stdout)
        self.assertNotIn("secret\":", result.stdout)
