from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

from mergemesh.api import TenantAuthenticator, create_server
from mergemesh.model import MAX_BODY_BYTES, Operation, ValidationError
from mergemesh.store import RegionStore


TOKEN = "test-token-with-at-least-thirty-two-bytes-12345"


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = RegionStore(Path(self.temporary.name) / "east.db", "east")
        self.server = create_server(self.store, {TOKEN: "acme"}, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: str | None = None,
        *,
        token: str | None = TOKEN,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, Any], dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        request_headers = dict(headers or {})
        if token is not None:
            request_headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            request_headers.setdefault("Content-Type", "application/json")
        connection.request(method, path, body=body, headers=request_headers)
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, payload, response_headers

    def test_health_is_public(self) -> None:
        status, payload, _ = self.request("GET", "/healthz", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"region": "east", "status": "ok"})

    def test_document_requires_authentication(self) -> None:
        status, payload, _ = self.request("GET", "/v1/documents/sku", token=None)
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "unauthorized")

    def test_invalid_token_rejected(self) -> None:
        status, _, _ = self.request(
            "GET", "/v1/documents/sku", token="x" * 40
        )
        self.assertEqual(status, 401)

    def test_patch_and_read_document(self) -> None:
        status, result, _ = self.request(
            "POST", "/v1/documents/sku", json.dumps({"changes": {"price": 9}})
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["document"]["values"]["price"], 9)
        status, document, _ = self.request("GET", "/v1/documents/sku")
        self.assertEqual(status, 200)
        self.assertEqual(document["values"]["price"], 9)

    def test_delete_field(self) -> None:
        self.store.patch("acme", "sku", changes={"note": "x"})
        status, result, _ = self.request(
            "POST", "/v1/documents/sku", json.dumps({"delete_fields": ["note"]})
        )
        self.assertEqual(status, 200)
        self.assertIn("note", result["document"]["deleted_fields"])

    def test_stale_etag_returns_conflict(self) -> None:
        stale = self.store.get_document("acme", "sku")["etag"]
        self.store.patch("acme", "sku", changes={"x": 1})
        status, payload, _ = self.request(
            "POST",
            "/v1/documents/sku",
            json.dumps({"changes": {"x": 2}, "expected_etag": stale}),
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"], "conflict")

    def test_unknown_request_field_rejected(self) -> None:
        status, payload, _ = self.request(
            "POST", "/v1/documents/sku", json.dumps({"changes": {"x": 1}, "admin": True})
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_request")

    def test_duplicate_json_key_rejected(self) -> None:
        status, payload, _ = self.request(
            "POST", "/v1/documents/sku", '{"changes":{"x":1},"changes":{"x":2}}'
        )
        self.assertEqual(status, 400)
        self.assertIn("duplicate", payload["message"])

    def test_invalid_document_path_rejected(self) -> None:
        status, _, _ = self.request("GET", "/v1/documents/bad%2Fid")
        self.assertEqual(status, 404)

    def test_conflicts_endpoint_is_tenant_scoped(self) -> None:
        self.store.patch("other", "sku", changes={"x": 1})
        status, payload, _ = self.request("GET", "/v1/conflicts")
        self.assertEqual(status, 200)
        self.assertEqual(payload["conflicts"], [])

    def test_resolve_conflict(self) -> None:
        east_id = self.store.patch("acme", "sku", changes={"price": 1})["op_id"]
        remote = Operation.create(
            tenant="acme",
            document="sku",
            origin="west",
            sequence=1,
            context={},
            changes={"price": 2},
            created_ns=1,
        )
        self.store.apply_remote(remote)
        status, result, _ = self.request(
            "POST",
            "/v1/documents/sku/resolve",
            json.dumps(
                {
                    "field": "price",
                    "expected_candidates": [east_id, remote.op_id],
                    "delete": False,
                    "value": 3,
                }
            ),
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["document"]["values"]["price"], 3)

    def test_security_headers_present(self) -> None:
        _, _, headers = self.request("GET", "/healthz", token=None)
        self.assertEqual(headers["cache-control"], "no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_oversize_body_rejected_before_read(self) -> None:
        status, payload, _ = self.request(
            "POST",
            "/v1/documents/sku",
            "{}",
            headers={"Content-Length": str(MAX_BODY_BYTES + 1)},
        )
        self.assertEqual(status, 413)
        self.assertEqual(payload["error"], "too_large")


class AuthenticatorTests(unittest.TestCase):
    def test_short_token_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            TenantAuthenticator({"short": "acme"})

    def test_plaintext_token_is_not_retained(self) -> None:
        authenticator = TenantAuthenticator({TOKEN: "acme"})
        self.assertNotIn(TOKEN, repr(authenticator.__dict__))
        self.assertEqual(authenticator.authenticate(f"Bearer {TOKEN}"), "acme")

    def test_wrong_scheme_rejected(self) -> None:
        authenticator = TenantAuthenticator({TOKEN: "acme"})
        self.assertIsNone(authenticator.authenticate(f"Basic {TOKEN}"))


if __name__ == "__main__":
    unittest.main()
