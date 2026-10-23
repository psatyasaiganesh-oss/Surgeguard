from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from cachefence.api import build_server
from cachefence.cache import CacheStore
from cachefence.model import ValidationError
from cachefence.origin import OriginStore

TOKEN = "test-api-token-with-more-than-thirty-two-bytes"
SECRET = b"test-signing-key-with-more-than-32-bytes"


class APITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.origin = OriginStore(root / "origin.db")
        self.cache = CacheStore(root / "cache.db")
        self.server = build_server(
            "127.0.0.1",
            0,
            origin=self.origin,
            cache=self.cache,
            tenant="acme",
            api_token=TOKEN,
            signing_secret=SECRET,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: str | None = None,
        *,
        authorized: bool = True,
        content_type: str = "application/json",
    ) -> tuple[int, dict, dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        headers = {"Content-Type": content_type}
        if authorized:
            headers["Authorization"] = f"Bearer {TOKEN}"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        data = json.loads(response.read())
        response_headers = {name.lower(): value for name, value in response.getheaders()}
        connection.close()
        return response.status, data, response_headers

    def test_health_is_public_and_minimal(self) -> None:
        status, data, headers = self.request("GET", "/healthz", authorized=False)
        self.assertEqual(status, 200)
        self.assertEqual(data, {"cache": "cache-a", "status": "ok"})
        self.assertEqual(headers["cache-control"], "no-store")

    def test_item_endpoint_requires_bearer_token(self) -> None:
        status, data, _ = self.request("GET", "/v1/items/x", authorized=False)
        self.assertEqual(status, 401)
        self.assertEqual(data["error"], "unauthorized")

    def test_put_then_get(self) -> None:
        status, data, headers = self.request(
            "PUT", "/v1/items/x", '{"value":{"n":1},"expected_version":0}'
        )
        self.assertEqual(status, 201)
        self.assertEqual(data["item"]["version"], 1)
        self.assertIn("etag", headers)
        status, data, _ = self.request("GET", "/v1/items/x")
        self.assertEqual(status, 200)
        self.assertEqual(data["item"]["value"], {"n": 1})
        self.assertEqual(data["source"], "origin")

    def test_compare_and_set_conflict(self) -> None:
        self.request("PUT", "/v1/items/x", '{"value":1,"expected_version":0}')
        status, data, _ = self.request(
            "PUT", "/v1/items/x", '{"value":2,"expected_version":0}'
        )
        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "version_conflict")

    def test_delete_creates_observable_tombstone(self) -> None:
        self.request("PUT", "/v1/items/x", '{"value":1,"expected_version":0}')
        status, data, _ = self.request(
            "DELETE", "/v1/items/x", '{"expected_version":1}'
        )
        self.assertEqual(status, 200)
        self.assertTrue(data["item"]["deleted"])
        status, data, _ = self.request("GET", "/v1/items/x")
        self.assertEqual(status, 404)
        self.assertEqual(data["version"], 2)

    def test_missing_item_is_404(self) -> None:
        status, data, _ = self.request("GET", "/v1/items/missing")
        self.assertEqual(status, 404)
        self.assertEqual(data["error"], "not_found")

    def test_unknown_json_field_is_rejected(self) -> None:
        status, data, _ = self.request("PUT", "/v1/items/x", '{"value":1,"admin":true}')
        self.assertEqual(status, 400)
        self.assertEqual(data["error"], "invalid_request")

    def test_duplicate_json_key_is_rejected(self) -> None:
        status, _, _ = self.request("PUT", "/v1/items/x", '{"value":1,"value":2}')
        self.assertEqual(status, 400)

    def test_wrong_content_type_is_rejected(self) -> None:
        status, _, _ = self.request(
            "PUT", "/v1/items/x", '{"value":1}', content_type="text/plain"
        )
        self.assertEqual(status, 400)

    def test_encoded_or_nested_key_is_not_routed(self) -> None:
        for path in ("/v1/items/a%2Fb", "/v1/items/a/b", "/v1/items/%78"):
            with self.subTest(path=path):
                status, _, _ = self.request("GET", path)
                self.assertEqual(status, 404)

    def test_api_never_accepts_tenant_from_body(self) -> None:
        status, _, _ = self.request("PUT", "/v1/items/x", '{"value":1,"tenant":"other"}')
        self.assertEqual(status, 400)
        self.assertIsNone(self.origin.peek_snapshot("other", "x"))

    def test_server_rejects_short_credentials_and_remote_bind(self) -> None:
        with self.assertRaises(ValidationError):
            build_server(
                "127.0.0.1",
                0,
                origin=self.origin,
                cache=self.cache,
                tenant="acme",
                api_token="short",
                signing_secret=SECRET,
            )
        with self.assertRaises(ValidationError):
            build_server(
                "0.0.0.0",
                0,
                origin=self.origin,
                cache=self.cache,
                tenant="acme",
                api_token=TOKEN,
                signing_secret=SECRET,
            )


if __name__ == "__main__":
    unittest.main()
