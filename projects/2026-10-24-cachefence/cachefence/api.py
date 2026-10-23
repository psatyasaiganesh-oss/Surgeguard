"""Small tenant-bound HTTP adapter for the CacheFence backend."""

from __future__ import annotations

import hmac
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote, urlsplit

from .cache import CacheStore
from .model import (
    ConflictError,
    IntegrityError,
    MAX_BODY_BYTES,
    ValidationError,
    require_keys,
    strict_loads,
    validate_id,
)
from .origin import OriginStore
from .pipeline import deliver_once
from .service import CacheAsideService


class CacheFenceHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        origin: OriginStore,
        cache: CacheStore,
        tenant: str,
        api_token: str,
        signing_secret: bytes,
    ) -> None:
        if not isinstance(api_token, str) or len(api_token.encode("utf-8")) < 32:
            raise ValidationError("API token must contain at least 32 UTF-8 bytes")
        if not isinstance(signing_secret, bytes) or len(signing_secret) < 32:
            raise ValidationError("signing secret must contain at least 32 bytes")
        self.origin = origin
        self.cache = cache
        self.service = CacheAsideService(origin, cache)
        self.tenant = validate_id(tenant, "tenant")
        self.api_token = api_token
        self.signing_secret = signing_secret
        super().__init__(address, CacheFenceHandler)


class CacheFenceHandler(BaseHTTPRequestHandler):
    server: CacheFenceHTTPServer
    protocol_version = "HTTP/1.1"
    server_version = "CacheFence"
    sys_version = ""

    def log_message(self, _format: str, *_args: object) -> None:
        # Avoid logging authorization headers or caller-controlled paths in this lab.
        return

    def _send(self, status: int, payload: dict[str, Any], *, etag: str | None = None) -> None:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        if etag is not None:
            self.send_header("ETag", etag)
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        value = self.headers.get("Authorization", "")
        prefix = "Bearer "
        supplied = value[len(prefix) :] if value.startswith(prefix) else ""
        return hmac.compare_digest(supplied, self.server.api_token)

    def _require_auth(self) -> bool:
        if self._authorized():
            return True
        self._send(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
        return False

    def _item_key(self) -> str | None:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment or not parsed.path.startswith("/v1/items/"):
            return None
        encoded = parsed.path.removeprefix("/v1/items/")
        try:
            key = unquote(encoded, errors="strict")
        except UnicodeDecodeError:
            return None
        if not key or "/" in key or key != encoded:
            # Percent encodings are rejected instead of normalized, keeping routing
            # and authorization decisions on exactly the same identifier.
            return None
        return validate_id(key, "key")

    def _body(self) -> dict[str, Any]:
        if self.headers.get("Transfer-Encoding"):
            raise ValidationError("chunked request bodies are not supported")
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValidationError("Content-Type must be application/json")
        raw_length = self.headers.get("Content-Length")
        if raw_length is None or not raw_length.isdigit():
            raise ValidationError("a numeric Content-Length is required")
        length = int(raw_length)
        if length > MAX_BODY_BYTES:
            raise ValidationError(f"JSON body exceeds {MAX_BODY_BYTES} bytes")
        value = strict_loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValidationError("request body must be an object")
        return value

    @staticmethod
    def _etag(snapshot: Any) -> str:
        return f'"v{snapshot.version}-{snapshot.digest}"'

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if urlsplit(self.path).path == "/healthz":
            self._send(HTTPStatus.OK, {"status": "ok", "cache": self.server.cache.cache_id})
            return
        if not self._require_auth():
            return
        try:
            key = self._item_key()
            if key is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            result = self.server.service.get(self.server.tenant, key)
            if result.status == "not_found":
                payload: dict[str, Any] = {"error": "not_found", "source": result.source}
                if result.snapshot is not None:
                    payload["version"] = result.snapshot.version
                self._send(HTTPStatus.NOT_FOUND, payload)
                return
            assert result.snapshot is not None
            self._send(
                HTTPStatus.OK,
                {"source": result.source, "item": result.snapshot.as_dict()},
                etag=self._etag(result.snapshot),
            )
        except (ValidationError, ValueError):
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
        except IntegrityError:
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "integrity_failure"})

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._mutate(delete=False)

    def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._mutate(delete=True)

    def _mutate(self, *, delete: bool) -> None:
        if not self._require_auth():
            return
        try:
            key = self._item_key()
            if key is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            body = self._body()
            if delete:
                require_keys(body, required=set(), optional={"expected_version"}, label="delete")
                value = None
            else:
                require_keys(body, required={"value"}, optional={"expected_version"}, label="write")
                value = body["value"]
            expected = body.get("expected_version")
            snapshot = self.server.origin.write(
                self.server.tenant,
                key,
                value,
                delete=delete,
                expected_version=expected,
            )
            delivery = deliver_once(
                self.server.origin,
                self.server.cache,
                secret=self.server.signing_secret,
                worker="api-worker",
            )
            if delivery.failed:
                self._send(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "error": "write_committed_delivery_pending",
                        "item": snapshot.as_dict(),
                    },
                    etag=self._etag(snapshot),
                )
                return
            self._send(
                HTTPStatus.OK if snapshot.version > 1 else HTTPStatus.CREATED,
                {"item": snapshot.as_dict(), "invalidations": delivery.as_dict()},
                etag=self._etag(snapshot),
            )
        except ConflictError as exc:
            self._send(HTTPStatus.CONFLICT, {"error": "version_conflict", "message": str(exc)})
        except (ValidationError, ValueError):
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
        except IntegrityError:
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "integrity_failure"})


def build_server(
    host: str,
    port: int,
    *,
    origin: OriginStore,
    cache: CacheStore,
    tenant: str,
    api_token: str,
    signing_secret: bytes,
) -> CacheFenceHTTPServer:
    if host not in {"127.0.0.1", "localhost"}:
        raise ValidationError("the lab HTTP server may bind only to loopback")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValidationError("port must be between 0 and 65535")
    return CacheFenceHTTPServer(
        (host, port),
        origin=origin,
        cache=cache,
        tenant=tenant,
        api_token=api_token,
        signing_secret=signing_secret,
    )
