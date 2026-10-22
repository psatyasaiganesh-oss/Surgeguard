"""Minimal authenticated HTTP API using only the Python standard library."""

from __future__ import annotations

import hashlib
import hmac
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from .model import (
    ConflictError,
    MAX_BODY_BYTES,
    ValidationError,
    require_exact_keys,
    strict_loads,
    validate_identifier,
)
from .store import RegionStore


class TenantAuthenticator:
    """Hold only token digests and return the bound tenant after comparison."""

    def __init__(self, token_to_tenant: Mapping[str, str]) -> None:
        if not token_to_tenant:
            raise ValidationError("at least one API token is required")
        digests: list[tuple[bytes, str]] = []
        for token, tenant in token_to_tenant.items():
            if not isinstance(token, str) or len(token.encode("utf-8")) < 32:
                raise ValidationError("API tokens must contain at least 32 UTF-8 bytes")
            digests.append(
                (hashlib.sha256(token.encode("utf-8")).digest(), validate_identifier(tenant, "tenant"))
            )
        self._digests = tuple(digests)

    def authenticate(self, authorization: str | None) -> str | None:
        if not isinstance(authorization, str) or not authorization.startswith("Bearer "):
            return None
        token = authorization[7:]
        supplied = hashlib.sha256(token.encode("utf-8")).digest()
        matched: str | None = None
        # Compare every digest so position does not create an obvious early-exit signal.
        for expected, tenant in self._digests:
            if hmac.compare_digest(supplied, expected):
                matched = tenant
        return matched


class MergeMeshHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        store: RegionStore,
        authenticator: TenantAuthenticator,
    ) -> None:
        self.store = store
        self.authenticator = authenticator
        super().__init__(address, MergeMeshHandler)


class MergeMeshHandler(BaseHTTPRequestHandler):
    server_version = "MergeMesh"
    sys_version = ""

    @property
    def app(self) -> MergeMeshHTTPServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # Avoid placing bearer tokens, document IDs, or request bodies in default logs.
        return

    def _send(self, status: int, body: Mapping[str, Any]) -> None:
        encoded = json.dumps(
            dict(body), ensure_ascii=False, allow_nan=False, sort_keys=True
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(encoded)

    def _tenant(self) -> str | None:
        tenant = self.app.authenticator.authenticate(self.headers.get("Authorization"))
        if tenant is None:
            self._send(
                HTTPStatus.UNAUTHORIZED,
                {"error": "unauthorized", "message": "a valid bearer token is required"},
            )
        return tenant

    def _document_path(self, suffix: str = "") -> str | None:
        path = unquote(urlsplit(self.path).path)
        prefix = "/v1/documents/"
        if not path.startswith(prefix):
            return None
        remainder = path[len(prefix) :]
        if suffix:
            if not remainder.endswith(suffix):
                return None
            remainder = remainder[: -len(suffix)]
        if not remainder or "/" in remainder:
            return None
        return validate_identifier(remainder, "document")

    def _read_json(self) -> Any:
        if self.headers.get("Transfer-Encoding"):
            raise ValidationError("transfer-encoded requests are not supported")
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValidationError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValidationError("invalid Content-Length") from exc
        if length < 0:
            raise ValidationError("invalid Content-Length")
        if length > MAX_BODY_BYTES:
            raise OverflowError("request body is too large")
        return strict_loads(self.rfile.read(length), max_bytes=MAX_BODY_BYTES)

    def do_GET(self) -> None:  # noqa: N802
        try:
            path = urlsplit(self.path).path
            if path == "/healthz":
                self._send(
                    HTTPStatus.OK,
                    {"status": "ok", "region": self.app.store.region},
                )
                return
            tenant = self._tenant()
            if tenant is None:
                return
            if path == "/v1/conflicts":
                self._send(
                    HTTPStatus.OK,
                    {"conflicts": self.app.store.list_conflicts(tenant=tenant)},
                )
                return
            document = self._document_path()
            if document is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            self._send(HTTPStatus.OK, self.app.store.get_document(tenant, document))
        except ValidationError as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid_request", "message": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        tenant = self._tenant()
        if tenant is None:
            return
        try:
            path = urlsplit(self.path).path
            resolving = path.endswith("/resolve")
            document = self._document_path("/resolve" if resolving else "")
            if document is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            body = self._read_json()
            if not isinstance(body, dict):
                raise ValidationError("request body must be an object")
            if resolving:
                require_exact_keys(
                    body,
                    required={"field", "expected_candidates", "delete"},
                    optional={"value"},
                    label="resolution request",
                )
                if body["delete"] and "value" in body:
                    raise ValidationError("delete resolution cannot include value")
                if not body["delete"] and "value" not in body:
                    raise ValidationError("value resolution must include value")
                result = self.app.store.resolve(
                    tenant,
                    document,
                    body["field"],
                    expected_candidates=body["expected_candidates"],
                    delete=body["delete"],
                    value=body.get("value"),
                )
            else:
                require_exact_keys(
                    body,
                    required=set(),
                    optional={"changes", "delete_fields", "expected_etag"},
                    label="patch request",
                )
                result = self.app.store.patch(
                    tenant,
                    document,
                    changes=body.get("changes", {}),
                    delete_fields=body.get("delete_fields", []),
                    expected_etag=body.get("expected_etag"),
                )
            self._send(HTTPStatus.OK, result)
        except OverflowError as exc:
            self._send(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "too_large", "message": str(exc)})
        except ConflictError as exc:
            self._send(HTTPStatus.CONFLICT, {"error": "conflict", "message": str(exc)})
        except ValidationError as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid_request", "message": str(exc)})


def create_server(
    store: RegionStore,
    token_to_tenant: Mapping[str, str],
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> MergeMeshHTTPServer:
    if not isinstance(host, str) or not host:
        raise ValidationError("host must be a non-empty string")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValidationError("port must be from 0 to 65535")
    return MergeMeshHTTPServer((host, port), store, TenantAuthenticator(token_to_tenant))
