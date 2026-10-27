from __future__ import annotations

import hmac
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .errors import (
    AuthenticationError,
    ConflictError,
    IntegrityError,
    NotFoundError,
    ValidationError,
)
from .model import parse_json, require_closed_object, require_identifier
from .store import ClearFlowStore


MAX_BODY_BYTES = 1_048_576


class ClearFlowHandler(BaseHTTPRequestHandler):
    store: ClearFlowStore
    tenant_tokens: dict[str, str]
    server_version = "ClearFlow/1"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")

    def _json(self, status: int, value: Any) -> None:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _error(self, status: int, code: str, message: str) -> None:
        self._json(status, {"error": {"code": code, "message": message}})

    def _tenant(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise AuthenticationError("missing bearer token")
        supplied = authorization[7:]
        match: str | None = None
        for tenant, token in self.tenant_tokens.items():
            if hmac.compare_digest(supplied, token):
                match = tenant
        if match is None:
            raise AuthenticationError("invalid bearer token")
        return match

    def _body(self) -> dict[str, Any]:
        content_type = self.headers.get("Content-Type", "")
        media_type, _, parameters = content_type.partition(";")
        if media_type.strip().lower() != "application/json":
            raise ValidationError("Content-Type must be application/json")
        if parameters and parameters.strip().lower() not in {"charset=utf-8", "charset=\"utf-8\""}:
            raise ValidationError("only UTF-8 JSON is supported")
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise ValidationError("Content-Length is required")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise ValidationError("invalid Content-Length") from exc
        if not 0 < length <= MAX_BODY_BYTES:
            raise ValidationError("request body must contain 1 byte to 1 MiB")
        try:
            raw = self.rfile.read(length).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValidationError("request body must be UTF-8") from exc
        value = parse_json(raw, "request JSON")
        if not isinstance(value, dict):
            raise ValidationError("request JSON must be an object")
        return value

    def _dispatch_error(self, exc: Exception) -> None:
        if isinstance(exc, AuthenticationError):
            self._error(HTTPStatus.UNAUTHORIZED, "unauthorized", str(exc))
        elif isinstance(exc, NotFoundError):
            self._error(HTTPStatus.NOT_FOUND, "not_found", str(exc))
        elif isinstance(exc, ConflictError):
            self._error(HTTPStatus.CONFLICT, "conflict", str(exc))
        elif isinstance(exc, ValidationError):
            self._error(HTTPStatus.BAD_REQUEST, "validation_error", str(exc))
        elif isinstance(exc, IntegrityError):
            self._error(HTTPStatus.UNPROCESSABLE_ENTITY, "integrity_error", str(exc))
        else:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "internal_error", "internal server error")

    @staticmethod
    def _parts(path: str) -> list[str]:
        parsed = urlsplit(path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not supported")
        return [part for part in parsed.path.split("/") if part]

    def do_GET(self) -> None:
        try:
            parts = self._parts(self.path)
            if parts == ["healthz"]:
                self._json(HTTPStatus.OK, {"status": "ok"})
                return
            tenant = self._tenant()
            if parts == ["v1", "balances"]:
                self._json(HTTPStatus.OK, self.store.account_balances(tenant))
            elif len(parts) == 3 and parts[:2] == ["v1", "events"]:
                self._json(HTTPStatus.OK, self.store.get_event(tenant, parts[2]))
            elif parts == ["v1", "stats"]:
                self._json(HTTPStatus.OK, self.store.stats(tenant))
            else:
                raise NotFoundError("route was not found")
        except Exception as exc:
            self._dispatch_error(exc)

    def do_POST(self) -> None:
        try:
            parts = self._parts(self.path)
            tenant = self._tenant()
            body = self._body()
            if parts == ["v1", "accounts"]:
                body = require_closed_object(body, "body", {"account_id", "kind", "currency"})
                result = self.store.create_account(
                    tenant, body["account_id"], body["kind"], body["currency"]
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "events"]:
                body = require_closed_object(
                    body, "body", {"idempotency_key", "event"}, {"received_ns"}
                )
                result = self.store.ingest_event(
                    tenant,
                    body["idempotency_key"],
                    body["event"],
                    received_ns=body.get("received_ns"),
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "manifests"]:
                body = require_closed_object(
                    body,
                    "body",
                    {
                        "idempotency_key",
                        "batch_id",
                        "producer",
                        "expected_references",
                        "deadline_ns",
                    },
                )
                result = self.store.publish_manifest(
                    tenant,
                    body["idempotency_key"],
                    body["batch_id"],
                    body["producer"],
                    body["expected_references"],
                    body["deadline_ns"],
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "quality", "evaluations"]:
                body = require_closed_object(
                    body, "body", {"idempotency_key", "as_of_ns", "policy"}
                )
                result = self.store.evaluate_quality(
                    tenant,
                    body["idempotency_key"],
                    body["as_of_ns"],
                    body["policy"],
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "quality", "attestations", "verify"]:
                body = require_closed_object(body, "body", {"attestation"})
                self._json(
                    HTTPStatus.OK,
                    self.store.verify_attestation(body["attestation"], expected_tenant=tenant),
                )
            else:
                raise NotFoundError("route was not found")
        except Exception as exc:
            self._dispatch_error(exc)


def build_handler(store: ClearFlowStore, tenant_tokens: dict[str, str]) -> type[ClearFlowHandler]:
    if not tenant_tokens:
        raise ValidationError("at least one tenant token is required")
    normalized: dict[str, str] = {}
    for tenant, token in tenant_tokens.items():
        require_identifier(tenant, "tenant")
        if not isinstance(token, str) or len(token) < 24:
            raise ValidationError("tenant tokens must contain at least 24 characters")
        if token in normalized.values():
            raise ValidationError("tenant tokens must be distinct")
        normalized[tenant] = token

    class Handler(ClearFlowHandler):
        pass

    Handler.store = store
    Handler.tenant_tokens = normalized
    return Handler


def serve(
    store: ClearFlowStore,
    tenant_tokens: dict[str, str],
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValidationError("the educational server may bind only to loopback")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValidationError("port must be between 1 and 65535")
    server = ThreadingHTTPServer((host, port), build_handler(store, tenant_tokens))
    server.serve_forever()
