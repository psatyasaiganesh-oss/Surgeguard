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
from .store import DEFAULT_MAX_BASELINE_AGE_NS, TestSieveStore


MAX_BODY_BYTES = 4_194_304


class TestSieveHandler(BaseHTTPRequestHandler):
    store: TestSieveStore
    repository_tokens: dict[str, str]
    server_version = "TestSieve/1"
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

    def _repository(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise AuthenticationError("missing bearer token")
        supplied = authorization[7:]
        matched: str | None = None
        for repository, token in self.repository_tokens.items():
            if hmac.compare_digest(supplied, token):
                matched = repository
        if matched is None:
            raise AuthenticationError("invalid bearer token")
        return matched

    def _body(self) -> dict[str, Any]:
        media_type, _, parameters = self.headers.get("Content-Type", "").partition(";")
        if media_type.strip().lower() != "application/json":
            raise ValidationError("Content-Type must be application/json")
        if parameters and parameters.strip().lower() not in {"charset=utf-8", "charset=\"utf-8\""}:
            raise ValidationError("only UTF-8 JSON is supported")
        header = self.headers.get("Content-Length")
        if header is None:
            raise ValidationError("Content-Length is required")
        try:
            length = int(header)
        except ValueError as exc:
            raise ValidationError("invalid Content-Length") from exc
        if not 0 < length <= MAX_BODY_BYTES:
            raise ValidationError("request body must contain 1 byte to 4 MiB")
        try:
            raw = self.rfile.read(length).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValidationError("request body must be UTF-8") from exc
        body = parse_json(raw, "request JSON")
        if not isinstance(body, dict):
            raise ValidationError("request JSON must be an object")
        return body

    @staticmethod
    def _parts(path: str) -> list[str]:
        parsed = urlsplit(path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not supported")
        return [part for part in parsed.path.split("/") if part]

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

    def do_GET(self) -> None:
        try:
            parts = self._parts(self.path)
            if parts == ["healthz"]:
                self._json(HTTPStatus.OK, {"status": "ok"})
                return
            repository = self._repository()
            if parts == ["v1", "baselines", "latest"]:
                self._json(HTTPStatus.OK, self.store.latest_baseline(repository))
            elif parts == ["v1", "stats"]:
                self._json(HTTPStatus.OK, self.store.stats(repository))
            else:
                raise NotFoundError("route was not found")
        except Exception as exc:
            self._dispatch_error(exc)

    def do_POST(self) -> None:
        try:
            parts = self._parts(self.path)
            repository = self._repository()
            body = self._body()
            if parts == ["v1", "baselines"]:
                body = require_closed_object(
                    body,
                    "body",
                    {"idempotency_key", "expected_head_revision", "snapshot"},
                )
                result = self.store.publish_baseline(
                    repository,
                    body["idempotency_key"],
                    body["expected_head_revision"],
                    body["snapshot"],
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "mode"]:
                body = require_closed_object(body, "body", {"mode"})
                self._json(HTTPStatus.OK, self.store.set_mode(repository, body["mode"]))
            elif parts == ["v1", "selections"]:
                body = require_closed_object(
                    body,
                    "body",
                    {"idempotency_key", "current_inventory"},
                    {"worker_count", "max_baseline_age_ns"},
                )
                result = self.store.select_tests(
                    repository,
                    body["idempotency_key"],
                    body["current_inventory"],
                    worker_count=body.get("worker_count", 4),
                    max_baseline_age_ns=body.get(
                        "max_baseline_age_ns", DEFAULT_MAX_BASELINE_AGE_NS
                    ),
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif (
                len(parts) == 4
                and parts[:2] == ["v1", "selections"]
                and parts[3] == "shadow-results"
            ):
                body = require_closed_object(
                    body, "body", {"idempotency_key", "actual_failures"}
                )
                result = self.store.record_shadow_result(
                    repository,
                    parts[2],
                    body["idempotency_key"],
                    body["actual_failures"],
                )
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "manifests", "verify"]:
                body = require_closed_object(body, "body", {"manifest"})
                self._json(
                    HTTPStatus.OK,
                    self.store.verify_manifest(
                        body["manifest"], expected_repository=repository
                    ),
                )
            else:
                raise NotFoundError("route was not found")
        except Exception as exc:
            self._dispatch_error(exc)


def build_handler(
    store: TestSieveStore, repository_tokens: dict[str, str]
) -> type[TestSieveHandler]:
    if not repository_tokens:
        raise ValidationError("at least one repository token is required")
    normalized: dict[str, str] = {}
    for repository, token in repository_tokens.items():
        require_identifier(repository, "repository")
        if not isinstance(token, str) or len(token) < 24:
            raise ValidationError("repository tokens must contain at least 24 characters")
        if token in normalized.values():
            raise ValidationError("repository tokens must be distinct")
        normalized[repository] = token

    class Handler(TestSieveHandler):
        pass

    Handler.store = store
    Handler.repository_tokens = normalized
    return Handler


def serve(
    store: TestSieveStore,
    repository_tokens: dict[str, str],
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValidationError("the educational server may bind only to loopback")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValidationError("port must be between 1 and 65535")
    server = ThreadingHTTPServer((host, port), build_handler(store, repository_tokens))
    server.serve_forever()
