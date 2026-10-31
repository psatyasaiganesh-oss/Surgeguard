from __future__ import annotations

import hmac
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .errors import ConflictError, IntegrityError, NotFoundError, StateError, ValidationError
from .model import parse_json, require_closed_object
from .store import TabletForge


MAX_BODY_BYTES = 4_194_304


class TabletForgeHandler(BaseHTTPRequestHandler):
    store: TabletForge
    bearer_token: str
    server_version = "TabletForge/1"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")

    def _json(self, status: int, value: Any) -> None:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self._headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _authenticate(self) -> None:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer ") or not hmac.compare_digest(
            authorization[7:], self.bearer_token
        ):
            raise PermissionError("invalid bearer token")

    def _body(self) -> dict[str, Any]:
        media_type, _, parameters = self.headers.get("Content-Type", "").partition(";")
        if media_type.strip().lower() != "application/json":
            raise ValidationError("Content-Type must be application/json")
        if parameters and parameters.strip().lower() not in {"charset=utf-8", 'charset="utf-8"'}:
            raise ValidationError("only UTF-8 JSON is supported")
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValidationError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValidationError("Content-Length is invalid") from exc
        if not 0 < length <= MAX_BODY_BYTES:
            raise ValidationError("request body must contain 1 byte to 4 MiB")
        try:
            raw = self.rfile.read(length).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValidationError("request body must be UTF-8") from exc
        value = parse_json(raw, "request JSON")
        if not isinstance(value, dict):
            raise ValidationError("request JSON must be an object")
        return value

    @staticmethod
    def _parts(path: str) -> list[str]:
        parsed = urlsplit(path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not supported")
        return [part for part in parsed.path.split("/") if part]

    def _dispatch_error(self, exc: Exception) -> None:
        if isinstance(exc, PermissionError):
            self._json(HTTPStatus.UNAUTHORIZED, {"error": {"code": "unauthorized", "message": str(exc)}})
        elif isinstance(exc, NotFoundError):
            self._json(HTTPStatus.NOT_FOUND, {"error": {"code": "not_found", "message": str(exc)}})
        elif isinstance(exc, (ConflictError, StateError)):
            self._json(HTTPStatus.CONFLICT, {"error": {"code": "conflict", "message": str(exc)}})
        elif isinstance(exc, ValidationError):
            self._json(
                HTTPStatus.BAD_REQUEST,
                {"error": {"code": "validation_error", "message": str(exc)}},
            )
        elif isinstance(exc, IntegrityError):
            self._json(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                {"error": {"code": "integrity_error", "message": str(exc)}},
            )
        else:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": {"code": "internal_error", "message": "internal server error"}},
            )

    def do_GET(self) -> None:
        try:
            parts = self._parts(self.path)
            if parts == ["healthz"]:
                self._json(HTTPStatus.OK, {"status": "ok"})
                return
            self._authenticate()
            if parts == ["v1", "stats"]:
                self._json(HTTPStatus.OK, self.store.stats())
            elif parts == ["v1", "verify"]:
                self._json(HTTPStatus.OK, self.store.verify_integrity())
            else:
                raise NotFoundError("route was not found")
        except Exception as exc:
            self._dispatch_error(exc)

    def do_POST(self) -> None:
        try:
            parts = self._parts(self.path)
            self._authenticate()
            body = self._body()
            if parts == ["v1", "batches"]:
                body = require_closed_object(body, "body", {"idempotency_key", "mutations"})
                result = self.store.write_batch(body["idempotency_key"], body["mutations"])
                self._json(HTTPStatus.OK if result["replayed"] else HTTPStatus.CREATED, result)
            elif parts == ["v1", "get"]:
                body = require_closed_object(body, "body", {"key"}, {"snapshot_id"})
                self._json(
                    HTTPStatus.OK,
                    self.store.get(body["key"], snapshot_id=body.get("snapshot_id")),
                )
            elif parts == ["v1", "scan"]:
                body = require_closed_object(
                    body, "body", set(), {"start", "end", "limit", "snapshot_id"}
                )
                self._json(
                    HTTPStatus.OK,
                    self.store.scan(
                        start=body.get("start"),
                        end=body.get("end"),
                        limit=body.get("limit", 100),
                        snapshot_id=body.get("snapshot_id"),
                    ),
                )
            elif parts == ["v1", "snapshots"]:
                body = require_closed_object(body, "body", set(), {"snapshot_id"})
                self._json(HTTPStatus.CREATED, self.store.create_snapshot(body.get("snapshot_id")))
            elif parts == ["v1", "snapshots", "release"]:
                body = require_closed_object(body, "body", {"snapshot_id"})
                self._json(HTTPStatus.OK, self.store.release_snapshot(body["snapshot_id"]))
            elif parts == ["v1", "flush"]:
                require_closed_object(body, "body", set())
                self._json(HTTPStatus.OK, self.store.flush())
            elif parts == ["v1", "compact"]:
                require_closed_object(body, "body", set())
                self._json(HTTPStatus.OK, self.store.compact())
            else:
                raise NotFoundError("route was not found")
        except Exception as exc:
            self._dispatch_error(exc)


def build_handler(store: TabletForge, bearer_token: str) -> type[TabletForgeHandler]:
    if not isinstance(bearer_token, str) or len(bearer_token) < 24:
        raise ValidationError("bearer token must contain at least 24 characters")

    class Handler(TabletForgeHandler):
        pass

    Handler.store = store
    Handler.bearer_token = bearer_token
    return Handler


def serve(
    store: TabletForge,
    bearer_token: str,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValidationError("the educational server may bind only to loopback")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValidationError("port must be between 1 and 65535")
    server = ThreadingHTTPServer((host, port), build_handler(store, bearer_token))
    server.serve_forever()
