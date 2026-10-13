"""Loopback-only JSON API for the in-memory QuorumForge cluster."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from .cluster import QuorumCluster
from .errors import ConflictError, UnavailableError, ValidationError


MAX_BODY_BYTES = 16 * 1024


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


class QuorumHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, cluster: QuorumCluster):
        self.cluster = cluster
        self.cluster_lock = threading.RLock()
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    server: QuorumHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):  # noqa: A002 - inherited API
        return

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _route(self):
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if set(query) != {"coordinator"} or len(query["coordinator"]) != 1:
            raise ValidationError("exactly one coordinator query parameter is required")
        if not parsed.path.startswith("/kv/"):
            raise ValidationError("invalid key-value path")
        try:
            key = unquote(parsed.path[4:], errors="strict")
        except UnicodeDecodeError as exc:
            raise ValidationError("key path must be valid UTF-8") from exc
        return key, query["coordinator"][0]

    def _body(self, *, allow_empty: bool = False) -> dict:
        if self.headers.get("Transfer-Encoding"):
            self.close_connection = True
            raise ValidationError("transfer encoding is not supported")
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValidationError("Content-Type must be application/json")
        value = self.headers.get("Content-Length")
        if value is None or not value.isascii() or not value.isdigit() or len(value) > 8:
            self.close_connection = True
            raise ValidationError("valid Content-Length is required")
        length = int(value)
        minimum = 0 if allow_empty else 2
        if not minimum <= length <= MAX_BODY_BYTES:
            self.close_connection = True
            raise ValidationError("request body size is out of range")
        raw = self.rfile.read(length)
        if len(raw) != length:
            self.close_connection = True
            raise ValidationError("request body ended early")
        if not raw and allow_empty:
            return {}
        try:
            result = json.loads(raw, object_pairs_hook=_unique_object)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError("body must be valid UTF-8 JSON") from exc
        if not isinstance(result, dict):
            raise ValidationError("body must be a JSON object")
        return result

    def _error(self, exc: Exception) -> None:
        if isinstance(exc, UnavailableError):
            self._json(503, {"error": "quorum_unavailable", "detail": str(exc)})
        elif isinstance(exc, ConflictError):
            self._json(409, {"error": "replica_conflict", "detail": str(exc)})
        else:
            self._json(400, {"error": "invalid_request", "detail": str(exc)})

    def do_GET(self):
        try:
            parsed = urlsplit(self.path)
            if parsed.path == "/health" and not parsed.query:
                self._json(200, {"status": "ok"})
                return
            if parsed.path == "/cluster" and not parsed.query:
                with self.server.cluster_lock:
                    self._json(200, self.server.cluster.snapshot())
                return
            key, coordinator = self._route()
            with self.server.cluster_lock:
                result = self.server.cluster.get(key, coordinator=coordinator)
            self._json(200, result.to_dict())
        except (ValidationError, UnavailableError, ConflictError) as exc:
            self._error(exc)

    def do_PUT(self):
        try:
            key, coordinator = self._route()
            body = self._body()
            if not set(body).issubset({"value", "context"}) or "value" not in body:
                raise ValidationError("body must contain value and optional context")
            with self.server.cluster_lock:
                result = self.server.cluster.put(
                    key, body["value"], coordinator=coordinator, context=body.get("context")
                )
            self._json(201, result.to_dict())
        except (ValidationError, UnavailableError, ConflictError) as exc:
            self._error(exc)

    def do_DELETE(self):
        try:
            key, coordinator = self._route()
            body = self._body(allow_empty=True)
            if set(body) - {"context"}:
                raise ValidationError("delete body may contain only context")
            with self.server.cluster_lock:
                result = self.server.cluster.delete(key, coordinator=coordinator, context=body.get("context"))
            self._json(200, result.to_dict())
        except (ValidationError, UnavailableError, ConflictError) as exc:
            self._error(exc)

    def do_POST(self):
        self._json(405, {"error": "method_not_allowed"})


class ServerRuntime:
    def __init__(self, cluster: QuorumCluster, *, host: str = "127.0.0.1", port: int = 0):
        if host not in {"127.0.0.1", "::1"}:
            raise ValidationError("reference server binds only to a literal loopback address")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValidationError("port must be an integer in [0, 65535]")
        self.server = QuorumHTTPServer((host, port), cluster)
        self.thread = threading.Thread(target=self.server.serve_forever, name="quorumforge-http", daemon=True)

    @property
    def address(self) -> tuple[str, int]:
        return self.server.server_address[:2]

    def start(self):
        self.thread.start()
        return self

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("HTTP server did not stop cleanly")

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()
