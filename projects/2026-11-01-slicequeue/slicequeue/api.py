from __future__ import annotations

import hashlib
import hmac
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote, urlsplit

from .engine import deterministic_handler, execute_tool
from .errors import (
    ConflictError,
    IntegrityError,
    LeaseError,
    NotFoundError,
    SimulatedCrash,
    SliceQueueError,
    StateError,
    ValidationError,
)
from .model import (
    MAX_JSON_BYTES,
    canonical_json,
    parse_json,
    require_closed_object,
    require_event_kind,
    require_identifier,
    require_int,
    require_payload,
)
from .store import SliceQueue, now_ns


class SliceQueueHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        store: SliceQueue,
        tokens: tuple[str, ...],
    ) -> None:
        valid_tokens = all(
            isinstance(token, str)
            and 16 <= len(token) <= 512
            and token.isascii()
            and token.isprintable()
            and "," not in token
            and not token.isspace()
            for token in tokens
        )
        if not tokens or not valid_tokens:
            raise ValidationError(
                "each API token must be 16-512 printable ASCII characters without commas"
            )
        super().__init__(address, SliceQueueHandler)
        self.store = store
        self.tokens = tokens


class SliceQueueHandler(BaseHTTPRequestHandler):
    server: SliceQueueHTTPServer
    server_version = "SliceQueue"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        message = format % args
        print(f"slicequeue_http {self.client_address[0]} {message}", file=sys.stderr)

    def _send(self, status: int, body: Any) -> None:
        encoded = canonical_json(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(encoded)

    def _error(self, status: int, code: str, message: str) -> None:
        self._send(status, {"error": {"code": code, "message": message}})

    def _read_body(self) -> dict[str, Any]:
        media_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            raise ValidationError("Content-Type must be application/json")
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValidationError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValidationError("Content-Length must be an integer") from exc
        if length < 2 or length > MAX_JSON_BYTES:
            raise ValidationError(f"request body must be 2-{MAX_JSON_BYTES} bytes")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ValidationError("request body ended before Content-Length bytes were received")
        try:
            text = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValidationError("request body must be UTF-8") from exc
        value = parse_json(text, "request body")
        if not isinstance(value, dict):
            raise ValidationError("request body must be an object")
        return value

    def _namespace(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise PermissionError("missing bearer token")
        presented = authorization[7:]
        match: str | None = None
        presented_bytes = presented.encode("utf-8")
        for candidate in self.server.tokens:
            if hmac.compare_digest(presented_bytes, candidate.encode("ascii")):
                match = candidate
        if match is None:
            raise PermissionError("invalid bearer token")
        identity = hmac.new(self.server.store.secret, match.encode(), hashlib.sha256).hexdigest()
        return f"tenant-{identity[:24]}"

    def _parts(self) -> list[str]:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not supported")
        try:
            decoded = unquote(parsed.path, errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValidationError("path encoding is invalid") from exc
        if "//" in decoded or decoded != "/" + "/".join(part for part in decoded.split("/") if part):
            raise ValidationError("path must be canonical")
        return [part for part in decoded.split("/") if part]

    def _dispatch(self) -> tuple[int, Any]:
        parts = self._parts()
        if self.command == "GET" and parts == ["health"]:
            return 200, {"status": "ok"}
        namespace = self._namespace()

        if self.command == "GET":
            if parts == ["v1", "stats"]:
                return 200, self.server.store.stats(namespace)
            if parts == ["v1", "integrity"]:
                return 200, self.server.store.verify_integrity(namespace)
            if len(parts) == 3 and parts[:2] == ["v1", "threads"]:
                thread_id = require_identifier(parts[2], "thread_id")
                return 200, self.server.store.get_thread(namespace, thread_id)
            raise NotFoundError("route was not found")

        if self.command != "POST":
            raise NotFoundError("route was not found")
        body = self._read_body()

        if parts == ["v1", "threads"]:
            body = require_closed_object(
                body, "request body", {"idempotency_key"}, {"thread_id", "parent_thread_id"}
            )
            result = self.server.store.launch_thread(
                namespace,
                require_identifier(body["idempotency_key"], "idempotency_key"),
                thread_id=(
                    None
                    if body.get("thread_id") is None
                    else require_identifier(body["thread_id"], "thread_id")
                ),
                parent_thread_id=(
                    None
                    if body.get("parent_thread_id") is None
                    else require_identifier(body["parent_thread_id"], "parent_thread_id")
                ),
            )
            return (200 if result["replayed"] else 201), result

        if len(parts) == 4 and parts[:2] == ["v1", "threads"] and parts[3] == "events":
            body = require_closed_object(
                body,
                "request body",
                {"event_id", "kind", "payload"},
                {"available_after_ms"},
            )
            delay = require_int(
                body.get("available_after_ms", 0),
                "available_after_ms",
                minimum=0,
                maximum=86_400_000,
            )
            current = now_ns()
            result = self.server.store.enqueue_event(
                namespace,
                require_identifier(body["event_id"], "event_id"),
                require_identifier(parts[2], "thread_id"),
                require_event_kind(body["kind"]),
                require_payload(body["payload"]),
                available_ns=current + delay * 1_000_000,
                created_ns=current,
            )
            return (200 if result["replayed"] else 202), result

        if len(parts) == 4 and parts[:3] == ["v1", "work", "slices"]:
            require_closed_object(body, "request body", set())
            result = self.server.store.run_one(
                namespace,
                require_identifier(parts[3], "worker_id"),
                deterministic_handler,
            )
            return 200, {"processed": result is not None, "result": result}

        if parts == ["v1", "work", "outbox"]:
            require_closed_object(body, "request body", set(), {"limit"})
            limit = require_int(body.get("limit", 100), "limit", minimum=1, maximum=1_000)
            return 200, self.server.store.dispatch_outbox(namespace, limit=limit)

        if len(parts) == 4 and parts[:3] == ["v1", "work", "tools"]:
            require_closed_object(body, "request body", set(), {"result_available_after_ms"})
            delay = require_int(
                body.get("result_available_after_ms", 0),
                "result_available_after_ms",
                minimum=0,
                maximum=86_400_000,
            )
            worker = require_identifier(parts[3], "worker_id")
            task = self.server.store.claim_tool(namespace, worker)
            if task is None:
                return 200, {"processed": False, "result": None}
            try:
                tool_result = execute_tool(task["tool_name"], task["arguments"])
            except ValidationError as exc:
                tool_result = {"ok": False, "error": str(exc)}
            current = now_ns()
            result = self.server.store.complete_tool(
                namespace,
                task["call_id"],
                worker,
                task["lease_token"],
                tool_result,
                current_ns=current,
                result_available_ns=current + delay * 1_000_000,
            )
            return 200, {"processed": True, "result": result, "tool_result": tool_result}

        raise NotFoundError("route was not found")

    def _handle(self) -> None:
        try:
            status, body = self._dispatch()
            self._send(status, body)
        except PermissionError as exc:
            self._error(401, "unauthorized", str(exc))
        except ValidationError as exc:
            self._error(400, "invalid_request", str(exc))
        except NotFoundError as exc:
            self._error(404, "not_found", str(exc))
        except (ConflictError, LeaseError) as exc:
            self._error(409, "conflict", str(exc))
        except SimulatedCrash:
            self._error(503, "temporarily_unavailable", "operation acknowledgement was interrupted")
        except (IntegrityError, StateError):
            self._error(500, "state_error", "stored state failed a safety check")
        except SliceQueueError:
            self._error(500, "runtime_error", "runtime operation failed")
        except Exception:
            self._error(500, "internal_error", "unexpected server failure")

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def do_DELETE(self) -> None:
        self._handle()


def build_server(
    host: str,
    port: int,
    store: SliceQueue,
    tokens: tuple[str, ...],
) -> SliceQueueHTTPServer:
    return SliceQueueHTTPServer((host, port), store, tokens)
