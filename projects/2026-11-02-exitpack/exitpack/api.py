from __future__ import annotations

import hashlib
import hmac
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote, urlsplit

from .errors import (
    AuthorizationError,
    CapacityError,
    ConflictError,
    ExitPackError,
    IntegrityError,
    NotFoundError,
    SimulatedCrash,
    ValidationError,
)
from .model import (
    MAX_JSON_BYTES,
    canonical_json,
    parse_json,
    require_closed_object,
    require_identifier,
    require_int,
    require_policy,
)
from .store import ExitPack


class ExitPackHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        store: ExitPack,
        tokens: tuple[str, ...],
    ) -> None:
        valid = all(
            isinstance(token, str)
            and 16 <= len(token) <= 512
            and token.isascii()
            and token.isprintable()
            and "," not in token
            and not token.isspace()
            for token in tokens
        )
        if not tokens or not valid:
            raise ValidationError(
                "each API token must be 16-512 printable ASCII characters without commas"
            )
        super().__init__(address, ExitPackHandler)
        self.store = store
        self.tokens = tokens


class ExitPackHandler(BaseHTTPRequestHandler):
    server: ExitPackHTTPServer
    server_version = "ExitPack"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        print(
            f"exitpack_http {self.client_address[0]} {format % args}",
            file=sys.stderr,
        )

    def _send(self, status: int, value: Any) -> None:
        body = canonical_json(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, code: str, message: str) -> None:
        self._send(status, {"error": {"code": code, "message": message}})

    def _body(self) -> dict[str, Any]:
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

    def _parts(self) -> list[str]:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not supported")
        try:
            decoded = unquote(parsed.path, errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValidationError("path encoding is invalid") from exc
        canonical = "/" + "/".join(part for part in decoded.split("/") if part)
        if "//" in decoded or decoded != canonical:
            raise ValidationError("path must be canonical")
        return [part for part in decoded.split("/") if part]

    def _namespace(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise AuthorizationError("missing bearer token")
        presented = authorization[7:].encode("utf-8")
        match: str | None = None
        for candidate in self.server.tokens:
            if hmac.compare_digest(presented, candidate.encode("ascii")):
                match = candidate
        if match is None:
            raise AuthorizationError("invalid bearer token")
        identity = hmac.new(
            self.server.store.secret, match.encode("ascii"), hashlib.sha256
        ).hexdigest()
        return f"tenant-{identity[:24]}"

    def _dispatch(self) -> tuple[int, Any]:
        parts = self._parts()
        if self.command == "GET" and parts == ["health"]:
            return 200, {"status": "ok"}
        namespace = self._namespace()

        if self.command == "GET":
            if parts == ["v1", "snapshot"]:
                return 200, self.server.store.snapshot(namespace)
            if parts == ["v1", "metrics"]:
                return 200, self.server.store.metrics(namespace)
            if parts == ["v1", "integrity"]:
                return 200, self.server.store.verify_integrity(namespace)
            raise NotFoundError("route was not found")

        if self.command != "POST":
            raise NotFoundError("route was not found")
        body = self._body()

        if parts == ["v1", "hosts"]:
            require_closed_object(
                body,
                "request body",
                {
                    "idempotency_key",
                    "host_id",
                    "zone",
                    "cpu_capacity",
                    "memory_capacity",
                },
            )
            result = self.server.store.register_host(
                namespace,
                require_identifier(body["idempotency_key"], "idempotency_key"),
                require_identifier(body["host_id"], "host_id"),
                require_identifier(body["zone"], "zone"),
                require_int(body["cpu_capacity"], "cpu_capacity", minimum=1),
                require_int(body["memory_capacity"], "memory_capacity", minimum=1),
            )
            return (200 if result["replayed"] else 201), result

        if parts == ["v1", "plans"]:
            require_closed_object(
                body,
                "request body",
                {"idempotency_key", "vm", "policy"},
                {"expected_revision"},
            )
            result = self.server.store.create_plan(
                namespace,
                require_identifier(body["idempotency_key"], "idempotency_key"),
                body["vm"],
                require_policy(body["policy"]),
                expected_revision=(
                    None
                    if body.get("expected_revision") is None
                    else require_int(body["expected_revision"], "expected_revision")
                ),
            )
            return (200 if result["replayed"] else 201), result

        if len(parts) == 4 and parts[:2] == ["v1", "plans"] and parts[3] == "commit":
            require_closed_object(body, "request body", {"commit_token"})
            result = self.server.store.commit_plan(
                namespace,
                require_identifier(parts[2], "plan_id"),
                require_identifier(body["commit_token"], "commit_token"),
            )
            return 200, result

        if len(parts) == 4 and parts[:2] == ["v1", "vms"] and parts[3] == "finish":
            require_closed_object(body, "request body", {"idempotency_key"})
            result = self.server.store.finish_vm(
                namespace,
                require_identifier(body["idempotency_key"], "idempotency_key"),
                require_identifier(parts[2], "vm_id"),
            )
            return 200, result

        if len(parts) == 4 and parts[:2] == ["v1", "vms"] and parts[3] == "migrate":
            require_closed_object(
                body,
                "request body",
                {"idempotency_key", "destination_host_id", "expected_revision"},
            )
            result = self.server.store.migrate_vm(
                namespace,
                require_identifier(body["idempotency_key"], "idempotency_key"),
                require_identifier(parts[2], "vm_id"),
                require_identifier(body["destination_host_id"], "destination_host_id"),
                require_int(body["expected_revision"], "expected_revision"),
            )
            return 200, result

        if (
            len(parts) == 4
            and parts[:2] == ["v1", "hosts"]
            and parts[3] == "maintenance-plan"
        ):
            require_closed_object(body, "request body", {"horizon_s"})
            return 200, self.server.store.maintenance_plan(
                namespace,
                require_identifier(parts[2], "host_id"),
                require_int(body["horizon_s"], "horizon_s", maximum=604_800),
            )

        if len(parts) == 4 and parts[:2] == ["v1", "hosts"] and parts[3] == "drain":
            require_closed_object(body, "request body", {"idempotency_key"})
            result = self.server.store.set_draining(
                namespace,
                require_identifier(body["idempotency_key"], "idempotency_key"),
                require_identifier(parts[2], "host_id"),
            )
            return 200, result

        if parts == ["v1", "reconcile"]:
            require_closed_object(body, "request body", set())
            return 200, self.server.store.reconcile(namespace)

        raise NotFoundError("route was not found")

    def _handle(self) -> None:
        try:
            status, body = self._dispatch()
            self._send(status, body)
        except AuthorizationError as exc:
            self._error(401, "unauthorized", str(exc))
        except ValidationError as exc:
            self._error(400, "invalid_request", str(exc))
        except NotFoundError as exc:
            self._error(404, "not_found", str(exc))
        except (ConflictError, CapacityError) as exc:
            self._error(409, "conflict", str(exc))
        except SimulatedCrash:
            self._error(503, "temporarily_unavailable", "operation acknowledgement was interrupted")
        except IntegrityError:
            self._error(500, "integrity_error", "stored state failed verification")
        except ExitPackError:
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
    store: ExitPack,
    tokens: tuple[str, ...],
) -> ExitPackHTTPServer:
    return ExitPackHTTPServer((host, port), store, tokens)
