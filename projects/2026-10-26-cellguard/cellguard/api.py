"""Small authenticated HTTP API around the CellGuard store."""

from __future__ import annotations

import hmac
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

from .errors import AuthorizationError, CellGuardError, ValidationError
from .model import validate_id
from .store import CellGuardStore


MAX_BODY_BYTES = 65_536


def _object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValidationError(f"non-finite JSON value is not allowed: {value}")


def _fields(
    body: dict[str, Any], *, required: set[str], optional: set[str] = frozenset()
) -> None:
    missing = required - body.keys()
    unknown = body.keys() - required - optional
    if missing:
        raise ValidationError(f"missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ValidationError(f"unknown fields: {', '.join(sorted(unknown))}")


class CellGuardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        store: CellGuardStore,
        tenant_tokens: dict[str, str],
        worker_token: str,
    ) -> None:
        if not tenant_tokens:
            raise ValidationError("at least one tenant API token is required")
        for tenant, token in tenant_tokens.items():
            validate_id(tenant, "tenant")
            if not isinstance(token, str) or len(token) < 24:
                raise ValidationError("tenant tokens must contain at least 24 characters")
        if len(set(tenant_tokens.values())) != len(tenant_tokens):
            raise ValidationError("tenant API tokens must be unique")
        if not isinstance(worker_token, str) or len(worker_token) < 24:
            raise ValidationError("worker token must contain at least 24 characters")
        self.store = store
        self.tenant_tokens = dict(tenant_tokens)
        self.worker_token = worker_token
        super().__init__(address, CellGuardHandler)


class CellGuardHandler(BaseHTTPRequestHandler):
    server: CellGuardHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        # Avoid emitting bearer tokens or request payloads through default logs.
        return

    def _send(self, status: int, value: Any) -> None:
        encoded = json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(encoded)

    def _error(self, error: CellGuardError) -> None:
        if isinstance(error, AuthorizationError):
            # This response is intentionally sent manually to include the challenge.
            encoded = json.dumps(
                {"error": {"code": error.code, "message": str(error)}},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            self.send_response(error.status)
            self.send_header("WWW-Authenticate", 'Bearer realm="cellguard"')
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.end_headers()
            self.wfile.write(encoded)
            return
        self._send(error.status, {"error": {"code": error.code, "message": str(error)}})

    def _bearer(self) -> str:
        value = self.headers.get("Authorization", "")
        if not value.startswith("Bearer ") or len(value) <= 7:
            raise AuthorizationError("a bearer token is required")
        return value[7:]

    def _tenant(self) -> str:
        presented = self._bearer()
        match: str | None = None
        # Always scan every configured token to reduce tenant-enumeration timing signal.
        for tenant, expected in self.server.tenant_tokens.items():
            if hmac.compare_digest(presented, expected):
                match = tenant
        if match is None:
            raise AuthorizationError("invalid bearer token")
        return match

    def _worker(self) -> None:
        if not hmac.compare_digest(self._bearer(), self.server.worker_token):
            raise AuthorizationError("invalid worker bearer token")

    def _json_body(self) -> dict[str, Any]:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValidationError("Content-Type must be application/json")
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValidationError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValidationError("Content-Length must be an integer") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise ValidationError(f"request body must not exceed {MAX_BODY_BYTES} bytes")
        raw = self.rfile.read(length)
        try:
            value = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_object_no_duplicates,
                parse_constant=_reject_constant,
            )
        except UnicodeDecodeError as exc:
            raise ValidationError("request body must be UTF-8") from exc
        except json.JSONDecodeError as exc:
            raise ValidationError("request body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ValidationError("request body must be a JSON object")
        return value

    def _dispatch(self, action: Callable[[], None]) -> None:
        try:
            action()
        except CellGuardError as exc:
            self._error(exc)
        except Exception:
            self._send(500, {"error": {"code": "internal_error", "message": "internal error"}})

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        self._dispatch(self._get)

    def _get(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not accepted")
        path = parsed.path
        if path == "/healthz":
            self._send(200, {"status": "ok"})
            return
        tenant = self._tenant()
        if path == "/v1/assignment":
            self._send(200, self.server.store.assignment(tenant).as_dict())
            return
        prefix = "/v1/jobs/"
        if path.startswith(prefix) and "/" not in path[len(prefix) :]:
            self._send(200, self.server.store.job(tenant, path[len(prefix) :]))
            return
        self._send(404, {"error": {"code": "not_found", "message": "route not found"}})

    def do_POST(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        self._dispatch(self._post)

    def _post(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not accepted")
        path = parsed.path
        if path == "/v1/jobs":
            tenant = self._tenant()
            body = self._json_body()
            _fields(body, required={"idempotency_key", "payload"}, optional={"priority"})
            result = self.server.store.submit(
                tenant,
                body["idempotency_key"],
                body["payload"],
                priority=body.get("priority", 0),
            )
            self._send(200 if result.replayed else 201, result.as_dict())
            return
        if path == "/internal/claims":
            self._worker()
            body = self._json_body()
            _fields(
                body,
                required={"cell_id", "worker"},
                optional={"lease_ns", "limit"},
            )
            claims = self.server.store.claim(
                body["cell_id"],
                body["worker"],
                limit=body.get("limit", 1),
                lease_ns=body.get("lease_ns", 30_000_000_000),
            )
            self._send(200, {"claims": [claim.as_dict() for claim in claims]})
            return
        internal_prefix = "/internal/jobs/"
        if path.startswith(internal_prefix):
            self._worker()
            remainder = path[len(internal_prefix) :]
            parts = remainder.split("/")
            if len(parts) == 2 and parts[0] and parts[1] in {"complete", "fail"}:
                body = self._json_body()
                if parts[1] == "complete":
                    _fields(body, required={"lease_token", "result"})
                    self._send(
                        200,
                        self.server.store.complete(parts[0], body["lease_token"], body["result"]),
                    )
                else:
                    _fields(
                        body,
                        required={"error", "lease_token"},
                        optional={"retry_delay_ns"},
                    )
                    self._send(
                        200,
                        self.server.store.fail(
                            parts[0],
                            body["lease_token"],
                            body["error"],
                            retry_delay_ns=body.get("retry_delay_ns", 0),
                        ),
                    )
                return
        self._send(404, {"error": {"code": "not_found", "message": "route not found"}})


def create_server(
    store: CellGuardStore,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
    tenant_tokens: dict[str, str],
    worker_token: str,
) -> CellGuardHTTPServer:
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65_535:
        raise ValidationError("port must be between 0 and 65535")
    return CellGuardHTTPServer((host, port), store, tenant_tokens, worker_token)
