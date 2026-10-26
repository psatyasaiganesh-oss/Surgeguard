"""Authenticated local HTTP adapter for rollout pipelines."""

from __future__ import annotations

import hmac
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

from .errors import AuthorizationError, RolloutJudgeError, ValidationError
from .model import validate_id
from .store import RolloutJudgeStore


MAX_BODY_BYTES = 1_048_576


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


class RolloutJudgeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        store: RolloutJudgeStore,
        service_tokens: dict[str, str],
    ) -> None:
        if not service_tokens:
            raise ValidationError("at least one service token is required")
        for service, token in service_tokens.items():
            validate_id(service, "service")
            if not isinstance(token, str) or len(token) < 24:
                raise ValidationError("service tokens must contain at least 24 characters")
        if len(set(service_tokens.values())) != len(service_tokens):
            raise ValidationError("service tokens must be unique")
        self.store = store
        self.service_tokens = dict(service_tokens)
        super().__init__(address, RolloutJudgeHandler)


class RolloutJudgeHandler(BaseHTTPRequestHandler):
    server: RolloutJudgeHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send(self, status: int, value: Any) -> None:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(encoded)

    def _error(self, error: RolloutJudgeError) -> None:
        encoded = json.dumps(
            {"error": {"code": error.code, "message": str(error)}},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(error.status)
        if isinstance(error, AuthorizationError):
            self.send_header("WWW-Authenticate", 'Bearer realm="rolloutjudge"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(encoded)

    def _dispatch(self, action: Callable[[], None]) -> None:
        try:
            action()
        except RolloutJudgeError as exc:
            self._error(exc)
        except Exception:
            self._send(500, {"error": {"code": "internal_error", "message": "internal error"}})

    def _service(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer ") or len(authorization) <= 7:
            raise AuthorizationError("a bearer token is required")
        presented = authorization[7:]
        match: str | None = None
        for service, expected in self.server.service_tokens.items():
            if hmac.compare_digest(presented, expected):
                match = service
        if match is None:
            raise AuthorizationError("invalid bearer token")
        return match

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
        if not 0 <= length <= MAX_BODY_BYTES:
            raise ValidationError(f"request body must not exceed {MAX_BODY_BYTES} bytes")
        raw = self.rfile.read(length)
        try:
            body = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_object_no_duplicates,
                parse_constant=_reject_constant,
            )
        except UnicodeDecodeError as exc:
            raise ValidationError("request body must be UTF-8") from exc
        except json.JSONDecodeError as exc:
            raise ValidationError("request body must be valid JSON") from exc
        if not isinstance(body, dict):
            raise ValidationError("request body must be a JSON object")
        return body

    def _path(self) -> str:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not accepted")
        return parsed.path

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch(self._get)

    def _get(self) -> None:
        path = self._path()
        if path == "/healthz":
            self._send(200, {"status": "ok"})
            return
        service = self._service()
        prefix = "/v1/runs/"
        if path.startswith(prefix) and "/" not in path[len(prefix) :]:
            self._send(200, self.server.store.get_run(service, path[len(prefix) :]))
            return
        self._send(404, {"error": {"code": "not_found", "message": "route not found"}})

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch(self._post)

    def _post(self) -> None:
        path = self._path()
        service = self._service()
        if path == "/v1/runs":
            body = self._json_body()
            _fields(
                body,
                required={"expected_trials", "idempotency_key", "policy_id", "release"},
            )
            result = self.server.store.start_run(
                service,
                body["release"],
                body["idempotency_key"],
                body["policy_id"],
                body["expected_trials"],
            )
            self._send(200 if result["replayed"] else 201, result)
            return
        if path == "/v1/receipts/verify":
            body = self._json_body()
            _fields(body, required={"receipt"})
            self._send(
                200,
                self.server.store.verify_receipt(
                    body["receipt"], expected_service=service
                ),
            )
            return
        prefix = "/v1/runs/"
        if path.startswith(prefix):
            remainder = path[len(prefix) :]
            parts = remainder.split("/")
            if len(parts) == 2 and parts[0] and parts[1] in {"evidence", "judge"}:
                body = self._json_body()
                if parts[1] == "evidence":
                    _fields(
                        body,
                        required={"baseline", "batch_key", "canary", "metric", "trial"},
                    )
                    result = self.server.store.ingest_evidence(
                        service,
                        parts[0],
                        body["batch_key"],
                        body["trial"],
                        body["metric"],
                        body["baseline"],
                        body["canary"],
                    )
                    self._send(200 if result["replayed"] else 201, result)
                else:
                    _fields(body, required={"decision_key", "expected_version"})
                    self._send(
                        200,
                        self.server.store.judge_run(
                            service,
                            parts[0],
                            body["decision_key"],
                            expected_version=body["expected_version"],
                        ),
                    )
                return
        self._send(404, {"error": {"code": "not_found", "message": "route not found"}})

    def _method_not_allowed(self) -> None:
        self._send(405, {"error": {"code": "method_not_allowed", "message": "method not allowed"}})

    do_DELETE = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_PUT = _method_not_allowed


def create_server(
    store: RolloutJudgeStore,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
    service_tokens: dict[str, str],
) -> RolloutJudgeHTTPServer:
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65_535:
        raise ValidationError("port must be between 0 and 65535")
    return RolloutJudgeHTTPServer((host, port), store, service_tokens)
