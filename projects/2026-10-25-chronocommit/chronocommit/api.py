"""Minimal authenticated HTTP adapter for the local transaction service."""

from __future__ import annotations

import hmac
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .coordinator import Coordinator
from .errors import ChronoCommitError, IntegrityError, ValidationError
from .service import AccountService


MAX_BODY_BYTES = 65_536


def _strict_json(raw: bytes) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValidationError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("request body must contain valid UTF-8 JSON") from exc


def _require_keys(
    body: dict[str, Any], *, required: set[str], optional: set[str] | None = None
) -> None:
    optional = optional or set()
    missing = required - set(body)
    extra = set(body) - required - optional
    if missing:
        raise ValidationError(f"missing fields: {', '.join(sorted(missing))}")
    if extra:
        raise ValidationError(f"unexpected fields: {', '.join(sorted(extra))}")


class ChronoCommitHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        coordinator: Coordinator,
        tenant: str,
        api_token: str,
    ) -> None:
        if not isinstance(api_token, str) or len(api_token.encode("utf-8")) < 32:
            raise ValidationError("API token must contain at least 32 UTF-8 bytes")
        from .model import validate_id

        self.coordinator = coordinator
        self.service = AccountService(coordinator)
        self.tenant = validate_id(tenant, "tenant")
        self.api_token = api_token
        super().__init__(address, ChronoCommitHandler)


class ChronoCommitHandler(BaseHTTPRequestHandler):
    server: ChronoCommitHTTPServer
    protocol_version = "HTTP/1.1"
    server_version = "ChronoCommit"
    sys_version = ""

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        value = self.headers.get("Authorization", "")
        supplied = value.removeprefix("Bearer ") if value.startswith("Bearer ") else ""
        return hmac.compare_digest(supplied, self.server.api_token)

    def _require_auth(self) -> bool:
        if self._authorized():
            return True
        self._send(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
        return False

    def _body(self) -> dict[str, Any]:
        if self.headers.get("Transfer-Encoding"):
            raise ValidationError("chunked request bodies are not supported")
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValidationError("Content-Type must be application/json")
        length_text = self.headers.get("Content-Length")
        if length_text is None or not length_text.isdigit():
            raise ValidationError("a numeric Content-Length is required")
        length = int(length_text)
        if length > MAX_BODY_BYTES:
            raise ValidationError(f"request body exceeds {MAX_BODY_BYTES} bytes")
        value = _strict_json(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValidationError("request body must be a JSON object")
        return value

    def _handle_error(self, error: Exception) -> None:
        if isinstance(error, ChronoCommitError):
            message = str(error) if error.status < 500 else "service integrity check failed"
            self._send(error.status, {"error": error.code, "message": message})
        else:
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_error", "message": "unexpected server failure"},
            )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz":
            try:
                report = self.server.coordinator.verify_integrity()
                self._send(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "shards": len(report["shards"]),
                        "transactions": report["transactions"],
                    },
                )
            except IntegrityError:
                self._send(HTTPStatus.SERVICE_UNAVAILABLE, {"status": "unhealthy"})
            return
        if not self._require_auth():
            return
        try:
            if parsed.path == "/v1/accounts":
                query = parse_qs(parsed.query, keep_blank_values=True)
                if set(query) - {"ids", "read_ts"} or len(query.get("ids", [])) != 1:
                    raise ValidationError("query requires exactly one ids parameter")
                accounts = query["ids"][0].split(",")
                read_ts = None
                if "read_ts" in query:
                    if len(query["read_ts"]) != 1 or not query["read_ts"][0].isdigit():
                        raise ValidationError("read_ts must be a non-negative integer")
                    read_ts = int(query["read_ts"][0])
                result = self.server.service.get_accounts(
                    self.server.tenant, accounts, read_ts=read_ts
                )
                self._send(HTTPStatus.OK, {"accounts": result, "tenant": self.server.tenant})
                return
            prefix = "/v1/transactions/"
            if parsed.path.startswith(prefix) and not parsed.query:
                encoded = parsed.path.removeprefix(prefix)
                value = unquote(encoded, errors="strict")
                if not value or value != encoded or "/" in value:
                    raise ValidationError("invalid transaction identifier")
                status = self.server.coordinator.transaction_status(
                    self.server.tenant, value
                )
                if status is None:
                    self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                else:
                    self._send(HTTPStatus.OK, {"transaction": status})
                return
            self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except Exception as exc:
            self._handle_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        if not self._require_auth():
            return
        parsed = urlsplit(self.path)
        try:
            if parsed.query:
                raise ValidationError("POST routes do not accept query parameters")
            body = self._body()
            if parsed.path == "/v1/accounts":
                _require_keys(
                    body, required={"account", "balance_cents", "request_id"}
                )
                result = self.server.service.create_account(
                    self.server.tenant,
                    body["account"],
                    body["balance_cents"],
                    request_id=body["request_id"],
                )
                self._send(HTTPStatus.CREATED, {"commit": result.as_dict()})
                return
            if parsed.path == "/v1/transfers":
                _require_keys(
                    body,
                    required={
                        "amount_cents",
                        "destination",
                        "source",
                        "transfer_id",
                    },
                )
                result = self.server.service.transfer(
                    self.server.tenant,
                    body["transfer_id"],
                    body["source"],
                    body["destination"],
                    body["amount_cents"],
                )
                self._send(HTTPStatus.OK, result)
                return
            self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except Exception as exc:
            self._handle_error(exc)


def build_server(
    host: str,
    port: int,
    *,
    coordinator: Coordinator,
    tenant: str,
    api_token: str,
) -> ChronoCommitHTTPServer:
    if host not in {"127.0.0.1", "localhost"}:
        raise ValidationError("the lab HTTP server may bind only to loopback")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65_535:
        raise ValidationError("port must be between 0 and 65535")
    return ChronoCommitHTTPServer(
        (host, port), coordinator=coordinator, tenant=tenant, api_token=api_token
    )

