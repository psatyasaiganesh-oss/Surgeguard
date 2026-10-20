"""Loopback-only JSON API for RouteProof."""

from __future__ import annotations

import json
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .control import AuthorizationError, ConflictError, Controller, NotFoundError
from .model import MAX_JSON_BYTES, ValidationError, strict_json

MAX_HEADER_VALUES = 1


class RouteProofHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET
    daemon_threads = True
    request_queue_size = 32
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], controller: Controller):
        super().__init__(address, RouteProofHandler)
        self.controller = controller


class RouteProofHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    server_version = "RouteProof"
    sys_version = ""

    @property
    def controller(self) -> Controller:
        return self.server.controller  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _reply(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def _token(self) -> str:
        values = self.headers.get_all("Authorization") or []
        if len(values) != MAX_HEADER_VALUES or not values[0].startswith("Bearer "):
            raise AuthorizationError("a single Bearer authorization header is required")
        token = values[0][7:]
        if not token or len(token) > 256:
            raise AuthorizationError("invalid bearer token")
        return token

    def _body(self) -> Any:
        if self.headers.get_all("Transfer-Encoding"):
            raise ValidationError("transfer encoding is not accepted")
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 or not lengths[0].isdigit():
            raise ValidationError("a single numeric Content-Length is required")
        length = int(lengths[0])
        if length > MAX_JSON_BYTES:
            raise ValidationError("request body is too large")
        media_type = (self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            raise ValidationError("Content-Type must be application/json")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ValidationError("truncated request body")
        return strict_json(raw)

    @staticmethod
    def _empty(body: Any) -> None:
        if body != {}:
            raise ValidationError("request body must be an empty object")

    def _dispatch(self) -> tuple[int, Any]:
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise ValidationError("query strings and fragments are not accepted")
        parts = [part for part in parsed.path.split("/") if part]

        if self.command == "GET" and parts == ["healthz"]:
            return 200, self.controller.health()

        token = self._token()
        if self.command == "GET" and parts == ["v1", "audit"]:
            return 200, {"events": self.controller.audit_log(token)}
        if self.command == "GET" and len(parts) == 3 and parts[:2] == ["v1", "plans"]:
            return 200, self.controller.status(token, parts[2])
        if self.command != "POST":
            raise NotFoundError("route not found")

        body = self._body()
        if parts == ["v1", "plans"]:
            return 201, self.controller.create_plan(token, body)
        if len(parts) != 4 or parts[:2] != ["v1", "plans"]:
            raise NotFoundError("route not found")
        plan_id, action = parts[2], parts[3]
        if action == "analyze":
            self._empty(body)
            return 200, self.controller.analyze_plan(token, plan_id)
        if action == "approve":
            self._empty(body)
            return 200, self.controller.approve(token, plan_id)
        if action == "execute":
            self._empty(body)
            return 200, self.controller.execute_next(token, plan_id)
        if action == "rollback":
            self._empty(body)
            return 200, self.controller.rollback(token, plan_id)
        if action == "observe":
            if not isinstance(body, dict) or set(body) != {"healthy", "evidence"}:
                raise ValidationError("observe requires exactly healthy and evidence")
            return 200, self.controller.observe(
                token, plan_id, body["healthy"], body["evidence"]
            )
        raise NotFoundError("route not found")

    def _handle(self) -> None:
        try:
            status, payload = self._dispatch()
            self._reply(status, payload)
        except AuthorizationError as exc:
            self._reply(401, {"error": "unauthorized", "message": str(exc)})
        except NotFoundError as exc:
            self._reply(404, {"error": "not_found", "message": str(exc)})
        except ValidationError as exc:
            self._reply(400, {"error": "invalid_request", "message": str(exc)})
        except ConflictError as exc:
            self._reply(409, {"error": "conflict", "message": str(exc)})
        except Exception:
            self._reply(500, {"error": "internal_error", "message": "request failed"})

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._reply(405, {"error": "method_not_allowed"})

    def do_DELETE(self) -> None:
        self._reply(405, {"error": "method_not_allowed"})


def create_server(controller: Controller, host: str = "127.0.0.1", port: int = 8080) -> RouteProofHTTPServer:
    if host != "127.0.0.1":
        raise ValueError("RouteProof's reference API binds only to 127.0.0.1")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError("port must be in [0, 65535]")
    server = RouteProofHTTPServer((host, port), controller)
    server.socket.settimeout(5.0)
    return server
