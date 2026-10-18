"""Loopback-only JSON API. Identity/tenant come only from signed access tokens."""

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .model import Problem, canonical, fields, parse_json, require


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, engine, host="127.0.0.1", port=0, *, max_workers=16):
        require(host == "127.0.0.1", "only IPv4 loopback binding is supported")
        require(type(max_workers) is int and 1 <= max_workers <= 64, "invalid worker limit")
        self.engine = engine
        self.slots = threading.BoundedSemaphore(max_workers)
        super().__init__((host, port), Handler)

    def get_request(self):
        conn, address = super().get_request()
        conn.settimeout(5)
        return conn, address

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n"
                                b"Connection: close\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()

    def handle_error(self, request, client_address):
        # No stack traces containing request headers or protected content.
        pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    server_version = "ScopeGraph"
    sys_version = ""

    def log_message(self, *_):
        pass

    def _send(self, status, data):
        raw = canonical(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)
        self.close_connection = True

    def send_error(self, code, message=None, explain=None):
        self._send(code, {"error": "http_error", "message": "request rejected"})

    def do_GET(self):
        if self.path == "/healthz":
            self._send(200, {"status": "ok", "service": "scopegraph"})
        else:
            self._send(404, {"error": "not_found"})

    def do_POST(self):
        try:
            require(self.path in ("/v1/check", "/v1/relationships", "/v1/read", "/v1/publish"),
                    "unknown route", "not_found", 404)
            require(not self.headers.get_all("Transfer-Encoding"), "transfer encoding is not supported")
            lengths = self.headers.get_all("Content-Length", [])
            require(len(lengths) == 1 and len(lengths[0]) <= 10 and lengths[0].isascii()
                    and lengths[0].isdigit(),
                    "one Content-Length is required", "invalid_framing", 400)
            length = int(lengths[0])
            require(0 < length <= 65536, "body must be 1..65536 bytes", "body_size", 413)
            types = self.headers.get_all("Content-Type", [])
            require(len(types) == 1 and types[0].lower().strip() in
                    ("application/json", "application/json; charset=utf-8"),
                    "application/json is required", "media_type", 415)
            headers = self.headers.get_all("Authorization", [])
            require(len(headers) == 1 and headers[0].startswith("Bearer "),
                    "bearer token required", "invalid_token", 401)
            actor = self.server.engine.tokens.authenticate(headers[0][7:])
            raw = self.rfile.read(length)
            require(len(raw) == length, "incomplete request body")
            body = parse_json(raw)
            engine, tenant, user = self.server.engine, actor["tenant"], actor["sub"]
            if self.path == "/v1/relationships":
                require(actor["role"] == "admin", "administrator token required", "forbidden", 403)
                fields(body, ("operations", "request_id"), ("expected_revision",))
                result = engine.mutate(tenant, **body)
            elif self.path == "/v1/check":
                fields(body, ("resource", "permission"), ("min_token",))
                result = engine.check(tenant, body["resource"], body["permission"], user,
                                      body.get("min_token"))
            elif self.path == "/v1/read":
                fields(body, ("resource",), ("min_token",))
                result = engine.read_content(tenant, body["resource"], user, body.get("min_token"))
            else:
                fields(body, ("resource", "text", "request_id", "expected_version"), ("min_token",))
                result = engine.publish(tenant, user=user, **body)
            self._send(200, result)
        except Problem as exc:
            self._send(exc.status, {"error": exc.code, "message": exc.message})
        except (socket.timeout, TimeoutError):
            self._send(408, {"error": "timeout", "message": "request timed out"})
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception:
            self._send(500, {"error": "internal", "message": "request could not be completed"})
