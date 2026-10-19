"""Bounded loopback JSON collector. Opaque local keys bind service and producer."""

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .model import Problem, canonical, fields, parse_json, require


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, engine, host="127.0.0.1", port=0, *, workers=16):
        require(host == "127.0.0.1", "only IPv4 loopback binding is supported")
        require(type(workers) is int and 1 <= workers <= 64, "invalid worker count")
        self.engine = engine
        self.slots = threading.BoundedSemaphore(workers)
        super().__init__((host, port), Handler)

    def get_request(self):
        conn, address = super().get_request()
        conn.settimeout(5)
        return conn, address

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
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
        pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    server_version = "BurnWatch"
    sys_version = ""

    def log_message(self, *_):
        pass

    def respond(self, status, value, metrics=False):
        raw = (value if metrics else canonical(value)).encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8" if metrics else "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)
        self.close_connection = True

    def send_error(self, code, message=None, explain=None):
        self.respond(code, {"error": "http_error", "message": "request rejected"})

    def actor(self):
        headers = self.headers.get_all("Authorization", [])
        require(len(headers) == 1 and headers[0].startswith("Bearer "), "bearer key required", "unauthorized", 401)
        return self.server.engine.authenticate(headers[0][7:])

    def handle_call(self, method):
        try:
            engine = self.server.engine
            if method == "GET" and self.path == "/healthz":
                self.respond(200, {"status": "ok", "service": "burnwatch"})
                return
            routes = ("/v1/status", "/metrics") if method == "GET" else ("/v1/buckets", "/v1/evaluate")
            require(self.path in routes, "route not found", "not_found", 404)
            actor = self.actor()
            if method == "GET":
                result = engine.metrics(actor["service"]) if self.path == "/metrics" else engine.snapshot(actor["service"])
            else:
                require(not self.headers.get_all("Transfer-Encoding"), "transfer encoding is unsupported")
                lengths = self.headers.get_all("Content-Length", [])
                require(len(lengths) == 1 and len(lengths[0]) <= 10 and lengths[0].isascii()
                        and lengths[0].isdigit(), "one bounded Content-Length is required")
                length = int(lengths[0])
                require(0 < length <= 65536, "body exceeds limits", "body_size", 413)
                types = self.headers.get_all("Content-Type", [])
                require(len(types) == 1 and types[0].lower().strip() in
                        ("application/json", "application/json; charset=utf-8"), "JSON required", "media_type", 415)
                raw = self.rfile.read(length)
                require(len(raw) == length, "truncated body")
                body = parse_json(raw)
                if self.path == "/v1/evaluate":
                    fields(body, ())
                    result = engine.evaluate(actor["service"])
                else:
                    fields(body, ("batch_id", "buckets"), ("watermark",))
                    result = engine.ingest(actor["service"], actor["producer"], **body)
            self.respond(200, result, self.path == "/metrics")
        except Problem as exc:
            self.respond(exc.status, {"error": exc.code, "message": exc.message})
        except (socket.timeout, TimeoutError):
            self.respond(408, {"error": "timeout"})
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception:
            self.respond(500, {"error": "internal", "message": "request could not be completed"})

    def do_GET(self):
        self.handle_call("GET")

    def do_POST(self):
        self.handle_call("POST")
