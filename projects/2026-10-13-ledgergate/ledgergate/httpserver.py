"""A loopback-first webhook endpoint with an asynchronous worker."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .schema import MAX_PAYLOAD_BYTES
from .service import LedgerGate


class Worker(threading.Thread):
    def __init__(self, service: LedgerGate, interval: float = 0.01):
        super().__init__(name="ledgergate-worker", daemon=True)
        if not 0.001 <= interval <= 10:
            raise ValueError("worker interval must be in [0.001, 10]")
        self.service = service
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.is_set():
            did_work = self.service.store.process_one() is not None
            did_work = (self.service.store.dispatch_one(lease_seconds=0) is not None) or did_work
            if not did_work:
                self.stop_event.wait(self.interval)

    def stop(self) -> None:
        self.stop_event.set()


class LedgerGateHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, service: LedgerGate):
        self.service = service
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    server: LedgerGateHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):  # noqa: A002 - inherited API name
        return

    def _json(self, status: int, data: dict) -> None:
        body = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"status": "ok"})
        elif self.path == "/metrics":
            self._json(200, self.server.service.store.snapshot())
        else:
            self._json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path != "/webhook":
            self._json(404, {"error": "not_found"})
            return
        if self.headers.get("Transfer-Encoding"):
            self.close_connection = True
            self._json(400, {"error": "transfer_encoding_not_supported"})
            return
        value = self.headers.get("Content-Length")
        if value is None or not value.isascii() or not value.isdigit() or len(value) > 9:
            self.close_connection = True
            self._json(411, {"error": "valid_content_length_required"})
            return
        length = int(value)
        if not 1 <= length <= MAX_PAYLOAD_BYTES:
            self.close_connection = True
            self._json(413, {"error": "payload_size_out_of_range"})
            return
        raw = self.rfile.read(length)
        if len(raw) != length:
            self.close_connection = True
            return
        signature = self.headers.get("Stripe-Signature", "")
        receipt = self.server.service.accept(raw, signature, now=int(time.time()))
        self._json(receipt.http_status, receipt.to_dict())


class ServerRuntime:
    def __init__(self, service: LedgerGate, *, host: str = "127.0.0.1", port: int = 0):
        if host not in {"127.0.0.1", "::1"}:
            raise ValueError("this reference server binds only to a literal loopback address")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("port must be an integer in [0, 65535]")
        self.server = LedgerGateHTTPServer((host, port), service)
        self.worker = Worker(service)
        self.server_thread = threading.Thread(target=self.server.serve_forever, name="ledgergate-http", daemon=True)

    @property
    def address(self) -> tuple[str, int]:
        return self.server.server_address[:2]

    def start(self):
        self.worker.start()
        self.server_thread.start()
        return self

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.stop()
        self.worker.join(timeout=5)
        self.server_thread.join(timeout=5)
        if self.worker.is_alive() or self.server_thread.is_alive():
            raise RuntimeError("server threads did not stop cleanly")

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()
