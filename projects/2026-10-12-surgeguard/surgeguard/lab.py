"""Loopback-only failure injection; real TCP, synthetic catalog responses."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from urllib.parse import parse_qs, urlsplit


class FaultServer:
    def __init__(self, mode: str = "healthy", delay: float = .002):
        if mode not in {"healthy", "outage", "transient", "slow"}:
            raise ValueError("Unknown fault mode")
        if not 0 <= delay <= 10:
            raise ValueError("delay must be between 0 and 10 seconds")
        self.mode = mode
        self.delay = delay
        self.attempts = 0
        self.by_status = Counter()
        self.by_id = Counter()
        self.active = 0
        self.max_active = 0
        self._server = None
        self._tasks = set()

    async def start(self, port: int = 0):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", port, limit=16384)
        self.port = self._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/work"
        return self

    async def close(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.close()

    def snapshot(self):
        return {"attempts_received": self.attempts, "responses": {str(key): value for key, value in self.by_status.items()}, "max_concurrent_handlers": self.max_active}

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._tasks.add(task)
        counted = False
        try:
            raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            first = raw.split(b"\r\n", 1)[0].decode("ascii")
            method, path, _ = first.split(" ", 2)
            parts = urlsplit(path)
            if method not in {"GET", "HEAD"}:
                code, data = 405, {"error": "read-only lab"}
            elif parts.path == "/metrics":
                code, data = 200, self.snapshot()
            elif parts.path != "/work":
                code, data = 404, {"error": "not found"}
            else:
                identifier = parse_qs(parts.query).get("id", ["default"])[0][:80]
                self.attempts += 1
                self.by_id[identifier] += 1
                ordinal = self.by_id[identifier]
                self.active += 1
                counted = True
                self.max_active = max(self.max_active, self.active)
                await asyncio.sleep(self.delay)
                if self.mode == "outage" or (self.mode == "transient" and ordinal == 1):
                    code, data = 503, {"error": "synthetic dependency unavailable"}
                else:
                    code, data = 200, {"item": identifier, "price_cents": 1200, "synthetic": True}
                self.by_status[code] += 1
            payload = b"" if method == "HEAD" else json.dumps(data).encode()
            writer.write(f"HTTP/1.1 {code} Lab\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode() + payload)
            await writer.drain()
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, UnicodeError, ValueError, ConnectionError):
            pass
        finally:
            if counted:
                self.active -= 1
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self._tasks.discard(task)
