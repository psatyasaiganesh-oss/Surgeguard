import asyncio
import unittest

from surgeguard import HttpTransport, Policy, ResilientClient
from surgeguard.demo import demo
from surgeguard.lab import FaultServer
from surgeguard.transport import TransportError


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def raw_server(self, payload):
        async def handle(reader, writer):
            try:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(payload)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        return f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"

    async def test_real_tcp_transient_recovery(self):
        async with FaultServer("transient") as server:
            result = await ResilientClient().fetch(server.url + "?id=test")
            self.assertTrue(result.ok)
            self.assertEqual(result.attempts, 2)
            self.assertEqual(server.attempts, 2)
            self.assertIn(b'"synthetic": true', result.response.body)

    async def test_head_request_returns_no_body(self):
        async with FaultServer() as server:
            result = await ResilientClient().fetch(server.url, method="HEAD")
            self.assertTrue(result.ok)
            self.assertEqual(result.response.body, b"")

    async def test_rejects_oversized_content_length(self):
        url = await self.raw_server(b"HTTP/1.1 200 OK\r\nContent-Length: 999999\r\n\r\n")
        result = await ResilientClient().fetch(url)
        self.assertEqual(result.reason, "protocol_error")
        self.assertEqual(result.attempts, 1)

    async def test_rejects_truncated_body(self):
        url = await self.raw_server(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
        with self.assertRaises(TransportError):
            await HttpTransport()(url)

    async def test_rejects_duplicate_framing_and_chunked_encoding(self):
        for payload in (b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n", b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", b"BAD STATUS\r\n\r\n"):
            url = await self.raw_server(payload)
            with self.assertRaises(TransportError):
                await HttpTransport()(url)

    async def test_body_without_content_length_is_capped(self):
        url = await self.raw_server(b"HTTP/1.1 200 OK\r\n\r\n" + b"x" * 33)
        with self.assertRaises(TransportError):
            await HttpTransport(max_body_bytes=32)(url)

    async def test_header_limit_rejects_response(self):
        url = await self.raw_server(b"HTTP/1.1 200 OK\r\nX-Long: " + b"x" * 20000 + b"\r\n\r\n")
        with self.assertRaises(TransportError):
            await HttpTransport()(url)

    async def test_deadline_includes_receiving_response_body(self):
        async def slow_body(reader, writer):
            try:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                await writer.drain()
                await asyncio.sleep(.1)
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(slow_body, "127.0.0.1", 0)
        async with server:
            url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
            result = await ResilientClient(Policy(total_timeout=.02, attempt_timeout=.2)).fetch(url)
            self.assertEqual(result.reason, "deadline_exhausted")
            self.assertLess(result.elapsed_s, .15)
            await asyncio.sleep(.12)

    async def test_complete_demo_invariants(self):
        report = await demo(count=16, concurrency=8)
        self.assertTrue(all(report["invariants"].values()))
        self.assertEqual(report["persistent_outage"]["unbounded_retry_baseline"]["backend"]["attempts_received"], 64)
        self.assertEqual(report["persistent_outage"]["surgeguard"]["backend"]["attempts_received"], 28)


if __name__ == "__main__":
    unittest.main()
