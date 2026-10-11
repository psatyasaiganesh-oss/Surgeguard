"""A deliberately narrow HTTP/1.1 transport for a credential-free local lab.

    Production integrations should adapt a mature HTTP library to the same
    async interface. This parser never follows redirects or retries itself.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import ssl
from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


class TransportError(Exception):
    """Invalid or unsupported protocol; repeating the request is not useful."""


def validate_url(url: str, *, allow_network: bool = False):
    if not isinstance(url, str) or not url or len(url) > 4096:
        raise ValueError("URL must be a nonempty string of at most 4096 characters")
    if any(ord(char) <= 32 or ord(char) >= 127 for char in url):
        raise ValueError("URL must be ASCII and contain no whitespace or control characters")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError("Only absolute http/https URLs are supported")
    if parts.username is not None or parts.password is not None or parts.fragment:
        raise ValueError("Credentials and fragments are not allowed in URLs")
    try:
        port = parts.port if parts.port is not None else (443 if parts.scheme == "https" else 80)
    except ValueError as exc:
        raise ValueError("Invalid URL port") from exc
    if not 1 <= port <= 65535:
        raise ValueError("Invalid URL port")
    if not allow_network:
        try:
            address = ipaddress.ip_address(parts.hostname)
        except ValueError as exc:
            raise ValueError("Default transport requires a literal loopback IP (127.0.0.1 or ::1)") from exc
        if not address.is_loopback:
            raise ValueError("Non-loopback destinations require explicit allow_network=True")
    return parts, port


class HttpTransport:
    def __init__(self, *, allow_network: bool = False, max_body_bytes: int = 65536):
        if type(max_body_bytes) is not int or not 1 <= max_body_bytes <= 16 * 1024 * 1024:
            raise ValueError("max_body_bytes must be an integer in [1, 16777216]")
        self.allow_network = allow_network
        self.max_body_bytes = max_body_bytes

    def validate(self, url: str) -> None:
        validate_url(url, allow_network=self.allow_network)

    async def __call__(self, url: str, method: str = "GET") -> Response:
        if method not in {"GET", "HEAD"}:
            raise ValueError("Only read-only GET and HEAD methods are supported")
        parts, port = validate_url(url, allow_network=self.allow_network)
        host = parts.hostname
        context = ssl.create_default_context() if parts.scheme == "https" else None
        reader, writer = await asyncio.open_connection(host, port, ssl=context, limit=16384)
        try:
            host_header = f"[{host}]" if ":" in host else host
            path = parts.path or "/"
            if parts.query:
                path += "?" + parts.query
            request = f"{method} {path} HTTP/1.1\r\nHost: {host_header}:{port}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
            writer.write(request.encode("ascii"))
            await writer.drain()
            try:
                raw = await reader.readuntil(b"\r\n\r\n")
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
                raise TransportError("Missing or oversized HTTP headers") from exc
            if len(raw) > 16384:
                raise TransportError("HTTP headers exceed 16 KiB")
            lines = raw[:-4].decode("iso-8859-1").split("\r\n")
            status = re.fullmatch(r"HTTP/1\.[01] ([1-5][0-9]{2})(?: [^\r\n]*)?", lines[0])
            if not status:
                raise TransportError("Malformed HTTP status line")
            headers = {}
            for line in lines[1:]:
                key, colon, value = line.partition(":")
                if not colon or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key):
                    raise TransportError("Malformed HTTP header")
                key = key.lower()
                if key in headers:
                    raise TransportError("Duplicate HTTP headers are unsupported")
                headers[key] = value.strip()
            if "transfer-encoding" in headers:
                raise TransportError("Transfer-Encoding is unsupported; use a mature adapter for chunked HTTP")
            code = int(status.group(1))
            if method == "HEAD" or code in {204, 304}:
                body = b""
            elif "content-length" in headers:
                if not re.fullmatch(r"[0-9]+", headers["content-length"]):
                    raise TransportError("Invalid Content-Length")
                if len(headers["content-length"]) > 9:
                    raise TransportError("Response exceeds configured body limit")
                length = int(headers["content-length"])
                if length > self.max_body_bytes:
                    raise TransportError("Response exceeds configured body limit")
                try:
                    body = await reader.readexactly(length)
                except asyncio.IncompleteReadError as exc:
                    raise TransportError("Truncated HTTP response body") from exc
            else:
                # Read until EOF, capped even when the peer omits Content-Length.
                chunks = []
                size = 0
                while True:
                    chunk = await reader.read(min(8192, self.max_body_bytes + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > self.max_body_bytes:
                        raise TransportError("Response exceeds configured body limit")
                    chunks.append(chunk)
                body = b"".join(chunks)
            return Response(code, headers, body)
        finally:
            # close() initiates shutdown without extending the caller's deadline.
            writer.close()
