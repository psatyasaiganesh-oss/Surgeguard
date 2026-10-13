"""Exercise create, causal update, and read against the local HTTP server."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request


def request(base: str, method: str, path: str, payload: dict | None = None) -> dict:
    body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    headers = {} if body is None else {"Content-Type": "application/json"}
    call = urllib.request.Request(base + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(call, timeout=3) as response:
            return json.loads(response.read())
    except urllib.error.URLError as exc:
        raise SystemExit(f"request failed: {exc.reason}") from exc


def main() -> int:
    if len(sys.argv) != 2 or not sys.argv[1].startswith("http://127.0.0.1:"):
        raise SystemExit("usage: python -m scripts.client_demo http://127.0.0.1:4343")
    base = sys.argv[1].rstrip("/")
    created = request(base, "PUT", "/kv/cart:demo?coordinator=n1", {"value": '{"items":["book"]}'})
    read = request(base, "GET", "/kv/cart:demo?coordinator=n2")
    updated = request(
        base,
        "PUT",
        "/kv/cart:demo?coordinator=n3",
        {"value": '{"items":["book","pen"]}', "context": read["context"]},
    )
    final = request(base, "GET", "/kv/cart:demo?coordinator=n4")
    print(json.dumps({"created": created, "updated": updated, "final": final}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
