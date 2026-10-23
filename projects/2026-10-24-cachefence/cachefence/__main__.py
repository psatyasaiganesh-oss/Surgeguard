"""Command-line interface for CacheFence."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .cache import CacheStore
from .api import build_server
from .demo import run_demo
from .origin import OriginStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cache consistency race laboratory")
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo = subparsers.add_parser("demo", help="run all scenarios and the benchmark")
    demo.add_argument("--workdir", type=Path, help="retain SQLite databases here")
    verify = subparsers.add_parser("verify", help="verify database data and audit chains")
    verify.add_argument("--origin-db", type=Path, required=True)
    verify.add_argument("--cache-db", type=Path, required=True)
    verify.add_argument("--origin-id", default="origin-a")
    verify.add_argument("--cache-id", default="cache-a")
    serve = subparsers.add_parser("serve", help="run the loopback-only tenant API")
    serve.add_argument("--state-dir", type=Path, required=True)
    serve.add_argument("--tenant", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "demo":
        result = run_demo(args.workdir)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["passed"] else 1
    if args.command == "verify":
        origin = OriginStore(args.origin_db, origin_id=args.origin_id)
        cache = CacheStore(args.cache_db, cache_id=args.cache_id, origin_id=args.origin_id)
        print(json.dumps({"origin": origin.verify(), "cache": cache.verify()}, indent=2, sort_keys=True))
        return 0
    token = os.environ.get("CACHEFENCE_API_TOKEN", "")
    signing_key = os.environ.get("CACHEFENCE_SIGNING_KEY", "").encode("utf-8")
    args.state_dir.mkdir(parents=True, exist_ok=True)
    origin = OriginStore(args.state_dir / "origin.db")
    cache = CacheStore(args.state_dir / "cache.db")
    server = build_server(
        args.host,
        args.port,
        origin=origin,
        cache=cache,
        tenant=args.tenant,
        api_token=token,
        signing_secret=signing_key,
    )
    print(f"CacheFence listening on http://{args.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
