from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .api import serve
from .demo import run_benchmark, run_demo
from .errors import TabletForgeError, ValidationError
from .store import TabletForge


def signing_secret() -> bytes:
    value = os.environ.get("TABLETFORGE_SIGNING_SECRET")
    if value is None:
        raise ValidationError("TABLETFORGE_SIGNING_SECRET is required")
    return value.encode()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="tabletforge")
    commands = result.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run deterministic crash and compaction scenarios")
    demo.add_argument("--directory", type=Path)
    benchmark = commands.add_parser("benchmark", help="run a controlled local benchmark")
    benchmark.add_argument("--entries", type=int, default=3_000)
    benchmark.add_argument("--reads", type=int, default=1_000)
    verify = commands.add_parser("verify", help="verify a stored tablet")
    verify.add_argument("directory", type=Path)
    server = commands.add_parser("serve", help="serve the loopback HTTP API")
    server.add_argument("directory", type=Path)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8080)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "demo":
            output = run_demo(args.directory)
        elif args.command == "benchmark":
            output = run_benchmark(args.entries, args.reads)
        elif args.command == "verify":
            with TabletForge(args.directory, secret=signing_secret()) as store:
                output = store.verify_integrity()
        elif args.command == "serve":
            token = os.environ.get("TABLETFORGE_BEARER_TOKEN")
            if token is None:
                raise ValidationError("TABLETFORGE_BEARER_TOKEN is required")
            with TabletForge(args.directory, secret=signing_secret()) as store:
                serve(store, token, host=args.host, port=args.port)
            return 0
        else:
            raise AssertionError("unreachable")
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except TabletForgeError as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
