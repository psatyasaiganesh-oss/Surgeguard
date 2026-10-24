"""Command-line interface."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .api import build_server
from .coordinator import Coordinator
from .demo import run_benchmark, run_demo
from .errors import ChronoCommitError, ValidationError


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="chronocommit")
    subcommands = value.add_subparsers(dest="command", required=True)
    demo = subcommands.add_parser("demo", help="run deterministic failure scenarios")
    demo.add_argument("--directory", help="retain the demo databases in this directory")
    benchmark = subcommands.add_parser("benchmark", help="run a local synthetic benchmark")
    benchmark.add_argument("--transactions", type=int, default=200)
    verify = subcommands.add_parser("verify", help="verify an existing database directory")
    verify.add_argument("directory")
    verify.add_argument("--shards", type=int, default=3)
    serve = subcommands.add_parser("serve", help="serve the authenticated loopback API")
    serve.add_argument("directory")
    serve.add_argument("--tenant", default="demo")
    serve.add_argument("--shards", type=int, default=3)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return value


def _secret() -> str:
    value = os.environ.get("CHRONOCOMMIT_SECRET", "")
    if len(value.encode("utf-8")) < 32:
        raise ValidationError("CHRONOCOMMIT_SECRET must contain at least 32 UTF-8 bytes")
    return value


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "demo":
            result = run_demo(args.directory)
        elif args.command == "benchmark":
            result = run_benchmark(args.transactions)
        elif args.command == "verify":
            coordinator = Coordinator(
                args.directory, shard_count=args.shards, secret=_secret()
            )
            result = coordinator.verify_integrity()
        elif args.command == "serve":
            token = os.environ.get("CHRONOCOMMIT_API_TOKEN", "")
            coordinator = Coordinator(
                args.directory, shard_count=args.shards, secret=_secret()
            )
            server = build_server(
                args.host,
                args.port,
                coordinator=coordinator,
                tenant=args.tenant,
                api_token=token,
            )
            print(
                json.dumps(
                    {"host": args.host, "port": server.server_address[1], "status": "ready"}
                ),
                flush=True,
            )
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
        else:  # pragma: no cover - argparse enforces commands
            raise AssertionError(args.command)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (ChronoCommitError, ValueError) as exc:
        print(json.dumps({"error": exc.__class__.__name__, "message": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

