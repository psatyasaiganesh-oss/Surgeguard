from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from .api import build_server
from .errors import ExitPackError, ValidationError
from .simulation import run_benchmark, run_demo
from .store import ExitPack


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))


def _secret() -> bytes:
    value = os.environ.get("EXITPACK_SIGNING_SECRET", "").encode()
    if len(value) < 32:
        raise ValidationError("EXITPACK_SIGNING_SECRET must contain at least 32 UTF-8 bytes")
    return value


def _tokens() -> tuple[str, ...]:
    values = tuple(
        token.strip()
        for token in os.environ.get("EXITPACK_API_TOKENS", "").split(",")
        if token.strip()
    )
    if not values:
        raise ValidationError("EXITPACK_API_TOKENS must contain at least one token")
    return values


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="exitpack", description="Lifetime-aware VM placement control-plane lab"
    )
    commands = root.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run deterministic correctness scenarios")
    demo.add_argument("--db", type=Path, help="persist to a new database")
    benchmark = commands.add_parser("benchmark", help="compare placement policies")
    benchmark.add_argument("--hosts", type=int, default=40)
    benchmark.add_argument("--groups", type=int, default=32)
    verify = commands.add_parser("verify", help="verify SQLite and audit integrity")
    verify.add_argument("--db", type=Path, required=True)
    verify.add_argument("--namespace")
    serve = commands.add_parser("serve", help="start the authenticated HTTP API")
    serve.add_argument("--db", type=Path, required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "demo":
            _print(run_demo(args.db))
            return 0
        if args.command == "benchmark":
            _print(run_benchmark(hosts=args.hosts, groups=args.groups))
            return 0
        if args.command == "verify":
            with ExitPack(args.db, secret=_secret()) as store:
                _print(store.verify_integrity(args.namespace))
            return 0
        if args.command == "serve":
            if not 1 <= args.port <= 65_535:
                raise ValidationError("port must be between 1 and 65535")
            store = ExitPack(args.db, secret=_secret())
            server = build_server(args.host, args.port, store, _tokens())
            print(f"ExitPack listening on http://{args.host}:{args.port}")
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
                store.close()
            return 0
    except (ExitPackError, FileExistsError, ValueError) as exc:
        parser().error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
