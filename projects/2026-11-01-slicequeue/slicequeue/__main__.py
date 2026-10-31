from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from .api import build_server
from .demo import run_benchmark, run_demo
from .errors import SliceQueueError, ValidationError
from .store import SliceQueue


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))


def _secret_from_env() -> bytes:
    value = os.environ.get("SLICEQUEUE_SIGNING_SECRET", "")
    secret = value.encode()
    if len(secret) < 32:
        raise ValidationError("SLICEQUEUE_SIGNING_SECRET must contain at least 32 UTF-8 bytes")
    return secret


def _tokens_from_env() -> tuple[str, ...]:
    values = tuple(
        token.strip()
        for token in os.environ.get("SLICEQUEUE_API_TOKENS", "").split(",")
        if token.strip()
    )
    if not values:
        raise ValidationError("SLICEQUEUE_API_TOKENS must contain at least one token")
    return values


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="slicequeue",
        description="Durable, event-driven bounded-slice agent runtime",
    )
    commands = root.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run the deterministic failure-recovery scenario")
    demo.add_argument("--db", type=Path, help="persist to a new database instead of a temporary one")

    benchmark = commands.add_parser("benchmark", help="run a local synthetic benchmark")
    benchmark.add_argument("--db", type=Path, help="persist to a new database")
    benchmark.add_argument("--threads", type=int, default=250)
    benchmark.add_argument("--events-per-thread", type=int, default=2)

    verify = commands.add_parser("verify", help="verify SQLite and HMAC hash-chain integrity")
    verify.add_argument("--db", type=Path, required=True)
    verify.add_argument("--namespace")

    serve = commands.add_parser("serve", help="start the authenticated local HTTP API")
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
            _print(
                run_benchmark(
                    args.db,
                    threads=args.threads,
                    events_per_thread=args.events_per_thread,
                )
            )
            return 0
        if args.command == "verify":
            with SliceQueue(args.db, secret=_secret_from_env()) as store:
                _print(store.verify_integrity(args.namespace))
            return 0
        if args.command == "serve":
            if not 1 <= args.port <= 65_535:
                raise ValidationError("port must be between 1 and 65535")
            store = SliceQueue(args.db, secret=_secret_from_env())
            server = build_server(args.host, args.port, store, _tokens_from_env())
            print(f"SliceQueue listening on http://{args.host}:{args.port}")
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
                store.close()
            return 0
    except (SliceQueueError, FileExistsError, ValueError) as exc:
        parser().error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
