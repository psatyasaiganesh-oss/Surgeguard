from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .api import serve
from .demo import run_benchmark, run_demo
from .errors import ClearFlowError, ValidationError
from .store import ClearFlowStore


def signing_secret() -> bytes:
    value = os.environ.get("CLEARFLOW_SIGNING_SECRET")
    if value is None:
        raise ValidationError("CLEARFLOW_SIGNING_SECRET is required")
    return value.encode()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="clearflow")
    commands = result.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run deterministic correctness scenarios")
    demo.add_argument("--database", type=Path)
    benchmark = commands.add_parser("benchmark", help="run a controlled local benchmark")
    benchmark.add_argument("--flows", type=int, default=200)
    verify = commands.add_parser("verify", help="verify stored hashes and signatures")
    verify.add_argument("database", type=Path)
    verify.add_argument("--tenant")
    server = commands.add_parser("serve", help="serve the loopback HTTP API")
    server.add_argument("database", type=Path)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8080)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "demo":
            output = run_demo(args.database)
        elif args.command == "benchmark":
            output = run_benchmark(args.flows)
        elif args.command == "verify":
            output = ClearFlowStore(args.database, secret=signing_secret()).verify_integrity(
                args.tenant
            )
        elif args.command == "serve":
            raw_tokens = os.environ.get("CLEARFLOW_TENANT_TOKENS")
            if raw_tokens is None:
                raise ValidationError("CLEARFLOW_TENANT_TOKENS is required")
            try:
                tokens = json.loads(raw_tokens)
            except json.JSONDecodeError as exc:
                raise ValidationError("CLEARFLOW_TENANT_TOKENS must be a JSON object") from exc
            if not isinstance(tokens, dict):
                raise ValidationError("CLEARFLOW_TENANT_TOKENS must be a JSON object")
            serve(
                ClearFlowStore(args.database, secret=signing_secret()),
                tokens,
                host=args.host,
                port=args.port,
            )
            return 0
        else:
            raise AssertionError("unreachable")
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except ClearFlowError as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
