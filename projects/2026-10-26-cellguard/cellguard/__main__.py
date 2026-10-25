"""CellGuard command-line interface."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from .api import create_server
from .demo import DEMO_SECRET, default_topology, run_benchmark, run_demo
from .errors import CellGuardError, ValidationError
from .store import CellGuardStore


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _serve(args: argparse.Namespace) -> None:
    lease_secret = os.environ.get("CELLGUARD_LEASE_SECRET", "")
    worker_token = os.environ.get("CELLGUARD_WORKER_TOKEN", "")
    raw_tokens = os.environ.get("CELLGUARD_TENANT_TOKENS", "")
    try:
        tenant_tokens = json.loads(raw_tokens)
    except json.JSONDecodeError as exc:
        raise ValidationError("CELLGUARD_TENANT_TOKENS must be a JSON object") from exc
    if not isinstance(tenant_tokens, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in tenant_tokens.items()
    ):
        raise ValidationError("CELLGUARD_TENANT_TOKENS must map tenant IDs to tokens")
    store = CellGuardStore(
        Path(args.database), default_topology(capacity=args.cell_capacity), secret=lease_secret
    )
    server = create_server(
        store,
        host=args.host,
        port=args.port,
        tenant_tokens=tenant_tokens,
        worker_token=worker_token,
    )
    print(f"CellGuard listening on http://{args.host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Shuffle-sharded multi-tenant queue lab")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run deterministic fault scenarios")
    demo.add_argument("--directory", help="persist the demo database in this directory")
    benchmark = commands.add_parser("benchmark", help="run a local synthetic benchmark")
    benchmark.add_argument("--jobs", type=int, default=500)
    benchmark.add_argument("--tenants", type=int, default=100)
    verify = commands.add_parser("verify", help="verify a persisted database")
    verify.add_argument("database")
    serve = commands.add_parser("serve", help="serve the authenticated HTTP API")
    serve.add_argument("--database", default="cellguard.sqlite")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--cell-capacity", type=int, default=1000)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "demo":
            _print(run_demo(args.directory))
        elif args.command == "benchmark":
            _print(run_benchmark(args.jobs, args.tenants))
        elif args.command == "verify":
            secret = os.environ.get("CELLGUARD_LEASE_SECRET", "").encode("utf-8")
            if not secret:
                secret = DEMO_SECRET
            store = CellGuardStore(args.database, default_topology(), secret=secret)
            _print(store.verify_integrity())
        else:
            _serve(args)
        return 0
    except (CellGuardError, ValueError) as exc:
        parser = build_parser()
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
