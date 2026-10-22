"""MergeMesh command-line interface."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .api import create_server
from .demo import run_demo
from .store import RegionStore


def _json(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="mergemesh",
        description="Conflict-visible, authenticated multi-region replication lab",
    )
    commands = root.add_subparsers(dest="command", required=True)

    demo = commands.add_parser("demo", help="run the complete local demonstration")
    demo.add_argument("--workdir", type=Path)

    status = commands.add_parser("status", help="render one persisted document")
    status.add_argument("--database", type=Path, required=True)
    status.add_argument("--region", required=True)
    status.add_argument("--tenant", required=True)
    status.add_argument("--document", required=True)

    verify = commands.add_parser("verify", help="verify persisted operation and audit integrity")
    verify.add_argument("--database", type=Path, required=True)
    verify.add_argument("--region", required=True)

    serve = commands.add_parser("serve", help="serve the authenticated local document API")
    serve.add_argument("--database", type=Path, required=True)
    serve.add_argument("--region", required=True)
    serve.add_argument("--tenant", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return root


def main() -> None:
    args = parser().parse_args()
    if args.command == "demo":
        _json(run_demo(args.workdir))
        return
    store = RegionStore(args.database, args.region)
    if args.command == "status":
        _json(store.get_document(args.tenant, args.document))
    elif args.command == "verify":
        result = store.verify()
        _json(result)
        if not result["ok"]:
            raise SystemExit(1)
    elif args.command == "serve":
        token = os.environ.get("MERGEMESH_API_TOKEN")
        if token is None:
            raise SystemExit("MERGEMESH_API_TOKEN must be set; tokens are not accepted as CLI arguments")
        server = create_server(
            store,
            {token: args.tenant},
            host=args.host,
            port=args.port,
        )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
