"""Operator CLI. Minting and graph writes require trusted local database access."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .engine import Engine
from .http import Server
from .model import Problem, parse_json


def main(argv=None):
    parser = argparse.ArgumentParser(description="Local relationship authorization laboratory")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="run synthetic correctness scenarios and benchmark")
    for name in ("init", "mint", "mutate", "check", "serve"):
        command = sub.add_parser(name)
        command.add_argument("--db", required=True)
        if name != "serve":
            command.add_argument("--tenant", required=True)
        if name in ("mint", "check"):
            command.add_argument("--user", required=True, help="user:identifier")
        if name == "mint":
            command.add_argument("--role", choices=("user", "admin"), default="user")
            command.add_argument("--ttl", type=int, default=900)
        elif name == "mutate":
            command.add_argument("--file", required=True, help="bounded JSON mutation request")
        elif name == "check":
            command.add_argument("--resource", required=True)
            command.add_argument("--permission", choices=("view", "edit", "share"), default="view")
        elif name == "serve":
            command.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            from .demo import run
            result = run()
        else:
            engine = Engine(args.db)
            if args.command == "init":
                result = engine.create_tenant(args.tenant)
            elif args.command == "mint":
                # Explicit operator action: this is the only CLI command emitting a token.
                result = {"access_token": engine.mint(args.tenant, args.user, args.role, args.ttl)}
            elif args.command == "mutate":
                from .model import fields
                with Path(args.file).open("rb") as handle:
                    request = parse_json(handle.read(65537))
                fields(request, ("operations", "request_id"), ("expected_revision",))
                result = engine.mutate(args.tenant, **request)
            elif args.command == "check":
                result = engine.check(args.tenant, args.resource, args.permission, args.user)
            else:
                with Server(engine, port=args.port) as server:
                    print(json.dumps({"listening": "127.0.0.1", "port": server.server_port}), flush=True)
                    server.serve_forever()
                return 0
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (Problem, OSError, sqlite3.Error) as exc:
        print(json.dumps({"error": getattr(exc, "code", "local_io"),
                          "message": exc.message if isinstance(exc, Problem) else "local I/O failed"}),
              file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
