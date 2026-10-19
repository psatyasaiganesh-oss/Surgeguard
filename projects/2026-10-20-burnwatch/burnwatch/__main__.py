"""Local operator CLI; the server does not automatically schedule evaluations."""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

from .delivery import Inbox, deliver
from .engine import Engine
from .http import Server
from .model import Problem, fields, parse_json


def main(argv=None):
    parser = argparse.ArgumentParser(description="Complete-data multi-window SLO alerting lab")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo")
    for name in ("init", "mint", "ingest", "tick", "status", "metrics", "verify", "prune", "deliver", "serve"):
        command = sub.add_parser(name)
        command.add_argument("--db", required=True)
        if name not in ("deliver", "serve"):
            command.add_argument("--service", required=True)
        if name in ("mint", "ingest"):
            command.add_argument("--producer", required=True)
        if name in ("init", "ingest"):
            command.add_argument("--file", required=True)
        if name == "init":
            command.add_argument("--start-minute", type=int, default=None)
        if name == "deliver":
            command.add_argument("--inbox", required=True)
        if name == "serve":
            command.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            from .demo import run
            result = run()
        else:
            engine = Engine(args.db)
            if args.command in ("init", "ingest"):
                with Path(args.file).open("rb") as handle:
                    data = parse_json(handle.read(65537))
            if args.command == "init":
                fields(data, ("producers", "policy"))
                result = engine.register(args.service, data["producers"], data["policy"],
                                         args.start_minute if args.start_minute is not None else int(time.time()) // 60 * 60)
            elif args.command == "mint":
                result = {"api_key": engine.mint(args.service, args.producer)}
            elif args.command == "ingest":
                fields(data, ("batch_id", "buckets"), ("watermark",))
                result = engine.ingest(args.service, args.producer, **data)
            elif args.command == "tick":
                result = engine.evaluate(args.service)
            elif args.command == "status":
                result = engine.snapshot(args.service)
            elif args.command == "metrics":
                print(engine.metrics(args.service), end="")
                return 0
            elif args.command in ("verify", "prune"):
                result = getattr(engine, args.command)(args.service)
            elif args.command == "deliver":
                result = deliver(engine, Inbox(args.inbox))
            elif args.command == "serve":
                with Server(engine, port=args.port) as server:
                    print(json.dumps({"listening": "127.0.0.1", "port": server.server_port}), flush=True)
                    server.serve_forever()
                return 0
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (Problem, OSError, sqlite3.Error) as exc:
        print(json.dumps({"error": getattr(exc, "code", "local_io"),
                          "message": exc.message if isinstance(exc, Problem) else "local I/O failed"}), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
