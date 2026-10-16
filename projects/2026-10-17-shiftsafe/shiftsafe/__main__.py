"""JSON CLI for operators; no network listener and no shell execution."""

import argparse
import json
from pathlib import Path
import sqlite3
import sys
import uuid

from .demo import run_demo
from .engine import Engine
from .errors import MigrationError, ValidationError


def parser():
    root = argparse.ArgumentParser(description="ShiftSafe transactional migration lab")
    root.add_argument("--db", default="migration.db", help="operator-owned local SQLite path")
    commands = root.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="isolated synthetic workload and fault injection")
    demo.add_argument("--customers", type=int, default=1000)
    demo.add_argument("--writes", type=int, default=400)
    demo.add_argument("--batch-size", type=int, default=100)
    for name in ("init", "status", "start", "shadow", "verify", "cutover", "rollback", "finalize"):
        commands.add_parser(name)
    put = commands.add_parser("put")
    put.add_argument("customer_id", type=int)
    put.add_argument("--file", required=True, help="UTF-8 JSON list of subscription objects, max 256 KiB")
    put.add_argument("--operation-id", required=True)
    put.add_argument("--expected-revision", type=int)
    delete = commands.add_parser("delete")
    delete.add_argument("customer_id", type=int)
    delete.add_argument("--operation-id", required=True)
    delete.add_argument("--expected-revision", type=int)
    for name in ("get", "repair"):
        commands.add_parser(name).add_argument("customer_id", type=int)
    backfill = commands.add_parser("backfill")
    backfill.add_argument("--batch-size", type=int, default=100)
    backfill.add_argument("--max-batches", type=int, default=10000)
    cleanup = commands.add_parser("cleanup")
    cleanup.add_argument("--batch-size", type=int, default=100)
    cleanup.add_argument("--confirm", required=True, choices=["DROP_LEGACY"])
    events = commands.add_parser("events")
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    return root


def dispatch(args):
    if args.command == "demo":
        return run_demo(customers=args.customers, writes=args.writes, batch_size=args.batch_size)
    engine = Engine(args.db)
    if args.command in ("init", "status"):
        return engine.status()
    if args.command == "put":
        with Path(args.file).open("rb") as stream:
            payload = stream.read(256 * 1024 + 1)
        if len(payload) > 256 * 1024:
            raise ValidationError("input exceeds 256 KiB")
        return engine.put(args.customer_id, json.loads(payload), operation_id=args.operation_id,
                          expected_revision=args.expected_revision)
    if args.command == "delete":
        return engine.delete(args.customer_id, operation_id=args.operation_id, expected_revision=args.expected_revision)
    if args.command in ("get", "repair"):
        result = getattr(engine, args.command)(args.customer_id)
        return result if result is not None else {"repaired": args.customer_id}
    if args.command == "backfill":
        from .model import integer
        integer(args.max_batches, "max_batches", 1, 100000)
        # Validate before obtaining a lease, so invalid input cannot occupy a worker slot.
        integer(args.batch_size, "batch_size", 1, 1000)
        lease = engine.acquire(f"cli:{uuid.uuid4().hex}", ttl=300)
        batches_committed, scanned, result = 0, 0, None
        try:
            for _ in range(args.max_batches):
                engine.renew(lease, ttl=300)
                result = engine.backfill(lease, batch_size=args.batch_size)
                batches_committed += 1
                scanned += result["scanned"]
                if result["done"]:
                    break
        finally:
            engine.release(lease)
        return {"batches_committed": batches_committed, "source_rows_scanned": scanned, "complete": result["done"]}
    if args.command == "cleanup":
        return {"removed": engine.cleanup(batch_size=args.batch_size)}
    if args.command == "events":
        return engine.events(after=args.after, limit=args.limit)
    return getattr(engine, args.command)()


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = dispatch(args)
    except (MigrationError, ValueError, OSError, sqlite3.DatabaseError) as exc:
        error = {"error": type(exc).__name__, "message": str(exc)}
        if hasattr(exc, "report"):
            error["verification"] = exc.report
        print(json.dumps(error, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
