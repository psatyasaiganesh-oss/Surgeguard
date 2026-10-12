"""Command-line entry point: python -m ledgergate --help."""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
from pathlib import Path

from .demo import run_demo
from .httpserver import ServerRuntime
from .service import LedgerGate
from .store import SQLiteStore


def bounded_integer(low: int, high: int):
    def parse(value: str) -> int:
        try:
            number = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("expected an integer") from exc
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f"value must be in [{low}, {high}]")
        return number
    return parse


def main() -> int:
    parser = argparse.ArgumentParser(description="LedgerGate: correct processing of duplicate and out-of-order payment webhooks")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Run deterministic correctness and crash-recovery experiments")
    demo.add_argument("--payments", type=bounded_integer(10, 20_000), default=400)
    demo.add_argument("--seed", type=int, default=13)
    demo.add_argument("--json", type=Path, help="save the complete JSON report")
    serve = commands.add_parser("serve", help="start the loopback webhook endpoint and worker")
    serve.add_argument("--db", type=Path, default=Path("ledgergate.db"))
    serve.add_argument("--port", type=bounded_integer(0, 65535), default=4242)
    args = parser.parse_args()

    if args.command == "demo":
        report = run_demo(args.payments, args.seed)
        rendered = json.dumps(report, indent=2, sort_keys=True)
        if args.json:
            args.json.parent.mkdir(parents=True, exist_ok=True)
            args.json.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0

    secret = os.environ.get("WEBHOOK_SECRET")
    if secret is None or len(secret.encode()) < 16:
        parser.error("WEBHOOK_SECRET must contain at least 16 bytes")
    store = SQLiteStore(args.db)
    service = LedgerGate(store, [secret.encode()])
    stopped = threading.Event()
    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: stopped.set())
    try:
        with ServerRuntime(service, port=args.port) as runtime:
            host, port = runtime.address
            print(json.dumps({"url": f"http://{host}:{port}/webhook", "database": str(args.db), "loopback_only": True}), flush=True)
            stopped.wait()
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
