"""Command-line entry point for the demo and local HTTP service."""

from __future__ import annotations

import argparse
import json
import signal
import threading
from pathlib import Path

from .cluster import ClusterConfig, QuorumCluster
from .demo import run_demo
from .httpserver import ServerRuntime


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
    parser = argparse.ArgumentParser(description="QuorumForge: fault-test a Dynamo-inspired replicated key-value store")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run deterministic availability, conflict, and repair experiments")
    demo.add_argument("--keys", type=bounded_integer(100, 20_000), default=600)
    demo.add_argument("--seed", type=int, default=14)
    demo.add_argument("--json", type=Path, help="save the complete report")
    serve = commands.add_parser("serve", help="start the loopback-only in-memory API")
    serve.add_argument("--nodes", type=bounded_integer(3, 16), default=5)
    serve.add_argument("--port", type=bounded_integer(0, 65535), default=4343)
    args = parser.parse_args()

    if args.command == "demo":
        report = run_demo(args.keys, seed=args.seed)
        rendered = json.dumps(report, indent=2, sort_keys=True)
        if args.json:
            args.json.parent.mkdir(parents=True, exist_ok=True)
            args.json.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0

    nodes = tuple(f"n{index}" for index in range(1, args.nodes + 1))
    cluster = QuorumCluster(nodes, ClusterConfig())
    stopped = threading.Event()
    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: stopped.set())
    with ServerRuntime(cluster, port=args.port) as runtime:
        host, port = runtime.address
        print(json.dumps({"url": f"http://{host}:{port}", "nodes": list(nodes), "N": 3, "R": 2, "W": 2}), flush=True)
        stopped.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
