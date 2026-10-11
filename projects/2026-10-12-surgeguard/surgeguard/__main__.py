"""Run with python -m surgeguard --help. No package installation is needed."""

import argparse
import asyncio
import json
import random
from pathlib import Path

from .client import Policy, ResilientClient
from .controls import RetryBudget
from .demo import demo, workload
from .lab import FaultServer
from .transport import HttpTransport


def integer_range(low, high):
    def convert(value):
        try:
            parsed = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("Expected an integer") from exc
        if not low <= parsed <= high:
            raise argparse.ArgumentTypeError(f"Value must be in [{low}, {high}]")
        return parsed
    return convert


async def execute(args):
    if args.command == "demo":
        return await demo(args.count, args.concurrency, args.seed)
    if args.command == "serve":
        server = FaultServer(args.mode, args.delay)
        await server.start(args.port)
        print(json.dumps({"url": server.url, "mode": server.mode, "synthetic": True}), flush=True)
        try:
            await asyncio.Event().wait()
        finally:
            await server.close()
    if args.command == "call":
        client = ResilientClient(Policy(max_inflight=args.max_inflight), transport=HttpTransport(allow_network=args.allow_network), budget=RetryBudget(args.retry_tokens, 1), rng=random.Random(7))
        return await workload(client, args.url, args.count, args.concurrency)


def main():
    parser = argparse.ArgumentParser(description="SurgeGuard: bounded retries and overload protection")
    commands = parser.add_subparsers(dest="command", required=True)
    example = commands.add_parser("demo", help="Run real loopback HTTP failure experiments")
    example.add_argument("--count", type=integer_range(1, 10000), default=96)
    example.add_argument("--concurrency", type=integer_range(1, 1024), default=24)
    example.add_argument("--seed", type=int, default=7)
    example.add_argument("--json", type=Path, help="Save complete measured report")
    server = commands.add_parser("serve", help="Start a loopback-only synthetic backend")
    server.add_argument("--port", type=integer_range(0, 65535), default=8090)
    server.add_argument("--mode", choices=("healthy", "outage", "transient", "slow"), default="transient")
    server.add_argument("--delay", type=float, default=.002)
    call = commands.add_parser("call", help="Exercise an explicitly supplied read-only endpoint")
    call.add_argument("--url", required=True)
    call.add_argument("--count", type=integer_range(1, 10000), default=50)
    call.add_argument("--concurrency", type=integer_range(1, 1024), default=10)
    call.add_argument("--max-inflight", type=integer_range(1, 10000), default=32)
    call.add_argument("--retry-tokens", type=integer_range(0, 10000), default=10)
    call.add_argument("--allow-network", action="store_true", help="Explicitly permit non-loopback destinations")
    args = parser.parse_args()
    try:
        result = asyncio.run(execute(args))
    except KeyboardInterrupt:
        return 0
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    if result is not None:
        rendered = json.dumps(result, indent=2, sort_keys=True)
        if getattr(args, "json", None):
            args.json.parent.mkdir(parents=True, exist_ok=True)
            args.json.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
