"""SpecuFlow command line."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .demo import main as demo_main
from .model import strict_json
from .runner import run_job
from .store import Scheduler


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="specuflow")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="run reproducible scenarios and benchmark")

    run = commands.add_parser("run", help="submit and execute a synthetic job JSON")
    run.add_argument("--database", required=True)
    run.add_argument("--blobs", required=True)
    run.add_argument("--job", required=True)
    run.add_argument("--workers", type=int, default=4)
    run.add_argument("--no-speculation", action="store_true")

    status = commands.add_parser("status", help="show durable job state")
    status.add_argument("--database", required=True)
    status.add_argument("--blobs", required=True)
    status.add_argument("--job-id", required=True)

    verify = commands.add_parser("verify", help="verify committed result blobs")
    verify.add_argument("--database", required=True)
    verify.add_argument("--blobs", required=True)
    verify.add_argument("--job-id", required=True)
    return root


def main() -> None:
    args = parser().parse_args()
    if args.command == "demo":
        demo_main()
        return
    scheduler = Scheduler(args.database, args.blobs)
    if args.command == "run":
        spec = strict_json(Path(args.job).read_bytes())
        scheduler.submit(spec)
        result = run_job(
            scheduler,
            spec["job_id"],
            workers=args.workers,
            allow_speculation=not args.no_speculation,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "status":
        print(json.dumps(scheduler.status(args.job_id), indent=2, sort_keys=True))
    elif args.command == "verify":
        print(json.dumps(scheduler.verify(args.job_id), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
