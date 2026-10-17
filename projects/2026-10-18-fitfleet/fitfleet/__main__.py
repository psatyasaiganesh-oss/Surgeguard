"""JSON operator interface. Apply modifies the local registry, never Kubernetes."""

import argparse
import json
import sqlite3
import sys

from .controller import Controller
from .demo import generate, run_demo
from .errors import FitFleetError
from .model import Budget, Policy, Trace, from_dict, load_json


def parser():
    root = argparse.ArgumentParser(description="FitFleet local evidence-gated batch resource tuner")
    root.add_argument("--db", default="fitfleet.db", help="private local registry path (never a cluster connection)")
    commands = root.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo")
    demo.add_argument("--training-count", type=int, default=1000)
    demo.add_argument("--holdout-count", type=int, default=300)
    gen = commands.add_parser("generate", help="emit a synthetic demand trace as JSON")
    gen.add_argument("--workload", default="encoder")
    gen.add_argument("--revision", default="v1")
    gen.add_argument("--count", type=int, default=1000)
    gen.add_argument("--start", type=int, default=0)
    gen.add_argument("--seed", type=int, default=7)
    gen.add_argument("--profile", choices=["steady", "rare-memory", "memory-shift", "cpu-shift"], default="steady")
    register = commands.add_parser("register")
    register.add_argument("workload")
    register.add_argument("--cpu-millis", type=int, required=True)
    register.add_argument("--memory-mib", type=int, required=True)
    register.add_argument("--code-revision", required=True)
    register.add_argument("--opt-in", action="append", choices=["cpu", "memory"], default=[])
    register.add_argument("--expected-version", type=int)
    commands.add_parser("status").add_argument("workload")
    plan = commands.add_parser("plan")
    plan.add_argument("workload")
    plan.add_argument("--trace", required=True)
    plan.add_argument("--policy", help="strict JSON Policy overrides")
    for action in ("verify", "observe"):
        sub = commands.add_parser(action)
        sub.add_argument("recommendation_id")
        sub.add_argument("--trace", required=True)
    for action in ("show", "apply", "rollback"):
        commands.add_parser(action).add_argument("recommendation_id")
    for action in ("pause", "resume"):
        commands.add_parser(action)
    events = commands.add_parser("events")
    events.add_argument("workload")
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    manifest = commands.add_parser("manifest", help="export a Job JSON; does not submit it")
    manifest.add_argument("workload")
    manifest.add_argument("--image", required=True)
    manifest.add_argument("--namespace", default="default")
    return root


def dispatch(args):
    if args.command == "demo":
        return run_demo(training_count=args.training_count, holdout_count=args.holdout_count)
    if args.command == "generate":
        return generate(workload=args.workload, revision=args.revision, count=args.count,
                        start_s=args.start, seed=args.seed, profile=args.profile).document()
    controller = Controller(args.db)
    if args.command == "register":
        return controller.register(args.workload, Budget(args.cpu_millis, args.memory_mib), args.code_revision,
                                   opt_cpu="cpu" in args.opt_in, opt_memory="memory" in args.opt_in,
                                   expected_version=args.expected_version)
    if args.command == "plan":
        policy = from_dict(Policy, load_json(args.policy)) if args.policy else Policy()
        return controller.plan(args.workload, Trace.parse(load_json(args.trace)), policy)
    if args.command in ("verify", "observe"):
        return getattr(controller, args.command)(args.recommendation_id, Trace.parse(load_json(args.trace)))
    if args.command in ("show", "apply", "rollback"):
        return getattr(controller, args.command)(args.recommendation_id)
    if args.command in ("pause", "resume"):
        return controller.set_enabled(args.command == "resume")
    if args.command == "status":
        return controller.status(args.workload)
    if args.command == "events":
        return controller.events(args.workload, after=args.after, limit=args.limit)
    if args.command == "manifest":
        return controller.manifest(args.workload, image=args.image, namespace=args.namespace)
    raise AssertionError("unhandled command")


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = dispatch(args)
    except (FitFleetError, ValueError, OSError, sqlite3.DatabaseError) as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
