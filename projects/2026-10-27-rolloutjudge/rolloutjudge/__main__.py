"""RolloutJudge command-line interface."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from .api import create_server
from .demo import DEMO_SECRET, default_policy, run_benchmark, run_demo
from .errors import RolloutJudgeError, ValidationError
from .store import RolloutJudgeStore


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _runtime_secret(*, allow_demo: bool = False) -> bytes:
    value = os.environ.get("ROLLOUTJUDGE_SIGNING_SECRET", "")
    if value:
        return value.encode("utf-8")
    if allow_demo:
        return DEMO_SECRET
    raise ValidationError("ROLLOUTJUDGE_SIGNING_SECRET is required")


def _service_tokens() -> dict[str, str]:
    raw = os.environ.get("ROLLOUTJUDGE_SERVICE_TOKENS", "")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValidationError("ROLLOUTJUDGE_SERVICE_TOKENS must be a JSON object") from exc
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(token, str) for key, token in value.items()
    ):
        raise ValidationError("ROLLOUTJUDGE_SERVICE_TOKENS must map services to tokens")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Durable statistical canary-release judge")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run deterministic rollout scenarios")
    demo.add_argument("--directory", help="persist the demo database in this directory")
    benchmark = commands.add_parser("benchmark", help="run a local synthetic benchmark")
    benchmark.add_argument("--analyses", type=int, default=100)
    init = commands.add_parser("init", help="create or update the default policy")
    init.add_argument("database")
    init.add_argument("--policy-id", default="standard")
    init.add_argument("--expected-version", type=int, default=0)
    verify = commands.add_parser("verify", help="verify a persisted database")
    verify.add_argument("database")
    serve = commands.add_parser("serve", help="serve the authenticated local API")
    serve.add_argument("--database", default="rolloutjudge.sqlite")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "demo":
            _print(run_demo(args.directory))
        elif args.command == "benchmark":
            _print(run_benchmark(args.analyses))
        elif args.command == "init":
            store = RolloutJudgeStore(args.database, secret=_runtime_secret())
            _print(
                store.create_policy(
                    args.policy_id,
                    default_policy(),
                    expected_version=args.expected_version,
                )
            )
        elif args.command == "verify":
            store = RolloutJudgeStore(args.database, secret=_runtime_secret(allow_demo=True))
            _print(store.verify_integrity())
        else:
            store = RolloutJudgeStore(
                Path(args.database), secret=_runtime_secret()
            )
            server = create_server(
                store,
                host=args.host,
                port=args.port,
                service_tokens=_service_tokens(),
            )
            print(f"RolloutJudge listening on http://{args.host}:{server.server_port}")
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
        return 0
    except (RolloutJudgeError, ValueError) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
