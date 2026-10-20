"""RouteProof command-line interface."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .actuator import Actuator
from .control import Controller
from .demo import main as demo_main
from .http import create_server
from .model import strict_json


def _load(path: str) -> object:
    return strict_json(Path(path).read_bytes())


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="routeproof")
    commands = root.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="initialize an actuator from a topology JSON file")
    init.add_argument("--topology", required=True)
    init.add_argument("--actuator", required=True)

    operator = commands.add_parser("operator", help="register an operator using ROUTEPROOF_TOKEN")
    operator.add_argument("--control", required=True)
    operator.add_argument("--actuator", required=True)
    operator.add_argument("--id", required=True)
    operator.add_argument("--role", choices=["requester", "reviewer", "executor"], required=True)

    serve = commands.add_parser("serve", help="run the loopback-only API")
    serve.add_argument("--control", required=True)
    serve.add_argument("--actuator", required=True)
    serve.add_argument("--port", type=int, default=8080)

    commands.add_parser("demo", help="run the complete synthetic scenario")
    return root


def main() -> None:
    args = parser().parse_args()
    if args.command == "demo":
        demo_main()
        return
    actuator = Actuator(args.actuator)
    if args.command == "init":
        actuator.initialize(_load(args.topology))
        print(json.dumps({"initialized": True, "version": actuator.snapshot()["version"]}))
        return
    controller = Controller(args.control, actuator)
    if args.command == "operator":
        token = os.environ.get("ROUTEPROOF_TOKEN")
        if not token:
            raise SystemExit("ROUTEPROOF_TOKEN is required")
        controller.register_operator(args.id, args.role, token)
        print(json.dumps({"registered": args.id, "role": args.role}))
        return
    if args.command == "serve":
        server = create_server(controller, port=args.port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
