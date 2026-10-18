import tempfile
import unittest
from pathlib import Path

from scopegraph.engine import Engine
from scopegraph.model import Problem


class Clock:
    def __init__(self):
        self.now = 2000000000

    def __call__(self):
        return self.now


def create(ref):
    return {"op": "create", "object": ref}


def edge(resource, relation, subject, **extra):
    return {"op": "touch", "resource": resource, "relation": relation, "subject": subject, **extra}


def delete(resource, relation, subject):
    return {"op": "delete", "resource": resource, "relation": relation, "subject": subject}


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="scopegraph-test-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.db"
        self.clock = Clock()
        self.engine = Engine(self.path, clock=self.clock)
        self.engine.create_tenant("acme")
        self.serial = 0
        self.seed = self.write([
            *[create(ref) for ref in ("folder:root", "folder:team", "folder:other", "document:plan",
                                     "document:other", "group:eng", "group:backend")],
            edge("folder:root", "owner", "user:owner"),
            edge("folder:team", "parent", "folder:root"),
            edge("document:plan", "parent", "folder:team"),
            edge("group:eng", "member", "group:backend#member"),
            edge("group:backend", "member", "user:alice"),
            edge("folder:team", "editor", "group:eng#member"),
        ])

    def write(self, operations, **kwargs):
        self.serial += 1
        return self.engine.mutate("acme", operations, f"test-{self.serial}", **kwargs)

    def check(self, user="user:alice", perm="view", resource="document:plan", **kwargs):
        return self.engine.check("acme", resource, perm, user, **kwargs)

    def problem(self, code, action, *args, **kwargs):
        with self.assertRaises(Problem) as raised:
            action(*args, **kwargs)
        self.assertEqual(code, raised.exception.code)
        return raised.exception

    def publish(self, text="version one", request_id="publish-one", version=0, **kwargs):
        return self.engine.publish("acme", "document:plan", "user:owner", text, request_id, version, **kwargs)
