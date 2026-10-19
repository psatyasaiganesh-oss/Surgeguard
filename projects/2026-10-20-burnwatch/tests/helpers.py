import tempfile
import unittest
from pathlib import Path

from burnwatch.engine import Engine
from burnwatch.model import Problem, default_policy

EPOCH = 2000000040


class Clock:
    def __init__(self, now=EPOCH):
        self.now = now

    def __call__(self):
        return self.now


def fast_policy():
    policy = default_policy()
    policy["rules"] = policy["rules"][:1]
    return policy


def values(start, count, total=1000, bad=0):
    return [{"minute": start + i * 60, "total": total, "bad": bad} for i in range(count)]


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="burnwatch-test-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.db"
        self.clock = Clock()
        self.engine = Engine(self.path, clock=self.clock)
        self.policy = fast_policy()
        self.start = EPOCH - 3600
        self.producers = ["api-a", "api-b"]
        self.engine.register("checkout", self.producers, self.policy, self.start)
        self.serial = 0

    def ingest(self, producer, buckets, watermark=None, batch_id=None):
        self.serial += 1
        return self.engine.ingest("checkout", producer, batch_id or f"batch-{self.serial}", buckets, watermark)

    def fill(self, total=1000, bad=0, producers=None):
        for producer in producers or self.producers:
            self.ingest(producer, values(self.start, 60, total, bad), EPOCH)
        return self.engine.evaluate("checkout")

    def advance(self, count=1, total=1000, bad=0, producers=None):
        start = self.clock.now
        self.clock.now += count * 60
        for producer in producers or self.producers:
            self.ingest(producer, values(start, count, total, bad), self.clock.now)
        return self.engine.evaluate("checkout")

    def problem(self, code, action, *args, **kwargs):
        with self.assertRaises(Problem) as raised:
            action(*args, **kwargs)
        self.assertEqual(code, raised.exception.code)
        return raised.exception
