from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from fitfleet import Budget, Controller, Policy, Trace
from fitfleet.demo import generate


class ControllerCase(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = str(Path(self.folder.name) / "registry.db")
        self.clock = [1000000]
        self.controller = Controller(self.path, clock=lambda: self.clock[0])
        self.baseline = Budget(8000, 8192)
        self.policy = Policy(min_training_samples=20, min_validation_samples=20,
                             min_window_s=0, cooldown_s=0, retry_memory=True)
        self.training = generate(count=40)
        self.holdout = generate(count=30, start_s=2400, seed=11)

    def plan(self, *, policy=None, opt_cpu=True, opt_memory=True):
        self.controller.register("encoder", self.baseline, "v1", opt_cpu=opt_cpu, opt_memory=opt_memory)
        return self.controller.plan("encoder", self.training, policy or self.policy)

    def verified(self, *, policy=None):
        rec = self.plan(policy=policy)
        return self.controller.verify(rec["id"], self.holdout)

    def active(self):
        rec = self.verified()
        self.controller.apply(rec["id"])
        return rec

    def fresh(self, **changes):
        trace = generate(count=30, start_s=4200, seed=13)
        return Trace(tuple(replace(r, **changes) for r in trace.runs))
