from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from unittest.mock import patch

from fitfleet import Budget, Controller
from fitfleet.demo import generate
from fitfleet.errors import Busy, Conflict
from fitfleet.sizing import compare, recommend
from .helpers import ControllerCase


class ConcurrencyTests(ControllerCase):
    def test_four_apply_retries_have_one_configuration_update_and_event(self):
        rec = self.verified()
        start = threading.Barrier(4, timeout=5)
        def activate(_):
            other = Controller(self.path)
            start.wait()
            return other.apply(rec["id"])
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(activate, range(4)))
        self.assertEqual(sum(not r["replayed"] for r in results), 1)
        self.assertEqual(self.controller.status("encoder")["version"], 2)
        self.assertEqual(sum(e["kind"] == "configuration_applied_locally" for e in self.controller.events("encoder")), 1)

    def test_two_candidates_for_same_config_revision_cannot_both_activate(self):
        from dataclasses import replace
        first = self.verified()
        second = self.controller.plan("encoder", self.training, replace(self.policy, max_cpu_reduction_bp=4000))
        self.controller.verify(second["id"], self.holdout)
        start = threading.Barrier(2, timeout=5)
        def activate(rec):
            other = Controller(self.path)
            start.wait()
            try:
                return other.apply(rec["id"])["state"]
            except Conflict:
                return "STALE"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(activate, [first, second]))
        self.assertCountEqual(results, ["APPLIED", "STALE"])
        self.assertEqual(self.controller.status("encoder")["version"], 2)

    def test_competing_verification_cannot_replace_first_committed_evidence(self):
        rec = self.plan()
        alternative = generate(count=30, start_s=2400, seed=99)
        start = threading.Barrier(2, timeout=5)
        def verify(trace):
            other = Controller(self.path)
            start.wait()
            try:
                return other.verify(rec["id"], trace)["state"]
            except Conflict:
                return "CONFLICT"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(verify, [self.holdout, alternative]))
        self.assertCountEqual(results, ["VERIFIED", "CONFLICT"])
        stored = self.controller.show(rec["id"])["verification"]["evidence"]["fingerprint"]
        self.assertIn(stored, [self.holdout.provenance()["fingerprint"], alternative.provenance()["fingerprint"]])

    def test_operator_change_during_replay_wins_over_late_rollback(self):
        rec = self.active()
        def replay(*args, **kwargs):
            self.controller.register("encoder", Budget(6000, 7808), "v2", expected_version=2)
            return compare(*args, **kwargs)
        with patch("fitfleet.controller.compare", side_effect=replay):
            result = self.controller.observe(rec["id"], self.fresh(memory_peak_mib=7000))
        self.assertEqual(result["state"], "SUPERSEDED")
        self.assertFalse(result["rolled_back"])
        self.assertEqual(self.controller.status("encoder")["code_revision"], "v2")
        self.assertEqual(self.controller.status("encoder")["memory_mib"], 7808)

    def test_operator_change_during_planning_rejects_stale_result(self):
        self.controller.register("encoder", self.baseline, "v1", opt_cpu=True, opt_memory=True)
        def planner(*args, **kwargs):
            self.controller.register("encoder", self.baseline, "v2", expected_version=1)
            return recommend(*args, **kwargs)
        with patch("fitfleet.controller.recommend", side_effect=planner), self.assertRaises(Conflict):
            self.controller.plan("encoder", self.training, self.policy)
        self.assertEqual(sum(e["kind"] == "recommendation_created" for e in self.controller.events("encoder")), 0)

    def test_abrupt_process_exit_between_config_and_state_updates_rolls_back(self):
        rec = self.verified()
        script = """
import os,sys
from fitfleet import Controller
def fault(point):
    if point == 'after_apply_config_update': os._exit(23)
Controller(sys.argv[1],fault=fault).apply(sys.argv[2])
"""
        process = subprocess.run([sys.executable, "-c", script, self.path, rec["id"]], timeout=10, capture_output=True)
        self.assertEqual(process.returncode, 23, process.stderr.decode())
        restarted = Controller(self.path)
        self.assertEqual(restarted.status("encoder")["version"], 1)
        self.assertEqual(restarted.status("encoder")["cpu_millis"], 8000)
        self.assertEqual(restarted.show(rec["id"])["state"], "VERIFIED")
        self.assertEqual(restarted.apply(rec["id"])["state"], "APPLIED")

    def test_abrupt_process_exit_during_rollback_preserves_previous_active_config(self):
        rec = self.active()
        script = """
import os,sys
from fitfleet import Controller
def fault(point):
    if point == 'after_rollback_config_update': os._exit(24)
Controller(sys.argv[1],fault=fault).rollback(sys.argv[2])
"""
        process = subprocess.run([sys.executable, "-c", script, self.path, rec["id"]], timeout=10, capture_output=True)
        self.assertEqual(process.returncode, 24, process.stderr.decode())
        restarted = Controller(self.path)
        self.assertEqual(restarted.status("encoder")["version"], 2)
        self.assertEqual(restarted.status("encoder")["memory_mib"], 7040)
        self.assertEqual(restarted.show(rec["id"])["state"], "APPLIED")
        self.assertEqual(restarted.rollback(rec["id"])["state"], "ROLLED_BACK")

    def test_write_lock_timeout_has_a_retryable_error(self):
        self.plan()
        other = Controller(self.path, timeout=0.01)
        con = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(con.close)
        con.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(Busy):
                other.set_enabled(False)
        finally:
            con.rollback()
        self.assertFalse(other.set_enabled(False)["enabled"])

    def test_parallel_first_open_does_not_corrupt_registry(self):
        path = str(Path(self.folder.name) / "new.db")
        start = threading.Barrier(4, timeout=5)
        def initialize(_):
            start.wait()
            for _ in range(10):
                try:
                    return Controller(path)
                except Busy:
                    continue
            raise AssertionError("initialization never progressed")
        with ThreadPoolExecutor(max_workers=4) as pool:
            controllers = list(pool.map(initialize, range(4)))
        controllers[0].register("encoder", self.baseline, "v1")
        for controller in controllers:
            self.assertEqual(controller.status("encoder")["version"], 1)
