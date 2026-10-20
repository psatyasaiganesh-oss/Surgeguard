import os
import tempfile
import unittest
from pathlib import Path

from routeproof.actuator import Actuator
from routeproof.control import (
    AuthorizationError,
    ConflictError,
    Controller,
    SimulatedCrash,
)
from routeproof.samples import safe_plan, sample_topology, unsafe_backbone_plan


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.actuator = Actuator(root / "actuator.db")
        self.actuator.initialize(sample_topology())
        self.controller = Controller(root / "control.db", self.actuator)
        self.tokens = {
            "alice": "a" * 40,
            "bob": "b" * 40,
            "carol": "c" * 40,
            "dave": "d" * 40,
        }
        self.controller.register_operator("alice", "requester", self.tokens["alice"])
        self.controller.register_operator("bob", "reviewer", self.tokens["bob"])
        self.controller.register_operator("carol", "reviewer", self.tokens["carol"])
        self.controller.register_operator("dave", "executor", self.tokens["dave"])

    def tearDown(self):
        self.temp.cleanup()

    def prepare(self, plan=None):
        plan = plan or safe_plan()
        plan_id = plan["plan_id"]
        self.controller.create_plan(self.tokens["alice"], plan)
        self.controller.analyze_plan(self.tokens["alice"], plan_id)
        self.controller.approve(self.tokens["bob"], plan_id)
        return self.controller.approve(self.tokens["carol"], plan_id)

    def test_invalid_token_is_rejected(self):
        with self.assertRaises(AuthorizationError):
            self.controller.create_plan("wrong", safe_plan())

    def test_role_is_enforced(self):
        with self.assertRaises(AuthorizationError):
            self.controller.create_plan(self.tokens["bob"], safe_plan())

    def test_create_is_idempotent(self):
        first = self.controller.create_plan(self.tokens["alice"], safe_plan())
        second = self.controller.create_plan(self.tokens["alice"], safe_plan())
        self.assertEqual(first["plan_hash"], second["plan_hash"])

    def test_plan_id_conflict_is_rejected(self):
        self.controller.create_plan(self.tokens["alice"], safe_plan())
        changed = safe_plan()
        changed["reason"] = "A different long enough maintenance reason."
        with self.assertRaises(ConflictError):
            self.controller.create_plan(self.tokens["alice"], changed)

    def test_unsafe_plan_cannot_be_approved(self):
        self.controller.create_plan(self.tokens["alice"], unsafe_backbone_plan())
        state = self.controller.analyze_plan(self.tokens["alice"], "unsafe-backbone")
        self.assertEqual(state["state"], "BLOCKED")
        with self.assertRaises(ConflictError):
            self.controller.approve(self.tokens["bob"], "unsafe-backbone")

    def test_validator_failure_is_fail_closed(self):
        broken = Controller(
            Path(self.temp.name) / "broken.db",
            self.actuator,
            analyzer=lambda topology, plan: (_ for _ in ()).throw(TimeoutError()),
        )
        broken.register_operator("alice", "requester", self.tokens["alice"])
        broken.create_plan(self.tokens["alice"], safe_plan())
        state = broken.analyze_plan(self.tokens["alice"], "safe-maintenance")
        self.assertEqual(state["state"], "BLOCKED")
        self.assertEqual(state["analysis"]["violations"][0]["code"], "VALIDATOR_FAILURE")

    def test_two_distinct_approvals_are_required(self):
        self.controller.create_plan(self.tokens["alice"], safe_plan())
        self.controller.analyze_plan(self.tokens["alice"], "safe-maintenance")
        first = self.controller.approve(self.tokens["bob"], "safe-maintenance")
        again = self.controller.approve(self.tokens["bob"], "safe-maintenance")
        final = self.controller.approve(self.tokens["carol"], "safe-maintenance")
        self.assertEqual(first["state"], "ANALYZED")
        self.assertEqual(again["approval_count"], 1)
        self.assertEqual(final["state"], "APPROVED")

    def test_execution_requires_approval_quorum(self):
        self.controller.create_plan(self.tokens["alice"], safe_plan())
        self.controller.analyze_plan(self.tokens["alice"], "safe-maintenance")
        with self.assertRaises(ConflictError):
            self.controller.execute_next(self.tokens["dave"], "safe-maintenance")

    def test_crash_after_actuation_replays_safely(self):
        self.prepare()
        with self.assertRaises(SimulatedCrash):
            self.controller.execute_next(
                self.tokens["dave"], "safe-maintenance", crash_after_actuation=True
            )
        self.assertEqual(self.actuator.snapshot()["version"], 2)
        state = self.controller.execute_next(self.tokens["dave"], "safe-maintenance")
        self.assertEqual(state["next_step"], 1)
        self.assertEqual(self.actuator.snapshot()["version"], 2)
        event = next(
            item for item in self.controller.audit_log(self.tokens["dave"])
            if item["event"] == "STEP_APPLIED"
        )
        self.assertTrue(event["details"]["replayed"])

    def test_health_observation_gates_completion(self):
        self.prepare()
        state = self.controller.execute_next(self.tokens["dave"], "safe-maintenance")
        self.assertEqual(state["state"], "AWAITING_OBSERVATION")
        state = self.controller.observe(
            self.tokens["dave"], "safe-maintenance", True, "all probes healthy"
        )
        self.assertEqual(state["state"], "SUCCEEDED")

    def test_unhealthy_observation_pauses(self):
        self.prepare()
        self.controller.execute_next(self.tokens["dave"], "safe-maintenance")
        state = self.controller.observe(
            self.tokens["dave"], "safe-maintenance", False, "packet loss above threshold"
        )
        self.assertEqual(state["state"], "PAUSED")
        self.assertEqual(state["error_code"], "UNHEALTHY_CANARY")

    def test_topology_drift_blocks_execution(self):
        self.prepare()
        self.actuator.external_change("dc-c-backbone-1", False)
        with self.assertRaises(ConflictError):
            self.controller.execute_next(self.tokens["dave"], "safe-maintenance")
        state = self.controller.status(self.tokens["dave"], "safe-maintenance")
        self.assertEqual(state["state"], "STALE")

    def test_successful_plan_can_be_rolled_back(self):
        self.prepare()
        self.controller.execute_next(self.tokens["dave"], "safe-maintenance")
        self.controller.observe(
            self.tokens["dave"], "safe-maintenance", True, "all probes healthy"
        )
        state = self.controller.rollback(self.tokens["dave"], "safe-maintenance")
        self.assertEqual(state["state"], "ROLLED_BACK")
        link = next(
            item for item in self.actuator.snapshot()["links"]
            if item["id"] == "dc-a-backbone-1"
        )
        self.assertTrue(link["up"])

    def test_rollback_recovers_after_crash(self):
        self.prepare()
        self.controller.execute_next(self.tokens["dave"], "safe-maintenance")
        self.controller.observe(
            self.tokens["dave"], "safe-maintenance", True, "all probes healthy"
        )
        with self.assertRaises(SimulatedCrash):
            self.controller.rollback(
                self.tokens["dave"], "safe-maintenance", crash_after_actuation=True
            )
        version = self.actuator.snapshot()["version"]
        state = self.controller.rollback(self.tokens["dave"], "safe-maintenance")
        self.assertEqual(state["state"], "ROLLED_BACK")
        self.assertEqual(self.actuator.snapshot()["version"], version)

    def test_operator_revocation_is_enforced(self):
        self.controller.revoke_operator(self.tokens["dave"], "bob")
        with self.assertRaises(AuthorizationError):
            self.controller.approve(self.tokens["bob"], "anything")

    def test_audit_does_not_contain_tokens(self):
        text = repr(self.controller.audit_log(self.tokens["dave"], limit=100))
        for token in self.tokens.values():
            self.assertNotIn(token, text)

    @unittest.skipIf(os.name == "nt", "POSIX mode check")
    def test_database_files_are_private(self):
        control_mode = os.stat(Path(self.temp.name) / "control.db").st_mode & 0o777
        actuator_mode = os.stat(Path(self.temp.name) / "actuator.db").st_mode & 0o777
        self.assertEqual(control_mode, 0o600)
        self.assertEqual(actuator_mode, 0o600)


if __name__ == "__main__":
    unittest.main()
