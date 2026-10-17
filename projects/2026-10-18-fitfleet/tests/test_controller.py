from dataclasses import replace

from fitfleet import Budget, Controller, Trace
from fitfleet.demo import generate
from fitfleet.errors import Conflict, EvidenceError, InvalidState, NotFound, Paused
from .helpers import ControllerCase


class ControllerTests(ControllerCase):
    def test_registration_is_opted_out_by_default(self):
        row = self.controller.register("encoder", self.baseline, "v1")
        self.assertFalse(row["opt_cpu"])
        self.assertFalse(row["opt_memory"])
        with self.assertRaises(EvidenceError):
            self.controller.plan("encoder", self.training, self.policy)

    def test_registration_requires_compare_and_swap_for_updates(self):
        self.controller.register("encoder", self.baseline, "v1")
        with self.assertRaises(Conflict):
            self.controller.register("encoder", Budget(6000, 8000), "v2")
        with self.assertRaises(Conflict):
            self.controller.register("encoder", self.baseline, "v2", expected_version=8)
        updated = self.controller.register("encoder", Budget(6000, 8000), "v2", expected_version=1)
        self.assertEqual(updated["version"], 2)

    def test_repeated_plan_reuses_same_recommendation_without_duplicate_event(self):
        first = self.plan()
        before = self.controller.events("encoder")
        second = self.controller.plan("encoder", self.training, self.policy)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(self.controller.events("encoder"), before)

    def test_plan_does_not_modify_active_configuration(self):
        rec = self.plan()
        self.assertEqual(rec["state"], "CREATED")
        self.assertEqual(self.controller.status("encoder")["cpu_millis"], 8000)
        self.assertEqual(self.controller.status("encoder")["version"], 1)

    def test_newer_disjoint_holdout_is_required(self):
        rec = self.plan()
        with self.assertRaises(EvidenceError):
            self.controller.verify(rec["id"], self.training)
        reused = list(self.holdout.runs)
        reused[0] = replace(reused[0], run_id=self.training.runs[0].run_id)
        with self.assertRaises(EvidenceError):
            self.controller.verify(rec["id"], Trace(tuple(reused)))
        self.assertEqual(self.controller.show(rec["id"])["state"], "CREATED")

    def test_code_revision_and_workload_cohort_are_bound_to_evidence(self):
        rec = self.plan()
        for trace in (generate(revision="v2", count=30, start_s=2400),
                      generate(workload="other", count=30, start_s=2400)):
            with self.assertRaises(EvidenceError):
                self.controller.verify(rec["id"], trace)

    def test_verification_is_idempotent_and_immutable(self):
        first = self.verified()
        self.assertEqual(self.controller.verify(first["id"], self.holdout), first)
        with self.assertRaises(Conflict):
            self.controller.verify(first["id"], self.fresh())

    def test_cannot_replace_failed_evidence_with_an_easier_holdout(self):
        rec = self.plan()
        heavy = generate(count=30, start_s=2400, profile="cpu-shift")
        rejected = self.controller.verify(rec["id"], heavy)
        self.assertEqual(rejected["state"], "REJECTED")
        with self.assertRaises(Conflict):
            self.controller.verify(rec["id"], self.holdout)
        with self.assertRaises(InvalidState):
            self.controller.apply(rec["id"])

    def test_unverified_recommendation_cannot_be_applied(self):
        rec = self.plan()
        with self.assertRaises(InvalidState):
            self.controller.apply(rec["id"])

    def test_apply_and_retry_change_configuration_exactly_once(self):
        rec = self.verified()
        first = self.controller.apply(rec["id"])
        second = self.controller.apply(rec["id"])
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(self.controller.status("encoder")["version"], 2)
        self.assertEqual(self.controller.status("encoder")["memory_mib"], 7040)

    def test_restart_preserves_plan_verification_and_active_version(self):
        rec = self.active()
        other = Controller(self.path)
        self.assertEqual(other.show(rec["id"])["state"], "APPLIED")
        self.assertEqual(other.status("encoder")["version"], 2)

    def test_operator_code_or_budget_change_before_apply_blocks_stale_candidate(self):
        rec = self.verified()
        self.controller.register("encoder", self.baseline, "v2", expected_version=1)
        with self.assertRaises(Conflict):
            self.controller.apply(rec["id"])
        self.assertEqual(self.controller.status("encoder")["code_revision"], "v2")

    def test_pause_blocks_activation_but_not_verification_or_rollback(self):
        rec = self.plan()
        self.controller.set_enabled(False)
        self.assertEqual(self.controller.verify(rec["id"], self.holdout)["state"], "VERIFIED")
        with self.assertRaises(Paused):
            self.controller.apply(rec["id"])
        self.controller.set_enabled(True)
        self.controller.apply(rec["id"])
        self.controller.set_enabled(False)
        self.assertEqual(self.controller.rollback(rec["id"])["state"], "ROLLED_BACK")

    def test_healthy_then_shifted_observations_trigger_local_rollback(self):
        rec = self.active()
        self.assertEqual(self.controller.observe(rec["id"], self.fresh())["state"], "MONITORED")
        shifted = generate(count=30, start_s=6000, profile="memory-shift")
        result = self.controller.observe(rec["id"], shifted)
        self.assertTrue(result["rolled_back"])
        self.assertEqual(self.controller.status("encoder")["cpu_millis"], 8000)
        self.assertEqual(self.controller.status("encoder")["memory_mib"], 8192)
        self.assertEqual(self.controller.status("encoder")["version"], 3)

    def test_observation_retry_after_rollback_is_idempotent(self):
        rec = self.active()
        shifted = self.fresh(memory_peak_mib=7000)
        first = self.controller.observe(rec["id"], shifted)
        version = self.controller.status("encoder")["version"]
        repeated = self.controller.observe(rec["id"], shifted)
        self.assertTrue(first["rolled_back"])
        self.assertTrue(repeated["replayed"])
        self.assertEqual(self.controller.status("encoder")["version"], version)

    def test_monitoring_rejects_reuse_of_verification_or_earlier_windows(self):
        rec = self.active()
        with self.assertRaises(EvidenceError):
            self.controller.observe(rec["id"], self.holdout)
        first = self.fresh()
        self.controller.observe(rec["id"], first)
        self.assertTrue(self.controller.observe(rec["id"], first)["replayed"])

    def test_rollback_never_overwrites_an_operator_override(self):
        rec = self.active()
        self.controller.register("encoder", Budget(6000, 7808), "v2", expected_version=2)
        result = self.controller.rollback(rec["id"])
        self.assertEqual(result["state"], "SUPERSEDED")
        self.assertEqual(self.controller.status("encoder")["code_revision"], "v2")
        self.assertEqual(self.controller.status("encoder")["cpu_millis"], 6000)
        self.assertEqual(self.controller.status("encoder")["version"], 3)

    def test_monitor_marks_operator_override_as_superseded_without_replaying_old_code(self):
        rec = self.active()
        self.controller.register("encoder", Budget(6000, 7808), "v2", expected_version=2)
        result = self.controller.observe(rec["id"], self.fresh(memory_peak_mib=7000))
        self.assertFalse(result["rolled_back"])
        self.assertEqual(result["state"], "SUPERSEDED")
        self.assertEqual(self.controller.status("encoder")["cpu_millis"], 6000)

    def test_cooldown_blocks_an_immediate_second_tune_after_rollback(self):
        self.controller.register("encoder", self.baseline, "v1", opt_cpu=True, opt_memory=True)
        policy = replace(self.policy, cooldown_s=100)
        first = self.controller.plan("encoder", self.training, policy)
        self.controller.verify(first["id"], self.holdout)
        self.controller.apply(first["id"])
        self.controller.rollback(first["id"])
        next_rec = self.controller.plan("encoder", self.training, policy)
        self.assertNotEqual(next_rec["id"], first["id"])
        self.controller.verify(next_rec["id"], self.holdout)
        with self.assertRaises(InvalidState):
            self.controller.apply(next_rec["id"])
        self.clock[0] += 100
        self.assertEqual(self.controller.apply(next_rec["id"])["state"], "APPLIED")

    def test_failure_after_config_update_rolls_back_config_and_recommendation(self):
        rec = self.verified()
        def fail(point):
            if point == "after_apply_config_update":
                raise RuntimeError("injected crash")
        with self.assertRaises(RuntimeError):
            Controller(self.path, fault=fail).apply(rec["id"])
        self.assertEqual(self.controller.status("encoder")["version"], 1)
        self.assertEqual(self.controller.show(rec["id"])["state"], "VERIFIED")
        self.assertEqual(self.controller.apply(rec["id"])["state"], "APPLIED")

    def test_rollback_failure_is_atomic_and_retryable(self):
        rec = self.active()
        def fail(point):
            if point == "after_rollback_config_update":
                raise RuntimeError("injected rollback failure")
        with self.assertRaises(RuntimeError):
            Controller(self.path, fault=fail).rollback(rec["id"])
        self.assertEqual(self.controller.status("encoder")["version"], 2)
        self.assertEqual(self.controller.show(rec["id"])["state"], "APPLIED")
        self.assertEqual(self.controller.rollback(rec["id"])["state"], "ROLLED_BACK")

    def test_rollback_is_terminal_and_retry_does_not_mutate_again(self):
        rec = self.active()
        self.controller.rollback(rec["id"])
        self.assertTrue(self.controller.rollback(rec["id"])["replayed"])
        with self.assertRaises(InvalidState):
            self.controller.apply(rec["id"])
        self.assertEqual(self.controller.status("encoder")["version"], 3)

    def test_manifest_exports_only_the_active_configuration(self):
        rec = self.verified()
        before = self.controller.manifest("encoder", image="fitfleet-local:demo")
        self.assertEqual(before["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"], "8000m")
        self.controller.apply(rec["id"])
        after = self.controller.manifest("encoder", image="fitfleet-local:demo")
        resources = after["spec"]["template"]["spec"]["containers"][0]["resources"]
        self.assertEqual(resources["limits"], {"cpu": "4000m", "memory": "7040Mi"})
        self.assertEqual(resources["limits"], resources["requests"])
        self.assertEqual(after["apiVersion"], "batch/v1")
        pod = after["spec"]["template"]["spec"]
        self.assertEqual(pod["containers"][0]["securityContext"]["runAsUser"], 10001)
        self.assertEqual(pod["containers"][0]["volumeMounts"][0]["mountPath"], "/tmp")
        self.assertEqual(pod["volumes"][0]["emptyDir"]["sizeLimit"], "128Mi")

    def test_event_pages_include_pause_events_without_revealing_execution_payloads(self):
        self.plan()
        self.controller.set_enabled(False)
        first = self.controller.events("encoder", limit=2)
        second = self.controller.events("encoder", after=first[-1]["seq"], limit=2)
        self.assertEqual(second[-1]["kind"], "activation_paused")
        self.assertNotIn("cpu_work_ms", str(first+second))

    def test_missing_workload_and_recommendation_have_explicit_errors(self):
        with self.assertRaises(NotFound):
            self.controller.status("missing")
        with self.assertRaises(NotFound):
            self.controller.show("0"*64)
