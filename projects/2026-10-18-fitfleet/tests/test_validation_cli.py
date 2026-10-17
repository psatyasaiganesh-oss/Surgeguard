from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

from fitfleet import Budget, Controller, Policy, Trace
from fitfleet.demo import generate, run_demo
from fitfleet.errors import EvidenceError, ValidationError
from fitfleet.model import MAX_FILE_BYTES, MAX_RUNS, from_dict, load_json
from .helpers import ControllerCase


class ValidationTests(ControllerCase):
    def test_cpu_and_memory_units_reject_boolean_float_negative_and_overflow(self):
        for cpu in (True, 0, 249, -1, 64001, 4000.0):
            with self.subTest(cpu=cpu), self.assertRaises(ValidationError):
                Budget(cpu, 8192)
        for memory in (False, 127, -1, 65537, float("nan"), "1024"):
            with self.subTest(memory=memory), self.assertRaises(ValidationError):
                Budget(4000, memory)

    def test_trace_schema_and_execution_fields_are_strict(self):
        doc = self.training.document()
        for bad in ({**doc, "extra": True}, {**doc, "schema_version": True}, {**doc, "schema_version": 2}, {"runs": []}):
            with self.assertRaises(ValidationError):
                Trace.parse(bad)
        row = doc["runs"][0]
        for bad_row in ({**row, "extra": "bad"}, {**row, "fixed_ms": True}, {**row, "deadline_ms": 0}, {"run_id": "a"}):
            with self.assertRaises(ValidationError):
                Trace.parse({"schema_version": 1, "runs": [bad_row]})

    def test_empty_oversized_and_duplicate_traces_are_rejected(self):
        for runs in ((), (self.training.runs[0],)*(MAX_RUNS+1), (self.training.runs[0],)*2):
            with self.assertRaises(ValidationError):
                Trace(runs)

    def test_mixed_workloads_and_code_revisions_are_rejected(self):
        for change in ({"workload": "other"}, {"code_revision": "v2"}):
            runs = list(self.training.runs)
            runs[0] = replace(runs[0], **change)
            with self.assertRaises(EvidenceError):
                Trace(tuple(runs))

    def test_trace_order_does_not_change_fingerprint(self):
        reversed_trace = Trace(tuple(reversed(self.training.runs)))
        self.assertEqual(reversed_trace.provenance(), self.training.provenance())

    def test_invalid_policy_ranges_and_fields_are_rejected(self):
        for updates in ({"memory_quantile_bp": 10001}, {"max_memory_reduction_bp": True}, {"retry_memory": 1},
                        {"min_training_samples": 0}, {"max_retries": 99}, {"memory_score_weight_bp": 0},
                        {"max_p95_runtime_ratio_bp": 20000, "max_single_runtime_ratio_bp": 11000}):
            with self.subTest(updates=updates), self.assertRaises(ValidationError):
                replace(self.policy, **updates)
        with self.assertRaises(ValidationError):
            from_dict(Policy, {"unknown_threshold": 1})

    def test_duplicate_json_keys_and_nonfinite_constants_are_rejected(self):
        path = Path(self.folder.name) / "bad.json"
        for text in ('{"a":1,"a":2}', '{"score":NaN}', '{"score":Infinity}'):
            path.write_text(text, encoding="utf-8")
            with self.assertRaises(ValidationError):
                load_json(path)

    def test_json_input_read_has_a_size_limit(self):
        path = Path(self.folder.name) / "large.json"
        path.write_bytes(b" "*(MAX_FILE_BYTES+1))
        with self.assertRaises(ValidationError):
            load_json(path)

    def test_excessive_json_nesting_is_a_structured_validation_failure(self):
        path = Path(self.folder.name) / "nested.json"
        path.write_text("["*2000+"0"+"]"*2000, encoding="utf-8")
        with self.assertRaises(ValidationError):
            load_json(path)
        literal = '['*2000+'"\\'+']'*2000
        path.write_text(json.dumps({"literal": literal}), encoding="utf-8")
        self.assertEqual(load_json(path)["literal"], literal)

    def test_identifiers_and_export_image_references_do_not_accept_shell_or_path_payloads(self):
        for workload in ("../../escape", "'; DROP TABLE workloads;--", "UPPER", "x"*64):
            with self.assertRaises(ValidationError):
                self.controller.register(workload, self.baseline, "v1")
        self.controller.register("encoder", self.baseline, "v1")
        for image in ("image; touch /tmp/pwned", "$(env)", "https://host/image?token=bad", "x"*257):
            with self.assertRaises(ValidationError):
                self.controller.manifest("encoder", image=image)
        with self.assertRaises(ValidationError):
            self.controller.show("'; DROP TABLE recommendations;--")

    def test_private_database_permissions_and_symlink_rejection(self):
        if os.name == "posix":
            self.assertEqual(Path(self.path).stat().st_mode & 0o777, 0o600)
        link = Path(self.folder.name) / "link.db"
        link.symlink_to(self.path)
        with self.assertRaises(ValidationError):
            Controller(link)

    def test_unrelated_database_is_not_modified(self):
        path = Path(self.folder.name) / "foreign.db"
        with sqlite3.connect(path) as con:
            con.execute("CREATE TABLE keep(value TEXT)")
            con.execute("INSERT INTO keep VALUES('preserve')")
        with self.assertRaises(ValidationError):
            Controller(path)
        with sqlite3.connect(path) as con:
            self.assertEqual(con.execute("SELECT value FROM keep").fetchone()[0], "preserve")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 1)

    def test_unknown_database_version_is_not_silently_accepted(self):
        with sqlite3.connect(self.path) as con:
            con.execute("PRAGMA user_version=99")
        with self.assertRaises(ValidationError):
            Controller(self.path)


class CLITests(ControllerCase):
    def run_cli(self, *args, expected=0):
        result = subprocess.run([sys.executable, "-m", "fitfleet", "--db", self.path, *args],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, expected, result.stderr)
        return json.loads(result.stdout if expected == 0 else result.stderr)

    def test_complete_cli_flow_and_manifest_without_cluster_credentials(self):
        training_file = Path(self.folder.name) / "training.json"
        validation_file = Path(self.folder.name) / "holdout.json"
        policy_file = Path(self.folder.name) / "policy.json"
        training_file.write_text(json.dumps(self.training.document()), encoding="utf-8")
        validation_file.write_text(json.dumps(self.holdout.document()), encoding="utf-8")
        policy_file.write_text(json.dumps(asdict(self.policy)), encoding="utf-8")
        self.run_cli("register", "encoder", "--cpu-millis", "8000", "--memory-mib", "8192", "--code-revision", "v1",
                     "--opt-in", "cpu", "--opt-in", "memory")
        rec = self.run_cli("plan", "encoder", "--trace", str(training_file), "--policy", str(policy_file))
        self.assertEqual(self.run_cli("verify", rec["id"], "--trace", str(validation_file))["state"], "VERIFIED")
        self.assertEqual(self.run_cli("apply", rec["id"])["state"], "APPLIED")
        manifest = self.run_cli("manifest", "encoder", "--image", "fitfleet-local:demo")
        self.assertEqual(manifest["kind"], "Job")
        self.assertFalse(manifest["spec"]["template"]["spec"]["automountServiceAccountToken"])
        self.assertEqual(self.run_cli("rollback", rec["id"])["state"], "ROLLED_BACK")
        self.assertEqual(self.run_cli("status", "encoder")["cpu_millis"], 8000)

    def test_cli_invalid_json_and_invalid_state_have_nonzero_structured_errors(self):
        path = Path(self.folder.name) / "invalid.json"
        path.write_text("invalid", encoding="utf-8")
        self.controller.register("encoder", self.baseline, "v1")
        error = self.run_cli("plan", "encoder", "--trace", str(path), expected=2)
        self.assertEqual(error["error"], "JSONDecodeError")
        rec = self.controller.register("other", self.baseline, "v1", opt_cpu=True)
        self.assertEqual(rec["version"], 1)
        missing = self.run_cli("apply", "0"*64, expected=2)
        self.assertEqual(missing["error"], "NotFound")

    def test_generated_trace_can_be_read_back_strictly(self):
        document = self.run_cli("generate", "--count", "30", "--start", "10000", "--profile", "memory-shift")
        trace = Trace.parse(document)
        self.assertEqual(len(trace.runs), 30)
        self.assertTrue(all(r.memory_peak_mib == 7000 for r in trace.runs))

    def test_repeated_demo_does_not_modify_existing_registry(self):
        self.controller.register("encoder", self.baseline, "v1")
        before = self.controller.status("encoder")
        first = run_demo(training_count=100, holdout_count=100)
        second = run_demo(training_count=100, holdout_count=100)
        self.assertTrue(first["success"] and second["success"])
        self.assertEqual(first["modeled_holdout_comparison"], second["modeled_holdout_comparison"])
        self.assertEqual(self.controller.status("encoder"), before)
