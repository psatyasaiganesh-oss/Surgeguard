import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

from shiftsafe import Engine
from shiftsafe.demo import run_demo
from shiftsafe.errors import ValidationError
from .helpers import EngineCase, item


class ValidationTests(EngineCase):
    def test_customer_identifiers_reject_sql_injection_bool_and_overflow(self):
        for cid in ("1; DROP TABLE customers;", True, 0, -1, 2**63, 1.0):
            with self.subTest(cid=cid), self.assertRaises(ValidationError):
                self.engine.put(cid, item(), operation_id="bad")
        self.assertEqual(self.engine.status()["customers"], 0)

    def test_money_is_bounded_integer_cents_not_float_or_boolean(self):
        for amount in (True, 1.5, -1, 10**12+1, float("nan"), "100"):
            value = item(amount=amount)
            with self.subTest(amount=amount), self.assertRaises(ValidationError):
                self.engine.put(1, value, operation_id="bad")

    def test_subscription_fields_currency_status_and_ids_are_strict(self):
        malformed = []
        for key, value in (("currency", "inr"), ("currency", "USDD"), ("status", "refunded"),
                           ("id", "../../etc/passwd"), ("plan", "<script>"), ("currency", None)):
            payload = item()
            payload[0][key] = value
            malformed.append(payload)
        malformed += [[{"id": "sub_1"}], [{**item()[0], "extra": "unexpected"}], item()+item(), item()*101, None]
        for value in malformed:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                self.engine.put(1, value, operation_id="bad")
        self.assertEqual(self.engine.status()["operations"], 0)

    def test_operation_key_and_expected_revision_validation(self):
        for key in ("", "a"*129, "has space", None, "<script>"):
            with self.subTest(key=key), self.assertRaises(ValidationError):
                self.engine.put(1, item(), operation_id=key)
        with self.assertRaises(ValidationError):
            self.engine.put(1, item(), operation_id="a", expected_revision=True)

    def test_new_db_permissions_are_private_on_posix(self):
        if os.name == "posix":
            self.assertEqual(Path(self.path).stat().st_mode & 0o777, 0o600)

    def test_database_symlink_is_rejected(self):
        linked = Path(self.folder.name) / "link.db"
        linked.symlink_to(self.path)
        with self.assertRaises(ValidationError):
            Engine(linked)

    def test_unknown_schema_version_is_not_silently_used(self):
        with sqlite3.connect(self.path) as con:
            con.execute("UPDATE migration SET schema_version=99 WHERE id=1")
        with self.assertRaises(ValidationError):
            Engine(self.path)


class CLITests(EngineCase):
    def run_cli(self, *args, expected=0):
        process = subprocess.run([sys.executable, "-m", "shiftsafe", "--db", self.path, *args],
                                 capture_output=True, text=True, timeout=30)
        self.assertEqual(process.returncode, expected, process.stderr)
        return json.loads(process.stdout if expected == 0 else process.stderr)

    def test_full_cli_operator_flow(self):
        path = Path(self.folder.name) / "input.json"
        path.write_text(json.dumps(item()), encoding="utf-8")
        self.assertEqual(self.run_cli("init")["phase"], "LEGACY")
        self.run_cli("put", "1", "--file", str(path), "--operation-id", "one", "--expected-revision", "0")
        self.run_cli("start")
        self.assertTrue(self.run_cli("backfill", "--batch-size", "1")["complete"])
        self.run_cli("shadow")
        self.assertTrue(self.run_cli("verify")["ok"])
        self.run_cli("cutover")
        self.assertEqual(self.run_cli("get", "1")["source"], "new")
        self.run_cli("rollback")
        self.assertEqual(self.run_cli("get", "1")["source"], "legacy")
        self.run_cli("shadow")
        self.run_cli("cutover")
        self.run_cli("finalize")
        self.assertEqual(self.run_cli("cleanup", "--confirm", "DROP_LEGACY")["removed"], 1)
        self.assertEqual(self.run_cli("status")["legacy_customers"], 0)

    def test_cli_invalid_json_has_structured_error_and_no_write(self):
        path = Path(self.folder.name) / "bad.json"
        path.write_text("{invalid", encoding="utf-8")
        result = self.run_cli("put", "1", "--file", str(path), "--operation-id", "a", expected=2)
        self.assertEqual(result["error"], "JSONDecodeError")
        self.assertEqual(self.engine.status()["customers"], 0)

    def test_cli_oversized_input_is_rejected(self):
        path = Path(self.folder.name) / "large.json"
        path.write_bytes(b" " * (256*1024+1))
        result = self.run_cli("put", "1", "--file", str(path), "--operation-id", "a", expected=2)
        self.assertEqual(result["error"], "ValidationError")

    def test_cli_failed_gate_is_nonzero_and_includes_diagnostics(self):
        self.ready(1)
        self.corrupt()
        result = self.run_cli("cutover", expected=2)
        self.assertEqual(result["verification"]["mismatches"], 1)

    def test_demo_is_repeatable_without_touching_existing_database(self):
        self.seed(1)
        before = self.engine.status()
        for _ in range(2):
            report = run_demo(customers=30, writes=20, batch_size=5)
            self.assertTrue(report["success"])
            self.assertEqual(report["safety"]["corruption_mismatches"], 1)
            self.assertEqual(report["safety"]["legacy_rows_removed"], 30)
        self.assertEqual(self.engine.status(), before)
