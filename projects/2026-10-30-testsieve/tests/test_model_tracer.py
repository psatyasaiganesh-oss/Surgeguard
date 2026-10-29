from __future__ import annotations

import builtins
import math
import os
import tempfile
import unittest
from pathlib import Path

from testsieve.errors import StateError, ValidationError
from testsieve.model import (
    BaselineSnapshot,
    TestSpec,
    canonical_json,
    normalize_inventory,
    normalize_path,
    normalize_test_ids,
    parse_json,
    require_digest,
    require_identifier,
    require_test_id,
)
from testsieve.tracer import FileAccessTracer


DIGEST = "a" * 64


def baseline_value():
    return {
        "inventory": {"lib/a.py": DIGEST, "tests/a_test.py": "b" * 64},
        "tests": [
            {
                "test_id": "tests/a_test.py",
                "estimated_ms": 10,
                "mandatory": False,
                "last_status": "PASS",
            }
        ],
        "observations": {"tests/a_test.py": ["lib/a.py", "tests/a_test.py"]},
        "root_files": [],
        "completed_commit": "abc123",
    }


class ModelTests(unittest.TestCase):
    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"z": 1, "a": 2}), '{"a":2,"z":1}')

    def test_canonical_json_rejects_nan(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json({"bad": math.nan})

    def test_parse_json_rejects_duplicate_fields(self) -> None:
        with self.assertRaises(ValidationError):
            parse_json('{"a":1,"a":2}')

    def test_parse_json_rejects_nonfinite_numbers(self) -> None:
        with self.assertRaises(ValidationError):
            parse_json('{"bad":Infinity}')

    def test_identifier_accepts_repository_name(self) -> None:
        self.assertEqual(require_identifier("checkout-monorepo", "repo"), "checkout-monorepo")

    def test_identifier_rejects_slash(self) -> None:
        with self.assertRaises(ValidationError):
            require_identifier("owner/repo", "repo")

    def test_test_id_accepts_path_and_scope(self) -> None:
        value = "tests/payments_test.py::test_refund[usd]"
        self.assertEqual(require_test_id(value), value)

    def test_test_id_rejects_parent_traversal(self) -> None:
        with self.assertRaises(ValidationError):
            require_test_id("tests/../secret")

    def test_path_accepts_normal_relative_file(self) -> None:
        self.assertEqual(normalize_path("config/currency.json"), "config/currency.json")

    def test_path_rejects_absolute_path(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_path("/etc/passwd")

    def test_path_rejects_parent_traversal(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_path("lib/../secret")

    def test_path_rejects_backslash(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_path("lib\\a.py")

    def test_digest_requires_lowercase_sha256(self) -> None:
        with self.assertRaises(ValidationError):
            require_digest("A" * 64)

    def test_inventory_is_sorted(self) -> None:
        result = normalize_inventory({"z.py": "b" * 64, "a.py": "a" * 64})
        self.assertEqual(list(result), ["a.py", "z.py"])

    def test_inventory_must_not_be_empty(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_inventory({})

    def test_test_spec_accepts_duration(self) -> None:
        spec = TestSpec.from_value(baseline_value()["tests"][0])
        self.assertEqual(spec.estimated_ms, 10.0)

    def test_test_spec_rejects_boolean_duration(self) -> None:
        value = baseline_value()["tests"][0]
        value["estimated_ms"] = True
        with self.assertRaises(ValidationError):
            TestSpec.from_value(value)

    def test_test_spec_rejects_unknown_status(self) -> None:
        value = baseline_value()["tests"][0]
        value["last_status"] = "FLAKY"
        with self.assertRaises(ValidationError):
            TestSpec.from_value(value)

    def test_baseline_normalizes_valid_snapshot(self) -> None:
        snapshot = BaselineSnapshot.from_value(baseline_value())
        self.assertEqual(len(snapshot.tests), 1)
        self.assertEqual(snapshot.observations["tests/a_test.py"], ("lib/a.py", "tests/a_test.py"))

    def test_baseline_rejects_duplicate_test_ids(self) -> None:
        value = baseline_value()
        value["tests"].append(dict(value["tests"][0]))
        with self.assertRaises(ValidationError):
            BaselineSnapshot.from_value(value)

    def test_baseline_rejects_unknown_observation_test(self) -> None:
        value = baseline_value()
        value["observations"]["tests/missing.py"] = ["lib/a.py"]
        with self.assertRaises(ValidationError):
            BaselineSnapshot.from_value(value)

    def test_baseline_rejects_dependency_outside_inventory(self) -> None:
        value = baseline_value()
        value["observations"]["tests/a_test.py"].append("missing.py")
        with self.assertRaises(ValidationError):
            BaselineSnapshot.from_value(value)

    def test_baseline_rejects_root_file_outside_inventory(self) -> None:
        value = baseline_value()
        value["root_files"] = ["missing.py"]
        with self.assertRaises(ValidationError):
            BaselineSnapshot.from_value(value)

    def test_test_id_list_rejects_unknown_test(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_test_ids(["tests/missing.py"], ["tests/a.py"], "failures")


class TracerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "lib").mkdir()
        (self.root / "lib" / "a.py").write_text("A", encoding="utf-8")
        (self.root / "config.json").write_text("{}", encoding="utf-8")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_builtin_open_is_attributed_to_scope(self) -> None:
        tracer = FileAccessTracer(self.root)
        with tracer:
            with tracer.scope("tests/a_test.py"):
                with open(self.root / "lib" / "a.py", encoding="utf-8") as handle:
                    handle.read()
        self.assertEqual(
            tracer.snapshot()["observations"], {"tests/a_test.py": ["lib/a.py"]}
        )

    def test_os_open_is_attributed_to_scope(self) -> None:
        tracer = FileAccessTracer(self.root)
        with tracer:
            with tracer.scope("tests/a_test.py"):
                descriptor = os.open(self.root / "config.json", os.O_RDONLY)
                os.close(descriptor)
        self.assertEqual(
            tracer.snapshot()["observations"], {"tests/a_test.py": ["config.json"]}
        )

    def test_root_scope_is_recorded_separately(self) -> None:
        tracer = FileAccessTracer(self.root)
        with tracer:
            with open(self.root / "config.json", encoding="utf-8") as handle:
                handle.read()
        self.assertEqual(tracer.snapshot()["root_files"], ["config.json"])

    def test_outside_file_is_ignored(self) -> None:
        tracer = FileAccessTracer(self.root)
        with tracer:
            with open(__file__, encoding="utf-8") as handle:
                handle.read(1)
        self.assertEqual(tracer.snapshot(), {"root_files": [], "observations": {}})

    def test_scope_requires_active_tracer(self) -> None:
        tracer = FileAccessTracer(self.root)
        with self.assertRaises(StateError):
            with tracer.scope("tests/a_test.py"):
                pass

    def test_tracer_restores_open_functions(self) -> None:
        original_builtin = builtins.open
        original_os = os.open
        with FileAccessTracer(self.root):
            self.assertIsNot(builtins.open, original_builtin)
            self.assertIsNot(os.open, original_os)
        self.assertIs(builtins.open, original_builtin)
        self.assertIs(os.open, original_os)

    def test_tracer_cannot_enter_twice(self) -> None:
        tracer = FileAccessTracer(self.root)
        with tracer:
            with self.assertRaises(StateError):
                tracer.__enter__()


if __name__ == "__main__":
    unittest.main()
