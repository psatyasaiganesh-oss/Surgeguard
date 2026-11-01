from __future__ import annotations

import unittest

from exitpack.errors import ValidationError
from exitpack.model import (
    VMRequest,
    canonical_json,
    lifetime_class,
    parse_json,
    require_identifier,
    require_int,
    require_policy,
)


class ModelTests(unittest.TestCase):
    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"z": 1, "a": [True]}), '{"a":[true],"z":1}')

    def test_duplicate_fields_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "duplicate field"):
            parse_json('{"vm_id":"a","vm_id":"b"}', "body")

    def test_non_finite_values_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json({"value": float("nan")})

    def test_vm_schema_is_closed(self) -> None:
        with self.assertRaisesRegex(ValidationError, "unknown fields"):
            VMRequest.from_value(
                {
                    "vm_id": "vm",
                    "cpu_millis": 1,
                    "memory_mib": 1,
                    "predicted_lifetime_s": 1,
                    "privileged": True,
                }
            )

    def test_boolean_is_not_an_integer(self) -> None:
        with self.assertRaises(ValidationError):
            require_int(True, "cpu")

    def test_identifier_grammar_rejects_path_characters(self) -> None:
        with self.assertRaises(ValidationError):
            require_identifier("../host", "host_id")

    def test_lifetime_class_boundaries(self) -> None:
        self.assertEqual(
            [lifetime_class(value) for value in (3_600, 3_601, 36_001, 360_001)],
            [0, 1, 2, 3],
        )

    def test_lifetime_has_a_safe_upper_bound(self) -> None:
        with self.assertRaises(ValidationError):
            lifetime_class(3_600_001)

    def test_policy_allow_list(self) -> None:
        self.assertEqual(require_policy("LIFETIME"), "LIFETIME")
        with self.assertRaises(ValidationError):
            require_policy("EXECUTE_ANYWHERE")


if __name__ == "__main__":
    unittest.main()
