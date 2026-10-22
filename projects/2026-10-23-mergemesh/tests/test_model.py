from __future__ import annotations

import json
import unittest

from mergemesh.model import (
    MAX_CLOCK_ENTRIES,
    MAX_COLLECTION_ITEMS,
    MAX_STRING_LENGTH,
    ConflictError,
    FieldMutation,
    Operation,
    ValidationError,
    canonical_json,
    compare_clocks,
    join_clocks,
    normalize_clock,
    strict_loads,
    validate_field_name,
    validate_identifier,
    validate_json_value,
)


class StrictJsonTests(unittest.TestCase):
    def test_strict_loads_accepts_object(self) -> None:
        self.assertEqual(strict_loads('{"a":1}'), {"a": 1})

    def test_strict_loads_rejects_duplicate_key(self) -> None:
        with self.assertRaisesRegex(ValidationError, "duplicate"):
            strict_loads('{"a":1,"a":2}')

    def test_strict_loads_rejects_nan(self) -> None:
        with self.assertRaisesRegex(ValidationError, "non-finite"):
            strict_loads('{"a":NaN}')

    def test_strict_loads_rejects_invalid_utf8(self) -> None:
        with self.assertRaisesRegex(ValidationError, "UTF-8"):
            strict_loads(b"\xff")

    def test_strict_loads_rejects_oversize(self) -> None:
        with self.assertRaisesRegex(ValidationError, "exceeds"):
            strict_loads("{}", max_bytes=1)

    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"z": 1, "a": 2}), '{"a":2,"z":1}')


class ValidationTests(unittest.TestCase):
    def test_identifier_accepts_safe_value(self) -> None:
        self.assertEqual(validate_identifier("east-1", "region"), "east-1")

    def test_identifier_rejects_slash(self) -> None:
        with self.assertRaises(ValidationError):
            validate_identifier("../east", "region")

    def test_identifier_rejects_too_long(self) -> None:
        with self.assertRaises(ValidationError):
            validate_identifier("a" * 65, "region")

    def test_field_requires_leading_letter(self) -> None:
        with self.assertRaises(ValidationError):
            validate_field_name("1field")

    def test_json_value_accepts_nested_value(self) -> None:
        validate_json_value({"a": [1, True, None, "x"]})

    def test_json_value_rejects_infinite_float(self) -> None:
        with self.assertRaises(ValidationError):
            validate_json_value(float("inf"))

    def test_json_value_rejects_long_string(self) -> None:
        with self.assertRaises(ValidationError):
            validate_json_value("x" * (MAX_STRING_LENGTH + 1))

    def test_json_value_rejects_large_collection(self) -> None:
        with self.assertRaises(ValidationError):
            validate_json_value(list(range(MAX_COLLECTION_ITEMS + 1)))

    def test_json_value_rejects_excessive_depth(self) -> None:
        value: object = 1
        for _ in range(10):
            value = [value]
        with self.assertRaises(ValidationError):
            validate_json_value(value)

    def test_conflict_error_is_runtime_error(self) -> None:
        self.assertIsInstance(ConflictError("changed"), RuntimeError)


class ClockTests(unittest.TestCase):
    def test_normalize_clock_drops_zero_and_sorts(self) -> None:
        self.assertEqual(normalize_clock({"west": 0, "east": 2}), {"east": 2})

    def test_normalize_clock_rejects_boolean(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_clock({"east": True})

    def test_normalize_clock_rejects_negative(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_clock({"east": -1})

    def test_normalize_clock_rejects_too_many_regions(self) -> None:
        clock = {f"r{i}": 1 for i in range(MAX_CLOCK_ENTRIES + 1)}
        with self.assertRaises(ValidationError):
            normalize_clock(clock)

    def test_join_clocks_uses_component_max(self) -> None:
        self.assertEqual(
            join_clocks({"east": 1, "west": 3}, {"east": 2, "central": 1}),
            {"central": 1, "east": 2, "west": 3},
        )

    def test_compare_equal(self) -> None:
        self.assertEqual(compare_clocks({"east": 1}, {"east": 1}), "equal")

    def test_compare_before(self) -> None:
        self.assertEqual(compare_clocks({"east": 1}, {"east": 2}), "before")

    def test_compare_after(self) -> None:
        self.assertEqual(compare_clocks({"east": 2}, {"east": 1}), "after")

    def test_compare_concurrent(self) -> None:
        self.assertEqual(compare_clocks({"east": 1}, {"west": 1}), "concurrent")


class OperationTests(unittest.TestCase):
    def operation(self, **overrides: object) -> Operation:
        values = {
            "tenant": "acme",
            "document": "sku-1",
            "origin": "east",
            "sequence": 1,
            "context": {},
            "changes": {"price": 10},
            "created_ns": 123,
        }
        values.update(overrides)
        return Operation.create(**values)  # type: ignore[arg-type]

    def test_create_round_trip(self) -> None:
        operation = self.operation()
        self.assertEqual(Operation.from_json(operation.to_json()), operation)

    def test_clock_contains_dot(self) -> None:
        operation = self.operation(sequence=3, context={"east": 1, "west": 2})
        self.assertEqual(operation.clock, {"east": 3, "west": 2})

    def test_delete_mutation_has_no_value(self) -> None:
        operation = self.operation(changes={}, delete_fields=["price"])
        self.assertEqual(operation.mutations["price"], FieldMutation(True))
        self.assertNotIn("value", operation.as_dict()["mutations"]["price"])

    def test_same_field_change_and_delete_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.operation(delete_fields=["price"])

    def test_empty_operation_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "at least one"):
            self.operation(changes={})

    def test_boolean_sequence_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.operation(sequence=True)

    def test_origin_context_must_precede_dot(self) -> None:
        with self.assertRaises(ValidationError):
            self.operation(sequence=2, context={"east": 2})

    def test_unknown_operation_field_rejected(self) -> None:
        data = self.operation().as_dict()
        data["unexpected"] = 1
        with self.assertRaisesRegex(ValidationError, "unknown"):
            Operation.from_dict(data)

    def test_wrong_operation_id_rejected(self) -> None:
        data = self.operation().as_dict()
        data["op_id"] = "west:9"
        with self.assertRaisesRegex(ValidationError, "op_id"):
            Operation.from_dict(data)

    def test_deleted_mutation_cannot_include_value(self) -> None:
        data = self.operation().as_dict()
        data["mutations"]["price"] = {"deleted": True, "value": 1}
        with self.assertRaises(ValidationError):
            Operation.from_dict(data)

    def test_live_mutation_requires_value_even_if_null_supported(self) -> None:
        with self.assertRaises(ValidationError):
            FieldMutation.from_dict({"deleted": False})
        self.assertEqual(FieldMutation.from_dict({"deleted": False, "value": None}).value, None)

    def test_operation_json_duplicate_key_rejected(self) -> None:
        encoded = self.operation().to_json()
        duplicate = encoded[:-1] + ',"schema":1}'
        with self.assertRaises(ValidationError):
            Operation.from_json(duplicate)

    def test_operation_is_canonical(self) -> None:
        operation = self.operation(changes={"zeta": 1, "alpha": 2})
        self.assertEqual(operation.to_json(), canonical_json(json.loads(operation.to_json())))


if __name__ == "__main__":
    unittest.main()
