from __future__ import annotations

import math
import unittest

from chronocommit.clock import ManualBoundedClock, SystemBoundedClock, TimeInterval
from chronocommit.errors import ClockUnsafeError, ValidationError
from chronocommit.model import (
    MAX_JSON_BYTES,
    CommitResult,
    Mutation,
    VersionedValue,
    canonical_json,
    digest_json,
    validate_id,
    validate_mutations,
    validate_timestamp,
)


class ClockTests(unittest.TestCase):
    def test_interval_reports_uncertainty(self) -> None:
        self.assertEqual(TimeInterval(10, 25).uncertainty_ns, 15)

    def test_interval_rejects_reverse_bounds(self) -> None:
        with self.assertRaises(ValidationError):
            TimeInterval(2, 1)

    def test_manual_clock_returns_bounds(self) -> None:
        clock = ManualBoundedClock(100, 7)
        self.assertEqual(clock.now(), TimeInterval(93, 107))

    def test_manual_clock_advances(self) -> None:
        clock = ManualBoundedClock(100, 7)
        clock.advance(9)
        self.assertEqual(clock.now(), TimeInterval(102, 116))

    def test_manual_clock_rejects_negative_advance(self) -> None:
        clock = ManualBoundedClock(100, 7)
        with self.assertRaises(ValidationError):
            clock.advance(-1)

    def test_manual_commit_wait_moves_earliest_past_timestamp(self) -> None:
        clock = ManualBoundedClock(100, 7)
        earliest = clock.wait_until_after(110)
        self.assertEqual(earliest, 111)
        self.assertEqual(clock.wait_count, 1)
        self.assertEqual(clock.waited_ns, 18)

    def test_manual_wait_that_is_already_safe_does_not_advance(self) -> None:
        clock = ManualBoundedClock(100, 7)
        self.assertEqual(clock.wait_until_after(90), 93)
        self.assertEqual(clock.waited_ns, 0)

    def test_manual_clock_can_be_set_for_fault_injection(self) -> None:
        clock = ManualBoundedClock(100, 7)
        clock.set_center(90)
        self.assertEqual(clock.now().latest_ns, 97)

    def test_manual_clock_rejects_uncertainty_larger_than_center(self) -> None:
        with self.assertRaises(ValidationError):
            ManualBoundedClock(5, 6)

    def test_system_clock_bounds_center(self) -> None:
        clock = SystemBoundedClock(uncertainty_ns=3, wall_time_ns=lambda: 100)
        self.assertEqual(clock.now(), TimeInterval(97, 103))

    def test_system_clock_fails_closed_on_backstep(self) -> None:
        readings = iter([100, 99])
        clock = SystemBoundedClock(0, wall_time_ns=lambda: next(readings))
        clock.now()
        with self.assertRaises(ClockUnsafeError):
            clock.now()

    def test_system_clock_rejects_large_uncertainty(self) -> None:
        with self.assertRaises(ValidationError):
            SystemBoundedClock(1_000_000_001)


class ModelTests(unittest.TestCase):
    def test_canonical_json_is_order_independent(self) -> None:
        self.assertEqual(canonical_json({"b": 2, "a": 1}), '{"a":1,"b":2}')

    def test_canonical_json_rejects_nan(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json({"x": math.nan})

    def test_canonical_json_rejects_oversize_values(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json("x" * MAX_JSON_BYTES)

    def test_digest_is_stable(self) -> None:
        self.assertEqual(digest_json({"a": 1}), digest_json({"a": 1}))
        self.assertNotEqual(digest_json({"a": 1}), digest_json({"a": 2}))

    def test_identifier_validation(self) -> None:
        self.assertEqual(validate_id("tenant-1", "tenant"), "tenant-1")
        for invalid in ("", " bad", "bad/slash", "x" * 129, 3):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                validate_id(invalid, "id")  # type: ignore[arg-type]

    def test_timestamp_rejects_bool_and_negative(self) -> None:
        for invalid in (True, -1, 1.5):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                validate_timestamp(invalid)  # type: ignore[arg-type]

    def test_mutation_rejects_value_on_delete(self) -> None:
        with self.assertRaises(ValidationError):
            Mutation("a", 3, 0, delete=True).validated()

    def test_mutation_set_is_sorted(self) -> None:
        result = validate_mutations([Mutation("b", 2, 0), Mutation("a", 1, 0)])
        self.assertEqual([item.key for item in result], ["a", "b"])

    def test_mutation_set_rejects_duplicates(self) -> None:
        with self.assertRaises(ValidationError):
            validate_mutations([Mutation("a", 1, 0), Mutation("a", 2, 0)])

    def test_mutation_set_rejects_empty(self) -> None:
        with self.assertRaises(ValidationError):
            validate_mutations([])

    def test_versioned_value_serializes_tombstone(self) -> None:
        value = VersionedValue("key", None, 7, deleted=True, txn_id="txn")
        self.assertIsNone(value.as_dict()["value"])

    def test_commit_result_serializes_replay_flag(self) -> None:
        result = CommitResult("txn", "t", "i", 9, 2, True, 10)
        self.assertTrue(result.as_dict()["replayed"])


if __name__ == "__main__":
    unittest.main()

