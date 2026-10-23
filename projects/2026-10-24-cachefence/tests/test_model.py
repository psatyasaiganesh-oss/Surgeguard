from __future__ import annotations

import json
import unittest

from cachefence.model import (
    IntegrityError,
    InvalidationEvent,
    Snapshot,
    ValidationError,
    canonical_json,
    content_digest,
    snapshot_for,
    strict_loads,
    validate_id,
    validate_json_value,
)


class ModelTests(unittest.TestCase):
    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"z": 1, "a": 2}), '{"a":2,"z":1}')

    def test_strict_json_rejects_duplicate_keys(self) -> None:
        with self.assertRaisesRegex(ValidationError, "duplicate"):
            strict_loads('{"x":1,"x":2}')

    def test_strict_json_rejects_nan(self) -> None:
        with self.assertRaises(ValidationError):
            strict_loads('{"x":NaN}')

    def test_strict_json_rejects_bad_utf8(self) -> None:
        with self.assertRaisesRegex(ValidationError, "UTF-8"):
            strict_loads(b"\xff")

    def test_strict_json_enforces_body_limit(self) -> None:
        with self.assertRaisesRegex(ValidationError, "exceeds"):
            strict_loads("{}", max_bytes=1)

    def test_identifier_is_closed(self) -> None:
        self.assertEqual(validate_id("tenant-1.prod", "tenant"), "tenant-1.prod")
        for value in ("", " space", "../escape", "a/b", "a" * 97):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_id(value, "tenant")

    def test_value_rejects_nonfinite_numbers(self) -> None:
        with self.assertRaises(ValidationError):
            validate_json_value(float("inf"))

    def test_value_rejects_excessive_depth(self) -> None:
        value: object = 1
        for _ in range(10):
            value = [value]
        with self.assertRaisesRegex(ValidationError, "nesting"):
            validate_json_value(value)

    def test_digest_is_semantic_and_deterministic(self) -> None:
        first = content_digest(deleted=False, value={"b": 2, "a": 1})
        second = content_digest(deleted=False, value={"a": 1, "b": 2})
        self.assertEqual(first, second)
        self.assertNotEqual(first, content_digest(deleted=True))

    def test_snapshot_round_trip(self) -> None:
        snapshot = snapshot_for(
            tenant="acme", key="x", version=7, deleted=False, value={"n": 9}, updated_ns=11
        )
        self.assertEqual(Snapshot.from_dict(snapshot.as_dict()), snapshot)

    def test_snapshot_detects_digest_mismatch(self) -> None:
        snapshot = Snapshot("acme", "x", 1, "0" * 64, False, {"n": 1}, 2)
        with self.assertRaises(IntegrityError):
            snapshot.validate()

    def test_deleted_snapshot_rejects_value(self) -> None:
        value = {
            "tenant": "acme",
            "key": "x",
            "version": 1,
            "digest": content_digest(deleted=True),
            "deleted": True,
            "value": 3,
            "updated_ns": 1,
        }
        with self.assertRaises(ValidationError):
            Snapshot.from_dict(value)

    def test_direct_deleted_snapshot_rejects_hidden_value(self) -> None:
        snapshot = Snapshot(
            "acme", "x", 1, content_digest(deleted=True), True, "hidden", 1
        )
        with self.assertRaises(ValidationError):
            snapshot.validate()

    def test_event_round_trip_and_stable_identity(self) -> None:
        event = InvalidationEvent("o", "t", "k", 1, "a" * 64, False, 9)
        restored = InvalidationEvent.from_json(event.to_json())
        self.assertEqual(restored, event)
        self.assertEqual(restored.event_id, event.event_id)

    def test_event_rejects_identity_tampering(self) -> None:
        event = InvalidationEvent("o", "t", "k", 1, "a" * 64, False, 9)
        value = event.as_dict()
        value["event_id"] = "0" * 64
        with self.assertRaisesRegex(ValidationError, "event_id"):
            InvalidationEvent.from_dict(value)

    def test_event_rejects_unknown_fields(self) -> None:
        event = InvalidationEvent("o", "t", "k", 1, "a" * 64, False, 9)
        value = event.as_dict() | {"admin": True}
        with self.assertRaisesRegex(ValidationError, "unknown"):
            InvalidationEvent.from_dict(value)

    def test_standard_json_parser_difference_is_explicit(self) -> None:
        # Regression guard: Python's default parser accepts this, ours must not.
        self.assertTrue(json.loads("NaN") != json.loads("NaN"))
        with self.assertRaises(ValidationError):
            strict_loads("NaN")


if __name__ == "__main__":
    unittest.main()
