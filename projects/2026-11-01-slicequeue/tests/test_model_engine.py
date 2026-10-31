from __future__ import annotations

import unittest

from slicequeue.engine import deterministic_handler, execute_tool
from slicequeue.errors import ValidationError
from slicequeue.model import SliceDecision, canonical_json, parse_json


class ModelTests(unittest.TestCase):
    def test_canonical_json_is_stable(self) -> None:
        self.assertEqual(canonical_json({"z": 1, "a": [True, None]}), '{"a":[true,null],"z":1}')

    def test_non_finite_numbers_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json({"value": float("nan")})

    def test_duplicate_json_fields_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "duplicate field"):
            parse_json('{"event_id":"a","event_id":"b"}', "body")

    def test_decision_schema_is_closed(self) -> None:
        with self.assertRaisesRegex(ValidationError, "unknown fields"):
            SliceDecision.from_value({"assistant_message": "ok", "debug": True})

    def test_each_command_has_an_exact_shape(self) -> None:
        decision = SliceDecision.from_value(
            {
                "assistant_message": "run",
                "command": {"type": "TOOL", "tool_name": "math.sum", "arguments": {"numbers": [1]}},
            }
        )
        self.assertEqual(decision.command["type"], "TOOL")
        with self.assertRaisesRegex(ValidationError, "TOOL requires only"):
            SliceDecision.from_value(
                {
                    "assistant_message": "run",
                    "command": {
                        "type": "TOOL",
                        "tool_name": "math.sum",
                        "arguments": {},
                        "payload": {},
                    },
                }
            )


class EngineTests(unittest.TestCase):
    def test_sum_uses_finite_allow_listed_input(self) -> None:
        result = execute_tool("math.sum", {"numbers": [0.1, 0.2, 0.3]})
        self.assertEqual(result, {"ok": True, "count": 3, "sum": 0.6})
        with self.assertRaises(ValidationError):
            execute_tool("math.sum", {"numbers": [float("inf")]})

    def test_unknown_tool_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "allow-listed"):
            execute_tool("shell.exec", {"command": "id"})

    def test_policy_rejects_unknown_or_extra_user_fields(self) -> None:
        unknown = deterministic_handler([], {"kind": "USER", "payload": {"action": "delete"}})
        extra = deterministic_handler(
            [], {"kind": "USER", "payload": {"action": "ping", "admin": True}}
        )
        self.assertIn("Rejected", unknown["assistant_message"])
        self.assertIn("Rejected", extra["assistant_message"])
        self.assertIsNone(unknown["command"])

    def test_word_count_is_deterministic(self) -> None:
        self.assertEqual(
            execute_tool("text.word_count", {"text": "durable slices resume safely"}),
            {"ok": True, "words": 4, "characters": 28},
        )


if __name__ == "__main__":
    unittest.main()
