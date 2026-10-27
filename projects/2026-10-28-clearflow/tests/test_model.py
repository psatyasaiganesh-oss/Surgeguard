from __future__ import annotations

import math
import unittest

from clearflow.errors import ValidationError
from clearflow.model import (
    MAX_POSTINGS,
    MoneyEvent,
    Posting,
    QualityPolicy,
    canonical_json,
    normalize_references,
    parse_json,
    require_identifier,
    validate_account_kind,
)


def valid_event() -> dict:
    return {
        "event_id": "evt:1",
        "producer": "payments",
        "reference_id": "charge:1:create",
        "currency": "USD",
        "occurred_ns": 10,
        "postings": [
            {"account_id": "cash", "amount_minor": -100},
            {"account_id": "pending:1", "amount_minor": 100},
        ],
        "metadata": {"region": "ap-south"},
    }


class ModelTests(unittest.TestCase):
    def test_canonical_json_sorts_fields(self) -> None:
        self.assertEqual(canonical_json({"b": 1, "a": 2}), '{"a":2,"b":1}')

    def test_canonical_json_rejects_nan(self) -> None:
        with self.assertRaises(ValidationError):
            canonical_json({"bad": math.nan})

    def test_parse_json_rejects_duplicate_keys(self) -> None:
        with self.assertRaises(ValidationError):
            parse_json('{"a":1,"a":2}')

    def test_parse_json_rejects_nonfinite_number(self) -> None:
        with self.assertRaises(ValidationError):
            parse_json('{"a":NaN}')

    def test_identifier_accepts_namespaced_values(self) -> None:
        self.assertEqual(require_identifier("pending:charge-1", "id"), "pending:charge-1")

    def test_identifier_rejects_path_syntax(self) -> None:
        with self.assertRaises(ValidationError):
            require_identifier("../secret", "id")

    def test_posting_accepts_integer_minor_units(self) -> None:
        posting = Posting.from_value({"account_id": "cash", "amount_minor": -125})
        self.assertEqual(posting.amount_minor, -125)

    def test_posting_rejects_zero(self) -> None:
        with self.assertRaises(ValidationError):
            Posting.from_value({"account_id": "cash", "amount_minor": 0})

    def test_posting_rejects_boolean_as_integer(self) -> None:
        with self.assertRaises(ValidationError):
            Posting.from_value({"account_id": "cash", "amount_minor": True})

    def test_event_accepts_balanced_postings(self) -> None:
        model = MoneyEvent.from_value(valid_event())
        self.assertEqual(sum(item.amount_minor for item in model.postings), 0)

    def test_event_rejects_unbalanced_postings(self) -> None:
        body = valid_event()
        body["postings"][1]["amount_minor"] = 99
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_event_rejects_duplicate_account_lines(self) -> None:
        body = valid_event()
        body["postings"] = [
            {"account_id": "cash", "amount_minor": -50},
            {"account_id": "cash", "amount_minor": -50},
            {"account_id": "pending:1", "amount_minor": 100},
        ]
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_event_requires_two_postings(self) -> None:
        body = valid_event()
        body["postings"] = [{"account_id": "cash", "amount_minor": 1}]
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_event_caps_posting_count(self) -> None:
        body = valid_event()
        body["postings"] = [
            {"account_id": f"a:{index}", "amount_minor": 1}
            for index in range(MAX_POSTINGS + 1)
        ]
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_event_rejects_invalid_metadata_key(self) -> None:
        body = valid_event()
        body["metadata"] = {"bad key": "value"}
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_event_rejects_large_metadata_value(self) -> None:
        body = valid_event()
        body["metadata"] = {"note": "x" * 513}
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_event_rejects_lowercase_currency(self) -> None:
        body = valid_event()
        body["currency"] = "usd"
        with self.assertRaises(ValidationError):
            MoneyEvent.from_value(body)

    def test_policy_accepts_explicit_bounds(self) -> None:
        policy = QualityPolicy.from_value(
            {
                "max_lag_ns": 1,
                "clearing_grace_ns": 2,
                "max_missing_references": 3,
                "minimum_score": 99.5,
            }
        )
        self.assertEqual(policy.minimum_score, 99.5)

    def test_policy_rejects_score_above_100(self) -> None:
        with self.assertRaises(ValidationError):
            QualityPolicy.from_value(
                {
                    "max_lag_ns": 1,
                    "clearing_grace_ns": 2,
                    "max_missing_references": 0,
                    "minimum_score": 100.1,
                }
            )

    def test_references_are_sorted(self) -> None:
        self.assertEqual(normalize_references(["z", "a"]), ("a", "z"))

    def test_references_must_be_unique(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_references(["same", "same"])

    def test_account_kind_is_closed(self) -> None:
        with self.assertRaises(ValidationError):
            validate_account_kind("asset")


if __name__ == "__main__":
    unittest.main()
